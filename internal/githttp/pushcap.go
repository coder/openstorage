package githttp

// Decoded-byte ingress cap for git receive-pack (issue #55). Without it,
// handleServiceRPC streamed the request body — including an unbounded gzip
// decode — straight into `git receive-pack`, which quarantines the pack on
// local disk BEFORE any policy or quota runs: the pack quota bounds
// retained storage, not ingress work, so a repo-scoped git:write token
// could exhaust replica disk/CPU with arbitrarily large push streams.
//
// The cap counts DECODED bytes (post-gzip, so a gzip bomb is caught by its
// expansion) through cappedReader, wrapped UNDER the atomic-capability
// sniffer: the sniffer tees its consumed prefix into a replay buffer, so
// wrapping below it counts each wire byte exactly once — during the sniff —
// while the replayed prefix is served from the buffer uncounted.
//
// Overflow yields a deterministic 413 with the surface's structured JSON
// envelope. That determinism needs one twist: receive-pack answers unpack
// failures with a normal report-status on stdout, which would commit the
// response to 200 before the handler learns of the overflow. pushGate
// therefore buffers receive-pack stdout until the request body has been
// fully consumed under the cap, then flushes and streams. Nothing
// user-visible changes on the happy path — receive-pack's meaningful
// output (report-status, sideband hook messages) starts only after the
// pack is fully received; the only pre-consumption output is tiny sideband
// keepalives, which are safe to delay.

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"time"
)

// errPushTooLarge marks a receive-pack body that exceeded the decoded-byte
// cap. It travels from cappedReader through the stdin copy so the handler
// can distinguish overflow from client hangups.
var errPushTooLarge = errors.New("push body exceeds the decoded-byte cap")

// pushTooLargeMsg is the client-facing 413 envelope message. It names the
// operator knob because the default derives from the pack quota and the
// remedy differs per deployment (raise the cap vs split the push), and it
// names both bounds because gzip pushes can trip the compressed-side
// allowance without reaching the decoded cap (Codex round 17).
func pushTooLargeMsg(limit int64) string {
	assertf(limit > 0, "pushTooLargeMsg(%d)", limit)
	return fmt.Sprintf("push body exceeds the server's push-size cap of %d decoded bytes (or its compressed-stream allowance; CODESTORAGE_MAX_PUSH_BYTES)", limit)
}

// wirePushLimit bounds the COMPRESSED stream of a gzip push whose decoded
// cap is limit. The decoded counter alone cannot bound wire consumption:
// concatenated empty gzip members decode to nothing, so a client could
// stream unbounded compressed bytes without ever advancing the decoded
// count (Codex round 6). Slack sizing: stored-mode deflate costs 5 bytes
// per 64 KiB block, and each concatenated member adds ~18 bytes of
// framing, so limit/16 + 4 KiB admits every plausible legitimate stream —
// single-member (what git actually sends) trivially, and multi-member
// streams down to ~300-byte members (Codex round 17) — while holding the
// wire amplification of decode-to-nothing streams to ~6%. Saturating.
func wirePushLimit(limit int64) int64 {
	assertf(limit > 0, "wirePushLimit(%d)", limit)
	slack := limit/16 + 4096
	if limit > math.MaxInt64-slack {
		return math.MaxInt64
	}
	return limit + slack
}

// cappedReader passes through at most limit bytes and fails with
// errPushTooLarge on the first byte past it. exceeded stays observable
// after the consumer (the sniffer or git's stdin copy) has swallowed the
// error.
type cappedReader struct {
	r         io.Reader
	remaining int64
	exceeded  bool
}

func newCappedReader(r io.Reader, limit int64) *cappedReader {
	assertf(r != nil && limit > 0, "newCappedReader(r=%v, limit=%d)", r, limit)
	return &cappedReader{r: r, remaining: limit}
}

func (c *cappedReader) Read(p []byte) (int, error) {
	if c.exceeded {
		return 0, errPushTooLarge
	}
	if c.remaining == 0 {
		// At the cap exactly: probe one byte to distinguish a stream that
		// ends here (fine) from one that continues (overflow). The probed
		// byte is dropped, which is safe — it only exists on the overflow
		// path, where the push is rejected whole.
		var b [1]byte
		if _, err := io.ReadFull(c.r, b[:]); err != nil {
			return 0, err // io.EOF for a clean end
		}
		c.exceeded = true
		return 0, errPushTooLarge
	}
	if int64(len(p)) > c.remaining {
		p = p[:c.remaining]
	}
	n, err := c.r.Read(p)
	assertf(int64(n) <= c.remaining, "cappedReader: underlying reader returned %d > remaining %d", n, c.remaining)
	c.remaining -= int64(n)
	return n, err
}

// pushDrainTimeout bounds the post-copy drains that keep the cap counters
// honest after receive-pack stops reading. The drains only exist for cap
// accounting — git is done by then — so a client that stalls instead of
// finishing its body must not hold the handler, its descriptors, and the
// buffered response indefinitely (Codex round 17): the read deadline turns
// the stall into a drain error, and the push resolves from whatever the
// counters saw. Thirty seconds moves any legitimately in-flight remainder
// (the drains consume at most one cap plus slack).
const pushDrainTimeout = 30 * time.Second

// gateBufferMax bounds pushGate's in-MEMORY pre-verdict buffer.
// receive-pack writes at most sideband keepalives before the request body
// is consumed, so the bound rarely binds; larger pre-verdict output (a
// completed multi-ref push reporting while the client still trickles
// trailing padding) SPILLS TO DISK rather than committing the response —
// flushing early would lock in a 200 before the drain could detect
// overflow, breaking the 413 contract (Codex round 18). Blocking the
// writer instead would deadlock: git stops reading stdin when its stdout
// stalls. Spill volume is bounded by receive-pack's output for one
// cap-bounded request (report-status is one line per command in the body).
const gateBufferMax = 1 << 20

// pushGate is the receive-pack stdout writer that keeps the 413 conversion
// possible: writes are buffered (spilling past gateBufferMax) until
// settle() reports how body consumption ended. settle(false) flushes and
// switches to pass-through; settle(true) discards everything, past and
// future, so the handler can still send the 413 envelope on the untouched
// http.ResponseWriter.
type pushGate struct {
	mu      sync.Mutex
	dst     io.Writer
	buf     bytes.Buffer
	spill   *os.File // unlinked-at-create overflow for buf, nil until needed
	settled bool
	discard bool
	wrote   bool // any byte reached dst (response committed)
}

func newPushGate(dst io.Writer) *pushGate {
	assertf(dst != nil, "newPushGate: nil dst")
	return &pushGate{dst: dst}
}

func (g *pushGate) Write(p []byte) (int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.discard {
		return len(p), nil
	}
	if g.settled {
		return g.writeDstLocked(p)
	}
	// Once spilling starts, everything goes to the spill to preserve
	// order behind the memory buffer's contents.
	if g.spill == nil && g.buf.Len()+len(p) <= gateBufferMax {
		return g.buf.Write(p)
	}
	if g.spill == nil {
		f, err := os.CreateTemp("", "openstorage-push-gate-*")
		if err != nil {
			return 0, fmt.Errorf("push gate spill: %w", err)
		}
		// Unlink immediately: the handle is the only reference, so the
		// bytes vanish with it on every exit path.
		_ = os.Remove(f.Name())
		g.spill = f
	}
	n, err := g.spill.Write(p)
	if err != nil {
		return n, fmt.Errorf("push gate spill: %w", err)
	}
	return n, nil
}

// settle resolves the gate once the request body copy has finished:
// overflowed=true discards all receive-pack output (the handler answers
// 413 instead), overflowed=false flushes the buffered and spilled output
// in order and streams from here on.
func (g *pushGate) settle(overflowed bool) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.settled {
		return nil
	}
	g.settled = true
	spill := g.spill
	g.spill = nil
	if spill != nil {
		defer spill.Close() // unlinked at creation: close is the delete
	}
	if overflowed {
		g.discard = true
		g.buf.Reset()
		return nil
	}
	if err := g.flushLocked(); err != nil {
		return err
	}
	if spill != nil {
		if _, err := spill.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("push gate spill: %w", err)
		}
		g.wrote = true
		if _, err := io.Copy(g.dst, spill); err != nil {
			return err
		}
	}
	return nil
}

// responseStarted reports whether any byte reached the underlying writer —
// after which the response status is committed and a 413 is impossible.
// False until settle by construction: pre-verdict output only ever lands
// in the buffer or the spill file.
func (g *pushGate) responseStarted() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.wrote
}

func (g *pushGate) flushLocked() error {
	if g.buf.Len() == 0 {
		return nil
	}
	_, err := g.writeDstLocked(g.buf.Bytes())
	g.buf.Reset()
	return err
}

func (g *pushGate) writeDstLocked(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	g.wrote = true
	return g.dst.Write(p)
}
