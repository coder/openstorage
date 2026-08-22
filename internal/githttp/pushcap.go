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
	"sync"
)

// errPushTooLarge marks a receive-pack body that exceeded the decoded-byte
// cap. It travels from cappedReader through the stdin copy so the handler
// can distinguish overflow from client hangups.
var errPushTooLarge = errors.New("push body exceeds the decoded-byte cap")

// pushTooLargeMsg is the client-facing 413 envelope message. It names the
// operator knob because the default derives from the pack quota and the
// remedy differs per deployment (raise the cap vs split the push).
func pushTooLargeMsg(limit int64) string {
	assertf(limit > 0, "pushTooLargeMsg(%d)", limit)
	return fmt.Sprintf("push body exceeds the server's decoded push-size cap of %d bytes (CODESTORAGE_MAX_PUSH_BYTES)", limit)
}

// wirePushLimit bounds the COMPRESSED stream of a gzip push whose decoded
// cap is limit. The decoded counter alone cannot bound wire consumption:
// concatenated empty gzip members decode to nothing, so a client could
// stream unbounded compressed bytes without ever advancing the decoded
// count (Codex round 6). A legitimate gzip body is at most marginally
// larger than its decoded form — stored-mode deflate costs 5 bytes per
// 64 KiB block plus constant member framing — so limit plus ~1.6% plus a
// constant never rejects anything the decoded cap would admit. Saturating.
func wirePushLimit(limit int64) int64 {
	assertf(limit > 0, "wirePushLimit(%d)", limit)
	slack := limit/64 + 1024
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

// gateBufferMax bounds pushGate's pre-consumption buffer. receive-pack
// writes at most sideband keepalives before the request body is consumed,
// so the bound should never bind; if some git version streams real output
// early, the gate opens and streams it (giving up the 413 conversion for
// that request) rather than buffering without bound.
const gateBufferMax = 1 << 20

// pushGate is the receive-pack stdout writer that keeps the 413 conversion
// possible: writes are buffered until settle() reports how body consumption
// ended. settle(false) flushes and switches to pass-through; settle(true)
// discards everything, past and future, so the handler can still send the
// 413 envelope on the untouched http.ResponseWriter.
type pushGate struct {
	mu      sync.Mutex
	dst     io.Writer
	buf     bytes.Buffer
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
	if !g.settled && g.buf.Len()+len(p) > gateBufferMax {
		// Unexpectedly large early output: stream rather than grow. See
		// gateBufferMax.
		if err := g.flushLocked(); err != nil {
			return 0, err
		}
		g.settled = true
	}
	if !g.settled {
		return g.buf.Write(p)
	}
	return g.writeDstLocked(p)
}

// settle resolves the gate once the request body copy has finished:
// overflowed=true discards all receive-pack output (the handler answers
// 413 instead), overflowed=false flushes the buffer and streams from here
// on. Calling settle on a gate that already forced itself open is a no-op
// for false and impossible-by-construction for true (the handler checks
// responseStarted before converting to 413).
func (g *pushGate) settle(overflowed bool) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if overflowed {
		g.discard = true
		g.buf.Reset()
		g.settled = true
		return nil
	}
	if g.settled {
		return nil
	}
	g.settled = true
	return g.flushLocked()
}

// responseStarted reports whether any byte reached the underlying writer —
// after which the response status is committed and a 413 is impossible.
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
