package githttp

// Fetch-negotiation ingress cap for git upload-pack (issue #70). The #55
// push caps (pushcap.go) deliberately scoped to receive-pack, which left
// handleServiceRPC decoding `Content-Encoding: gzip` upload-pack bodies
// without any bound: a git:read token — the most widely distributed token
// class — could stream concatenated empty gzip members (each ~20 wire
// bytes, decoding to nothing) into a clone/fetch negotiation forever while
// an idle upload-pack process, its goroutines, and the connection were all
// pinned. The same decode-to-nothing vector #62 closed on the push path.
//
// The cap reuses #62's cappedReader on BOTH sides of the decoder — the
// compressed wire stream (empty members never advance the decoded counter,
// so a decoded-only cap would never trip) and the decoded stream (which a
// gzip bomb inflates past the wire limit). Unlike the push path there is
// no operator knob: negotiation requests are small (want/have/shallow
// lines — low MBs even for enormous fetches; only the RESPONSE is large),
// so a fixed generous constant suffices.
//
// Rejection is a deterministic 413 with the structured JSON envelope,
// decided BEFORE hydrate or git upload-pack ever run: the decoded body is
// spooled through the caps into a small memory buffer (spilling past
// uploadPackMemBufferMax to an unlinked temp file, so a flood of
// concurrent capped requests costs disk, not RAM) and git only starts once
// the whole negotiation has arrived under both bounds. Buffering is safe
// here because stateless-RPC upload-pack consumes its entire request
// before responding — and it keeps receive-pack's pushGate machinery out
// of the read path, where buffering the response (a whole clone's pack)
// would be unacceptable.

import (
	"bytes"
	"fmt"
	"io"
	"os"
)

// maxUploadPackBytes caps the DECODED bytes one gzip-encoded upload-pack
// negotiation request may carry; wirePushLimit(maxUploadPackBytes) bounds
// its compressed stream. 64 MiB is ~1.3M want/have pkt-lines — orders of
// magnitude past real negotiations (which reach low MBs only for mirror
// fetches of huge-ref-count repos) while holding the per-request wire
// amplification of a decode-to-nothing stream to ~68 MiB. Fixed by design:
// the bound exists to end unbounded streams, not to meter fetch sizes, so
// there is nothing for an operator to tune (uploadcap_e2e_test.go pins the
// value; resizing must update it and the deploy-production doc).
const maxUploadPackBytes = 64 << 20

// uploadPackMemBufferMax bounds the in-memory tier of the negotiation
// spool; real negotiation bodies (KBs) never spill.
const uploadPackMemBufferMax = 1 << 20

// uploadPackTooLargeMsg is the client-facing 413 envelope message. Unlike
// pushTooLargeMsg there is no knob to name.
func uploadPackTooLargeMsg() string {
	return fmt.Sprintf("fetch negotiation body exceeds the server's %d-byte cap (or its compressed-stream allowance)", maxUploadPackBytes)
}

// bufferedBody is a fully spooled request body: mem first, then the spill
// file. Close releases the spill (unlinked at creation, so close is the
// delete).
type bufferedBody struct {
	io.Reader
	spill *os.File
}

func (b *bufferedBody) Close() error {
	if b.spill == nil {
		return nil
	}
	return b.spill.Close()
}

// bufferUploadPackBody spools r (the capped, decoded negotiation stream)
// to completion. Any read error — cap overflow, gzip corruption, client
// hangup — aborts the spool; the caller distinguishes overflow via the cap
// counters, not the returned error.
func bufferUploadPackBody(r io.Reader) (*bufferedBody, error) {
	assertf(r != nil, "bufferUploadPackBody: nil reader")
	var mem bytes.Buffer
	if _, err := io.CopyN(&mem, r, uploadPackMemBufferMax); err != nil {
		if err == io.EOF {
			return &bufferedBody{Reader: &mem}, nil
		}
		return nil, err
	}
	// More than the memory tier: spill the rest to an unlinked temp file.
	f, err := os.CreateTemp("", "codestorage-upload-pack-*")
	if err != nil {
		return nil, fmt.Errorf("upload-pack spool: %w", err)
	}
	// Unlink immediately: the handle is the only reference, so the bytes
	// vanish with it on every exit path.
	_ = os.Remove(f.Name())
	if _, err := io.Copy(f, r); err != nil {
		_ = f.Close()
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("upload-pack spool: %w", err)
	}
	return &bufferedBody{Reader: io.MultiReader(&mem, f), spill: f}, nil
}
