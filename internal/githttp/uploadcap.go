package githttp

// Fetch-negotiation ingress cap for git upload-pack (issue #70). The #55
// push caps (pushcap.go) deliberately scoped to receive-pack, which left
// handleServiceRPC decoding `Content-Encoding: gzip` upload-pack bodies
// without any bound: a git:read token could stream concatenated empty gzip
// members (each ~20 wire bytes, decoding to nothing) forever while the
// connection stayed pinned. The same decode-to-nothing vector #62 closed
// on the push path.
//
// The cap reuses #62's cappedReader on BOTH sides of the decoder — the
// compressed wire stream (empty members never advance the decoded counter,
// so a decoded-only cap would never trip) and the decoded stream (which a
// gzip bomb inflates past the wire limit). The full request is read under a
// deadline BEFORE hydrate or git upload-pack runs, so overflow can still
// produce a deterministic structured 413. A modest fixed in-memory bound
// avoids both an operator knob and replica-global temp-disk pressure from
// concurrent stalled negotiations; only the RESPONSE carries a large pack.

import (
	"fmt"
	"time"
)

// maxUploadPackBytes caps the DECODED bytes one gzip-encoded upload-pack
// negotiation request may carry; wirePushLimit(maxUploadPackBytes) bounds
// its compressed stream. Four MiB allows roughly 80,000 want/have pkt-lines
// — generous for negotiation metadata, which remains low-MB even for huge
// fetches — while keeping aggregate per-request memory modest. Fixed by
// design: this ends unbounded decode work, it is not a tenant quota.
const maxUploadPackBytes = 4 << 20

// uploadPackReadTimeout bounds an under-cap client that stalls between gzip
// members or before EOF. It covers only request-body ingestion and is cleared
// before hydrate/git so normal large pack responses are unaffected.
const defaultUploadPackReadTimeout = 30 * time.Second

// uploadPackTooLargeMsg is the client-facing 413 envelope message. Unlike
// pushTooLargeMsg there is no knob to name.
func uploadPackTooLargeMsg() string {
	return fmt.Sprintf("fetch negotiation body exceeds the server's %d-byte cap (or its compressed-stream allowance)", maxUploadPackBytes)
}
