package githttp

// End-to-end acceptance for the fetch-negotiation ingress cap (issue #70):
// gzip-encoded git-upload-pack request bodies are bounded on BOTH sides of
// the decoder by a fixed cap — the decoded stream (a gzip bomb inflates
// past it) and the compressed wire stream (concatenated empty gzip members
// decode to nothing, so only wire-side counting can stop them) — and
// rejected with 413 BEFORE git upload-pack runs. Normal fetches, gzip or
// not, are unaffected.

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// testUploadPackCap pins the fixed maxUploadPackBytes constant (there is
// deliberately no knob); testUploadWireLimit pins its compressed-stream
// allowance. A sizing change must consciously update this test and the
// deploy-production docs.
const testUploadPackCap = 64 << 20

var testUploadWireLimit = wirePushLimit(testUploadPackCap)

// countingReader counts the wire bytes the HTTP transport actually pulled
// from the request body — the evidence trail for the amplification tests.
// The transport reads on its own goroutine, so the counter is atomic.
type countingReader struct {
	r io.Reader
	n atomic.Int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(int64(n))
	return n, err
}

// postUploadPack sends a raw git-upload-pack POST and returns the status
// code and body. body streams as chunked when it is not a *bytes.Reader.
func postUploadPack(t *testing.T, n *testNode, repo, token string, body io.Reader, gzipEncoded bool) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://"+n.addr+"/"+repo+".git/git-upload-pack", body)
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth("t", token)
	req.Header.Set("Content-Type", "application/x-git-upload-pack-request")
	if gzipEncoded {
		req.Header.Set("Content-Encoding", "gzip")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST git-upload-pack: %v", err)
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return resp.StatusCode, string(got)
}

// assertFetchCapEnvelope asserts the issue #70 rejection contract: 413 plus
// the structured JSON envelope naming the negotiation cap.
func assertFetchCapEnvelope(t *testing.T, status int, body string) {
	t.Helper()
	if status != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413\nbody: %s", status, body)
	}
	var envelope struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal([]byte(body), &envelope); err != nil {
		t.Fatalf("413 body is not the structured envelope: %v\n%s", err, body)
	}
	if !strings.Contains(envelope.Error, "negotiation body exceeds") {
		t.Fatalf("413 envelope does not name the negotiation cap: %q", envelope.Error)
	}
}

// emptyGzipMembers builds at least n bytes of concatenated EMPTY gzip
// members: a wire stream that decodes to nothing.
func emptyGzipMembers(t *testing.T, n int) []byte {
	t.Helper()
	var one bytes.Buffer
	zw := gzip.NewWriter(&one)
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	member := one.Bytes()
	return bytes.Repeat(member, n/len(member)+1)
}

// setupFetchRepo registers a repo, pushes one commit, and returns the repo
// key, a git:read-only token, and the pushed head.
func setupFetchRepo(t *testing.T, node *testNode, name string) (repo, readToken, head string) {
	t.Helper()
	repo = registerRepo(t, name)
	work := initWorkRepo(t)
	writeToken := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(node, repo, writeToken), "HEAD:refs/heads/main")
	return repo, mintTestToken(t, repo, "git:read", ""), revParse(t, work, "HEAD")
}

// TestUploadPackCap_EmptyMemberFlood is the wire-amplification acceptance:
// a git:read token streams concatenated empty gzip members into a fetch
// negotiation. The decoded output stays ~0 forever, so only the
// compressed-side wire counter can stop it — the server must answer 413
// after at most the wire allowance, never consume the whole flood.
func TestUploadPackCap_EmptyMemberFlood(t *testing.T) {
	node := startNodeT(t)
	repo, token, _ := setupFetchRepo(t, node, "t-fetchcap-flood")

	// Comfortably past the wire allowance: a capless server consumes all
	// of it (the red-first evidence), a capped one must stop ~68 MiB in.
	flood := emptyGzipMembers(t, int(testUploadWireLimit)+8<<20)
	counter := &countingReader{r: bytes.NewReader(flood)}
	status, body := postUploadPack(t, node, repo, token, io.MultiReader(counter), true)
	t.Logf("wire bytes streamed before response: %d (wire allowance %d, flood %d)",
		counter.n.Load(), testUploadWireLimit, len(flood))
	assertFetchCapEnvelope(t, status, body)
	if streamed := counter.n.Load(); streamed > testUploadWireLimit+4<<20 {
		t.Fatalf("server consumed %d wire bytes, want <= allowance %d (+ transport slack)",
			streamed, testUploadWireLimit)
	}

	// Clean server state: the same node still serves a healthy clone.
	cloneDir := filepath.Join(t.TempDir(), "clone")
	mustGit(t, "", "clone", remoteURL(node, repo, token), cloneDir)
	if out, err := tryGit(cloneDir, "fsck", "--strict"); err != nil {
		t.Fatalf("fsck after flood: %v\n%s", err, out)
	}
}

// TestUploadPackCap_GzipBomb proves the DECODED side: a compressed body far
// under the wire allowance that inflates past the negotiation cap must be
// rejected by the post-gzip counter.
func TestUploadPackCap_GzipBomb(t *testing.T) {
	node := startNodeT(t)
	repo, token, _ := setupFetchRepo(t, node, "t-fetchcap-bomb")

	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	if _, err := zw.Write(make([]byte, testUploadPackCap+1<<20)); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("gzip bomb: %d bytes compressed, %d decoded", compressed.Len(), testUploadPackCap+1<<20)
	// The compressed body must sit far below the wire allowance: a server
	// counting only wire bytes would accept it, so a 413 here proves the
	// decoded counter runs.
	if int64(compressed.Len()) >= testUploadWireLimit {
		t.Fatalf("bomb did not compress below the wire allowance (%d bytes)", compressed.Len())
	}
	status, body := postUploadPack(t, node, repo, token, bytes.NewReader(compressed.Bytes()), true)
	assertFetchCapEnvelope(t, status, body)
}

// TestUploadPackCap_DeclaredOversize pins the Content-Length fast path: a
// gzip negotiation body declaring more than the wire allowance is rejected
// before reading a single byte.
func TestUploadPackCap_DeclaredOversize(t *testing.T) {
	node := startNodeT(t)
	repo, token, _ := setupFetchRepo(t, node, "t-fetchcap-declared")

	// *bytes.Reader ⇒ Content-Length set ⇒ rejected on the declaration.
	counter := &countingReader{r: bytes.NewReader(make([]byte, int(testUploadWireLimit)+1))}
	req, err := http.NewRequest(http.MethodPost, "http://"+node.addr+"/"+repo+".git/git-upload-pack", counter)
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = testUploadWireLimit + 1
	req.SetBasicAuth("t", token)
	req.Header.Set("Content-Type", "application/x-git-upload-pack-request")
	req.Header.Set("Content-Encoding", "gzip")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST git-upload-pack: %v", err)
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	t.Logf("wire bytes streamed before response: %d", counter.n.Load())
	assertFetchCapEnvelope(t, resp.StatusCode, string(got))
	if streamed := counter.n.Load(); streamed > 1<<20 {
		t.Fatalf("declared oversize streamed %d bytes, want none beyond transport buffering", streamed)
	}
}

// TestUploadPackCap_PaddedNegotiationRejected pins the trailing-junk shape:
// a complete valid gzip member followed by non-gzip padding must be
// rejected (bad member header ⇒ 400), never forwarded to git as if the
// negotiation were clean.
func TestUploadPackCap_PaddedNegotiationRejected(t *testing.T) {
	node := startNodeT(t)
	repo, token, head := setupFetchRepo(t, node, "t-fetchcap-padded")

	var member bytes.Buffer
	zw := gzip.NewWriter(&member)
	if _, err := zw.Write([]byte(pktLine(fmt.Sprintf("want %s\n", head)) + "0000" + pktLine("done\n"))); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	body := append(member.Bytes(), bytes.Repeat([]byte{0xde, 0xad}, 2048)...)
	status, respBody := postUploadPack(t, node, repo, token, io.MultiReader(bytes.NewReader(body)), true)
	if status != http.StatusBadRequest {
		t.Fatalf("padded negotiation status = %d, want 400\nbody: %s", status, respBody)
	}
}

// TestUploadPackCap_ValidNegotiationUnaffected is the no-regression side: a
// real protocol-v0 fetch negotiation — identity and gzip encoded — still
// answers 200 with the pack.
func TestUploadPackCap_ValidNegotiationUnaffected(t *testing.T) {
	node := startNodeT(t)
	repo, token, head := setupFetchRepo(t, node, "t-fetchcap-ok")

	request := []byte(pktLine(fmt.Sprintf("want %s\n", head)) + "0000" + pktLine("done\n"))

	t.Run("identity", func(t *testing.T) {
		status, body := postUploadPack(t, node, repo, token, bytes.NewReader(request), false)
		assertFetchResponse(t, status, body)
	})

	t.Run("gzip", func(t *testing.T) {
		var compressed bytes.Buffer
		zw := gzip.NewWriter(&compressed)
		if _, err := zw.Write(request); err != nil {
			t.Fatal(err)
		}
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
		status, body := postUploadPack(t, node, repo, token, bytes.NewReader(compressed.Bytes()), true)
		assertFetchResponse(t, status, body)
	})
}

// assertFetchResponse asserts a successful v0 fetch: 200, NAK, and a pack.
func assertFetchResponse(t *testing.T, status int, body string) {
	t.Helper()
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200\nbody: %.200s", status, body)
	}
	if !strings.Contains(body, "NAK") || !strings.Contains(body, "PACK") {
		t.Fatalf("fetch response missing NAK/PACK: %.200s", body)
	}
}
