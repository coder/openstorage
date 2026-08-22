package githttp

// Unit tests for the LFS Batch API (docs/05): auth mapping (Basic and
// Bearer), batch validation, the proxy transfer round-trip over the
// fileblob backend (no presign support), and the presigned-href path via a
// stub store. Reuses the server_test.go auth scaffolding.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/code-storage/internal/auth"
	"github.com/coder/code-storage/internal/blob"
	"github.com/coder/code-storage/internal/meta"
)

const (
	lfsTestRepoURL = "myrepo"
	lfsTestRepoID  = "repo_0123456789ab"
)

// newLFSServer wires a Server around bl with one resolvable repo
// (lfsTestRepoURL / lfsTestRepoID) and returns a token minter for it.
func newLFSServer(t *testing.T, bl blob.BlobStore) (*Server, func(scopes ...string) string) {
	t.Helper()
	key, pem := genKeyPEM(t, "ES256")
	verifier := auth.NewVerifier(memKeys{"org1": {pem}})
	srv := &Server{
		Blob:     bl,
		Verifier: verifier,
		Org:      "org1",
		ResolveRepo: func(_ context.Context, base string) (meta.Repo, bool) {
			if base == lfsTestRepoURL || base == lfsTestRepoID {
				return meta.Repo{RepoID: lfsTestRepoID, URL: lfsTestRepoURL}, true
			}
			return meta.Repo{}, false
		},
	}
	mint := func(scopes ...string) string {
		return mintUnitToken(t, key, "ES256", unitClaims("org1", lfsTestRepoURL, scopes...))
	}
	return srv, mint
}

func openFileBlob(t *testing.T) blob.BlobStore {
	t.Helper()
	bl, err := blob.OpenFile("file://" + t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { bl.Close() })
	return bl
}

// doLFS performs one request against the server's Handler. auth is either
// "" (no credentials), "basic:<token>" or "bearer:<token>".
func doLFS(t *testing.T, srv *Server, method, path, authSpec, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if tok, ok := strings.CutPrefix(authSpec, "basic:"); ok {
		req.SetBasicAuth("t", tok)
	} else if tok, ok := strings.CutPrefix(authSpec, "bearer:"); ok {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	return w
}

func batchBody(op, oid string, size int64) string {
	return fmt.Sprintf(`{"operation":%q,"transfers":["basic"],"objects":[{"oid":%q,"size":%d}]}`, op, oid, size)
}

func decodeBatch(t *testing.T, w *httptest.ResponseRecorder) lfsBatchResponse {
	t.Helper()
	if w.Code != http.StatusOK {
		t.Fatalf("batch status = %d, body %s", w.Code, w.Body)
	}
	if ct := w.Header().Get("Content-Type"); ct != lfsMediaType {
		t.Fatalf("batch Content-Type = %q, want %q", ct, lfsMediaType)
	}
	var resp lfsBatchResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode batch response: %v\n%s", err, w.Body)
	}
	return resp
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

const lfsBatchPath = "/" + lfsTestRepoURL + ".git/info/lfs/objects/batch"

// TestLFS_ProxyRoundTrip drives the full local-mode flow over fileblob:
// batch upload → proxy PUT → batch upload again (no actions) → batch
// download → proxy GET returns the original bytes.
func TestLFS_ProxyRoundTrip(t *testing.T) {
	srv, mint := newLFSServer(t, openFileBlob(t))
	writeTok, readTok := mint(auth.ScopeGitWrite), mint(auth.ScopeGitRead)
	content := []byte("hello lfs proxy round-trip")
	oid := sha256Hex(content)

	// 1. Batch upload: missing object gets a proxy upload action.
	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+writeTok,
		batchBody("upload", oid, int64(len(content)))))
	if len(resp.Objects) != 1 || resp.Objects[0].Error != nil {
		t.Fatalf("upload batch objects = %+v", resp.Objects)
	}
	up := resp.Objects[0].Actions["upload"]
	if up == nil {
		t.Fatalf("no upload action: %+v", resp.Objects[0])
	}
	wantHref := "http://example.com/" + lfsTestRepoURL + ".git/info/lfs/objects/" + oid
	if up.Href != wantHref {
		t.Fatalf("upload href = %q, want %q", up.Href, wantHref)
	}
	if !strings.HasPrefix(up.Header["Authorization"], "Basic ") {
		t.Fatalf("upload action must echo the Authorization header, got %v", up.Header)
	}

	// 2. Proxy PUT stores the bytes.
	objPath := "/" + lfsTestRepoURL + ".git/info/lfs/objects/" + oid
	if w := doLFS(t, srv, http.MethodPut, objPath, "basic:"+writeTok, string(content)); w.Code != http.StatusOK {
		t.Fatalf("proxy PUT status = %d, body %s", w.Code, w.Body)
	}

	// 3. Batch upload again: object present → no actions (client skips).
	resp = decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+writeTok,
		batchBody("upload", oid, int64(len(content)))))
	if o := resp.Objects[0]; o.Error != nil || len(o.Actions) != 0 {
		t.Fatalf("re-upload batch object = %+v, want no actions and no error", o)
	}

	// 4. Batch download advertises the proxy href; GET returns the bytes.
	resp = decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "bearer:"+readTok,
		batchBody("download", oid, int64(len(content)))))
	down := resp.Objects[0].Actions["download"]
	if down == nil || down.Href != wantHref {
		t.Fatalf("download action = %+v, want href %q", resp.Objects[0], wantHref)
	}
	if resp.Objects[0].Size != int64(len(content)) {
		t.Fatalf("download size = %d, want %d", resp.Objects[0].Size, len(content))
	}
	w := doLFS(t, srv, http.MethodGet, objPath, "basic:"+readTok, "")
	if w.Code != http.StatusOK || w.Body.String() != string(content) {
		t.Fatalf("proxy GET status = %d, body %q", w.Code, w.Body)
	}
	if cl := w.Header().Get("Content-Length"); cl != fmt.Sprint(len(content)) {
		t.Fatalf("proxy GET Content-Length = %q, want %d", cl, len(content))
	}
}

func TestLFS_BatchDownloadMissingObject(t *testing.T) {
	srv, mint := newLFSServer(t, openFileBlob(t))
	oid := sha256Hex([]byte("never uploaded"))
	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+mint(auth.ScopeGitRead),
		batchBody("download", oid, 14)))
	o := resp.Objects[0]
	if o.Error == nil || o.Error.Code != http.StatusNotFound || len(o.Actions) != 0 {
		t.Fatalf("missing-object result = %+v, want per-object 404", o)
	}
}

func TestLFS_AuthMapping(t *testing.T) {
	srv, mint := newLFSServer(t, openFileBlob(t))
	oid := sha256Hex([]byte("auth"))
	readTok := mint(auth.ScopeGitRead)
	objPath := "/" + lfsTestRepoURL + ".git/info/lfs/objects/" + oid

	cases := []struct {
		name   string
		method string
		path   string
		auth   string
		body   string
		want   int
	}{
		{"batch no credentials", http.MethodPost, lfsBatchPath, "", batchBody("download", oid, 4), http.StatusUnauthorized},
		{"batch bad token", http.MethodPost, lfsBatchPath, "bearer:garbage", batchBody("download", oid, 4), http.StatusUnauthorized},
		{"download scope ok via bearer", http.MethodPost, lfsBatchPath, "bearer:" + readTok, batchBody("download", oid, 4), http.StatusOK},
		{"upload needs git:write", http.MethodPost, lfsBatchPath, "basic:" + readTok, batchBody("upload", oid, 4), http.StatusForbidden},
		{"bad operation", http.MethodPost, lfsBatchPath, "basic:" + readTok, batchBody("verify", oid, 4), http.StatusBadRequest},
		{"proxy GET no credentials", http.MethodGet, objPath, "", "", http.StatusUnauthorized},
		{"proxy PUT needs git:write", http.MethodPut, objPath, "basic:" + readTok, "auth", http.StatusForbidden},
		{"unknown LFS subpath", http.MethodPost, "/" + lfsTestRepoURL + ".git/info/lfs/locks/verify", "basic:" + readTok, "{}", http.StatusNotFound},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := doLFS(t, srv, c.method, c.path, c.auth, c.body)
			if w.Code != c.want {
				t.Fatalf("status = %d, want %d (body %s)", w.Code, c.want, w.Body)
			}
			if c.want == http.StatusUnauthorized && c.path == lfsBatchPath {
				if w.Header().Get("LFS-Authenticate") == "" {
					t.Fatal("401 batch response missing LFS-Authenticate header")
				}
			}
		})
	}
}

func TestLFS_BatchValidation(t *testing.T) {
	srv, mint := newLFSServer(t, openFileBlob(t))
	tok := "basic:" + mint(auth.ScopeGitRead, auth.ScopeGitWrite)
	goodOID := sha256Hex([]byte("x"))

	// Top-level rejections.
	if w := doLFS(t, srv, http.MethodPost, lfsBatchPath, tok,
		`{"operation":"download","transfers":["tus"],"objects":[]}`); w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("non-basic transfers: status = %d, body %s", w.Code, w.Body)
	}
	if w := doLFS(t, srv, http.MethodPost, lfsBatchPath, tok,
		`{"operation":"download","hash_algo":"sha512","objects":[]}`); w.Code != http.StatusConflict {
		t.Fatalf("unsupported hash_algo: status = %d, body %s", w.Code, w.Body)
	}
	if w := doLFS(t, srv, http.MethodPost, lfsBatchPath, tok, `not json`); w.Code != http.StatusBadRequest {
		t.Fatalf("bad json: status = %d, body %s", w.Code, w.Body)
	}

	// Per-object rejections keep the batch at 200.
	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, tok, fmt.Sprintf(
		`{"operation":"upload","objects":[{"oid":"NOT-HEX","size":1},{"oid":%q,"size":-1}]}`, goodOID)))
	for i, o := range resp.Objects {
		if o.Error == nil || o.Error.Code != http.StatusUnprocessableEntity {
			t.Fatalf("object %d = %+v, want per-object 422", i, o)
		}
	}
}

func TestLFS_ProxyUploadHashMismatch(t *testing.T) {
	srv, mint := newLFSServer(t, openFileBlob(t))
	oid := sha256Hex([]byte("expected content"))
	objPath := "/" + lfsTestRepoURL + ".git/info/lfs/objects/" + oid

	w := doLFS(t, srv, http.MethodPut, objPath, "basic:"+mint(auth.ScopeGitWrite), "different content")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("hash-mismatch PUT status = %d, body %s", w.Code, w.Body)
	}
	// The corrupt body must not have been published under the oid key.
	if _, err := srv.Blob.Stat(context.Background(), blob.LFSKey(lfsTestRepoID, oid)); err == nil {
		t.Fatal("hash-mismatched upload was stored")
	}
}

// TestLFS_NamespaceSharesObjects pins the docs/05 keying: `+ephemeral`
// paths resolve to the same base-RepoID LFS space, and proxy hrefs preserve
// the namespaced path element.
func TestLFS_NamespaceSharesObjects(t *testing.T) {
	srv, mint := newLFSServer(t, openFileBlob(t))
	tok := mint(auth.ScopeGitRead, auth.ScopeGitWrite)
	content := []byte("shared across namespaces")
	oid := sha256Hex(content)

	ephPath := "/" + lfsTestRepoURL + "+ephemeral.git/info/lfs/objects/"
	if w := doLFS(t, srv, http.MethodPut, ephPath+oid, "basic:"+tok, string(content)); w.Code != http.StatusOK {
		t.Fatalf("ephemeral PUT status = %d, body %s", w.Code, w.Body)
	}
	// Visible from the persistent namespace (same LFSKey).
	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+tok,
		batchBody("download", oid, int64(len(content)))))
	if o := resp.Objects[0]; o.Error != nil || o.Actions["download"] == nil {
		t.Fatalf("persistent download after ephemeral upload = %+v", o)
	}
	// Ephemeral batch hrefs keep the namespaced element.
	resp = decodeBatch(t, doLFS(t, srv, http.MethodPost,
		"/"+lfsTestRepoURL+"+ephemeral.git/info/lfs/objects/batch", "basic:"+tok,
		batchBody("download", oid, int64(len(content)))))
	if href := resp.Objects[0].Actions["download"].Href; !strings.Contains(href, lfsTestRepoURL+"+ephemeral.git") {
		t.Fatalf("ephemeral href = %q, want namespaced path element", href)
	}
}

// signedBlob wraps a BlobStore, overriding SignedURL to simulate a
// presign-capable backend (S3) without containers.
type signedBlob struct {
	blob.BlobStore
}

func (s signedBlob) SignedURL(_ context.Context, key, method string, expiry time.Duration) (string, error) {
	return "https://signed.example/" + method + "/" + key, nil
}

func TestLFS_BatchPresignedHref(t *testing.T) {
	srv, mint := newLFSServer(t, signedBlob{openFileBlob(t)})
	oid := sha256Hex([]byte("presigned"))
	resp := decodeBatch(t, doLFS(t, srv, http.MethodPost, lfsBatchPath, "basic:"+mint(auth.ScopeGitWrite),
		batchBody("upload", oid, 9)))
	up := resp.Objects[0].Actions["upload"]
	if up == nil {
		t.Fatalf("no upload action: %+v", resp.Objects[0])
	}
	wantHref := "https://signed.example/PUT/" + blob.LFSKey(lfsTestRepoID, oid)
	if up.Href != wantHref {
		t.Fatalf("presigned href = %q, want %q", up.Href, wantHref)
	}
	if up.ExpiresIn != int(lfsURLExpiry.Seconds()) {
		t.Fatalf("expires_in = %d, want %d", up.ExpiresIn, int(lfsURLExpiry.Seconds()))
	}
	if len(up.Header) != 0 {
		t.Fatalf("presigned action must not echo Authorization, got %v", up.Header)
	}
}
