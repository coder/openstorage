package api

// Container-free unit tests for the envelope helper, middleware, and input
// validation. The full status-code matrix runs against real containers in
// api_integration_test.go.

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/ref"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestUsageFloorPrefersInjectedHook (Codex round 21): when the serve
// command wires Config.UsageFloor (the git server's shared single-flight
// cached listing), REST pack admission must use it instead of re-listing
// the repo's pack and LFS prefixes per write. A nil blob store proves no
// listing happened (blob.StorageUsageBytes asserts on nil).
func TestUsageFloorPrefersInjectedHook(t *testing.T) {
	var gotRepo string
	s := &server{usageFloorFn: func(_ context.Context, repo string) (int64, int64, error) {
		gotRepo = repo
		return 4321, 7, nil
	}}
	floor, gen, err := s.usageFloor(context.Background(), "repo_x")
	if err != nil || floor != 4321 || gen != 7 || gotRepo != "repo_x" {
		t.Fatalf("usageFloor = (%d, %v) for %q, want (4321, nil) for repo_x", floor, err, gotRepo)
	}
}

// cancelingUploadBlob cancels the request context mid-upload and fails —
// the aborted REST pack write.
type cancelingUploadBlob struct {
	blob.BlobStore
	cancel context.CancelFunc
}

func (b cancelingUploadBlob) UploadFile(ctx context.Context, _, _ string) error {
	b.cancel()
	return ctx.Err()
}

func (cancelingUploadBlob) Stat(context.Context, string) (int64, error) {
	return 0, blob.ErrNotFound
}

// TestPublishPack_CanceledUploadStillRetiresReservation (Codex round 31):
// the clean-failure retirement must run on a context DETACHED from the
// request — the most common clean failure is a client cancellation, and a
// retire on the already-canceled context can never reach the database, so
// repeated canceled writes with distinct keys would pin
// maxActiveReservations rows for the full TTL.
func TestPublishPack_CanceledUploadStillRetiresReservation(t *testing.T) {
	st, err := ref.Open(context.Background(), "sqlite://"+t.TempDir()+"/refs.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.InitSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	packPath := filepath.Join(t.TempDir(), "p.pack")
	if err := os.WriteFile(packPath, []byte("tiny pack bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	s := &server{
		refStore:           st,
		blobStore:          cancelingUploadBlob{cancel: cancel},
		logger:             discardLogger(),
		packQuotaBytes:     1 << 20,
		packReservationTTL: time.Hour,
		usageFloorFn:       func(context.Context, string) (int64, int64, error) { return 0, ref.ReserveNoFloorGen, nil },
	}
	if err := s.publishPack(ctx, "repo_c", "abcd", packPath); err == nil {
		t.Fatal("canceled publishPack reported success")
	}
	ls, err := st.QuotaLedgerState(context.Background(), blob.QuotaRepoGroup("repo_c"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if ls.ReservedBytes != 0 {
		t.Fatalf("canceled write left %d reserved bytes pinned to the TTL, want 0 (attempt retired)", ls.ReservedBytes)
	}
}

func decodeEnvelope(t *testing.T, rr *httptest.ResponseRecorder) string {
	t.Helper()
	if got := rr.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	var env errorEnvelope
	if err := json.Unmarshal(rr.Body.Bytes(), &env); err != nil {
		t.Fatalf("error body %q is not an {\"error\"} envelope: %v", rr.Body.String(), err)
	}
	if env.Error == "" {
		t.Fatalf("envelope has empty error message: %s", rr.Body.String())
	}
	assertProblemFields(t, rr.Code, rr.Body.Bytes())
	return env.Error
}

func TestWriteErrEnvelope(t *testing.T) {
	rr := httptest.NewRecorder()
	writeErr(rr, http.StatusNotFound, "repository not found")
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rr.Code)
	}
	if msg := decodeEnvelope(t, rr); msg != "repository not found" {
		t.Fatalf("error = %q", msg)
	}
	if rr.Header().Get("WWW-Authenticate") != "" {
		t.Fatal("non-401 response must not set WWW-Authenticate")
	}
}

func TestWriteErr401SetsWWWAuthenticate(t *testing.T) {
	rr := httptest.NewRecorder()
	writeErr(rr, http.StatusUnauthorized, "invalid token")
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rr.Code)
	}
	if got := rr.Header().Get("WWW-Authenticate"); got != "Bearer" {
		t.Fatalf("WWW-Authenticate = %q, want Bearer", got)
	}
	decodeEnvelope(t, rr)
}

func TestRecoverPanicsWritesEnvelope(t *testing.T) {
	h := recoverPanics(discardLogger())(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/v1/repo", nil))
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rr.Code)
	}
	if msg := decodeEnvelope(t, rr); msg != "internal server error" {
		t.Fatalf("error = %q", msg)
	}
}

func TestRecoverPanicsRethrowsErrAbortHandler(t *testing.T) {
	h := recoverPanics(discardLogger())(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(http.ErrAbortHandler)
	}))
	defer func() {
		if recover() != http.ErrAbortHandler {
			t.Fatal("http.ErrAbortHandler was swallowed instead of re-panicked")
		}
	}()
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
}

func TestLogRequestsPassesThroughStatus(t *testing.T) {
	h := logRequests(discardLogger())(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusTeapot {
		t.Fatalf("status = %d, want 418", rr.Code)
	}
}

func TestBearerToken(t *testing.T) {
	cases := []struct {
		header    string
		wantToken string
		wantOK    bool
	}{
		{"", "", false},
		{"Bearer", "", false},
		{"Bearer ", "", false},
		{"Basic dXNlcjpwYXNz", "", false},
		{"Bearer abc.def.ghi", "abc.def.ghi", true},
		{"bearer abc.def.ghi", "abc.def.ghi", true}, // scheme is case-insensitive
	}
	for _, tc := range cases {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		if tc.header != "" {
			r.Header.Set("Authorization", tc.header)
		}
		token, ok := bearerToken(r)
		if token != tc.wantToken || ok != tc.wantOK {
			t.Errorf("bearerToken(%q) = (%q, %v), want (%q, %v)",
				tc.header, token, ok, tc.wantToken, tc.wantOK)
		}
	}
}

func TestValidBranchName(t *testing.T) {
	valid := []string{"main", "develop", "release-1.0", "feature/foo", "a/b/c", "v1.2.3_rc1"}
	invalid := []string{
		"", "-x", ".hidden", "a..b", "a b", "feat/", "/lead", "a//b",
		"branch.lock", "refs/heads/x.lock", "über", strings.Repeat("a", 256),
	}
	for _, name := range valid {
		if !validBranchName(name) {
			t.Errorf("validBranchName(%q) = false, want true", name)
		}
	}
	for _, name := range invalid {
		if validBranchName(name) {
			t.Errorf("validBranchName(%q) = true, want false", name)
		}
	}
}
