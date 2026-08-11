package api

// Container-free unit tests for the envelope helper, middleware, and input
// validation. The full status-code matrix runs against real containers in
// api_integration_test.go.

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
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
