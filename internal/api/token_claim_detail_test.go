package api

import (
	"bytes"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/coder/openstorage/internal/auth"
)

// tcdRoute is a route that checks the ref policies of the token before it
// does anything else, with a request that reaches the check.
type tcdRoute struct{ name, method, path, body string }

func tcdRoutes() []tcdRoute {
	return []tcdRoute{
		{"create", http.MethodPost, "/api/v1/repos", `{}`},
		{"create, canonical path", http.MethodPost, "/api/repos", `{}`},
		{"copy", http.MethodPost, "/api/v1/repos", `{"base_repo":{"name":"src","auth":{"token":"x"}}}`},
		{"update", http.MethodPatch, "/api/v1/repo", `{"default_branch":"main"}`},
		{"delete", http.MethodDelete, "/api/v1/repos/delete", ``},
		{"detach base", http.MethodDelete, "/api/v1/repos/base", ``},
		{"pull-upstream", http.MethodPost, "/api/v1/repos/pull-upstream", `{}`},
		{"push-upstream", http.MethodPost, "/api/v1/repos/push-upstream", `{}`},
	}
}

// tcdRequireFixed fails unless the body holds the fixed text and no text of
// a decoder.
func tcdRequireFixed(t *testing.T, what string, raw []byte, want string) {
	t.Helper()
	if !bytes.Contains(raw, []byte(want)) {
		t.Fatalf("%s: the answer does not hold %q: %.300s", what, want, raw)
	}
	for _, bad := range append([]string{"parse refs policy", "ref policy"}, goDecoderText...) {
		if bytes.Contains(raw, []byte(bad)) {
			t.Fatalf("%s: the answer holds %q: %.400s", what, bad, raw)
		}
	}
}

type tcdLog struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *tcdLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *tcdLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// tcdCaptureSlog replaces slog's default logger until the test ends. No test
// that calls it may run in parallel.
func tcdCaptureSlog(t *testing.T) *tcdLog {
	t.Helper()
	out := &tcdLog{}
	prev := slog.Default()
	// slog.SetDefault also redirects the log package, and restoring slog's
	// own default handler does not undo that.
	prevOut, prevFlags := log.Writer(), log.Flags()
	slog.SetDefault(slog.New(slog.NewTextHandler(out, nil)))
	t.Cleanup(func() {
		slog.SetDefault(prev)
		log.SetOutput(prevOut)
		log.SetFlags(prevFlags)
	})
	return out
}

// TestMalformedRefsClaimAnswersFixedText covers issue #841: a token whose
// `refs` claim does not parse fails closed with 403 on each route below, and
// the answer holds no text of the decoder, which can name Go structs and types.
// The decoder's text goes to the log.
func TestMalformedRefsClaimAnswersFixedText(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		url := "tcd-" + randomHex(t, 4)
		createRepo(t, e, org, url, []byte(`{"default_branch":"main"}`), http.StatusCreated)
		scopes := []string{auth.ScopeRepoWrite, auth.ScopeGitRead, auth.ScopeGitWrite}
		for _, refs := range []string{`["not-a-tuple"]`, `[["main"]]`, `{"main":[]}`, `[[5,[]]]`, `[["main",[1]]]`} {
			tok := e.mint(t, tokenSpec{iss: org, repo: url, scopes: scopes, refs: refs})
			for _, r := range tcdRoutes() {
				t.Run(r.name+" "+refs, func(t *testing.T) {
					logs := tcdCaptureSlog(t)
					var body []byte
					if r.body != "" {
						body = []byte(r.body)
					}
					resp, raw := e.doReq(t, r.method, r.path, tok, body, "")
					if resp.StatusCode != http.StatusForbidden {
						t.Fatalf("%s %s: status %d, want 403 (body %.300s)", r.method, r.path, resp.StatusCode, raw)
					}
					requireEnvelope(t, resp, raw)
					tcdRequireFixed(t, r.method+" "+r.path, raw, "malformed refs claim")
					if got := logs.String(); !strings.Contains(got, "refs claim does not parse") || !strings.Contains(got, "iss="+org) {
						t.Fatalf("the log of a malformed refs claim: %q", got)
					}
				})
			}
		}
	})
}

// TestRefPoliciesErrorIsFixedText: the error of refPolicies, which a handler
// shows a client, is the fixed text, and the decoder's error is behind it.
func TestRefPoliciesErrorIsFixedText(t *testing.T) {
	logs := tcdCaptureSlog(t)
	_, err := refPolicies(&auth.Claims{Iss: "acme", Repo: "r", Refs: []byte(`[[5,[]]]`)})
	var bad *auth.MalformedError
	if !errors.As(err, &bad) || err.Error() != "malformed refs claim" || errors.Unwrap(err) == nil {
		t.Fatalf("error %T %v, want *auth.MalformedError with the fixed text and the decoder's error behind it", err, err)
	}
	if got := logs.String(); !strings.Contains(got, "cannot unmarshal") {
		t.Fatalf("the log does not hold the decoder's text: %q", got)
	}
}

// TestMalformedClaimsAnswerGenericText covers issue #841 on the REST surface:
// a token whose signed claims do not decode (a claim of the wrong JSON type)
// gets 403 without decoder text, on each route below, the embedded
// fork-source token included.
func TestMalformedClaimsAnswerGenericText(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		url := "tcd-" + randomHex(t, 4)
		createRepo(t, e, org, url, []byte(`{"default_branch":"main"}`), http.StatusCreated)
		scopes := []string{auth.ScopeRepoWrite, auth.ScopeGitRead, auth.ScopeGitWrite}
		for name, claims := range map[string]map[string]any{
			"exp":    {"exp": "tomorrow"},
			"scopes": {"scopes": 5},
			"repo":   {"repo": 5},
			"sub":    {"sub": []string{}},
			"ops":    {"ops": "no-push"},
		} {
			tok := e.mint(t, tokenSpec{iss: org, repo: url, scopes: scopes, claims: claims})
			for _, r := range tcdRoutes() {
				t.Run(r.name+" "+name, func(t *testing.T) {
					logs := tcdCaptureSlog(t)
					var body []byte
					if r.body != "" {
						body = []byte(r.body)
					}
					resp, raw := e.doReq(t, r.method, r.path, tok, body, "")
					if resp.StatusCode != http.StatusForbidden {
						t.Fatalf("%s %s: status %d, want 403 (body %.300s)", r.method, r.path, resp.StatusCode, raw)
					}
					requireEnvelope(t, resp, raw)
					tcdRequireFixed(t, r.method+" "+r.path, raw, "Invalid or expired token")
					if got := logs.String(); !strings.Contains(got, "do not decode") {
						t.Fatalf("the log of a signed claim that does not decode: %q", got)
					}
				})
			}
			t.Run("fork source token "+name, func(t *testing.T) {
				src := e.mint(t, tokenSpec{iss: org, repo: url, scopes: []string{auth.ScopeGitRead}, claims: claims})
				dst := e.mint(t, tokenSpec{iss: org, repo: "tcd-dst-" + randomHex(t, 4), scopes: scopes})
				body := `{"base_repo":{"name":"` + url + `","auth":{"token":"` + src + `"}}}`
				resp, raw := e.doReq(t, http.MethodPost, "/api/v1/repos", dst, []byte(body), "")
				if resp.StatusCode != http.StatusForbidden {
					t.Fatalf("status %d, want 403 (body %.300s)", resp.StatusCode, raw)
				}
				requireEnvelope(t, resp, raw)
				tcdRequireFixed(t, "fork source token", raw, "base_repo.auth.token: invalid token")
			})
		}
	})
}

// TestForkSourceTokenWithCritIsRefused: a source token for a copy goes through
// the verifier like the caller's token. A token that a registered key signed
// is refused when its header has `crit` (RFC 7515 §4.1.11), and the same token
// without `crit` passes, so the copy fails on the missing source instead.
func TestForkSourceTokenWithCritIsRefused(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		dst := e.mint(t, tokenSpec{iss: org, repo: "crit-dst-" + randomHex(t, 4), scopes: []string{auth.ScopeRepoWrite}})
		for _, tc := range []struct {
			name       string
			header     map[string]any
			wantStatus int
			wantError  string
		}{
			{"without crit", nil, http.StatusNotFound, "fork source repository not found"},
			{"with crit", critHeader, http.StatusForbidden, "base_repo.auth.token: invalid token"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				src := e.mint(t, tokenSpec{iss: org, repo: "crit-src", scopes: []string{auth.ScopeGitRead}, header: tc.header})
				body := `{"base_repo":{"name":"crit-src","auth":{"token":"` + src + `"}}}`
				resp, raw := e.doReq(t, http.MethodPost, "/api/v1/repos", dst, []byte(body), "")
				if resp.StatusCode != tc.wantStatus {
					t.Fatalf("status %d, want %d (body %.300s)", resp.StatusCode, tc.wantStatus, raw)
				}
				if msg := requireEnvelope(t, resp, raw); msg != tc.wantError {
					t.Fatalf("error %q, want %q", msg, tc.wantError)
				}
			})
		}
	})
}
