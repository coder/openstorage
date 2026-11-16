package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/ref"
)

// TestRefusalsQuoteRequestTextBounded: an answer that quotes a webhook URL,
// its host, or the ref policy op of a token cuts the text at 256 bytes and
// names its length (errtext.Quote), so it stays small whatever the request
// holds. Before, each of these answered about 196 KB for 100,000 bytes of
// `<`, which JSON escapes to six bytes each.
func TestRefusalsQuoteRequestTextBounded(t *testing.T) {
	long := strings.Repeat("<", 100_000)
	cut := strings.Repeat("<", 256) + "…"
	check := func(t *testing.T, resp *http.Response, out []byte, wantStatus int, want string) {
		t.Helper()
		var p problemBody
		if err := json.Unmarshal(out, &p); err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != wantStatus || p.Detail != want {
			t.Fatalf("status %d, detail %.400q; want %d %.400q", resp.StatusCode, p.Detail, wantStatus, want)
		}
		if len(out) > 8<<10 {
			t.Fatalf("answer is %d bytes, want at most 8 KiB", len(out))
		}
	}
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		whTok := orgToken(t, e, org)
		for _, tc := range []struct{ name, url, want string }{
			{"a zone-scoped host", "https://[fe80::1%25" + long + "]/hook",
				`invalid webhook url: webhook target "fe80::1%` + strings.Repeat("<", 256-8) + `…" (100008 bytes) denied: zone-scoped address`},
			{"a URL with no host", "https://:80/" + long,
				`invalid webhook url: webhook url "https://:80/` + strings.Repeat("<", 256-12) + `…" (100012 bytes) has no host`},
			{"a short host", "https://[fe80::1%25eth0]/hook",
				`invalid webhook url: webhook target "fe80::1%eth0" denied: zone-scoped address`},
		} {
			t.Run("webhook "+tc.name, func(t *testing.T) {
				body, err := json.Marshal(map[string]string{"url": tc.url})
				if err != nil {
					t.Fatal(err)
				}
				resp, out := e.doReq(t, http.MethodPost, "/api/v1/webhooks", whTok, body, "")
				check(t, resp, out, http.StatusBadRequest, tc.want)
			})
		}

		for _, op := range []string{long, "no-push"} {
			quoted := `"no-push"`
			if op == long {
				quoted = `"` + cut + `" (100000 bytes)`
			}
			refs := `[["*",["` + op + `"]]]`
			name := "ops-" + randomHex(t, 4)
			// Each route checks the policy before it looks the repository
			// up, so the repository need not exist.
			for _, tc := range []struct {
				name, method, path, body, what string
				scopes                         []string
			}{
				{"sync create", http.MethodPost, "/api/v1/repos", `{"upstream":{"provider":"gitea","owner":"o","name":"n","host":"forge.example.invalid"}}`, "sync repository create", []string{auth.ScopeRepoWrite}},
				{"repository deletion", http.MethodDelete, "/api/v1/repos/delete", "", "repository deletion", []string{auth.ScopeRepoWrite}},
				{"default branch change", http.MethodPatch, "/api/v1/repo", `{"default_branch":"main"}`, "changing the default branch", []string{auth.ScopeRepoWrite}},
				{"pull-upstream", http.MethodPost, "/api/v1/repos/pull-upstream", "", "pull-upstream", []string{auth.ScopeGitWrite}},
				{"push-upstream", http.MethodPost, "/api/v1/repos/push-upstream", "", "push-upstream", []string{auth.ScopeGitWrite, auth.ScopeGitRead}},
			} {
				opName := "a short op"
				if op == long {
					opName = "a long op"
				}
				t.Run(tc.name+" with "+opName, func(t *testing.T) {
					tok := e.mint(t, tokenSpec{iss: org, repo: name, scopes: tc.scopes, refs: refs})
					var body []byte
					if tc.body != "" {
						body = []byte(tc.body)
					}
					resp, out := e.doReq(t, tc.method, tc.path, tok, body, "")
					check(t, resp, out, http.StatusForbidden, "ref policy op "+quoted+" denies "+tc.what)
				})
			}
		}
	})
}

// TestUnknownPolicyOpQuotedBounded: a ref write (refs and tags), a commit
// write and a merge refuse a ref policy op that they do not know with a
// 403 that quotes the op with errtext.Quote, so an op of any length makes
// a short answer.
func TestUnknownPolicyOpQuotedBounded(t *testing.T) {
	const refName = "refs/heads/main"
	long := strings.Repeat("<", 100_000)
	for _, tc := range []struct{ name, op, quoted string }{
		{"a long op", long, `"` + strings.Repeat("<", 256) + `…" (100000 bytes)`},
		{"a short op", "no-fly", `"no-fly"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := "unknown policy op " + tc.quoted + " for " + refName
			claims := &auth.Claims{Refs: json.RawMessage(`[["main",["` + tc.op + `"]]]`)}
			newSHA := strings.Repeat("a", 40)

			deny, err := wrefPolicyCheck(claims, refName, ref.ZeroSHA, newSHA, nil)
			if err != nil || deny != want {
				t.Fatalf("wrefPolicyCheck = %.300q, %v; want %.300q", deny, err, want)
			}

			// A commit write route answers a 403 in a problem body
			// (commitWriteEnvelopeStatuses lists no 403).
			rec := httptest.NewRecorder()
			if wcCheckRefPolicy(&envelopeWriter{ResponseWriter: rec}, claims, refName) {
				t.Fatal("wcCheckRefPolicy admitted an unknown op")
			}
			var wc problemBody
			if err := json.Unmarshal(rec.Body.Bytes(), &wc); err != nil || rec.Code != http.StatusForbidden || wc.Detail != want {
				t.Fatalf("wcCheckRefPolicy: %d %.300s; want 403 %.300q", rec.Code, rec.Body.Bytes(), want)
			}

			rec = httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodPost, "/api/v1/repos/merge", nil).WithContext(auth.WithClaims(t.Context(), claims))
			if (&server{}).wmCheckRefPolicy(rec, r, nil, refName, ref.ZeroSHA, newSHA, false, "main") {
				t.Fatal("wmCheckRefPolicy admitted an unknown op")
			}
			var p problemBody
			if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil || rec.Code != http.StatusForbidden || p.Detail != want {
				t.Fatalf("wmCheckRefPolicy: %d %.300s; want 403 %.300q", rec.Code, rec.Body.Bytes(), want)
			}
			if rec.Body.Len() > 8<<10 {
				t.Fatalf("merge answer is %d bytes, want at most 8 KiB", rec.Body.Len())
			}
		})
	}
}
