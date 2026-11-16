package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/errtext"
)

// TestLongValuesAreCutInAnswers: a 400 repeats request text in `error`,
// `detail` and, for a member name, `invalid_params`. A 1 MiB value must not
// make a 3 MiB answer, and a short value is echoed whole.
func TestLongValuesAreCutInAnswers(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := ipNewFixture(t, e)
		long := strings.Repeat("<", 900_000)
		for _, tc := range []struct {
			name, path, body string
			status           int
		}{
			{"a bad strategy", "/api/v1/repos/merge", `{"source_ref":"f","target_branch":"main","strategy":"` + long + `"}`, http.StatusBadRequest},
			{"an unknown member name", "/api/repos/" + fx.url + "/notes/append", `{"` + long + `":1}`, http.StatusBadRequest},
			// The cut is not tied to the 400: a 404 repeats request text too.
			{"an object_ref that does not resolve", "/api/v1/repos/notes", `{"object_ref":"` + long + `","note":"n","author":{"name":"a","email":"e"}}`, http.StatusNotFound},
		} {
			t.Run(tc.name, func(t *testing.T) {
				resp, body := e.doReq(t, http.MethodPost, tc.path, fx.gitTok, []byte(tc.body), "")
				if resp.StatusCode != tc.status {
					t.Fatalf("status = %d, want %d (body %.200s)", resp.StatusCode, tc.status, body)
				}
				requireEnvelope(t, resp, body)
				// error, detail and the name: three cuts of at most 16 KiB, each
				// with up to six bytes for every escaped `<`.
				if len(body) > 320_000 {
					t.Fatalf("the answer has %d bytes for a 900,000-byte value", len(body))
				}
				if !strings.Contains(string(body), "…") {
					t.Fatalf("the answer does not show that it cut the value: %.300s", body)
				}
			})
		}
		// A short value is echoed whole.
		resp, body := e.doReq(t, http.MethodPost, "/api/v1/repos/merge", fx.gitTok,
			[]byte(`{"source_ref":"f","target_branch":"main","strategy":"rebase"}`), "")
		if resp.StatusCode != http.StatusBadRequest || requireEnvelope(t, resp, body) != `invalid strategy "rebase" (want merge|ff_only|ff_prefer)` {
			t.Fatalf("a short value: status %d, body %s", resp.StatusCode, body)
		}
	})
}

// TestCommitWriteResultMessageIsCut: the result envelope of a commit write
// repeats request text in `result.message`, which is cut as `error` is. A
// short value is repeated whole.
func TestCommitWriteResultMessageIsCut(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wctServer(t, e)
		_, writeTok, _ := wctRepo(t, e)
		long := strings.Repeat("<", 900_000)
		file := func(mode string) []map[string]any {
			return []map[string]any{{"path": "a", "content_id": "c", "mode": mode}}
		}
		gitPath := "x/.git/" + long
		patch := "diff --git a/" + gitPath + " b/" + gitPath + "\nnew file mode 100644\n--- /dev/null\n+++ b/" + gitPath + "\n@@ -0,0 +1 @@\n+hi\n"
		for _, tc := range []struct {
			name, path, want string
			body             []byte
		}{
			{"a commit-pack mode", "/api/v1/repos/commit-pack", `invalid mode "<<<`,
				wctLines(t, wctMeta("main", "m", file(long), nil))},
			{"a diff-commit path", "/api/v1/repos/diff-commit", `invalid patch: .git component in path "x/.git/<<<`,
				wctLines(t, wctMeta("main", "m", nil, nil), wctDiffChunk([]byte(patch), true))},
		} {
			t.Run(tc.name, func(t *testing.T) {
				resp, body := wctPost(t, srv, tc.path, writeTok, strings.NewReader(string(tc.body)))
				msg := wctRequireFail(t, resp, body, http.StatusBadRequest, "invalid").Result.Message
				// A cut of at most 16 KiB, with six bytes for every escaped `<`.
				if len(body) > 120_000 || !strings.HasPrefix(msg, tc.want) || !strings.HasSuffix(msg, "…") || len(msg) > errtext.MaxBytes+len("…") {
					t.Fatalf("the answer has %d bytes and a message of %d bytes (%.80q) for a 900,000-byte value", len(body), len(msg), msg)
				}
			})
		}
		resp, body := wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, strings.NewReader(string(wctLines(t, wctMeta("main", "m", file("<x>"), nil)))))
		if msg := wctRequireFail(t, resp, body, http.StatusBadRequest, "invalid").Result.Message; msg != `invalid mode "<x>" for "a"` {
			t.Fatalf("a short value: message %q", msg)
		}
	})
}

// TestNewProblemKeepsTheCallersParams: the cut of a name must not write
// through to the slice that the caller passed in.
func TestNewProblemKeepsTheCallersParams(t *testing.T) {
	long := strings.Repeat("n", errtext.MaxBytes+10)
	params := []invalidParam{{Name: long, Reason: "is not a known field"}}
	rec := httptest.NewRecorder()
	got := newProblem(rec, http.StatusBadRequest, "bad_request", "msg", params, "", "")
	if params[0].Name != long {
		t.Fatalf("the caller's name was cut to %d bytes", len(params[0].Name))
	}
	if n := got.InvalidParams[0].Name; len(n) != errtext.MaxBytes+len("…") {
		t.Fatalf("the answer's name has %d bytes", len(n))
	}
}
