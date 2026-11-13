package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestBoundEcho(t *testing.T) {
	if maxEchoBytes != 2048 {
		t.Fatalf("maxEchoBytes = %d, the docs say 2048", maxEchoBytes)
	}
	exact := strings.Repeat("a", maxEchoBytes)
	if got := boundEcho(exact); got != exact {
		t.Fatalf("a text of exactly %d bytes changed", maxEchoBytes)
	}
	if got := boundEcho(exact + "b"); got != exact+"…" {
		t.Fatalf("a text one byte over: got %d bytes, want the cut text and an ellipsis", len(got))
	}
	// A cut inside a multi-byte rune moves back to the rune start.
	runes := strings.Repeat("é", maxEchoBytes) // 2 bytes each
	got := boundEcho(runes[:maxEchoBytes+1])
	if !utf8.ValidString(got) || got != strings.Repeat("é", maxEchoBytes/2)+"…" {
		t.Fatalf("rune cut: %d bytes, valid %v", len(got), utf8.ValidString(got))
	}
	// Three-byte runes: 2048 is not a multiple of 3.
	three := strings.Repeat("€", 1000)
	got = boundEcho(three)
	if !utf8.ValidString(got) || len(got) > maxEchoBytes+len("…") || !strings.HasSuffix(got, "…") {
		t.Fatalf("three-byte runes: %d bytes, valid %v", len(got), utf8.ValidString(got))
	}
}

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
				// error, detail and the name: three cuts of at most 2 KiB, each
				// with up to six bytes for every escaped `<`.
				if len(body) > 40_000 {
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

// TestNewProblemKeepsTheCallersParams: the cut of a name must not write
// through to the slice that the caller passed in.
func TestNewProblemKeepsTheCallersParams(t *testing.T) {
	long := strings.Repeat("n", maxEchoBytes+10)
	params := []invalidParam{{Name: long, Reason: "is not a known field"}}
	rec := httptest.NewRecorder()
	got := newProblem(rec, http.StatusBadRequest, "bad_request", "msg", params, "", "")
	if params[0].Name != long {
		t.Fatalf("the caller's name was cut to %d bytes", len(params[0].Name))
	}
	if n := got.InvalidParams[0].Name; len(n) != maxEchoBytes+len("…") {
		t.Fatalf("the answer's name has %d bytes", len(n))
	}
}
