package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// TestLongPathIsCutInInstance: the request path is repeated in `instance`
// and no route validates its length, so a 900,000-byte path of `&` (six bytes
// each in JSON) must not make a 6 MB answer. The cut never leaves part of a
// %XX escape, and a short path is echoed whole.
func TestLongPathIsCutInInstance(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		const prefix = "/api/v1/"
		instanceOf := func(t *testing.T, path string) (string, int) {
			t.Helper()
			resp, body := e.doReq(t, http.MethodGet, path, "", nil, "")
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("status = %d, want 404 (body %.200s)", resp.StatusCode, body)
			}
			// requireEnvelope expects the whole path in `instance`, so read it here.
			var got struct {
				Instance string `json:"instance"`
			}
			if err := json.Unmarshal(body, &got); err != nil {
				t.Fatalf("answer is not JSON: %v", err)
			}
			return got.Instance, len(body)
		}

		got, size := instanceOf(t, prefix+strings.Repeat("&", 900_000))
		if len(got) != maxInstanceBytes {
			t.Fatalf("instance has %d bytes, want %d", len(got), maxInstanceBytes)
		}
		// instance is the one member that repeats the path: at most six bytes
		// for each of its bytes.
		if size > 120_000 {
			t.Fatalf("the answer has %d bytes for a 900,000-byte path", size)
		}

		// Each place where the cut meets a %XX escape: the cut keeps every
		// whole escape and drops the part of the one that it would split.
		for _, lead := range []int{0, 1, 2} {
			path := prefix + strings.Repeat("a", maxInstanceBytes-len(prefix)-lead) + strings.Repeat("%E2%82%AC", 10)
			got, _ := instanceOf(t, path)
			if !strings.HasPrefix(path, got) {
				t.Fatalf("lead %d: instance of %d bytes is not a prefix of the path", lead, len(got))
			}
			if want := maxInstanceBytes - lead; len(got) != want {
				t.Fatalf("lead %d: instance has %d bytes, want %d", lead, len(got), want)
			}
			if i := strings.LastIndexByte(got, '%'); i >= 0 && len(got)-i < 3 {
				t.Fatalf("lead %d: instance ends in a partial escape %q", lead, got[i:])
			}
		}

		if got, _ := instanceOf(t, prefix+"nope"); got != prefix+"nope" {
			t.Fatalf("a short path: instance %q", got)
		}
	})
}
