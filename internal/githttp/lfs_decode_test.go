package githttp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestLFSBatchDecodeAnswersHoldNoGoTypeNames covers issue #841 for the LFS
// batch route: it decodes the body before it authenticates the request, so
// any client can read its 400. The answer names the field of the wrong JSON
// type and holds no text of encoding/json, which names Go types and structs.
func TestLFSBatchDecodeAnswersHoldNoGoTypeNames(t *testing.T) {
	for _, tc := range []struct{ name, body, want string }{
		{"a field of the wrong type", `{"operation":5}`, "bad batch request: operation has the wrong JSON type"},
		{"an array element", `{"transfers":["basic",1]}`, "bad batch request: transfers.1 has the wrong JSON type"},
		{"a nested field", `{"objects":[{"oid":"a","size":"x"}]}`, "bad batch request: objects.0.size has the wrong JSON type"},
		{"a syntax error", `{"operation":`, "bad batch request"},
		{"a wrong top-level type", `[1]`, "bad batch request"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/repo.git/info/lfs/objects/batch", strings.NewReader(tc.body))
			rec := httptest.NewRecorder()
			(&Server{}).handleLFSBatch(rec, req, "repo")
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body %s)", rec.Code, rec.Body)
			}
			var got struct {
				Message string `json:"message"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil || got.Message != tc.want {
				t.Fatalf("answer %s (decode error %v), want message %q", rec.Body, err, tc.want)
			}
			for _, bad := range []string{"Go struct", "Go value", "json:", "cannot unmarshal", "of type", "unexpected end of JSON", "invalid character"} {
				if strings.Contains(rec.Body.String(), bad) {
					t.Fatalf("the answer holds %q: %s", bad, rec.Body)
				}
			}
		})
	}
}
