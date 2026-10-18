package api

import (
	"encoding/json"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

// TestMergeExpectedSourceSHA pins the source commit with
// expected_source_sha (spec 2026-10-16, #531): the merge uses the pinned
// commit, not the source tip, and a pin that source_ref does not contain
// is a 409 problem body with code conflict before any merge work.
func TestMergeExpectedSourceSHA(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wmServer(t, e)
		fx := wmSeedFixture(t, e)

		t.Run("ff_only merges the pinned commit, not the tip", func(t *testing.T) {
			// Upper case also checks the full-SHA case folding.
			resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
				wmMergeBody("ffsrc", "t-pin-ff", "ff_only", map[string]any{
					"expected_source_sha": strings.ToUpper(fx.ffMid),
				}))
			body := wmDecode(t, raw)
			if resp.StatusCode != http.StatusOK || body["result"] != "fast_forward" || body["commit_sha"] != fx.ffMid {
				t.Fatalf("status %d body %s, want fast_forward to ffMid %s", resp.StatusCode, raw, fx.ffMid)
			}
			if source := body["source"].(map[string]any); source["sha"] != fx.ffMid {
				t.Errorf("source = %v, want sha %s", source, fx.ffMid)
			}
			if got := wmRefsSnapshot(t, e, fx.repoID)["refs/heads/t-pin-ff"]; got != fx.ffMid {
				t.Fatalf("t-pin-ff tip = %q, want the pinned %s (source tip is %s)", got, fx.ffMid, fx.ffTip)
			}
		})

		t.Run("merge commit takes the pinned commit as parent", func(t *testing.T) {
			resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
				wmMergeBody("ffsrc", "t-pin-merge", "merge", map[string]any{
					"author": wmTestAuthor, "expected_source_sha": fx.ffMid,
				}))
			body := wmDecode(t, raw)
			if resp.StatusCode != http.StatusOK || body["result"] != "merge_commit" {
				t.Fatalf("status %d body %s", resp.StatusCode, raw)
			}
			if parents := wmParents(t, e, fx.repoID, body["commit_sha"].(string)); !reflect.DeepEqual(parents, []string{fx.c2, fx.ffMid}) {
				t.Fatalf("parents = %v, want [%s %s]", parents, fx.c2, fx.ffMid)
			}
		})

		t.Run("pin outside source_ref is 409 conflict and mutates nothing", func(t *testing.T) {
			tree := coreRevParse(t, fx.work, fx.c2+"^{tree}")
			// featTip is a commit on another branch, 1111… is absent from
			// the odb, and tree is not a commit. Source "conflict" would
			// otherwise be a merge conflict: the guard must answer first.
			for _, pin := range []string{fx.featTip, strings.Repeat("1", 40), tree} {
				refsBefore := wmRefsSnapshot(t, e, fx.repoID)
				packsBefore, err := e.refStore.ListPacks(t.Context(), fx.repoID)
				if err != nil {
					t.Fatal(err)
				}
				resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
					wmMergeBody("conflict", "main", "merge", map[string]any{
						"author": wmTestAuthor, "expected_source_sha": pin,
					}))
				if resp.StatusCode != http.StatusConflict {
					t.Fatalf("pin %s: status = %d, body %s", pin, resp.StatusCode, raw)
				}
				body := wmDecode(t, raw)
				want := "source branch moved: the source branch does not contain commit " + pin
				if body["code"] != "conflict" || body["detail"] != want || body["error"] != want {
					t.Fatalf("pin %s: body = %v, want problem code conflict and %q", pin, body, want)
				}
				if _, has := body["conflict_paths"]; has {
					t.Fatalf("pin %s: merge ran before the guard: %v", pin, body)
				}
				if refsAfter := wmRefsSnapshot(t, e, fx.repoID); !reflect.DeepEqual(refsBefore, refsAfter) {
					t.Fatalf("pin %s: refs changed on a refused merge", pin)
				}
				packsAfter, err := e.refStore.ListPacks(t.Context(), fx.repoID)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(packsBefore, packsAfter) {
					t.Fatalf("pin %s: packs registered on a refused merge", pin)
				}
			}
		})

		t.Run("malformed pin is 400", func(t *testing.T) {
			for _, pin := range []any{"", nil, "abc123", fx.ffMid + "0", 5} {
				resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
					wmMergeBody("ffsrc", "t-pin-ff", "ff_only", map[string]any{"expected_source_sha": pin}))
				if resp.StatusCode != http.StatusBadRequest {
					t.Fatalf("pin %#v: status = %d, body %s", pin, resp.StatusCode, raw)
				}
				if res := wmResult(t, wmDecode(t, raw)); res["message"] != "expected_source_sha must be a 40-hex sha" {
					t.Fatalf("pin %#v: result = %v", pin, res)
				}
			}
		})

		t.Run("path-style route honors the pin", func(t *testing.T) {
			path := "/api/repos/" + url.PathEscape(fx.url) + "/merge"
			post := func(pin string) (*http.Response, []byte) {
				payload, err := json.Marshal(wmMergeBody("ffsrc", "t-pin-alias", "ff_only", map[string]any{"expected_source_sha": pin}))
				if err != nil {
					t.Fatal(err)
				}
				return e.doReq(t, http.MethodPost, path, fx.token, payload, "")
			}
			resp, raw := post(fx.featTip)
			if resp.StatusCode != http.StatusConflict || wmDecode(t, raw)["code"] != "conflict" {
				t.Fatalf("stale pin: status %d body %s, want 409 conflict", resp.StatusCode, raw)
			}
			resp, raw = post(fx.ffMid)
			if body := wmDecode(t, raw); resp.StatusCode != http.StatusOK || body["commit_sha"] != fx.ffMid {
				t.Fatalf("status %d body %s, want fast_forward to %s", resp.StatusCode, raw, fx.ffMid)
			}
			if got := wmRefsSnapshot(t, e, fx.repoID)["refs/heads/t-pin-alias"]; got != fx.ffMid {
				t.Fatalf("t-pin-alias tip = %q, want %s", got, fx.ffMid)
			}
		})
	})
}
