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
// is a 409 ref_guard_failed problem body before any merge work (#568).
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

		t.Run("pin outside source_ref is 409 ref_guard_failed and mutates nothing", func(t *testing.T) {
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
				wmRequireRefGuard(t, resp, raw, "expected_source_sha", fx.conflictTip,
					"source branch moved: the source branch does not contain commit "+pin)
				if _, has := wmDecode(t, raw)["conflict_paths"]; has {
					t.Fatalf("pin %s: merge ran before the guard: %s", pin, raw)
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

		t.Run("pin on a missing source branch is 409 with 40 zeros", func(t *testing.T) {
			// Upstream (spec 2026-10-27, changelog 2026-10-22): a source
			// branch that does not exist fails the guard, and current_sha is
			// 40 zeros. Without a pin it stays a 404.
			refsBefore := wmRefsSnapshot(t, e, fx.repoID)
			resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
				wmMergeBody("gone", "main", "merge", map[string]any{"author": wmTestAuthor, "expected_source_sha": fx.featTip}))
			wmRequireRefGuard(t, resp, raw, "expected_source_sha", strings.Repeat("0", 40),
				"source branch moved: the source branch does not contain commit "+fx.featTip)
			if refsAfter := wmRefsSnapshot(t, e, fx.repoID); !reflect.DeepEqual(refsBefore, refsAfter) {
				t.Fatal("refs changed on a refused merge")
			}
			resp, raw = wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
				wmMergeBody("gone", "main", "merge", map[string]any{"author": wmTestAuthor}))
			if resp.StatusCode != http.StatusNotFound || wmProblem(t, resp, raw) != "source branch gone not found" {
				t.Fatalf("no pin: status %d body %s, want 404 source branch gone not found", resp.StatusCode, raw)
			}
			// The target guard runs first, and a missing target is a 404.
			resp, raw = wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
				wmMergeBody("gone", "main", "merge", map[string]any{"expected_source_sha": fx.featTip, "expected_target_sha": fx.c1}))
			wmRequireRefGuard(t, resp, raw, "expected_target_sha", fx.c2,
				"expected_target_sha "+fx.c1+" does not match current tip "+fx.c2)
			resp, raw = wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
				wmMergeBody("gone", "nope", "merge", map[string]any{"expected_source_sha": fx.featTip}))
			if resp.StatusCode != http.StatusNotFound || wmProblem(t, resp, raw) != "target branch nope not found" {
				t.Fatalf("missing target: status %d body %s, want 404 target branch nope not found", resp.StatusCode, raw)
			}
		})

		t.Run("malformed pin is 400", func(t *testing.T) {
			for _, pin := range []any{"", nil, "abc123", fx.ffMid + "0", 5} {
				resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
					wmMergeBody("ffsrc", "t-pin-ff", "ff_only", map[string]any{"expected_source_sha": pin}))
				if resp.StatusCode != http.StatusBadRequest {
					t.Fatalf("pin %#v: status = %d, body %s", pin, resp.StatusCode, raw)
				}
				if msg := wmProblem(t, resp, raw); msg != "expected_source_sha must be a 40-hex sha" {
					t.Fatalf("pin %#v: error = %q", pin, msg)
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
			wmRequireRefGuard(t, resp, raw, "expected_source_sha", fx.ffTip,
				"source branch moved: the source branch does not contain commit "+fx.featTip)
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
