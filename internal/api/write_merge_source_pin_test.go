package api

import (
	"encoding/json"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/gitlimit"
	"github.com/coder/openstorage/internal/gitread"
	"github.com/coder/openstorage/internal/ref"
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

// The source branch can reach a pin that no registered pack holds: a push can
// leave a commit behind a ref tip while the node's cache holds it. A target ref
// or a merge commit at that pin would wedge every other node, which hydrates
// from the registered packs alone, so the pin answers like a commit that the
// source branch does not contain. The tip of the source branch is a ref already
// and needs no check.
func TestMergeExpectedSourceSHARefusesAPinThatThePacksLack(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wmServer(t, e)
		fx := wmSeedFixture(t, e)
		ctx := t.Context()

		// pin is in the cache only. plantsrc is a branch whose tip is a child of
		// pin, in a registered pack that lacks pin: the push that left it there
		// was checked against a cache that held pin.
		repoPath, err := gitread.Materialize(ctx, e.refStore, e.blobStore, e.cacheDir, fx.repoID, "main")
		if err != nil {
			t.Fatal(err)
		}
		pin := plantCacheOnlyCommit(t, repoPath, fx.c2)
		dir := t.TempDir()
		scratchGit(t, dir, "", "init", "-q", "-b", "main", ".")
		tip := scratchGit(t, dir, commitText(coreRevParse(t, fx.work, fx.c2+"^{tree}"), pin), "hash-object", "-t", "commit", "-w", "--stdin", "--literally")
		registerLooseObjects(t, e, fx.repoID, dir, tip)
		for name, sha := range map[string]string{"plantsrc": tip, "t-plant-ff": fx.c2, "t-plant-merge": fx.c2, "t-plant-ok": fx.c2} {
			if won, err := e.refStore.CASRef(ctx, fx.repoID, "refs/heads/"+name, ref.ZeroSHA, sha); err != nil || !won {
				t.Fatalf("create %s: won %v, err %v", name, won, err)
			}
		}

		for _, tc := range []struct{ target, strategy string }{{"t-plant-ff", "ff_only"}, {"t-plant-merge", "merge"}} {
			t.Run(tc.strategy, func(t *testing.T) {
				before := wmRefsSnapshot(t, e, fx.repoID)
				resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
					wmMergeBody("plantsrc", tc.target, tc.strategy, map[string]any{"author": wmTestAuthor, "expected_source_sha": pin}))
				wmRequireRefGuard(t, resp, raw, "expected_source_sha", tip,
					"source branch moved: the source branch does not contain commit "+pin)
				if after := wmRefsSnapshot(t, e, fx.repoID); !reflect.DeepEqual(before, after) {
					t.Fatalf("refs changed on a refused merge: before %v, after %v", before, after)
				}
			})
		}

		// The check is git work that the merge starts, so it runs under the same
		// admission slots as a create: with every slot taken the merge answers
		// 503 with Retry-After (not 500) and writes nothing, and it frees its
		// slot when it is done. ffMid is held completely and reached by ffsrc.
		t.Run("no free slot", func(t *testing.T) {
			p := withGitReadPolicy(t, testGitReadPolicy(1, time.Minute, gitread.DefaultOutputBytes, gitread.DefaultLimits()))
			if won, err := e.refStore.CASRef(ctx, fx.repoID, "refs/heads/t-plant-busy", ref.ZeroSHA, fx.c2); err != nil || !won {
				t.Fatalf("create t-plant-busy: won %v, err %v", won, err)
			}
			body := wmMergeBody("ffsrc", "t-plant-busy", "ff_only", map[string]any{"expected_source_sha": fx.ffMid})
			before := wmRefsSnapshot(t, e, fx.repoID)
			key := gitlimit.RepoKey(fx.repoID)
			if err := p.slots.Acquire(ctx, key); err != nil {
				t.Fatal(err)
			}
			resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token, body)
			if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
				t.Fatalf("with no free slot: status %d, Retry-After %q, body %s, want 503 with Retry-After", resp.StatusCode, resp.Header.Get("Retry-After"), raw)
			}
			if after := wmRefsSnapshot(t, e, fx.repoID); !reflect.DeepEqual(before, after) {
				t.Fatalf("refs changed while no slot was free: before %v, after %v", before, after)
			}
			p.slots.Release(key)
			resp, raw = wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token, body)
			if got := wmDecode(t, raw); resp.StatusCode != http.StatusOK || got["commit_sha"] != fx.ffMid {
				t.Fatalf("with a free slot: status %d body %s, want fast_forward to %s", resp.StatusCode, raw, fx.ffMid)
			}
			requireSlotsFree(t, p)
		})

		// A panic after the first git child must not take the slot with it.
		t.Run("a panic after the walk", func(t *testing.T) {
			p := withGitReadPolicy(t, testGitReadPolicy(1, time.Minute, gitread.DefaultOutputBytes, gitread.DefaultLimits()))
			if won, err := e.refStore.CASRef(ctx, fx.repoID, "refs/heads/t-plant-panic", ref.ZeroSHA, fx.c2); err != nil || !won {
				t.Fatalf("create t-plant-panic: won %v, err %v", won, err)
			}
			probe := &panicsWhileASlotIsHeld{Store: e.refStore, p: p}
			env := *e
			env.refStore = probe
			panicSrv := wmServer(t, &env)
			probe.armed.Store(true)
			resp, raw := wmDo(t, panicSrv, "POST", "/api/v1/repos/merge", fx.token,
				wmMergeBody("plantsrc", "t-plant-panic", "ff_only", map[string]any{"expected_source_sha": pin}))
			probe.armed.Store(false)
			if resp.StatusCode != http.StatusInternalServerError {
				t.Fatalf("status %d, body %s, want the 500 of a panic", resp.StatusCode, raw)
			}
			requireSlotsFree(t, p)
		})

		// The tip itself is a ref already: pinning it is no walk and no refusal.
		t.Run("the tip", func(t *testing.T) {
			resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
				wmMergeBody("plantsrc", "t-plant-ok", "ff_only", map[string]any{"expected_source_sha": tip}))
			if body := wmDecode(t, raw); resp.StatusCode != http.StatusOK || body["commit_sha"] != tip {
				t.Fatalf("status %d body %s, want fast_forward to the tip %s", resp.StatusCode, raw, tip)
			}
		})
	})
}
