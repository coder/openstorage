package api

// A ref write must not store a commit that only the node's cache holds. The
// cache can hold objects that no registered pack holds (a cache that was copied
// or edited out of band, or a registry that was rolled back), and a ref at such
// an object wedges every other node, and this node after its cache is gone,
// because they hydrate from the registered packs alone.

import (
	"context"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/gitread"
	"github.com/coder/openstorage/internal/ref"
)

// plantCacheOnlyCommit writes a commit (a child of parent, with a new blob) as
// loose objects into the cache repository at repoPath only: no pack holds it.
func plantCacheOnlyCommit(t *testing.T, repoPath, parent string) string {
	t.Helper()
	run := func(stdin string, args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = repoPath
		for _, kv := range os.Environ() {
			if !strings.HasPrefix(kv, "GIT_") {
				cmd.Env = append(cmd.Env, kv)
			}
		}
		cmd.Env = append(cmd.Env, "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null",
			"GIT_AUTHOR_NAME=p", "GIT_AUTHOR_EMAIL=p@x.invalid", "GIT_COMMITTER_NAME=p", "GIT_COMMITTER_EMAIL=p@x.invalid")
		cmd.Stdin = strings.NewReader(stdin)
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("git %v: %v", args, err)
		}
		return strings.TrimSpace(string(out))
	}
	blob := run("cache only\n", "hash-object", "-w", "--stdin")
	tree := run("100644 blob "+blob+"\tcacheonly.txt\n", "mktree")
	return run("", "commit-tree", "-p", parent, "-m", "cache only", tree)
}

// refWriteShape is one request that stores a commit of the body in a ref.
type refWriteShape struct {
	name, path string
	body       func(sha string) map[string]any
	ref        string // the ref that the request would create
}

// refWriteShapes lists the requests that store a commit that the request names
// in a ref: the base of a branch (named, as a fallback, forced, ephemeral) and
// the target of a tag.
func refWriteShapes() []refWriteShape {
	return []refWriteShape{
		{"branch base_ref", "/api/v1/repos/branches/create", func(sha string) map[string]any {
			return map[string]any{"target_branch": "co", "base_ref": sha}
		}, "refs/heads/co"},
		{"branch preferred_base", "/api/v1/repos/branches/create", func(sha string) map[string]any {
			return map[string]any{"target_branch": "co", "base_ref": "nosuch", "preferred_base": sha}
		}, "refs/heads/co"},
		{"branch force", "/api/v1/repos/branches/create", func(sha string) map[string]any {
			return map[string]any{"target_branch": "feature", "base_ref": sha, "force": true}
		}, "refs/heads/feature"},
		{"ephemeral branch", "/api/v1/repos/branches/create", func(sha string) map[string]any {
			return map[string]any{"target_branch": "co", "base_ref": sha, "base_is_ephemeral": true, "target_is_ephemeral": true}
		}, "refs/namespaces/ephemeral/refs/heads/co"},
		{"tag", "/api/v1/repos/tags", func(sha string) map[string]any {
			return map[string]any{"name": "co", "ref": sha}
		}, "refs/tags/co"},
	}
}

func TestRefWritesRefuseACommitThatOnlyTheCacheHolds(t *testing.T) {
	shapes := refWriteShapes()
	runBackends(t, func(t *testing.T, e *testEnv) {
		for _, tc := range shapes {
			t.Run(tc.name, func(t *testing.T) {
				fx := wrefSeedFixture(t, e)
				repoPath, err := gitread.Materialize(context.Background(), e.refStore, e.blobStore, e.cacheDir, fx.repoID, "main")
				if err != nil {
					t.Fatal(err)
				}
				planted := plantCacheOnlyCommit(t, repoPath, fx.c2)
				srv := newWrefServer(t, e)
				before := wrefRefs(t, e, fx.repoID)

				resp, body := wrefDo(t, srv, http.MethodPost, tc.path, fx.token, tc.body(planted))
				if resp.StatusCode != http.StatusNotFound {
					t.Fatalf("a commit that only the cache holds: status %d, want 404, body %v", resp.StatusCode, body)
				}
				after := wrefRefs(t, e, fx.repoID)
				if len(after) != len(before) || after[tc.ref] != before[tc.ref] {
					t.Fatalf("refs changed after the refused request: before %v, after %v", before, after)
				}
				// Every other node hydrates from the registered packs alone.
				if _, err := gitread.Materialize(context.Background(), e.refStore, e.blobStore, t.TempDir(), fx.repoID, "main"); err != nil {
					t.Fatalf("a fresh node cannot hydrate the repository: %v", err)
				}

				// A commit that a registered pack holds still works.
				resp, body = wrefDo(t, srv, http.MethodPost, tc.path, fx.token, tc.body(fx.c1))
				if resp.StatusCode != http.StatusOK {
					t.Fatalf("a commit that a pack holds: status %d, want 200, body %v", resp.StatusCode, body)
				}
				if got := wrefRefs(t, e, fx.repoID)[tc.ref]; got != fx.c1 {
					t.Fatalf("%s = %q, want %s", tc.ref, got, fx.c1)
				}
			})
		}
	})
}

// countsPackListings counts the listings of the registry's packs.
type countsPackListings struct {
	ref.Store
	n atomic.Int64
}

func (c *countsPackListings) ListPacks(ctx context.Context, repo string) ([]ref.Pack, error) {
	c.n.Add(1)
	return c.Store.ListPacks(ctx, repo)
}

// The registry is read only for a request that is about to write a ref. A
// writer with a token can send the same request again and again, and a request
// that the ref policy denies, that names a tag or a branch that exists, or that
// leaves a branch where it is, must not cost a registry read, a view of the
// cache and a walk of its history.
func TestRefWritesReadTheRegistryOnlyBeforeAWrite(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := wrefSeedFixture(t, e)
		if _, err := gitread.Materialize(context.Background(), e.refStore, e.blobStore, e.cacheDir, fx.repoID, "main"); err != nil {
			t.Fatal(err)
		}
		counter := &countsPackListings{Store: e.refStore}
		e.refStore = counter
		srv := newWrefServer(t, e)
		denyAll := e.mint(t, tokenSpec{iss: fx.org, repo: fx.url,
			scopes: []string{auth.ScopeGitRead, auth.ScopeGitWrite}, refs: `[["refs/*", ["no-push"]]]`})

		for _, tc := range []struct {
			name   string
			path   string
			token  string
			body   map[string]any
			status int
		}{
			{"policy denies a branch", "/api/v1/repos/branches/create", denyAll, map[string]any{"target_branch": "denied", "base_ref": fx.c1}, http.StatusForbidden},
			{"policy denies a tag", "/api/v1/repos/tags", denyAll, map[string]any{"name": "denied", "ref": fx.c1}, http.StatusForbidden},
			{"the tag exists", "/api/v1/repos/tags", fx.token, map[string]any{"name": "annot", "ref": fx.c1}, http.StatusConflict},
			{"the branch is already at the base", "/api/v1/repos/branches/create", fx.token, map[string]any{"target_branch": "feature", "base_ref": fx.c1, "force": true}, http.StatusOK},
			{"a plain create of a branch that exists", "/api/v1/repos/branches/create", fx.token, map[string]any{"target_branch": "feature", "base_ref": fx.c2}, http.StatusConflict},
		} {
			before := counter.n.Load()
			resp, body := wrefDo(t, srv, http.MethodPost, tc.path, tc.token, tc.body)
			if resp.StatusCode != tc.status {
				t.Fatalf("%s: status %d, want %d, body %v", tc.name, resp.StatusCode, tc.status, body)
			}
			if got := counter.n.Load() - before; got != 0 {
				t.Errorf("%s: the request listed the registry's packs %d times, want 0", tc.name, got)
			}
		}

		// A request that writes asks the registry.
		before := counter.n.Load()
		resp, body := wrefDo(t, srv, http.MethodPost, "/api/v1/repos/tags", fx.token, map[string]any{"name": "fresh", "ref": fx.c1})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("a new tag: status %d, body %v", resp.StatusCode, body)
		}
		if counter.n.Load() == before {
			t.Error("a request that writes a ref did not ask the registry")
		}
	})
}

// A base that only the cache holds names no usable commit, so a registered
// preferred_base is still the fallback.
func TestBranchCreateFallsBackToPreferredBaseWhenTheBaseIsOnlyInTheCache(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := wrefSeedFixture(t, e)
		repoPath, err := gitread.Materialize(context.Background(), e.refStore, e.blobStore, e.cacheDir, fx.repoID, "main")
		if err != nil {
			t.Fatal(err)
		}
		planted := plantCacheOnlyCommit(t, repoPath, fx.c2)
		srv := newWrefServer(t, e)
		resp, body := wrefDo(t, srv, http.MethodPost, "/api/v1/repos/branches/create", fx.token,
			map[string]any{"target_branch": "fallback", "base_ref": planted, "preferred_base": fx.c1})
		if resp.StatusCode != http.StatusOK || body["commit_sha"] != fx.c1 {
			t.Fatalf("status %d, body %v, want 200 at the registered preferred_base %s", resp.StatusCode, body, fx.c1)
		}
		if got := wrefRefs(t, e, fx.repoID)["refs/heads/fallback"]; got != fx.c1 {
			t.Fatalf("refs/heads/fallback = %q, want %s", got, fx.c1)
		}
	})
}
