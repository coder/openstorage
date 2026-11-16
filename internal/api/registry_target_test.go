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
	"testing"

	"github.com/coder/openstorage/internal/gitread"
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

func TestRefWritesRefuseACommitThatOnlyTheCacheHolds(t *testing.T) {
	type shape struct {
		name, path string
		body       func(sha string) map[string]any
		ref        string // the ref that the request would create
	}
	shapes := []shape{
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
