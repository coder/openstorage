package api

import (
	"context"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/gitread"
)

// TestFilesMetadataOrderWalkUnderGitReadBudget pins #292's response policy
// on top of the #313 chokepoint: `order` is exact or the request fails with
// #313's documented status, never a 200 without `order`.
//
// The fixture makes the rank walk the only expensive child: every commit
// rewrites hot.txt, so the per-file `git log -1` stops at the tip, while
// `rev-list --topo-order` (without a commit-graph) reads all 20,000 commits
// before its first line (measured: about 1.5 MiB vs 7 MiB of heap).
func TestFilesMetadataOrderWalkUnderGitReadBudget(t *testing.T) {
	skipWithoutHeapCap(t)
	// The commit-graph is best-effort (ADR-47); the limits must hold
	// without one, so this test's git children ignore it.
	t.Setenv("GIT_CONFIG_COUNT", "1")
	t.Setenv("GIT_CONFIG_KEY_0", "core.commitGraph")
	t.Setenv("GIT_CONFIG_VALUE_0", "false")
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		url := "https://example.com/" + org + "/hot-" + randomHex(t, 4)
		repo, err := e.store.CreateRepo(context.Background(), org, url, "main")
		if err != nil {
			t.Fatalf("create repo: %v", err)
		}
		dir := t.TempDir()
		coreGit(t, dir, "init", "-q", "-b", "main", ".")
		var in strings.Builder
		const n = 20000
		for i := range n {
			fmt.Fprintf(&in, "commit refs/heads/main\nmark :%d\ncommitter D <d@example.com> %d +0000\ndata 2\nc\n", i+1, 1700000000+i)
			if i > 0 {
				fmt.Fprintf(&in, "from :%d\n", i)
			}
			fmt.Fprintf(&in, "M 100644 inline hot.txt\ndata %d\n%d\n", len(fmt.Sprint(i))+1, i)
		}
		cmd := exec.Command("git", "fast-import", "--quiet")
		cmd.Dir = dir
		cmd.Stdin = strings.NewReader(in.String())
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("fast-import: %v\n%s", err, out)
		}
		tip := coreRevParse(t, dir, "main")
		coreSeedStores(t, e, repo.RepoID, dir, map[string]string{"refs/heads/main": tip})
		token := e.mint(t, tokenSpec{iss: org, repo: url, scopes: []string{auth.ScopeGitRead}})

		t.Run("rank walk over the heap cap answers 422", func(t *testing.T) {
			small := gitread.Limits{HeavyHeap: 4 << 20, LightHeap: gitread.LightHeapBytes, MaxBlob: gitread.MaxBlobBytes}
			p := withGitReadPolicy(t, testGitReadPolicy(2, time.Minute, gitread.DefaultOutputBytes, small))
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/files/metadata", token, nil)
			requireLimitStatus(t, resp, body, http.StatusUnprocessableEntity, false, "memory")
			requireSlotsFree(t, p)
		})

		t.Run("default limits return the exact rank", func(t *testing.T) {
			p := withGitReadPolicy(t, testGitReadPolicy(2, time.Minute, gitread.DefaultOutputBytes, gitread.DefaultLimits()))
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/files/metadata", token, nil)
			coreRequireStatus(t, resp, body, http.StatusOK)
			var got listFilesMetadataResponse
			coreDecode(t, body, &got)
			if len(got.Files) != 1 || got.Files[0].LastCommitSHA != tip || got.Commits[tip].Order != 0 {
				t.Fatalf("files = %+v commits = %+v, want hot.txt last changed at the tip with order 0", got.Files, got.Commits)
			}
			requireSlotsFree(t, p)
		})
	})
}
