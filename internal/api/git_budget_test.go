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

// withGitReadPolicy swaps the process-wide read-side git policy for one
// test (api tests are sequential).
func withGitReadPolicy(t *testing.T, p gitReadPolicy) *gitReadPolicy {
	t.Helper()
	old := gitReads
	gitReads = p
	t.Cleanup(func() { gitReads = old })
	return &gitReads
}

// testGitReadPolicy is the production policy shape with test-sized knobs.
func testGitReadPolicy(slots int, timeout time.Duration, output int64, limits gitread.Limits) gitReadPolicy {
	return gitReadPolicy{
		slots:          gitread.NewSlots(slots, 0),
		archiveSlots:   gitread.NewSlots(slots, 0),
		timeout:        timeout,
		archiveTimeout: timeout,
		outputBytes:    output,
		limits:         limits,
	}
}

func requireSlotsFree(t *testing.T, p *gitReadPolicy) {
	t.Helper()
	if n := p.slots.InUse() + p.archiveSlots.InUse(); n != 0 {
		t.Fatalf("%d git read slots still held after the responses", n)
	}
}

// deepFixture seeds a repo whose main branch is a linear history of n
// commits: cold.txt exists only in the root commit, hot.txt changes in
// every commit (the #313 attack shape, scaled down).
type deepFixture struct {
	token     string
	tip, root string
}

func seedDeepHistory(t *testing.T, e *testEnv, org string, n int) deepFixture {
	t.Helper()
	url := "https://example.com/" + org + "/deep-" + randomHex(t, 4)
	repo, err := e.store.CreateRepo(context.Background(), org, url, "main")
	if err != nil {
		t.Fatalf("create repo: %v", err)
	}
	dir := t.TempDir()
	coreGit(t, dir, "init", "-q", "-b", "main", ".")
	var in strings.Builder
	for i := range n {
		fmt.Fprintf(&in, "commit refs/heads/main\nmark :%d\ncommitter D <d@example.com> %d +0000\ndata 2\nc\n", i+1, 1700000000+i)
		if i > 0 {
			fmt.Fprintf(&in, "from :%d\n", i)
		} else {
			in.WriteString("M 100644 inline cold.txt\ndata 5\ncold\n")
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
	root := strings.TrimSpace(coreGit(t, dir, "rev-list", "--max-parents=0", "main"))
	coreSeedStores(t, e, repo.RepoID, dir, map[string]string{"refs/heads/main": tip})
	return deepFixture{
		token: e.mint(t, tokenSpec{iss: org, repo: url, scopes: []string{auth.ScopeGitRead}}),
		tip:   tip, root: root,
	}
}

func requireLimitStatus(t *testing.T, resp *http.Response, body []byte, want int, retryAfter bool, msgPart string) {
	t.Helper()
	coreRequireStatus(t, resp, body, want)
	if got := resp.Header.Get("Retry-After"); (got != "") != retryAfter {
		t.Fatalf("Retry-After = %q, want present=%v", got, retryAfter)
	}
	if !strings.Contains(string(body), msgPart) {
		t.Fatalf("body %s does not mention %q", body, msgPart)
	}
}

func skipWithoutHeapCap(t *testing.T) {
	t.Helper()
	if !gitread.HeapLimitSupported() {
		t.Skip("git heap caps are enforced on Linux only")
	}
}

func TestGitReadBudgetDeepHistory(t *testing.T) {
	skipWithoutHeapCap(t)
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		fx := seedDeepHistory(t, e, org, 20000)
		small := gitread.Limits{HeavyHeap: 4 << 20, LightHeap: gitread.LightHeapBytes, MaxBlob: gitread.MaxBlobBytes}

		t.Run("commits deep skip hits the heap cap", func(t *testing.T) {
			p := withGitReadPolicy(t, testGitReadPolicy(2, time.Minute, gitread.DefaultOutputBytes, small))
			cursor := coreCursorEncode(cursorKindCommits, fx.tip, "19990")
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/commits?cursor="+cursor, fx.token, nil)
			requireLimitStatus(t, resp, body, http.StatusUnprocessableEntity, false, "memory")
			requireSlotsFree(t, p)
			// The service survives: a shallow page still works.
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/commits?limit=5", fx.token, nil)
			coreRequireStatus(t, resp, body, http.StatusOK)
			requireSlotsFree(t, p)
		})

		t.Run("files/metadata per-file walk hits the heap cap", func(t *testing.T) {
			p := withGitReadPolicy(t, testGitReadPolicy(2, time.Minute, gitread.DefaultOutputBytes, small))
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/files/metadata", fx.token, nil)
			requireLimitStatus(t, resp, body, http.StatusUnprocessableEntity, false, "memory")
			requireSlotsFree(t, p)
		})

		t.Run("blame of a root-commit file hits the heap cap", func(t *testing.T) {
			withGitReadPolicy(t, testGitReadPolicy(2, time.Minute, gitread.DefaultOutputBytes, small))
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/blame?path=cold.txt", fx.token, nil)
			requireLimitStatus(t, resp, body, http.StatusUnprocessableEntity, false, "memory")
		})

		t.Run("buffered output over the request allowance", func(t *testing.T) {
			p := withGitReadPolicy(t, testGitReadPolicy(2, time.Minute, 8<<10, gitread.DefaultLimits()))
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/commits?limit=100", fx.token, nil)
			requireLimitStatus(t, resp, body, http.StatusUnprocessableEntity, false, "output")
			requireSlotsFree(t, p)
		})

		t.Run("default policy serves the same requests", func(t *testing.T) {
			cursor := coreCursorEncode(cursorKindCommits, fx.tip, "19990")
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/commits?cursor="+cursor, fx.token, nil)
			coreRequireStatus(t, resp, body, http.StatusOK)
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/files/metadata", fx.token, nil)
			coreRequireStatus(t, resp, body, http.StatusOK)
			requireSlotsFree(t, &gitReads)
		})
	})
}

func TestGitReadBudgetAdmissionAndDeadline(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		fx := coreSeedFixture(t, e, org)

		t.Run("saturated slots answer 503 with Retry-After", func(t *testing.T) {
			p := withGitReadPolicy(t, testGitReadPolicy(1, time.Minute, gitread.DefaultOutputBytes, gitread.DefaultLimits()))
			// Another request holds the only slot: it ran one Heavy child
			// and has not finished.
			holder := gitread.NewBudget(p.slots, time.Minute, gitread.DefaultOutputBytes)
			hctx := gitread.WithBudget(context.Background(), holder)
			if _, err := gitread.Run(hctx, gitread.Cmd{Class: gitread.Heavy, Dir: t.TempDir(), Args: []string{"version"}}); err != nil {
				t.Fatal(err)
			}
			for _, path := range []string{
				"/api/v1/repos/commits",
				"/api/v1/repos/files/metadata",
				"/api/v1/repos/diff?ref=main",
				"/api/v1/repos/merge/preview?source_branch=feature&target_branch=main",
			} {
				resp, body := coreGet(t, e, "GET", path, fx.token, nil)
				requireLimitStatus(t, resp, body, http.StatusServiceUnavailable, true, "busy")
			}
			holder.Close()
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/commits", fx.token, nil)
			coreRequireStatus(t, resp, body, http.StatusOK)
			requireSlotsFree(t, p)
		})

		t.Run("archives use their own slots", func(t *testing.T) {
			p := withGitReadPolicy(t, testGitReadPolicy(1, time.Minute, gitread.DefaultOutputBytes, gitread.DefaultLimits()))
			hold := func(slots *gitread.Slots) *gitread.Budget {
				b := gitread.NewBudget(slots, time.Minute, gitread.DefaultOutputBytes)
				if _, err := gitread.Run(gitread.WithBudget(context.Background(), b),
					gitread.Cmd{Class: gitread.Heavy, Dir: t.TempDir(), Args: []string{"version"}}); err != nil {
					t.Fatal(err)
				}
				return b
			}
			// Every archive slot is held (slow downloads): archives are
			// refused, other reads are not.
			archives := hold(p.archiveSlots)
			resp, body := e.doReq(t, "POST", "/api/v1/repos/archive", fx.token, []byte(`{}`), "")
			requireLimitStatus(t, resp, body, http.StatusServiceUnavailable, true, "busy")
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/commits", fx.token, nil)
			coreRequireStatus(t, resp, body, http.StatusOK)
			archives.Close()
			// And the other way round.
			reads := hold(p.slots)
			resp, body = e.doReq(t, "POST", "/api/v1/repos/archive", fx.token, []byte(`{}`), "")
			coreRequireStatus(t, resp, body, http.StatusOK)
			reads.Close()
			requireSlotsFree(t, p)
		})

		t.Run("spent time budget answers 503 without Retry-After", func(t *testing.T) {
			p := withGitReadPolicy(t, testGitReadPolicy(2, time.Nanosecond, gitread.DefaultOutputBytes, gitread.DefaultLimits()))
			for _, path := range []string{
				"/api/v1/repos/commits",
				"/api/v1/repos/merge/preview?source_branch=feature&target_branch=main",
			} {
				resp, body := coreGet(t, e, "GET", path, fx.token, nil)
				requireLimitStatus(t, resp, body, http.StatusServiceUnavailable, false, "time budget")
			}
			requireSlotsFree(t, p)
		})

		t.Run("blob over the size limit answers 422 before streaming", func(t *testing.T) {
			withGitReadPolicy(t, testGitReadPolicy(2, time.Minute, gitread.DefaultOutputBytes,
				gitread.Limits{HeavyHeap: gitread.HeavyHeapBytes, LightHeap: gitread.LightHeapBytes, MaxBlob: 4}))
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/file?path=README.md", fx.token, nil)
			requireLimitStatus(t, resp, body, http.StatusUnprocessableEntity, false, "size limit")
			resp, body = e.doReq(t, "POST", "/api/v1/repos/archive", fx.token, []byte(`{}`), "")
			requireLimitStatus(t, resp, body, http.StatusUnprocessableEntity, false, "size limit")
		})

		t.Run("ref resolution limit is not reported as not found", func(t *testing.T) {
			skipWithoutHeapCap(t)
			withGitReadPolicy(t, testGitReadPolicy(2, time.Minute, gitread.DefaultOutputBytes,
				gitread.Limits{HeavyHeap: gitread.HeavyHeapBytes, LightHeap: 64 << 10, MaxBlob: gitread.MaxBlobBytes}))
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/commits?ref=main", fx.token, nil)
			requireLimitStatus(t, resp, body, http.StatusUnprocessableEntity, false, "memory")
		})
	})
}
