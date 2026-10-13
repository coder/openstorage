package api

// Issue #388, PR 2: a repeat sync create of the same upstream answers 200.
// A complete repo schedules nothing; a pending one is finished through
// finishPendingSync with its stored settings. Other repeats stay 409.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	stdsync "sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/meta"
)

// syncBaseBody is pendingSyncBody's upstream in the base_repo sync form.
func syncBaseBody(ref string, forward bool) string {
	return fmt.Sprintf(`{"base_repo":{"provider":"gitea","owner":"o","name":"n","upstream_host":"upstream.invalid","ref":%q,"forward_pushes":%t}}`, ref, forward)
}

// retry is a create by a repo:write + git:write token: it may finish a
// pending create.
func (s *pendingSyncStack) retry(t *testing.T, name, body string) (int, string) {
	t.Helper()
	code, _, msg := s.do(t, "/api/v1/repos", s.token(t, name, "", auth.ScopeRepoWrite, auth.ScopeGitWrite), body)
	return code, msg
}

func TestSyncCreateRetry(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		t.Run("a complete repo answers 200 and schedules nothing", func(t *testing.T) {
			s := newPendingSyncStack(t, e, true)
			code, _, raw := s.create(t, "done", pendingSyncBody)
			var created createRepoResponse
			if err := json.Unmarshal([]byte(raw), &created); code != http.StatusCreated || err != nil {
				t.Fatalf("create = %d %q, %v; want 201", code, raw, err)
			}
			// Either form, any ref: the stored upstream is what counts.
			for _, b := range []string{pendingSyncBody, syncBaseBody("other", false)} {
				code, _, raw := s.create(t, "done", b)
				var got createRepoResponse
				if err := json.Unmarshal([]byte(raw), &got); code != http.StatusOK || err != nil ||
					got.RepoID != created.RepoID || got.Message != "Repository 'done' already exists" {
					t.Fatalf("repeat %q = %d %q, %v; want 200 for repo %s", b, code, raw, err, created.RepoID)
				}
			}
			s.expect(t, "repeats", "done", 1, 1, 1, meta.CreateStateComplete)
		})

		t.Run("a git:write retry finishes a pending create with the stored settings", func(t *testing.T) {
			s := newPendingSyncStack(t, e, true)
			s.createPending(t, "fix", `{"default_branch":"trunk","base_repo":{"provider":"gitea","owner":"o","name":"n","upstream_host":"upstream.invalid","ref":"a"}}`)
			if code, msg := s.retry(t, "fix", `{"default_branch":"other","upstream":{"provider":"gitea","owner":"o","name":"n","host":"upstream.invalid"}}`); code != http.StatusOK {
				t.Fatalf("retry = %d %q, want 200", code, msg)
			}
			s.expect(t, "retry", "fix", 2, 1, 1, meta.CreateStateComplete)
			if r, err := s.e.store.GetRepoByURL(context.Background(), s.org, "fix"); err != nil || r.DefaultBranch != "trunk" {
				t.Fatalf("repo = %+v, %v; want the stored default branch trunk", r, err)
			}
		})

		t.Run("a repo:write-only retry is refused until pull-upstream finishes it", func(t *testing.T) {
			// #388 desk decision (question 1, option A): 403 naming pull-upstream.
			s := newPendingSyncStack(t, e, true)
			s.createPending(t, "scope", pendingSyncBody)
			if code, _, msg := s.create(t, "scope", pendingSyncBody); code != http.StatusForbidden || !strings.Contains(msg, "pull-upstream") {
				t.Fatalf("repo:write-only retry = %d %q, want 403 naming pull-upstream", code, msg)
			}
			s.expect(t, "repo:write-only retry", "scope", 1, 0, 0, meta.CreateStatePending)
			if code, msg := s.pull(t, s.token(t, "scope", "", auth.ScopeGitWrite)); code != http.StatusAccepted {
				t.Fatalf("pull-upstream = %d %q, want 202", code, msg)
			}
			if code, _, msg := s.create(t, "scope", pendingSyncBody); code != http.StatusOK {
				t.Fatalf("repo:write-only retry after pull-upstream = %d %q, want 200", code, msg)
			}
			s.expect(t, "after pull-upstream", "scope", 2, 1, 1, meta.CreateStateComplete)
		})

		t.Run("concurrent identical creates complete once", func(t *testing.T) {
			s := newPendingSyncStack(t, e, true)
			var mu stdsync.Mutex
			codes := map[int]int{}
			var wg stdsync.WaitGroup
			for range 8 {
				wg.Go(func() {
					code, _ := s.retry(t, "conc", pendingSyncBody)
					mu.Lock()
					codes[code]++
					mu.Unlock()
				})
			}
			wg.Wait()
			if codes[http.StatusCreated] != 1 || codes[http.StatusOK] != 7 {
				t.Fatalf("statuses = %v, want one 201 and seven 200", codes)
			}
			if _, won, _ := s.q.counts(); won != 1 {
				t.Fatalf("won completions = %d, want 1", won)
			}
		})

		t.Run("a retry and pull-upstream on one pending create complete once", func(t *testing.T) {
			s := newPendingSyncStack(t, e, true)
			s.createPending(t, "race", pendingSyncBody)
			var pull, retry int
			var wg stdsync.WaitGroup
			wg.Go(func() { pull, _ = s.pull(t, s.token(t, "race", "", auth.ScopeGitWrite)) })
			wg.Go(func() { retry, _ = s.retry(t, "race", pendingSyncBody) })
			wg.Wait()
			if pull != http.StatusAccepted || retry != http.StatusOK {
				t.Fatalf("pull-upstream = %d, retry = %d; want 202 and 200", pull, retry)
			}
			if _, won, _ := s.q.counts(); won != 1 {
				t.Fatalf("won completions = %d, want 1", won)
			}
		})

		t.Run("an identical create while the first one schedules", func(t *testing.T) {
			// T4.1 in the #388 plan: the first create pauses before its
			// schedule transaction, so it holds no row lock, then fails.
			s := newPendingSyncStack(t, e, true)
			paused, release := make(chan struct{}), make(chan struct{})
			var first atomic.Bool
			s.q.setHook(func(string) error {
				if first.CompareAndSwap(false, true) {
					close(paused)
					<-release
					return errors.New("injected schedule failure")
				}
				return nil
			})
			firstCode := make(chan int, 1)
			go func() {
				code, _, _ := s.create(t, "mixed", pendingSyncBody)
				firstCode <- code
			}()
			select {
			case <-paused:
			case <-time.After(30 * time.Second):
				t.Fatal("the first create never reached its schedule")
			}
			if code, msg := s.retry(t, "mixed", pendingSyncBody); code != http.StatusOK {
				t.Fatalf("identical create = %d %q, want 200 after its own repair", code, msg)
			}
			if code, _, _ := s.create(t, "mixed", ""); code != http.StatusConflict {
				t.Fatalf("plain create = %d, want 409", code)
			}
			close(release)
			if code := <-firstCode; code != http.StatusServiceUnavailable {
				t.Fatalf("first create = %d, want 503", code)
			}
			s.expect(t, "after the race", "mixed", 2, 1, 1, meta.CreateStateComplete)
		})

		t.Run("incompatible creates answer 409", func(t *testing.T) {
			s := newPendingSyncStack(t, e, true)
			if code, _, msg := s.create(t, "fwd", syncBaseBody("", true)); code != http.StatusCreated {
				t.Fatalf("create = %d %q, want 201", code, msg)
			}
			for b, want := range map[string]int{
				syncBaseBody("", false): http.StatusConflict, // a 200 must not hide forwarding
				`{"upstream":{"provider":"gitea","owner":"o","name":"other","host":"upstream.invalid"}}`: http.StatusConflict,
				"":                     http.StatusConflict, // plain
				syncBaseBody("", true): http.StatusOK,
			} {
				if code, msg := s.retry(t, "fwd", b); code != want {
					t.Fatalf("repeat %q = %d %q, want %d", b, code, msg, want)
				}
			}
			if code, _, _ := s.create(t, "plain", ""); code != http.StatusCreated {
				t.Fatalf("plain create = %d, want 201", code)
			}
			// A legacy row with the same upstream: its create may never
			// have finished (a crashed old binary).
			legacy, err := s.e.store.CreateRepo(context.Background(), s.org, "legacy", "main")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.q.es.SetRepoUpstream(context.Background(), meta.RepoUpstream{RepoID: legacy.RepoID,
				Provider: "gitea", Owner: "o", Name: "n", UpstreamHost: "upstream.invalid"}); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"plain", "legacy"} {
				if code, msg := s.retry(t, name, pendingSyncBody); code != http.StatusConflict {
					t.Fatalf("sync create of %s = %d %q, want 409", name, code, msg)
				}
			}
			if calls, _, _ := s.q.counts(); calls != 1 {
				t.Fatalf("scheduler calls = %d, want 1 (the first create)", calls)
			}
		})
	})
}
