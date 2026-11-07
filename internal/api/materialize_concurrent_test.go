package api

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/githttp"
	"github.com/coder/openstorage/internal/gitread"
)

// maxBusyAttempts and busyRetryPause bound TestMaterializeColdConcurrent's
// retries of a read that found every git-read slot busy, and
// coldRequestTimeout bounds each request with its retries.
const (
	maxBusyAttempts    = 100
	busyRetryPause     = 50 * time.Millisecond
	coldRequestTimeout = 3 * time.Minute
)

// gitReadsBusyMsg is writeGitLimitErr's answer to gitread.ErrBusy: no
// git-read slot freed up in time, and git did not start.
const gitReadsBusyMsg = "server is busy running git reads; retry shortly"

// gitReadsBusy reports whether a response is that answer: 503 with
// Retry-After and the busy message. Any other 503 is a failure.
func gitReadsBusy(resp *http.Response, body []byte) bool {
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
		return false
	}
	var env errorEnvelope
	return json.Unmarshal(body, &env) == nil && env.Error == gitReadsBusyMsg
}

func TestMaterializeColdConcurrent(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		for _, writes := range []bool{true, false} {
			t.Run(fmt.Sprintf("writes=%t", writes), func(t *testing.T) {
				fx := wrefSeedFixture(t, e)
				repoPath := githttp.RepoPath(e.cacheDir, fx.repoID)
				if _, err := os.Stat(repoPath); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("fixture cache must be cold: %v", err)
				}
				start := make(chan struct{})
				var wg sync.WaitGroup
				for i := range 12 {
					wg.Go(func() {
						<-start
						ctx, cancel := context.WithTimeout(t.Context(), coldRequestTimeout)
						defer cancel()
						prefix := "/api/v1/repos"
						if i%2 != 0 {
							prefix = "/api/repos/" + url.PathEscape(fx.url)
						}
						method, path, payload := http.MethodGet, prefix+"/commit?ref=main", ""
						target := fmt.Sprintf("cold-%02d", i)
						if writes {
							method, path = http.MethodPost, prefix+"/branches/create"
							payload = fmt.Sprintf(`{"target_branch":%q,"base_branch":"main"}`, target)
						}
						var (
							resp *http.Response
							body []byte
						)
						for attempt := 1; ; attempt++ {
							req, err := http.NewRequestWithContext(ctx, method, e.srv.URL+path, strings.NewReader(payload))
							if err != nil {
								t.Error(err)
								return
							}
							req.Header.Set("Authorization", "Bearer "+fx.token)
							req.Header.Set("Content-Type", "application/json")
							resp, err = e.srv.Client().Do(req)
							if err != nil {
								t.Error(err)
								return
							}
							body, err = io.ReadAll(resp.Body)
							resp.Body.Close()
							if err != nil {
								t.Error(err)
								return
							}
							// Twelve reads share gitRepoSlots git-read slots. On a
							// busy host one can wait longer than gitReadSlotWait for
							// a slot and get the busy answer before git starts
							// (gitread.ErrBusy). That is load shedding, not a
							// materialization failure: retry it, as a client would.
							// The cap fails a slot that never frees.
							if writes || !gitReadsBusy(resp, body) || attempt == maxBusyAttempts {
								break
							}
							time.Sleep(busyRetryPause)
						}
						if resp.StatusCode != http.StatusOK {
							t.Errorf("%s %s: status %d: %s", method, path, resp.StatusCode, body)
							return
						}
						if writes {
							var got struct {
								CommitSHA string `json:"commit_sha"`
								Target    string `json:"target_branch"`
							}
							if err := json.Unmarshal(body, &got); err != nil || got.CommitSHA != fx.c2 || got.Target != target {
								t.Errorf("create %s: body %s, decode %v", target, body, err)
							}
						} else {
							var got getCommitResponse
							if err := json.Unmarshal(body, &got); err != nil || got.Commit.SHA != fx.c2 {
								t.Errorf("read main: body %s, decode %v", body, err)
							}
						}
					})
				}
				close(start)
				wg.Wait()
				if writes {
					refs := wrefRefs(t, e, fx.repoID)
					for i := range 12 {
						target := fmt.Sprintf("cold-%02d", i)
						if got := refs["refs/heads/"+target]; got != fx.c2 {
							t.Errorf("authority %s = %q, want %s", target, got, fx.c2)
						}
						resp, body := coreGet(t, e, http.MethodGet, "/api/v1/repos/commit?ref="+target, fx.token, nil)
						coreRequireStatus(t, resp, body, http.StatusOK)
						var got getCommitResponse
						coreDecode(t, body, &got)
						if got.Commit.SHA != fx.c2 {
							t.Errorf("read %s = %q, want %s", target, got.Commit.SHA, fx.c2)
						}
					}
				}
			})
		}
	})
}

// TestGitReadsBusyMatchesOnlyTheBusyAnswer: the cold-read retry above
// retries writeGitLimitErr's gitread.ErrBusy answer and no other 503.
func TestGitReadsBusyMatchesOnlyTheBusyAnswer(t *testing.T) {
	s := &server{logger: slog.New(slog.DiscardHandler)}
	answer := func(err error) (*http.Response, []byte) {
		rr := httptest.NewRecorder()
		if !s.writeGitLimitErr(rr, httptest.NewRequest(http.MethodGet, "/api/v1/repos/commit", nil), "read", err) {
			t.Fatalf("writeGitLimitErr did not answer %v", err)
		}
		return rr.Result(), rr.Body.Bytes()
	}
	if resp, body := answer(fmt.Errorf("resolve: %w", gitread.ErrBusy)); !gitReadsBusy(resp, body) {
		t.Fatalf("the ErrBusy answer (%d, Retry-After %q, %s) is not matched", resp.StatusCode, resp.Header.Get("Retry-After"), body)
	}
	if resp, body := answer(gitread.ErrTimeBudget); gitReadsBusy(resp, body) {
		t.Fatalf("the time-budget 503 (%s) is matched: a retry fails the same way", body)
	}
	rr := httptest.NewRecorder()
	rr.Header().Set("Retry-After", "1")
	writeErr(rr, http.StatusServiceUnavailable, "ref is busy")
	if gitReadsBusy(rr.Result(), rr.Body.Bytes()) {
		t.Fatal("another 503 with Retry-After is matched")
	}
}
