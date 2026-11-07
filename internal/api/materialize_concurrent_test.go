package api

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/githttp"
	"github.com/coder/openstorage/internal/gitread"
)

// TestMaterializeColdConcurrent (issue #235): twelve requests start together
// at one repository whose cache is cold, and every one of them is answered
// from the same correctly initialized cache.
//
// The test is about cache initialization, not admission control. The
// production read pool admits two git reads per repository and answers a
// third with 503 and Retry-After after one second (ADR-049), so a read that
// finds git slow, or a host busy, is shed by design: TestGitReadBudget* pins
// that. The burst gets a pool that admits all twelve for as long as the
// context lives, so no answer depends on how fast git runs.
func TestMaterializeColdConcurrent(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		for _, writes := range []bool{true, false} {
			t.Run(fmt.Sprintf("writes=%t", writes), func(t *testing.T) {
				burst := gitReads
				burst.slots = gitread.NewSlots(12, 12, time.Minute)
				withGitReadPolicy(t, burst)
				fx := wrefSeedFixture(t, e)
				repoPath := githttp.RepoPath(e.cacheDir, fx.repoID)
				if _, err := os.Stat(repoPath); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("fixture cache must be cold: %v", err)
				}
				ctx, cancel := context.WithTimeout(t.Context(), time.Minute) // bounds a hang, not git's speed
				defer cancel()
				start := make(chan struct{})
				var wg sync.WaitGroup
				for i := range 12 {
					wg.Go(func() {
						<-start
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
						req, err := http.NewRequestWithContext(ctx, method, e.srv.URL+path, strings.NewReader(payload))
						if err != nil {
							t.Error(err)
							return
						}
						req.Header.Set("Authorization", "Bearer "+fx.token)
						req.Header.Set("Content-Type", "application/json")
						resp, err := e.srv.Client().Do(req)
						if err != nil {
							t.Error(err)
							return
						}
						defer resp.Body.Close()
						body, err := io.ReadAll(resp.Body)
						if err != nil {
							t.Error(err)
							return
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
