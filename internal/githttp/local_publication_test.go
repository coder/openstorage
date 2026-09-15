package githttp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"
)

func TestWaitForPushPublication(t *testing.T) {
	for _, outcome := range []string{"committed", "released", "canceled", "read_error"} {
		t.Run(outcome, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cache := t.TempDir()
				path := RepoPath(cache, "repo_publication")
				hold, err := mintPushHold(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := appendPushHolds(hold, []string{"refs/heads/main"}); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- WaitForPushPublication(ctx, cache, "repo_publication") }()
				synctest.Wait()
				select {
				case err := <-done:
					t.Fatalf("returned before cache publication: %v", err)
				default:
				}
				switch outcome {
				case "committed":
					if err := appendPushReleased(hold, []string{"refs/heads/main"}); err != nil {
						t.Fatal(err)
					}
				case "released":
					removePushHoldFiles(hold)
				case "canceled":
					cancel()
				case "read_error":
					removePushHoldFiles(hold)
					if err := os.RemoveAll(filepath.Join(path, pushHoldDir)); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(path, pushHoldDir), nil, 0o600); err != nil {
						t.Fatal(err)
					}
				}
				time.Sleep(25 * time.Millisecond)
				err = <-done
				switch outcome {
				case "canceled":
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("cancel: %v", err)
					}
				case "read_error":
					if err == nil {
						t.Fatal("unreadable holds authorized a snapshot")
					}
				default:
					if err != nil {
						t.Fatal(err)
					}
				}
			})
		})
	}
}
