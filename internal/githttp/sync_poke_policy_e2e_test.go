package githttp

// Issue #421: a won ref write on a synced (pull-only) repo pokes an inbound
// sync. The sync mirror-pushes every upstream head and tag through the
// unpoliced loopback token, so a token whose ref policies carry any op must
// not trigger it implicitly, matching the explicit pull-upstream gate. The
// write itself stays local; an authorized trigger converges the mirror.

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/meta"
)

// syncPokeQuietWindow is how long a denied poke must stay silent. The
// unrestricted control of the same lane must mirror well inside it.
const syncPokeQuietWindow = 8 * time.Second

func TestSyncPokeRefPolicyGate(t *testing.T) {
	const restricted = `[["refs/heads/main",["no-force-push"]]]`
	for _, tc := range []struct {
		name, surface, refs string
		wantPoke            bool
	}{
		{"rest_unrestricted", "rest", "", true},
		{"rest_restricted", "rest", restricted, false},
		{"git_unrestricted", "git", "", true},
		{"git_restricted", "git", restricted, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			up := newSyncUpstream(t)
			url := runUnique("sync-poke-" + tc.name)
			id := createSyncedRepo(t, nodeA, up, url)
			waitForMain(t, id, revParse(t, up.work, "HEAD"))
			base := waitSyncSettled(t, id, 1)
			head := up.commitAndPublish(t, "upstream moved")

			token := mintTestToken(t, url, "git:read,git:write", tc.refs)
			switch tc.surface {
			case "rest":
				body := `{"base_ref":"main","target_branch":"agent"}`
				if status := apiJSON(t, nodeA, http.MethodPost, token, "/api/v1/repos/branches/create", body, nil); status != http.StatusOK {
					t.Fatalf("branch create: status %d", status)
				}
			case "git":
				mustGit(t, initWorkRepo(t), "push", remoteURL(nodeA, id, token), "HEAD:refs/heads/agent")
			}
			if _, ok := pgRefs(t, id)["refs/heads/agent"]; !ok {
				t.Fatal("the accepted write is missing locally")
			}

			if tc.wantPoke {
				start := time.Now()
				waitForMain(t, id, head)
				t.Logf("poke mirrored upstream in %v", time.Since(start).Round(time.Millisecond))
				return
			}
			time.Sleep(syncPokeQuietWindow)
			if up := getUpstream(t, id); up.RunCount != base.RunCount || up.Syncing || up.SyncPending {
				t.Fatalf("restricted write started a sync: runs %d -> %d, syncing=%v pending=%v",
					base.RunCount, up.RunCount, up.Syncing, up.SyncPending)
			}
			if got := pgRefs(t, id)["refs/heads/main"]; got == head {
				t.Fatal("restricted write mirrored the upstream")
			}
			// The next authorized trigger converges the mirror.
			pull := mintTestToken(t, url, "git:write", "")
			if status := apiJSON(t, nodeA, http.MethodPost, pull, "/api/v1/repos/pull-upstream", "{}", nil); status != http.StatusAccepted {
				t.Fatalf("pull-upstream: status %d", status)
			}
			waitForMain(t, id, head)
		})
	}
}

func getUpstream(t *testing.T, id string) meta.RepoUpstream {
	t.Helper()
	up, err := testMeta.GetRepoUpstream(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return up
}

// waitSyncSettled waits until runs completed sync runs exist and nothing is
// in flight or pending, so a later poke is not swallowed by the claim.
func waitSyncSettled(t *testing.T, id string, runs int) meta.RepoUpstream {
	t.Helper()
	for deadline := time.Now().Add(90 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		up := getUpstream(t, id)
		if up.RunCount >= runs && !up.Syncing && !up.SyncPending {
			return up
		}
		if time.Now().After(deadline) {
			t.Fatalf("sync never settled: %+v", up)
		}
	}
}
