package githttp

// End-to-end acceptance for issue #45: upstream-sync loopback pushes must
// keep working on EVERY node of a multi-node fleet, not just the most
// recently booted one. Two real serve processes share one database; a dumb
// HTTP upstream (git update-server-info + FileServer) feeds the sync worker.
// Before the fix, the later boot's org-key upsert deleted the earlier
// process's persisted loopback public key, so the earlier node's sync
// pushes 403'd. Now loopback keys are in-memory only (unioned into each
// process's verifier) and pushes terminate at the minting process's own
// listener.

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// syncUpstream is a dumb-HTTP git upstream the serve nodes can fetch from.
type syncUpstream struct {
	srv  *httptest.Server
	bare string // <root>/up/mirror.git
	work string // workdir clone pushing via file://
}

func newSyncUpstream(t *testing.T) *syncUpstream {
	t.Helper()
	root := t.TempDir()
	bare := filepath.Join(root, "up", "mirror.git")
	if err := os.MkdirAll(bare, 0o755); err != nil {
		t.Fatal(err)
	}
	mustGit(t, "", "init", "--bare", bare)
	work := filepath.Join(root, "work")
	mustGit(t, "", "init", "-b", "main", work)
	u := &syncUpstream{bare: bare, work: work}
	u.commitAndPublish(t, "seed")
	u.srv = httptest.NewServer(http.FileServer(http.Dir(root)))
	t.Cleanup(u.srv.Close)
	return u
}

// commitAndPublish adds one commit, pushes it to the bare repo, and
// refreshes the dumb-HTTP metadata. Returns the new head SHA.
func (u *syncUpstream) commitAndPublish(t *testing.T, msg string) string {
	t.Helper()
	if err := os.WriteFile(filepath.Join(u.work, "file.txt"), []byte(msg+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	mustGit(t, u.work, "add", ".")
	mustGit(t, u.work, "commit", "-m", msg)
	mustGit(t, u.work, "push", u.bare, "main")
	// Dumb HTTP serves from static metadata; refresh it after every push.
	mustGit(t, u.bare, "update-server-info")
	return revParse(t, u.work, "HEAD")
}

// createSyncedRepo creates a repo mirroring the upstream through the given
// node and returns its RepoID. The create enqueues the first sync run.
func createSyncedRepo(t *testing.T, n *testNode, u *syncUpstream, url string) string {
	t.Helper()
	body := fmt.Sprintf(`{"base_repo": {"provider": "github", "owner": "up", "name": "mirror", "operation": "sync", "upstream_host": %q}}`, u.srv.URL)
	var out struct {
		RepoID string `json:"repo_id"`
	}
	token := mintTestToken(t, url, "repo:write", "")
	if status := apiJSON(t, n, http.MethodPost, token, "/api/v1/repos", body, &out); status != http.StatusOK {
		t.Fatalf("create synced repo %q on %s: status %d", url, n.addr, status)
	}
	if out.RepoID == "" {
		t.Fatalf("create synced repo %q: empty repo_id", url)
	}
	return out.RepoID
}

// waitForMain polls the shared ref store until the repo's refs/heads/main
// reaches want. The generous deadline covers River pickup latency under
// CI load; failures dump the last observed refs.
func waitForMain(t *testing.T, repoID, want string) {
	t.Helper()
	deadline := time.After(90 * time.Second)
	for {
		refs, err := testStore.ListRefs(context.Background(), repoID)
		if err == nil && refs["refs/heads/main"] == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("repo %s never synced refs/heads/main to %s (refs: %v, err: %v)", repoID, want, refs, err)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// TestSync_LoopbackPushesSurviveLaterBoots is the two-node #45 acceptance:
// nodeA boots first (TestMain), nodeB boots later, and BOTH must complete
// sync runs — including nodeA, whose loopback key a pre-fix nodeB boot
// would have deleted. On the local lane the enqueuing node runs the sync
// in-process, making "nodeA performs the loopback push after nodeB booted"
// deterministic; on pg either node's River worker may run it, which the fix
// makes equally safe (mint + push stay inside one process).
func TestSync_LoopbackPushesSurviveLaterBoots(t *testing.T) {
	up := newSyncUpstream(t)
	head := up.commitAndPublish(t, "one")

	// Boot the later node BEFORE nodeA does any sync work.
	nodeB := startNodeT(t)

	// The earlier-booted node must sync: create (first run) + explicit
	// pull-upstream of a fresh upstream commit.
	repoA := createSyncedRepo(t, nodeA, up, "sync-a")
	waitForMain(t, repoA, head)

	head = up.commitAndPublish(t, "two")
	pullToken := mintTestToken(t, repoA, "git:write", "")
	if status := apiJSON(t, nodeA, http.MethodPost, pullToken, "/api/v1/repos/pull-upstream", "{}", nil); status != http.StatusAccepted {
		t.Fatalf("pull-upstream on nodeA: status %d, want 202", status)
	}
	waitForMain(t, repoA, head)

	// The later-booted node syncs too.
	repoB := createSyncedRepo(t, nodeB, up, "sync-b")
	waitForMain(t, repoB, head)

	// Loopback keys never reach the database: the org's registered keys
	// are exactly the two configured file keys, no matter how many nodes
	// have booted (issue #45's deleted-slot is gone entirely).
	pems, err := testMeta.PublicKeysForOrg(context.Background(), testOrg)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		string(mustPubPEM(orgES256Key)): true,
		string(mustPubPEM(orgEdKey)):    true,
	}
	if len(pems) != len(want) {
		t.Fatalf("org_public_keys has %d rows, want %d (file keys only): %v", len(pems), len(want), pems)
	}
	for _, pem := range pems {
		if !want[pem] {
			t.Fatalf("unexpected org key row (loopback key persisted?):\n%s", pem)
		}
	}
}
