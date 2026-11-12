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
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/meta"
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
	if status := apiJSON(t, n, http.MethodPost, token, "/api/v1/repos", body, &out); status != http.StatusCreated {
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

	// Fresh repo names per run: nodeA and the lane database outlive the
	// test, so fixed names would collide under -count>1 (issue #429).
	suffix := fmt.Sprint(time.Now().UnixNano())

	// Boot the later node BEFORE nodeA does any sync work.
	nodeB := startNodeT(t)

	// The earlier-booted node must sync: create (first run) + explicit
	// pull-upstream of a fresh upstream commit.
	repoA := createSyncedRepo(t, nodeA, up, "sync-a-"+suffix)
	waitForMain(t, repoA, head)

	// Deliberately NO wait for the create-triggered run's claim to clear
	// (issue #79): a pull-upstream that coalesces with a still-finishing
	// run must be observed anyway — the request marker survives to the
	// run's FinishSync and the finisher schedules the follow-up run.
	head = up.commitAndPublish(t, "two")
	pullToken := mintTestToken(t, repoA, "git:write", "")
	if status := apiJSON(t, nodeA, http.MethodPost, pullToken, "/api/v1/repos/pull-upstream", "{}", nil); status != http.StatusAccepted {
		t.Fatalf("pull-upstream on nodeA: status %d, want 202", status)
	}
	waitForMain(t, repoA, head)

	// The later-booted node syncs too.
	repoB := createSyncedRepo(t, nodeB, up, "sync-b-"+suffix)
	waitForMain(t, repoB, head)

	// Loopback keys never reach the database: the org's registered keys
	// are exactly the two configured file keys, no matter how many nodes
	// have booted (issue #45's deleted-slot is gone entirely).
	//
	// Read through a fresh handle, not testMeta (issue #429). On S3-WAL,
	// PublicKeysForOrg answers from an in-memory snapshot. testMeta's
	// snapshot dates from TestMain's secondOrg upsert, before nodeA
	// registered testOrg, and picks up other processes' writes only on
	// its 15s refresh. A new handle's first lookup reads the store.
	ms, err := meta.Open(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Close()
	pems, err := ms.PublicKeysForOrg(t.Context(), testOrg)
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

// TestNamespace_ImportRefusesSyncedRepo pins the third `+import` rule of
// code.storage docs/repos/imports: a repository with a sync base refuses an
// import push as a whole, so no imported ref reaches its upstream.
func TestNamespace_ImportRefusesSyncedRepo(t *testing.T) {
	up := newSyncUpstream(t)
	head := up.commitAndPublish(t, "one")
	repo := createSyncedRepo(t, nodeA, up, "import-synced-"+fmt.Sprint(time.Now().UnixNano()))
	waitForMain(t, repo, head)

	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	out, err := tryGit(work, "push", remoteURL(nodeA, repo+"+import", token), "HEAD:refs/heads/imported")
	if err == nil {
		t.Fatalf("import push to a synced repo succeeded:\n%s", out)
	}
	if !strings.Contains(out, importSyncedRefusal) {
		t.Fatalf("import push to a synced repo: want %q, got:\n%s", importSyncedRefusal, out)
	}
	refs, err := testStore.ListRefs(context.Background(), repo)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := refs["refs/heads/imported"]; ok || refs["refs/heads/main"] != head {
		t.Fatalf("refs after the refused import push = %v, want only main@%s", refs, head)
	}
}

// TestRefuseSyncedImportDecision pins pre-receive's import decision against
// the lane's store: no link passes, a link refuses with text the pusher
// sees, and a store that cannot answer fails closed with a hidden cause.
func TestRefuseSyncedImportDecision(t *testing.T) {
	ctx := context.Background()
	t.Setenv("OPENSTORAGE_PG_URL", dbURL)
	if err := refuseSyncedImport(ctx, registerRepo(t, "import-plain")); err != nil {
		t.Fatalf("repo without a link: %v", err)
	}

	es, ok := testMeta.(meta.EventingStore)
	if !ok {
		t.Fatalf("lane store %T is not a meta.EventingStore", testMeta)
	}
	linked, err := es.CreateLinkedRepo(ctx, testOrg, runUnique("import-linked"), "main",
		meta.RepoUpstream{Provider: "github", Owner: "up", Name: "mirror", UpstreamHost: "https://upstream.invalid"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = refuseSyncedImport(ctx, linked.RepoID)
	if err == nil || err.Error() != importSyncedRefusal || !ShownToPusher(err) {
		t.Fatalf("linked repo: err = %v (shown %v), want the shown refusal", err, ShownToPusher(err))
	}

	t.Setenv("OPENSTORAGE_PG_URL", "unsupported://store")
	if err := refuseSyncedImport(ctx, linked.RepoID); err == nil || ShownToPusher(err) {
		t.Fatalf("unreachable store: err = %v (shown %v), want a hidden error", err, err != nil && ShownToPusher(err))
	}
}
