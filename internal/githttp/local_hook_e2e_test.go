package githttp

import (
	"testing"

	"github.com/coder/openstorage/internal/meta"
)

// The external serve binary is essential: in-process fixtures dispatch hooks
// through this package's TestMain, which does not wire command-side eventing.
func TestLocalHookAcceptedPushRecordsIntent(t *testing.T) {
	if testBackend == backendPG {
		t.Skip("local hook scheduling contract")
	}
	ctx := t.Context()
	repo := "https://example.com/local-hook-intent.git"
	id := registerRepo(t, repo)
	if _, err := testMeta.SetRepoUpstream(ctx, meta.RepoUpstream{
		RepoID: id, Provider: "github", Owner: "acme", Name: "demo",
		UpstreamHost: "http://127.0.0.1:1", ForwardOnPush: true,
	}); err != nil {
		t.Fatal(err)
	}
	// No seed marker or active claim: boot recovery cannot manufacture the
	// trigger we are testing. The closed loopback endpoint avoids external upstream I/O.
	work := initWorkRepo(t)
	head := revParse(t, work, "HEAD")
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, id, token), "HEAD:refs/heads/main")
	if got := pgRefs(t, id)["refs/heads/main"]; got != head {
		t.Fatalf("authority main=%q, want accepted commit %q", got, head)
	}
	up, err := testMeta.GetRepoUpstream(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if up.PushPending == 0 && !up.PushSyncing && up.PushRunCount == 0 {
		t.Fatal("authority accepted the commit, but no durable push intent or claimed run exists")
	}
}
