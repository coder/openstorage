package githttp

import (
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref"
)

// PostgreSQL forwards through River jobs, not the local schedule. These
// tests pin the two places where an accepted git push to a forward_on_push
// repository could miss the upstream on that lane: a run that snapshots
// the cache before the push publishes its ref, and a hook that dies after
// its authority CAS.

func skipUnlessPG(t *testing.T) {
	t.Helper()
	if testBackend != backendPG {
		t.Skipf("PostgreSQL forwarding through River (running the %s lane)", testBackend)
	}
}

// pgForwardFixture is one private PostgreSQL database, so only this test's
// node works its River jobs, plus a smart-HTTP upstream bare repository.
type pgForwardFixture struct {
	db, blobs string
	bare      string
	upstream  *httptest.Server
	ms        meta.Backend
}

func newPGForwardFixture(t *testing.T) *pgForwardFixture {
	t.Helper()
	root := t.TempDir()
	bare := filepath.Join(root, "up", "mirror.git")
	mustGit(t, "", "init", "--bare", "-b", "main", bare)
	mustGit(t, bare, "config", "http.receivepack", "true")
	backend := filepath.Join(strings.TrimSpace(mustGit(t, "", "--exec-path")), "git-http-backend")
	upstream := httptest.NewServer(&cgi.Handler{Path: backend, Env: []string{
		"GIT_PROJECT_ROOT=" + root, "GIT_HTTP_EXPORT_ALL=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null",
	}})
	t.Cleanup(upstream.Close)
	db, blobs := privateStores(t, t.TempDir())
	return &pgForwardFixture{db: db, blobs: blobs, bare: bare, upstream: upstream}
}

// start runs a node on the fixture's stores and opens a metadata handle
// once the node has created the schema.
func (f *pgForwardFixture) start(t *testing.T, extraEnv ...string) *testNode {
	t.Helper()
	env := append([]string{"OPENSTORAGE_PG_URL=" + f.db, "OPENSTORAGE_S3_URL=" + f.blobs, "OPENSTORAGE_SYNC_ALLOW_PRIVATE=true"}, extraEnv...)
	node, err := startNode(t.TempDir(), env...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(node.stop)
	if f.ms == nil {
		ms, err := meta.Open(t.Context(), f.db)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(ms.Close)
		f.ms = ms
	}
	return node
}

// forwardRepo creates a repository linked to the fixture upstream with
// forward_on_push and returns its id and a git:read+git:write token.
func (f *pgForwardFixture) forwardRepo(t *testing.T, node *testNode, url string) (string, string) {
	t.Helper()
	token := mintTestToken(t, url, "repo:write,git:read,git:write", "")
	var created struct {
		RepoID string `json:"repo_id"`
	}
	if status := apiJSON(t, node, "POST", token, "/api/v1/repos", "{}", &created); status != http.StatusCreated || created.RepoID == "" {
		t.Fatalf("create: status=%d id=%q", status, created.RepoID)
	}
	if _, err := f.ms.SetRepoUpstream(t.Context(), meta.RepoUpstream{
		RepoID: created.RepoID, Provider: "github", Owner: "up", Name: "mirror", UpstreamHost: f.upstream.URL, ForwardOnPush: true,
	}); err != nil {
		t.Fatal(err)
	}
	return created.RepoID, token
}

// waitUpstreamMain waits for the upstream's main to reach want.
func (f *pgForwardFixture) waitUpstreamMain(t *testing.T, id, want string, timeout time.Duration) {
	t.Helper()
	var got string
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		got = strings.TrimSpace(mustGit(t, f.bare, "for-each-ref", "--format=%(objectname)", "refs/heads/main"))
		if got == want {
			return
		}
	}
	up, err := f.ms.GetRepoUpstream(t.Context(), id)
	t.Fatalf("upstream main = %q, want accepted %s; metadata=%+v err=%v", got, want, up, err)
}

// TestPGHookForwardBeforeCachePublication is the River twin of
// TestLocalHookForwardBeforeCachePublication: the push parks after its
// authority CAS and before receive-pack publishes the cache ref. A run on
// the same node that claims the intent then must not snapshot the cache,
// whose held ref is still the old value, and end done.
func TestPGHookForwardBeforeCachePublication(t *testing.T) {
	skipUnlessPG(t)
	f := newPGForwardFixture(t)
	barrier := filepath.Join(t.TempDir(), "publication")
	node := f.start(t, EnvUpdateBarrier+"="+barrier)
	// Unpark before the node stops, including after a failing assertion.
	t.Cleanup(func() { _ = os.WriteFile(barrier+".release", nil, 0o644) })
	id, token := f.forwardRepo(t, node, "https://example.com/pg-publication.git")

	work := initWorkRepo(t)
	want := revParse(t, work, "HEAD")
	type pushResult struct {
		out string
		err error
	}
	done := make(chan pushResult, 1)
	go func() {
		out, err := tryGit(work, "push", remoteURL(node, id, token), "HEAD:refs/heads/main")
		done <- pushResult{out, err}
	}()
	waitForFile(t, barrier+".ready", 30*time.Second)
	// The intent is already a River job. Give the node's worker time to
	// take it while the push stays parked.
	var parked meta.RepoUpstream
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(25 * time.Millisecond) {
		var err error
		if parked, err = f.ms.GetRepoUpstream(t.Context(), id); err != nil {
			t.Fatal(err)
		}
		if parked.PushRunCount > 0 {
			break
		}
	}
	if parked.PushRunCount != 0 || parked.PushSuccessCount != 0 {
		t.Errorf("forward run finished before the push published its cache ref: %+v", parked)
	}
	if err := os.WriteFile(barrier+".release", nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if res := <-done; res.err != nil {
		t.Fatalf("push failed: %v\n%s", res.err, res.out)
	}
	f.waitUpstreamMain(t, id, want, 30*time.Second)
}

// TestPGHookForwardAfterLostCASOutcome: the update hook's CAS commits, then
// the hook loses the outcome (EnvCASDropHard: the connection is gone, so
// the recovery read fails too) and never reaches the code after the CAS,
// as when it dies there. The client sees a failed push, but the authority
// holds the new ref, so the upstream must get it.
func TestPGHookForwardAfterLostCASOutcome(t *testing.T) {
	skipUnlessPG(t)
	f := newPGForwardFixture(t)
	marker := filepath.Join(t.TempDir(), "cas-drop-consumed")
	node := f.start(t, EnvCASDropHard+"="+marker)
	id, token := f.forwardRepo(t, node, "https://example.com/pg-lost-cas.git")

	work := initWorkRepo(t)
	want := revParse(t, work, "HEAD")
	if out, err := tryGit(work, "push", remoteURL(node, id, token), "HEAD:refs/heads/main"); err == nil {
		t.Fatalf("push succeeded under the hard CAS drop knob:\n%s", out)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("the update hook never consumed the CAS drop knob: %v", err)
	}
	st, err := ref.Open(t.Context(), f.db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if got, err := st.GetRef(t.Context(), id, "refs/heads/main"); err != nil || got != want {
		t.Fatalf("authority main = %q (err %v), want the committed %s", got, err, want)
	}
	f.waitUpstreamMain(t, id, want, 30*time.Second)
}
