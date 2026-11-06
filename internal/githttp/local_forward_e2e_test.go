package githttp

import (
	"database/sql"
	"encoding/json"
	"net"
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref/s3wal/s3waltest"
	"github.com/coder/openstorage/internal/testutil/heldport"
)

// skipUnlessLocalSchedule skips a test of git-surface hook scheduling on
// PostgreSQL, which schedules through River instead of the local schedule
// (ADR-50 on SQLite, ADR-51 on S3WAL).
func skipUnlessLocalSchedule(t *testing.T) {
	t.Helper()
	if testBackend == backendPG {
		t.Skip("local schedule hook scheduling (ADR-50, ADR-51); PostgreSQL uses River")
	}
}

// localSchedulingStorage gives a test its own metadata authority and blob
// storage: TestMain's node must not consume these markers under a
// different egress policy or repair their claims.
func localSchedulingStorage(t *testing.T) (string, string) {
	t.Helper()
	assertf(testBackend == backendLocal || testBackend == backendS3WAL, "localSchedulingStorage: %s has no local schedule", testBackend)
	return privateStores(t, t.TempDir())
}

func TestLocalHookLiveForwardAndPendingRestart(t *testing.T) {
	skipUnlessLocalSchedule(t)
	root := t.TempDir()
	bare := filepath.Join(root, "up", "mirror.git")
	mustGit(t, "", "init", "--bare", "-b", "main", bare)
	mustGit(t, bare, "config", "http.receivepack", "true")
	backend := filepath.Join(strings.TrimSpace(mustGit(t, "", "--exec-path")), "git-http-backend")
	upstream := httptest.NewServer(&cgi.Handler{Path: backend, Env: []string{
		"GIT_PROJECT_ROOT=" + root, "GIT_HTTP_EXPORT_ALL=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null",
	}})
	defer upstream.Close()
	db, blobs := localSchedulingStorage(t)
	cache := t.TempDir()
	start := func() *testNode {
		n, err := startNode(cache, "OPENSTORAGE_PG_URL="+db, "OPENSTORAGE_S3_URL="+blobs, "OPENSTORAGE_SYNC_ALLOW_PRIVATE=true")
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	node := start()
	defer func() {
		if node != nil {
			node.stop()
		}
	}()
	ms, err := meta.Open(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Close()
	url := "https://example.com/owned-forward.git"
	token := mintTestToken(t, url, "repo:write,git:read,git:write", "")
	var created struct {
		RepoID string `json:"repo_id"`
	}
	if status := apiJSON(t, node, "POST", token, "/api/v1/repos", "{}", &created); status != http.StatusCreated || created.RepoID == "" {
		t.Fatalf("create: status=%d id=%q", status, created.RepoID)
	}
	id := created.RepoID
	if _, err := ms.SetRepoUpstream(t.Context(), meta.RepoUpstream{
		RepoID: id, Provider: "github", Owner: "up", Name: "mirror", UpstreamHost: upstream.URL, ForwardOnPush: true,
	}); err != nil {
		t.Fatal(err)
	}
	waitSettled := func(wantRuns int) meta.RepoUpstream {
		t.Helper()
		deadline := time.Now().Add(90 * time.Second)
		var up meta.RepoUpstream
		for time.Now().Before(deadline) {
			var err error
			up, err = ms.GetRepoUpstream(t.Context(), id)
			if err != nil {
				t.Fatal(err)
			}
			if up.PushSuccessCount >= wantRuns && !up.PushSyncing && up.PushPending == 0 && !up.Syncing && !up.SyncPending && up.SuccessCount >= wantRuns {
				return up
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("forward + mirror-back did not settle: %+v", up)
		return up
	}
	work := initWorkRepo(t)
	first := revParse(t, work, "HEAD")
	mustGit(t, work, "push", remoteURL(node, id, token), "HEAD:refs/heads/main")
	settled := waitSettled(1)
	if got := revParse(t, bare, "refs/heads/main"); got != first {
		t.Fatalf("upstream=%s want accepted=%s", got, first)
	}
	// A write-only caller can update locally, but must trigger neither a full
	// forward nor a pull which would overwrite that newly accepted commit.
	second := commitFile(t, work, "second.txt", "local only\n", "write-only", nil)
	writeOnly := mintTestToken(t, url, "git:write", "")
	mustGit(t, work, "push", remoteURL(node, id, writeOnly), "HEAD:refs/heads/main")
	time.Sleep(3 * time.Second) // multiple live-consumer ticks after first-run settlement
	up, err := ms.GetRepoUpstream(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if up.PushRunCount != settled.PushRunCount || up.RunCount != settled.RunCount || up.PushPending != 0 || up.SyncPending || up.PushSyncing || up.Syncing {
		t.Fatalf("denied forwarding produced work: %+v", up)
	}
	if got := revParse(t, bare, "refs/heads/main"); got != first {
		t.Fatalf("write-only token exported %s", got)
	}
	if got := lsRemoteSHA(t, "", remoteURL(node, id, token), "refs/heads/main"); got != second {
		t.Fatalf("accepted denied-forward write was reverted: main = %q, want %s", got, second)
	}
	// Restart tests the existing durable-pending contract, not persistence of
	// retry timers: seed precisely that marker while no worker can consume it.
	node.stop()
	node = nil
	if err := ms.RequestPush(t.Context(), id, false); err != nil {
		t.Fatal(err)
	}
	node = start()
	waitSettled(2)
	if got := revParse(t, bare, "refs/heads/main"); got != second {
		t.Fatalf("restart did not consume durable pending work: %s", got)
	}
	t.Logf("forwarded %s, denied write stayed local at %s, restart consumed the existing pending marker", first, second)
}

// Reproduce a real live consumer covering an authority CAS before receive-pack
// publishes that same ref in its cache. S3WAL uses owned SeaweedFS, not MemCAS.
func TestLocalHookForwardBeforeCachePublication(t *testing.T) {
	skipUnlessLocalSchedule(t)
	root := t.TempDir()
	bare := filepath.Join(root, "up", "mirror.git")
	mustGit(t, "", "init", "--bare", "-b", "main", bare)
	mustGit(t, bare, "config", "http.receivepack", "true")
	backend := filepath.Join(strings.TrimSpace(mustGit(t, "", "--exec-path")), "git-http-backend")
	upstream := httptest.NewServer(&cgi.Handler{Path: backend, Env: []string{
		"GIT_PROJECT_ROOT=" + root, "GIT_HTTP_EXPORT_ALL=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null",
	}})
	defer upstream.Close()
	db, blobs := localSchedulingStorage(t)
	barrier := filepath.Join(t.TempDir(), "publication")
	node, err := startNode(t.TempDir(), "OPENSTORAGE_PG_URL="+db, "OPENSTORAGE_S3_URL="+blobs,
		"OPENSTORAGE_SYNC_ALLOW_PRIVATE=true", EnvUpdateBarrier+"="+barrier)
	if err != nil {
		t.Fatal(err)
	}
	defer node.stop()
	// Unpark before stopping the node, including any failing assertion.
	defer func() { _ = os.WriteFile(barrier+".release", nil, 0o644) }()
	ms, err := meta.Open(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Close()
	token := mintTestToken(t, "https://example.com/publication.git", "repo:write,git:read,git:write", "")
	var created struct {
		RepoID string `json:"repo_id"`
	}
	if status := apiJSON(t, node, "POST", token, "/api/v1/repos", "{}", &created); status != http.StatusCreated || created.RepoID == "" {
		t.Fatalf("create: status=%d id=%q", status, created.RepoID)
	}
	id := created.RepoID
	if _, err := ms.SetRepoUpstream(t.Context(), meta.RepoUpstream{
		RepoID: id, Provider: "github", Owner: "up", Name: "mirror", UpstreamHost: upstream.URL, ForwardOnPush: true,
	}); err != nil {
		t.Fatal(err)
	}
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
	// Observe for 20 s (past the sweep's 15 s start delay): the loop exits
	// early only if the forward completed, which the checks below reject.
	// A correct consumer claims and stays parked in its publication wait.
	var before meta.RepoUpstream
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); {
		before, err = ms.GetRepoUpstream(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		if before.PushSuccessCount > 0 && !before.PushSyncing && before.PushPending == 0 && !before.Syncing && !before.SyncPending {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if before.PushPending != 0 || (before.PushClaimHolder == "" && before.PushSuccessCount == 0) {
		t.Fatalf("did not claim intent inside the controlled publication window: %+v", before)
	}
	// Run counts advance at completion, not claim admission.
	if before.PushRunCount != 0 || before.PushSuccessCount != 0 || !before.PushSyncing {
		t.Errorf("forward finished before cache publication: %+v", before)
	}
	if out := mustGit(t, bare, "for-each-ref", "--format=%(refname)"); strings.TrimSpace(out) != "" {
		t.Fatalf("fixture upstream was not empty before publication: %s", out)
	}
	t.Logf("paused after authority CAS: run_count=%d success=%d pending=%d upstream empty", before.PushRunCount, before.PushSuccessCount, before.PushPending)
	if err := os.WriteFile(barrier+".release", nil, 0o644); err != nil {
		t.Fatal(err)
	}
	res := <-done
	if res.err != nil {
		t.Fatalf("real push failed: %v\n%s", res.err, res.out)
	}
	if got := lsRemoteSHA(t, "", remoteURL(node, id, token), "refs/heads/main"); got != want {
		t.Fatalf("accepted cache ref missing after publication: main = %q, want %s", got, want)
	}
	// Observe actual upstream convergence, not just a successful metadata run.
	var got string
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		got = strings.TrimSpace(mustGit(t, bare, "for-each-ref", "--format=%(objectname)", "refs/heads/main"))
		if got == want {
			t.Logf("released cache publication: accepted=%s upstream=%s", want, got)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	after, err := ms.GetRepoUpstream(t.Context(), id)
	t.Fatalf("accepted=%s upstream=%q after released publication; metadata=%+v err=%v", want, got, after, err)
}

// toggledUpstream is a smart-HTTP upstream on a fixed address that can be
// taken down (connection refused: a transient push failure) and brought
// back. heldport keeps the port while it is down, so another server takes
// the port and answers in its place (a 404 there is a permanent push
// failure) only if it also sets SO_REUSEPORT and binds that exact port.
type toggledUpstream struct {
	addr    string
	handler http.Handler
	srv     *http.Server
}

func newToggledUpstream(t *testing.T, root string) *toggledUpstream {
	t.Helper()
	backend := filepath.Join(strings.TrimSpace(mustGit(t, "", "--exec-path")), "git-http-backend")
	u := &toggledUpstream{handler: &cgi.Handler{Path: backend, Env: []string{
		"GIT_PROJECT_ROOT=" + root, "GIT_HTTP_EXPORT_ALL=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null",
	}}}
	ln := heldport.Listen(t)
	u.addr = ln.Addr().String()
	u.serve(ln)
	t.Cleanup(func() {
		if u.srv != nil {
			_ = u.srv.Close()
		}
	})
	return u
}

func (u *toggledUpstream) serve(ln net.Listener) {
	u.srv = &http.Server{Handler: u.handler, ReadHeaderTimeout: 10 * time.Second}
	srv := u.srv
	go func() { _ = srv.Serve(ln) }()
}

func (u *toggledUpstream) down(t *testing.T) {
	t.Helper()
	if err := u.srv.Close(); err != nil {
		t.Fatal(err)
	}
	u.srv = nil
}

func (u *toggledUpstream) up(t *testing.T) {
	t.Helper()
	u.serve(heldport.Relisten(t, u.addr))
}

// readPushSchedule reads repoID's push retry position from the private
// store at dbURL: the SQLite row, or the S3WAL metadata document.
func readPushSchedule(t *testing.T, dbURL, repoID string) (attempt int, due int64) {
	t.Helper()
	u, err := url.Parse(dbURL)
	if err != nil {
		t.Fatal(err)
	}
	if u.Scheme == "sqlite" {
		db, err := sql.Open("sqlite", "file:"+u.Path+"?mode=ro&_pragma=busy_timeout(5000)")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		if err := db.QueryRowContext(t.Context(), `SELECT push_attempt, push_due FROM repo_upstreams WHERE repo_id = ?`, repoID).Scan(&attempt, &due); err != nil {
			t.Fatal(err)
		}
		return attempt, due
	}
	assertf(u.Scheme == "s3wal", "readPushSchedule: scheme %q", u.Scheme)
	client := s3waltest.RawS3Client(u.Query().Get("endpoint"), "us-east-1", s3AccessKey, s3SecretKey)
	obj, err := client.GetObject(t.Context(), &s3.GetObjectInput{Bucket: aws.String(u.Host), Key: aws.String("_wal/meta.json")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = obj.Body.Close() }()
	var doc struct {
		Upstreams map[string]struct {
			PushAttempt int   `json:"push_attempt"`
			PushDue     int64 `json:"push_due"`
		} `json:"upstreams"`
	}
	if err := json.NewDecoder(obj.Body).Decode(&doc); err != nil {
		t.Fatal(err)
	}
	lane, ok := doc.Upstreams[repoID]
	if !ok {
		t.Fatalf("metadata document has no upstream for %s", repoID)
	}
	return lane.PushAttempt, lane.PushDue
}

// TestLocalHookForwardBackoffSurvivesRestart: a git push to a forwarding
// repo while the upstream is down schedules a retry in the schedule row
// (attempt 1, due one backoff later). A serve restart keeps it and does not
// run early; an explicit push-upstream then expedites it and converges.
func TestLocalHookForwardBackoffSurvivesRestart(t *testing.T) {
	skipUnlessLocalSchedule(t)
	root := t.TempDir()
	bare := filepath.Join(root, "up", "mirror.git")
	mustGit(t, "", "init", "--bare", "-b", "main", bare)
	mustGit(t, bare, "config", "http.receivepack", "true")
	upstream := newToggledUpstream(t, root)
	db, blobs := localSchedulingStorage(t)
	cache := t.TempDir()
	start := func() *testNode {
		n, err := startNode(cache, "OPENSTORAGE_PG_URL="+db, "OPENSTORAGE_S3_URL="+blobs, "OPENSTORAGE_SYNC_ALLOW_PRIVATE=true")
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	node := start()
	defer func() {
		if node != nil {
			node.stop()
		}
	}()
	ms, err := meta.Open(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Close()
	url := "https://example.com/backoff-forward.git"
	token := mintTestToken(t, url, "repo:write,git:read,git:write", "")
	var created struct {
		RepoID string `json:"repo_id"`
	}
	if status := apiJSON(t, node, "POST", token, "/api/v1/repos", "{}", &created); status != http.StatusCreated || created.RepoID == "" {
		t.Fatalf("create: status=%d id=%q", status, created.RepoID)
	}
	id := created.RepoID
	if _, err := ms.SetRepoUpstream(t.Context(), meta.RepoUpstream{
		RepoID: id, Provider: "github", Owner: "up", Name: "mirror", UpstreamHost: "http://" + upstream.addr, ForwardOnPush: true,
	}); err != nil {
		t.Fatal(err)
	}
	schedule := func() (attempt int, due int64, up meta.RepoUpstream) {
		t.Helper()
		attempt, due = readPushSchedule(t, db, id)
		up, err := ms.GetRepoUpstream(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		return attempt, due, up
	}

	upstream.down(t)
	work := initWorkRepo(t)
	head := revParse(t, work, "HEAD")
	mustGit(t, work, "push", remoteURL(node, id, token), "HEAD:refs/heads/main")
	deadline := time.Now().Add(90 * time.Second)
	attempt, due, up := schedule()
	for attempt != 1 || up.PushSyncing {
		if time.Now().After(deadline) {
			t.Fatalf("hook push with the upstream down did not schedule a retry: attempt=%d due=%d %+v", attempt, due, up)
		}
		time.Sleep(100 * time.Millisecond)
		attempt, due, up = schedule()
	}
	if up.PushPending == 0 || due <= time.Now().UnixNano() {
		t.Fatalf("retry not pending in the future: pending=%d due=%d", up.PushPending, due)
	}
	runs := up.PushRunCount

	node.stop()
	node = start()
	time.Sleep(20 * time.Second) // past the sweep's start delay and several passes
	a2, d2, up2 := schedule()
	if a2 != 1 || d2 != due || up2.PushRunCount != runs || up2.PushPending == 0 {
		t.Fatalf("restart changed the backoff: attempt=%d due=%d runs=%d pending=%d; want attempt 1, due %d, runs %d",
			a2, d2, up2.PushRunCount, up2.PushPending, due, runs)
	}

	upstream.up(t)
	if status := apiJSON(t, node, "POST", token, "/api/v1/repos/push-upstream", "{}", nil); status != 202 {
		t.Fatalf("push-upstream status=%d", status)
	}
	deadline = time.Now().Add(60 * time.Second)
	upstreamMain := func() string {
		out, err := exec.Command("git", "-C", bare, "rev-parse", "--verify", "-q", "refs/heads/main").Output()
		if err != nil {
			return ""
		}
		return strings.TrimSpace(string(out))
	}
	for upstreamMain() != head {
		if time.Now().After(deadline) {
			t.Fatalf("explicit push-upstream after the restart did not converge: %+v", up2)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Logf("backoff attempt 1 due %d survived the restart; push-upstream forwarded %s", due, head)
}
