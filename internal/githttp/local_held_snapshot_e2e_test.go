package githttp

import (
	"context"
	"log/slog"
	"net/http/cgi"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref"
	upsync "github.com/coder/openstorage/internal/sync"
)

// discardSyncEvents drops the run's webhook events.
type discardSyncEvents struct{}

func (discardSyncEvents) Emit(context.Context, string, string, []byte) {}

// TestLocalForwardHeldStaleRefRestores replays verify-lane-schedule
// finding 2 on the real Hydrate, push holds, and Worker.RunPush. A REST
// write moves main from A to B in the authority only and records intent.
// A run claims it and passes its publication wait. Then a push whose
// advertisement predates the REST write (a forward-denied token, so it
// records nothing) reserves its hold on main, and its CAS later fails on
// the stale base. The run's loopback fetch must not forward the cache's
// held A: the lane keeps the request until a run can forward B.
func TestLocalForwardHeldStaleRefRestores(t *testing.T) {
	skipUnlessLocalSchedule(t)
	ctx := t.Context()
	metaURL, blobsURL := privateStores(t, t.TempDir())
	// The in-process server plays the serve node, so the seed pushes'
	// hook subprocesses inherit this process's environment.
	t.Setenv("OPENSTORAGE_PG_URL", metaURL)
	t.Setenv("OPENSTORAGE_S3_URL", blobsURL)
	st, err := ref.Open(ctx, metaURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.InitSchema(ctx); err != nil {
		t.Fatal(err)
	}
	bl, err := blob.Open(ctx, blobsURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bl.Close() })
	var clock atomic.Int64
	clock.Store(time.Now().UnixNano())
	ms, err := meta.Open(ctx, metaURL, meta.WithNowFunc(func() time.Time { return time.Unix(0, clock.Load()) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ms.Close)
	if err := ms.InitSchema(ctx); err != nil {
		t.Fatal(err)
	}
	key, pemBlock := genKeyPEM(t, "ES256")
	if _, err := ms.UpsertOrgWithKeys(ctx, testOrg, []string{pemBlock}); err != nil {
		t.Fatal(err)
	}
	repo, err := ms.CreateRepo(ctx, testOrg, "https://example.com/held-snapshot.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	id := repo.RepoID
	cacheDir := t.TempDir()
	srv := &Server{
		Store:    st,
		Blob:     bl,
		CacheDir: cacheDir,
		Verifier: auth.NewVerifier(memKeys{testOrg: {pemBlock}}),
		Org:      testOrg,
		ResolveRepo: func(_ context.Context, base string) (meta.Repo, bool) {
			return repo, base == id
		},
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	// main = A, and B (a child of A) on another branch, so B's objects are
	// stored and a REST write can point main at it.
	work := initWorkRepo(t)
	a := revParse(t, work, "HEAD")
	token := mintUnitToken(t, key, "ES256", unitClaims(testOrg, id, auth.ScopeGitRead, auth.ScopeGitWrite))
	gitURL := strings.Replace(ts.URL, "http://", "http://t:"+token+"@", 1) + "/" + id + ".git"
	mustGit(t, work, "push", gitURL, "HEAD:refs/heads/main")
	b := commitFile(t, work, "b.txt", "b\n", "b", nil)
	mustGit(t, work, "push", gitURL, "HEAD:refs/heads/other")

	// The upstream already has A.
	root := t.TempDir()
	bare := filepath.Join(root, "up", "mirror.git")
	mustGit(t, "", "init", "--bare", "-b", "main", bare)
	mustGit(t, bare, "config", "http.receivepack", "true")
	mustGit(t, work, "push", bare, a+":refs/heads/main")
	backend := filepath.Join(strings.TrimSpace(mustGit(t, "", "--exec-path")), "git-http-backend")
	upstream := httptest.NewServer(&cgi.Handler{Path: backend, Env: []string{
		"GIT_PROJECT_ROOT=" + root, "GIT_HTTP_EXPORT_ALL=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null",
	}})
	t.Cleanup(upstream.Close)
	if _, err := ms.SetRepoUpstream(ctx, meta.RepoUpstream{
		RepoID: id, Provider: "github", Owner: "up", Name: "mirror", UpstreamHost: upstream.URL, ForwardOnPush: true,
	}); err != nil {
		t.Fatal(err)
	}

	creds, ups := upsync.MetaStores(ms)
	lanes, ok := upsync.MetaLocalLanes(ms)
	if !ok {
		t.Fatalf("%T has no local schedule", ms)
	}
	var holdPath string
	w := &upsync.Worker{
		Upstreams:   ups,
		Credentials: creds,
		Repos: func(_ context.Context, _, repoID string) (upsync.RepoInfo, error) {
			return upsync.RepoInfo{RepoID: repoID, URL: repo.URL}, nil
		},
		Events: discardSyncEvents{},
		Loopback: upsync.Loopback{BaseURL: ts.URL, Token: func(_ context.Context, org string, r upsync.RepoInfo, ttl time.Duration, holder string) (string, error) {
			c := unitClaims(org, r.RepoID, auth.ScopeGitRead, auth.ScopeGitWrite)
			c.Exp, c.SyncClaim = time.Now().Add(ttl).Unix(), holder
			return auth.MintToken(key, "ES256", c)
		}},
		Logger:                 slog.New(slog.DiscardHandler),
		Local:                  lanes,
		PushEgressAllowPrivate: true,
		// The publication wait passes (nothing holds main yet). Then the
		// push reserves its hold, before the run's loopback fetch.
		WaitPublication: func(ctx context.Context, repoID string) error {
			if err := WaitForPushPublication(ctx, cacheDir, repoID); err != nil {
				return err
			}
			if holdPath == "" {
				holdPath = mintHold(t, RepoPath(cacheDir, repoID), "refs/heads/main")
			}
			return nil
		},
	}

	// The REST write: the authority CAS, then the intent.
	if won, err := st.CASRef(ctx, id, "refs/heads/main", a, b); err != nil || !won {
		t.Fatalf("REST write CAS = (%v, %v)", won, err)
	}
	if err := ms.RequestPush(ctx, id, false); err != nil {
		t.Fatal(err)
	}
	job := upsync.Job{Org: testOrg, RepoID: id}
	if err := w.RunPush(ctx, job); err != nil {
		t.Logf("run with the held ref: %v", err)
	}
	if holdPath == "" {
		t.Fatal("the run never reached its publication wait")
	}
	// The push's CAS fails on its stale base: it exits and its hold goes.
	removePushHoldFiles(holdPath)

	var up meta.RepoUpstream
	for range 4 {
		if up, err = ms.GetRepoUpstream(ctx, id); err != nil {
			t.Fatal(err)
		}
		if up.PushPending == 0 && !up.PushSyncing {
			break
		}
		clock.Add(time.Minute.Nanoseconds()) // past any restore or retry delay
		if err := w.RunPush(ctx, job); err != nil {
			t.Logf("follow-up run: %v", err)
		}
	}
	if up.PushPending != 0 || up.PushSyncing {
		t.Fatalf("lane did not settle: %+v", up)
	}
	if got := revParse(t, bare, "refs/heads/main"); got != b {
		t.Fatalf("lane idle with upstream main = %s, authority main = %s (cache held %s): the run forwarded the stale held snapshot and ended done, so the REST write is never forwarded", got, b, a)
	}
}
