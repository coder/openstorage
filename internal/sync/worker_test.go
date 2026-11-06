package sync

// Worker lifecycle tests over real git transfers: file:// upstreams and a
// file:// loopback bare repo keep the happy path hermetic; small
// httptest servers exercise the documented failure classifications.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	stdsync "sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestAuthGitEnvCredentialHelper runs real `git credential` against the
// authGitEnv environment: "get" answers with the given pair (an empty
// password included, issue #400), "store" and "erase" print nothing,
// inherited credential aliases never survive, and an empty username
// installs no helper at all. A repo-local helper never answers first.
func TestAuthGitEnvCredentialHelper(t *testing.T) {
	for _, name := range []string{
		"OPENSTORAGE_ASKPASS_USERNAME", "CODESTORAGE_ASKPASS_USERNAME",
		"OPENSTORAGE_ASKPASS_PASSWORD", "CODESTORAGE_ASKPASS_PASSWORD",
	} {
		t.Setenv(name, "inherited")
	}
	// A repo-local helper must not answer first: authGitEnv resets the
	// multi-valued credential.helper list before adding its own.
	repoDir := t.TempDir()
	testGit(t, repoDir, "init", "-q", ".")
	testGit(t, repoDir, "config", "credential.helper", "!f() { echo username=local; echo password=local; }; f")
	gitCredential := func(t *testing.T, env []string, action string) string {
		t.Helper()
		cmd := exec.CommandContext(t.Context(), "git", "credential", action)
		cmd.Dir = repoDir
		cmd.Env = env
		cmd.Stdin = strings.NewReader("protocol=https\nhost=forge.invalid\n\n")
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("git credential %s: %v", action, err)
		}
		return string(out)
	}
	for _, tc := range []struct{ user, pass string }{{"trusted-user", "trusted-password"}, {"tok-as-user", ""}} {
		env := authGitEnv(tc.user, tc.pass, "https")
		for _, entry := range env {
			if strings.Contains(entry, "STORAGE_ASKPASS_") && strings.HasSuffix(entry, "=inherited") {
				t.Fatal("inherited credential alias survived")
			}
			if strings.HasPrefix(entry, "CODESTORAGE_ASKPASS_") {
				t.Fatal("the credential helper reads only OPENSTORAGE_ASKPASS_*; no legacy copy may reach git")
			}
			if strings.HasPrefix(entry, "GIT_CONFIG_VALUE_") && tc.pass != "" && strings.Contains(entry, tc.pass) {
				t.Fatal("credential value reached git config (argv of the helper shell)")
			}
		}
		got := gitCredential(t, env, "fill")
		want := "protocol=https\nhost=forge.invalid\nusername=" + tc.user + "\npassword=" + tc.pass + "\n"
		if got != want {
			t.Fatalf("credential fill = %q, want %q", got, want)
		}
		for _, action := range []string{"approve", "reject"} {
			if out := gitCredential(t, env, action); out != "" {
				t.Fatalf("credential %s printed %q", action, out)
			}
		}
	}
	for _, entry := range authGitEnv("", "", "https") {
		if strings.HasPrefix(entry, "GIT_CONFIG_") && !strings.HasPrefix(entry, "GIT_CONFIG_GLOBAL=") && !strings.HasPrefix(entry, "GIT_CONFIG_SYSTEM=") ||
			strings.HasPrefix(entry, "OPENSTORAGE_ASKPASS_") || strings.HasPrefix(entry, "CODESTORAGE_ASKPASS_") {
			t.Fatalf("public upstream got a credential helper or credential: %q", entry)
		}
	}
}

func discardTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// eventRec records emitted events in order (thread-safe).
type eventRec struct {
	mu     stdsync.Mutex
	events []Event
	raws   []rawEvent
}

// rawEvent is one Emit call as delivered: the org it was emitted for, the
// X-Pierre-Event type, and the exact body bytes.
type rawEvent struct {
	org, eventType string
	body           []byte
}

func (r *eventRec) Emit(_ context.Context, org, eventType string, payload []byte) {
	var e Event
	if err := json.Unmarshal(payload, &e); err != nil {
		panic("eventRec: bad payload: " + err.Error())
	}
	if e.Type != eventType {
		panic("eventRec: payload type " + e.Type + " != " + eventType)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
	r.raws = append(r.raws, rawEvent{org: org, eventType: eventType, body: append([]byte(nil), payload...)})
}

func (r *eventRec) list() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Event(nil), r.events...)
}

func (r *eventRec) rawList() []rawEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]rawEvent(nil), r.raws...)
}

// testGit runs one git command with a hermetic env (host config masked,
// fixed identity), failing the test on error.
func testGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null",
		"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com",
		"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com",
		"GIT_TERMINAL_PROMPT=0",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

// seedUpstream creates root/{owner}/{name}.git with one commit on main and
// one tag, returning the head SHA.
func seedUpstream(t *testing.T, root, owner, name string) string {
	t.Helper()
	bare := filepath.Join(root, owner, name+".git")
	if err := os.MkdirAll(bare, 0o755); err != nil {
		t.Fatal(err)
	}
	testGit(t, root, "init", "--bare", "-b", "main", bare)
	work := t.TempDir()
	testGit(t, work, "init", "-b", "main")
	if err := os.WriteFile(filepath.Join(work, "f.txt"), []byte("v1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	testGit(t, work, "add", ".")
	testGit(t, work, "commit", "-m", "c1")
	testGit(t, work, "tag", "v1")
	testGit(t, work, "push", "file://"+bare, "main", "v1")
	return testGit(t, work, "rev-parse", "HEAD")
}

// workerFixture is one wired worker over MemStore with file:// endpoints.
type workerFixture struct {
	worker      *Worker
	store       *MemStore
	rec         *eventRec
	upstreamDir string // ProjectRoot-style dir behind file:// upstreams
	loopbackDir string // dir behind the loopback base URL
}

func newWorkerFixture(t *testing.T) *workerFixture {
	t.Helper()
	f := &workerFixture{
		store:       NewMemStore(),
		rec:         &eventRec{},
		upstreamDir: t.TempDir(),
		loopbackDir: t.TempDir(),
	}
	f.worker = &Worker{
		Upstreams:   f.store,
		Credentials: f.store,
		Repos: func(_ context.Context, org, repoID string) (RepoInfo, error) {
			return RepoInfo{RepoID: repoID, URL: "acme/" + repoID}, nil
		},
		Events: f.rec,
		Loopback: Loopback{
			BaseURL: "file://" + f.loopbackDir,
			Token:   func(context.Context, string, RepoInfo, time.Duration, string) (string, error) { return "", nil },
		},
		Logger:  discardTestLogger(),
		Timeout: time.Minute,
		// The hermetic fixture endpoints are file:// URLs, which the
		// production default ("http:https") deliberately blocks.
		AllowProtocol: "http:https:file",
		// Fixture upstreams that ARE network endpoints live on
		// 127.0.0.1 (httptest), which the egress guard's strict policy
		// denies; the escape hatch routes them through the vetting
		// proxy like an internal-forge deployment would (the pull
		// side's transitional serve default is also allow, issue #139).
		PushEgressAllowPrivate: true,
		PullEgressAllowPrivate: true,
	}
	return f
}

// addLoopbackRepo creates the bare repo the loopback push lands in.
func (f *workerFixture) addLoopbackRepo(t *testing.T, repoID string) string {
	t.Helper()
	bare := filepath.Join(f.loopbackDir, repoID+".git")
	testGit(t, f.loopbackDir, "init", "--bare", "-b", "main", bare)
	return bare
}

func (f *workerFixture) putUpstream(t *testing.T, repoID, owner, name, host string) {
	t.Helper()
	if _, err := f.store.PutUpstream(context.Background(), Upstream{
		RepoID: repoID, Provider: "gitea", Owner: owner, Name: name, UpstreamHost: host,
	}); err != nil {
		t.Fatal(err)
	}
}

func assertEventTypes(t *testing.T, events []Event, want ...string) {
	t.Helper()
	if len(events) != len(want) {
		t.Fatalf("got %d events (%+v), want %d (%v)", len(events), events, len(want), want)
	}
	for i, w := range want {
		if events[i].Type != w {
			t.Fatalf("event[%d].Type = %q, want %q (all: %+v)", i, events[i].Type, w, events)
		}
	}
}

func TestWorkerFirstSyncSucceeds(t *testing.T) {
	f := newWorkerFixture(t)
	headSHA := seedUpstream(t, f.upstreamDir, "own", "proj")
	loop := f.addLoopbackRepo(t, "repo_aa")
	f.putUpstream(t, "repo_aa", "own", "proj", "file://"+f.upstreamDir)

	if err := f.worker.Run(context.Background(), Job{Org: "acme", RepoID: "repo_aa"}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	events := f.rec.list()
	assertEventTypes(t, events, EventSyncStarted, EventSyncSucceeded)
	started, done := events[0], events[1]
	if started.RunCount != 0 || !started.IsFirstSync || started.StartedAt == "" || started.CompletedAt != "" {
		t.Fatalf("bad started event: %+v", started)
	}
	if done.RunCount != 1 || !done.IsFirstSync || done.CompletedAt == "" || done.Error != "" {
		t.Fatalf("bad succeeded event: %+v", done)
	}
	if done.Repository.ID != "repo_aa" || done.Repository.URL != "acme/repo_aa" {
		t.Fatalf("bad repository identity: %+v", done.Repository)
	}

	if got := testGit(t, f.loopbackDir, "-C", loop, "rev-parse", "refs/heads/main"); got != headSHA {
		t.Fatalf("loopback main = %s, want %s", got, headSHA)
	}
	if got := testGit(t, f.loopbackDir, "-C", loop, "rev-parse", "refs/tags/v1"); got != headSHA {
		t.Fatalf("loopback tag = %s, want %s", got, headSHA)
	}

	up, err := f.store.GetUpstream(context.Background(), "repo_aa")
	if err != nil {
		t.Fatal(err)
	}
	if up.RunCount != 1 || up.SuccessCount != 1 || up.Syncing {
		t.Fatalf("bad upstream state after run: %+v", up)
	}
}

// TestWorkerDefaultProtocolBlocksFile: under the production protocol
// allowlist (AllowProtocol == "" -> http:https), a file:// upstream row —
// however it got past API validation — must fail the fetch instead of
// ingesting local repositories.
func TestWorkerDefaultProtocolBlocksFile(t *testing.T) {
	f := newWorkerFixture(t)
	seedUpstream(t, f.upstreamDir, "own", "proj")
	f.addLoopbackRepo(t, "repo_pp")
	f.putUpstream(t, "repo_pp", "own", "proj", "file://"+f.upstreamDir)
	f.worker.AllowProtocol = "" // production default

	if err := f.worker.Run(context.Background(), Job{Org: "acme", RepoID: "repo_pp"}); err == nil {
		t.Fatal("Run succeeded fetching a file:// upstream under the default protocol allowlist")
	}
	events := f.rec.list()
	assertEventTypes(t, events, EventSyncStarted, EventSyncFailed)
	if events[1].Error != FailClone {
		t.Fatalf("error = %q, want %q", events[1].Error, FailClone)
	}
}

func TestWorkerSecondRunCountsAndForceUpdates(t *testing.T) {
	f := newWorkerFixture(t)
	seedUpstream(t, f.upstreamDir, "own", "proj")
	loop := f.addLoopbackRepo(t, "repo_bb")
	f.putUpstream(t, "repo_bb", "own", "proj", "file://"+f.upstreamDir)
	ctx := context.Background()

	if err := f.worker.Run(ctx, Job{Org: "acme", RepoID: "repo_bb"}); err != nil {
		t.Fatalf("run 1: %v", err)
	}
	// Rewrite upstream history (force-push) — the mirror must follow.
	bare := filepath.Join(f.upstreamDir, "own", "proj.git")
	work := t.TempDir()
	testGit(t, work, "init", "-b", "main")
	if err := os.WriteFile(filepath.Join(work, "g.txt"), []byte("v2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	testGit(t, work, "add", ".")
	testGit(t, work, "commit", "-m", "rewritten")
	testGit(t, work, "push", "--force", "file://"+bare, "main")
	newHead := testGit(t, work, "rev-parse", "HEAD")

	if err := f.worker.Run(ctx, Job{Org: "acme", RepoID: "repo_bb"}); err != nil {
		t.Fatalf("run 2: %v", err)
	}

	events := f.rec.list()
	assertEventTypes(t, events, EventSyncStarted, EventSyncSucceeded, EventSyncStarted, EventSyncSucceeded)
	if events[2].RunCount != 1 || events[2].IsFirstSync {
		t.Fatalf("bad second started event: %+v", events[2])
	}
	if events[3].RunCount != 2 || events[3].IsFirstSync {
		t.Fatalf("bad second succeeded event: %+v", events[3])
	}
	if got := testGit(t, f.loopbackDir, "-C", loop, "rev-parse", "refs/heads/main"); got != newHead {
		t.Fatalf("loopback main = %s, want force-updated %s", got, newHead)
	}
}

// TestWorkerPullKeepsNewerLocalRefsOnForwardOnPush (issue #90, Codex
// round 7): on a forward_on_push repo, LOCAL wins — an inbound pull whose
// fetch snapshot predates a just-accepted local push must not force-rewind
// it (the loopback push is fast-forward-only there), while genuine
// upstream additions still land. The refused refs are the converged
// outcome: the run succeeds.
func TestWorkerPullKeepsNewerLocalRefsOnForwardOnPush(t *testing.T) {
	f := newWorkerFixture(t)
	seedUpstream(t, f.upstreamDir, "own", "proj")
	// A genuine upstream addition: a feature branch the pull must import.
	bare := filepath.Join(f.upstreamDir, "own", "proj.git")
	fwork := t.TempDir()
	testGit(t, fwork, "init", "-b", "feature")
	if err := os.WriteFile(filepath.Join(fwork, "feat.txt"), []byte("feat\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	testGit(t, fwork, "add", ".")
	testGit(t, fwork, "commit", "-m", "feature c1")
	testGit(t, fwork, "push", "file://"+bare, "feature")
	featureHead := testGit(t, fwork, "rev-parse", "HEAD")

	// Local refs are NEWER than the upstream snapshot (the raced-push
	// shape: the pull fetched before a local push won its CAS).
	localHead := seedLocalRepo(t, f, "repo_lw")
	loop := filepath.Join(f.loopbackDir, "repo_lw.git")
	if _, err := f.store.PutUpstream(context.Background(), Upstream{
		RepoID: "repo_lw", Provider: "gitea", Owner: "own", Name: "proj",
		UpstreamHost: "file://" + f.upstreamDir, ForwardOnPush: true,
	}); err != nil {
		t.Fatal(err)
	}

	if err := f.worker.Run(context.Background(), Job{Org: "acme", RepoID: "repo_lw"}); err != nil {
		t.Fatalf("pull run: %v", err)
	}
	assertEventTypes(t, f.rec.list(), EventSyncStarted, EventSyncSucceeded)
	if got := testGit(t, f.loopbackDir, "-C", loop, "rev-parse", "refs/heads/main"); got != localHead {
		t.Fatalf("local main = %s, want preserved %s (stale pull rewound an accepted local push)", got, localHead)
	}
	if got := testGit(t, f.loopbackDir, "-C", loop, "rev-parse", "refs/heads/feature"); got != featureHead {
		t.Fatalf("local feature = %s, want imported %s", got, featureHead)
	}
}

// TestWorkerPullForwardOnPushPropagatesHookDeclines: local-wins tolerance
// is scoped to stale-snapshot rejections — a server-side hook decline
// (quota, persistence failure) on a forward_on_push repo means upstream
// refs were NOT installed and must fail the run, never emit
// repo.sync.succeeded.
func TestWorkerPullForwardOnPushPropagatesHookDeclines(t *testing.T) {
	f := newWorkerFixture(t)
	seedUpstream(t, f.upstreamDir, "own", "proj")
	loop := f.addLoopbackRepo(t, "repo_hd")
	hook := filepath.Join(loop, "hooks", "pre-receive")
	if err := os.WriteFile(hook, []byte("#!/bin/sh\necho quota exceeded >&2\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.PutUpstream(context.Background(), Upstream{
		RepoID: "repo_hd", Provider: "gitea", Owner: "own", Name: "proj",
		UpstreamHost: "file://" + f.upstreamDir, ForwardOnPush: true,
	}); err != nil {
		t.Fatal(err)
	}

	err := f.worker.Run(context.Background(), Job{Org: "acme", RepoID: "repo_hd"})
	if err == nil {
		t.Fatal("pull run succeeded although the loopback hook declined every ref")
	}
	events := f.rec.list()
	assertEventTypes(t, events, EventSyncStarted, EventSyncFailed)
	if events[1].Error != FailPush {
		t.Fatalf("error = %q, want %q", events[1].Error, FailPush)
	}
}

// TestHermeticGitEnvStripsInheritedGitVars: host environments export
// credential/config injectors (GIT_ASKPASS, GIT_SSH_COMMAND,
// GIT_CONFIG_COUNT/KEY/VALUE) that would run arbitrary helpers inside the
// worker's git subprocesses — a workspace-agent GIT_ASKPASS answering a
// 401 by phoning home through the push egress proxy misclassified an auth
// failure as a transient dial outage. Every inherited GIT_* must go; the
// controlled values must survive.
func TestHermeticGitEnvStripsInheritedGitVars(t *testing.T) {
	t.Setenv("GIT_ASKPASS", "/host/injected-helper")
	t.Setenv("GIT_CONFIG_COUNT", "1")
	t.Setenv("UNRELATED_VAR", "kept")

	got := map[string]string{}
	for _, kv := range hermeticGitEnv("http:https") {
		if k, v, ok := strings.Cut(kv, "="); ok {
			got[k] = v
		}
	}
	if v, ok := got["GIT_ASKPASS"]; ok {
		t.Fatalf("inherited GIT_ASKPASS survived: %q", v)
	}
	if _, ok := got["GIT_CONFIG_COUNT"]; ok {
		t.Fatal("inherited GIT_CONFIG_COUNT survived")
	}
	if got["UNRELATED_VAR"] != "kept" {
		t.Fatal("non-GIT_ environment was not preserved")
	}
	for k, want := range map[string]string{
		"GIT_TERMINAL_PROMPT": "0",
		"GIT_CONFIG_GLOBAL":   "/dev/null",
		"GIT_CONFIG_SYSTEM":   "/dev/null",
		"GIT_ALLOW_PROTOCOL":  "http:https",
	} {
		if got[k] != want {
			t.Fatalf("%s = %q, want %q", k, got[k], want)
		}
	}
}

func TestWorkerDetachedBeforeRunIsSilent(t *testing.T) {
	f := newWorkerFixture(t)
	if err := f.worker.Run(context.Background(), Job{Org: "acme", RepoID: "repo_cc"}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	assertEventTypes(t, f.rec.list()) // no events at all
}

// TestWorkerDeletedRepoOnRiverLane: a River sync run for a repository
// deleted while queued returns ErrRepoGone, so its job completes, and
// touches nothing.
func TestWorkerDeletedRepoOnRiverLane(t *testing.T) {
	f := newWorkerFixture(t)
	f.putUpstream(t, "repo_gone", "own", "proj", "file://"+f.upstreamDir)
	if err := f.store.RequestSync(context.Background(), "repo_gone"); err != nil {
		t.Fatal(err)
	}
	f.worker.Repos = func(context.Context, string, string) (RepoInfo, error) { return RepoInfo{}, ErrNotFound }
	if err := f.worker.Run(context.Background(), Job{Org: "acme", RepoID: "repo_gone"}); !errors.Is(err, ErrRepoGone) {
		t.Fatalf("Run = %v, want ErrRepoGone", err)
	}
	up, err := f.store.GetUpstream(context.Background(), "repo_gone")
	if err != nil {
		t.Fatal(err)
	}
	if !up.SyncPending || up.Syncing || len(f.rec.list()) != 0 {
		t.Fatalf("deleted repo's run touched the lane: %+v, %d events", up, len(f.rec.list()))
	}
	// The local schedule keeps its silent return: its sweep has no job to
	// complete. Run returns before it uses any lane method.
	f.worker.Local = unusedLocalLanes{}
	if err := f.worker.Run(context.Background(), Job{Org: "acme", RepoID: "repo_gone"}); err != nil {
		t.Fatalf("local Run = %v, want nil", err)
	}
}

// unusedLocalLanes is a LocalLanes whose methods must not be called.
type unusedLocalLanes struct{ LocalLanes }

// detachDuringFetch wraps the store so the detach re-check (GetUpstream
// after the fetch) observes a row deleted mid-run.
type detachDuringFetch struct {
	*MemStore
}

func (d *detachDuringFetch) GetUpstream(ctx context.Context, repoID string) (Upstream, error) {
	// Simulate DELETE /repos/base landing between fetch and push.
	_ = d.MemStore.DeleteUpstream(ctx, repoID)
	return d.MemStore.GetUpstream(ctx, repoID)
}

func TestWorkerDetachMidRunEmitsNoTerminalEvent(t *testing.T) {
	f := newWorkerFixture(t)
	seedUpstream(t, f.upstreamDir, "own", "proj")
	f.addLoopbackRepo(t, "repo_dd")
	f.putUpstream(t, "repo_dd", "own", "proj", "file://"+f.upstreamDir)
	f.worker.Upstreams = &detachDuringFetch{MemStore: f.store}

	if err := f.worker.Run(context.Background(), Job{Org: "acme", RepoID: "repo_dd"}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// started was emitted before the detach; no succeeded/failed after it.
	assertEventTypes(t, f.rec.list(), EventSyncStarted)
}

func TestWorkerOverlappingRunSkips(t *testing.T) {
	f := newWorkerFixture(t)
	seedUpstream(t, f.upstreamDir, "own", "proj")
	f.putUpstream(t, "repo_ee", "own", "proj", "file://"+f.upstreamDir)
	if _, _, err := f.store.BeginSync(context.Background(), "repo_ee", "other-holder", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := f.worker.Run(context.Background(), Job{Org: "acme", RepoID: "repo_ee"}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	assertEventTypes(t, f.rec.list()) // skipped silently
}

func TestWorkerConfigErrorFails(t *testing.T) {
	f := newWorkerFixture(t)
	if _, err := f.store.PutUpstream(context.Background(), Upstream{
		RepoID: "repo_ff", Provider: "gitea", Owner: "", Name: "proj", UpstreamHost: "file:///nope",
	}); err != nil {
		t.Fatal(err)
	}
	_ = f.worker.Run(context.Background(), Job{Org: "acme", RepoID: "repo_ff"})
	events := f.rec.list()
	assertEventTypes(t, events, EventSyncStarted, EventSyncFailed)
	if events[1].Error != FailConfig {
		t.Fatalf("error = %q, want %q", events[1].Error, FailConfig)
	}
	up, err := f.store.GetUpstream(context.Background(), "repo_ff")
	if err != nil {
		t.Fatal(err)
	}
	if up.RunCount != 1 || up.SuccessCount != 0 || up.Syncing {
		t.Fatalf("bad upstream state after failed run: %+v", up)
	}
}

func TestWorkerAuthFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("WWW-Authenticate", `Basic realm="up"`)
		http.Error(w, "auth required", http.StatusUnauthorized)
	}))
	defer srv.Close()

	f := newWorkerFixture(t)
	f.putUpstream(t, "repo_gg", "own", "proj", srv.URL)
	_ = f.worker.Run(context.Background(), Job{Org: "acme", RepoID: "repo_gg"})
	events := f.rec.list()
	assertEventTypes(t, events, EventSyncStarted, EventSyncFailed)
	if events[1].Error != FailAuth {
		t.Fatalf("error = %q, want %q", events[1].Error, FailAuth)
	}
}

func TestWorkerUnreachableUpstream(t *testing.T) {
	f := newWorkerFixture(t)
	// Port 1 is reserved and never listening: connection refused.
	f.putUpstream(t, "repo_hh", "own", "proj", "http://127.0.0.1:1")
	_ = f.worker.Run(context.Background(), Job{Org: "acme", RepoID: "repo_hh"})
	events := f.rec.list()
	assertEventTypes(t, events, EventSyncStarted, EventSyncFailed)
	if events[1].Error != FailUnreachable {
		t.Fatalf("error = %q, want %q", events[1].Error, FailUnreachable)
	}
}

// TestWorkerPrivateUpstreamDeniedInStrictMode: with the strict sync egress
// policy (PullEgressAllowPrivate off — today's explicit
// OPENSTORAGE_SYNC_ALLOW_PRIVATE=false, the announced future default), a
// loopback/private upstream_host is refused at dial time by the vetting
// proxy — the upstream is NEVER contacted — and the run fails as
// configuration: upstream_host is caller-linked data, so unguarded fetch
// egress would be an SSRF read/credential-echo primitive into the
// server's network (issue #139).
func TestWorkerPrivateUpstreamDeniedInStrictMode(t *testing.T) {
	var reached atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached.Store(true)
		http.Error(w, "unreachable", http.StatusTeapot)
	}))
	defer srv.Close()

	f := newWorkerFixture(t)
	f.putUpstream(t, "repo_ge", "own", "proj", srv.URL)
	f.worker.PullEgressAllowPrivate = false // strict policy

	if err := f.worker.Run(context.Background(), Job{Org: "acme", RepoID: "repo_ge"}); err == nil {
		t.Fatal("Run succeeded against a policy-denied private upstream")
	}
	if reached.Load() {
		t.Fatal("denied egress still reached the private upstream")
	}
	events := f.rec.list()
	assertEventTypes(t, events, EventSyncStarted, EventSyncFailed)
	if events[1].Error != FailConfig {
		t.Fatalf("error = %q, want %q", events[1].Error, FailConfig)
	}
}

// TestWorkerPrivateUpstreamAllowedByTransitionalDefault: with the
// allow-private pull policy (the transitional serve default this release,
// issue #139), the same private upstream IS dialed — through the vetting
// proxy — preserving released 0.6.x internal-forge deployments. The fetch
// still fails here (not a git server), but as an ordinary clone failure,
// never an egress denial.
func TestWorkerPrivateUpstreamAllowedByTransitionalDefault(t *testing.T) {
	var reached atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached.Store(true)
		http.Error(w, "not a git server", http.StatusNotFound)
	}))
	defer srv.Close()

	f := newWorkerFixture(t) // fixture default: PullEgressAllowPrivate on
	f.putUpstream(t, "repo_gf", "own", "proj", srv.URL)
	_ = f.worker.Run(context.Background(), Job{Org: "acme", RepoID: "repo_gf"})
	if !reached.Load() {
		t.Fatal("allow-private policy did not dial the private upstream")
	}
	events := f.rec.list()
	assertEventTypes(t, events, EventSyncStarted, EventSyncFailed)
	if events[1].Error != FailClone {
		t.Fatalf("error = %q, want %q", events[1].Error, FailClone)
	}
}

// TestWorkerHTTPSUpstreamDialFailureStaysUnreachable: a transient dial
// failure INSIDE the pull egress proxy's CONNECT arm (https upstream on a
// refused port) must keep the documented FailUnreachable classification —
// git only sees an opaque proxy HTTP error, so the classification rides
// the proxy's recorded dial failure, not stderr phrasing (issue #139).
func TestWorkerHTTPSUpstreamDialFailureStaysUnreachable(t *testing.T) {
	f := newWorkerFixture(t)
	f.putUpstream(t, "repo_gh", "own", "proj", "https://127.0.0.1:1")
	_ = f.worker.Run(context.Background(), Job{Org: "acme", RepoID: "repo_gh"})
	events := f.rec.list()
	assertEventTypes(t, events, EventSyncStarted, EventSyncFailed)
	if events[1].Error != FailUnreachable {
		t.Fatalf("error = %q, want %q", events[1].Error, FailUnreachable)
	}
}

func TestWorkerCloneFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	f := newWorkerFixture(t)
	f.putUpstream(t, "repo_ii", "own", "proj", srv.URL)
	_ = f.worker.Run(context.Background(), Job{Org: "acme", RepoID: "repo_ii"})
	events := f.rec.list()
	assertEventTypes(t, events, EventSyncStarted, EventSyncFailed)
	if events[1].Error != FailClone {
		t.Fatalf("error = %q, want %q", events[1].Error, FailClone)
	}
}

func TestWorkerPushFailure(t *testing.T) {
	f := newWorkerFixture(t)
	seedUpstream(t, f.upstreamDir, "own", "proj")
	// No loopback bare repo created: the push target does not exist.
	f.putUpstream(t, "repo_jj", "own", "proj", "file://"+f.upstreamDir)
	_ = f.worker.Run(context.Background(), Job{Org: "acme", RepoID: "repo_jj"})
	events := f.rec.list()
	assertEventTypes(t, events, EventSyncStarted, EventSyncFailed)
	if events[1].Error != FailPush {
		t.Fatalf("error = %q, want %q", events[1].Error, FailPush)
	}
}

func TestWorkerEmptyUpstreamSucceedsWithoutPush(t *testing.T) {
	f := newWorkerFixture(t)
	bare := filepath.Join(f.upstreamDir, "own", "empty.git")
	if err := os.MkdirAll(bare, 0o755); err != nil {
		t.Fatal(err)
	}
	testGit(t, f.upstreamDir, "init", "--bare", "-b", "main", bare)
	// Deliberately no loopback repo: a push attempt would fail the run.
	f.putUpstream(t, "repo_kk", "own", "empty", "file://"+f.upstreamDir)

	if err := f.worker.Run(context.Background(), Job{Org: "acme", RepoID: "repo_kk"}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	events := f.rec.list()
	assertEventTypes(t, events, EventSyncStarted, EventSyncSucceeded)
}

func TestClassifyFetchFailure(t *testing.T) {
	cases := []struct {
		stderr string
		want   string
	}{
		{"fatal: Authentication failed for 'http://x/'", FailAuth},
		{"fatal: could not read Username for 'http://x': terminal prompts disabled", FailAuth},
		{"remote: Invalid username or password.", FailAuth},
		{"fatal: unable to access 'http://x/': The requested URL returned error: 403", FailAuth},
		{"fatal: unable to access 'http://x/': Could not resolve host: nope.example", FailUnreachable},
		{"fatal: unable to access 'http://x/': Failed to connect to 127.0.0.1 port 1: Connection refused", FailUnreachable},
		{"fatal: unable to access 'http://x/': Failed to connect to 127.0.0.1:1 after 0 ms: Could not connect to server", FailUnreachable},
		{"fatal: unable to access 'http://x/': The requested URL returned error: 500", FailClone},
		{"fatal: repository 'http://x/' not found", FailClone},
		{"", FailClone},
	}
	for _, c := range cases {
		if got := classifyFetchFailure(c.stderr); got != c.want {
			t.Errorf("classifyFetchFailure(%q) = %q, want %q", c.stderr, got, c.want)
		}
	}
}

func TestCloneURL(t *testing.T) {
	cases := []struct {
		up   Upstream
		want string
	}{
		{Upstream{Owner: "o", Name: "n", UpstreamHost: "github.com"}, "https://github.com/o/n.git"},
		{Upstream{Owner: "o", Name: "n", UpstreamHost: "http://127.0.0.1:99"}, "http://127.0.0.1:99/o/n.git"},
		{Upstream{Owner: "o", Name: "n", UpstreamHost: "file:///tmp/root/"}, "file:///tmp/root/o/n.git"},
	}
	for _, c := range cases {
		if got := c.up.CloneURL(); got != c.want {
			t.Errorf("CloneURL(%+v) = %q, want %q", c.up, got, c.want)
		}
	}
}

func TestProviderHostDefaults(t *testing.T) {
	for provider, wantHost := range map[string]string{
		"github": "github.com", "gitlab": "gitlab.com", "bitbucket": "bitbucket.org",
		"codeberg": "codeberg.org", "sr.ht": "git.sr.ht", "gitea": "", "forgejo": "",
	} {
		host, known := ProviderHost(provider)
		if !known || host != wantHost {
			t.Errorf("ProviderHost(%q) = %q,%v, want %q,true", provider, host, known, wantHost)
		}
	}
	for _, provider := range []string{"code", "code.storage", "svn", ""} {
		if _, known := ProviderHost(provider); known {
			t.Errorf("ProviderHost(%q) unexpectedly known", provider)
		}
	}
}

func TestEventMarshalShape(t *testing.T) {
	e := Event{
		Type:        EventSyncFailed,
		Org:         "acme",
		Repository:  EventRepository{ID: "repo_x", URL: "acme/x", RepoName: "acme/x"},
		RunCount:    3,
		IsFirstSync: false,
		StartedAt:   "2026-03-17T10:00:00Z",
		CompletedAt: "2026-03-17T10:02:30Z",
		Error:       FailAuth,
	}
	var got map[string]any
	if err := json.Unmarshal(e.marshal(), &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"type":          "repo.sync.failed",
		"org":           "acme",
		"repository":    map[string]any{"id": "repo_x", "url": "acme/x", "repo_name": "acme/x"},
		"run_count":     float64(3),
		"is_first_sync": false,
		"started_at":    "2026-03-17T10:00:00Z",
		"completed_at":  "2026-03-17T10:02:30Z",
		"error":         "authentication failed",
	}
	for k, w := range want {
		g, ok := got[k]
		if !ok {
			t.Fatalf("payload missing key %q: %v", k, got)
		}
		switch wv := w.(type) {
		case map[string]any:
			gm, ok := g.(map[string]any)
			if !ok || len(gm) != len(wv) || gm["id"] != wv["id"] || gm["url"] != wv["url"] || gm["repo_name"] != wv["repo_name"] {
				t.Fatalf("payload[%q] = %v, want %v", k, g, w)
			}
		default:
			if g != w {
				t.Fatalf("payload[%q] = %v, want %v", k, g, w)
			}
		}
	}
	if len(got) != len(want) {
		t.Fatalf("payload has extra keys: %v", got)
	}

	// started events omit completed_at/error entirely.
	s := Event{
		Type:       EventSyncStarted,
		Org:        "acme",
		Repository: EventRepository{ID: "repo_x", URL: "acme/x", RepoName: "acme/x"},
		StartedAt:  "2026-03-17T10:00:00Z",
	}
	var sm map[string]any
	if err := json.Unmarshal(s.marshal(), &sm); err != nil {
		t.Fatal(err)
	}
	if _, ok := sm["completed_at"]; ok {
		t.Fatalf("started payload carries completed_at: %v", sm)
	}
	if _, ok := sm["error"]; ok {
		t.Fatalf("started payload carries error: %v", sm)
	}
}
