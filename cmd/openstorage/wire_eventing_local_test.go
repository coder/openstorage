package main

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/events"
	"github.com/coder/openstorage/internal/githttp"
	"github.com/coder/openstorage/internal/jobs"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref"
	"github.com/coder/openstorage/internal/ref/s3wal"
	upsync "github.com/coder/openstorage/internal/sync"
)

// SQLite and S3WAL serves get the durable local schedule (issue #266,
// ADR-51): the worker claims through the row, and the queue is the
// scheduler, not the in-process queue with boot recovery and retry timers.
func TestBuildLocalSyncQueueUsesDurableSchedule(t *testing.T) {
	t.Run("sqlite", func(t *testing.T) {
		ms, err := meta.OpenSQLite(t.Context(), "sqlite://"+filepath.Join(t.TempDir(), "meta.sqlite"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(ms.Close)
		if err := ms.InitSchema(t.Context()); err != nil {
			t.Fatal(err)
		}
		assertDurableLocalQueue(t, ms)
	})
	t.Run("s3wal", func(t *testing.T) {
		ms := meta.NewS3WAL(s3wal.NewMemCAS())
		t.Cleanup(ms.Close)
		assertDurableLocalQueue(t, ms)
	})
}

func assertDurableLocalQueue(t *testing.T, ms meta.Store) {
	t.Helper()
	logger := slog.New(slog.DiscardHandler)
	worker := &upsync.Worker{}
	deliverer := &events.Deliverer{Webhooks: events.MetaWebhookStore{Store: ms}, Logger: logger}
	queue, cleanup, err := buildLocalSyncQueue(ms, "org", t.TempDir(), worker, deliverer, nil, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if worker.Local == nil || worker.WaitPublication == nil {
		t.Fatal("worker has no local schedule or publication wait")
	}
	if q, ok := queue.(*jobs.Local); !ok || !q.TracksCreateState() {
		t.Fatalf("queue is %T, want *jobs.Local over the local schedule (it tracks create state)", queue)
	}
	if worker.Requeue != nil || worker.RequeuePush != nil {
		t.Fatal("worker still wires process-local requeue")
	}
	// The API's explicit triggers reach the schedule through these
	// optional extensions: pull-upstream offers without re-marking, and
	// push-upstream records with RequestPushNow.
	if _, ok := queue.(interface {
		OfferSync(ctx context.Context, job upsync.Job) error
	}); !ok {
		t.Fatalf("queue %T cannot offer an already recorded pull", queue)
	}
	_, ups := upsync.MetaStores(ms)
	if _, ok := ups.(interface {
		RequestPushNow(ctx context.Context, repoID string, force bool) error
	}); !ok {
		t.Fatalf("upstream store %T has no RequestPushNow", ups)
	}
}

// cancelAfterCAS reproduces a client disappearing AFTER the authority commit.
// The real SQLite CAS decides the write; cancellation must not skip the marker.
type cancelAfterCAS struct {
	ref.RefStore
	cancel context.CancelFunc
}

func (s cancelAfterCAS) CASRef(ctx context.Context, repo, name, old, next string) (bool, error) {
	won, err := s.RefStore.CASRef(ctx, repo, name, old, next)
	s.cancel()
	return won, err
}

func (s cancelAfterCAS) CASRefGen(ctx context.Context, repo, name, old, next string) (bool, int64, error) {
	won, gen, err := s.RefStore.CASRefGen(ctx, repo, name, old, next)
	s.cancel()
	return won, gen, err
}

// observeAtCAS calls observe the moment the authority CAS commits: a hook
// that serve's shutdown kills right there leaves the row observe reads.
type observeAtCAS struct {
	ref.RefStore
	observe func()
}

func (s observeAtCAS) CASRefGen(ctx context.Context, repo, name, old, next string) (bool, int64, error) {
	won, gen, err := s.RefStore.CASRefGen(ctx, repo, name, old, next)
	if won && err == nil {
		s.observe()
	}
	return won, gen, err
}

// errStoreBug is panicOnceAtCAS's panic value.
const errStoreBug = "store bug in the CAS"

// panicOnceAtCAS panics in its first CAS, before the inner store runs it,
// as a bug in a store would.
type panicOnceAtCAS struct {
	ref.RefStore
	done bool
}

func (s *panicOnceAtCAS) panicOnce() {
	if !s.done {
		s.done = true
		panic(errStoreBug)
	}
}

func (s *panicOnceAtCAS) CASRef(ctx context.Context, repo, name, old, next string) (bool, error) {
	s.panicOnce()
	return s.RefStore.CASRef(ctx, repo, name, old, next)
}

func (s *panicOnceAtCAS) CASRefGen(ctx context.Context, repo, name, old, next string) (bool, int64, error) {
	s.panicOnce()
	return s.RefStore.CASRefGen(ctx, repo, name, old, next)
}

// TestLocalHookIntentAuthorizationAndFailures (carried over from #271):
// on SQLite an accepted git-surface ref write records durable intent for
// the serve sweep, behind the same gates as the API. Forwarding needs
// git:read and no ref-policy operations; pulling needs no ref-policy
// operations; a sync run's own loopback push (claim env) records nothing;
// a live pull claim suppresses pull marking (C10, shared with PG).
//
// The forward intent is in the row before the authority CAS commits
// (verify-lane-schedule finding 1): a hook killed right after the CAS, or
// one whose metadata store fails, never leaves an accepted write that no
// run forwards. A failed intent write refuses the update instead.
func TestLocalHookIntentAuthorizationAndFailures(t *testing.T) {
	const rw = `["git:read","git:write"]`
	for _, tc := range []struct {
		name, scopes, refs, ops, claim            string
		forward, syncing, disconnect, storeClosed bool
		pullEndsAtCAS                             bool
		wantPull, wantPush, wantRefused           bool
	}{
		{name: "forward", scopes: rw, forward: true, wantPush: true},
		{name: "write_only", scopes: `["git:write"]`, forward: true},
		{name: "no_scopes", forward: true},
		{name: "ref_policy", scopes: rw, refs: `[["refs/heads/other",["no-push"]]]`, forward: true},
		{name: "ops", scopes: rw, ops: `["no-force-push"]`, forward: true},
		{name: "malformed_refs", scopes: rw, refs: `{bad`, forward: true},
		{name: "malformed_ops", scopes: rw, ops: `{bad`, forward: true},
		{name: "malformed_scopes", scopes: `{bad`, forward: true},
		{name: "loopback_forward", scopes: rw, claim: "holder", forward: true},
		{name: "loopback_pull", scopes: rw, claim: "holder"},
		{name: "pull", scopes: rw, wantPull: true},
		{name: "pull_write_only", scopes: `["git:write"]`, wantPull: true},
		{name: "pull_ref_policy", scopes: rw, refs: `[["refs/heads/other",["no-push"]]]`},
		{name: "pull_ops", scopes: rw, ops: `["no-force-push"]`},
		{name: "pull_unknown_ops", scopes: rw, ops: `["unknown"]`},
		{name: "pull_empty_ops", scopes: rw, refs: `[["*",[]]]`, wantPull: true},
		{name: "pull_malformed_refs", scopes: rw, refs: `{bad`},
		{name: "pull_malformed_ops", scopes: rw, ops: `{bad`},
		{name: "pull_in_flight", scopes: rw, syncing: true},
		// The pull claimed when the intent step read the row ends before the
		// poke: it cannot have seen this write, so the poke must mark.
		{name: "pull_ends_during_update", scopes: rw, syncing: true, pullEndsAtCAS: true, wantPull: true},
		{name: "disconnect_after_commit", scopes: rw, forward: true, disconnect: true, wantPush: true},
		{name: "metadata_unavailable", scopes: rw, forward: true, storeClosed: true, wantRefused: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			dbURL := "sqlite://" + filepath.Join(t.TempDir(), "hook.db")
			ms, err := meta.Open(ctx, dbURL)
			if err != nil {
				t.Fatal(err)
			}
			defer ms.Close()
			if err := ms.InitSchema(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := ms.UpsertOrgWithKeys(ctx, "hook-org", []string{"-----BEGIN PUBLIC KEY-----\ndummy\n-----END PUBLIC KEY-----\n"}); err != nil {
				t.Fatal(err)
			}
			repo, err := ms.CreateRepo(ctx, "hook-org", "https://example.com/hook.git", "main")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ms.SetRepoUpstream(ctx, meta.RepoUpstream{RepoID: repo.RepoID, ForwardOnPush: tc.forward}); err != nil {
				t.Fatal(err)
			}
			if tc.syncing {
				if _, ok, _, err := ms.TryBeginSync(ctx, repo.RepoID, "holder", time.Minute); err != nil || !ok {
					t.Fatalf("claim: %v %v", ok, err)
				}
			}
			rs, err := ref.Open(ctx, dbURL)
			if err != nil {
				t.Fatal(err)
			}
			defer rs.Close()
			if err := rs.InitSchema(ctx); err != nil {
				t.Fatal(err)
			}
			t.Setenv(envOrg, "hook-org")
			t.Setenv("OPENSTORAGE_PG_URL", dbURL)
			t.Setenv(envSecretsKey, "")
			t.Setenv(githttp.EnvScopes, tc.scopes)
			t.Setenv(githttp.EnvRefsPolicy, tc.refs)
			t.Setenv(githttp.EnvOps, tc.ops)
			t.Setenv(githttp.EnvSyncClaim, tc.claim)
			original := githttp.WrapHookRefStore
			defer func() { githttp.WrapHookRefStore = original }()
			cleanup := sync.OnceFunc(wireHookEventing(ctx, slog.New(slog.DiscardHandler)))
			defer cleanup()
			if tc.storeClosed {
				cleanup()
			}
			var inner ref.RefStore = rs
			pushCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			if tc.disconnect {
				inner = cancelAfterCAS{RefStore: rs, cancel: cancel}
			}
			pushAtCAS := -1
			inner = observeAtCAS{RefStore: inner, observe: func() {
				up, err := ms.GetRepoUpstream(ctx, repo.RepoID)
				if err != nil {
					t.Errorf("read the row at the CAS: %v", err)
				}
				pushAtCAS = up.PushPending
				if tc.pullEndsAtCAS {
					if _, err := ms.FinishSync(ctx, repo.RepoID, "holder", true); err != nil {
						t.Errorf("end the pull: %v", err)
					}
				}
			}}
			wrapped, err := githttp.WrapHookRefStore(ctx, inner)
			if err != nil {
				t.Fatal(err)
			}
			sha := strings.Repeat("a", 40)
			won, _, err := wrapped.CASRefGen(pushCtx, repo.RepoID, "refs/heads/main", ref.ZeroSHA, sha)
			if tc.wantRefused {
				got, gerr := rs.GetRef(ctx, repo.RepoID, "refs/heads/main")
				if err == nil || won || !errors.Is(gerr, ref.ErrNotFound) {
					t.Fatalf("CAS = (won %v, err %v), authority main = (%q, %v); want the update refused before the CAS: an accepted write without its forward intent is never forwarded", won, err, got, gerr)
				}
				return
			}
			if err != nil || !won {
				t.Fatalf("accepted CAS: %v %v", won, err)
			}
			if tc.wantPush && pushAtCAS <= 0 {
				t.Fatalf("push_pending = %d when the authority CAS committed; a hook killed right there loses the forward", pushAtCAS)
			}
			up, err := ms.GetRepoUpstream(ctx, repo.RepoID)
			if err != nil {
				t.Fatal(err)
			}
			if up.SyncPending != tc.wantPull || (up.PushPending > 0) != tc.wantPush {
				t.Fatalf("pending pull=%v push=%d; want pull=%v push=%v", up.SyncPending, up.PushPending, tc.wantPull, tc.wantPush)
			}
		})
	}
}

// CASRef is the REST surface's CAS: the API handlers write through it.
func (s observeAtCAS) CASRef(ctx context.Context, repo, name, old, next string) (bool, error) {
	won, err := s.RefStore.CASRef(ctx, repo, name, old, next)
	if won && err == nil {
		s.observe()
	}
	return won, err
}

// TestServeRestIntentBeforeCAS: on the local schedule a REST ref write
// (the API handlers' CASRef through buildServeEventing's decorator) holds
// the ref and records its forward intent before the authority CAS, as the
// update hook does (verify-lane-schedule finding 1, REST writes). When the
// CAS commits, the row has the intent, so serve dying right there loses no
// forward, and the ref's push hold is live, so a run that claims the
// intent first waits for the CAS. The hold ends with the write. A failed
// intent read refuses the write.
func TestServeRestIntentBeforeCAS(t *testing.T) {
	rw := []string{auth.ScopeGitRead, auth.ScopeGitWrite}
	for _, backend := range []string{"sqlite", "s3wal"} {
		t.Run(backend, func(t *testing.T) {
			for _, tc := range []struct {
				name               string
				forward, noClaims  bool
				scopes             []string
				refName            string
				storeFails         bool
				panics             bool
				heldElsewhere      bool
				noUpstream         bool
				lostCAS            bool
				wantIntent, refuse bool
			}{
				{name: "forward", forward: true, scopes: rw, wantIntent: true},
				// The first CAS panics, as a store bug would. Serve's API
				// recovers the panic and keeps running, so the hold must
				// end anyway: the retried write takes the same hold.
				{name: "panicking_cas", forward: true, scopes: rw, panics: true, wantIntent: true},
				// Another writer holds the ref past pushHoldWaitMax (10 s):
				// the write is refused with githttp.ErrRefHeld, which the
				// API answers with 503 and Retry-After.
				{name: "held_elsewhere", forward: true, scopes: rw, heldElsewhere: true, refuse: true},
				// No upstream link: nothing to forward, so a missing row
				// (meta.ErrNotFound) neither refuses the write nor holds.
				{name: "no_upstream", forward: true, scopes: rw, noUpstream: true},
				// A write that loses its CAS (409) has recorded its intent
				// already, so the lane runs once and finds nothing new.
				{name: "lost_cas", forward: true, scopes: rw, lostCAS: true},
				{name: "server_initiated", forward: true, noClaims: true, wantIntent: true},
				{name: "write_only", forward: true, scopes: []string{auth.ScopeGitWrite}},
				{name: "pull", scopes: rw},
				{name: "ephemeral", forward: true, scopes: rw, refName: auth.EphemeralNamespacePrefix + "refs/heads/main"},
				// Closing the store fails its reads on SQLite only. A caller
				// that may not forward records no intent, so a failed read
				// does not refuse its write.
				{name: "metadata_unavailable", forward: true, scopes: rw, storeFails: true, refuse: true},
				{name: "metadata_unavailable_write_only", forward: true, scopes: []string{auth.ScopeGitWrite}, storeFails: true},
			} {
				if (tc.storeFails || tc.heldElsewhere) && backend != "sqlite" {
					continue // the hold is a cache file: one backend is enough
				}
				t.Run(tc.name, func(t *testing.T) {
					ctx := t.Context()
					dir := t.TempDir()
					var (
						ms        meta.Store
						closeMeta func()
					)
					if backend == "sqlite" {
						sq, err := meta.OpenSQLite(ctx, "sqlite://"+filepath.Join(dir, "meta.sqlite"))
						if err != nil {
							t.Fatal(err)
						}
						if err := sq.InitSchema(ctx); err != nil {
							t.Fatal(err)
						}
						ms, closeMeta = sq, sync.OnceFunc(sq.Close)
					} else {
						wal := meta.NewS3WAL(s3wal.NewMemCAS())
						ms, closeMeta = wal, sync.OnceFunc(wal.Close)
					}
					t.Cleanup(closeMeta)
					rs, err := ref.Open(ctx, "sqlite://"+filepath.Join(dir, "refs.sqlite"))
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(rs.Close)
					if err := rs.InitSchema(ctx); err != nil {
						t.Fatal(err)
					}
					key, pub, err := newLoopbackKey()
					if err != nil {
						t.Fatal(err)
					}
					const org = "org"
					if _, err := ms.UpsertOrgWithKeys(ctx, org, []string{pub}); err != nil {
						t.Fatal(err)
					}
					repo, err := ms.CreateRepo(ctx, org, "https://example.com/rest.git", "main")
					if err != nil {
						t.Fatal(err)
					}
					if !tc.noUpstream {
						if _, err := ms.SetRepoUpstream(ctx, meta.RepoUpstream{
							RepoID: repo.RepoID, Provider: "github", Owner: "up", Name: "mirror",
							UpstreamHost: "https://example.invalid", ForwardOnPush: tc.forward,
						}); err != nil {
							t.Fatal(err)
						}
					}
					cacheDir := filepath.Join(dir, "cache")
					_, decorate, cleanup, err := buildServeEventing(ctx, ms, "sqlite://"+filepath.Join(dir, "meta.sqlite"), org,
						"http://127.0.0.1:1", cacheDir, key, nil, nil, nil, nil, false, syncEgressSettings{}, slog.New(slog.DiscardHandler))
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(cleanup)

					// held reports whether a live push hold names an
					// unreleased ref: a run's publication wait blocks.
					held := func(wait time.Duration) bool {
						waitCtx, cancel := context.WithTimeout(ctx, wait)
						defer cancel()
						err := githttp.WaitForPushPublication(waitCtx, cacheDir, repo.RepoID)
						if err != nil && !errors.Is(err, context.DeadlineExceeded) {
							t.Errorf("publication wait: %v", err)
						}
						return err != nil
					}
					intentAtCAS, heldAtCAS := -1, false
					var inner ref.RefStore = rs
					if tc.panics {
						inner = &panicOnceAtCAS{RefStore: rs}
					}
					store := decorate(observeAtCAS{RefStore: inner, observe: func() {
						intentAtCAS = 0
						if !tc.storeFails && !tc.noUpstream {
							up, err := ms.GetRepoUpstream(ctx, repo.RepoID)
							if err != nil {
								t.Errorf("read the row at the CAS: %v", err)
							}
							intentAtCAS = up.PushPending
						}
						heldAtCAS = held(200 * time.Millisecond)
					}})
					writeCtx := ctx
					if !tc.noClaims {
						writeCtx = auth.WithClaims(ctx, &auth.Claims{Iss: org, Sub: auth.SubjectLiteral, Repo: repo.URL, Scopes: tc.scopes})
					}
					refName := cmp.Or(tc.refName, "refs/heads/main")
					if tc.storeFails {
						closeMeta()
					}
					if tc.heldElsewhere {
						release, err := githttp.HoldRef(ctx, cacheDir, repo.RepoID, refName)
						if err != nil {
							t.Fatal(err)
						}
						defer release()
					}
					sha := strings.Repeat("b", 40)
					if tc.panics {
						func() {
							defer func() {
								if r := recover(); r != errStoreBug {
									t.Fatalf("first CAS recovered %v, want the store's panic", r)
								}
							}()
							_, _ = store.CASRef(writeCtx, repo.RepoID, refName, ref.ZeroSHA, sha)
						}()
					}
					old := ref.ZeroSHA
					if tc.lostCAS {
						old = strings.Repeat("a", 40) // the ref does not exist: the CAS loses
					}
					won, err := store.CASRef(writeCtx, repo.RepoID, refName, old, sha)
					if tc.lostCAS {
						up, uerr := ms.GetRepoUpstream(ctx, repo.RepoID)
						if err != nil || won || uerr != nil || up.PushPending != upsync.PushPendingPush || held(5*time.Second) {
							t.Fatalf("lost CAS = (won %v, err %v), push_pending = %d (%v), or the ref still held; want a lost CAS whose plain intent stays and whose hold ended", won, err, up.PushPending, uerr)
						}
						return
					}
					if tc.refuse {
						got, gerr := rs.GetRef(ctx, repo.RepoID, refName)
						if err == nil || won || !errors.Is(gerr, ref.ErrNotFound) {
							t.Fatalf("CAS = (won %v, err %v), authority = (%q, %v); want the write refused before the CAS: an accepted write without its forward intent is never forwarded", won, err, got, gerr)
						}
						if errors.Is(err, githttp.ErrRefHeld) != tc.heldElsewhere {
							t.Fatalf("refusal %v matches githttp.ErrRefHeld = %v, want %v", err, !tc.heldElsewhere, tc.heldElsewhere)
						}
						return
					}
					if err != nil || !won {
						t.Fatalf("accepted CAS: %v %v", won, err)
					}
					if tc.wantIntent && (intentAtCAS != upsync.PushPendingPush || !heldAtCAS) {
						t.Fatalf("at the authority CAS push_pending = %d, ref held = %v; want a plain intent (%d: a REST write never forces) and the ref held: serve dying there loses the forward, and a run that claims the intent can snapshot the ref before the CAS", intentAtCAS, heldAtCAS, upsync.PushPendingPush)
					}
					if !tc.wantIntent && (intentAtCAS != 0 || heldAtCAS) {
						t.Fatalf("at the authority CAS push_pending = %d, ref held = %v; want neither for a write that records no intent", intentAtCAS, heldAtCAS)
					}
					if held(5 * time.Second) {
						t.Fatal("the ref is still held after the write returned")
					}
				})
			}
		})
	}
}

// restIntentMeta is the slice of meta.EventingStore that
// restIntentBeforeCAS reads and writes, with scripted answers.
type restIntentMeta struct {
	meta.EventingStore
	up      meta.RepoUpstream
	upErr   error
	pushErr error
	pushes  []bool // the force flag of each RequestPush
}

func (m *restIntentMeta) GetRepoUpstream(context.Context, string) (meta.RepoUpstream, error) {
	return m.up, m.upErr
}

func (m *restIntentMeta) RequestPush(_ context.Context, _ string, force bool) error {
	m.pushes = append(m.pushes, force)
	return m.pushErr
}

// TestRestIntentBeforeCASPaths drives restIntentBeforeCAS through each
// answer of the metadata store and the cache: whether it refuses, whether
// the ref is held when it returns, and the intent it records. A recorded
// intent is always a plain request, never a forced one, and a refusal
// never leaves the ref held.
func TestRestIntentBeforeCASPaths(t *testing.T) {
	forward := meta.RepoUpstream{ForwardOnPush: true}
	for _, tc := range []struct {
		name       string
		m          restIntentMeta
		cacheFile  bool // the cache path is a file: the hold fails
		wantErr    bool
		wantHeld   bool
		wantPushes int
	}{
		{name: "no_upstream_link", m: restIntentMeta{upErr: meta.ErrNotFound}},
		{name: "row_read_fails", m: restIntentMeta{upErr: errors.New("row read failed")}, wantErr: true},
		{name: "not_forward_on_push", m: restIntentMeta{}},
		{name: "hold_fails", m: restIntentMeta{up: forward}, cacheFile: true, wantErr: true},
		{name: "intent_write_fails", m: restIntentMeta{up: forward, pushErr: errors.New("intent write failed")}, wantErr: true, wantPushes: 1},
		// The link went away between the read and the intent write:
		// nothing to forward, so the write goes on, still holding.
		{name: "intent_link_gone", m: restIntentMeta{up: forward, pushErr: meta.ErrNotFound}, wantHeld: true, wantPushes: 1},
		{name: "forward", m: restIntentMeta{up: forward}, wantHeld: true, wantPushes: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const repoID = "repo_x"
			cacheDir := filepath.Join(t.TempDir(), "cache")
			if tc.cacheFile {
				if err := os.WriteFile(cacheDir, nil, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			held := func(wait time.Duration) bool {
				waitCtx, cancel := context.WithTimeout(t.Context(), wait)
				defer cancel()
				return githttp.WaitForPushPublication(waitCtx, cacheDir, repoID) != nil
			}
			m := tc.m
			release, err := restIntentBeforeCAS(&m, cacheDir, func(context.Context) bool { return false })(t.Context(), repoID, "refs/heads/main")
			if (err != nil) != tc.wantErr || (err == nil) != (release != nil) {
				t.Fatalf("restIntentBeforeCAS = (release %v, err %v), want an error = %v", release != nil, err, tc.wantErr)
			}
			if len(m.pushes) != tc.wantPushes || slices.Contains(m.pushes, true) {
				t.Fatalf("RequestPush calls (force flags) = %v, want %d plain ones", m.pushes, tc.wantPushes)
			}
			if !tc.cacheFile {
				if got := held(time.Second); got != tc.wantHeld {
					t.Fatalf("ref held after the call = %v, want %v", got, tc.wantHeld)
				}
			}
			if release != nil {
				release()
				if held(5 * time.Second) {
					t.Fatal("the ref is still held after release")
				}
			}
		})
	}
}
