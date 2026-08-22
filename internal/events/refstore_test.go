package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"codestorage/internal/meta"
	"codestorage/internal/ref"
)

const (
	shaA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	shaB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// fakeInner scripts CASRef outcomes; the other RefStore methods delegate
// through the embedded interface by construction. sha scripts GetRef — the
// ref.IdempotentCAS recovery recheck — and defaults to a not-found miss so
// non-recovery tests keep their plain lost/failed semantics.
type fakeInner struct {
	won bool
	err error
	sha string
}

func (f *fakeInner) ListRefs(context.Context, string) (map[string]string, error) {
	return map[string]string{"refs/heads/main": shaA}, nil
}
func (f *fakeInner) ListRefsWithMeta(context.Context, string) ([]ref.RefInfo, error) {
	return nil, nil
}
func (f *fakeInner) GetRef(context.Context, string, string) (string, error) {
	if f.sha == "" {
		return "", ref.ErrNotFound
	}
	return f.sha, nil
}
func (f *fakeInner) CASRef(_ context.Context, _, _, _, _ string) (bool, error) {
	return f.won, f.err
}
func (f *fakeInner) RegisterPacks(context.Context, string, []ref.Pack) error { return nil }
func (f *fakeInner) ReplacePacks(context.Context, string, []ref.Pack, ref.Pack) (bool, error) {
	return false, nil
}
func (f *fakeInner) ListPacks(context.Context, string) ([]ref.Pack, error) { return nil, nil }
func (f *fakeInner) HasPack(context.Context, string, string) (bool, error) { return false, nil }
func (f *fakeInner) GetGeneration(context.Context, string) (int64, error)  { return 0, nil }
func (f *fakeInner) ReservePackBytes(context.Context, []string, []ref.Pack, string, int64, int64, time.Duration) (ref.ReserveDecision, error) {
	return ref.ReserveDecision{OK: true}, nil
}
func (f *fakeInner) ReleasePackReservations(context.Context, []string, string) error { return nil }
func (f *fakeInner) SweepExpiredPackReservations(context.Context, time.Duration) (int64, error) {
	return 0, nil
}
func (f *fakeInner) IsRetainedPack(context.Context, string, string) (bool, error) {
	return false, nil
}
func (f *fakeInner) ReclaimRetainedPack(context.Context, string, string) (int64, error) {
	return 0, nil
}

type fakeRepos struct {
	repo meta.Repo
	err  error
	// byID records lookups so tests can assert the suffix was stripped.
	lookups []string
}

func (f *fakeRepos) GetRepoByID(_ context.Context, org, repoID string) (meta.Repo, error) {
	f.lookups = append(f.lookups, org+"/"+repoID)
	if f.err != nil {
		return meta.Repo{}, f.err
	}
	return f.repo, nil
}

type fakeLister struct {
	hooks []Webhook
	err   error
}

func (f *fakeLister) ListWebhooks(context.Context, string) ([]Webhook, error) {
	return f.hooks, f.err
}

// emitEnv is one decorated store plus capture buffers for its outputs.
type emitEnv struct {
	repos      *fakeRepos
	store      *RefStore
	deliveries []Delivery
	syncPokes  []string
}

func newEmitEnv(t *testing.T, inner *fakeInner, hooks []Webhook) *emitEnv {
	t.Helper()
	env := &emitEnv{}
	env.repos = &fakeRepos{repo: meta.Repo{
		RepoID: "repo_abc123", OrgID: 42, URL: "acme/widgets",
		CreatedAt: time.Now(),
	}}
	env.store = NewRefStore(inner, RefStoreOptions{
		Org:      "acme",
		Repos:    env.repos,
		Webhooks: &fakeLister{hooks: hooks},
		EnqueueDelivery: func(_ context.Context, d Delivery) error {
			env.deliveries = append(env.deliveries, d)
			return nil
		},
		EnqueueSync: func(_ context.Context, repoID string) error {
			env.syncPokes = append(env.syncPokes, repoID)
			return nil
		},
		Now:    func() time.Time { return time.Date(2026, 1, 20, 10, 30, 0, 0, time.UTC) },
		Logger: testLogger(),
	})
	return env
}

func twoHooks() []Webhook {
	return []Webhook{
		{ID: "wh_1", OrgID: 42, URL: "https://a.example/hook", Secret: "s1"},
		{ID: "wh_2", OrgID: 42, URL: "https://b.example/hook", Secret: "s2"},
	}
}

func TestCASRefEmitsOnePushPerWebhook(t *testing.T) {
	env := newEmitEnv(t, &fakeInner{won: true}, twoHooks())
	won, err := env.store.CASRef(context.Background(), "repo_abc123", "refs/heads/main", shaA, shaB)
	if err != nil || !won {
		t.Fatalf("CASRef = (%v, %v), want (true, nil)", won, err)
	}
	if len(env.deliveries) != 2 {
		t.Fatalf("got %d deliveries, want 2 (one per webhook)", len(env.deliveries))
	}
	if env.deliveries[0].WebhookID != "wh_1" || env.deliveries[1].WebhookID != "wh_2" {
		t.Errorf("delivery webhook ids = %s, %s", env.deliveries[0].WebhookID, env.deliveries[1].WebhookID)
	}
	if string(env.deliveries[0].Payload) != string(env.deliveries[1].Payload) {
		t.Errorf("fan-out payloads differ")
	}

	// docs/06: exactly these seven fields, all non-empty.
	var got map[string]any
	if err := json.Unmarshal(env.deliveries[0].Payload, &got); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if len(got) != 6 {
		t.Errorf("top-level payload keys = %d (%v), want 6", len(got), got)
	}
	repoObj, ok := got["repository"].(map[string]any)
	if !ok || len(repoObj) != 2 {
		t.Fatalf("repository object = %v, want {id, url}", got["repository"])
	}
	for field, want := range map[string]string{
		"ref":    "refs/heads/main",
		"before": shaA,
		"after":  shaB,
		// docs/14 deferred #3: customer_id is the org-name string matching
		// the JWT `iss` claim, not the internal integer org ID.
		"customer_id": "acme",
		"pushed_at":   "2026-01-20T10:30:00Z",
	} {
		if got[field] != want {
			t.Errorf("payload.%s = %v, want %q", field, got[field], want)
		}
	}
	if repoObj["id"] != "repo_abc123" || repoObj["url"] != "acme/widgets" {
		t.Errorf("payload.repository = %v", repoObj)
	}
	if env.deliveries[0].Event != EventPush {
		t.Errorf("delivery event = %q, want %q", env.deliveries[0].Event, EventPush)
	}
	if len(env.syncPokes) != 1 || env.syncPokes[0] != "repo_abc123" {
		t.Errorf("sync pokes = %v, want [repo_abc123]", env.syncPokes)
	}
}

func TestCASRefCreateAndDeleteKeepZeroSHAs(t *testing.T) {
	for name, tc := range map[string]struct{ old, new string }{
		"create": {ref.ZeroSHA, shaB},
		"delete": {shaA, ref.ZeroSHA},
	} {
		t.Run(name, func(t *testing.T) {
			env := newEmitEnv(t, &fakeInner{won: true}, twoHooks()[:1])
			if _, err := env.store.CASRef(context.Background(), "repo_abc123", "refs/heads/x", tc.old, tc.new); err != nil {
				t.Fatal(err)
			}
			var got PushPayload
			if err := json.Unmarshal(env.deliveries[0].Payload, &got); err != nil {
				t.Fatal(err)
			}
			if got.Before != tc.old || got.After != tc.new {
				t.Errorf("before/after = %s/%s, want %s/%s", got.Before, got.After, tc.old, tc.new)
			}
		})
	}
}

func TestCASRefLostOrFailedEmitsNothing(t *testing.T) {
	t.Run("lost", func(t *testing.T) {
		env := newEmitEnv(t, &fakeInner{won: false}, twoHooks())
		won, err := env.store.CASRef(context.Background(), "repo_abc123", "refs/heads/main", shaA, shaB)
		if won || err != nil {
			t.Fatalf("CASRef = (%v, %v), want (false, nil)", won, err)
		}
		if len(env.deliveries)+len(env.syncPokes) != 0 {
			t.Errorf("lost CAS emitted %d deliveries, %d sync pokes", len(env.deliveries), len(env.syncPokes))
		}
	})
	t.Run("error", func(t *testing.T) {
		innerErr := errors.New("db down")
		env := newEmitEnv(t, &fakeInner{err: innerErr}, twoHooks())
		_, err := env.store.CASRef(context.Background(), "repo_abc123", "refs/heads/main", shaA, shaB)
		if !errors.Is(err, innerErr) {
			t.Fatalf("CASRef error = %v, want inner error", err)
		}
		if len(env.deliveries)+len(env.syncPokes) != 0 {
			t.Errorf("failed CAS emitted %d deliveries, %d sync pokes", len(env.deliveries), len(env.syncPokes))
		}
	})
}

// TestCASRefAmbiguousRecoveryEmitsPush is issue #2's acceptance test at the
// events seam: the inner store commits the update, then reports a connection
// drop. The ref.IdempotentCAS wrap under this decorator re-reads the ref,
// reconciles the outcome to a win, and the push webhook is emitted instead
// of silently dropped.
func TestCASRefAmbiguousRecoveryEmitsPush(t *testing.T) {
	env := newEmitEnv(t, &fakeInner{err: errors.New("post-commit drop"), sha: shaB}, twoHooks())
	won, err := env.store.CASRef(context.Background(), "repo_abc123", "refs/heads/main", shaA, shaB)
	if !won || err != nil {
		t.Fatalf("CASRef = (%v, %v), want recovered (true, nil)", won, err)
	}
	if len(env.deliveries) != 2 {
		t.Fatalf("got %d deliveries, want 2 (one per webhook)", len(env.deliveries))
	}
	var got PushPayload
	if err := json.Unmarshal(env.deliveries[0].Payload, &got); err != nil {
		t.Fatal(err)
	}
	if got.Before != shaA || got.After != shaB {
		t.Errorf("payload before/after = %s/%s, want %s/%s", got.Before, got.After, shaA, shaB)
	}
	if len(env.syncPokes) != 1 {
		t.Errorf("sync pokes = %v, want exactly one", env.syncPokes)
	}
}

// TestCASRefLostToDifferentSHAEmitsNothing: a genuine conflict (the ref
// holds someone else's sha) must stay a hard loss with zero emissions.
func TestCASRefLostToDifferentSHAEmitsNothing(t *testing.T) {
	const shaC = "cccccccccccccccccccccccccccccccccccccccc"
	env := newEmitEnv(t, &fakeInner{won: false, sha: shaC}, twoHooks())
	won, err := env.store.CASRef(context.Background(), "repo_abc123", "refs/heads/main", shaA, shaB)
	if won || err != nil {
		t.Fatalf("CASRef = (%v, %v), want (false, nil)", won, err)
	}
	if len(env.deliveries)+len(env.syncPokes) != 0 {
		t.Errorf("genuine conflict emitted %d deliveries, %d sync pokes", len(env.deliveries), len(env.syncPokes))
	}
}

// TestCASRefCleanCreateLossEmitsNothing pins the create decision: a clean
// create loss stays a conflict even when the existing ref already holds the
// target sha — no fabricated success, no fabricated zero->sha push event
// (duplicate same-sha creates keep their "already exists" API 409).
func TestCASRefCleanCreateLossEmitsNothing(t *testing.T) {
	env := newEmitEnv(t, &fakeInner{won: false, sha: shaB}, twoHooks())
	won, err := env.store.CASRef(context.Background(), "repo_abc123", "refs/heads/main", ref.ZeroSHA, shaB)
	if won || err != nil {
		t.Fatalf("CASRef = (%v, %v), want (false, nil)", won, err)
	}
	if len(env.deliveries)+len(env.syncPokes) != 0 {
		t.Errorf("clean create loss emitted %d deliveries, %d sync pokes", len(env.deliveries), len(env.syncPokes))
	}
}

// TestCASRefAmbiguousDeleteStaysFailed pins the delete decision: absence is
// not positive evidence, so an ambiguous-error delete keeps its error and
// emits nothing (no fabricated deletion webhook).
func TestCASRefAmbiguousDeleteStaysFailed(t *testing.T) {
	innerErr := errors.New("post-commit drop")
	env := newEmitEnv(t, &fakeInner{err: innerErr}, twoHooks())
	won, err := env.store.CASRef(context.Background(), "repo_abc123", "refs/heads/main", shaA, ref.ZeroSHA)
	if won || !errors.Is(err, innerErr) {
		t.Fatalf("CASRef = (%v, %v), want (false, inner error)", won, err)
	}
	if len(env.deliveries)+len(env.syncPokes) != 0 {
		t.Errorf("ambiguous delete emitted %d deliveries, %d sync pokes", len(env.deliveries), len(env.syncPokes))
	}
}

// TestEphemeralUpdatesEmitNothing covers invariant 7 on BOTH write
// surfaces: the git surface (storage key `<RepoID>+ephemeral`, plain ref
// names) and the API surface (bare RepoID, namespaced ref names).
func TestEphemeralUpdatesEmitNothing(t *testing.T) {
	for name, tc := range map[string]struct{ repoKey, refName string }{
		"git surface key suffix":  {"repo_abc123+ephemeral", "refs/heads/main"},
		"api surface ref prefix":  {"repo_abc123", "refs/namespaces/ephemeral/refs/heads/main"},
		"both (defense in depth)": {"repo_abc123+ephemeral", "refs/namespaces/ephemeral/refs/heads/main"},
	} {
		t.Run(name, func(t *testing.T) {
			env := newEmitEnv(t, &fakeInner{won: true}, twoHooks())
			won, err := env.store.CASRef(context.Background(), tc.repoKey, tc.refName, shaA, shaB)
			if err != nil || !won {
				t.Fatalf("CASRef = (%v, %v), want (true, nil)", won, err)
			}
			if len(env.deliveries) != 0 {
				t.Errorf("ephemeral update emitted %d deliveries", len(env.deliveries))
			}
			if len(env.syncPokes) != 0 {
				t.Errorf("ephemeral update poked sync %d times", len(env.syncPokes))
			}
		})
	}
}

// TestImportNamespaceEmitsWithBaseRepoID: `+import` pushes update
// persistent history, so they DO emit — keyed by the base RepoID.
func TestImportNamespaceEmitsWithBaseRepoID(t *testing.T) {
	env := newEmitEnv(t, &fakeInner{won: true}, twoHooks()[:1])
	if _, err := env.store.CASRef(context.Background(), "repo_abc123+import", "refs/heads/main", shaA, shaB); err != nil {
		t.Fatal(err)
	}
	if len(env.deliveries) != 1 {
		t.Fatalf("got %d deliveries, want 1", len(env.deliveries))
	}
	var got PushPayload
	if err := json.Unmarshal(env.deliveries[0].Payload, &got); err != nil {
		t.Fatal(err)
	}
	if got.Repository.ID != "repo_abc123" {
		t.Errorf("repository.id = %q, want base RepoID repo_abc123", got.Repository.ID)
	}
	if want := []string{"acme/repo_abc123"}; len(env.repos.lookups) != 1 || env.repos.lookups[0] != want[0] {
		t.Errorf("repo lookups = %v, want %v", env.repos.lookups, want)
	}
	if len(env.syncPokes) != 1 || env.syncPokes[0] != "repo_abc123" {
		t.Errorf("sync pokes = %v", env.syncPokes)
	}
}

func TestSyncPokedEvenWithoutWebhooks(t *testing.T) {
	env := newEmitEnv(t, &fakeInner{won: true}, nil)
	if _, err := env.store.CASRef(context.Background(), "repo_abc123", "refs/heads/main", shaA, shaB); err != nil {
		t.Fatal(err)
	}
	if len(env.deliveries) != 0 {
		t.Errorf("no webhooks registered but %d deliveries enqueued", len(env.deliveries))
	}
	if len(env.syncPokes) != 1 {
		t.Errorf("sync pokes = %v, want exactly one", env.syncPokes)
	}
	if len(env.repos.lookups) != 0 {
		t.Errorf("repo looked up %d times with no webhooks (wasted query)", len(env.repos.lookups))
	}
}

// TestEmissionFailuresNeverFailTheUpdate: the CAS committed, so listing,
// lookup, and enqueue failures are logged and swallowed.
func TestEmissionFailuresNeverFailTheUpdate(t *testing.T) {
	t.Run("list webhooks fails", func(t *testing.T) {
		var pokes []string
		st := NewRefStore(&fakeInner{won: true}, RefStoreOptions{
			Org:      "acme",
			Repos:    &fakeRepos{},
			Webhooks: &fakeLister{err: errors.New("db down")},
			EnqueueDelivery: func(context.Context, Delivery) error {
				t.Error("enqueue called despite list failure")
				return nil
			},
			EnqueueSync: func(_ context.Context, id string) error { pokes = append(pokes, id); return nil },
			Logger:      testLogger(),
		})
		won, err := st.CASRef(context.Background(), "repo_x", "refs/heads/main", shaA, shaB)
		if err != nil || !won {
			t.Fatalf("CASRef = (%v, %v), want (true, nil)", won, err)
		}
		if len(pokes) != 1 {
			t.Errorf("sync poke skipped on webhook-list failure: %v", pokes)
		}
	})
	t.Run("repo lookup fails", func(t *testing.T) {
		env := newEmitEnv(t, &fakeInner{won: true}, twoHooks())
		env.repos.err = fmt.Errorf("row: %w", meta.ErrNotFound)
		won, err := env.store.CASRef(context.Background(), "repo_abc123", "refs/heads/main", shaA, shaB)
		if err != nil || !won {
			t.Fatalf("CASRef = (%v, %v), want (true, nil)", won, err)
		}
		if len(env.deliveries) != 0 {
			t.Errorf("deliveries enqueued without a payload source")
		}
	})
	t.Run("enqueue fails", func(t *testing.T) {
		st := NewRefStore(&fakeInner{won: true}, RefStoreOptions{
			Org:             "acme",
			Repos:           &fakeRepos{repo: meta.Repo{RepoID: "repo_x", OrgID: 1, URL: "a/b"}},
			Webhooks:        &fakeLister{hooks: twoHooks()},
			EnqueueDelivery: func(context.Context, Delivery) error { return errors.New("queue full") },
			Logger:          testLogger(),
		})
		won, err := st.CASRef(context.Background(), "repo_x", "refs/heads/main", shaA, shaB)
		if err != nil || !won {
			t.Fatalf("CASRef = (%v, %v), want (true, nil)", won, err)
		}
	})
}

// TestEmissionSurvivesCanceledRequestContext: the client may hang up right
// after the CAS commits; emission must still run (detached context).
func TestEmissionSurvivesCanceledRequestContext(t *testing.T) {
	env := newEmitEnv(t, &fakeInner{won: true}, twoHooks()[:1])
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // canceled before CASRef even runs; fake inner ignores ctx
	won, err := env.store.CASRef(ctx, "repo_abc123", "refs/heads/main", shaA, shaB)
	if err != nil || !won {
		t.Fatalf("CASRef = (%v, %v), want (true, nil)", won, err)
	}
	if len(env.deliveries) != 1 {
		t.Fatalf("emission lost under canceled request context")
	}
}

func TestHelperFunctions(t *testing.T) {
	if got := BaseRepoID("repo_ab+ephemeral"); got != "repo_ab" {
		t.Errorf("BaseRepoID(+ephemeral) = %q", got)
	}
	if got := BaseRepoID("repo_ab+import"); got != "repo_ab" {
		t.Errorf("BaseRepoID(+import) = %q", got)
	}
	if got := BaseRepoID("repo_ab"); got != "repo_ab" {
		t.Errorf("BaseRepoID(bare) = %q", got)
	}
	if EphemeralUpdate("repo_ab+import", "refs/heads/main") {
		t.Error("EphemeralUpdate treats +import as ephemeral")
	}
}

// TestEndToEndPushDelivery closes the in-package loop: a won CAS through
// the decorator enqueues a delivery, the Deliverer posts it, and the
// receiver verifies the signature against the raw body — the same check
// the vendor SDK helpers perform.
func TestEndToEndPushDelivery(t *testing.T) {
	rc := &receiver{statuses: []int{http.StatusOK}}
	srv := httptest.NewServer(rc.handler())
	defer srv.Close()

	const secret = "whsec_e2e"
	hook := Webhook{ID: "wh_e2e", OrgID: 42, URL: srv.URL, Secret: secret}
	lookup := &fakeLookup{hooks: map[string]Webhook{hook.ID: hook}}
	deliverer := &Deliverer{Webhooks: lookup, Logger: testLogger(),
		AllowPrivate: true, // httptest receiver is loopback (issue #53)
		Backoff:      func(int) time.Duration { return 0 }}

	// The "queue": enqueue delivers synchronously, as the in-process
	// local-mode queue would (minus the goroutine).
	var wg sync.WaitGroup
	st := NewRefStore(&fakeInner{won: true}, RefStoreOptions{
		Org:      "acme",
		Repos:    &fakeRepos{repo: meta.Repo{RepoID: "repo_e2e", OrgID: 42, URL: "acme/e2e"}},
		Webhooks: &fakeLister{hooks: []Webhook{hook}},
		EnqueueDelivery: func(ctx context.Context, d Delivery) error {
			wg.Add(1)
			go func() { defer wg.Done(); _ = deliverer.Deliver(ctx, d) }()
			return nil
		},
		Logger: testLogger(),
	})

	won, err := st.CASRef(context.Background(), "repo_e2e", "refs/heads/main", ref.ZeroSHA, shaB)
	if err != nil || !won {
		t.Fatalf("CASRef = (%v, %v), want (true, nil)", won, err)
	}
	wg.Wait()

	reqs := rc.requests()
	if len(reqs) != 1 {
		t.Fatalf("receiver saw %d deliveries, want 1", len(reqs))
	}
	got := reqs[0]
	if want := Sign(secret, sigTimestamp(t, got.signature), got.body); got.signature != want {
		t.Fatalf("end-to-end signature does not verify")
	}
	var payload PushPayload
	if err := json.Unmarshal(got.body, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Before != ref.ZeroSHA || payload.After != shaB || payload.Repository.ID != "repo_e2e" {
		t.Errorf("payload = %+v", payload)
	}
}
