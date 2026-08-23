// wire_eventing.go is the phase-5 sibling of wire.go: it assembles the
// eventing/sync stack (docs/reference/webhooks.md, docs/archive/phases/07-implementation-roadmap.md p5) from the slice-owned packages —
// jobs queue (River on pg / in-process in local mode), webhook deliverer,
// upstream-sync worker with loopback push, and the events.RefStore CASRef
// decorator — for both the serve process and the `codestorage hook`
// subprocess git re-execs.
package main

import (
	"context"
	"crypto"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/coder/code-storage/internal/auth"
	"github.com/coder/code-storage/internal/events"
	"github.com/coder/code-storage/internal/githttp"
	"github.com/coder/code-storage/internal/jobs"
	"github.com/coder/code-storage/internal/meta"
	"github.com/coder/code-storage/internal/ref"
	upsync "github.com/coder/code-storage/internal/sync"
)

// pendingSyncRecoveryDelay defers the local-mode boot recovery scan until
// the serve listener is up (the recovered runs loopback-push to it).
const pendingSyncRecoveryDelay = 15 * time.Second

// loopbackTokenTTL is the CEILING for the internally minted git:write JWT
// of one sync run's loopback push (sync runs are capped at 10 minutes).
// The worker passes a per-run fence TTL derived from its claim lease
// (issue #86); mint honors the SHORTER of the two, so a zombie run whose
// claim can be stolen never holds a token that still authenticates.
const loopbackTokenTTL = 15 * time.Minute

// loopbackKey is the serve process's runtime-generated signing key for
// loopback sync pushes. Its public half is registered with the org's keys
// at bootstrap (ADR: rotates per process; the key never leaves memory).
type loopbackKey struct {
	priv crypto.Signer
	alg  string
}

// newLoopbackKey generates the per-process loopback signing keypair and
// returns it with the PEM public block to append to the org key registry.
func newLoopbackKey() (loopbackKey, string, error) {
	priv, err := auth.GenerateKey("ES256")
	if err != nil {
		return loopbackKey{}, "", fmt.Errorf("generate loopback key: %w", err)
	}
	pubPEM, err := auth.MarshalPublicKeyPEM(priv.Public())
	if err != nil {
		return loopbackKey{}, "", fmt.Errorf("marshal loopback public key: %w", err)
	}
	return loopbackKey{priv: priv, alg: "ES256"}, string(pubPEM), nil
}

// mint returns a short-lived repo-scoped git:write token for one loopback
// push (docs/explanation/git-protocol-and-namespaces.md basic auth `t:<jwt>`),
// capped at min(ttl, loopbackTokenTTL) — ttl is the worker's claim-lease
// push fence (sync.Loopback.Token contract). holder is the run's claim
// token, carried as the sync_claim JWT claim so the update hook can fence
// an in-flight receive-pack that outlives a claim steal (issue #86).
func (k loopbackKey) mint(org string, repo upsync.RepoInfo, ttl time.Duration, holder string) (string, error) {
	assertf(k.priv != nil, "loopbackKey.mint: missing key")
	assertf(ttl > 0, "loopbackKey.mint: non-positive ttl %v", ttl)
	assertf(holder != "", "loopbackKey.mint: empty claim holder")
	if ttl > loopbackTokenTTL {
		ttl = loopbackTokenTTL
	}
	now := time.Now()
	return auth.MintToken(k.priv, k.alg, auth.Claims{
		Iss:       org,
		Sub:       auth.SubjectLiteral,
		Repo:      repo.URL,
		Scopes:    []string{auth.ScopeGitRead, auth.ScopeGitWrite},
		Iat:       now.Unix(),
		Exp:       now.Add(ttl).Unix(),
		SyncClaim: holder,
	})
}

// webhookSink fans one repo.sync.* event out to the org's registered
// webhooks (sync.EventSink). Emission is fire-and-forget: failures are
// logged, never propagated (the sync run outcome is already decided).
type webhookSink struct {
	webhooks events.WebhookLister
	enqueue  func(ctx context.Context, d events.Delivery) error
	logger   *slog.Logger
}

func (s webhookSink) Emit(ctx context.Context, org, eventType string, payload []byte) {
	assertf(s.webhooks != nil && s.enqueue != nil && s.logger != nil, "webhookSink.Emit: nil deps")
	assertf(org != "" && eventType != "", "webhookSink.Emit: empty org/eventType")
	hooks, err := s.webhooks.ListWebhooks(ctx, org)
	if err != nil {
		s.logger.Error("sync event lost: list webhooks", "org", org, "event", eventType, "err", err)
		return
	}
	for _, wh := range hooks {
		d := events.Delivery{WebhookID: wh.ID, Event: eventType, Payload: payload}
		if err := s.enqueue(ctx, d); err != nil {
			s.logger.Error("sync event lost: enqueue delivery",
				"org", org, "event", eventType, "webhook_id", wh.ID, "err", err)
		}
	}
}

// enqueueSyncIfDue implements the events.RefStoreOptions.EnqueueSync
// contract (docs/explanation/domain-model.md invariant 11): a won persistent ref update schedules a
// sync only when the repo has an upstream row AND no run is currently in
// flight — the in-flight check is what keeps the sync worker's own loopback
// pushes from re-triggering themselves. Deliberately NO RequestSync here
// (issue #79): the pending marker would turn every loopback push into a
// follow-up run, self-perpetuating the sync chain. Push-triggered syncs
// carry no 202 promise, so dropping one under an in-flight claim stays the
// documented invariant-11 behavior.
func enqueueSyncIfDue(ms meta.EventingStore, enqueue func(ctx context.Context, job upsync.Job) error, org string) func(ctx context.Context, repoID string) error {
	assertf(ms != nil && enqueue != nil && org != "", "enqueueSyncIfDue: missing deps")
	return func(ctx context.Context, repoID string) error {
		up, err := ms.GetRepoUpstream(ctx, repoID)
		if errors.Is(err, meta.ErrNotFound) {
			return nil // no upstream link: nothing to schedule
		}
		if err != nil {
			return err
		}
		if up.Syncing {
			return nil // run in flight (or our own loopback push): skip
		}
		return enqueue(ctx, upsync.Job{Org: org, RepoID: repoID})
	}
}

// isPGURL dispatches queue construction on the metadata DB scheme, exactly
// like meta.Open/ref.Open.
func isPGURL(dbURL string) bool {
	u, err := url.Parse(dbURL)
	return err == nil && (u.Scheme == "postgres" || u.Scheme == "postgresql")
}

// buildServeEventing assembles the serve-side eventing stack on top of the
// already-opened meta store: the jobs queue (with delivery + sync workers),
// and the events.RefStore decorator the API's write handlers flow through.
// A non-nil sweep schedules the periodic orphan-pack sweep on the queue
// backend (River periodic job on pg, background ticker in local mode); a
// non-nil repack registers the background-repack worker on both backends.
// The returned cleanup stops the sweep and the queue (waiting for in-flight
// local work).
func buildServeEventing(ctx context.Context, ms meta.Store, dbURL, org, loopbackBaseURL string, key loopbackKey, sweep *jobs.SweepSchedule, repack *jobs.Repacker, webhookAllowPrivate bool, logger *slog.Logger) (jobs.Queue, func(inner ref.RefStore) ref.RefStore, func(), error) {
	assertf(ms != nil && dbURL != "" && org != "" && loopbackBaseURL != "", "buildServeEventing: missing deps")
	assertf(logger != nil, "buildServeEventing: nil logger")

	whStore := events.MetaWebhookStore{Store: ms}
	creds, ups := upsync.MetaStores(ms)
	// AllowPrivate steers the delivery client's SSRF dial guard (issue
	// #53, CODESTORAGE_WEBHOOK_ALLOW_PRIVATE); the guard itself is always
	// on and redirects are always refused.
	deliverer := &events.Deliverer{Webhooks: whStore, AllowPrivate: webhookAllowPrivate, Logger: logger}

	// The worker's event sink enqueues into the queue built below; the
	// late-bound pointer breaks the queue<->worker construction cycle
	// (workers only run after the queue exists).
	var queue jobs.Queue
	enqueueDelivery := func(ctx context.Context, d events.Delivery) error {
		assertf(queue != nil, "eventing: delivery enqueued before queue construction")
		return queue.EnqueueWebhookDelivery(ctx, d)
	}
	worker := &upsync.Worker{
		Upstreams:   ups,
		Credentials: creds,
		Repos: func(ctx context.Context, org, repoID string) (upsync.RepoInfo, error) {
			r, err := ms.GetRepoByID(ctx, org, repoID)
			if errors.Is(err, meta.ErrNotFound) {
				return upsync.RepoInfo{}, fmt.Errorf("repo %s: %w", repoID, upsync.ErrNotFound)
			}
			if err != nil {
				return upsync.RepoInfo{}, err
			}
			return upsync.RepoInfo{RepoID: r.RepoID, URL: r.URL}, nil
		},
		Events: webhookSink{webhooks: whStore, enqueue: enqueueDelivery, logger: logger},
		// The push target is THIS process's own listener, never the
		// external base URL (issue #45): the minted token verifies only
		// against this process's in-memory loopback key, so a push routed
		// through a load balancer could land on a replica that must
		// reject it.
		Loopback: upsync.Loopback{BaseURL: loopbackBaseURL, Token: func(_ context.Context, org string, repo upsync.RepoInfo, ttl time.Duration, holder string) (string, error) {
			return key.mint(org, repo, ttl, holder)
		}},
		Logger: logger,
	}

	cleanup := func() { queue.Close() }
	if isPGURL(dbURL) {
		// worker.Requeue stays nil on pg: river's insert-time uniqueness
		// would swallow a requeue of a still-running job; the river sync
		// worker schedules successors transactionally instead (issue #79).
		q, err := jobs.NewRiver(ctx, dbURL, deliverer, worker.Run, ups, sweep, repack, logger)
		if err != nil {
			return nil, nil, nil, err
		}
		queue = q
	} else {
		queue = jobs.NewLocal(deliverer, worker.Run, repack, logger)
		// Pending pull-upstream requests that arrive while a run is in
		// flight are re-enqueued by the finishing run (issue #79); the
		// in-process queue never deduplicates against a running run, so
		// a plain re-enqueue is sound here.
		worker.Requeue = queue.EnqueueSync
		// Boot recovery (issue #86): queued local work and deferred retry
		// timers die with the process, but the SyncPending marker is
		// durable — re-enqueue marked repos so accepted pull-upstream
		// requests survive restarts (in particular the restart that
		// applies CODESTORAGE_SYNC_STEAL_LEGACY). Delayed so the loopback
		// listener is serving before the first recovered run pushes; a
		// run that still meets a held claim re-arms the deferred retry.
		recovery := time.AfterFunc(pendingSyncRecoveryDelay, func() {
			rctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			ids, err := ms.PendingSyncRepoIDs(rctx)
			if err != nil {
				logger.Warn("pending sync boot recovery failed", "err", err)
				return
			}
			for _, id := range ids {
				if err := queue.EnqueueSync(rctx, upsync.Job{Org: org, RepoID: id}); err != nil {
					logger.Warn("pending sync boot recovery enqueue failed", "repo", id, "err", err)
				}
			}
		})
		cleanup = func() {
			recovery.Stop()
			queue.Close()
		}
		if sweep != nil {
			stopSweep := jobs.StartSweepTicker(sweep.Sweeper, sweep.Interval)
			cleanup = func() {
				recovery.Stop()
				stopSweep()
				queue.Close()
			}
		}
	}

	decorate := func(inner ref.RefStore) ref.RefStore {
		return events.NewRefStore(inner, events.RefStoreOptions{
			Org:             org,
			Repos:           ms,
			Webhooks:        whStore,
			EnqueueDelivery: queue.EnqueueWebhookDelivery,
			EnqueueSync:     enqueueSyncIfDue(ms, queue.EnqueueSync, org),
			Logger:          logger,
		})
	}
	return queue, decorate, cleanup, nil
}

// hookDeliveryAttempts bounds synchronous local-mode hook deliveries: the
// update hook blocks the push until its deliveries finish, so retries are
// short (ADR gap: local mode has no cross-process durable queue).
const hookDeliveryAttempts = 3

// hookWebhookAllowPrivate reads the SSRF escape hatch (issue #53) inside
// the hook subprocess. Unlike serve's fail-fast resolveWebhookAllowPrivate,
// a malformed value here must not abort the push: log and keep the guard
// strict (the safe direction).
func hookWebhookAllowPrivate(logger *slog.Logger) bool {
	assertf(logger != nil, "hookWebhookAllowPrivate: nil logger")
	raw := os.Getenv(envWebhookAllowPrivate)
	if raw == "" {
		return false
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		logger.Warn("ignoring malformed webhook escape hatch; SSRF guard stays strict",
			"env", envWebhookAllowPrivate, "value", raw, "err", err)
		return false
	}
	return v
}

// wireHookEventing points githttp.WrapHookRefStore at the events.RefStore
// decorator for the `codestorage hook` subprocess, so git pushes emit push
// events exactly like API writes (docs/reference/webhooks.md). It reads the same env the hook
// itself uses (serve exports PG_URL; CODESTORAGE_ORG is inherited). The
// returned cleanup must run after the hook finishes: in local mode it waits
// for in-flight deliveries (the subprocess IS the queue), in pg mode it
// closes the insert-only River client.
//
// Cross-process queue story (ADR): pg-mode hooks insert durable River jobs
// worked by the serve process. Local mode has no shared durable queue, so
// hook deliveries run synchronously (bounded attempts) inside the hook
// subprocess, and git-surface pushes do NOT schedule upstream syncs (API
// writes still do); queued local work dies with the process.
func wireHookEventing(ctx context.Context, logger *slog.Logger) func() {
	org := os.Getenv(envOrg)
	dbURL := firstNonEmpty(os.Getenv("CODESTORAGE_PG_URL"), os.Getenv("PG_URL"))
	if org == "" || dbURL == "" {
		// No eventing wiring (e.g. direct hook invocation outside serve):
		// keep the identity default so pushes still work, just eventless.
		return func() {}
	}

	var cleanups []func()
	cleanup := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}

	// Secrets codec (issue #54): hook subprocesses inherit serve's
	// environment, so a serve process that booted with a valid
	// CODESTORAGE_SECRETS_KEY hands the same value down here. A malformed
	// value (possible only for direct hook invocation outside serve, which
	// validates at boot) degrades to eventless — fail closed for secrets:
	// no delivery ever signs with a wrong or plaintext-passthrough key.
	secretsCodec, err := resolveSecretsCodec(os.Getenv)
	if err != nil {
		logger.Error("hook eventing disabled: secrets key", "err", err)
		return func() {}
	}

	// Hooks never run DDL: meta.Open without InitSchema (serve owns it).
	ms, err := meta.Open(ctx, dbURL, meta.WithSecretsCodec(secretsCodec))
	if err != nil {
		// Fail open (eventless) rather than denying the push: the ref
		// store open below would fail too if the DB were truly down.
		logger.Error("hook eventing disabled: open meta store", "err", err)
		return func() {}
	}
	cleanups = append(cleanups, ms.Close)

	whStore := events.MetaWebhookStore{Store: ms}
	var (
		enqueueDelivery func(ctx context.Context, d events.Delivery) error
		enqueueSync     func(ctx context.Context, repoID string) error
	)
	if isPGURL(dbURL) {
		q, err := jobs.NewRiverInsertOnly(ctx, dbURL)
		if err != nil {
			logger.Error("hook eventing disabled: river insert-only client", "err", err)
			cleanup()
			return func() {}
		}
		cleanups = append(cleanups, q.Close)
		enqueueDelivery = q.EnqueueWebhookDelivery
		enqueueSync = enqueueSyncIfDue(ms, q.EnqueueSync, org)
	} else {
		// Local mode: deliver synchronously (bounded) before hook exit.
		deliverer := &events.Deliverer{
			Webhooks:    whStore,
			MaxAttempts: hookDeliveryAttempts,
			// The hook subprocess inherits serve's environment, so the
			// SSRF escape hatch (issue #53) matches the serve process.
			// Lenient parse, strict direction: a malformed value keeps
			// the guard ON (fail closed) rather than failing the push.
			AllowPrivate: hookWebhookAllowPrivate(logger),
			Logger:       logger,
		}
		local := jobs.NewLocal(deliverer, func(context.Context, upsync.Job) error {
			panic("local hook queue must never run sync jobs")
		}, nil /* hook subprocesses never fold */, logger)
		cleanups = append(cleanups, local.Close) // waits for in-flight deliveries
		enqueueDelivery = local.EnqueueWebhookDelivery
		enqueueSync = nil // documented local-mode gap (see doc comment)
	}

	githttp.WrapHookRefStore = func(_ context.Context, st ref.RefStore) ref.RefStore {
		return events.NewRefStore(st, events.RefStoreOptions{
			Org:             org,
			Repos:           ms,
			Webhooks:        whStore,
			EnqueueDelivery: enqueueDelivery,
			EnqueueSync:     enqueueSync,
			Logger:          logger,
		})
	}
	return cleanup
}

// slogForHook builds the hook subprocess logger (stderr reaches the git
// client prefixed with "remote:", so keep it warn-level).
func slogForHook() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}
