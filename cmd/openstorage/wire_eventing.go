// wire_eventing.go is the phase-5 sibling of wire.go: it assembles the
// eventing/sync stack (docs/reference/webhooks.md, docs/archive/phases/07-implementation-roadmap.md p5) from the slice-owned packages —
// jobs queue (River on pg / in-process in local mode), webhook deliverer,
// upstream-sync worker with loopback push, and the events.RefStore CASRef
// decorator — for both the serve process and the `openstorage hook`
// subprocess git re-execs.
package main

import (
	"context"
	"crypto"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"slices"
	"strconv"
	"time"

	"github.com/coder/open-storage/internal/auth"
	"github.com/coder/open-storage/internal/events"
	"github.com/coder/open-storage/internal/githttp"
	"github.com/coder/open-storage/internal/jobs"
	"github.com/coder/open-storage/internal/meta"
	"github.com/coder/open-storage/internal/ref"
	upsync "github.com/coder/open-storage/internal/sync"
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

// enqueueSyncOrForward implements the events.RefStoreOptions.EnqueueSync
// contract (docs/explanation/domain-model.md invariant 11): a won persistent
// ref update pokes the sync machinery when the repo has an upstream row.
//
// Default (pull) direction: schedule an inbound sync only when no run is
// currently in flight — the in-flight check is what keeps the sync worker's
// own loopback pushes from re-triggering themselves. Deliberately NO
// RequestSync here (issue #79): the pending marker would turn every
// loopback push into a follow-up run, self-perpetuating the sync chain.
// Push-triggered syncs carry no 202 promise, so dropping one under an
// in-flight claim stays the documented invariant-11 behavior.
//
// forward_on_push repos (issue #90) REROUTE the poke outbound instead:
// pulling first would force local refs back to the upstream's pre-push
// state (the loopback push uses upstream-wins refspecs) and the forward's
// later snapshot would then silently lose the very push it was meant to
// publish. The worker schedules the mirror-back pull strictly AFTER a
// successful forward (Worker.AfterForward). Unlike the pull poke, the
// forward poke IS marked durable (RequestPush): marking cannot
// self-perpetuate here — forward runs never CAS local refs, and a sync
// run's own loopback pushes carry the sync claim, which suppresses the
// hook-side poke entirely (wireHookEventing).
//
// callerDenied gates the FORWARD reroute only (Codex rounds 3 + security
// on #90): the forward publishes every local head and tag with the stored
// upstream credential, so it must not be implicitly triggerable by (a) a
// caller whose token carries ANY ref-policy op — one allowed to update
// only the refs its policies name — or (b) a write-only token lacking
// git:read, which could otherwise exfiltrate repository content it cannot
// clone to an upstream it can read. The explicit POST /repos/push-upstream
// fails closed on both (policyDeniesPullUpstream coarseness + required
// git:read scope); the poke matches it. The write itself was accepted and
// stays local; convergence resumes on the next authorized trigger.
// Deliberately NOT falling through to the pull poke: pulling now would
// force-revert the write that was just accepted. The pull poke stays
// ungated — documented invariant-11 behavior this slice does not change.
func enqueueSyncOrForward(ms meta.EventingStore, enqueuePull, enqueuePush func(ctx context.Context, job upsync.Job) error, org string, callerDenied func(ctx context.Context) bool) func(ctx context.Context, repoID string) error {
	assertf(ms != nil && enqueuePull != nil && enqueuePush != nil && org != "" && callerDenied != nil,
		"enqueueSyncOrForward: missing deps")
	return func(ctx context.Context, repoID string) error {
		up, err := ms.GetRepoUpstream(ctx, repoID)
		if errors.Is(err, meta.ErrNotFound) {
			return nil // no upstream link: nothing to schedule
		}
		if err != nil {
			return err
		}
		if up.ForwardOnPush {
			if callerDenied(ctx) {
				return nil // fail closed: no implicit full-mirror publication (see doc)
			}
			if err := ms.RequestPush(ctx, repoID, false); err != nil {
				if errors.Is(err, meta.ErrNotFound) {
					return nil // detached between the reads: nothing to forward
				}
				return err
			}
			return enqueuePush(ctx, upsync.Job{Org: org, RepoID: repoID})
		}
		if up.Syncing {
			return nil // run in flight (or our own loopback push): skip
		}
		return enqueuePull(ctx, upsync.Job{Org: org, RepoID: repoID})
	}
}

// anyRefPolicyOps reports whether any parsed policy rule carries ops — the
// same coarseness as the API's policyDeniesPullUpstream: the pattern is
// not consulted because the forward publishes every synced ref.
func anyRefPolicyOps(policies []auth.RefPolicy) bool {
	for _, rule := range policies {
		if len(rule.Ops) > 0 {
			return true
		}
	}
	return false
}

// forwardDeniedFromContext resolves enqueueSyncOrForward's callerDenied
// for the serve process, whose ref mutations run under the request
// context: the auth middleware injects the verified claims, and absent
// claims mean a server-initiated write (import machinery, maintenance),
// which is trusted by definition. A caller is denied when its token lacks
// git:read (write-only tokens must not export the repo — see the
// enqueueSyncOrForward doc) or carries any ref-policy op. A refs claim
// that fails to parse counts as DENIED — a malformed policy must not
// grant a broader implicit trigger than a parseable one.
func forwardDeniedFromContext(ctx context.Context) bool {
	claims, ok := auth.ClaimsFromContext(ctx)
	if !ok {
		return false
	}
	if !claims.HasScope(auth.ScopeGitRead) {
		return true
	}
	var policies []auth.RefPolicy
	if len(claims.Refs) > 0 {
		var err error
		policies, err = auth.ParseRefPolicies(string(claims.Refs))
		if err != nil {
			return true // fail closed
		}
	}
	return anyRefPolicyOps(auth.FoldOps(policies, claims.Ops))
}

// hookCallerDeniedForward is forwardDeniedFromContext for the hook
// subprocess, whose caller identity arrives as environment (the same
// EnvRefsPolicy / EnvOps the update hook's enforceRefPolicy consumes,
// plus EnvScopes; all set only when the claim is non-empty). Malformed
// values count as denied — the update hook fails malformed-policy pushes
// anyway, but the poke must not assume it ran first — and so does a
// missing/unparseable scope list: only a proven git:read forwards.
func hookCallerDeniedForward() bool {
	var scopes []string
	if raw := os.Getenv(githttp.EnvScopes); raw != "" {
		if err := json.Unmarshal([]byte(raw), &scopes); err != nil {
			return true // fail closed
		}
	}
	if !slices.Contains(scopes, auth.ScopeGitRead) {
		return true
	}
	policyJSON := os.Getenv(githttp.EnvRefsPolicy)
	opsJSON := os.Getenv(githttp.EnvOps)
	if policyJSON == "" && opsJSON == "" {
		return false
	}
	var policies []auth.RefPolicy
	if policyJSON != "" {
		var err error
		policies, err = auth.ParseRefPolicies(policyJSON)
		if err != nil {
			return true // fail closed
		}
	}
	var ops []string
	if opsJSON != "" {
		if err := json.Unmarshal([]byte(opsJSON), &ops); err != nil {
			return true // fail closed
		}
	}
	return anyRefPolicyOps(auth.FoldOps(policies, ops))
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
func buildServeEventing(ctx context.Context, ms meta.Store, dbURL, org, loopbackBaseURL string, key loopbackKey, sweep *jobs.SweepSchedule, lfsReclaim *jobs.LFSReclaimSchedule, absence *jobs.AbsenceReconcileSchedule, repack *jobs.Repacker, webhookAllowPrivate bool, syncEgress syncEgressSettings, logger *slog.Logger) (jobs.Queue, func(inner ref.RefStore) ref.RefStore, func(), error) {
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
		// Sync egress guard policy (see upsync/egress.go, issue #139):
		// operators syncing with genuinely internal forges opt private
		// receiver classes in; metadata/special ranges stay denied. The
		// pull side's transitional default is allow (0.6.x compatibility)
		// until a future release flips it — logSyncEgressWarnings
		// announces the flip at boot.
		PushEgressAllowPrivate: syncEgress.pushAllowPrivate,
		PullEgressAllowPrivate: syncEgress.pullAllowPrivate,
	}
	// The mirror-back pull after every successful outbound run (issue
	// #90; explicit runs included — see the AfterForward field doc)
	// enqueues through the same late-bound queue.
	worker.AfterForward = func(ctx context.Context, job upsync.Job) error {
		assertf(queue != nil, "eventing: forward finished before queue construction")
		return queue.EnqueueSync(ctx, job)
	}

	cleanup := func() { queue.Close() }
	if isPGURL(dbURL) {
		// worker.Requeue/RequeuePush stay nil on pg: river's insert-time
		// uniqueness would swallow a requeue of a still-running job; the
		// river sync/push workers schedule successors transactionally
		// instead (issue #79).
		q, err := jobs.NewRiver(ctx, dbURL, deliverer, worker.Run, worker.RunPush, ups, sweep, lfsReclaim, absence, repack, logger)
		if err != nil {
			return nil, nil, nil, err
		}
		queue = q
	} else {
		queue = jobs.NewLocal(deliverer, worker.Run, worker.RunPush, repack, logger)
		// Pending pull-upstream requests that arrive while a run is in
		// flight are re-enqueued by the finishing run (issue #79); the
		// in-process queue never deduplicates against a running run, so
		// a plain re-enqueue is sound here. The push lane mirrors it and
		// additionally drives the transient-retry backoff timers (#90).
		worker.Requeue = queue.EnqueueSync
		worker.RequeuePush = queue.EnqueuePush
		// Boot recovery (issue #86): queued local work and deferred retry
		// timers die with the process, but the SyncPending/PushPending
		// markers are durable — re-enqueue marked repos so accepted
		// pull-upstream and push-upstream (#90) requests survive restarts
		// (in particular the restart that applies
		// CODESTORAGE_SYNC_STEAL_LEGACY). Delayed so the loopback
		// listener is serving before the first recovered run pushes; a
		// run that still meets a held claim re-arms the deferred retry.
		recovery := time.AfterFunc(pendingSyncRecoveryDelay, func() {
			rctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			ids, err := ms.PendingSyncRepoIDs(rctx)
			if err != nil {
				logger.Warn("pending sync boot recovery failed", "err", err)
			} else {
				for _, id := range ids {
					if err := queue.EnqueueSync(rctx, upsync.Job{Org: org, RepoID: id}); err != nil {
						logger.Warn("pending sync boot recovery enqueue failed", "repo", id, "err", err)
					}
				}
			}
			pushIDs, err := ms.PendingPushRepoIDs(rctx)
			if err != nil {
				logger.Warn("pending push boot recovery failed", "err", err)
				return
			}
			for _, id := range pushIDs {
				// Re-mark BEFORE enqueueing: the listing includes repos
				// whose previous process died AFTER its claim consumed
				// the marker (orphaned push_syncing rows — always dead
				// here, local mode is single-process). Their recovered
				// run meets the unexpired dead lease and skips; only a
				// set marker arms the deferred retry that steals it
				// after expiry. MAX-coalescing keeps any existing level
				// (never downgrades force), and the dead claim's own
				// force mandate is inherited at the steal regardless
				// (push_claim_force).
				if err := ms.RequestPush(rctx, id, false); err != nil {
					logger.Warn("pending push boot recovery re-mark failed", "repo", id, "err", err)
					continue
				}
				if err := queue.EnqueuePush(rctx, upsync.Job{Org: org, RepoID: id}); err != nil {
					logger.Warn("pending push boot recovery enqueue failed", "repo", id, "err", err)
				}
			}
		})
		cleanup = func() {
			recovery.Stop()
			queue.Close()
		}
		if sweep != nil {
			stopSweep := jobs.StartSweepTicker(sweep)
			prev := cleanup
			cleanup = func() {
				stopSweep()
				prev()
			}
		}
		if lfsReclaim != nil {
			// LFS reclamation ticker (issue #73): local mode's analogue
			// of the River periodic job.
			stopReclaim := jobs.StartLFSReclaimTicker(lfsReclaim)
			prev := cleanup
			cleanup = func() {
				stopReclaim()
				prev()
			}
		}
		if absence != nil {
			// Absence reconciliation ticker (issue #101): local mode's
			// analogue of the River periodic job.
			stopAbsence := jobs.StartAbsenceReconcileTicker(absence)
			prev := cleanup
			cleanup = func() {
				stopAbsence()
				prev()
			}
		}
	}

	decorate := func(inner ref.RefStore) ref.RefStore {
		return events.NewRefStore(inner, events.RefStoreOptions{
			Org:             org,
			Repos:           ms,
			Webhooks:        whStore,
			EnqueueDelivery: queue.EnqueueWebhookDelivery,
			EnqueueSync:     enqueueSyncOrForward(ms, queue.EnqueueSync, queue.EnqueuePush, org, forwardDeniedFromContext),
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
// decorator for the `openstorage hook` subprocess, so git pushes emit push
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
		for _, cleanup := range slices.Backward(cleanups) {
			cleanup()
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
		// The hook subprocess serves exactly one push by one caller, so
		// the denial verdict is a process-wide constant from its env.
		denied := hookCallerDeniedForward()
		enqueueSync = enqueueSyncOrForward(ms, q.EnqueueSync, q.EnqueuePush, org,
			func(context.Context) bool { return denied })
		if os.Getenv(githttp.EnvSyncClaim) != "" {
			// This receive-pack IS a sync run's own loopback push (the
			// claim-fenced token, issue #86): poking the sync seam from it
			// would chain runs. The pull poke was already suppressed by the
			// in-flight claim check; the forward poke (issue #90) must be
			// suppressed here or every inbound mirror update would enqueue
			// an outbound no-op forward. Push webhooks still emit.
			enqueueSync = nil
		}
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
		neverRun := func(context.Context, upsync.Job) error {
			panic("local hook queue must never run sync jobs")
		}
		local := jobs.NewLocal(deliverer, neverRun, neverRun, nil /* hook subprocesses never fold */, logger)
		cleanups = append(cleanups, local.Close) // waits for in-flight deliveries
		enqueueSync = nil                        // documented local-mode gap (see doc comment)
		enqueueDelivery = local.EnqueueWebhookDelivery
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
