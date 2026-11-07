package sync

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	stdsync "sync"
	"syscall"
	"time"

	"github.com/coder/openstorage/internal/envconfig"
	"github.com/coder/openstorage/internal/gitlimit"
	"github.com/coder/openstorage/internal/meta"
)

// RepoInfo is the minimal repo identity a sync run needs: the storage key
// (loopback push path element) and the JWT `repo` claim value (event
// repository.url).
type RepoInfo struct {
	RepoID string
	URL    string
}

// RepoLookup resolves a live repo by org + repo_id (production adapter:
// meta.Store.GetRepoByID). Unknown or soft-deleted repos must return an
// error wrapping ErrNotFound; the worker then drops the job silently.
type RepoLookup func(ctx context.Context, org, repoID string) (RepoInfo, error)

// Loopback is the worker's push target: this server's own git smart-HTTP
// endpoint (phase-5 Decision 4 — sync reuses pack storage, ref policies,
// CAS, and push-event emission by doing a real `git push`).
type Loopback struct {
	// BaseURL prefixes push URLs as {BaseURL}/{repo_id}.git. Any scheme git
	// understands works (http(s):// in production, file:// in tests). It
	// must terminate in THIS process when Token mints keys only this
	// process can verify (issue #45): the serve command passes its own
	// bound listener, never the external (load-balanced) base URL.
	BaseURL string
	// Token mints a short-lived git:write JWT for the loopback push (basic
	// auth `t:<jwt>`, docs/explanation/git-protocol-and-namespaces.md). The token is passed to git via a credential helper
	// and never logged. May return "" when BaseURL needs no auth (file://).
	// ttl and holder FENCE the push against a claim steal (issue #86):
	// implementations must cap the token lifetime at ttl (no new push
	// request authenticates once the lease can be stolen) and carry
	// holder as the auth.Claims.SyncClaim so the update hook refuses ref
	// updates from a run whose claim was stolen mid-receive-pack.
	Token func(ctx context.Context, org string, repo RepoInfo, ttl time.Duration, holder string) (string, error)
}

// defaultRunTimeout caps one sync run end-to-end.
const defaultRunTimeout = 10 * time.Minute

// detachedOpTimeout bounds each lifecycle step that runs on a context
// DETACHED from the run budget (started/terminal emissions, finishSync,
// requeue): they must survive run-context death without running unbounded.
const detachedOpTimeout = 30 * time.Second

// claimLeaseSlack pads the sync claim lease beyond the worst-case claim
// window. From claim to release a run spends at most: the started
// emission's detached window (detachedOpTimeout — it runs between the
// claim and the transfer) + the remaining run budget (the transfer runs
// under the run context, ≤ Timeout) + finishSync's detached window
// (detachedOpTimeout, which internally bounds its retries). The slack
// absorbs scheduler stalls, DB latency, and clock granularity on top of
// that hard bound. On Postgres both sides of the expiry comparison
// evaluate on the database clock, so node clock skew does not participate.
// A holder that still outlives the lease (VM freeze) remains SAFE: its
// late FinishSync fails the holder-token guard (ErrClaimLost) and
// abandons without touching the thief's claim.
const claimLeaseSlack = 5 * time.Minute

// claimLease derives the lease for one run from its effective timeout.
func claimLease(timeout time.Duration) time.Duration {
	assertf(timeout > 0, "claimLease: non-positive timeout %v", timeout)
	return timeout + 2*detachedOpTimeout + claimLeaseSlack
}

func DefaultClaimLease() time.Duration { return claimLease(defaultRunTimeout) }

// ClaimHeldWithin is how long after its start a run with the default
// Timeout can still hold its claim when its context ends at deadline: it
// claims before then and holds the claim for one lease. claimFenceMargin
// absorbs clock skew against the database clock, which times the lease.
func ClaimHeldWithin(deadline time.Duration) time.Duration {
	return deadline + claimLease(defaultRunTimeout) + claimFenceMargin
}

// claimFenceMargin is how long BEFORE the lease expiry the run's loopback
// token dies (the push fence, issue #86): a steal is only possible at
// expiry, so no zombie can authenticate a new push request within the
// margin of one. Structurally safe for live runs: the push starts at most
// timeout + one detached window after the claim, and
// claimLease - claimFenceMargin = timeout + 4min leaves ≥ 3min of token
// life at that point. The margin also absorbs clock skew between the
// database clock (which governs the expiry on Postgres) and this
// process's clock (which mints the token), and bounds how stale an
// in-flight request can be: with the run context killing the git client
// (exec.CommandContext) well before the lease can expire, the residual
// exposure is a worker frozen (SIGSTOP/VM pause) past its own run budget
// whose orphaned localhost push request stays continuously in flight
// longer than this margin.
const claimFenceMargin = 2 * time.Minute

// processTag identifies this process in claim-holder tokens (observability
// only; uniqueness comes from the per-run nonce).
var processTag = func() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}()

// newClaimHolder mints a per-RUN claim token. Uniqueness per claim (not
// per process) matters: FinishSync must clear exactly the claim its run
// took, or a lingering late finisher from the same process could clear a
// successor's claim.
func newClaimHolder() string {
	var buf [8]byte
	_, err := rand.Read(buf[:])
	assertf(err == nil, "crypto/rand failed: %v", err)
	return processTag + "-" + hex.EncodeToString(buf[:])
}

// Worker executes sync runs (docs/reference/webhooks.md lifecycle): claim the run, emit
// repo.sync.started, mirror-fetch the upstream, loopback-push to storage,
// then emit succeeded/failed with the exact documented error strings.
// DELETE /repos/base at any point cancels silently (no failed event).
type Worker struct {
	Upstreams   UpstreamStore
	Credentials CredentialStore
	Repos       RepoLookup
	Events      EventSink
	Loopback    Loopback
	Logger      *slog.Logger
	// Timeout caps one run; 0 means defaultRunTimeout.
	Timeout time.Duration
	// AllowProtocol overrides GIT_ALLOW_PROTOCOL for the run's git
	// transfers; empty means "http:https". Only tests widen it (file://
	// endpoints); production keeps the default so hostile upstream rows
	// can never reach local files, ssh, or transport helpers (ext::).
	AllowProtocol string
	// deferMu guards deferredRetry: at most ONE pending deferred retry
	// timer per repository (issue #86 — request volume against a dead
	// holder's unexpired lease must not accumulate timers).
	deferMu       stdsync.Mutex
	deferredRetry map[string]bool

	// Requeue, when non-nil, schedules a follow-up run when FinishSync
	// reports sync requests that arrived after this run's claim
	// (Upstream.SyncPending, issue #79): this run's upstream fetch may
	// predate those requests, so dropping them would break the
	// pull-upstream 202 promise. Local-mode wiring points it at the
	// queue's EnqueueSync (safe there: the in-process queue never
	// deduplicates against a running run). River wiring leaves it nil —
	// its insert-time uniqueness would swallow a follow-up inserted while
	// this job's row is still running, so the river sync worker schedules
	// successors transactionally at job completion instead (jobs/river.go).
	Requeue func(ctx context.Context, job Job) error

	// RequeuePush is the outbound lane's Requeue (issue #90): follow-up
	// AND transient-retry scheduling for push-upstream runs. Same wiring
	// split: local mode points it at EnqueuePush, river leaves it nil and
	// schedules successors transactionally (jobs/river.go pushWorker).
	RequeuePush func(ctx context.Context, job Job) error
	// AfterForward, when non-nil, enqueues one inbound sync after every
	// SUCCESSFUL outbound run. On ForwardOnPush repos it is the
	// mirror-back half of the upstream model ("pushes are proxied to the
	// provider and changes mirrored back"): their push->pull poke is
	// rerouted to the forward (cmd wiring), so this restores the
	// freshness pull strictly AFTER the forward landed, never racing it.
	// On explicit push-upstream runs it heals the pull‖push interleaving:
	// a concurrent pull that fetched upstream BEFORE the forward would
	// otherwise force-write its pre-forward snapshot locally and stand
	// divergent until some unrelated future trigger.
	// RunPush marks the request durable (RequestSync) BEFORE invoking
	// this hook — an enqueue that coalesces with an in-flight pull run
	// (river insert-time uniqueness, a held local claim) then survives to
	// that run's completion gate instead of silently dropping the
	// promised mirror. Enqueue failures are logged; the marker carries
	// the request to the next finisher (or, on the local schedule, the
	// next sweep). Both wirings
	// point it at the queue's EnqueueSync.
	AfterForward func(ctx context.Context, job Job) error

	// Local, when non-nil, is the durable local schedule (SQLite and
	// S3WAL, issue #266, ADR-50, ADR-51): runs claim through ClaimDue* (refused claims end the
	// run with nothing written), and one end-of-run write schedules any
	// retry, follow-up, or mirror-back in the row. Requeue, RequeuePush,
	// AfterForward and the deferred-retry timers are then unused: a
	// LocalScheduler sweep turns due rows into runs.
	Local LocalLanes

	// WaitPublication, when non-nil, blocks until no in-flight push
	// still holds the repo's cache refs unpublished
	// (githttp.WaitForPushPublication). A local run calls it after its
	// claim and before its snapshot and started event, and so does a
	// River push run: a hook records intent at the authority CAS, before
	// receive-pack publishes the cache ref. A failed wait restores the
	// consumed request. Without Local, Upstreams must be a
	// ShutdownRestorer, and only RunPush waits.
	WaitPublication func(ctx context.Context, repoID string) error

	// DetachPoll is the cadence of the outbound push watchdog that
	// re-reads the upstream row WHILE `git push` runs and kills the
	// subprocess when the repo is detached or the claim stolen mid-push
	// (issue #90); 0 means defaultDetachPoll. Only tests shorten it.
	DetachPoll time.Duration

	// PushEgressAllowPrivate relaxes the outbound push egress guard's
	// private receiver classes (resolved from OPENSTORAGE_SYNC_ALLOW_PRIVATE
	// with OPENSTORAGE_SYNC_PUSH_ALLOW_PRIVATE as the deprecated push-only
	// spelling, see egress.go): loopback/RFC1918/link-local/ULA upstreams
	// for genuinely internal forges. Metadata and the special-purpose
	// registry stay denied either way — every http(s) push dials through
	// the guard's vetting proxy. Tests set it (fixture upstreams are
	// 127.0.0.1).
	PushEgressAllowPrivate bool

	// PullEgressAllowPrivate is the inbound twin (issue #139): every
	// http(s) upstream fetch dials through the same vetting proxy, and
	// this relaxes only its private receiver classes. The serve wiring
	// resolves it from the unified OPENSTORAGE_SYNC_ALLOW_PRIVATE knob,
	// whose TRANSITIONAL default keeps it true — internal-forge mirrors
	// are a mainstream released deployment shape, and a future release
	// flips the unset default to deny (boot warning announces the flip).
	// Metadata and the special-purpose registry stay denied either way.
	// Tests set it (fixture upstreams are 127.0.0.1).
	PullEgressAllowPrivate bool
}

// allowProtocol resolves the effective GIT_ALLOW_PROTOCOL value.
func (w *Worker) allowProtocol() string {
	if w.AllowProtocol == "" {
		return "http:https"
	}
	return w.AllowProtocol
}

func (w *Worker) assertReady() {
	assertf(w.Upstreams != nil, "Worker.Upstreams is required")
	assertf(w.Credentials != nil, "Worker.Credentials is required")
	assertf(w.Repos != nil, "Worker.Repos is required")
	assertf(w.Events != nil, "Worker.Events is required")
	assertf(w.Loopback.BaseURL != "" && w.Loopback.Token != nil, "Worker.Loopback is required")
	assertf(w.Logger != nil, "Worker.Logger is required")
}

// syncFailure carries the exact documented failure string plus the real
// cause for server-side logs.
type syncFailure struct {
	code  string // one of the Fail* constants
	cause error
}

func (f *syncFailure) Error() string { return f.code + ": " + f.cause.Error() }
func (f *syncFailure) Unwrap() error { return f.cause }

// errDetached signals that DELETE /repos/base raced the run — the
// documented semantics are silent cancellation (no failed event).
var errDetached = errors.New("upstream detached during sync")

// errClaimStolen signals the pre-push barrier found the claim held by a
// DIFFERENT holder: this run outlived its lease and was stolen (issue
// #86). Abandoning BEFORE the loopback push keeps the zombie's stale fetch
// snapshot from overwriting the thief's newer push; like errDetached, the
// run ends silently (the thief owns the lifecycle).
var errClaimStolen = errors.New("sync claim stolen during run")

// failureCode maps a run error onto the documented repo.sync.failed string.
func failureCode(err error) string {
	assertf(err != nil, "failureCode: nil error")
	if f, ok := errors.AsType[*syncFailure](err); ok {
		return f.code
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return FailGeneric
	}
	return FailInternal
}

// localPublicationRetryDelay is how long a run restored after a failed
// publication wait waits before it is due again.
const localPublicationRetryDelay = 5 * time.Second

// localPublicationWaitTimeout bounds one publication wait, so a hold that
// never releases cannot keep the claim for the whole run budget.
const localPublicationWaitTimeout = time.Minute

func (w *Worker) waitPublication(ctx context.Context, repoID string) error {
	if w.WaitPublication == nil {
		return nil
	}
	_, restores := w.Upstreams.(ShutdownRestorer)
	assertf(w.Local != nil || restores, "WaitPublication needs the local schedule or a ShutdownRestorer")
	wctx, cancel := context.WithTimeout(ctx, localPublicationWaitTimeout)
	defer cancel()
	return w.WaitPublication(wctx, repoID)
}

// restoreLocal ends a claimed local run that got no fresh snapshot: its
// publication wait failed (no event emitted yet), or a push run's loopback
// fetch found a push-held ref stale (after its started event, like a
// shutdown restore). Nothing was forwarded, so the run hands its request
// back to the row (restore) instead of counting a run.
func (w *Worker) restoreLocal(ctx context.Context, job Job, holder, lane string, waitErr error, finish func(ctx context.Context) (Upstream, error)) error {
	w.Logger.Warn("no fresh snapshot; request restored for a later run",
		"org", job.Org, "repo", job.RepoID, "lane", lane, "err", waitErr)
	_, err := w.retryFinish(ctx, lane, job.RepoID, finish)
	switch {
	case errors.Is(err, ErrNotFound):
		return nil // detached meanwhile: nothing to restore
	case errors.Is(err, ErrClaimLost):
		w.Logger.Warn("claim lost before the restore; abandoning",
			"org", job.Org, "repo", job.RepoID, "lane", lane, "holder", holder)
		return nil
	case err != nil:
		return fmt.Errorf("restore %s %s: %w", lane, job.RepoID, err)
	}
	return nil
}

// Run executes one sync run for job. It returns an error for logging by the
// queue; documented outcomes (detached, overlapping run, failed sync with an
// emitted event) are not errors to the caller beyond that. Two errors steer
// River's sync job: ErrShutdown (keep the job) and ErrRepoGone (complete
// it).
func (w *Worker) Run(ctx context.Context, job Job) error {
	w.assertReady()
	assertf(job.Org != "" && job.RepoID != "", "Run: incomplete job %+v", job)
	timeout := w.Timeout
	if timeout <= 0 {
		timeout = defaultRunTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	repo, err := w.Repos(ctx, job.Org, job.RepoID)
	if errors.Is(err, ErrNotFound) {
		// Repo deleted while queued: nothing to sync or emit.
		if w.Local != nil {
			return nil
		}
		return fmt.Errorf("sync %s: %w", job.RepoID, ErrRepoGone)
	}
	if err != nil {
		return fmt.Errorf("resolve repo %s: %w", job.RepoID, err)
	}
	assertf(repo.RepoID == job.RepoID, "RepoLookup returned %q for %q", repo.RepoID, job.RepoID)

	holder := newClaimHolder()
	var up Upstream
	var stolenFrom string
	if w.Local != nil {
		c, err := w.claimLocal(ctx, func(cctx context.Context) (LaneClaim, error) {
			return w.Local.ClaimDueSync(cctx, job.RepoID, holder, claimLease(timeout))
		})
		switch {
		case errors.Is(err, ErrNotFound):
			return nil // detached before start: cancel silently
		case err != nil:
			return fmt.Errorf("begin sync %s: %w", job.RepoID, err)
		case !c.Claimed:
			return nil // busy or nothing due: the row keeps the work for a later sweep
		}
		up, stolenFrom = c.Up, c.StolenFrom
	} else {
		if shutdownAborted(ctx) {
			// Not claimed: the request stays marked (issue #343).
			return fmt.Errorf("begin sync %s: %w", job.RepoID, ErrShutdown)
		}
		cctx, ccancel := claimContext(ctx)
		up, stolenFrom, err = w.Upstreams.BeginSync(cctx, job.RepoID, holder, claimLease(timeout))
		ccancel()
		switch {
		case errors.Is(err, ErrNotFound):
			return nil // detached before start: cancel silently (docs/reference/webhooks.md)
		case errors.Is(err, ErrSyncInProgress):
			// Overlapping run: skip (invariant 11). A LIVE holder covers any
			// pending request at its FinishSync (#85) — but a dead holder's
			// unexpired lease refuses the steal AND never finishes, and this
			// run was the request's only local vehicle: schedule one retry
			// for just after the lease expires (issue #86).
			w.deferPendingRetry(ctx, job)
			return nil
		case err != nil:
			return shutdownErr(ctx, fmt.Errorf("begin sync %s: %w", job.RepoID, err))
		}
	}
	if stolenFrom != "" {
		// The previous holder's lease expired without a FinishSync — its
		// process died mid-run (issue #86). Loud on purpose: steals are
		// normal only after crashes; a pattern of them means the lease is
		// racing live runs (clock trouble or budget drift).
		w.Logger.Warn("stole expired sync claim",
			"org", job.Org, "repo", job.RepoID, "holder", holder, "stolen_from", stolenFrom)
	}

	if w.Local != nil {
		if err := w.waitPublication(ctx, job.RepoID); err != nil {
			return w.restoreLocal(ctx, job, holder, "sync", err, func(fctx context.Context) (Upstream, error) {
				return w.Local.FinishSyncOutcome(fctx, job.RepoID, holder,
					meta.SyncOutcome{Kind: meta.OutcomeRestore, Delay: localPublicationRetryDelay})
			})
		}
	}

	startedAt := eventTime(time.Now())
	isFirst := up.SuccessCount == 0
	base := Event{
		Org:         job.Org,
		Repository:  newEventRepository(repo),
		RunCount:    up.RunCount,
		IsFirstSync: isFirst,
		StartedAt:   startedAt,
	}
	started := base
	started.Type = EventSyncStarted
	w.emitDetached(ctx, job.Org, EventSyncStarted, started.marshal())

	runErr := w.transfer(ctx, job, repo, up, holder)
	if errors.Is(runErr, errDetached) {
		return nil // detach raced the run: the row is gone, cancel silently
	}
	if errors.Is(runErr, errClaimStolen) {
		// Lease expired mid-run and the claim was stolen before our push:
		// abandon without finishing or terminal events — but loudly, so
		// operators can spot leases racing live runs.
		w.Logger.Warn("sync claim stolen mid-run; abandoning before the loopback push",
			"org", job.Org, "repo", job.RepoID, "holder", holder)
		return nil
	}

	if runErr != nil && shutdownAborted(ctx) {
		// Shutdown cancelled the run: keep its request for the next
		// process instead of counting a failure.
		if w.Local != nil { // SQLite and S3WAL (issue #342)
			return w.restoreForShutdown(ctx, "sync", job, holder, runErr, func(fctx context.Context) (Upstream, error) {
				return w.Local.FinishSyncOutcome(fctx, job.RepoID, holder, meta.SyncOutcome{Kind: meta.OutcomeRestore, Delay: shutdownRestoreDelay})
			})
		}
		if rs, ok := w.Upstreams.(ShutdownRestorer); ok { // PostgreSQL/River (issue #343)
			if err := w.restoreForShutdown(ctx, "sync", job, holder, runErr, func(fctx context.Context) (Upstream, error) {
				return rs.RestoreSync(fctx, job.RepoID, holder)
			}); err != nil {
				return err
			}
			// Still an error: River's completion gate snoozes the job
			// (keeping it for the next process) only for a run that
			// failed without being counted while a request is pending.
			return fmt.Errorf("sync %s: %w", job.RepoID, ErrShutdown)
		}
	}

	var fin Upstream
	if w.Local != nil {
		fin, err = w.retryFinish(ctx, "sync", job.RepoID, func(fctx context.Context) (Upstream, error) {
			return w.Local.FinishSyncOutcome(fctx, job.RepoID, holder, meta.SyncOutcome{Kind: meta.OutcomeDone, Success: runErr == nil})
		})
	} else {
		fin, err = w.finishSync(ctx, job.RepoID, holder, runErr == nil)
	}
	if errors.Is(err, ErrNotFound) {
		return nil // detached mid-run: cancel silently (docs/reference/webhooks.md)
	}
	if errors.Is(err, ErrClaimLost) {
		// This run outlived its lease and the claim was stolen: the thief
		// owns the lifecycle (its fetch postdates ours), so abandon
		// without terminal events or counter writes — but loudly, so
		// operators can spot leases racing live runs.
		w.Logger.Warn("sync claim lost before completion; abandoning late run",
			"org", job.Org, "repo", job.RepoID, "holder", holder, "runErr", runErr)
		return nil
	}
	if err != nil {
		return fmt.Errorf("finish sync %s: %w", job.RepoID, err)
	}
	terminal := base
	terminal.RunCount = fin.RunCount // includes the run that just finished
	terminal.CompletedAt = eventTime(time.Now())
	if runErr == nil {
		terminal.Type = EventSyncSucceeded
		w.emitDetached(ctx, job.Org, EventSyncSucceeded, terminal.marshal())
	} else {
		terminal.Type = EventSyncFailed
		terminal.Error = failureCode(runErr)
		w.emitDetached(ctx, job.Org, EventSyncFailed, terminal.marshal())
		w.Logger.Warn("upstream sync failed",
			"org", job.Org, "repo", job.RepoID, "error", terminal.Error, "cause", runErr)
	}

	if w.Local != nil {
		return runErr // intent that arrived mid-run stays due in the row; the sweep runs it
	}
	if fin.SyncPending && w.Requeue != nil {
		// Requests marked (RequestSync) after this run claimed are not
		// covered by its fetch snapshot: schedule the coalesced follow-up
		// now that the claim is free (the follow-up's BeginSync consumes
		// the marker). This runs strictly AFTER the terminal event above
		// so the successor's repo.sync.started cannot be emitted before
		// this run's lifecycle completes. Failure is log-only — local
		// queued work is already documented as best-effort across
		// restarts.
		rqCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), detachedOpTimeout)
		defer cancel()
		// fin is a snapshot from FinishSync, and the emissions above can
		// block long enough for another run (e.g. the request's own
		// queued goroutine claiming the freed row) to consume the marker
		// and cover every request. Re-read and skip the follow-up when
		// the marker is already covered or the repo was detached; a
		// failed read requeues conservatively — an extra run is a
		// harmless no-op, a dropped request is not.
		switch up, err := w.Upstreams.GetUpstream(rqCtx, job.RepoID); {
		case err == nil && !up.SyncPending, errors.Is(err, ErrNotFound):
			return runErr
		}
		if rqErr := w.Requeue(rqCtx, job); rqErr != nil {
			w.Logger.Warn("requeue pending sync failed",
				"org", job.Org, "repo", job.RepoID, "err", rqErr)
		}
	}
	return runErr
}

// shutdownRestoreDelay is how long a request kept by a shutdown-cancelled
// run waits before it is due again; the next process's first sweep starts
// later than this anyway.
const shutdownRestoreDelay = time.Second

// shutdownRestoreTimeout bounds a shutdown restore write, retries
// included, to serve's restore window (cmd shutdownRestoreWait), so a
// stalled store cannot hold shutdown past its budget. A restore that runs
// out leaves the claim to expire with its lease (issue #343 review).
const shutdownRestoreTimeout = 2 * time.Second

// claimLocal runs a local-schedule claim unless shutdown already cancelled
// the run, on a claimContext.
func (w *Worker) claimLocal(ctx context.Context, claim func(context.Context) (LaneClaim, error)) (LaneClaim, error) {
	if shutdownAborted(ctx) {
		return LaneClaim{}, nil // not claimed: the request stays in the row
	}
	cctx, cancel := claimContext(ctx)
	defer cancel()
	return claim(cctx)
}

// claimContext is the context a claim write runs on. A cancellation of the
// run (shutdown, timeout) reaches it only after shutdownRestoreTimeout: a
// claim cut off mid-write could commit without the run knowing and hold
// the request until its lease expires (issue #342), but a stalled store
// must not hold shutdown past its budget either (issue #343 review). A
// claim that still commits unseen is recovered once its lease expires: by
// the local schedule's sweep, or by the River job that shutdownSnooze keeps.
func claimContext(ctx context.Context) (context.Context, context.CancelFunc) {
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), detachedOpTimeout)
	stop := context.AfterFunc(ctx, func() { time.AfterFunc(shutdownRestoreTimeout, cancel) })
	return cctx, func() {
		stop()
		cancel()
	}
}

// shutdownErr marks err as shutdown-caused when the drain cancelled the
// run: River keeps the job of a run that returns such an error, since the
// run committed no outcome (jobs.shutdownSnooze).
func shutdownErr(ctx context.Context, err error) error {
	if err != nil && shutdownAborted(ctx) && !errors.Is(err, ErrShutdown) {
		return fmt.Errorf("%w: %w", ErrShutdown, err)
	}
	return err
}

// restoreForShutdown ends a run that a shutdown drain cancelled with
// finish, a restore write (OutcomeRestore on the local schedule, #342;
// ShutdownRestorer on PostgreSQL, issue #343): the request stays pending
// for the next process, no run is counted, and no terminal event is
// emitted (like detach and claim-steal abandonment).
func (w *Worker) restoreForShutdown(ctx context.Context, lane string, job Job, holder string, runErr error,
	finish func(context.Context) (Upstream, error)) error {
	assertf(shutdownAborted(ctx), "restoreForShutdown outside a drained run")
	_, err := w.retryFinishWithin(ctx, shutdownRestoreTimeout, lane, job.RepoID, finish)
	switch {
	case errors.Is(err, ErrNotFound):
		return nil // detached meanwhile: nothing to keep
	case errors.Is(err, ErrClaimLost):
		w.Logger.Warn(lane+" claim lost before the shutdown restore; abandoning",
			"org", job.Org, "repo", job.RepoID, "holder", holder)
		return nil
	case err != nil:
		// Nothing committed: the claim is left to expire (claimContext).
		return fmt.Errorf("restore %s %s: %w: %w", lane, job.RepoID, ErrShutdown, err)
	}
	w.Logger.Info(lane+" run cancelled by shutdown; request kept for the next process",
		"org", job.Org, "repo", job.RepoID, "cause", runErr)
	return nil
}

// deferRetrySlack delays the deferred retry past the observed lease expiry
// so the retry's claim attempt lands strictly after the lease can be
// stolen (clock granularity, store latency).
const deferRetrySlack = time.Second

// deferredRetryDelay computes how long a claim-skip retry waits for the
// observed claim: just past its expiry normally, but a LEGACY claim
// (adopted from a pre-lease replica) may be deliberately non-stealable
// (meta.EnvSyncStealLegacy unset), so a lapsed one is probed at
// lease-slack cadence instead of spinning at the floor — the retry loop
// stays alive for the moment the operator enables the opt-in (or the old
// replica finishes) without burning queue and store work every second.
func deferredRetryDelay(up Upstream) time.Duration {
	delay := max(time.Until(up.SyncClaimExpiresAt)+deferRetrySlack, deferRetrySlack)
	if up.SyncClaimHolder == LegacyClaimHolder && delay < claimLeaseSlack {
		delay = claimLeaseSlack
	}
	return delay
}

// deferPendingRetry schedules one delayed re-enqueue after a claim-skip
// that would otherwise strand an accepted pull-upstream request: when the
// refusing claim's holder is DEAD (crashed process, lease not yet
// expired), no FinishSync will ever report the pending marker, so the
// skip is the request's last local vehicle until the lease can be stolen.
// The timer is gated on the marker both now and at fire time: a live
// holder's finisher (or any interim run's claim) consumes it, making the
// deferred retry a cheap no-op. Timers die with the process — the
// documented local-mode trade-off; the river lane instead relies on its
// durable job being rescued and re-run (jobs/river.go syncRescueAttempts).
func (w *Worker) deferPendingRetry(ctx context.Context, job Job) {
	if w.Requeue == nil {
		return // river lane: durable rescue owns crashed-holder recovery
	}
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), detachedOpTimeout)
	defer cancel()
	up, err := w.Upstreams.GetUpstream(rctx, job.RepoID)
	if err != nil || !up.SyncPending {
		return // nothing promised (or detached): the skip is final
	}
	// At most one pending timer per repo: repeated requests against the
	// same dead lease coalesce into it (their markers are one flag), and
	// the slot frees when it fires so a later skip can schedule anew.
	w.deferMu.Lock()
	if w.deferredRetry == nil {
		w.deferredRetry = make(map[string]bool)
	}
	if w.deferredRetry[job.RepoID] {
		w.deferMu.Unlock()
		return
	}
	w.deferredRetry[job.RepoID] = true
	w.deferMu.Unlock()

	time.AfterFunc(deferredRetryDelay(up), func() {
		w.deferMu.Lock()
		delete(w.deferredRetry, job.RepoID)
		w.deferMu.Unlock()
		bg, cancel := context.WithTimeout(context.Background(), detachedOpTimeout)
		defer cancel()
		cur, err := w.Upstreams.GetUpstream(bg, job.RepoID)
		if err != nil || !cur.SyncPending {
			return // covered meanwhile (or detached)
		}
		if rqErr := w.Requeue(bg, job); rqErr != nil {
			w.Logger.Warn("deferred pending sync requeue failed",
				"org", job.Org, "repo", job.RepoID, "err", rqErr)
		}
	})
}

// emitDetached emits one lifecycle event on a bounded context DETACHED
// from the run budget: run-context death (deadline expiry is a normal
// failure cause, and cancellation can land right after the claim) must not
// drop lifecycle events — the production sink uses the context for webhook
// listing and queue insertion, so a dead context would silently lose the
// event and leave observers with an incomplete started/terminal sequence.
func (w *Worker) emitDetached(ctx context.Context, org, eventType string, payload []byte) {
	ectx, cancel := context.WithTimeout(context.WithoutCancel(ctx), detachedOpTimeout)
	defer cancel()
	w.Events.Emit(ectx, org, eventType, payload)
}

// finishSyncAttempts bounds finishSync retries; the backoff between
// attempt n and n+1 is n seconds.
const finishSyncAttempts = 3

// finishSync completes the claimed run on a context DETACHED from the
// run's own budget, retrying transient failures. Only the claim holder can
// clear Syncing before the lease expires, so a FinishSync failed on the
// run's already-expired context (the run timeout is the most likely cause
// of reaching here late) would leave the row claimed until the lease
// steal (issue #86) — recover in-process first. ErrNotFound (detach) and
// ErrClaimLost (claim stolen: retrying cannot win it back) return
// immediately; exhausting the attempts leaves the claim to expire and be
// stolen by the next run.
func (w *Worker) finishSync(ctx context.Context, repoID, holder string, success bool) (Upstream, error) {
	return w.retryFinish(ctx, "sync", repoID, func(fctx context.Context) (Upstream, error) {
		return w.Upstreams.FinishSync(fctx, repoID, holder, success)
	})
}

// retryFinish runs one end-of-run write with bounded retries on a context
// detached from the run budget. ErrNotFound and ErrClaimLost are final. If
// every attempt fails the claim stays held and expires with its lease; the
// next claim steals it (with its force mandate, and on the local schedule
// its retry position).
func (w *Worker) retryFinish(ctx context.Context, lane, repoID string, finish func(context.Context) (Upstream, error)) (Upstream, error) {
	return w.retryFinishWithin(ctx, detachedOpTimeout, lane, repoID, finish)
}

// retryFinishWithin is retryFinish bounded by timeout instead of
// detachedOpTimeout.
func (w *Worker) retryFinishWithin(ctx context.Context, timeout time.Duration, lane, repoID string, finish func(context.Context) (Upstream, error)) (Upstream, error) {
	assertf(timeout > 0, "retryFinishWithin: non-positive timeout %v", timeout)
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	var lastErr error
	for attempt := 1; attempt <= finishSyncAttempts; attempt++ {
		fin, err := finish(fctx)
		if err == nil || errors.Is(err, ErrNotFound) || errors.Is(err, ErrClaimLost) {
			return fin, err
		}
		lastErr = err
		w.Logger.Warn("finish "+lane+" failed; retrying while the claim is held",
			"repo", repoID, "attempt", attempt, "err", err)
		select {
		case <-fctx.Done():
			return Upstream{}, lastErr
		case <-time.After(time.Duration(attempt) * time.Second):
		}
	}
	return Upstream{}, lastErr
}

// mirrorRefspecs mirror heads and tags; the leading + lets upstream
// force-pushes propagate. Deliberately NOT --mirror on the loopback push:
// that would delete server-side namespace refs absent upstream.
var mirrorRefspecs = []string{"+refs/heads/*:refs/heads/*", "+refs/tags/*:refs/tags/*"}

// transfer performs the fetch + loopback push. A nil return means success;
// errDetached means DELETE /repos/base raced the run; errClaimStolen means
// the claim barrier found the run's lease stolen; every other failure is a
// *syncFailure carrying the documented error string.
func (w *Worker) transfer(ctx context.Context, job Job, repo RepoInfo, up Upstream, holder string) error {
	if up.Owner == "" || up.Name == "" || up.UpstreamHost == "" {
		return &syncFailure{code: FailConfig, cause: fmt.Errorf(
			"incomplete upstream row: owner=%q name=%q host=%q", up.Owner, up.Name, up.UpstreamHost)}
	}

	tmp, err := os.MkdirTemp("", "openstorage-sync-*")
	if err != nil {
		return &syncFailure{code: FailInternal, cause: err}
	}
	defer os.RemoveAll(tmp)
	mirror := filepath.Join(tmp, "mirror.git")

	if stderr, err := runGit(ctx, tmp, hermeticGitEnv(w.allowProtocol()), "init", "--bare", mirror); err != nil {
		return &syncFailure{code: FailInternal, cause: fmt.Errorf("git init: %w: %s", err, stderr)}
	}

	// Upstream credential: absent = public upstream. An omitted username
	// gets the per-provider default (credentialAuth).
	username, password, fail := w.upstreamAuth(ctx, job.RepoID, up.Provider)
	if fail != nil {
		return fail
	}
	fetchEnv := authGitEnv(username, password, w.allowProtocol())
	// Egress guard (egress.go, issue #139): the inbound fetch dials
	// through the same vetting proxy as outbound pushes — upstream_host
	// is caller-linked data, so unguarded egress is an SSRF read/
	// stored-credential-echo primitive into the server's network.
	// file:// upstreams (tests; GIT_ALLOW_PROTOCOL-blocked in production)
	// have no network egress to guard. Ambient HTTP(S)_PROXY settings no
	// longer reach the upstream fetch: the guard dials destinations
	// directly, exactly like webhook delivery and push forwarding.
	var egress *egressProxy
	if u := up.CloneURL(); strings.HasPrefix(u, "http://") || strings.HasPrefix(u, "https://") {
		egress, err = startEgressProxy(w.PullEgressAllowPrivate)
		if err != nil {
			return &syncFailure{code: FailInternal, cause: err}
		}
		defer egress.Close()
		fetchEnv = proxyEnv(fetchEnv, egress.URL())
	}
	fetchArgs := append([]string{"fetch", "--prune", up.CloneURL()}, mirrorRefspecs...)
	if stderr, err := runGit(ctx, mirror, fetchEnv, fetchArgs...); err != nil {
		if f := argumentFailure("git fetch", err); f != nil {
			return f
		}
		// The proxy's recorded verdicts outrank stderr phrasing — git only
		// saw an opaque proxy HTTP error (same split as the push lane): a
		// policy denial is permanent configuration, a non-policy dial
		// failure is the documented transient-outage classification.
		if egress != nil {
			if reason := egress.deniedReason(); reason != "" {
				return &syncFailure{code: FailConfig, cause: fmt.Errorf("fetch egress denied: %s", reason)}
			}
			if dialErr := egress.dialFailure(); dialErr != "" {
				return &syncFailure{code: FailUnreachable, cause: fmt.Errorf("fetch egress dial: %s", dialErr)}
			}
		}
		return &syncFailure{code: classifyFetchFailure(stderr), cause: fmt.Errorf("git fetch: %w: %s", err, stderr)}
	}

	// Detach/steal barrier between fetch and the storage push: DELETE
	// /repos/base must cancel in-flight runs without a failed event
	// (docs/reference/webhooks.md), and a run whose expired claim was
	// stolen (issue #86) must not overwrite the thief's newer push with
	// this run's stale fetch snapshot. The barrier's fresh row also
	// supplies the lease expiry the push fence below is computed from.
	var claimExpiresAt time.Time
	switch cur, err := w.Upstreams.GetUpstream(ctx, job.RepoID); {
	case errors.Is(err, ErrNotFound):
		return errDetached
	case err != nil:
		return &syncFailure{code: FailInternal, cause: fmt.Errorf("detach check: %w", err)}
	case cur.SyncClaimHolder != holder:
		return errClaimStolen
	default:
		claimExpiresAt = cur.SyncClaimExpiresAt
	}

	// A create that named no default branch takes the upstream HEAD
	// (issue #599); adoptHead records it before the run is finished.
	var head string
	if up.DefaultBranchOmitted {
		head = w.upstreamHead(ctx, job, mirror, fetchEnv, up.CloneURL())
	}

	// Empty upstream: nothing to push, the run still counts as a success.
	refs, stderr, err := runGitOut(ctx, mirror, hermeticGitEnv(w.allowProtocol()), "for-each-ref", "--count=1", "--format=%(refname)")
	if err != nil {
		return &syncFailure{code: FailInternal, cause: fmt.Errorf("git for-each-ref: %w: %s", err, stderr)}
	}
	if strings.TrimSpace(refs) == "" {
		return nil
	}

	// The token TTL fences the push against a claim steal (issue #86):
	// it dies claimFenceMargin before the lease can be stolen, so a
	// zombie run cannot authenticate a new push request once a thief may
	// own the refs. Non-positive means this run is already inside the
	// fence window (only reachable when it has far outlived its own
	// budget): fail the run instead of pushing unfenced — the claim is
	// still OURS here, so the normal FinishSync path releases it cleanly
	// rather than leaving it to expire.
	tokenTTL := time.Until(claimExpiresAt) - claimFenceMargin
	if tokenTTL <= 0 {
		return &syncFailure{code: FailInternal, cause: fmt.Errorf(
			"claim lease expires at %v, inside the push fence margin: refusing to push", claimExpiresAt)}
	}
	token, err := w.Loopback.Token(ctx, job.Org, repo, tokenTTL, holder)
	if err != nil {
		return &syncFailure{code: FailInternal, cause: fmt.Errorf("mint loopback token: %w", err)}
	}
	pushEnv := loopbackGitEnv(token, w.allowProtocol())
	// The loopback push is a self-call (Loopback.BaseURL doc): proxy env
	// must never reroute it through a load balancer, where the token could
	// land on a replica that cannot verify it (issue #45). os/exec keeps
	// the last duplicate env entry, so these override inherited settings.
	// (The upstream FETCH above instead dials through its own vetting
	// proxy — issue #139.)
	pushEnv = append(pushEnv, "NO_PROXY=*", "no_proxy=*")
	pushURL := strings.TrimRight(w.Loopback.BaseURL, "/") + "/" + repo.RepoID + ".git"
	// forward_on_push repos invert the conflict rule (issue #90, Codex
	// round 7): LOCAL wins. An inbound pull whose upstream fetch predates
	// a just-accepted local push must not force-rewind it — the poke
	// reroutes to the forward lane, but a pull already in flight raced
	// it, and any scheduling-side coordination leaves a window. Making
	// the loopback push fast-forward-only closes it structurally: an
	// accepted local push can never be erased by a stale snapshot, and
	// genuine upstream additions still land as fast-forwards. Refused
	// refs are the converged local-wins outcome, not a failure; upstream
	// rewinds/divergence then surface on the next forward as its
	// documented FailRejected, where a human decides force. Residual
	// (documented in ADR-44): a FORCE-accepted local rewind to an
	// ancestor of the stale snapshot is itself fast-forwardable, so a
	// concurrent pull can still undo exactly that shape.
	refspecs := mirrorRefspecs
	if up.ForwardOnPush {
		refspecs = pushRefspecs
	}
	pushArgs := append([]string{"push", "--porcelain", pushURL}, refspecs...)
	stdout, stderr, err := runGitOut(ctx, mirror, pushEnv, pushArgs...)
	if err != nil {
		// Local-wins tolerance is scoped to the rejection REASONS: only
		// stale-snapshot shapes (the client-side fast-forward/exists
		// checks) mean "the loopback already has newer content", which is
		// the converged forward-on-push outcome. Server-side rejections
		// (hook declines: quota, persistence failures) mean upstream refs
		// were NOT installed — succeeding on those would emit
		// repo.sync.succeeded for a run that synced nothing.
		if rejected := parsePushRejections(stdout); up.ForwardOnPush && allLocalWins(rejected) {
			w.Logger.Info("mirror-back kept newer local refs (forward-on-push: local wins)",
				"org", job.Org, "repo", job.RepoID, "refused", len(rejected))
			w.adoptHead(ctx, job, head)
			return nil
		}
		return &syncFailure{code: FailPush, cause: fmt.Errorf("git push: %w: %s", err, stderr)}
	}
	w.adoptHead(ctx, job, head)
	return nil
}

// allLocalWins reports whether every porcelain rejection is a
// stale-snapshot shape — git's client-side refusal phrasings for "the
// destination ref moved past this snapshot" ("non-fast-forward", "fetch
// first") or "the destination already has a different tag" ("already
// exists"). Empty input is false: an error without tolerated rejections
// is a real failure.
func allLocalWins(rejected []RejectedRef) bool {
	if len(rejected) == 0 {
		return false
	}
	for _, r := range rejected {
		switch {
		case strings.Contains(r.Reason, "non-fast-forward"),
			strings.Contains(r.Reason, "fetch first"),
			strings.Contains(r.Reason, "already exists"):
		default:
			return false
		}
	}
	return true
}

// authFailureMarkers/networkFailureMarkers classify git transport stderr
// for both transfer directions. Auth is checked first (an
// unreachable-looking 403 is still auth), then network reachability.
var authFailureMarkers = []string{
	"authentication failed",
	"terminal prompts disabled", // 401 with no credential: git can't prompt
	"invalid username or password",
	"access denied",
	"returned error: 401",
	"returned error: 403",
}

var networkFailureMarkers = []string{
	"could not resolve host",
	"connection refused",
	"couldn't connect to server",
	"could not connect to server",
	"failed to connect to",
	"unable to connect",
	"connection timed out",
	"operation timed out",
	"network is unreachable",
	"no route to host",
}

func containsAnyMarker(stderr string, markers []string) bool {
	s := strings.ToLower(stderr)
	for _, marker := range markers {
		if strings.Contains(s, marker) {
			return true
		}
	}
	return false
}

// classifyFetchFailure maps git fetch stderr onto the documented failure
// strings; everything past auth/network is a generic clone failure.
func classifyFetchFailure(stderr string) string {
	switch {
	case containsAnyMarker(stderr, authFailureMarkers):
		return FailAuth
	case containsAnyMarker(stderr, networkFailureMarkers):
		return FailUnreachable
	default:
		return FailClone
	}
}

// classifyPushFailure is the outbound twin (issue #90): a `git push` to the
// upstream that failed WITHOUT per-ref rejections (those map to
// FailRejected before this runs) is auth, network, or a generic forward
// failure.
func classifyPushFailure(stderr string) string {
	switch {
	case containsAnyMarker(stderr, authFailureMarkers):
		return FailAuth
	case containsAnyMarker(stderr, networkFailureMarkers):
		return FailUnreachable
	default:
		return FailForward
	}
}

// hermeticGitEnv is the base environment for worker git invocations:
// prompts disabled, host/global git config masked out (credential helpers
// or URL rewrites there must not leak into sync transfers), and transports
// pinned via GIT_ALLOW_PROTOCOL — the fetch URL derives from an
// attacker-controlled upstream_host, so file://, ssh://, and transport
// helpers (ext::) must be unreachable even if a hostile row slips past the
// API-layer validation.
func hermeticGitEnv(allowProtocol string) []string {
	assertf(allowProtocol != "", "hermeticGitEnv: empty allowProtocol")
	// Strip EVERY inherited GIT_* variable before setting the controlled
	// ones: hosts export credential and config injectors — GIT_ASKPASS,
	// GIT_SSH_COMMAND, GIT_CONFIG_COUNT/KEY/VALUE — that run arbitrary
	// helpers inside the worker's git subprocesses (observed: a
	// workspace-agent GIT_ASKPASS answering a 401 by phoning its own
	// auth server through the push egress proxy, which misclassified the
	// auth failure as a transient dial outage). Nulling the config FILES
	// alone does not close the env channel.
	env := envconfig.Strip(os.Environ(), "OPENSTORAGE_ASKPASS_USERNAME", "OPENSTORAGE_ASKPASS_PASSWORD")
	out := make([]string, 0, len(env)+4)
	for _, kv := range env {
		if strings.HasPrefix(kv, "GIT_") {
			continue
		}
		out = append(out, kv)
	}
	return append(out,
		"GIT_TERMINAL_PROMPT=0",
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_CONFIG_SYSTEM=/dev/null",
		"GIT_ALLOW_PROTOCOL="+allowProtocol,
	)
}

// upstreamAuth loads the repo's stored upstream credential and returns
// the Basic-auth pair the worker sends. ("", "") means none: no stored
// credential, or one with an empty password (sent nothing before #400
// either). The pair travels through git's line-based credential-helper
// protocol, so a stored value with CR, LF or NUL fails the run as
// configuration instead of injecting helper attributes; the error never
// names the value.
func (w *Worker) upstreamAuth(ctx context.Context, repoID, provider string) (username, password string, fail *syncFailure) {
	cred, err := w.Credentials.CredentialForRepo(ctx, repoID)
	switch {
	case errors.Is(err, ErrNotFound):
		return "", "", nil
	case err != nil:
		return "", "", &syncFailure{code: FailInternal, cause: fmt.Errorf("load credential: %w", err)}
	case cred.Password == "":
		return "", "", nil
	case strings.ContainsAny(cred.Username+cred.Password, "\r\n\x00"):
		return "", "", &syncFailure{code: FailConfig, cause: errors.New("stored git credential contains CR, LF or NUL")}
	}
	username, password = credentialAuth(provider, cred.Username, cred.Password)
	return username, password, nil
}

// credentialAuth returns the Basic-auth pair for a stored credential with
// a non-empty password. A stored username always wins. The API makes the
// username optional, and git sends no credentials at all for an empty
// username, so an omitted one gets upstream's per-provider default (spec
// 2026-10-08, issue #400): x-token-auth (Bitbucket) or oauth2 (GitLab)
// with the token as the password, and the token as the USERNAME with an
// empty password for Gitea, Forgejo and Codeberg (git sends "<token>:").
// GitHub and every other provider keep "git" plus the token as the
// password (docs/reference/http-api.md, "Git credentials").
func credentialAuth(provider, storedUsername, password string) (username, pass string) {
	assertf(password != "", "credentialAuth: empty password")
	if storedUsername != "" {
		return storedUsername, password
	}
	switch provider {
	case "bitbucket":
		return "x-token-auth", password
	case "gitlab":
		return "oauth2", password
	case "gitea", "forgejo", "codeberg":
		return password, ""
	}
	return "git", password
}

// gitCredentialHelper answers git's credential "get" with the pair from
// the OPENSTORAGE_ASKPASS_* environment and ignores "store" and "erase".
// It is static shell: secrets reach git through the child environment
// only, never argv (visible in ps) or a file. GIT_ASKPASS is not used
// because git passes its password prompt as argv, and that prompt names
// the username, which is the token for Gitea-family hosts.
const gitCredentialHelper = `!f() { while read -r _; do :; done; test "$1" = get || return 0; ` +
	`printf 'username=%s\npassword=%s\n' "$OPENSTORAGE_ASKPASS_USERNAME" "$OPENSTORAGE_ASKPASS_PASSWORD"; }; f`

// loopbackGitEnv is authGitEnv for a loopback token: git basic auth is
// username "t", password = JWT (docs/explanation/git-protocol-and-namespaces.md).
// An empty token (file:// loopback) sends no credential.
func loopbackGitEnv(token, allowProtocol string) []string {
	if token == "" {
		return authGitEnv("", "", allowProtocol)
	}
	return authGitEnv("t", token, allowProtocol)
}

// authGitEnv extends hermeticGitEnv with a credential helper feeding the
// given Basic-auth pair. An empty username installs no helper (public
// upstream / file:// loopback); the password may be empty (Gitea-family
// token-as-username). Callers reject CR/LF/NUL first (upstreamAuth).
func authGitEnv(username, password, allowProtocol string) []string {
	env := hermeticGitEnv(allowProtocol)
	if username == "" {
		assertf(password == "", "authGitEnv: password without username")
		return env
	}
	assertf(!strings.ContainsAny(username+password, "\r\n\x00"), "authGitEnv: credential with CR, LF or NUL")
	env = envconfig.Set(env, "OPENSTORAGE_ASKPASS_USERNAME", username)
	env = envconfig.Set(env, "OPENSTORAGE_ASKPASS_PASSWORD", password)
	// The empty first value resets any helper list git may have collected
	// (credential.helper is multi-valued), so only ours answers.
	return append(env,
		"GIT_CONFIG_COUNT=2",
		"GIT_CONFIG_KEY_0=credential.helper", "GIT_CONFIG_VALUE_0=",
		"GIT_CONFIG_KEY_1=credential.helper", "GIT_CONFIG_VALUE_1="+gitCredentialHelper,
	)
}

// runGit executes one git command, returning its stderr for failure
// classification. Credentials never appear in argv or stderr (askpass).
func runGit(ctx context.Context, dir string, env []string, args ...string) (stderr string, err error) {
	_, stderr, err = runGitOut(ctx, dir, env, args...)
	return stderr, err
}

func runGitOut(ctx context.Context, dir string, env []string, args ...string) (stdout, stderr string, err error) {
	var outBuf bytes.Buffer
	stderr, err = runGitTo(ctx, dir, env, &outBuf, args...)
	return outBuf.String(), stderr, err
}

// runGitTo runs git with stdout written to out.
func runGitTo(ctx context.Context, dir string, env []string, out io.Writer, args ...string) (stderr string, err error) {
	assertf(dir != "" && len(args) > 0 && out != nil, "runGit: missing dir/args/stdout")
	// exec refuses a NUL byte, and an argument or variable of 128 KiB or
	// more. The upstream URL (owner, name, host) and the credential reach
	// git here from a client's request, so the run reports them
	// (argumentFailure) and does not read them as a clone failure.
	for _, v := range append(append([]string{}, args...), env...) {
		if strings.IndexByte(v, 0) >= 0 {
			return "", gitlimit.ErrArgNUL
		}
	}
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	cmd.Env = env
	var errBuf bytes.Buffer
	cmd.Stdout = out
	cmd.Stderr = &errBuf
	cmd.Stdin = bytes.NewReader(nil)
	// Cancellation must kill git's whole process group (setProcGroup):
	// the transfer lives in a git-remote-http(s) CHILD, which would
	// otherwise keep pushing/fetching after the parent died — and keep
	// the output pipes open, blocking Run indefinitely. WaitDelay is the
	// portable backstop for any pipe holder that still survives.
	setProcGroup(cmd)
	cmd.WaitDelay = 10 * time.Second
	err = cmd.Run()
	if errors.Is(err, syscall.E2BIG) {
		err = fmt.Errorf("%w: %w", gitlimit.ErrArgTooLong, err)
	}
	if err != nil && ctx.Err() != nil {
		err = fmt.Errorf("%w (%v)", ctx.Err(), err)
	}
	return errBuf.String(), err
}

// argumentFailure is the permanent configuration failure of a run whose
// git command line exec refused: an upstream owner, name or host, or a
// stored credential, that is too long for one argument (about 128 KiB on
// Linux) or holds a NUL byte. Retrying cannot help, and neither can a
// clone or push failure code. It returns nil for any other error. The
// message never names the value.
func argumentFailure(op string, err error) *syncFailure {
	if !errors.Is(err, gitlimit.ErrArgTooLong) && !errors.Is(err, gitlimit.ErrArgNUL) {
		return nil
	}
	return &syncFailure{code: FailConfig, cause: fmt.Errorf("%s: the upstream URL or the stored credential cannot be passed to git: %w", op, err)}
}
