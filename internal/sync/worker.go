package sync

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	stdsync "sync"
	"time"
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
	// auth `t:<jwt>`, docs/explanation/git-protocol-and-namespaces.md). The token is passed to git via GIT_ASKPASS
	// and never logged. May return "" when BaseURL needs no auth (file://).
	// ttl is the FENCE for the run's claim lease (issue #86): the token
	// must not be honored past it, so a zombie holder whose claim was
	// stolen cannot authenticate a new push request — implementations
	// must cap their token lifetime at ttl.
	Token func(ctx context.Context, org string, repo RepoInfo, ttl time.Duration) (string, error)
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
	var f *syncFailure
	if errors.As(err, &f) {
		return f.code
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return FailGeneric
	}
	return FailInternal
}

// Run executes one sync run for job. It returns an error for logging by the
// queue; documented outcomes (detached, overlapping run, failed sync with an
// emitted event) are not errors to the caller beyond that.
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
		return nil // repo deleted while queued: nothing to sync or emit
	}
	if err != nil {
		return fmt.Errorf("resolve repo %s: %w", job.RepoID, err)
	}
	assertf(repo.RepoID == job.RepoID, "RepoLookup returned %q for %q", repo.RepoID, job.RepoID)

	holder := newClaimHolder()
	up, stolenFrom, err := w.Upstreams.BeginSync(ctx, job.RepoID, holder, claimLease(timeout))
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
		return fmt.Errorf("begin sync %s: %w", job.RepoID, err)
	}
	if stolenFrom != "" {
		// The previous holder's lease expired without a FinishSync — its
		// process died mid-run (issue #86). Loud on purpose: steals are
		// normal only after crashes; a pattern of them means the lease is
		// racing live runs (clock trouble or budget drift).
		w.Logger.Warn("stole expired sync claim",
			"org", job.Org, "repo", job.RepoID, "holder", holder, "stolen_from", stolenFrom)
	}

	startedAt := eventTime(time.Now())
	isFirst := up.SuccessCount == 0
	base := Event{
		Repository:  EventRepository{ID: repo.RepoID, URL: repo.URL},
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

	fin, err := w.finishSync(ctx, job.RepoID, holder, runErr == nil)
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

// deferRetrySlack delays the deferred retry past the observed lease expiry
// so the retry's claim attempt lands strictly after the lease can be
// stolen (clock granularity, store latency).
const deferRetrySlack = time.Second

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

	delay := time.Until(up.SyncClaimExpiresAt) + deferRetrySlack
	if delay < deferRetrySlack {
		delay = deferRetrySlack
	}
	time.AfterFunc(delay, func() {
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
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), detachedOpTimeout)
	defer cancel()
	var lastErr error
	for attempt := 1; attempt <= finishSyncAttempts; attempt++ {
		fin, err := w.Upstreams.FinishSync(fctx, repoID, holder, success)
		if err == nil || errors.Is(err, ErrNotFound) || errors.Is(err, ErrClaimLost) {
			return fin, err
		}
		lastErr = err
		w.Logger.Warn("finish sync failed; retrying while the claim is held",
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

	tmp, err := os.MkdirTemp("", "codestorage-sync-*")
	if err != nil {
		return &syncFailure{code: FailInternal, cause: err}
	}
	defer os.RemoveAll(tmp)
	mirror := filepath.Join(tmp, "mirror.git")

	if stderr, err := runGit(ctx, tmp, hermeticGitEnv(w.allowProtocol()), "init", "--bare", mirror); err != nil {
		return &syncFailure{code: FailInternal, cause: fmt.Errorf("git init: %w: %s", err, stderr)}
	}

	// Upstream credential: absent = public upstream. Username defaults to
	// "git" for token-only providers (docs/reference/http-api.md: username optional).
	username, password := "", ""
	switch cred, err := w.Credentials.CredentialForRepo(ctx, job.RepoID); {
	case err == nil:
		username, password = cred.Username, cred.Password
	case !errors.Is(err, ErrNotFound):
		return &syncFailure{code: FailInternal, cause: fmt.Errorf("load credential: %w", err)}
	}
	if username == "" {
		username = "git"
	}
	fetchEnv, err := authGitEnv(filepath.Join(tmp, "askpass-fetch.sh"), username, password, w.allowProtocol())
	if err != nil {
		return &syncFailure{code: FailInternal, cause: err}
	}
	fetchArgs := append([]string{"fetch", "--prune", up.CloneURL()}, mirrorRefspecs...)
	if stderr, err := runGit(ctx, mirror, fetchEnv, fetchArgs...); err != nil {
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
	token, err := w.Loopback.Token(ctx, job.Org, repo, tokenTTL)
	if err != nil {
		return &syncFailure{code: FailInternal, cause: fmt.Errorf("mint loopback token: %w", err)}
	}
	// docs/explanation/git-protocol-and-namespaces.md: git basic auth is username "t", password = JWT.
	pushEnv, err := authGitEnv(filepath.Join(tmp, "askpass-push.sh"), "t", token, w.allowProtocol())
	if err != nil {
		return &syncFailure{code: FailInternal, cause: err}
	}
	// The loopback push is a self-call (Loopback.BaseURL doc): proxy env
	// must never reroute it through a load balancer, where the token could
	// land on a replica that cannot verify it (issue #45). os/exec keeps
	// the last duplicate env entry, so these override inherited settings.
	// The upstream FETCH above keeps proxy env untouched on purpose.
	pushEnv = append(pushEnv, "NO_PROXY=*", "no_proxy=*")
	pushURL := strings.TrimRight(w.Loopback.BaseURL, "/") + "/" + repo.RepoID + ".git"
	pushArgs := append([]string{"push", pushURL}, mirrorRefspecs...)
	if stderr, err := runGit(ctx, mirror, pushEnv, pushArgs...); err != nil {
		return &syncFailure{code: FailPush, cause: fmt.Errorf("git push: %w: %s", err, stderr)}
	}
	return nil
}

// classifyFetchFailure maps git fetch stderr onto the documented failure
// strings. Auth is checked first (an unreachable-looking 403 is still auth),
// then network reachability; everything else is a generic clone failure.
func classifyFetchFailure(stderr string) string {
	s := strings.ToLower(stderr)
	for _, marker := range []string{
		"authentication failed",
		"terminal prompts disabled", // 401 with no credential: git can't prompt
		"invalid username or password",
		"access denied",
		"returned error: 401",
		"returned error: 403",
	} {
		if strings.Contains(s, marker) {
			return FailAuth
		}
	}
	for _, marker := range []string{
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
	} {
		if strings.Contains(s, marker) {
			return FailUnreachable
		}
	}
	return FailClone
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
	return append(os.Environ(),
		"GIT_TERMINAL_PROMPT=0",
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_CONFIG_SYSTEM=/dev/null",
		"GIT_ALLOW_PROTOCOL="+allowProtocol,
	)
}

// authGitEnv extends hermeticGitEnv with a GIT_ASKPASS helper feeding the
// given basic-auth credentials. Secrets travel via the child process
// environment only — never argv (visible in ps) and never logs. An empty
// password installs no helper (public upstream / file:// loopback).
func authGitEnv(scriptPath, username, password, allowProtocol string) ([]string, error) {
	env := hermeticGitEnv(allowProtocol)
	if password == "" {
		return env, nil
	}
	assertf(username != "", "authGitEnv: password without username")
	const script = `#!/bin/sh
case "$1" in
[Uu]sername*) printf '%s\n' "$CODESTORAGE_ASKPASS_USERNAME" ;;
*) printf '%s\n' "$CODESTORAGE_ASKPASS_PASSWORD" ;;
esac
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o700); err != nil {
		return nil, fmt.Errorf("write askpass helper: %w", err)
	}
	return append(env,
		"GIT_ASKPASS="+scriptPath,
		"CODESTORAGE_ASKPASS_USERNAME="+username,
		"CODESTORAGE_ASKPASS_PASSWORD="+password,
	), nil
}

// runGit executes one git command, returning its stderr for failure
// classification. Credentials never appear in argv or stderr (askpass).
func runGit(ctx context.Context, dir string, env []string, args ...string) (stderr string, err error) {
	_, stderr, err = runGitOut(ctx, dir, env, args...)
	return stderr, err
}

func runGitOut(ctx context.Context, dir string, env []string, args ...string) (stdout, stderr string, err error) {
	assertf(dir != "" && len(args) > 0, "runGit: missing dir/args")
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	cmd.Env = env
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf
	cmd.Stdin = bytes.NewReader(nil)
	err = cmd.Run()
	if err != nil && ctx.Err() != nil {
		err = fmt.Errorf("%w (%v)", ctx.Err(), err)
	}
	return outBuf.String(), errBuf.String(), err
}
