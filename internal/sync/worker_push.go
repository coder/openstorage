package sync

// Outbound push-upstream runs (issue #90): the mirror image of worker.go's
// pull lane. A run claims the repo's push lease, mirror-fetches LOCAL refs
// through this server's own smart-HTTP loopback (ADR-30 symmetry: reads go
// through the front door too), and performs a real `git push` of
// refs/heads/* + refs/tags/* to the linked upstream with the stored
// credential. Non-force by default — a ref the upstream refuses fails the
// run ATOMICALLY (--atomic: the forward is one remote transaction, so a
// refused batch leaves the upstream untouched) with per-ref reasons
// (FailRejected + rejected_refs) instead of rewriting upstream history;
// force is an explicit, marker-carried opt-in.
// Ref DELETIONS are not propagated in either direction (the pull lane
// deliberately avoids --mirror; this lane deliberately avoids --prune and
// delete refspecs).
//
// Egress posture: the push targets the CloneURL the pull lane already
// fetches from — a host validated at link time (providerHosts /
// validUpstreamHost) — under the same hermetic environment and
// GIT_ALLOW_PROTOCOL pin, PLUS a dial-time destination guard (egress.go):
// writes into private networks are a stronger SSRF primitive than the
// pull lane's reads, so every http(s) push dials through a vetting proxy
// that denies private/special destinations unless the operator opts in
// (OPENSTORAGE_SYNC_PUSH_ALLOW_PRIVATE); see docs/adr/044.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// MaxPushTransientRetries bounds the transient-failure retry chain of one
// outbound trigger (issue #90): an unreachable upstream re-marks the
// durable pending level and reschedules with exponential backoff, so a
// forward accepted just before an upstream outage converges once the
// outage ends. Permanent failures (auth, config, rejected refs) never
// retry — a rejected non-fast-forward can only be resolved by a human
// decision (pull or force), and hammering a 401 is abuse-shaped.
const MaxPushTransientRetries = 5

// PushRetryDelay is the backoff before transient retry n (1-based):
// 1m, 2m, 4m, 8m, 16m.
func PushRetryDelay(retry int) time.Duration {
	assertf(retry >= 1 && retry <= MaxPushTransientRetries, "PushRetryDelay: retry %d out of range", retry)
	return time.Minute << (retry - 1)
}

// TransientPushFailure reports whether a run error is worth an automatic
// retry: only network unreachability qualifies. Auth/config/rejection are
// permanent until a human acts; internal errors are more likely local bugs
// than weather, and the pull lane does not retry them either.
func TransientPushFailure(err error) bool {
	var f *syncFailure
	return errors.As(err, &f) && f.code == FailUnreachable
}

// pushRefspecs are the non-force outbound refspecs: the upstream refuses
// non-fast-forwards per ref, which is the documented default. Force reuses
// mirrorRefspecs (the + variants).
var pushRefspecs = []string{"refs/heads/*:refs/heads/*", "refs/tags/*:refs/tags/*"}

// defaultDetachPoll is the production cadence of the mid-push
// detach/steal watchdog (Worker.DetachPoll): one cheap upstream-row read
// every few seconds only while `git push` is actually running.
const defaultDetachPoll = 2 * time.Second

// RunPush executes one outbound push-upstream run for job. Lifecycle,
// abandonment, and event semantics mirror Run: detach cancels silently at
// every stage, a stolen claim abandons loudly without terminal events, and
// documented failures emit repo.sync.failed with direction "push".
func (w *Worker) RunPush(ctx context.Context, job Job) error {
	w.assertReady()
	assertf(job.Org != "" && job.RepoID != "", "RunPush: incomplete job %+v", job)
	finishLocal, admitted := w.startLocal(job, true)
	if !admitted {
		return nil
	}
	defer finishLocal()
	timeout := w.Timeout
	if timeout <= 0 {
		timeout = defaultRunTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	repo, err := w.Repos(ctx, job.Org, job.RepoID)
	if errors.Is(err, ErrNotFound) {
		return nil // repo deleted while queued: nothing to push or emit
	}
	if err != nil {
		return fmt.Errorf("resolve repo %s: %w", job.RepoID, err)
	}
	assertf(repo.RepoID == job.RepoID, "RepoLookup returned %q for %q", repo.RepoID, job.RepoID)

	holder := newClaimHolder()
	var up Upstream
	var force bool
	var stolenFrom string
	err = w.admitLocal(ctx, job, true, func() error {
		var claimErr error
		up, force, stolenFrom, claimErr = w.Upstreams.BeginPush(ctx, job.RepoID, holder, claimLease(timeout))
		return claimErr
	})
	switch {
	case errors.Is(err, errNoPending):
		return nil // queued observation was covered before admission
	case errors.Is(err, ErrNotFound):
		return nil // detached before start: cancel silently
	case errors.Is(err, ErrPushInProgress):
		// Overlapping outbound run: skip. A LIVE holder covers pending
		// requests at its FinishPush; a dead holder's unexpired lease
		// needs the deferred retry, exactly like the pull lane (#86).
		w.deferPendingPushRetry(ctx, job)
		return nil
	case err != nil:
		return fmt.Errorf("begin push %s: %w", job.RepoID, err)
	}
	if stolenFrom != "" {
		w.Logger.Warn("stole expired push claim",
			"org", job.Org, "repo", job.RepoID, "holder", holder, "stolen_from", stolenFrom)
	}

	startedAt := eventTime(time.Now())
	base := Event{
		Repository:  EventRepository{ID: repo.RepoID, URL: repo.URL},
		RunCount:    up.PushRunCount,
		IsFirstSync: up.PushSuccessCount == 0,
		StartedAt:   startedAt,
		Direction:   DirectionPush,
		Forced:      force,
	}
	started := base
	started.Type = EventSyncStarted
	w.emitDetached(ctx, job.Org, EventSyncStarted, started.marshal())

	rejected, runErr := w.transferPush(ctx, job, repo, up, holder, force)
	if errors.Is(runErr, errDetached) {
		return nil // detach raced the run: the row is gone, cancel silently
	}
	if errors.Is(runErr, errClaimStolen) {
		w.Logger.Warn("push claim stolen mid-run; abandoning before the upstream push",
			"org", job.Org, "repo", job.RepoID, "holder", holder)
		return nil
	}

	fin, err := w.finishPush(ctx, job.RepoID, holder, runErr == nil)
	if errors.Is(err, ErrNotFound) {
		return nil // detached mid-run: cancel silently
	}
	if errors.Is(err, ErrClaimLost) {
		w.Logger.Warn("push claim lost before completion; abandoning late run",
			"org", job.Org, "repo", job.RepoID, "holder", holder, "runErr", runErr)
		return nil
	}
	if err != nil {
		return fmt.Errorf("finish push %s: %w", job.RepoID, err)
	}
	terminal := base
	terminal.RunCount = fin.PushRunCount // includes the run that just finished
	terminal.CompletedAt = eventTime(time.Now())
	if runErr == nil {
		terminal.Type = EventSyncSucceeded
		w.emitDetached(ctx, job.Org, EventSyncSucceeded, terminal.marshal())
	} else {
		terminal.Type = EventSyncFailed
		terminal.Error = failureCode(runErr)
		if terminal.Error == FailRejected {
			terminal.RejectedRefs = rejected
		}
		w.emitDetached(ctx, job.Org, EventSyncFailed, terminal.marshal())
		w.Logger.Warn("push-upstream failed",
			"org", job.Org, "repo", job.RepoID, "error", terminal.Error, "cause", runErr)
	}

	if runErr == nil && w.AfterForward != nil {
		// Mirror-back pull after EVERY successful outbound run (see the
		// AfterForward field doc): scheduled strictly after the forward
		// landed, and marked durable BEFORE enqueueing (the
		// RequestSync-before-enqueue protocol). If the enqueue coalesces
		// with a pull run that already claimed — river's insert-time
		// uniqueness swallows the insert, a held local claim skips the
		// run — the marker survives to that run's FinishSync and its
		// completion gate schedules the follow-up. Without it, a pull
		// that fetched upstream BEFORE this forward could write its
		// pre-forward snapshot afterward and leave the repo silently
		// divergent from the refs it just published — which is why the
		// mirror-back is NOT limited to ForwardOnPush repos: an explicit
		// push-upstream racing a poke-scheduled pull needs the same
		// convergence, or the stale pull's force-write stands until some
		// unrelated future trigger.
		afCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), detachedOpTimeout)
		defer cancel()
		// The marker write gets bounded retries (finishPush's rationale):
		// it is load-bearing exactly when a pull is ALREADY in flight —
		// the enqueue below is then swallowed by queue deduplication and
		// only the marker reaches that run's completion gate — so a
		// transient meta blip must not silently downgrade the promise to
		// an unmarked enqueue.
		var markErr error
		for attempt := 1; attempt <= finishSyncAttempts; attempt++ {
			markErr = w.Upstreams.RequestSync(afCtx, job.RepoID)
			if markErr == nil || errors.Is(markErr, ErrNotFound) {
				break
			}
			w.Logger.Warn("mirror-back pending mark failed; retrying",
				"org", job.Org, "repo", job.RepoID, "attempt", attempt, "err", markErr)
			select {
			case <-afCtx.Done():
			case <-time.After(time.Duration(attempt) * time.Second):
			}
			if afCtx.Err() != nil {
				break
			}
		}
		switch {
		case errors.Is(markErr, ErrNotFound):
			// Detached after the forward: the mirror-back is moot.
		default:
			if markErr != nil {
				// Marker write failed even with retries: enqueue anyway —
				// an unmarked mirror-back still covers the uncontended
				// common case, and dropping both would break the promise
				// outright. Residual: an in-flight pull racing a meta
				// outage that outlives the retries.
				w.Logger.Warn("mirror-back pending mark failed after retries; enqueueing unmarked",
					"org", job.Org, "repo", job.RepoID, "err", markErr)
			}
			if afErr := w.AfterForward(afCtx, Job{Org: job.Org, RepoID: job.RepoID}); afErr != nil {
				w.Logger.Warn("mirror-back sync enqueue failed",
					"org", job.Org, "repo", job.RepoID, "err", afErr)
			}
		}
	}

	if runErr != nil && TransientPushFailure(runErr) && job.Retry < MaxPushTransientRetries {
		// Transient failure with retry budget: re-mark the DURABLE pending
		// level (preserving this run's force; MAX keeps any fresher force
		// request) so the retry survives a process death, then reschedule
		// with backoff. River wiring reads the re-marked level in its
		// transactional completion gate (RequeuePush nil there); local
		// wiring arms a timer here. Ordered before the plain pending
		// follow-up below, which would requeue immediately — during an
		// outage the backoff owns the cadence and the marker carries any
		// coalesced fresh requests along.
		w.schedulePushRetry(ctx, job, force)
		return runErr
	}

	if fin.PushPending != PushPendingNone && w.RequeuePush != nil {
		rqCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), detachedOpTimeout)
		defer cancel()
		// Re-read like the pull lane: an interim run may have consumed the
		// marker while the emissions above ran; a failed read requeues
		// conservatively (an extra run is a no-op, a dropped request is
		// not). Retry resets to 0: this is a fresh request chain.
		switch up, err := w.Upstreams.GetUpstream(rqCtx, job.RepoID); {
		case err == nil && up.PushPending == PushPendingNone, errors.Is(err, ErrNotFound):
			return runErr
		}
		if rqErr := w.RequeuePush(rqCtx, Job{Org: job.Org, RepoID: job.RepoID}); rqErr != nil {
			w.Logger.Warn("requeue pending push failed",
				"org", job.Org, "repo", job.RepoID, "err", rqErr)
		}
	}
	return runErr
}

// schedulePushRetry re-marks the durable pending level and, on the local
// lane, arms a single-flight backoff timer for the retry (river schedules
// its delayed successor transactionally instead — see RequeuePush doc).
func (w *Worker) schedulePushRetry(ctx context.Context, job Job, force bool) {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), detachedOpTimeout)
	defer cancel()
	if err := w.Upstreams.RequestPush(rctx, job.RepoID, force); err != nil {
		if !errors.Is(err, ErrNotFound) { // detached: retry moot
			w.Logger.Warn("re-mark push retry failed",
				"org", job.Org, "repo", job.RepoID, "err", err)
		}
		return
	}
	if w.RequeuePush == nil {
		return // river lane: the completion gate schedules the delayed successor
	}
	job.Retry++
	delay := PushRetryDelay(job.Retry)
	w.Logger.Warn("push-upstream unreachable; retrying with backoff",
		"org", job.Org, "repo", job.RepoID, "retry", job.Retry, "delay", delay)
	w.scheduleLocalRetry("push-retry:"+job.RepoID, delay, job, true)
}

// deferPendingPushRetry mirrors deferPendingRetry for the outbound lane: a
// claim-skip against a DEAD holder's unexpired push lease is a pending
// request's last local vehicle until the lease can be stolen. No legacy
// grace exists here — push claims only ever come from #90-aware binaries.
func (w *Worker) deferPendingPushRetry(ctx context.Context, job Job) {
	if w.RequeuePush == nil {
		return // river lane: durable rescue owns crashed-holder recovery
	}
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), detachedOpTimeout)
	defer cancel()
	up, err := w.Upstreams.GetUpstream(rctx, job.RepoID)
	if err != nil || up.PushPending == PushPendingNone {
		return // nothing promised (or detached): the skip is final
	}
	delay := max(time.Until(up.PushClaimExpiresAt)+deferRetrySlack, deferRetrySlack)
	w.scheduleLocalRetry("push-defer:"+job.RepoID, delay, job, true)
}

// finishPush completes the claimed outbound run on a detached context with
// bounded retries — finishSync's rationale applies verbatim to the push
// lease.
func (w *Worker) finishPush(ctx context.Context, repoID, holder string, success bool) (Upstream, error) {
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), detachedOpTimeout)
	defer cancel()
	var lastErr error
	for attempt := 1; attempt <= finishSyncAttempts; attempt++ {
		fin, err := w.Upstreams.FinishPush(fctx, repoID, holder, success)
		if err == nil || errors.Is(err, ErrNotFound) || errors.Is(err, ErrClaimLost) {
			return fin, err
		}
		lastErr = err
		w.Logger.Warn("finish push failed; retrying while the claim is held",
			"repo", repoID, "attempt", attempt, "err", err)
		select {
		case <-fctx.Done():
			return Upstream{}, lastErr
		case <-time.After(time.Duration(attempt) * time.Second):
		}
	}
	return Upstream{}, lastErr
}

// transferPush performs the loopback fetch + upstream push. A nil error is
// success; errDetached/errClaimStolen abandon silently; other failures are
// *syncFailure values, with rejected non-nil exactly when the failure is
// FailRejected.
func (w *Worker) transferPush(ctx context.Context, job Job, repo RepoInfo, up Upstream, holder string, force bool) (rejected []RejectedRef, err error) {
	if up.Owner == "" || up.Name == "" || up.UpstreamHost == "" {
		return nil, &syncFailure{code: FailConfig, cause: fmt.Errorf(
			"incomplete upstream row: owner=%q name=%q host=%q", up.Owner, up.Name, up.UpstreamHost)}
	}

	tmp, err := os.MkdirTemp("", "openstorage-push-*")
	if err != nil {
		return nil, &syncFailure{code: FailInternal, cause: err}
	}
	defer os.RemoveAll(tmp)
	mirror := filepath.Join(tmp, "mirror.git")

	if stderr, err := runGit(ctx, tmp, hermeticGitEnv(w.allowProtocol()), "init", "--bare", mirror); err != nil {
		return nil, &syncFailure{code: FailInternal, cause: fmt.Errorf("git init: %w: %s", err, stderr)}
	}

	// Loopback fetch of the local refs. The token fence mirrors the pull
	// lane's push fence: mint nothing once the lease is inside the steal
	// margin. The fetch never exercises the receive-side claim fence
	// (upload-pack runs no update hook), but a fenced TTL keeps zombie
	// runs from authenticating ANY new loopback request past their lease.
	tokenTTL := time.Until(up.PushClaimExpiresAt) - claimFenceMargin
	if tokenTTL <= 0 {
		return nil, &syncFailure{code: FailInternal, cause: fmt.Errorf(
			"push claim lease expires at %v, inside the fence margin: refusing to fetch", up.PushClaimExpiresAt)}
	}
	token, err := w.Loopback.Token(ctx, job.Org, repo, tokenTTL, holder)
	if err != nil {
		return nil, &syncFailure{code: FailInternal, cause: fmt.Errorf("mint loopback token: %w", err)}
	}
	fetchEnv, err := authGitEnv(filepath.Join(tmp, "askpass-loopback.sh"), "t", token, w.allowProtocol())
	if err != nil {
		return nil, &syncFailure{code: FailInternal, cause: err}
	}
	// Self-call: never route through a proxy/load balancer (issue #45).
	fetchEnv = append(fetchEnv, "NO_PROXY=*", "no_proxy=*")
	fetchURL := strings.TrimRight(w.Loopback.BaseURL, "/") + "/" + repo.RepoID + ".git"
	fetchArgs := append([]string{"fetch", "--prune", fetchURL}, mirrorRefspecs...)
	if stderr, err := runGit(ctx, mirror, fetchEnv, fetchArgs...); err != nil {
		return nil, &syncFailure{code: FailInternal, cause: fmt.Errorf("loopback fetch: %w: %s", err, stderr)}
	}

	// Empty local repo: nothing to publish, the run still counts as a
	// success (converged by definition).
	refs, stderr, err := runGitOut(ctx, mirror, hermeticGitEnv(w.allowProtocol()), "for-each-ref", "--count=1", "--format=%(refname)")
	if err != nil {
		return nil, &syncFailure{code: FailInternal, cause: fmt.Errorf("git for-each-ref: %w: %s", err, stderr)}
	}
	if strings.TrimSpace(refs) == "" {
		return nil, nil
	}

	// Detach/steal barrier between the local fetch and the upstream push:
	// DELETE /repos/base cancels in-flight outbound runs silently, and a
	// stolen claim must not publish this run's stale local snapshot over
	// the thief's newer push. The push itself then runs under a
	// detach/steal WATCHDOG (below) that kills the subprocess mid-flight,
	// so the residual exposure is one poll interval, not the whole push.
	// Residual zombie window (a worker frozen past both): the run context
	// kills git via exec.CommandContext, and a non-force zombie push of
	// stale refs is refused per ref by the upstream itself; only a FORCED
	// zombie inside that window could rewind the upstream — same residual
	// class the pull lane documents for its fence margin, minus the token
	// expiry (upstream credentials are static).
	switch cur, err := w.Upstreams.GetUpstream(ctx, job.RepoID); {
	case errors.Is(err, ErrNotFound):
		return nil, errDetached
	case err != nil:
		return nil, &syncFailure{code: FailInternal, cause: fmt.Errorf("detach check: %w", err)}
	case cur.PushClaimHolder != holder:
		return nil, errClaimStolen
	}

	// Upstream credential: absent = let the push fail with git's own auth
	// classification (public upstreams reject unauthenticated pushes;
	// credential-less file:// upstreams in tests just work). Username
	// defaults to "git" for token-only providers, like the pull fetch.
	username, password := "", ""
	switch cred, err := w.Credentials.CredentialForRepo(ctx, job.RepoID); {
	case err == nil:
		username, password = cred.Username, cred.Password
	case !errors.Is(err, ErrNotFound):
		return nil, &syncFailure{code: FailInternal, cause: fmt.Errorf("load credential: %w", err)}
	}
	if username == "" {
		username = "git"
	}
	pushEnv, err := authGitEnv(filepath.Join(tmp, "askpass-upstream.sh"), username, password, w.allowProtocol())
	if err != nil {
		return nil, &syncFailure{code: FailInternal, cause: err}
	}
	// Egress guard (egress.go): every network push dials through the
	// vetting proxy — upstream_host is caller-linked data, so unguarded
	// egress would be an SSRF/write primitive into private networks.
	// file:// upstreams (tests; GIT_ALLOW_PROTOCOL-blocked in production)
	// have no network egress to guard.
	var egress *egressProxy
	if u := up.CloneURL(); strings.HasPrefix(u, "http://") || strings.HasPrefix(u, "https://") {
		egress, err = startEgressProxy(w.PushEgressAllowPrivate)
		if err != nil {
			return nil, &syncFailure{code: FailInternal, cause: err}
		}
		defer egress.Close()
		pushEnv = proxyEnv(pushEnv, egress.URL())
	}
	refspecs := pushRefspecs
	if force {
		refspecs = mirrorRefspecs
	}
	// --porcelain makes per-ref outcomes parseable (stdout) so a refused
	// non-fast-forward names the exact refs in the failed event.
	// --atomic makes the multi-ref forward all-or-nothing: without it, a
	// batch with one refused ref PARTIALLY updates the upstream while the
	// run still fails — and a failed run schedules no mirror-back, so a
	// concurrent pull that fetched pre-forward could permanently clobber
	// the refs that did land. With the transaction, a failed run means
	// the upstream is untouched and skipping AfterForward is sound; the
	// innocent refs in a refused batch report "atomic push failed" in
	// rejected_refs.
	pushArgs := append([]string{"push", "--porcelain", "--atomic", up.CloneURL()}, refspecs...)
	// Detach/steal watchdog for the push itself: the barrier above cannot
	// cover a DELETE /repos/base (or a steal) that commits while `git
	// push` is already talking to a slow upstream — without it, the
	// detach response claims cancellation while the subprocess keeps
	// publishing refs. Poll the row and kill the subprocess through its
	// context. Read ERRORS never kill a healthy push: the watchdog is a
	// guard, not a dependency, and a meta blip must not fail a forward.
	poll := w.DetachPoll
	if poll <= 0 {
		poll = defaultDetachPoll
	}
	pushCtx, cancelPush := context.WithCancel(ctx)
	defer cancelPush()
	watch := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(poll)
		defer ticker.Stop()
		for {
			select {
			case <-pushCtx.Done():
				return
			case <-ticker.C:
				switch cur, err := w.Upstreams.GetUpstream(pushCtx, job.RepoID); {
				case errors.Is(err, ErrNotFound):
					watch <- errDetached
					cancelPush()
					return
				case err != nil:
					continue
				case cur.PushClaimHolder != holder:
					watch <- errClaimStolen
					cancelPush()
					return
				}
			}
		}
	}()
	stdout, stderr, err := runGitOut(pushCtx, mirror, pushEnv, pushArgs...)
	cancelPush()
	select {
	case werr := <-watch:
		// Killed (or raced) by the watchdog: abandon silently/loudly per
		// the verdict — never emit a failed event for a canceled run.
		return nil, werr
	default:
	}
	if err != nil {
		// A recorded policy denial dominates every other classification:
		// the destination is refused by configuration, so retrying (or
		// reading rejection porcelain) would be noise.
		if egress != nil {
			if reason := egress.deniedReason(); reason != "" {
				return nil, &syncFailure{code: FailConfig, cause: fmt.Errorf("push egress denied: %s", reason)}
			}
		}
		if rejected := parsePushRejections(stdout); len(rejected) > 0 {
			return rejected, &syncFailure{code: FailRejected, cause: fmt.Errorf(
				"git push: %d ref(s) rejected by upstream: %w", len(rejected), err)}
		}
		// A recorded non-policy proxy dial failure IS the outage signal:
		// git itself only saw an opaque proxy HTTP error, whose stderr
		// would misclassify the transient failure as permanent and skip
		// the documented backoff retry. Checked after rejections — a
		// completed push conversation's per-ref verdicts outrank a blip
		// on some other connection.
		if egress != nil {
			if dialErr := egress.dialFailure(); dialErr != "" {
				return nil, &syncFailure{code: FailUnreachable, cause: fmt.Errorf("push egress dial: %s", dialErr)}
			}
		}
		return nil, &syncFailure{code: classifyPushFailure(stderr), cause: fmt.Errorf("git push: %w: %s", err, stderr)}
	}
	return nil, nil
}
