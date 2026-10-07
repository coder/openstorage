package sync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	stdsync "sync"
	"time"

	"github.com/coder/openstorage/internal/meta"
)

// LocalLanes is the durable local schedule (issue #266, ADR-50): the lane
// row owns pending level, due time, retry position, and claim. Only SQLite
// provides it (meta.LocalScheduleStore). A worker with Local set claims
// through ClaimDue* and ends runs through Finish*Outcome, and keeps no
// process-local retry or defer timers.
type LocalLanes interface {
	RequestSync(ctx context.Context, repoID string) error
	ClaimDuePush(ctx context.Context, repoID, holder string, lease time.Duration) (LaneClaim, error)
	ClaimDueSync(ctx context.Context, repoID, holder string, lease time.Duration) (LaneClaim, error)
	FinishPushOutcome(ctx context.Context, repoID, holder string, o meta.PushOutcome) (Upstream, error)
	FinishSyncOutcome(ctx context.Context, repoID, holder string, o meta.SyncOutcome) (Upstream, error)
	ListLocalWork(ctx context.Context, org, after string, limit int) ([]meta.LocalWork, error)
	CompleteSyncRepoCreate(ctx context.Context, org, repoID string) (meta.CreateOutcome, error)
}

// LaneClaim is a conditional claim's result: exactly one of Claimed, Busy
// (a live claim holds the lane), or neither (no durable work due) holds.
type LaneClaim struct {
	Up         Upstream
	Claimed    bool
	Busy       bool
	Force      bool
	Attempt    int
	StolenFrom string
}

// MetaLocalLanes adapts ms when it provides the local schedule (SQLite);
// ok is false for PG and S3WAL, which keep their existing scheduling.
func MetaLocalLanes(ms meta.EventingStore) (LocalLanes, bool) {
	assertf(ms != nil, "MetaLocalLanes: nil meta store")
	ls, ok := ms.(meta.LocalScheduleStore)
	if !ok {
		return nil, false
	}
	return metaLocalLanes{es: ms, ls: ls}, true
}

type metaLocalLanes struct {
	es meta.EventingStore
	ls meta.LocalScheduleStore
}

func mapLaneErr(err error) error {
	if errors.Is(err, meta.ErrClaimLost) {
		return fmt.Errorf("%w: %v", ErrClaimLost, err)
	}
	return mapMetaErr(err, ErrNotFound, nil)
}

func (m metaLocalLanes) RequestSync(ctx context.Context, repoID string) error {
	return mapLaneErr(m.es.RequestSync(ctx, repoID))
}

func laneClaim(c meta.LocalClaim, err error) (LaneClaim, error) {
	if err != nil {
		return LaneClaim{}, mapLaneErr(err)
	}
	return LaneClaim{Up: fromMetaUpstream(c.Up), Claimed: c.Claimed, Busy: c.Busy, Force: c.Force,
		Attempt: c.Attempt, StolenFrom: c.StolenFrom}, nil
}

func (m metaLocalLanes) ClaimDuePush(ctx context.Context, repoID, holder string, lease time.Duration) (LaneClaim, error) {
	return laneClaim(m.ls.ClaimDuePush(ctx, repoID, holder, lease))
}

func (m metaLocalLanes) ClaimDueSync(ctx context.Context, repoID, holder string, lease time.Duration) (LaneClaim, error) {
	return laneClaim(m.ls.ClaimDueSync(ctx, repoID, holder, lease))
}

func (m metaLocalLanes) FinishPushOutcome(ctx context.Context, repoID, holder string, o meta.PushOutcome) (Upstream, error) {
	row, err := m.ls.FinishPushOutcome(ctx, repoID, holder, o)
	if err != nil {
		return Upstream{}, mapLaneErr(err)
	}
	return fromMetaUpstream(row), nil
}

func (m metaLocalLanes) FinishSyncOutcome(ctx context.Context, repoID, holder string, o meta.SyncOutcome) (Upstream, error) {
	row, err := m.ls.FinishSyncOutcome(ctx, repoID, holder, o)
	if err != nil {
		return Upstream{}, mapLaneErr(err)
	}
	return fromMetaUpstream(row), nil
}

func (m metaLocalLanes) ListLocalWork(ctx context.Context, org, after string, limit int) ([]meta.LocalWork, error) {
	return m.ls.ListLocalWork(ctx, org, after, limit)
}

// localSweepSlots bounds jobs started by the sweep (queued or running).
// Explicit enqueues are not bounded, as with InProcessQueue.
const localSweepSlots = 4

// localSweepPage is how many due repos one sweep pass reads.
const localSweepPage = 16

// localSweepTimeout bounds one pass's metadata read.
const localSweepTimeout = 10 * time.Second

// LocalScheduler replaces InProcessQueue on SQLite (ADR-50). Durable rows
// own the schedule; this type only turns them into runs. Every run claims
// through ClaimDue*, so a stale, duplicate, or dropped hint cannot admit,
// delay, or cancel work: a dropped hint is offered again by the next sweep.
// Process-local state is limited to hint bookkeeping (in-flight keys, sweep
// slots, the sweep cursor), and no lock is held across store I/O.
type LocalScheduler struct {
	lanes   LocalLanes
	org     string
	runSync func(ctx context.Context, job Job) error
	runPush func(ctx context.Context, job Job) error
	logger  *slog.Logger
	slots   chan struct{}

	mu       stdsync.Mutex
	inflight map[string]bool // lane-prefixed repo ID -> queued or running
	closed   bool
	draining bool              // Drain started: no new runs, requests still recorded
	wg       stdsync.WaitGroup // enqueues and sweep passes
	runs     stdsync.WaitGroup // run goroutines; no Add once draining or closed
	// runCtx is every run's parent context. Drain cancels it with
	// ErrShutdown when its wait ends, and the worker then ends the run
	// with OutcomeRestore (issue #342).
	runCtx     context.Context
	cancelRuns context.CancelCauseFunc

	sweepMu  stdsync.Mutex // serializes passes; guards cursor
	cursor   string
	stop     context.CancelFunc
	sweeping stdsync.WaitGroup
}

var _ Queue = (*LocalScheduler)(nil)

// NewLocalScheduler builds a scheduler for org's repos over lanes, running
// runSync ((*Worker).Run) and runPush ((*Worker).RunPush).
func NewLocalScheduler(lanes LocalLanes, org string, runSync, runPush func(ctx context.Context, job Job) error, logger *slog.Logger) *LocalScheduler {
	assertf(lanes != nil && org != "" && runSync != nil && runPush != nil && logger != nil,
		"NewLocalScheduler: missing dependency")
	runCtx, cancelRuns := context.WithCancelCause(context.Background())
	return &LocalScheduler{lanes: lanes, org: org, runSync: runSync, runPush: runPush, logger: logger,
		slots: make(chan struct{}, localSweepSlots), inflight: make(map[string]bool),
		runCtx: runCtx, cancelRuns: cancelRuns}
}

// EnqueueSync records a durable pull request, then offers a run. It serves
// callers without a request of their own (the initial sync at repo
// creation, the invariant-11 poke); pull-upstream records its request
// first and uses OfferSync.
func (s *LocalScheduler) EnqueueSync(ctx context.Context, job Job) error {
	done, err := s.enter()
	if err != nil {
		return err
	}
	defer done()
	if err := s.lanes.RequestSync(ctx, job.RepoID); err != nil {
		return fmt.Errorf("record sync request: %w", err)
	}
	return s.offer(job, false, false)
}

// OfferSync offers a pull run for a request the caller already recorded.
// Recording it again here could re-mark a request that a run claimed in
// between, and that would start a second run for one request.
func (s *LocalScheduler) OfferSync(_ context.Context, job Job) error {
	done, err := s.enter()
	if err != nil {
		return err
	}
	defer done()
	return s.offer(job, false, false)
}

// EnqueuePush offers a push run. Every caller records its request first:
// the API's push-upstream with RequestPushNow (due now), the forward poke
// with RequestPush (joins a waiting backoff). The row decides whether the
// run may claim.
func (s *LocalScheduler) EnqueuePush(_ context.Context, job Job) error {
	done, err := s.enter()
	if err != nil {
		return err
	}
	defer done()
	return s.offer(job, true, false)
}

// enter admits one enqueue: Close waits for it (so no store call follows
// Close) until the returned done runs.
func (s *LocalScheduler) enter() (done func(), err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errSchedulerClosed
	}
	s.wg.Add(1)
	return s.wg.Done, nil
}

var errSchedulerClosed = errors.New("sync queue is closed")

// offer starts a run for job's lane unless one is already queued or
// running (that run, or the next sweep, covers the durable request). Sweep
// offers also need a free slot; they are dropped otherwise, and every offer
// is dropped once Drain started. It fails only when the scheduler closed.
// A dropped or failed offer leaves the durable request for a later sweep,
// in this process or the next.
func (s *LocalScheduler) offer(job Job, push, fromSweep bool) error {
	assertf(job.Org != "" && job.RepoID != "", "offer: incomplete job %+v", job)
	key, run := "pull:"+job.RepoID, s.runSync
	if push {
		key, run = "push:"+job.RepoID, s.runPush
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errSchedulerClosed
	}
	if s.draining || s.inflight[key] {
		return nil
	}
	if fromSweep && !push && s.inflight["push:"+job.RepoID] {
		// A push run's success write marks its mirror-back pull before
		// the run emits its terminal event. Start the pull on a later
		// pass, after the push lifecycle, as AfterForward does off SQLite.
		return nil
	}
	if fromSweep {
		select {
		case s.slots <- struct{}{}:
		default:
			return nil
		}
	}
	s.inflight[key] = true
	s.runs.Go(func() {
		defer func() {
			s.mu.Lock()
			delete(s.inflight, key)
			s.mu.Unlock()
			if fromSweep {
				<-s.slots
			}
		}()
		if err := run(s.runCtx, job); err != nil {
			s.logger.Warn("local sync run finished with error", "lane", key, "err", err)
		}
	})
	return nil
}

// SweepOnce reads one page of due lanes (rotating through the served org's
// repos) and offers a run for each. It is the only way deferred work
// (backoffs, expired claims, mid-run intent, hook-written intent) becomes a
// run.
func (s *LocalScheduler) SweepOnce(ctx context.Context) error {
	s.mu.Lock()
	if s.closed || s.draining {
		s.mu.Unlock()
		return nil
	}
	s.wg.Add(1) // Close waits for this pass: no store call after Close returns
	s.mu.Unlock()
	defer s.wg.Done()
	s.sweepMu.Lock()
	defer s.sweepMu.Unlock()
	rctx, cancel := context.WithTimeout(ctx, localSweepTimeout)
	defer cancel()
	work, err := s.lanes.ListLocalWork(rctx, s.org, s.cursor, localSweepPage)
	if err != nil {
		return fmt.Errorf("list local work: %w", err)
	}
	for _, w := range work {
		// An offer fails only after Close; the next process sweeps again.
		if w.Push {
			_ = s.offer(Job{Org: s.org, RepoID: w.RepoID}, true, true)
		}
		if w.Pull {
			_ = s.offer(Job{Org: s.org, RepoID: w.RepoID}, false, true)
		}
	}
	s.cursor = ""
	if len(work) == localSweepPage {
		s.cursor = work[len(work)-1].RepoID // continue after this page next pass
	}
	return nil
}

// Start runs SweepOnce after delay (the loopback listener must be serving
// before recovered runs push) and then every interval, until Close.
func (s *LocalScheduler) Start(delay, interval time.Duration) {
	assertf(delay >= 0 && interval > 0, "Start: bad delay %v or interval %v", delay, interval)
	ctx, cancel := context.WithCancel(context.Background())
	s.mu.Lock()
	assertf(s.stop == nil && !s.closed, "LocalScheduler started twice or after Close")
	s.stop = cancel
	s.mu.Unlock()
	s.sweeping.Go(func() {
		wait := delay
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
			if err := s.SweepOnce(ctx); err != nil && ctx.Err() == nil {
				s.logger.Warn("local schedule sweep failed", "err", err)
			}
			wait = interval
		}
	})
}

// Stop stops the sweep and stops accepting runs without waiting for runs
// in flight.
func (s *LocalScheduler) Stop() {
	s.mu.Lock()
	stop := s.stop
	s.closed = true
	s.mu.Unlock()
	if stop != nil {
		stop()
	}
}

// ErrShutdown is the cause serve's shutdown drain cancels sync and push
// runs with (LocalScheduler.Drain, jobs.River.DrainSyncRuns). A run whose
// context carries it keeps its request instead of counting a failure.
var ErrShutdown = errors.New("server shutting down")

// shutdownAborted reports whether a shutdown drain cancelled the run
// behind ctx.
func shutdownAborted(ctx context.Context) bool {
	return errors.Is(context.Cause(ctx), ErrShutdown)
}

// Drain prepares shutdown while the loopback listener still serves (issue
// #342). It stops the sweep and new runs, waits up to wait for runs in
// flight, then cancels the remaining runs with ErrShutdown and waits for
// them to record that. Enqueues still record their durable request; its
// run is left to the next process's sweep. A cancelled run ends with
// OutcomeRestore and keeps its request, so correctness does not depend on
// wait. ctx bounds the whole drain: a run still unfinished when it ends is
// left to Close, and if the process dies first, its claim expires with its
// lease.
func (s *LocalScheduler) Drain(ctx context.Context, wait time.Duration) {
	assertf(wait >= 0, "Drain: negative wait %v", wait)
	s.mu.Lock()
	s.draining = true
	stop := s.stop
	s.mu.Unlock()
	if stop != nil {
		stop()
	}
	s.sweeping.Wait()
	done := make(chan struct{})
	go func() {
		s.runs.Wait() // no run starts once draining is set
		close(done)
	}()
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-done:
		return
	case <-timer.C:
	case <-ctx.Done():
	}
	s.cancelRuns(ErrShutdown)
	select {
	case <-done: // every cancelled run has written its restore
	case <-ctx.Done():
	}
}

// Close stops the scheduler and waits for in-flight runs. After it returns
// the scheduler makes no store calls.
func (s *LocalScheduler) Close() {
	s.Stop()
	s.sweeping.Wait()
	s.wg.Wait()   // enqueues and sweep passes
	s.runs.Wait() // runs, including any Drain cancelled
}
