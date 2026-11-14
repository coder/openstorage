package ref

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

const (
	idemSHA1 = "1111111111111111111111111111111111111111"
	idemSHA2 = "2222222222222222222222222222222222222222"
	idemSHA3 = "3333333333333333333333333333333333333333"
)

// errDrop is the fault the commitThenFail decorator injects after the inner
// CAS statement has committed.
var errDrop = errors.New("synthetic post-commit connection drop")

// commitThenFail is the fault-injecting decorator from issue #2's acceptance
// criteria: the inner CAS statement commits, then the store reports a
// connection drop (the ambiguous outcome). recheckDead additionally fails
// GetRef, as a truly dead connection would.
type commitThenFail struct {
	RefStore
	recheckDead bool
}

func (s *commitThenFail) CASRef(ctx context.Context, repo, name, old, new string) (bool, error) {
	if _, err := s.RefStore.CASRef(ctx, repo, name, old, new); err != nil {
		return false, err
	}
	return false, errDrop
}

func (s *commitThenFail) GetRef(ctx context.Context, repo, name string) (string, error) {
	if s.recheckDead {
		return "", errDrop
	}
	return s.RefStore.GetRef(ctx, repo, name)
}

// seedRef creates repo/name at sha directly through the raw store.
func seedRef(t *testing.T, s RefStore, repo, name, sha string) {
	t.Helper()
	won, err := s.CASRef(context.Background(), repo, name, ZeroSHA, sha)
	if err != nil || !won {
		t.Fatalf("seed CASRef = (%v, %v), want (true, nil)", won, err)
	}
}

func TestIdempotentCASRecoversAmbiguousUpdate(t *testing.T) {
	ctx := context.Background()
	raw, _ := openTestSQLite(t)
	seedRef(t, raw, "r1", "refs/heads/main", idemSHA1)
	st := IdempotentCAS{RefStore: &commitThenFail{RefStore: raw}}

	won, err := st.CASRef(ctx, "r1", "refs/heads/main", idemSHA1, idemSHA2)
	if !won || err != nil {
		t.Fatalf("CASRef = (%v, %v), want recovered (true, nil)", won, err)
	}
	if cur, err := raw.GetRef(ctx, "r1", "refs/heads/main"); err != nil || cur != idemSHA2 {
		t.Fatalf("ref after recovery = (%q, %v), want (%q, nil)", cur, err, idemSHA2)
	}
}

func TestIdempotentCASRecoversAmbiguousCreate(t *testing.T) {
	ctx := context.Background()
	raw, _ := openTestSQLite(t)
	st := IdempotentCAS{RefStore: &commitThenFail{RefStore: raw}}

	won, err := st.CASRef(ctx, "r1", "refs/heads/new", ZeroSHA, idemSHA1)
	if !won || err != nil {
		t.Fatalf("CASRef = (%v, %v), want recovered (true, nil)", won, err)
	}
}

func TestIdempotentCASRecheckFailureFallsBackToError(t *testing.T) {
	ctx := context.Background()
	raw, _ := openTestSQLite(t)
	seedRef(t, raw, "r1", "refs/heads/main", idemSHA1)
	st := IdempotentCAS{RefStore: &commitThenFail{RefStore: raw, recheckDead: true}}

	won, err := st.CASRef(ctx, "r1", "refs/heads/main", idemSHA1, idemSHA2)
	if won || !errors.Is(err, errDrop) {
		t.Fatalf("CASRef = (%v, %v), want (false, errDrop)", won, err)
	}
	// The update itself committed: this is the residual gap a dead recheck
	// leaves. The client's retry heals it (see the retry test below).
	if cur, _ := raw.GetRef(ctx, "r1", "refs/heads/main"); cur != idemSHA2 {
		t.Fatalf("ref = %q, want committed %q", cur, idemSHA2)
	}
}

// TestIdempotentCASRetryHealsUnreportedWin covers the client-visible symptom
// from issue #2: a retry of an UPDATE whose first attempt committed but
// failed to report matches zero rows (WHERE sha = old) and used to surface a
// spurious "moved concurrently" conflict. The recheck reconciles it to a win.
func TestIdempotentCASRetryHealsUnreportedWin(t *testing.T) {
	ctx := context.Background()
	raw, _ := openTestSQLite(t)
	seedRef(t, raw, "r1", "refs/heads/main", idemSHA1)
	st := IdempotentCAS{RefStore: raw}

	for attempt := 1; attempt <= 2; attempt++ {
		won, err := st.CASRef(ctx, "r1", "refs/heads/main", idemSHA1, idemSHA2)
		if !won || err != nil {
			t.Fatalf("attempt %d: CASRef = (%v, %v), want (true, nil)", attempt, won, err)
		}
	}
}

// failWith scripts a CASRef error WITHOUT executing the statement — the
// definitive-failure shape (backend rejection / request never sent), where
// any current == new state was put there by someone else.
type failWith struct {
	RefStore
	err error
}

func (s *failWith) CASRef(context.Context, string, string, string, string) (bool, error) {
	return false, s.err
}

// safeToRetryErr satisfies pgconn.SafeToRetry: the request provably never
// reached the server.
type safeToRetryErr struct{}

func (safeToRetryErr) Error() string     { return "context canceled before send" }
func (safeToRetryErr) SafeToRetry() bool { return true }

// TestIdempotentCASDefinitiveErrorNeverRecovers: when the backend itself
// rejected the statement (or the request never reached it), the statement
// did not commit — current == new is someone else's write, and recovering
// would fabricate a success (and a push event) for a transition this call
// never committed. The error must propagate unchanged.
func TestIdempotentCASDefinitiveErrorNeverRecovers(t *testing.T) {
	ctx := context.Background()
	for name, derr := range map[string]error{
		"pg backend rejection": fmt.Errorf("cas update r1/refs/heads/main: %w",
			&pgconn.PgError{Severity: "ERROR", Code: "40001", Message: "serialization failure"}),
		"request never sent": fmt.Errorf("cas update r1/refs/heads/main: %w", safeToRetryErr{}),
	} {
		t.Run(name, func(t *testing.T) {
			raw, _ := openTestSQLite(t)
			seedRef(t, raw, "r1", "refs/heads/main", idemSHA2) // someone else's write, already at target
			st := IdempotentCAS{RefStore: &failWith{RefStore: raw, err: derr}}

			won, err := st.CASRef(ctx, "r1", "refs/heads/main", idemSHA1, idemSHA2)
			if won || !errors.Is(err, derr) {
				t.Fatalf("CASRef = (%v, %v), want (false, original error)", won, err)
			}
		})
	}
}

// TestAmbiguousErrorClassification pins the boundary between definitive
// failures (statement provably did not commit) and ambiguous ones (result
// lost in transit; recovery applies).
func TestAmbiguousErrorClassification(t *testing.T) {
	// A real modernc.org/sqlite backend rejection, captured through the
	// same driver the store uses.
	raw, _ := openTestSQLite(t)
	_, sqliteErr := raw.db.ExecContext(context.Background(), `INSERT INTO no_such_table VALUES (1)`)
	if sqliteErr == nil {
		t.Fatal("expected a sqlite backend error")
	}

	for name, tc := range map[string]struct {
		err  error
		want bool
	}{
		"pg backend rejection":  {&pgconn.PgError{Code: "42501"}, false},
		"wrapped pg rejection":  {fmt.Errorf("cas: %w", &pgconn.PgError{Code: "40001"}), false},
		"sqlite backend error":  {fmt.Errorf("cas: %w", sqliteErr), false},
		"request never sent":    {safeToRetryErr{}, false},
		"acquire-stage failure": {fmt.Errorf("cas acquire r/n: %w", notExecuted(context.DeadlineExceeded)), false},
		"connection drop":       {fmt.Errorf("cas: %w", io.ErrUnexpectedEOF), true},
		"mid-flight cancel":     {fmt.Errorf("cas: %w", context.Canceled), true},
		"opaque driver failure": {errors.New("write: broken pipe"), true},
		// SQLSTATEs that explicitly encode an unknown outcome are the
		// backend itself reporting post-commit ambiguity.
		"pg statement completion unknown":   {fmt.Errorf("cas: %w", &pgconn.PgError{Code: "40003"}), true},
		"pg transaction resolution unknown": {fmt.Errorf("cas: %w", &pgconn.PgError{Code: "08007"}), true},
		// notExecuted proves the statement took no effect, so it wins over those SQLSTATEs.
		"not executed wraps statement completion unknown":   {fmt.Errorf("cas: %w", notExecuted(&pgconn.PgError{Code: "40003"})), false},
		"not executed wraps transaction resolution unknown": {fmt.Errorf("cas: %w", notExecuted(&pgconn.PgError{Code: "08007"})), false},
	} {
		t.Run(name, func(t *testing.T) {
			if got := ambiguousError(tc.err); got != tc.want {
				t.Fatalf("ambiguousError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestIdempotentCASAcquireFailureNeverRecovers pins the acquire-stage
// classification end to end through a REAL store: a context that is already
// dead when CASRef runs fails connection acquisition — before the statement
// could possibly execute — so even with the ref already at the target
// (someone else's write), the error must propagate instead of fabricating a
// success.
func TestIdempotentCASAcquireFailureNeverRecovers(t *testing.T) {
	raw, _ := openTestSQLite(t)
	seedRef(t, raw, "r1", "refs/heads/main", idemSHA2) // someone else's write, already at target
	st := IdempotentCAS{RefStore: raw}

	dead, cancel := context.WithCancel(context.Background())
	cancel()
	won, err := st.CASRef(dead, "r1", "refs/heads/main", idemSHA1, idemSHA2)
	if won || err == nil {
		t.Fatalf("CASRef = (%v, %v), want (false, acquire error)", won, err)
	}
	if ambiguousError(err) {
		t.Fatalf("acquire-stage error %v classified ambiguous, want definitive", err)
	}
}

// errOnlyCtx simulates a cancellation that lands immediately AFTER the
// store's connection acquire: Done() stays open (the acquire path's select
// proceeds), while Err() — which the store's pre-dispatch guard consults —
// already reports the cancellation.
type errOnlyCtx struct{ context.Context }

func (errOnlyCtx) Done() <-chan struct{} { return nil }
func (errOnlyCtx) Err() error            { return context.Canceled }

// TestIdempotentCASPostAcquireCancelNeverRecovers pins the pre-dispatch
// guard: a context canceled between acquire and statement dispatch is
// definitive non-execution, so even with the ref already at the target
// (someone else's write) the error propagates instead of fabricating a
// success.
func TestIdempotentCASPostAcquireCancelNeverRecovers(t *testing.T) {
	raw, _ := openTestSQLite(t)
	seedRef(t, raw, "r1", "refs/heads/main", idemSHA2) // someone else's write, already at target
	st := IdempotentCAS{RefStore: raw}

	won, err := st.CASRef(errOnlyCtx{context.Background()}, "r1", "refs/heads/main", idemSHA1, idemSHA2)
	if won || !errors.Is(err, context.Canceled) {
		t.Fatalf("CASRef = (%v, %v), want (false, context.Canceled)", won, err)
	}
	if ambiguousError(err) {
		t.Fatalf("post-acquire cancel %v classified ambiguous, want definitive", err)
	}
	// The guard fired before dispatch: the ref must be untouched.
	if cur, gerr := raw.GetRef(context.Background(), "r1", "refs/heads/main"); gerr != nil || cur != idemSHA2 {
		t.Fatalf("ref = (%q, %v), want untouched %q", cur, gerr, idemSHA2)
	}
}

// cancelAtFence ends its own context when casFence reads the sync-claim
// fence, which both stores do after the purge-fence read and right before
// the swap statement: Err() was still nil at the pre-dispatch guard, and the
// statement sees a done context.
type cancelAtFence struct {
	context.Context
	cancel func()
}

func (c cancelAtFence) Value(k any) any {
	if _, ok := k.(syncClaimFenceKey); ok {
		c.cancel()
	}
	return c.Context.Value(k)
}

// TestIdempotentCASStatementCancelNeverRecovers: a cancellation that lands
// right before the swap statement ends the statement with a plain context
// error. No COMMIT was sent, so nothing committed: even with the ref already
// at the target (someone else's write), the error propagates instead of
// fabricating a win and a push event.
func TestIdempotentCASStatementCancelNeverRecovers(t *testing.T) {
	for name, old := range map[string]string{"duplicate-create": ZeroSHA, "stale-update": idemSHA1} {
		t.Run(name, func(t *testing.T) {
			raw, _ := openTestSQLite(t)
			seedRef(t, raw, "r1", "refs/heads/main", idemSHA2)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			won, err := IdempotentCAS{RefStore: raw}.CASRef(cancelAtFence{ctx, cancel}, "r1", "refs/heads/main", old, idemSHA2)
			if won || !errors.Is(err, context.Canceled) || ambiguousError(err) {
				t.Fatalf("CASRef = (%v, %v), want (false, a definitive context.Canceled)", won, err)
			}
		})
	}
}

// TestIdempotentCASRandomCancelNeverFabricatesCreateWin: a create of the sha
// that the ref already holds changes no row, so no cancellation, at any point
// of the swap (acquire, begin, purge fence, statement or commit), may turn it
// into a win. The cancellations come from timers at random delays around one
// call's duration, so the run reaches every stage; the loop is not a
// flake risk, because a correct store never reports a win here.
func TestIdempotentCASRandomCancelNeverFabricatesCreateWin(t *testing.T) {
	raw, _ := openTestSQLite(t)
	seedRef(t, raw, "r1", "refs/heads/main", idemSHA2)
	start := time.Now()
	const calibration = 100
	for range calibration {
		if won, err := raw.CASRef(context.Background(), "r1", "refs/heads/main", ZeroSHA, idemSHA2); won || err != nil {
			t.Fatalf("calibration CASRef = (%v, %v), want (false, nil)", won, err)
		}
	}
	span := max(time.Since(start)/calibration*2, time.Microsecond)
	st := IdempotentCAS{RefStore: raw}
	for i := range 1000 {
		ctx, cancel := context.WithCancel(context.Background())
		timer := time.AfterFunc(rand.N(span), cancel)
		won, err := st.CASRef(ctx, "r1", "refs/heads/main", ZeroSHA, idemSHA2)
		timer.Stop()
		cancel()
		if won {
			t.Fatalf("iteration %d: duplicate create reported a win (err %v)", i, err)
		}
	}
}

// TestSQLiteCASErrorIsDefinitiveAndTrue: a SQLite CAS that fails, at any
// stage, committed nothing, and its error is definitive, so IdempotentCAS
// never recovers it into a win. Creates of new refs are cancelled by timers
// at random delays up to twice one call's typical duration (the median of a
// calibration run, so one stall does not stretch it), which lands the
// cancellations across the stages of the call, the COMMIT of a swap that
// changed a row among them. When the store returns an error, the ref must be
// absent and the error must not be ambiguous. A recovered win would need a
// second writer of the same sha, so the loop checks the premise (nothing
// committed) instead of racing one.
func TestSQLiteCASErrorIsDefinitiveAndTrue(t *testing.T) {
	raw, _ := openTestSQLite(t)
	const calibration = 100
	took := make([]time.Duration, calibration)
	for i := range took {
		start := time.Now()
		if won, err := raw.CASRef(context.Background(), "cal", fmt.Sprintf("refs/heads/c%d", i), ZeroSHA, idemSHA1); !won || err != nil {
			t.Fatalf("calibration CASRef = (%v, %v), want (true, nil)", won, err)
		}
		took[i] = time.Since(start)
	}
	slices.Sort(took)
	span := max(took[calibration/2]*2, time.Microsecond)
	errs := 0
	for i := range 1500 {
		name := fmt.Sprintf("refs/heads/b%d", i)
		ctx, cancel := context.WithCancel(context.Background())
		timer := time.AfterFunc(rand.N(span), cancel)
		won, err := raw.CASRef(ctx, "r1", name, ZeroSHA, idemSHA1)
		timer.Stop()
		cancel()
		cur, gerr := raw.GetRef(context.Background(), "r1", name)
		switch {
		case errors.Is(gerr, ErrNotFound):
			cur = ZeroSHA
		case gerr != nil:
			t.Fatalf("iteration %d: GetRef: %v", i, gerr)
		}
		switch {
		case err == nil && won && cur == idemSHA1:
		case err != nil && !won && cur == ZeroSHA && !ambiguousError(err):
			errs++
		default:
			t.Fatalf("iteration %d: CASRef = (%v, %v) with the ref at %q, want a win with the ref created, or a definitive error with no ref", i, won, err, cur)
		}
	}
	t.Logf("%d of 1500 creates were cancelled into a definitive error", errs)
}

// TestIdempotentCASCleanCreateLossIsAuthoritative pins the create decision:
// a clean create loss (won=false, err=nil) affirmatively wrote nothing, so
// it stays a conflict even when the existing ref holds the target sha —
// same-sha duplicate creates keep their "already exists" outcome instead of
// fabricating a success (and a zero->sha push event). Only the ambiguous
// ERROR create recovers (TestIdempotentCASRecoversAmbiguousCreate).
func TestIdempotentCASCleanCreateLossIsAuthoritative(t *testing.T) {
	ctx := context.Background()
	raw, _ := openTestSQLite(t)
	seedRef(t, raw, "r1", "refs/heads/main", idemSHA1)
	st := IdempotentCAS{RefStore: raw}

	won, err := st.CASRef(ctx, "r1", "refs/heads/main", ZeroSHA, idemSHA1)
	if won || err != nil {
		t.Fatalf("same-sha duplicate create = (%v, %v), want (false, nil)", won, err)
	}
}

func TestIdempotentCASGenuineConflictStillFails(t *testing.T) {
	ctx := context.Background()
	raw, _ := openTestSQLite(t)
	seedRef(t, raw, "r1", "refs/heads/main", idemSHA3) // moved by someone else

	// Clean loss: the ref holds a different sha, so the conflict stands.
	st := IdempotentCAS{RefStore: raw}
	if won, err := st.CASRef(ctx, "r1", "refs/heads/main", idemSHA1, idemSHA2); won || err != nil {
		t.Fatalf("CASRef = (%v, %v), want genuine conflict (false, nil)", won, err)
	}

	// Ambiguous error + a different current sha: the error stands too.
	faulty := IdempotentCAS{RefStore: &commitThenFail{RefStore: raw}}
	if won, err := faulty.CASRef(ctx, "r1", "refs/heads/main", idemSHA1, idemSHA2); won || !errors.Is(err, errDrop) {
		t.Fatalf("CASRef = (%v, %v), want (false, errDrop)", won, err)
	}

	// Create against an existing ref at a different sha: conflict stands.
	if won, err := st.CASRef(ctx, "r1", "refs/heads/main", ZeroSHA, idemSHA2); won || err != nil {
		t.Fatalf("create CASRef = (%v, %v), want (false, nil)", won, err)
	}
}

// TestIdempotentCASDeleteNeverRecovers pins the documented delete decision:
// absence is not positive evidence (never-existed and concurrent-delete look
// identical), so neither a clean lost delete nor an ambiguous-error delete
// short-circuits to success.
func TestIdempotentCASDeleteNeverRecovers(t *testing.T) {
	ctx := context.Background()
	raw, _ := openTestSQLite(t)
	st := IdempotentCAS{RefStore: raw}

	// Deleting a ref that does not exist is a conflict, not a success.
	if won, err := st.CASRef(ctx, "r1", "refs/heads/gone", idemSHA1, ZeroSHA); won || err != nil {
		t.Fatalf("absent delete = (%v, %v), want (false, nil)", won, err)
	}

	// Ambiguous-error delete: the DELETE commits, the error is still
	// reported (documented decision: no fabricated success, no fabricated
	// deletion webhook).
	seedRef(t, raw, "r1", "refs/heads/doomed", idemSHA1)
	faulty := IdempotentCAS{RefStore: &commitThenFail{RefStore: raw}}
	won, err := faulty.CASRef(ctx, "r1", "refs/heads/doomed", idemSHA1, ZeroSHA)
	if won || !errors.Is(err, errDrop) {
		t.Fatalf("ambiguous delete = (%v, %v), want (false, errDrop)", won, err)
	}
	if _, err := raw.GetRef(ctx, "r1", "refs/heads/doomed"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetRef after committed delete = %v, want ErrNotFound", err)
	}

	// Accepted cost, pinned: the retry of that delete sees a spurious
	// conflict (the row is already gone).
	if won, err := st.CASRef(ctx, "r1", "refs/heads/doomed", idemSHA1, ZeroSHA); won || err != nil {
		t.Fatalf("retried delete = (%v, %v), want (false, nil)", won, err)
	}
}

// CASRefGen on commitThenFail mirrors its CASRef fault: the inner swap
// commits (and attributes a generation), then the response is lost — so
// the attribution is lost with it, exactly like a real dead connection.
func (s *commitThenFail) CASRefGen(ctx context.Context, repo, name, old, new string) (bool, int64, error) {
	if _, _, err := s.RefStore.CASRefGen(ctx, repo, name, old, new); err != nil {
		return false, 0, err
	}
	return false, 0, errDrop
}

// TestIdempotentCASRefGenRecoveredWinUnattributed (issue #186 finding 5):
// a recovered win must forward gen == 0 — the committed generation is
// unknowable after the fact, and guessing via GetGeneration could blend
// in a concurrent writer's bump, over-claiming a range for the push-hold
// coalescer. Clean wins forward the inner attribution untouched.
func TestIdempotentCASRefGenRecoveredWinUnattributed(t *testing.T) {
	ctx := context.Background()
	raw, _ := openTestSQLite(t)
	seedRef(t, raw, "r-gen", "refs/heads/main", idemSHA1)

	// Clean win: exact attribution passes through.
	clean := IdempotentCAS{RefStore: raw}
	won, gen, err := clean.CASRefGen(ctx, "r-gen", "refs/heads/main", idemSHA1, idemSHA2)
	if !won || err != nil || gen <= 0 {
		t.Fatalf("clean CASRefGen = (%v, %d, %v), want (true, >0, nil)", won, gen, err)
	}
	if g, err := raw.GetGeneration(ctx, "r-gen"); err != nil || g != gen {
		t.Fatalf("GetGeneration = (%d, %v), want (%d, nil)", g, err, gen)
	}

	// Recovered win: the swap committed, the attribution is gone.
	st := IdempotentCAS{RefStore: &commitThenFail{RefStore: raw}}
	won, gen, err = st.CASRefGen(ctx, "r-gen", "refs/heads/main", idemSHA2, idemSHA1)
	if !won || err != nil {
		t.Fatalf("CASRefGen = (%v, %v), want recovered (true, nil)", won, err)
	}
	if gen != 0 {
		t.Fatalf("recovered win attribution = %d, want 0 (unattributed, fail-safe)", gen)
	}
}
