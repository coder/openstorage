package ref

import (
	"context"
	"errors"
	"testing"
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
// from issue #2: a retry of an update whose first attempt committed but
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
	// Same for a retried create: INSERT ... ON CONFLICT DO NOTHING affects
	// zero rows on the retry, but the ref holds the target.
	for attempt := 1; attempt <= 2; attempt++ {
		won, err := st.CASRef(ctx, "r1", "refs/heads/created", ZeroSHA, idemSHA3)
		if !won || err != nil {
			t.Fatalf("create attempt %d: CASRef = (%v, %v), want (true, nil)", attempt, won, err)
		}
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
