package ref

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestPGCASTxHookCommitsWithSwap: a CASTxHook runs only for a CAS that
// changed a row, its writes commit with the swap, Committed runs only after
// a successful COMMIT, and an InTx error rolls the swap back as a
// definitive failure that IdempotentCAS does not recover.
func TestPGCASTxHookCommitsWithSwap(t *testing.T) {
	s := pgTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS cas_tx_hook_probe (repo text, sha text)`); err != nil {
		t.Fatal(err)
	}
	// A row whose deferred foreign key has no parent fails only at COMMIT.
	if _, err := s.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS cas_tx_hook_parent (id int PRIMARY KEY);
		CREATE TABLE IF NOT EXISTS cas_tx_hook_orphan (parent int REFERENCES cas_tx_hook_parent (id) DEFERRABLE INITIALLY DEFERRED)`); err != nil {
		t.Fatal(err)
	}
	repo := fmt.Sprintf("castxhook-%d", time.Now().UnixNano())
	probes := func() int {
		t.Helper()
		var n int
		if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM cas_tx_hook_probe WHERE repo = $1`, repo).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	calls, committed := 0, 0
	hookErr := error(nil)
	failCommit := false
	hctx := WithCASTxHook(ctx, CASTxHook{
		InTx: func(ctx context.Context, tx pgx.Tx) error {
			calls++
			if _, err := tx.Exec(ctx, `INSERT INTO cas_tx_hook_probe VALUES ($1, 'x')`, repo); err != nil {
				return err
			}
			if failCommit {
				if _, err := tx.Exec(ctx, `INSERT INTO cas_tx_hook_orphan VALUES (-1)`); err != nil {
					return err
				}
			}
			return hookErr
		},
		Committed: func() { committed++ },
	})
	a, b := testSHA(1), testSHA(2)

	if won, _, err := s.CASRefGen(hctx, repo, "refs/heads/main", ZeroSHA, a); err != nil || !won {
		t.Fatalf("create: won=%v err=%v", won, err)
	}
	if calls != 1 || committed != 1 || probes() != 1 {
		t.Fatalf("won create: hook calls=%d committed=%d probe rows=%d, want 1, 1, 1", calls, committed, probes())
	}

	if won, err := s.CASRef(hctx, repo, "refs/heads/main", b, a); err != nil || won {
		t.Fatalf("stale update: won=%v err=%v", won, err)
	}
	if calls != 1 || committed != 1 || probes() != 1 {
		t.Fatalf("lost CAS ran the hook: calls=%d committed=%d probe rows=%d", calls, committed, probes())
	}

	hookErr = errors.New("intent write failed")
	won, err := IdempotentCAS{RefStore: s}.CASRef(hctx, repo, "refs/heads/main", a, b)
	if won || !errors.Is(err, hookErr) {
		t.Fatalf("failing hook: won=%v err=%v, want the hook error", won, err)
	}
	if got, err := s.GetRef(ctx, repo, "refs/heads/main"); err != nil || got != a {
		t.Fatalf("failing hook left main = %q (err %v), want %s", got, err, a)
	}
	if calls != 2 || committed != 1 || probes() != 1 {
		t.Fatalf("failing hook's write survived: calls=%d committed=%d probe rows=%d", calls, committed, probes())
	}

	// A COMMIT that fails after the hook ran: Committed must not run, and
	// nothing the transaction wrote survives.
	hookErr, failCommit = nil, true
	if won, err := s.CASRef(hctx, repo, "refs/heads/main", a, b); won || err == nil || !strings.Contains(err.Error(), "commit") {
		t.Fatalf("failed commit: won=%v err=%v, want a commit error", won, err)
	}
	if got, err := s.GetRef(ctx, repo, "refs/heads/main"); err != nil || got != a {
		t.Fatalf("failed commit left main = %q (err %v), want %s", got, err, a)
	}
	if calls != 3 || committed != 1 || probes() != 1 {
		t.Fatalf("failed commit: calls=%d committed=%d probe rows=%d, want 3, 1, 1 (Committed must wait for COMMIT)", calls, committed, probes())
	}
}
