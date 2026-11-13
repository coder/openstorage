package ref

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/coder/openstorage/internal/testutil/pgfault"
)

// TestPGCASResolvesFailedCommit: when the COMMIT of a swap that changed a
// row fails, the CAS asks PostgreSQL for the outcome. A reply lost after
// the server committed is a win and runs Committed, also when the pool has
// one connection, when the COMMIT lands late, and when the caller's context
// ends with the fault. A COMMIT the server never received is a definitive
// failure. An outcome PostgreSQL cannot confirm (the transaction still in
// progress when the wait runs out, or the status query failing) is an
// ambiguous error, which IdempotentCAS rechecks. A swap that changed no row
// wrote nothing: its COMMIT error is marked not executed, with no status
// query, so IdempotentCAS never recovers it (a duplicate create stays a
// conflict). Up to pgx v5.12 each of these losses read as a request never
// sent, so without the status query every case would read as definitive.
// Since v5.13 each reads as ambiguous, and the status query still tells a
// commit that landed from one that did not.
func TestPGCASResolvesFailedCommit(t *testing.T) {
	s := pgTestStore(t)
	ctx := context.Background()
	const name = "refs/heads/main"
	a, b := testSHA(1), testSHA(2)
	for _, tc := range []struct {
		name          string
		fault         pgfault.Fault
		oneConn       bool // pool_max_conns=1: the CAS holds the pool's only connection
		cancelAtFault bool // the caller's context ends when the fault fires
		noRowChanged  bool // the swap's old sha is stale, so it changes no row
		dupCreate     bool // with noRowChanged: a create (old is ZeroSHA) of the sha that the ref already holds
		warmPool      bool // the pool holds a second idle connection, so the first status query answers at once
		won           bool
		ambiguous     bool   // for a failed CAS that changed a row: the error IdempotentCAS rechecks
		want          string // the ref afterwards
	}{
		{name: "lost-reply", fault: pgfault.DropReply, won: true, want: b},
		{name: "lost-reply-one-conn", fault: pgfault.DropReply, oneConn: true, won: true, want: b},
		{name: "lost-reply-canceled", fault: pgfault.DropReply, cancelAtFault: true, won: true, want: b},
		{name: "delayed-commit", fault: pgfault.DelayCommit, warmPool: true, won: true, want: b},
		{name: "dropped-commit", fault: pgfault.DropCommit, want: a},
		{name: "in-progress", fault: pgfault.HoldCommit, ambiguous: true, want: a},
		{name: "status-query-fails", fault: pgfault.CutAll, ambiguous: true, want: a},
		{name: "no-row-changed", fault: pgfault.DropReply, noRowChanged: true, want: a},
		{name: "duplicate-create", fault: pgfault.DropReply, noRowChanged: true, dupCreate: true, want: a},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := fmt.Sprintf("commitstatus-%s-%d", tc.name, time.Now().UnixNano())
			if won, err := s.CASRef(ctx, repo, name, ZeroSHA, a); err != nil || !won {
				t.Fatalf("seed: won=%v err=%v", won, err)
			}
			proxy, url := pgfault.Start(t, pgTestURL)
			if tc.oneConn {
				url += "&pool_max_conns=1"
			}
			ps, err := OpenPG(ctx, url)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(ps.Close)
			if tc.warmPool {
				// Two connections at once, so the pool keeps a second one
				// idle and the status query needs no new connection.
				c1, err := ps.pool.Acquire(ctx)
				if err != nil {
					t.Fatal(err)
				}
				c2, err := ps.pool.Acquire(ctx)
				c1.Release()
				if err != nil {
					t.Fatal(err)
				}
				c2.Release()
			}
			cctx, cancel := context.WithCancel(ctx)
			defer cancel()
			committed := 0
			hctx := WithCASTxHook(cctx, CASTxHook{
				InTx:      func(context.Context, pgx.Tx) error { return nil },
				Committed: func() { committed++ },
			})
			var onFault func()
			if tc.cancelAtFault {
				onFault = cancel
			}
			old, target := a, b
			if tc.noRowChanged {
				old = testSHA(3)
			}
			if tc.dupCreate {
				old, target = ZeroSHA, a
			}

			proxy.Arm(tc.fault, onFault)
			start := time.Now()
			won, gen, err := ps.CASRefGen(hctx, repo, name, old, target)
			if elapsed := time.Since(start); elapsed > commitStatusTimeout+5*time.Second {
				t.Fatalf("the CAS took %v: the status wait is not bounded by %v", elapsed, commitStatusTimeout)
			}
			if !proxy.Fired() {
				t.Fatal("the COMMIT fault never fired")
			}
			switch {
			case tc.won:
				g, gerr := s.GetGeneration(ctx, repo)
				if !won || err != nil || gerr != nil || gen != g {
					t.Fatalf("won=%v gen=%d err=%v (generation %d, %v), want a win with the committed generation", won, gen, err, g, gerr)
				}
			case won || err == nil || !strings.Contains(err.Error(), "commit"):
				t.Fatalf("won=%v err=%v, want a commit error", won, err)
			case tc.noRowChanged:
				// Nothing was written, so the error is definitive whatever tag pgx puts.
				var notExec notExecutedError
				if !errors.As(err, &notExec) || ambiguousError(err) {
					t.Fatalf("err=%v (ambiguous %v), want a not-executed error", err, ambiguousError(err))
				}
			case ambiguousError(err) != tc.ambiguous:
				t.Fatalf("ambiguousError(%v) = %v, want %v", err, !tc.ambiguous, tc.ambiguous)
			}
			wantCommitted := 0
			if tc.won {
				wantCommitted = 1
			}
			if committed != wantCommitted {
				t.Fatalf("Committed ran %d times, want %d", committed, wantCommitted)
			}
			if got, err := s.GetRef(ctx, repo, name); err != nil || got != tc.want {
				t.Fatalf("ref = %q (err %v), want %s", got, err, tc.want)
			}
		})
	}
}
