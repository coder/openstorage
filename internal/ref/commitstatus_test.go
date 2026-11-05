package ref

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/coder/openstorage/internal/testutil/pgfault"
)

// TestPGCASResolvesFailedCommit: when the COMMIT of a swap that changed a
// row fails, the CAS asks PostgreSQL for the outcome. A reply lost after
// the server committed is a win and runs Committed. A COMMIT the server
// never received is a definitive failure. An outcome PostgreSQL cannot
// confirm (the transaction still in progress when the wait runs out, or the
// status query failing) is an ambiguous error, which IdempotentCAS
// rechecks. pgx reports each of these losses as a request never sent, so
// without the status query every case would read as definitive.
func TestPGCASResolvesFailedCommit(t *testing.T) {
	s := pgTestStore(t)
	ctx := context.Background()
	const name = "refs/heads/main"
	a, b := testSHA(1), testSHA(2)
	for _, tc := range []struct {
		name      string
		fault     pgfault.Fault
		won       bool
		ambiguous bool   // for a failed CAS: the error IdempotentCAS rechecks
		want      string // the ref afterwards
	}{
		{"lost-reply", pgfault.DropReply, true, false, b},
		{"dropped-commit", pgfault.DropCommit, false, false, a},
		{"in-progress", pgfault.HoldCommit, false, true, a},
		{"status-query-fails", pgfault.CutAll, false, true, a},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := fmt.Sprintf("commitstatus-%s-%d", tc.name, time.Now().UnixNano())
			if won, err := s.CASRef(ctx, repo, name, ZeroSHA, a); err != nil || !won {
				t.Fatalf("seed: won=%v err=%v", won, err)
			}
			proxy, url := pgfault.Start(t, pgTestURL)
			ps, err := OpenPG(ctx, url)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(ps.Close)
			committed := 0
			hctx := WithCASTxHook(ctx, CASTxHook{
				InTx:      func(context.Context, pgx.Tx) error { return nil },
				Committed: func() { committed++ },
			})

			proxy.Arm(tc.fault, nil)
			start := time.Now()
			won, gen, err := ps.CASRefGen(hctx, repo, name, a, b)
			if elapsed := time.Since(start); elapsed > commitStatusTimeout+5*time.Second {
				t.Fatalf("the CAS took %v: the status wait is not bounded by %v", elapsed, commitStatusTimeout)
			}
			if !proxy.Fired() {
				t.Fatal("the COMMIT fault never fired")
			}
			if tc.won {
				g, gerr := s.GetGeneration(ctx, repo)
				if !won || err != nil || gerr != nil || gen != g {
					t.Fatalf("won=%v gen=%d err=%v (generation %d, %v), want a win with the committed generation", won, gen, err, g, gerr)
				}
			} else {
				if won || err == nil || !strings.Contains(err.Error(), "commit") {
					t.Fatalf("won=%v err=%v, want a commit error", won, err)
				}
				if got := ambiguousError(err); got != tc.ambiguous {
					t.Fatalf("ambiguousError(%v) = %v, want %v", err, got, tc.ambiguous)
				}
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
