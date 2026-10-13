package meta

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestPGCompleteRepoCreateWaitsForLock (issue #388, T4.2): a second
// completion of one pending repo waits on the first one's row lock, then
// sees its outcome. A committed first completion leaves nothing for the
// second to schedule; a failed one leaves the create for the second.
// SQLite needs no such test: BEGIN IMMEDIATE admits one writer at a time.
func TestPGCompleteRepoCreateWaitsForLock(t *testing.T) {
	pg := pgBackend(t).(*PG)
	for _, firstFails := range []bool{false, true} {
		name := map[bool]string{false: "first commits", true: "first fails"}[firstFails]
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			org := seedOrg(t, pg)
			r, err := pg.CreateLinkedRepo(ctx, org, "locked-"+newRepoID(), "main",
				RepoUpstream{Provider: "github", Owner: "o", Name: "n", UpstreamHost: "github.com"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			// complete runs one completion in its own transaction; its
			// schedule step counts calls and runs step (nil: nothing).
			complete := func(step func() error) (out CreateOutcome, calls int, err error) {
				err = pgx.BeginFunc(ctx, pg.pool, func(tx pgx.Tx) error {
					var err error
					out, err = PGCompleteRepoCreate(ctx, tx, org, r.RepoID, func(context.Context) error {
						calls++
						if step != nil {
							return step()
						}
						return nil
					})
					return err
				})
				return out, calls, err
			}

			paused, release := make(chan struct{}), make(chan struct{})
			firstErr := make(chan error, 1)
			go func() {
				_, _, err := complete(func() error {
					close(paused) // the row lock is held from here to commit
					<-release
					if firstFails {
						return errors.New("injected schedule failure")
					}
					return nil
				})
				firstErr <- err
			}()
			select {
			case <-paused:
			case <-time.After(30 * time.Second):
				t.Fatal("the first completion never reached its schedule step")
			}

			type result struct {
				out   CreateOutcome
				calls int
				err   error
			}
			second := make(chan result, 1)
			go func() {
				out, calls, err := complete(nil)
				second <- result{out, calls, err}
			}()
			// The second completion must be waiting on the row lock.
			deadline := time.Now().Add(30 * time.Second)
			for {
				var waiting int
				if err := pg.pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
					WHERE datname = current_database() AND wait_event_type = 'Lock' AND query LIKE '%FOR UPDATE%'`).Scan(&waiting); err != nil {
					t.Fatal(err)
				}
				if waiting == 1 {
					break
				}
				select {
				case res := <-second:
					t.Fatalf("the second completion did not wait for the row lock: %+v", res)
				default:
				}
				if time.Now().After(deadline) {
					t.Fatalf("no completion waits on the row lock (%d waiting)", waiting)
				}
				time.Sleep(10 * time.Millisecond)
			}
			close(release)

			err = <-firstErr
			res := <-second
			switch {
			case !firstFails && (err != nil || res.err != nil || res.out != CreateAlreadyComplete || res.calls != 0):
				t.Fatalf("first = %v; second = %+v; want the second to see complete and schedule nothing", err, res)
			case firstFails && (err == nil || res.err != nil || res.out != CreateCompleted || res.calls != 1):
				t.Fatalf("first = %v; second = %+v; want the first to fail and the second to complete once", err, res)
			}
			if got, err := pg.GetRepoByID(ctx, org, r.RepoID); err != nil || got.CreateState != CreateStateComplete {
				t.Fatalf("repo = %+v, %v; want complete", got, err)
			}
		})
	}
}
