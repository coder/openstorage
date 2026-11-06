package jobs

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/events"
	"github.com/coder/openstorage/internal/lfsscan"
	"github.com/coder/openstorage/internal/sync"
)

type listCountingBlobs struct {
	blob.BlobStore
	lists atomic.Int32
}

func (b *listCountingBlobs) ListPages(ctx context.Context, prefix string, pageSize int, fn func([]blob.ListedObject) error) error {
	b.lists.Add(1)
	return b.BlobStore.ListPages(ctx, prefix, pageSize, fn)
}

type scanCountingRegistry struct {
	LFSReclaimRegistry
	scans atomic.Int32
}

func (r *scanCountingRegistry) LFSReposWithObjects(ctx context.Context, after string, limit int) ([]string, error) {
	r.scans.Add(1)
	return r.LFSReclaimRegistry.LFSReposWithObjects(ctx, after, limit)
}

// A sweep or LFS reclaim job waits for a replica whose schedule for it is on.
// On the default queue, a replica with the schedule off fetched the job and
// River discarded it, because MaxAttempts is 1: that tick was lost.
func TestRiverPassJobsWaitForAReplicaWithTheScheduleOn(t *testing.T) {
	ctx := context.Background()
	pgURL := livenessPG(t, ctx)
	deliverer := &events.Deliverer{Webhooks: stubWebhooks{}, Logger: slog.New(slog.DiscardHandler)}
	noop := func(context.Context, sync.Job) error { return nil }
	logger := slog.New(slog.DiscardHandler)

	off, err := NewRiver(ctx, pgURL, deliverer, noop, noop, sync.NewMemStore(), nil, nil, nil, nil, logger)
	if err != nil {
		t.Fatalf("NewRiver with the schedules off: %v", err)
	}
	defer off.Close()
	ins, err := NewRiverInsertOnly(ctx, pgURL)
	if err != nil {
		t.Fatalf("NewRiverInsertOnly: %v", err)
	}
	defer ins.Close()

	insert := func(args river.JobArgs) int64 {
		t.Helper()
		res, err := ins.client.Insert(ctx, args, nil)
		if err != nil {
			t.Fatalf("insert %s: %v", args.Kind(), err)
		}
		return res.Job.ID
	}
	jobs := map[string]int64{"sweep": insert(sweepArgs{}), "LFS reclaim": insert(lfsReclaimArgs{})}
	jobState := func(id int64) string {
		t.Helper()
		var state string
		if err := ins.pool.QueryRow(ctx, `SELECT state FROM river_job WHERE id = $1`, id).Scan(&state); err != nil {
			t.Fatalf("read job %d: %v", id, err)
		}
		return state
	}

	// River fetches a new job within about a second, so a replica that
	// fetched these two would have discarded them well before this ends.
	for until := time.Now().Add(4 * time.Second); time.Now().Before(until); time.Sleep(100 * time.Millisecond) {
		for what, id := range jobs {
			if state := jobState(id); state != "available" {
				t.Fatalf("%s job is %s after a replica with its schedule off started, want it left available", what, state)
			}
		}
	}

	f := newSweepFixture(t)
	blobs := &listCountingBlobs{BlobStore: f.blobs}
	_, reg := newReclaimStore(t)
	scans := &scanCountingRegistry{LFSReclaimRegistry: reg}
	on, err := NewRiver(ctx, pgURL, deliverer, noop, noop, sync.NewMemStore(),
		&SweepSchedule{Sweeper: NewSweeper(blobs, f.store, SweepConfig{Threshold: 24 * time.Hour}, logger), Interval: time.Hour},
		&LFSReclaimSchedule{Reclaimer: NewLFSReclaimer(scans, staticHydrate(t, nil), lfsscan.ReachableLFSOIDs, alwaysMain,
			time.Hour, 1000, false, false, logger), Interval: time.Hour},
		nil, nil, logger)
	if err != nil {
		t.Fatalf("NewRiver with the schedules on: %v", err)
	}
	defer on.Close()

	waitFor(t, "the replica with the schedules on to run both jobs", func() bool {
		return jobState(jobs["sweep"]) == "completed" && jobState(jobs["LFS reclaim"]) == "completed"
	})
	if blobs.lists.Load() == 0 || scans.scans.Load() == 0 {
		t.Errorf("both jobs completed, but the sweep listed %d times and the reclaim scanned %d times", blobs.lists.Load(), scans.scans.Load())
	}
}

// A rollback can strand an unfetched sweep or LFS reclaim job in its own
// queue, where no older replica fetches it. It must not block the older
// fleet's inserts, which carry a unique key without the queue.
func TestRiverPassJobsDoNotBlockOlderDefaultQueueInserts(t *testing.T) {
	ctx := context.Background()
	pgURL := livenessPG(t, ctx)
	migrateRiver(t, pgURL)
	ins, err := NewRiverInsertOnly(ctx, pgURL)
	if err != nil {
		t.Fatalf("NewRiverInsertOnly: %v", err)
	}
	defer ins.Close()

	older := &river.InsertOpts{
		Queue:       river.QueueDefault,
		MaxAttempts: 1,
		UniqueOpts: river.UniqueOpts{
			ByArgs: true,
			ByState: []rivertype.JobState{
				rivertype.JobStateAvailable,
				rivertype.JobStatePending,
				rivertype.JobStateRetryable,
				rivertype.JobStateRunning,
				rivertype.JobStateScheduled,
			},
		},
	}
	for _, args := range []river.JobArgs{sweepArgs{}, lfsReclaimArgs{}} {
		stranded, err := ins.client.Insert(ctx, args, nil)
		if err != nil || stranded.UniqueSkippedAsDuplicate {
			t.Fatalf("insert %s: skipped=%v err=%v", args.Kind(), stranded.UniqueSkippedAsDuplicate, err)
		}
		if res, err := ins.client.Insert(ctx, args, older); err != nil || res.UniqueSkippedAsDuplicate {
			t.Errorf("%s: an older binary's insert was deduplicated against a job stranded in its own queue (skipped=%v err=%v)", args.Kind(), res.UniqueSkippedAsDuplicate, err)
		}
		if res, err := ins.client.Insert(ctx, args, nil); err != nil || !res.UniqueSkippedAsDuplicate {
			t.Errorf("%s: a second insert in the job's own queue was not deduplicated (skipped=%v err=%v)", args.Kind(), res.UniqueSkippedAsDuplicate, err)
		}
	}
}
