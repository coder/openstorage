package sync

import (
	"context"
	"path/filepath"
	stdsync "sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/meta"
)

// TestLocalSchedulerCompleteSyncCreate: on SQLite the first-sync request
// commits with the complete mark (issue #388). Concurrent completions
// record one request and offer one run.
func TestLocalSchedulerCompleteSyncCreate(t *testing.T) {
	ctx := t.Context()
	ms, err := meta.OpenSQLite(ctx, "sqlite://"+filepath.Join(t.TempDir(), "meta.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ms.Close)
	if err := ms.InitSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.UpsertOrgWithKeys(ctx, "acme", []string{"-----BEGIN PUBLIC KEY-----\ndummy\n-----END PUBLIC KEY-----\n"}); err != nil {
		t.Fatal(err)
	}
	lanes, ok := MetaLocalLanes(ms)
	if !ok {
		t.Fatal("SQLite store does not provide local lanes")
	}
	runs := make(chan Job, 16)
	sched := NewLocalScheduler(lanes, "acme", func(_ context.Context, j Job) error { runs <- j; return nil },
		func(context.Context, Job) error { return nil }, discardTestLogger())
	t.Cleanup(sched.Close)
	r, err := ms.CreateLinkedRepo(ctx, "acme", "race", "main",
		meta.RepoUpstream{Provider: "github", Owner: "o", Name: "n", UpstreamHost: "github.com"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	var completed atomic.Int32 // 8 concurrent completions: one wins
	var wg stdsync.WaitGroup
	for range 8 {
		wg.Go(func() {
			out, err := sched.CompleteSyncCreate(ctx, "acme", r.RepoID)
			if err != nil || (out != meta.CreateCompleted && out != meta.CreateAlreadyComplete) {
				t.Errorf("CompleteSyncCreate = %v, %v", out, err)
			} else if out == meta.CreateCompleted {
				completed.Add(1)
			}
		})
	}
	wg.Wait()
	if got := completed.Load(); got != 1 {
		t.Fatalf("%d completions won, want 1", got)
	}
	select {
	case j := <-runs:
		if j.RepoID != r.RepoID || j.Ref != "" {
			t.Fatalf("offered run = %+v, want repo %s with no ref", j, r.RepoID)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("no first-sync run offered")
	}
	if up, err := ms.GetRepoUpstream(ctx, r.RepoID); err != nil || !up.SyncPending {
		t.Fatalf("upstream = %+v, %v; want the durable first-sync request", up, err)
	}
}
