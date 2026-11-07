package main

// Container-free tests for the purge wiring of serve (issue #52): the sweep
// config carries the fence probe whatever the retention window, and the
// schedule runs the retention purge exactly when a window is set.

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/jobs"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref"
)

func openWiringStores(t *testing.T) (ref.Store, meta.Store, jobs.SweepConfig) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	url := "sqlite://" + filepath.Join(dir, "codestorage.db")
	ms, err := meta.Open(ctx, url)
	if err != nil {
		t.Fatalf("open meta store: %v", err)
	}
	t.Cleanup(ms.Close)
	if err := ms.InitSchema(ctx); err != nil {
		t.Fatalf("meta InitSchema: %v", err)
	}
	rs, err := ref.Open(ctx, url)
	if err != nil {
		t.Fatalf("open ref store: %v", err)
	}
	t.Cleanup(rs.Close)
	if err := rs.InitSchema(ctx); err != nil {
		t.Fatalf("ref InitSchema: %v", err)
	}
	return rs, ms, jobs.SweepConfig{Threshold: time.Hour}
}

// TestWirePurge_SweepProbesTheFence: the sweeper reaps straggler blobs of a
// purge-fenced repository only through SweepConfig.PurgedRepoProbe. Without
// it, bytes uploaded after a purge's own scan stay stored for good.
func TestWirePurge_SweepProbesTheFence(t *testing.T) {
	ctx := context.Background()
	rs, ms, cfg := openWiringStores(t)
	reg, pm := wirePurge(rs, ms, &cfg)
	if reg == nil || pm == nil {
		t.Fatalf("wirePurge returned the seams (%v, %v)", reg, pm)
	}
	if cfg.PurgedRepoProbe == nil {
		t.Fatal("wirePurge left SweepConfig.PurgedRepoProbe unset")
	}
	if purged, err := cfg.PurgedRepoProbe(ctx, "wired-repo"); err != nil || purged {
		t.Fatalf("probe before the fence = (%v, %v), want (false, <nil>)", purged, err)
	}
	if err := reg.FenceRepoPurge(ctx, "wired-repo"); err != nil {
		t.Fatalf("FenceRepoPurge: %v", err)
	}
	if purged, err := cfg.PurgedRepoProbe(ctx, "wired-repo"); err != nil || !purged {
		t.Fatalf("probe after the fence = (%v, %v), want (true, <nil>)", purged, err)
	}
	if purged, err := cfg.PurgedRepoProbe(ctx, "other-repo"); err != nil || purged {
		t.Fatalf("probe of an unfenced repository = (%v, %v), want (false, <nil>)", purged, err)
	}
}

// TestNewSweepSchedule_PurgerFollowsRetention: OPENSTORAGE_REPO_RETENTION
// turns the retention purge on, and only it does.
func TestNewSweepSchedule_PurgerFollowsRetention(t *testing.T) {
	rs, ms, cfg := openWiringStores(t)
	reg, pm := wirePurge(rs, ms, &cfg)
	bl, err := blob.OpenFile("file://" + filepath.Join(t.TempDir(), "blobs"))
	if err != nil {
		t.Fatalf("open blob store: %v", err)
	}
	t.Cleanup(func() { _ = bl.Close() })
	logger := slog.New(slog.DiscardHandler)
	sweeper := jobs.NewSweeper(bl, rs, cfg, logger)

	without := newSweepSchedule(sweeper, time.Hour, 0, reg, pm, logger)
	if without.Purger != nil {
		t.Fatal("a schedule without a retention window runs the retention purge")
	}
	with := newSweepSchedule(sweeper, time.Hour, 24*time.Hour, reg, pm, logger)
	if with.Purger == nil {
		t.Fatal("a schedule with a retention window does not run the retention purge")
	}
	if with.Sweeper != sweeper || with.Interval != time.Hour {
		t.Fatalf("schedule = %+v, want the sweeper and the interval it was built with", with)
	}
}
