// Concurrent packs_repo_s3_key build tests (issue #24): InitSchema builds
// the index out-of-transaction with CREATE INDEX CONCURRENTLY, and repairs
// the INVALID leftover a crashed/failed concurrent build leaves behind.
// PG-only — the SQLite backend keeps its plain transactional index (single
// writer, no build-vs-writer stall to avoid). The shared-container boot in
// pgTestStore already runs four InitSchema calls concurrently, so the
// fresh-database build races through the session-lock poll path on every
// package run.
package ref

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// mustPacksIndexState reads the index state through the store's own pool.
func mustPacksIndexState(t *testing.T, s *PG) (exists, valid bool) {
	t.Helper()
	exists, valid, err := packsIndexState(context.Background(), s.pool)
	if err != nil {
		t.Fatalf("packs index state: %v", err)
	}
	return exists, valid
}

func TestPGInitSchema_RepairsInvalidPacksIndex(t *testing.T) {
	s := pgTestStore(t)
	ctx := context.Background()

	if exists, valid := mustPacksIndexState(t, s); !exists || !valid {
		t.Fatalf("after boot: packs index exists=%v valid=%v, want a valid index", exists, valid)
	}

	// Simulate the leftover of a concurrent build that died mid-flight: a
	// real crashed CREATE INDEX CONCURRENTLY leaves exactly this catalog
	// state (entry present, indisvalid = false). Flipping the catalog flag
	// as superuser produces it deterministically without racing a real
	// build against an injected crash.
	if _, err := s.pool.Exec(ctx,
		`UPDATE pg_index SET indisvalid = false WHERE indexrelid = to_regclass('packs_repo_s3_key')`); err != nil {
		t.Fatalf("inject invalid index: %v", err)
	}
	if exists, valid := mustPacksIndexState(t, s); !exists || valid {
		t.Fatalf("injection failed: exists=%v valid=%v, want an existing INVALID index", exists, valid)
	}

	// Concurrent replica boots against the poisoned catalog: every
	// InitSchema must succeed, and they must converge on ONE valid index
	// (the session lock serializes the drop+rebuild; waiters observe the
	// repaired state). CREATE INDEX CONCURRENTLY IF NOT EXISTS alone would
	// silently skip the INVALID entry — this pins the detect-drop-rebuild
	// path.
	errs := make(chan error, 4)
	for range 4 {
		go func() { errs <- s.InitSchema(ctx) }()
	}
	for range 4 {
		if err := <-errs; err != nil {
			t.Fatalf("InitSchema over invalid index: %v", err)
		}
	}
	if exists, valid := mustPacksIndexState(t, s); !exists || !valid {
		t.Fatalf("after repair: exists=%v valid=%v, want a valid index", exists, valid)
	}

	// A dropped index (no catalog entry at all) is rebuilt the same way.
	if _, err := s.pool.Exec(ctx, `DROP INDEX packs_repo_s3_key`); err != nil {
		t.Fatalf("drop index: %v", err)
	}
	if err := s.InitSchema(ctx); err != nil {
		t.Fatalf("InitSchema after drop: %v", err)
	}
	if exists, valid := mustPacksIndexState(t, s); !exists || !valid {
		t.Fatalf("after rebuild: exists=%v valid=%v, want a valid index", exists, valid)
	}

	// The rebuilt index must actually be the sweeper's (repo, s3_key)
	// membership probe, not just a catalog row: exercise HasPack through it.
	repo := t.Name()
	key := fmt.Sprintf("packs/%s/pack-1.pack", repo)
	if err := s.RegisterPacks(ctx, repo, []Pack{{ID: "1", Key: key}}); err != nil {
		t.Fatalf("register pack: %v", err)
	}
	ok, err := s.HasPack(ctx, repo, key)
	if err != nil || !ok {
		t.Fatalf("HasPack through rebuilt index = %v, %v; want true", ok, err)
	}
}

// TestPGInitSchema_SingleConnPoolBuildsIndex pins the Codex P1 on #43: the
// build/repair path holds a dedicated conn for the SPIDX session lock, and
// with pool_max_conns=1 that conn is the pool's only slot — the
// transactional DDL must run on the held conn, or InitSchema waits forever
// for a second slot on a non-expiring boot context.
func TestPGInitSchema_SingleConnPoolBuildsIndex(t *testing.T) {
	s := pgTestStore(t)
	ctx := context.Background()

	// Poison the index so the single-conn boot takes the lock-holding
	// build path, not the steady-state fast path.
	if _, err := s.pool.Exec(ctx,
		`UPDATE pg_index SET indisvalid = false WHERE indexrelid = to_regclass('packs_repo_s3_key')`); err != nil {
		t.Fatalf("inject invalid index: %v", err)
	}

	one, err := OpenPG(ctx, pgTestURL+"&pool_max_conns=1")
	if err != nil {
		t.Fatalf("open single-conn store: %v", err)
	}
	defer one.Close()
	done := make(chan error, 1)
	go func() { done <- one.InitSchema(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("single-conn InitSchema: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("single-conn InitSchema deadlocked: transactional DDL must run on the held lock conn")
	}
	if exists, valid := mustPacksIndexState(t, s); !exists || !valid {
		t.Fatalf("after single-conn repair: exists=%v valid=%v, want a valid index", exists, valid)
	}
}
