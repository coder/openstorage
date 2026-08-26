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
	"strings"
	"testing"
	"time"
)

// mustIndexState reads one managed index's state through the store's pool.
func mustIndexState(t *testing.T, s *PG, name string) (exists, valid bool) {
	t.Helper()
	exists, valid, err := indexState(context.Background(), s.pool, name)
	if err != nil {
		t.Fatalf("%s index state: %v", name, err)
	}
	return exists, valid
}

// mustPacksIndexState reads the packs index state through the store's pool.
func mustPacksIndexState(t *testing.T, s *PG) (exists, valid bool) {
	t.Helper()
	return mustIndexState(t, s, "packs_repo_s3_key")
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

// TestPGInitSchema_RepairsInvalidRefsNameIndex covers the second managed
// index (refs_repo_name_c, issue #119) through the same detect-drop-rebuild
// path, and pins the property the index exists for: ListRefsAfter's keyset
// page must be an index range scan in byte order, never a sort of the
// repo's whole remaining ref set (attacker-scaled fanout would turn paging
// quadratic).
func TestPGInitSchema_RepairsInvalidRefsNameIndex(t *testing.T) {
	s := pgTestStore(t)
	ctx := context.Background()

	if exists, valid := mustIndexState(t, s, "refs_repo_name_c"); !exists || !valid {
		t.Fatalf("after boot: refs name index exists=%v valid=%v, want a valid index", exists, valid)
	}

	if _, err := s.pool.Exec(ctx,
		`UPDATE pg_index SET indisvalid = false WHERE indexrelid = to_regclass('refs_repo_name_c')`); err != nil {
		t.Fatalf("inject invalid index: %v", err)
	}
	if err := s.InitSchema(ctx); err != nil {
		t.Fatalf("InitSchema over invalid refs index: %v", err)
	}
	if exists, valid := mustIndexState(t, s, "refs_repo_name_c"); !exists || !valid {
		t.Fatalf("after repair: exists=%v valid=%v, want a valid index", exists, valid)
	}

	// The planner must serve the COLLATE "C" keyset page from the index.
	// EXPLAIN with generic-plan-defeating literals mirrors production
	// parameterized queries closely enough to catch a collation mismatch
	// (a default-collation PK cannot serve this ORDER BY).
	rows, err := s.pool.Query(ctx, `EXPLAIN SELECT name, sha FROM refs
		WHERE repo = 'r' AND name COLLATE "C" > 'refs/heads/a'
		ORDER BY name COLLATE "C" LIMIT 100`)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	defer rows.Close()
	var plan string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan plan: %v", err)
		}
		plan += line + "\n"
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("plan rows: %v", err)
	}
	if !strings.Contains(plan, "refs_repo_name_c") {
		t.Fatalf("ListRefsAfter page not served by refs_repo_name_c:\n%s", plan)
	}
	if strings.Contains(plan, "Sort") {
		t.Fatalf("ListRefsAfter page plans a sort (index should stream byte order):\n%s", plan)
	}
}

// TestPGInitSchema_RepairsInvalidScanStateScopeIndex covers the managed
// job_scan_state scope index (job_scan_state_job_scope_c, issues #120/#121)
// through the same detect-drop-rebuild path, and pins the property the
// index exists for: the byte-ordered scope keyset page behind
// ListJobScanStateScopes and SettleLFSRefunds' bucket paging must be an
// index range scan, never a sort of the job's whole remaining scope set
// (refund buckets grow per base and sweep epoch, so on a non-C default
// collation the (job, scope) primary key cannot serve the page and every
// settlement pass would re-sort everything it has not yet consumed).
func TestPGInitSchema_RepairsInvalidScanStateScopeIndex(t *testing.T) {
	s := pgTestStore(t)
	ctx := context.Background()

	if exists, valid := mustIndexState(t, s, "job_scan_state_job_scope_c"); !exists || !valid {
		t.Fatalf("after boot: scan-state scope index exists=%v valid=%v, want a valid index", exists, valid)
	}

	if _, err := s.pool.Exec(ctx,
		`UPDATE pg_index SET indisvalid = false WHERE indexrelid = to_regclass('job_scan_state_job_scope_c')`); err != nil {
		t.Fatalf("inject invalid index: %v", err)
	}
	if err := s.InitSchema(ctx); err != nil {
		t.Fatalf("InitSchema over invalid scan-state index: %v", err)
	}
	if exists, valid := mustIndexState(t, s, "job_scan_state_job_scope_c"); !exists || !valid {
		t.Fatalf("after repair: exists=%v valid=%v, want a valid index", exists, valid)
	}

	// The planner must serve the COLLATE "C" keyset page from the index.
	// EXPLAIN with generic-plan-defeating literals mirrors the production
	// parameterized queries (ListJobScanStateScopes, SettleLFSRefunds)
	// closely enough to catch a collation mismatch (a default-collation
	// PK cannot serve this ORDER BY). The prefix successor is an
	// exclusive-upper INDEX bound (Codex round 38): a filter-shaped
	// prefix check has no upper bound, so proving a short final page
	// empty scanned every later scope of the job — the no-Filter
	// assertion below pins the whole predicate into the index range.
	rows, err := s.pool.Query(ctx, `EXPLAIN SELECT scope FROM job_scan_state
		WHERE job = 'lfs_reclaim' AND scope COLLATE "C" > 'refund/'
		AND scope COLLATE "C" < 'refund0'
		ORDER BY scope COLLATE "C" ASC LIMIT 100`)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	defer rows.Close()
	var plan string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan plan: %v", err)
		}
		plan += line + "\n"
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("plan rows: %v", err)
	}
	if !strings.Contains(plan, "job_scan_state_job_scope_c") {
		t.Fatalf("scan-state scope page not served by job_scan_state_job_scope_c:\n%s", plan)
	}
	if strings.Contains(plan, "Sort") {
		t.Fatalf("scan-state scope page plans a sort (index should stream byte order):\n%s", plan)
	}
	if strings.Contains(plan, "Filter") {
		t.Fatalf("scan-state scope page plans a filter (the whole predicate must be index range bounds):\n%s", plan)
	}
}
