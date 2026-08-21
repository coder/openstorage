package ref

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// testSHA returns a deterministic well-formed 40-hex sha for b.
func testSHA(b byte) string {
	s := strings.Repeat(fmt.Sprintf("%02x", b), 20)
	if !ValidSHA(s) {
		panic("testSHA produced malformed sha " + s)
	}
	return s
}

// openTestSQLite opens (and schema-inits) a fresh store under t.TempDir()
// and returns it plus its sqlite:// URL so tests can open second handles on
// the same file (simulating a re-exec'd hook process). Zero containers.
func openTestSQLite(t *testing.T) (*SQLite, string) {
	t.Helper()
	ctx := context.Background()
	dbURL := "sqlite://" + filepath.Join(t.TempDir(), "refs.sqlite")
	s, err := OpenSQLite(ctx, dbURL)
	if err != nil {
		t.Fatalf("OpenSQLite(%q): %v", dbURL, err)
	}
	t.Cleanup(s.Close)
	if err := s.InitSchema(ctx); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	return s, dbURL
}

// TestOpenSQLiteURLForms covers URL parsing: the scheme prefix is required,
// and missing parent directories are created (data-dir bootstrap).
func TestOpenSQLiteURLForms(t *testing.T) {
	ctx := context.Background()

	for _, bad := range []string{"postgres://h/db", "/no/scheme/at/all", "sqlite://"} {
		if _, err := OpenSQLite(ctx, bad); err == nil {
			t.Fatalf("OpenSQLite(%q) must fail", bad)
		}
	}

	nested := "sqlite://" + filepath.Join(t.TempDir(), "a", "b", "c", "db.sqlite")
	s, err := OpenSQLite(ctx, nested)
	if err != nil {
		t.Fatalf("OpenSQLite must MkdirAll missing parents: %v", err)
	}
	defer s.Close()
	if err := s.InitSchema(ctx); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
}

// TestSQLitePragmas proves the DSN pragmas actually took effect on live
// connections: WAL journaling and the 5s busy handler are the concurrency
// story for serve + hook processes sharing the file.
func TestSQLitePragmas(t *testing.T) {
	s, _ := openTestSQLite(t)

	var mode string
	if err := s.db.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil {
		t.Fatalf("PRAGMA journal_mode: %v", err)
	}
	if mode != "wal" {
		t.Fatalf("journal_mode = %q, want wal", mode)
	}
	var timeout int
	if err := s.db.QueryRow(`PRAGMA busy_timeout`).Scan(&timeout); err != nil {
		t.Fatalf("PRAGMA busy_timeout: %v", err)
	}
	if timeout != 5000 {
		t.Fatalf("busy_timeout = %d, want 5000", timeout)
	}
}

// TestSQLiteInitSchemaIdempotent covers repeated + concurrent InitSchema
// from two handles on one file (serve restart / racing startups).
func TestSQLiteInitSchemaIdempotent(t *testing.T) {
	ctx := context.Background()
	s, dbURL := openTestSQLite(t)

	s2, err := OpenSQLite(ctx, dbURL)
	if err != nil {
		t.Fatalf("second OpenSQLite: %v", err)
	}
	defer s2.Close()

	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for _, h := range []*SQLite{s, s2, s, s2} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- h.InitSchema(ctx)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent InitSchema: %v", err)
		}
	}

	// The sweeper's HasPack point read depends on this index (issue #5);
	// losing it silently degrades sweeps to O(N²) repo scans.
	var n int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'packs_repo_s3_key'`).Scan(&n); err != nil {
		t.Fatalf("query sqlite_master: %v", err)
	}
	if n != 1 {
		t.Fatalf("packs_repo_s3_key index missing after InitSchema")
	}
}

// TestSQLiteGetRef pins the point-read contract: the current sha for a
// present ref, ErrNotFound for an absent one.
func TestSQLiteGetRef(t *testing.T) {
	ctx := context.Background()
	s, _ := openTestSQLite(t)
	const repo, name = "org/repo", "refs/heads/main"
	a := testSHA(0xaa)

	if _, err := s.GetRef(ctx, repo, name); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetRef(absent) = %v, want ErrNotFound", err)
	}
	if won, err := s.CASRef(ctx, repo, name, ZeroSHA, a); err != nil || !won {
		t.Fatalf("create CASRef = (%v, %v)", won, err)
	}
	if sha, err := s.GetRef(ctx, repo, name); err != nil || sha != a {
		t.Fatalf("GetRef = (%q, %v), want (%q, nil)", sha, err, a)
	}
	if _, err := s.GetRef(ctx, "other/repo", name); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetRef(other repo) = %v, want ErrNotFound (no cross-repo leak)", err)
	}
}

// TestSQLiteCASRefMatrix is the CAS contract matrix: create vs existing,
// update/delete with wrong old sha, won/lost strictly from rows affected.
func TestSQLiteCASRefMatrix(t *testing.T) {
	ctx := context.Background()
	s, _ := openTestSQLite(t)
	const repo, name = "org/repo", "refs/heads/main"
	a, b, c := testSHA(0xaa), testSHA(0xbb), testSHA(0xcc)

	mustCAS := func(old, new string, wantWon bool, step string) {
		t.Helper()
		won, err := s.CASRef(ctx, repo, name, old, new)
		if err != nil {
			t.Fatalf("%s: %v", step, err)
		}
		if won != wantWon {
			t.Fatalf("%s: won = %v, want %v", step, won, wantWon)
		}
	}
	mustRef := func(want string, step string) {
		t.Helper()
		refs, err := s.ListRefs(ctx, repo)
		if err != nil {
			t.Fatalf("%s: ListRefs: %v", step, err)
		}
		got, present := refs[name]
		if want == "" && present {
			t.Fatalf("%s: ref must be absent, got %q", step, got)
		}
		if want != "" && got != want {
			t.Fatalf("%s: ref = %q, want %q", step, got, want)
		}
	}

	mustCAS(ZeroSHA, a, true, "create new")
	mustRef(a, "after create")
	mustCAS(ZeroSHA, b, false, "create existing loses")
	mustRef(a, "after lost create")
	mustCAS(c, b, false, "update with wrong old loses")
	mustRef(a, "after lost update")
	mustCAS(a, b, true, "update with correct old")
	mustRef(b, "after update")
	mustCAS(a, ZeroSHA, false, "delete with wrong old loses")
	mustRef(b, "after lost delete")
	mustCAS(b, ZeroSHA, true, "delete with correct old")
	mustRef("", "after delete")
	mustCAS(b, ZeroSHA, false, "delete of absent ref loses")
}

// TestSQLiteListRefsIsolation: repos don't leak refs into each other, and an
// unknown repo yields an empty (non-nil) map.
func TestSQLiteListRefsIsolation(t *testing.T) {
	ctx := context.Background()
	s, _ := openTestSQLite(t)
	a, b := testSHA(0x0a), testSHA(0x0b)

	for repo, sha := range map[string]string{"org/one": a, "org/two": b} {
		if won, err := s.CASRef(ctx, repo, "refs/heads/main", ZeroSHA, sha); err != nil || !won {
			t.Fatalf("seed %s: won=%v err=%v", repo, won, err)
		}
	}
	refs, err := s.ListRefs(ctx, "org/one")
	if err != nil {
		t.Fatalf("ListRefs: %v", err)
	}
	if len(refs) != 1 || refs["refs/heads/main"] != a {
		t.Fatalf("org/one refs = %v, want only main=%s", refs, a)
	}
	empty, err := s.ListRefs(ctx, "org/absent")
	if err != nil {
		t.Fatalf("ListRefs absent repo: %v", err)
	}
	if empty == nil || len(empty) != 0 {
		t.Fatalf("absent repo must list zero refs, got %v", empty)
	}
}

// TestSQLiteRegisterPacksIdempotent: re-registering the same packs (push
// retry) is a no-op, and pack listings are per-repo.
func TestSQLiteRegisterPacksIdempotent(t *testing.T) {
	ctx := context.Background()
	s, _ := openTestSQLite(t)
	const repo = "org/repo"
	packs := []Pack{{ID: "p1", Key: "org/repo/p1.pack"}, {ID: "p2", Key: "org/repo/p2.pack"}}

	for i := 0; i < 2; i++ {
		if err := s.RegisterPacks(ctx, repo, packs); err != nil {
			t.Fatalf("RegisterPacks round %d: %v", i, err)
		}
	}
	got, err := s.ListPacks(ctx, repo)
	if err != nil {
		t.Fatalf("ListPacks: %v", err)
	}
	if len(got) != 2 || got[0] != packs[0] || got[1] != packs[1] {
		t.Fatalf("ListPacks = %+v, want %+v exactly once each", got, packs)
	}
	if other, err := s.ListPacks(ctx, "org/other"); err != nil || len(other) != 0 {
		t.Fatalf("other repo packs = %+v err=%v, want none", other, err)
	}
}

// TestSQLiteListPacksOrdering: oldest first by created_at, id as tie-break
// within one clock second. An explicitly backdated row (raw SQL — the test
// owns the schema) must sort ahead of newer rows regardless of id.
func TestSQLiteListPacksOrdering(t *testing.T) {
	ctx := context.Background()
	s, _ := openTestSQLite(t)
	const repo = "org/repo"

	// Registered out of lexicographic order in the same second → id tie-break.
	if err := s.RegisterPacks(ctx, repo, []Pack{
		{ID: "b", Key: "k-b"}, {ID: "a", Key: "k-a"}, {ID: "c", Key: "k-c"},
	}); err != nil {
		t.Fatalf("RegisterPacks: %v", err)
	}
	// Backdate "z" so created_at dominates the id tie-break.
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO packs (repo, id, s3_key, created_at) VALUES (?, 'z', 'k-z', unixepoch() - 100)`,
		repo); err != nil {
		t.Fatalf("backdate insert: %v", err)
	}

	got, err := s.ListPacks(ctx, repo)
	if err != nil {
		t.Fatalf("ListPacks: %v", err)
	}
	var ids []string
	for _, p := range got {
		ids = append(ids, p.ID)
	}
	if want := "z a b c"; strings.Join(ids, " ") != want {
		t.Fatalf("pack order = %v, want %q", ids, want)
	}
}

// TestSQLiteConcurrentCAS races parallel goroutines split across TWO store
// handles on one DB file — the serve process + re-exec'd hook process shape.
// Exactly one create and exactly one update may win; busy_timeout must
// absorb the write contention (any SQLITE_BUSY escaping is a failure).
func TestSQLiteConcurrentCAS(t *testing.T) {
	ctx := context.Background()
	s1, dbURL := openTestSQLite(t)
	s2, err := OpenSQLite(ctx, dbURL)
	if err != nil {
		t.Fatalf("second OpenSQLite: %v", err)
	}
	defer s2.Close()
	const repo, name = "org/repo", "refs/heads/main"
	handles := []*SQLite{s1, s2}

	// base keeps each race's candidate shas disjoint from `old`: a candidate
	// equal to old would make UPDATE ... WHERE sha=old a matching no-op
	// self-update and count as a second winner.
	race := func(old string, base byte, step string) string {
		t.Helper()
		const workers = 8
		var wg sync.WaitGroup
		winners := make(chan string, workers)
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				sha := testSHA(base + byte(i))
				won, err := handles[i%len(handles)].CASRef(ctx, repo, name, old, sha)
				if err != nil {
					t.Errorf("%s worker %d: %v", step, i, err)
					return
				}
				if won {
					winners <- sha
				}
			}(i)
		}
		wg.Wait()
		close(winners)
		var won []string
		for sha := range winners {
			won = append(won, sha)
		}
		if len(won) != 1 {
			t.Fatalf("%s: %d winners %v, want exactly 1", step, len(won), won)
		}
		return won[0]
	}

	created := race(ZeroSHA, 0x10, "create race")
	updated := race(created, 0x20, "update race")

	// The second handle must observe the first handle's final state (WAL).
	refs, err := s2.ListRefs(ctx, repo)
	if err != nil {
		t.Fatalf("ListRefs via second handle: %v", err)
	}
	if refs[name] != updated {
		t.Fatalf("second handle sees %q, want update winner %q", refs[name], updated)
	}
}

// TestSQLiteRefCreatedAt pins the refs.created_at contract: set on create,
// preserved across SHA updates, reset by delete + recreate, and surfaced by
// ListRefsWithMeta sorted by name.
func TestSQLiteRefCreatedAt(t *testing.T) {
	ctx := context.Background()
	s, _ := openTestSQLite(t)
	const repo = "org/repo"
	a, b := testSHA(0xaa), testSHA(0xbb)

	mustCAS := func(name, old, new string) {
		t.Helper()
		won, err := s.CASRef(ctx, repo, name, old, new)
		if err != nil || !won {
			t.Fatalf("CASRef(%s, %.8s -> %.8s) = won=%v err=%v", name, old, new, won, err)
		}
	}

	mustCAS("refs/heads/zeta", ZeroSHA, a)
	mustCAS("refs/heads/alpha", ZeroSHA, a)

	refs, err := s.ListRefsWithMeta(ctx, repo)
	if err != nil {
		t.Fatalf("ListRefsWithMeta: %v", err)
	}
	if len(refs) != 2 || refs[0].Name != "refs/heads/alpha" || refs[1].Name != "refs/heads/zeta" {
		t.Fatalf("want [alpha zeta] sorted by name, got %+v", refs)
	}
	for _, r := range refs {
		if r.SHA != a {
			t.Fatalf("%s sha = %q, want %q", r.Name, r.SHA, a)
		}
		if r.CreatedAt.IsZero() || time.Since(r.CreatedAt) > time.Minute {
			t.Fatalf("%s created_at = %v, want ~now", r.Name, r.CreatedAt)
		}
	}
	created := refs[1].CreatedAt

	// SHA update preserves the creation time.
	mustCAS("refs/heads/zeta", a, b)
	refs, err = s.ListRefsWithMeta(ctx, repo)
	if err != nil {
		t.Fatalf("ListRefsWithMeta after update: %v", err)
	}
	if refs[1].SHA != b {
		t.Fatalf("zeta sha = %q, want %q after update", refs[1].SHA, b)
	}
	if !refs[1].CreatedAt.Equal(created) {
		t.Fatalf("created_at changed on SHA update: %v != %v", refs[1].CreatedAt, created)
	}

	// Delete + recreate resets it (a new ref row is a new creation). The
	// second-granularity clock may not tick, so only assert non-zero.
	mustCAS("refs/heads/zeta", b, ZeroSHA)
	mustCAS("refs/heads/zeta", ZeroSHA, a)
	refs, err = s.ListRefsWithMeta(ctx, repo)
	if err != nil {
		t.Fatalf("ListRefsWithMeta after recreate: %v", err)
	}
	if len(refs) != 2 || refs[1].CreatedAt.IsZero() {
		t.Fatalf("recreated ref missing/zero created_at: %+v", refs)
	}
}

// TestSQLiteInitSchemaAddsCreatedAt covers the additive migration: a pre-p3
// refs table (no created_at) gains the column and a backfill on InitSchema.
func TestSQLiteInitSchemaAddsCreatedAt(t *testing.T) {
	ctx := context.Background()
	dbURL := "sqlite://" + filepath.Join(t.TempDir(), "old.sqlite")
	s, err := OpenSQLite(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Old-shape schema with one existing row.
	if _, err := s.db.ExecContext(ctx,
		`CREATE TABLE refs (repo TEXT, name TEXT, sha TEXT NOT NULL, PRIMARY KEY (repo, name))`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO refs (repo, name, sha) VALUES ('r', 'refs/heads/main', ?)`, testSHA(0xaa)); err != nil {
		t.Fatal(err)
	}

	if err := s.InitSchema(ctx); err != nil {
		t.Fatalf("InitSchema on old-shape DB: %v", err)
	}
	refs, err := s.ListRefsWithMeta(ctx, "r")
	if err != nil {
		t.Fatalf("ListRefsWithMeta after migration: %v", err)
	}
	if len(refs) != 1 || refs[0].CreatedAt.IsZero() {
		t.Fatalf("migration must backfill created_at, got %+v", refs)
	}
	// Idempotent: running it again must not fail or duplicate the column.
	if err := s.InitSchema(ctx); err != nil {
		t.Fatalf("second InitSchema: %v", err)
	}
}
