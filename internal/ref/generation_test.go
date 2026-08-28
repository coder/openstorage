// Generation-counter conformance tests (issue #4): every mutation path on
// the RefStore surface bumps the per-repo generation in the same atomic unit
// as the mutation, lost CAS races bump nothing, and GetGeneration is a
// point-read. Test bodies fan out over ALL backends via runRefBackends —
// the pg lane lazily boots one testcontainers-go Postgres and the s3wal
// lane one SeaweedFS for the whole package (sqlite-filtered runs boot zero
// containers) — and double as direct coverage for the CTE-based PG CASRef
// statements and the manifest-CAS driver (issue #133).
package ref

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/coder/code-storage/internal/ref/s3wal"
	"github.com/coder/code-storage/internal/ref/s3wal/s3waltest"
)

var (
	pgOnce    sync.Once
	pgErr     error
	pgStore   *PG
	pgTestURL string // container URL for tests needing their own pool options
	pgCleanup []func()
)

// pgTestStore lazily boots the package-wide Postgres container and store.
// Concurrent InitSchema runs here to exercise the advisory-lock
// serialization with the repo_generations DDL included.
func pgTestStore(t *testing.T) *PG {
	t.Helper()
	pgOnce.Do(func() {
		ctx := context.Background()

		pgC, err := tcpostgres.Run(ctx, "postgres:17-alpine",
			tcpostgres.WithDatabase("postgres"),
			tcpostgres.WithUsername("postgres"),
			tcpostgres.WithPassword("postgres"),
			tcpostgres.BasicWaitStrategies(),
		)
		if err != nil {
			pgErr = fmt.Errorf("start postgres container: %w", err)
			return
		}
		pgCleanup = append(pgCleanup, func() { _ = pgC.Terminate(ctx) })

		pgURL, err := pgC.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			pgErr = err
			return
		}
		pgTestURL = pgURL
		store, err := OpenPG(ctx, pgURL)
		if err != nil {
			pgErr = err
			return
		}
		pgCleanup = append(pgCleanup, store.Close)

		errs := make(chan error, 4)
		for range 4 {
			go func() { errs <- store.InitSchema(ctx) }()
		}
		for range 4 {
			if err := <-errs; err != nil {
				pgErr = fmt.Errorf("concurrent InitSchema: %w", err)
				return
			}
		}
		pgStore = store
	})
	if pgErr != nil {
		t.Fatalf("pg backend: %v", pgErr)
	}
	return pgStore
}

var (
	s3walOnce    sync.Once
	s3walErr     error
	s3walStore   *S3WAL
	s3walCleanup []func()
)

// s3walTestStore lazily boots the package-wide SeaweedFS container and the
// manifest-CAS store (issue #133), running InitSchema — the §2.6
// conditional-write probe — once at boot, exactly like the real serve
// path. Same Docker requirement as the pg lane: s3wal-filtered-out runs
// boot zero containers.
func s3walTestStore(t *testing.T) *S3WAL {
	t.Helper()
	s3walOnce.Do(func() {
		ctx := context.Background()

		endpoint, terminate, err := s3waltest.StartSeaweedFSContainer(ctx)
		if err != nil {
			s3walErr = fmt.Errorf("start seaweedfs container: %w", err)
			return
		}
		s3walCleanup = append(s3walCleanup, terminate)

		raw := s3waltest.RawS3Client(endpoint, "us-east-1", s3waltest.SeaweedFSAccessKey, s3waltest.SeaweedFSSecretKey)
		if err := s3waltest.EnsureBucket(ctx, raw, "s3wal-ref-conformance"); err != nil {
			s3walErr = err
			return
		}
		c := s3wal.New(raw, "s3wal-ref-conformance")
		c.DecodeSeq = s3wal.DecodeSeq // arm the read-side seq watermark
		store := newS3WALStore(c, func(ctx context.Context) error { return s3wal.Probe(ctx, c) }, c.Close)
		s3walCleanup = append(s3walCleanup, store.Close)
		if err := store.InitSchema(ctx); err != nil {
			s3walErr = fmt.Errorf("s3wal InitSchema: %w", err)
			return
		}
		s3walStore = store
	})
	if s3walErr != nil {
		t.Fatalf("s3wal backend: %v", s3walErr)
	}
	return s3walStore
}

func TestMain(m *testing.M) {
	code := m.Run()
	for i := len(pgCleanup) - 1; i >= 0; i-- {
		pgCleanup[i]()
	}
	for i := len(s3walCleanup) - 1; i >= 0; i-- {
		s3walCleanup[i]()
	}
	os.Exit(code)
}

// runRefBackends fans one conformance test body out over every Store
// implementation. The pg and s3wal lanes share one container-wide store
// each — tests isolate by repo name (t.Name() is subtest-unique) — and the
// sqlite lane gets a fresh DB file per subtest.
func runRefBackends(t *testing.T, fn func(t *testing.T, s Store)) {
	t.Run("pg", func(t *testing.T) { fn(t, pgTestStore(t)) })
	t.Run("sqlite", func(t *testing.T) {
		s, _ := openTestSQLite(t)
		fn(t, s)
	})
	t.Run("s3wal", func(t *testing.T) { fn(t, s3walTestStore(t)) })
}

func TestGenerationBumpsOnEveryMutation(t *testing.T) {
	runRefBackends(t, testGenerationBumpsOnEveryMutation)
}

func testGenerationBumpsOnEveryMutation(t *testing.T, s Store) {
	ctx := context.Background()
	repo := t.Name()
	other := repo + "-isolated"

	gen := func(r string) int64 {
		t.Helper()
		g, err := s.GetGeneration(ctx, r)
		if err != nil {
			t.Fatalf("GetGeneration(%q): %v", r, err)
		}
		return g
	}
	mustCAS := func(name, old, new string, wantWon bool) {
		t.Helper()
		won, err := s.CASRef(ctx, repo, name, old, new)
		if err != nil {
			t.Fatalf("CASRef(%s, %s -> %s): %v", name, old, new, err)
		}
		if won != wantWon {
			t.Fatalf("CASRef(%s, %s -> %s) won = %v, want %v", name, old, new, won, wantWon)
		}
	}

	if g := gen(repo); g != 0 {
		t.Fatalf("virgin repo generation = %d, want 0", g)
	}

	// A second repo pins the isolation property: its generation must not
	// move while the mutations below hammer the first repo.
	if err := s.RegisterPacks(ctx, other, []Pack{{ID: "iso", Key: "k-iso"}}); err != nil {
		t.Fatalf("RegisterPacks(%q): %v", other, err)
	}
	if g := gen(other); g != 1 {
		t.Fatalf("isolated repo generation = %d, want 1 after first bump", g)
	}

	// The full RefStore mutation surface, table-driven (issue #4 acceptance:
	// EVERY mutation path bumps; lost races must NOT).
	const branch = "refs/heads/main"
	steps := []struct {
		name   string
		mutate func(t *testing.T)
		bump   bool
	}{
		{"RegisterPacks", func(t *testing.T) {
			if err := s.RegisterPacks(ctx, repo, []Pack{{ID: "p1", Key: "k1"}}); err != nil {
				t.Fatal(err)
			}
		}, true},
		// The generation is an invalidation token over committed row
		// changes, not a call counter: a retried identical registration
		// changes no rows (ON CONFLICT DO NOTHING), fires no trigger, and
		// must not invalidate hydrate markers — nothing changed.
		{"RegisterPacks idempotent retry", func(t *testing.T) {
			if err := s.RegisterPacks(ctx, repo, []Pack{{ID: "p1", Key: "k1"}}); err != nil {
				t.Fatal(err)
			}
		}, false},
		{"CASRef create won", func(t *testing.T) { mustCAS(branch, ZeroSHA, testSHA(0x11), true) }, true},
		{"CASRef create lost", func(t *testing.T) { mustCAS(branch, ZeroSHA, testSHA(0x22), false) }, false},
		{"CASRef update won", func(t *testing.T) { mustCAS(branch, testSHA(0x11), testSHA(0x22), true) }, true},
		{"CASRef update lost", func(t *testing.T) { mustCAS(branch, testSHA(0x11), testSHA(0x33), false) }, false},
		{"CASRef delete lost", func(t *testing.T) { mustCAS(branch, testSHA(0x11), ZeroSHA, false) }, false},
		{"CASRef delete won", func(t *testing.T) { mustCAS(branch, testSHA(0x22), ZeroSHA, true) }, true},
	}
	for _, step := range steps {
		before := gen(repo)
		step.mutate(t)
		want := before
		if step.bump {
			want++
		}
		if got := gen(repo); got != want {
			t.Fatalf("%s: generation %d -> %d, want %d", step.name, before, got, want)
		}
	}

	if g := gen(other); g != 1 {
		t.Fatalf("isolated repo generation moved to %d; mutations must only bump their own repo", g)
	}

	// Multi-pack registration: the triggers fire per changed row, so two
	// new packs advance the generation by exactly two. Callers only rely on
	// "strictly increases"; the exact delta pins the per-row granularity.
	before := gen(repo)
	if err := s.RegisterPacks(ctx, repo, []Pack{{ID: "m1", Key: "km1"}, {ID: "m2", Key: "km2"}}); err != nil {
		t.Fatal(err)
	}
	if got := gen(repo); got != before+2 {
		t.Fatalf("multi-pack registration: generation %d -> %d, want %d", before, got, before+2)
	}
}

// TestGenerationLegacyWriterStillBumps pins the rollout-safety property the
// schema triggers exist for (PR #14 review, P1): a writer running a binary
// that predates the generation counter commits plain refs/packs DML with no
// application-side bump — the INSERT/UPDATE/DELETE statements below are
// exactly what such a binary executes. Once any upgraded node's InitSchema
// installed the triggers, those legacy writes must bump the generation
// anyway, on both backends.
func TestGenerationLegacyWriterStillBumps(t *testing.T) {
	runRefBackends(t, testGenerationLegacyWriterStillBumps)
}

func testGenerationLegacyWriterStillBumps(t *testing.T, s Store) {
	if _, ok := s.(*S3WAL); ok {
		// s3wal has no SQL schema, hence no legacy binary that could write
		// raw DML around the driver: the driver's per-row bumps are the
		// ONLY write path, so the trigger rollout-safety property this
		// test pins does not exist there.
		t.Skip("s3wal has no SQL triggers or legacy raw-DML writers")
	}
	ctx := context.Background()
	repo := t.Name()

	// Raw SQL, bypassing every store method — the legacy binary's writes.
	exec := func(pgSQL, liteSQL string, args ...any) {
		t.Helper()
		var err error
		switch b := s.(type) {
		case *PG:
			_, err = b.pool.Exec(ctx, pgSQL, args...)
		case *SQLite:
			_, err = b.db.ExecContext(ctx, liteSQL, args...)
		default:
			t.Fatalf("unexpected store type %T", s)
		}
		if err != nil {
			t.Fatalf("legacy write: %v", err)
		}
	}
	gen := func() int64 {
		t.Helper()
		g, err := s.GetGeneration(ctx, repo)
		if err != nil {
			t.Fatalf("GetGeneration: %v", err)
		}
		return g
	}

	steps := []struct {
		name           string
		pgSQL, liteSQL string
		args           []any
	}{
		{
			"legacy ref create",
			`INSERT INTO refs (repo, name, sha, created_at) VALUES ($1, $2, $3, now()) ON CONFLICT DO NOTHING`,
			`INSERT INTO refs (repo, name, sha, created_at) VALUES (?, ?, ?, unixepoch()) ON CONFLICT DO NOTHING`,
			[]any{repo, "refs/heads/main", testSHA(0x41)},
		},
		{
			"legacy ref update",
			`UPDATE refs SET sha = $1 WHERE repo = $2 AND name = $3 AND sha = $4`,
			`UPDATE refs SET sha = ? WHERE repo = ? AND name = ? AND sha = ?`,
			[]any{testSHA(0x42), repo, "refs/heads/main", testSHA(0x41)},
		},
		{
			"legacy pack insert",
			`INSERT INTO packs (repo, id, s3_key) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
			`INSERT INTO packs (repo, id, s3_key) VALUES (?, ?, ?) ON CONFLICT DO NOTHING`,
			[]any{repo, "lp1", "lk1"},
		},
		{
			"legacy ref delete",
			`DELETE FROM refs WHERE repo = $1 AND name = $2 AND sha = $3`,
			`DELETE FROM refs WHERE repo = ? AND name = ? AND sha = ?`,
			[]any{repo, "refs/heads/main", testSHA(0x42)},
		},
	}
	for _, step := range steps {
		before := gen()
		exec(step.pgSQL, step.liteSQL, step.args...)
		if got := gen(); got != before+1 {
			t.Fatalf("%s: generation %d -> %d, want %d (schema trigger must fire for legacy DML)",
				step.name, before, got, before+1)
		}
	}
}

// TestGenerationRecoveredWinBumpsExactlyOnce pins the issue #2 / issue #4
// interaction: an ambiguous CASRef whose statement actually committed
// already bumped the generation atomically with the swap, so the
// IdempotentCAS recovery recheck — a pure point-read — must neither bump
// again nor leave the recovered win without its bump.
func TestGenerationRecoveredWinBumpsExactlyOnce(t *testing.T) {
	ctx := context.Background()
	raw, _ := openTestSQLite(t)
	const repo, branch = "r-gen-recovery", "refs/heads/main"
	seedRef(t, raw, repo, branch, idemSHA1)

	before, err := raw.GetGeneration(ctx, repo)
	if err != nil {
		t.Fatalf("GetGeneration: %v", err)
	}
	if before != 1 {
		t.Fatalf("generation after seed create = %d, want 1", before)
	}

	st := IdempotentCAS{RefStore: &commitThenFail{RefStore: raw}}
	won, err := st.CASRef(ctx, repo, branch, idemSHA1, idemSHA2)
	if !won || err != nil {
		t.Fatalf("CASRef = (%v, %v), want recovered (true, nil)", won, err)
	}

	after, err := raw.GetGeneration(ctx, repo)
	if err != nil {
		t.Fatalf("GetGeneration: %v", err)
	}
	if after != before+1 {
		t.Fatalf("generation after recovered win = %d, want exactly %d (one bump, from the committed statement)", after, before+1)
	}
}

// TestCASRefGenAttributesWonGeneration (issue #186 finding 5): a winning
// CASRefGen must report the EXACT generation its own commit produced —
// not a post-hoc read that can blend in concurrent writers — on every
// backend, so push-hold range coalescing can attribute each bump to the
// hold that caused it. Losses report gen 0.
func TestCASRefGenAttributesWonGeneration(t *testing.T) {
	runRefBackends(t, testCASRefGenAttributesWonGeneration)
}

func testCASRefGenAttributesWonGeneration(t *testing.T, s Store) {
	ctx := context.Background()
	repo := t.Name()
	const branch = "refs/heads/main"

	gen := func() int64 {
		t.Helper()
		g, err := s.GetGeneration(ctx, repo)
		if err != nil {
			t.Fatalf("GetGeneration: %v", err)
		}
		return g
	}
	casGen := func(name, old, new string, wantWon bool) int64 {
		t.Helper()
		won, g, err := s.CASRefGen(ctx, repo, name, old, new)
		if err != nil {
			t.Fatalf("CASRefGen(%s, %s -> %s): %v", name, old, new, err)
		}
		if won != wantWon {
			t.Fatalf("CASRefGen(%s, %s -> %s) won = %v, want %v", name, old, new, won, wantWon)
		}
		return g
	}

	// Create on a virgin repo: the first mutation is generation 1 exactly.
	if g := casGen(branch, ZeroSHA, testSHA(0x11), true); g != 1 {
		t.Fatalf("create attribution = %d, want 1", g)
	}
	if g := gen(); g != 1 {
		t.Fatalf("GetGeneration after create = %d, want 1", g)
	}
	// A lost race attributes nothing.
	if g := casGen(branch, ZeroSHA, testSHA(0x22), false); g != 0 {
		t.Fatalf("lost create attribution = %d, want 0", g)
	}
	if g := casGen(branch, testSHA(0x33), testSHA(0x22), false); g != 0 {
		t.Fatalf("lost update attribution = %d, want 0", g)
	}
	if g := gen(); g != 1 {
		t.Fatalf("GetGeneration after losses = %d, want 1 (no bump)", g)
	}
	// Update and delete attribute their own +1.
	if g := casGen(branch, testSHA(0x11), testSHA(0x22), true); g != 2 {
		t.Fatalf("update attribution = %d, want 2", g)
	}
	// A non-CAS mutation in between (pack registration bumps via the same
	// machinery): the next win must attribute the value AFTER it.
	if err := s.RegisterPacks(ctx, repo, []Pack{{ID: "p1", Key: "k1"}}); err != nil {
		t.Fatalf("RegisterPacks: %v", err)
	}
	if g := casGen(branch, testSHA(0x22), ZeroSHA, true); g != 4 {
		t.Fatalf("delete attribution = %d, want 4 (after the pack bump)", g)
	}
	if g := gen(); g != 4 {
		t.Fatalf("GetGeneration after delete = %d, want 4", g)
	}

	// The attribution property under contention (the reason a post-hoc
	// GetGeneration cannot substitute): N concurrent winning swaps must
	// attribute N DISTINCT consecutive generations — every bump owned by
	// exactly one winner, none blended or double-counted.
	const writers = 8
	base := gen()
	gens := make([]int64, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("refs/heads/w%d", i)
			won, g, err := s.CASRefGen(ctx, repo, name, ZeroSHA, testSHA(byte(0x40+i)))
			if err != nil || !won {
				t.Errorf("concurrent CASRefGen(%s) = (%v, %v), want win", name, won, err)
				return
			}
			gens[i] = g
		}(i)
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}
	seen := make(map[int64]bool, writers)
	for i, g := range gens {
		if g <= base || g > base+writers {
			t.Fatalf("writer %d attributed generation %d outside (%d, %d]", i, g, base, base+writers)
		}
		if seen[g] {
			t.Fatalf("generation %d attributed to two writers", g)
		}
		seen[g] = true
	}
	if g := gen(); g != base+writers {
		t.Fatalf("GetGeneration after %d wins = %d, want %d", writers, g, base+writers)
	}
}
