// Generation-counter conformance tests (issue #4): every mutation path on
// the RefStore surface bumps the per-repo generation in the same atomic unit
// as the mutation, lost CAS races bump nothing, and GetGeneration is a
// point-read. Test bodies fan out over BOTH backends via runRefBackends —
// the pg lane lazily boots one testcontainers-go Postgres for the whole
// package (sqlite-filtered runs boot zero containers) and doubles as direct
// PG coverage for the CTE-based CASRef statements.
package ref

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

var (
	pgOnce    sync.Once
	pgErr     error
	pgStore   *PG
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

func TestMain(m *testing.M) {
	code := m.Run()
	for i := len(pgCleanup) - 1; i >= 0; i-- {
		pgCleanup[i]()
	}
	os.Exit(code)
}

// runRefBackends fans one conformance test body out over both Store
// implementations. The pg lane shares one container-wide store — tests
// isolate by repo name (t.Name() is subtest-unique) — and the sqlite lane
// gets a fresh DB file per subtest.
func runRefBackends(t *testing.T, fn func(t *testing.T, s Store)) {
	t.Run("pg", func(t *testing.T) { fn(t, pgTestStore(t)) })
	t.Run("sqlite", func(t *testing.T) {
		s, _ := openTestSQLite(t)
		fn(t, s)
	})
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
		// A retried identical registration still bumps ("always" per the
		// accepted scope): one spurious full hydrate, never a stale serve.
		{"RegisterPacks idempotent retry", func(t *testing.T) {
			if err := s.RegisterPacks(ctx, repo, []Pack{{ID: "p1", Key: "k1"}}); err != nil {
				t.Fatal(err)
			}
		}, true},
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
