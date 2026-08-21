// Package ref provides the ref authority + pack registry: the RefStore
// contract plus the Postgres implementation (pgx).
package ref

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	// Registers the pure-Go "sqlite" database/sql driver the sqlite backend
	// opens (cgo-free cross-compiled single binaries).
	_ "modernc.org/sqlite"
)

// ZeroSHA is git's null object id (40 zeros) marking ref create/delete.
const ZeroSHA = "0000000000000000000000000000000000000000"

var shaRe = regexp.MustCompile(`\A[0-9a-f]{40}\z`)

// ValidSHA reports whether s is a well-formed 40-hex git object id.
func ValidSHA(s string) bool { return shaRe.MatchString(s) }

// ErrNotFound reports a GetRef miss: the ref does not currently exist.
var ErrNotFound = errors.New("ref not found")

// assertf is a fail-fast invariant check (crash-and-burn style).
func assertf(cond bool, format string, args ...any) {
	if !cond {
		panic(fmt.Sprintf("ref assertion failed: "+format, args...))
	}
}

// Pack is one registered pack for a repo. Key is the blob-store object key
// (blob.PackKey); the backing SQL column stays named s3_key in every backend
// so the pack SQL is byte-comparable across them (no PG migration).
type Pack struct {
	ID  string
	Key string
}

// RefInfo is one ref row with its metadata. CreatedAt is the row's creation
// time (docs/03 branches[].created_at): set on first insert, preserved
// across SHA updates, and reset when a ref is deleted and recreated.
type RefInfo struct {
	Name      string
	SHA       string
	CreatedAt time.Time // UTC
}

// RefStore is the ref-authority contract: refs only ever change through
// CASRef, and packs are registered strictly after their bytes are durable
// in blob storage.
type RefStore interface {
	// ListRefs returns all refs for repo as name -> sha.
	ListRefs(ctx context.Context, repo string) (map[string]string, error)
	// ListRefsWithMeta returns all refs for repo sorted by name ascending,
	// including creation timestamps (the read API's branches/tags source).
	ListRefsWithMeta(ctx context.Context, repo string) ([]RefInfo, error)
	// GetRef returns the current sha of one ref (a point-read: the
	// IdempotentCAS recovery recheck must not scan the repo's whole ref
	// table). Returns ErrNotFound when the ref does not exist.
	GetRef(ctx context.Context, repo, name string) (string, error)
	// CASRef performs a compare-and-swap ref update (old/new = ZeroSHA for
	// create/delete). Returns won=false (no error) when the precondition
	// failed, i.e. someone else won the race.
	CASRef(ctx context.Context, repo, name, old, new string) (won bool, err error)
	// RegisterPacks records packs for repo atomically; idempotent on retry.
	RegisterPacks(ctx context.Context, repo string, packs []Pack) error
	// ListPacks returns all registered packs for repo, oldest first.
	ListPacks(ctx context.Context, repo string) ([]Pack, error)
	// GetGeneration returns repo's mutation generation: 0 for a repo no
	// mutation ever touched, strictly positive and monotonically increasing
	// afterwards. Every mutation bumps it in the same atomic unit as the
	// mutation itself — RegisterPacks always, CASRef exactly on a won swap,
	// and future pack rewrites (issue #1's ReplacePacks) must do the same —
	// so an unchanged value proves the repo's packs and refs are unchanged.
	// The hydrate short-circuit relies on exactly that (issue #4).
	GetGeneration(ctx context.Context, repo string) (int64, error)
}

// Store extends RefStore with the lifecycle the wire seam needs: serve runs
// InitSchema once at startup and Close on shutdown. Consumers keep coding
// against RefStore; hooks never run DDL.
type Store interface {
	RefStore
	// InitSchema creates the ref/pack tables (idempotent, safe under
	// concurrent startups).
	InitSchema(ctx context.Context) error
	Close()
}

// Open dispatches on the URL scheme to the matching Store backend:
//
//	postgres:// or postgresql://  → OpenPG
//	sqlite:///abs/path/db.sqlite  → pure-Go SQLite store
//
// Open never runs InitSchema: hooks reconstruct stores from env URLs and
// must not run DDL — the serve/wire seam owns schema init.
func Open(ctx context.Context, dbURL string) (Store, error) {
	assertf(dbURL != "", "Open: empty ref store URL")
	u, err := url.Parse(dbURL)
	if err != nil {
		return nil, fmt.Errorf("parse ref store URL: %w", err)
	}
	switch u.Scheme {
	case "postgres", "postgresql":
		return OpenPG(ctx, dbURL)
	case "sqlite":
		return OpenSQLite(ctx, dbURL)
	default:
		return nil, fmt.Errorf("ref store URL scheme %q unsupported (want postgres, postgresql, or sqlite)", u.Scheme)
	}
}

// PG implements Store on Postgres.
type PG struct {
	pool *pgxpool.Pool
}

var _ Store = (*PG)(nil)

// OpenPG connects to pgURL and pings the database.
func OpenPG(ctx context.Context, pgURL string) (*PG, error) {
	assertf(pgURL != "", "OpenPG: empty PG URL")
	pool, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		return nil, fmt.Errorf("pg connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pg ping: %w", err)
	}
	return &PG{pool: pool}, nil
}

func (s *PG) Close() { s.pool.Close() }

// InitSchema creates the ref/pack tables (idempotent). The advisory lock
// serializes concurrent node startups: two racing CREATE TABLE IF NOT EXISTS
// can fail with a duplicate pg_type key error (found via dogfooding).
func (s *PG) InitSchema(ctx context.Context) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(0x5350494B45)`); err != nil { // "SPIKE"
			return err
		}
		_, err := tx.Exec(ctx, `
CREATE TABLE IF NOT EXISTS refs  (repo text, name text, sha text NOT NULL,
                                  created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (repo, name));
CREATE TABLE IF NOT EXISTS packs (repo text, id text, s3_key text NOT NULL,
                                  created_at timestamptz DEFAULT now(), PRIMARY KEY (repo, id));
-- Per-repo mutation generation (issue #4). No row means "never mutated"
-- (GetGeneration reports 0); the first bump's upsert creates the row at the
-- DEFAULT, so 0 and 1 stay distinguishable and a hydrate marker taken before
-- the first mutation can never match the state after it.
CREATE TABLE IF NOT EXISTS repo_generations (repo text PRIMARY KEY,
                                             generation bigint NOT NULL DEFAULT 1);
-- Idempotent additive migration for pre-p3 dev databases (backfills
-- existing rows via the NOT NULL DEFAULT).
ALTER TABLE refs ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now();
`)
		return err
	})
}

// ListRefs returns all refs for repo as name -> sha.
func (s *PG) ListRefs(ctx context.Context, repo string) (map[string]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT name, sha FROM refs WHERE repo = $1`, repo)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	refs := make(map[string]string)
	for rows.Next() {
		var name, sha string
		if err := rows.Scan(&name, &sha); err != nil {
			return nil, err
		}
		assertf(ValidSHA(sha), "refs table holds malformed sha %q for %s/%s", sha, repo, name)
		refs[name] = sha
	}
	return refs, rows.Err()
}

// ListRefsWithMeta returns all refs for repo sorted by name ascending.
func (s *PG) ListRefsWithMeta(ctx context.Context, repo string) ([]RefInfo, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT name, sha, created_at FROM refs WHERE repo = $1 ORDER BY name`, repo)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var refs []RefInfo
	for rows.Next() {
		var r RefInfo
		if err := rows.Scan(&r.Name, &r.SHA, &r.CreatedAt); err != nil {
			return nil, err
		}
		assertf(ValidSHA(r.SHA), "refs table holds malformed sha %q for %s/%s", r.SHA, repo, r.Name)
		r.CreatedAt = r.CreatedAt.UTC()
		refs = append(refs, r)
	}
	return refs, rows.Err()
}

// GetRef returns the current sha of one ref, or ErrNotFound.
func (s *PG) GetRef(ctx context.Context, repo, name string) (string, error) {
	assertf(repo != "" && name != "", "GetRef: empty repo %q or name %q", repo, name)
	var sha string
	err := s.pool.QueryRow(ctx,
		`SELECT sha FROM refs WHERE repo = $1 AND name = $2`, repo, name).Scan(&sha)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", err
	}
	assertf(ValidSHA(sha), "refs table holds malformed sha %q for %s/%s", sha, repo, name)
	return sha, nil
}

// CASRef performs the compare-and-swap ref update. Returns won=false (no
// error) when the precondition failed, i.e. someone else won the race.
// Refs only ever change inside these single statements, so a broken/partial
// ref state is impossible by construction.
//
// Each statement carries the ref mutation in a data-modifying CTE and bumps
// the repo generation exactly when the CTE emitted a row (the swap matched):
// mutation + bump commit atomically WITHOUT an explicit transaction, so the
// statement stays the single unit of commit and the IdempotentCAS ambiguity
// classification (idempotent.go) is unchanged. The outer upsert's row count
// equals the CTE's, so RowsAffected still reports won 0/1. Lock order is
// refs/packs row first, then the repo_generations row, everywhere — no
// deadlock cycles.
func (s *PG) CASRef(ctx context.Context, repo, name, old, new string) (won bool, err error) {
	assertf(ValidSHA(old), "CASRef: malformed old sha %q", old)
	assertf(ValidSHA(new), "CASRef: malformed new sha %q", new)
	assertf(old != ZeroSHA || new != ZeroSHA, "CASRef: zero -> zero update for %s/%s", repo, name)

	// Acquire explicitly so acquisition failures (pool exhausted, deadline
	// expired while waiting) are distinguishable from statement failures:
	// the statement provably never executed, so IdempotentCAS must not
	// recover such errors (see notExecuted).
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return false, fmt.Errorf("cas acquire %s/%s: %w", repo, name, notExecuted(err))
	}
	defer conn.Release()
	// Pre-dispatch guard: a context canceled after acquire but before Exec
	// sends the statement is definitive non-execution too. (The residual
	// race — cancellation between this check and the wire write — is
	// covered by pgconn itself, whose pre-send check returns a
	// SafeToRetry-tagged contextAlreadyDoneError.)
	if cerr := ctx.Err(); cerr != nil {
		return false, fmt.Errorf("cas %s/%s: %w", repo, name, notExecuted(cerr))
	}

	var tag string
	var ct interface{ RowsAffected() int64 }
	switch {
	case old == ZeroSHA: // create
		// created_at is set here once; SHA updates below never touch it.
		c, cerr := conn.Exec(ctx, `
WITH cas AS (
    INSERT INTO refs (repo, name, sha, created_at) VALUES ($1, $2, $3, now())
    ON CONFLICT DO NOTHING
    RETURNING 1
)
INSERT INTO repo_generations (repo)
SELECT $1::text FROM cas
ON CONFLICT (repo) DO UPDATE SET generation = repo_generations.generation + 1`,
			repo, name, new)
		ct, err, tag = c, cerr, "create"
	case new == ZeroSHA: // delete
		c, cerr := conn.Exec(ctx, `
WITH cas AS (
    DELETE FROM refs WHERE repo = $1 AND name = $2 AND sha = $3
    RETURNING 1
)
INSERT INTO repo_generations (repo)
SELECT $1::text FROM cas
ON CONFLICT (repo) DO UPDATE SET generation = repo_generations.generation + 1`,
			repo, name, old)
		ct, err, tag = c, cerr, "delete"
	default: // update
		c, cerr := conn.Exec(ctx, `
WITH cas AS (
    UPDATE refs SET sha = $1 WHERE repo = $2 AND name = $3 AND sha = $4
    RETURNING 1
)
INSERT INTO repo_generations (repo)
SELECT $2::text FROM cas
ON CONFLICT (repo) DO UPDATE SET generation = repo_generations.generation + 1`,
			new, repo, name, old)
		ct, err, tag = c, cerr, "update"
	}
	if err != nil {
		return false, fmt.Errorf("cas %s %s/%s: %w", tag, repo, name, err)
	}
	n := ct.RowsAffected()
	assertf(n == 0 || n == 1, "cas %s %s/%s affected %d rows", tag, repo, name, n)
	return n == 1, nil
}

// RegisterPacks inserts pack rows in ONE transaction (crash-ordering: this
// runs strictly after the blob upload succeeded). Idempotent on retry: a
// re-pushed identical pack has the same id.
func (s *PG) RegisterPacks(ctx context.Context, repo string, packs []Pack) error {
	assertf(len(packs) > 0, "RegisterPacks called with no packs")
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		for _, p := range packs {
			assertf(p.ID != "" && p.Key != "", "RegisterPacks: empty pack id/key %+v", p)
			if _, err := tx.Exec(ctx,
				`INSERT INTO packs (repo, id, s3_key) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
				repo, p.ID, p.Key); err != nil {
				return err
			}
		}
		// Same-transaction generation bump (issue #4): a committed pack
		// registration must invalidate the hydrate short-circuit. Bumped
		// unconditionally — a retried registration whose rows all already
		// existed costs one spurious full hydrate, never a stale serve.
		if _, err := tx.Exec(ctx, `
INSERT INTO repo_generations (repo) VALUES ($1)
ON CONFLICT (repo) DO UPDATE SET generation = repo_generations.generation + 1`, repo); err != nil {
			return err
		}
		return nil
	})
}

// GetGeneration returns repo's mutation generation (0 = never mutated).
func (s *PG) GetGeneration(ctx context.Context, repo string) (int64, error) {
	assertf(repo != "", "GetGeneration: empty repo")
	var gen int64
	err := s.pool.QueryRow(ctx,
		`SELECT generation FROM repo_generations WHERE repo = $1`, repo).Scan(&gen)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil // never mutated: the first bump creates the row at 1
	}
	if err != nil {
		return 0, err
	}
	assertf(gen >= 1, "repo_generations holds non-positive generation %d for %s", gen, repo)
	return gen, nil
}

// ListPacks returns all registered packs for repo, oldest first.
func (s *PG) ListPacks(ctx context.Context, repo string) ([]Pack, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, s3_key FROM packs WHERE repo = $1 ORDER BY created_at, id`, repo)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var packs []Pack
	for rows.Next() {
		var p Pack
		if err := rows.Scan(&p.ID, &p.Key); err != nil {
			return nil, err
		}
		packs = append(packs, p)
	}
	return packs, rows.Err()
}
