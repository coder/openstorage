package spike

import (
	"context"
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ZeroSHA is git's null object id (40 zeros) marking ref create/delete.
const ZeroSHA = "0000000000000000000000000000000000000000"

var shaRe = regexp.MustCompile(`\A[0-9a-f]{40}\z`)

// Store is the Postgres ref authority + pack registry (spike-local schema;
// the docs deliberately do not specify one).
type Store struct {
	pool *pgxpool.Pool
}

func OpenStore(ctx context.Context, pgURL string) (*Store, error) {
	assertf(pgURL != "", "OpenStore: empty PG_URL")
	pool, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		return nil, fmt.Errorf("pg connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pg ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

// InitSchema creates the spike tables (idempotent). The advisory lock
// serializes concurrent node startups: two racing CREATE TABLE IF NOT EXISTS
// can fail with a duplicate pg_type key error (found via dogfooding).
func (s *Store) InitSchema(ctx context.Context) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(0x5350494B45)`); err != nil { // "SPIKE"
			return err
		}
		_, err := tx.Exec(ctx, `
CREATE TABLE IF NOT EXISTS refs  (repo text, name text, sha text NOT NULL, PRIMARY KEY (repo, name));
CREATE TABLE IF NOT EXISTS packs (repo text, id text, s3_key text NOT NULL,
                                  created_at timestamptz DEFAULT now(), PRIMARY KEY (repo, id));
`)
		return err
	})
}

// ListRefs returns all refs for repo as name -> sha.
func (s *Store) ListRefs(ctx context.Context, repo string) (map[string]string, error) {
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
		assertf(shaRe.MatchString(sha), "refs table holds malformed sha %q for %s/%s", sha, repo, name)
		refs[name] = sha
	}
	return refs, rows.Err()
}

// CASRef performs the compare-and-swap ref update. Returns won=false (no
// error) when the precondition failed, i.e. someone else won the race.
// Refs only ever change inside these single statements, so a broken/partial
// ref state is impossible by construction.
func (s *Store) CASRef(ctx context.Context, repo, name, old, new string) (won bool, err error) {
	assertf(shaRe.MatchString(old), "CASRef: malformed old sha %q", old)
	assertf(shaRe.MatchString(new), "CASRef: malformed new sha %q", new)
	assertf(old != ZeroSHA || new != ZeroSHA, "CASRef: zero -> zero update for %s/%s", repo, name)

	var tag string
	var ct interface{ RowsAffected() int64 }
	switch {
	case old == ZeroSHA: // create
		c, cerr := s.pool.Exec(ctx,
			`INSERT INTO refs (repo, name, sha) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
			repo, name, new)
		ct, err, tag = c, cerr, "create"
	case new == ZeroSHA: // delete
		c, cerr := s.pool.Exec(ctx,
			`DELETE FROM refs WHERE repo = $1 AND name = $2 AND sha = $3`,
			repo, name, old)
		ct, err, tag = c, cerr, "delete"
	default: // update
		c, cerr := s.pool.Exec(ctx,
			`UPDATE refs SET sha = $1 WHERE repo = $2 AND name = $3 AND sha = $4`,
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

// Pack is one registered pack for a repo.
type Pack struct {
	ID    string
	S3Key string
}

// RegisterPacks inserts pack rows in ONE transaction (crash-ordering: this
// runs strictly after the S3 upload succeeded). Idempotent on retry: a
// re-pushed identical pack has the same id.
func (s *Store) RegisterPacks(ctx context.Context, repo string, packs []Pack) error {
	assertf(len(packs) > 0, "RegisterPacks called with no packs")
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		for _, p := range packs {
			assertf(p.ID != "" && p.S3Key != "", "RegisterPacks: empty pack id/key %+v", p)
			if _, err := tx.Exec(ctx,
				`INSERT INTO packs (repo, id, s3_key) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
				repo, p.ID, p.S3Key); err != nil {
				return err
			}
		}
		return nil
	})
}

// ListPacks returns all registered packs for repo, oldest first.
func (s *Store) ListPacks(ctx context.Context, repo string) ([]Pack, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, s3_key FROM packs WHERE repo = $1 ORDER BY created_at, id`, repo)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var packs []Pack
	for rows.Next() {
		var p Pack
		if err := rows.Scan(&p.ID, &p.S3Key); err != nil {
			return nil, err
		}
		packs = append(packs, p)
	}
	return packs, rows.Err()
}
