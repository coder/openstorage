// SQLite implementation of Store for local mode: the serve process and
// re-exec'd hook processes share one DB file on the same host, so the DSN
// forces WAL + busy_timeout, and _txlock=immediate turns every explicit
// transaction into BEGIN IMMEDIATE (no deferred→write lock upgrade
// deadlocks). CAS semantics are byte-for-byte the PG contract: refs only
// ever change through single statements.

package ref

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// sqliteDSN builds the modernc.org/sqlite DSN for a DB file path. The
// _pragma values apply to every pooled connection, so serve and hook
// processes all agree on WAL + a 5s busy handler.
func sqliteDSN(path string) string {
	return "file:" + path +
		"?_pragma=journal_mode(WAL)" +
		"&_pragma=busy_timeout(5000)" +
		"&_pragma=foreign_keys(1)" +
		"&_txlock=immediate"
}

// SQLite implements Store on one local SQLite file via the pure-Go
// modernc.org/sqlite driver (cgo-free cross-compiled single binaries).
type SQLite struct {
	db *sql.DB
}

var _ Store = (*SQLite)(nil)

// OpenSQLite opens sqlite:///abs/path/db.sqlite. The path is everything
// after "sqlite://" (relative paths are tolerated; --data-dir always derives
// absolute ones). Parent directories are created; the DB file itself is
// created by the driver on first open.
func OpenSQLite(ctx context.Context, sqliteURL string) (*SQLite, error) {
	assertf(sqliteURL != "", "OpenSQLite: empty sqlite URL")
	path, ok := strings.CutPrefix(sqliteURL, "sqlite://")
	if !ok || path == "" {
		return nil, fmt.Errorf("sqlite URL %q: want sqlite:///path/to/db.sqlite", sqliteURL)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("sqlite mkdir for %s: %w", path, err)
	}
	db, err := sql.Open("sqlite", sqliteDSN(path))
	if err != nil {
		return nil, fmt.Errorf("sqlite open %s: %w", path, err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite ping %s: %w", path, err)
	}
	return &SQLite{db: db}, nil
}

func (s *SQLite) Close() { _ = s.db.Close() }

// InitSchema creates the ref/pack tables (idempotent). No advisory lock
// (unlike PG): SQLite's file lock + busy_timeout serialize concurrent
// startups, and CREATE TABLE IF NOT EXISTS is race-free under that lock.
func (s *SQLite) InitSchema(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite init begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS refs  (repo TEXT, name TEXT, sha TEXT NOT NULL, PRIMARY KEY (repo, name))`,
		// The pack key column stays named s3_key so the pack SQL is
		// byte-comparable with the PG backend (see Pack). created_at is
		// INTEGER unix seconds; ORDER BY created_at, id keeps the tie-break.
		`CREATE TABLE IF NOT EXISTS packs (repo TEXT, id TEXT, s3_key TEXT NOT NULL,
		                                   created_at INTEGER DEFAULT (unixepoch()), PRIMARY KEY (repo, id))`,
	} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("sqlite init schema: %w", err)
		}
	}
	return tx.Commit()
}

// ListRefs returns all refs for repo as name -> sha.
func (s *SQLite) ListRefs(ctx context.Context, repo string) (map[string]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT name, sha FROM refs WHERE repo = ?`, repo)
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

// CASRef performs the compare-and-swap ref update. Returns won=false (no
// error) when the precondition failed, i.e. someone else won the race.
// Refs only ever change inside these single statements, so a broken/partial
// ref state is impossible by construction.
func (s *SQLite) CASRef(ctx context.Context, repo, name, old, new string) (won bool, err error) {
	assertf(ValidSHA(old), "CASRef: malformed old sha %q", old)
	assertf(ValidSHA(new), "CASRef: malformed new sha %q", new)
	assertf(old != ZeroSHA || new != ZeroSHA, "CASRef: zero -> zero update for %s/%s", repo, name)

	var tag string
	var res sql.Result
	switch {
	case old == ZeroSHA: // create
		res, err = s.db.ExecContext(ctx,
			`INSERT INTO refs (repo, name, sha) VALUES (?, ?, ?) ON CONFLICT DO NOTHING`,
			repo, name, new)
		tag = "create"
	case new == ZeroSHA: // delete
		res, err = s.db.ExecContext(ctx,
			`DELETE FROM refs WHERE repo = ? AND name = ? AND sha = ?`,
			repo, name, old)
		tag = "delete"
	default: // update
		res, err = s.db.ExecContext(ctx,
			`UPDATE refs SET sha = ? WHERE repo = ? AND name = ? AND sha = ?`,
			new, repo, name, old)
		tag = "update"
	}
	if err != nil {
		return false, fmt.Errorf("cas %s %s/%s: %w", tag, repo, name, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("cas %s %s/%s rows: %w", tag, repo, name, err)
	}
	assertf(n == 0 || n == 1, "cas %s %s/%s affected %d rows", tag, repo, name, n)
	return n == 1, nil
}

// RegisterPacks inserts pack rows in ONE transaction (crash-ordering: this
// runs strictly after the blob upload succeeded). Idempotent on retry: a
// re-pushed identical pack has the same id.
func (s *SQLite) RegisterPacks(ctx context.Context, repo string, packs []Pack) error {
	assertf(len(packs) > 0, "RegisterPacks called with no packs")
	tx, err := s.db.BeginTx(ctx, nil) // BEGIN IMMEDIATE via _txlock
	if err != nil {
		return fmt.Errorf("register packs begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, p := range packs {
		assertf(p.ID != "" && p.Key != "", "RegisterPacks: empty pack id/key %+v", p)
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO packs (repo, id, s3_key) VALUES (?, ?, ?) ON CONFLICT DO NOTHING`,
			repo, p.ID, p.Key); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ListPacks returns all registered packs for repo, oldest first.
func (s *SQLite) ListPacks(ctx context.Context, repo string) ([]Pack, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, s3_key FROM packs WHERE repo = ? ORDER BY created_at, id`, repo)
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
