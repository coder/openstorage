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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
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
		// refs.created_at is INTEGER unix seconds (docs/reference/http-api.md
		// branches[].created_at); CASRef sets it explicitly on insert so
		// rows are correct even on databases migrated by the ALTER below
		// (SQLite cannot ADD COLUMN with a non-constant default).
		`CREATE TABLE IF NOT EXISTS refs  (repo TEXT, name TEXT, sha TEXT NOT NULL,
		                                   created_at INTEGER NOT NULL DEFAULT (unixepoch()), PRIMARY KEY (repo, name))`,
		// The pack key column stays named s3_key so the pack SQL is
		// byte-comparable with the PG backend (see Pack). created_at is
		// INTEGER unix seconds; ORDER BY created_at, id keeps the tie-break.
		`CREATE TABLE IF NOT EXISTS packs (repo TEXT, id TEXT, s3_key TEXT NOT NULL,
		                                   created_at INTEGER DEFAULT (unixepoch()), PRIMARY KEY (repo, id))`,
		// The orphan sweeper's HasPack filters on (repo, s3_key); the
		// (repo, id) primary key cannot serve it (see the PG DDL).
		`CREATE INDEX IF NOT EXISTS packs_repo_s3_key ON packs (repo, s3_key)`,
		// Per-repo mutation generation (issue #4). No row means "never
		// mutated" (GetGeneration reports 0); the first bump creates the
		// row at 1 — same contract as PG.
		`CREATE TABLE IF NOT EXISTS repo_generations (repo TEXT PRIMARY KEY,
		                                              generation INTEGER NOT NULL DEFAULT 1)`,
		// Pack-quota ledger (issues #29/#31, see ledger.go). Accounting
		// state only — no repo_generations triggers (same rationale as PG).
		// created_at is INTEGER epoch milliseconds stamped by the writing
		// statement (sqliteEpochMS), matching the admission predicate.
		`CREATE TABLE IF NOT EXISTS pack_reservations (key TEXT PRIMARY KEY, repo TEXT NOT NULL,
		                                               bytes INTEGER NOT NULL, owner TEXT NOT NULL,
		                                               created_at INTEGER NOT NULL,
		                                               attempts INTEGER NOT NULL DEFAULT 1,
		                                               gen TEXT NOT NULL DEFAULT '')`,
		`CREATE INDEX IF NOT EXISTS pack_reservations_repo ON pack_reservations (repo)`,
		`CREATE TABLE IF NOT EXISTS pack_retained (repo TEXT, key TEXT, bytes INTEGER NOT NULL,
		                                           superseded_at INTEGER NOT NULL DEFAULT (unixepoch()),
		                                           PRIMARY KEY (repo, key))`,
		`CREATE TABLE IF NOT EXISTS pack_usage (repo TEXT PRIMARY KEY, bytes INTEGER NOT NULL DEFAULT 0)`,
		// Registered LFS objects (issue #51): the LFS sweep registry and
		// quota settle record — see the PG DDL for the rationale.
		`CREATE TABLE IF NOT EXISTS lfs_objects (repo TEXT, key TEXT, bytes INTEGER NOT NULL,
		                                         created_at INTEGER NOT NULL DEFAULT (unixepoch()),
		                                         PRIMARY KEY (repo, key))`,
		// O(1) object-count cap reads — see the PG DDL for the rationale.
		`CREATE TABLE IF NOT EXISTS lfs_object_counts (repo TEXT PRIMARY KEY, count INTEGER NOT NULL DEFAULT 0)`,
		`CREATE TABLE IF NOT EXISTS lfs_adoption_gens (repo TEXT PRIMARY KEY, gen INTEGER NOT NULL DEFAULT 0)`,
		// Repository purge fence (issue #52, PR #105 security round):
		// see the PG schema comment.
		`CREATE TABLE IF NOT EXISTS purged_repos (repo TEXT PRIMARY KEY,
		    fenced_at INTEGER NOT NULL DEFAULT 0)`,
	} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("sqlite init schema: %w", err)
		}
	}
	// Idempotent additive migration for pre-p3 dev databases. SQLite lacks
	// ADD COLUMN IF NOT EXISTS, so probe the table info first.
	var haveCreatedAt int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pragma_table_info('refs') WHERE name = 'created_at'`).Scan(&haveCreatedAt); err != nil {
		return fmt.Errorf("sqlite init schema: probe refs.created_at: %w", err)
	}
	if haveCreatedAt == 0 {
		for _, stmt := range []string{
			`ALTER TABLE refs ADD COLUMN created_at INTEGER`,
			`UPDATE refs SET created_at = unixepoch() WHERE created_at IS NULL`,
		} {
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("sqlite init schema: add refs.created_at: %w", err)
			}
		}
	}
	// Idempotent additive migration for pre-attempt ledgers (issue #51,
	// Codex round 30): rows minted by older binaries count as one attempt.
	var haveAttempts int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pragma_table_info('pack_reservations') WHERE name = 'attempts'`).Scan(&haveAttempts); err != nil {
		return fmt.Errorf("sqlite init schema: probe pack_reservations.attempts: %w", err)
	}
	if haveAttempts == 0 {
		if _, err := tx.ExecContext(ctx,
			`ALTER TABLE pack_reservations ADD COLUMN attempts INTEGER NOT NULL DEFAULT 1`); err != nil {
			return fmt.Errorf("sqlite init schema: add pack_reservations.attempts: %w", err)
		}
	}
	// gen identifies the row incarnation (Codex round 33) — see the PG DDL.
	var haveGen int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pragma_table_info('pack_reservations') WHERE name = 'gen'`).Scan(&haveGen); err != nil {
		return fmt.Errorf("sqlite init schema: probe pack_reservations.gen: %w", err)
	}
	if haveGen == 0 {
		if _, err := tx.ExecContext(ctx,
			`ALTER TABLE pack_reservations ADD COLUMN gen TEXT NOT NULL DEFAULT ''`); err != nil {
			return fmt.Errorf("sqlite init schema: add pack_reservations.gen: %w", err)
		}
	}
	// Generation-bump triggers (issue #4): in SCHEMA, not application code,
	// so writers running older binaries (stale hook scripts re-execing a
	// pre-upgrade binary) bump too — see the PG InitSchema rationale. SQLite
	// has no multi-event triggers, so one trigger per (table, event); bodies
	// use INSERT OR IGNORE + UPDATE (no UPSERT inside trigger bodies).
	// Immediate per-row firing is deadlock-free here: SQLite has one writer
	// at a time (BEGIN IMMEDIATE + busy_timeout). DROP + CREATE keeps future
	// body changes effective on existing databases; the transaction plus
	// SQLite's file lock make it safe under concurrent startups.
	for _, table := range []string{"refs", "packs"} {
		for _, ev := range []struct{ event, key string }{
			{"INSERT", "NEW.repo"}, {"UPDATE", "NEW.repo"}, {"DELETE", "OLD.repo"},
		} {
			name := fmt.Sprintf("%s_bump_generation_%s", table, strings.ToLower(ev.event))
			if _, err := tx.ExecContext(ctx,
				fmt.Sprintf(`DROP TRIGGER IF EXISTS %s`, name)); err != nil {
				return fmt.Errorf("sqlite init schema: drop trigger %s: %w", name, err)
			}
			if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
CREATE TRIGGER %s AFTER %s ON %s FOR EACH ROW BEGIN
    INSERT OR IGNORE INTO repo_generations (repo, generation) VALUES (%s, 0);
    UPDATE repo_generations SET generation = generation + 1 WHERE repo = %s;
END`, name, ev.event, table, ev.key, ev.key)); err != nil {
				return fmt.Errorf("sqlite init schema: create trigger %s: %w", name, err)
			}
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

// ListRefsWithMeta returns all refs for repo sorted by name ascending.
func (s *SQLite) ListRefsWithMeta(ctx context.Context, repo string) ([]RefInfo, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT name, sha, created_at FROM refs WHERE repo = ? ORDER BY name`, repo)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var refs []RefInfo
	for rows.Next() {
		var r RefInfo
		var sec int64 // unix seconds; NULL is impossible after InitSchema's backfill
		if err := rows.Scan(&r.Name, &r.SHA, &sec); err != nil {
			return nil, err
		}
		assertf(ValidSHA(r.SHA), "refs table holds malformed sha %q for %s/%s", r.SHA, repo, r.Name)
		r.CreatedAt = time.Unix(sec, 0).UTC()
		refs = append(refs, r)
	}
	return refs, rows.Err()
}

// GetRef returns the current sha of one ref, or ErrNotFound.
func (s *SQLite) GetRef(ctx context.Context, repo, name string) (string, error) {
	assertf(repo != "" && name != "", "GetRef: empty repo %q or name %q", repo, name)
	var sha string
	err := s.db.QueryRowContext(ctx,
		`SELECT sha FROM refs WHERE repo = ? AND name = ?`, repo, name).Scan(&sha)
	if errors.Is(err, sql.ErrNoRows) {
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
// ref state is impossible by construction. A won swap also bumps the repo
// generation via the InitSchema-installed triggers — inside the same
// implicit transaction, so the statement stays the single unit of commit
// and the IdempotentCAS ambiguity classification (idempotent.go) is
// unchanged.
//
// Under WithSyncClaimFence (ref.go) the statement additionally requires
// the repo's sync claim to be held by the fencing holder: SQLite's
// single-writer lock serializes the fenced CAS against a concurrent claim
// steal, so a stolen run's update can never commit after the thief's
// claim (issue #86). Requires the shared local database (meta + ref in
// one --data-dir file).
func (s *SQLite) CASRef(ctx context.Context, repo, name, old, new string) (won bool, err error) {
	assertf(ValidSHA(old), "CASRef: malformed old sha %q", old)
	assertf(ValidSHA(new), "CASRef: malformed new sha %q", new)
	assertf(old != ZeroSHA || new != ZeroSHA, "CASRef: zero -> zero update for %s/%s", repo, name)

	// Acquire explicitly so acquisition failures are distinguishable from
	// statement failures: the statement provably never executed, so
	// IdempotentCAS must not recover such errors (see notExecuted).
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return false, fmt.Errorf("cas acquire %s/%s: %w", repo, name, notExecuted(err))
	}
	defer func() { _ = conn.Close() }()
	// Pre-dispatch guard: a context canceled after acquire but before
	// ExecContext dispatches the statement is definitive non-execution too.
	// (The residual race — cancellation landing during driver execution —
	// surfaces as a *sqlite.Error interrupt, which the ambiguity classifier
	// already treats as a definitive backend rejection.)
	if cerr := ctx.Err(); cerr != nil {
		return false, fmt.Errorf("cas %s/%s: %w", repo, name, notExecuted(cerr))
	}

	fence, fenced := casFence(ctx, repo)
	var tag string
	var res sql.Result
	switch {
	case old == ZeroSHA && !fenced: // create
		// created_at set explicitly (not via column default): databases
		// migrated by the InitSchema ALTER have no default on the column.
		// SHA updates below never touch it.
		res, err = conn.ExecContext(ctx,
			`INSERT INTO refs (repo, name, sha, created_at) VALUES (?, ?, ?, unixepoch()) ON CONFLICT DO NOTHING`,
			repo, name, new)
		tag = "create"
	case old == ZeroSHA: // fenced create
		res, err = conn.ExecContext(ctx,
			`INSERT INTO refs (repo, name, sha, created_at)
			 SELECT ?, ?, ?, unixepoch()
			 WHERE EXISTS (SELECT 1 FROM repo_upstreams WHERE repo_id = ? AND sync_claim_holder = ?)
			 ON CONFLICT DO NOTHING`,
			repo, name, new, repo, fence.holder)
		tag = "fenced create"
	case new == ZeroSHA && !fenced: // delete
		res, err = conn.ExecContext(ctx,
			`DELETE FROM refs WHERE repo = ? AND name = ? AND sha = ?`,
			repo, name, old)
		tag = "delete"
	case new == ZeroSHA: // fenced delete
		res, err = conn.ExecContext(ctx,
			`DELETE FROM refs WHERE repo = ? AND name = ? AND sha = ?
			 AND EXISTS (SELECT 1 FROM repo_upstreams WHERE repo_id = ? AND sync_claim_holder = ?)`,
			repo, name, old, repo, fence.holder)
		tag = "fenced delete"
	case !fenced: // update
		res, err = conn.ExecContext(ctx,
			`UPDATE refs SET sha = ? WHERE repo = ? AND name = ? AND sha = ?`,
			new, repo, name, old)
		tag = "update"
	default: // fenced update
		res, err = conn.ExecContext(ctx,
			`UPDATE refs SET sha = ? WHERE repo = ? AND name = ? AND sha = ?
			 AND EXISTS (SELECT 1 FROM repo_upstreams WHERE repo_id = ? AND sync_claim_holder = ?)`,
			new, repo, name, old, repo, fence.holder)
		tag = "fenced update"
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
// re-pushed identical pack has the same id. The transaction settles the
// quota ledger exactly like the PG twin (see its doc): reservations are
// consumed by key, and newly inserted non-retained rows credit the usage
// counter.
func (s *SQLite) RegisterPacks(ctx context.Context, repo string, packs []Pack) error {
	assertf(len(packs) > 0, "RegisterPacks called with no packs")
	tx, err := s.db.BeginTx(ctx, nil) // BEGIN IMMEDIATE via _txlock
	if err != nil {
		return fmt.Errorf("register packs begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	// Purge fence (issue #52): the single-writer transaction orders this
	// read against FenceRepoPurge's insert — see the PG twin.
	if fenced, err := repoPurgeFencedSQLite(ctx, tx, repo); err != nil {
		return err
	} else if fenced {
		return fmt.Errorf("register packs %s: %w", repo, ErrRepoPurged)
	}
	var credit int64
	adopted := false
	for _, p := range packs {
		assertf(p.ID != "" && p.Key != "", "RegisterPacks: empty pack id/key %+v", p)
		res, err := tx.ExecContext(ctx,
			`INSERT INTO packs (repo, id, s3_key) VALUES (?, ?, ?) ON CONFLICT DO NOTHING`,
			repo, p.ID, p.Key)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n > 0 {
			c, err := registerLedgerSQLite(ctx, tx, repo, p)
			if err != nil {
				return err
			}
			credit += c
			adopted = adopted || (p.Adopted && c > 0)
		}
		// Settlement consumes BOTH era spellings — see the PG twin.
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM pack_reservations WHERE key IN (?, ?)`, reservRowKey(p.Key), p.Key); err != nil {
			return err
		}
	}
	if adopted {
		// Atomic with the credit — see the PG twin (Codex round 53).
		if _, err := tx.ExecContext(ctx, `
INSERT INTO lfs_adoption_gens (repo, gen) VALUES (?, 1)
ON CONFLICT (repo) DO UPDATE SET gen = lfs_adoption_gens.gen + 1`, baseRepoOf(repo)); err != nil {
			return err
		}
	}
	if err := creditUsageSQLite(ctx, tx, repo, credit); err != nil {
		return err
	}
	return tx.Commit()
}

// sqliteDeleteChunk bounds one DELETE ... IN (...) statement's host
// parameters during ReplacePacks: an old-pack set larger than SQLite's
// variable limit (32766 by default; 999 in older builds) would otherwise
// fail EVERY fold of a large backlog — and since later triggers submit the
// same oversized set, repacking could never recover. Chunks execute inside
// the one transaction, so atomicity and the rowcount CAS are unaffected.
const sqliteDeleteChunk = 500

// ReplacePacks installs a repack fold in one transaction (see RefStore and
// the PG implementation, whose semantics this mirrors byte-for-byte: the
// delete-rowcount is the CAS precondition — summed across bounded delete
// chunks here — the insert tolerates an existing identical (repo, id) row,
// and a lost race rolls back with won=false).
func (s *SQLite) ReplacePacks(ctx context.Context, repo string, old []Pack, newPack Pack) (won bool, err error) {
	validateReplacePacks(repo, old, newPack)
	tx, err := s.db.BeginTx(ctx, nil) // BEGIN IMMEDIATE via _txlock
	if err != nil {
		return false, fmt.Errorf("replace packs begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	deleted := 0
	for chunkStart := 0; chunkStart < len(old); chunkStart += sqliteDeleteChunk {
		chunk := old[chunkStart:min(chunkStart+sqliteDeleteChunk, len(old))]
		args := make([]any, 0, len(chunk)+1)
		args = append(args, repo)
		for _, p := range chunk {
			args = append(args, p.ID)
		}
		res, err := tx.ExecContext(ctx,
			`DELETE FROM packs WHERE repo = ? AND id IN (?`+strings.Repeat(",?", len(chunk)-1)+`)`, args...)
		if err != nil {
			return false, fmt.Errorf("replace packs %s: %w", repo, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return false, fmt.Errorf("replace packs %s rows: %w", repo, err)
		}
		deleted += int(n)
	}
	if deleted != len(old) {
		return false, nil // another fold got here first: roll back via defer
	}
	// Ledger (issue #31): mirror the PG twin — record superseded keys as
	// retained (their blobs linger; bytes stay counted), then credit the
	// new pack only when its row is newly inserted and not itself retained.
	for _, p := range old {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO pack_retained (repo, key, bytes) VALUES (?, ?, ?)
ON CONFLICT (repo, key) DO UPDATE SET bytes = excluded.bytes, superseded_at = unixepoch()`,
			repo, p.Key, p.Size); err != nil {
			return false, fmt.Errorf("replace packs %s: %w", repo, err)
		}
	}
	res, err := tx.ExecContext(ctx,
		`INSERT INTO packs (repo, id, s3_key) VALUES (?, ?, ?) ON CONFLICT DO NOTHING`,
		repo, newPack.ID, newPack.Key)
	if err != nil {
		return false, fmt.Errorf("replace packs %s: %w", repo, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("replace packs %s rows: %w", repo, err)
	}
	if n > 0 {
		credit, err := registerLedgerSQLite(ctx, tx, repo, newPack)
		if err != nil {
			return false, fmt.Errorf("replace packs %s: %w", repo, err)
		}
		if err := creditUsageSQLite(ctx, tx, repo, credit); err != nil {
			return false, fmt.Errorf("replace packs %s: %w", repo, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("replace packs %s commit: %w", repo, err)
	}
	return true, nil
}

// HasPack reports whether repo has a registered pack with blob key.
func (s *SQLite) HasPack(ctx context.Context, repo, key string) (bool, error) {
	assertf(repo != "" && key != "", "HasPack(%q, %q)", repo, key)
	var exists bool
	err := s.db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM packs WHERE repo = ? AND s3_key = ?)`, repo, key).Scan(&exists)
	return exists, err
}

// GetGeneration returns repo's mutation generation (0 = never mutated).
func (s *SQLite) GetGeneration(ctx context.Context, repo string) (int64, error) {
	assertf(repo != "", "GetGeneration: empty repo")
	var gen int64
	err := s.db.QueryRowContext(ctx,
		`SELECT generation FROM repo_generations WHERE repo = ?`, repo).Scan(&gen)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil // never mutated: the first bump creates the row at 1
	}
	if err != nil {
		return 0, err
	}
	assertf(gen >= 1, "repo_generations holds non-positive generation %d for %s", gen, repo)
	return gen, nil
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
