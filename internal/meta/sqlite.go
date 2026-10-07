// SQLite implementation of Backend for local mode: the serve process and
// re-exec'd hook processes share one DB file on the same host, so the DSN
// forces WAL + busy_timeout, and _txlock=immediate turns every explicit
// transaction into BEGIN IMMEDIATE (no deferred→write lock upgrade
// deadlocks) — that write-serialization is what replaces PG's
// SELECT ... FOR UPDATE row lock in UpsertOrgWithKeys.
//
// Semantics mirror pg.go 1:1: same sentinels, soft delete with the 404/409
// distinction, UNIQUE(org_id, url) spanning soft-deleted rows, and the same
// opaque cursors. Timestamps are stored as INTEGER unixnano set Go-side (not
// SQL defaults) so the "<unixnano>|<repo_id>" cursor round-trips exactly and
// the keyset (created_at, repo_id) < (?, ?) row-value comparison maps onto
// the stored integers with no precision loss.

package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	"github.com/coder/openstorage/internal/secrets"
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

// SQLite implements Backend on one local SQLite file via the pure-Go
// modernc.org/sqlite driver (cgo-free cross-compiled single binaries).
type SQLite struct {
	db *sql.DB
	// codec, when non-nil, envelope-encrypts the secret columns
	// (webhooks.secret, git_credentials.password) — see secrets.go.
	codec *secrets.Codec
	// now, when non-nil, replaces time.Now for every stored timestamp and
	// the sync-claim lease expiry comparison (WithNowFunc; the DST harness
	// injects its logical clock here).
	now func() time.Time
}

var _ Backend = (*SQLite)(nil)

// OpenSQLite opens sqlite:///abs/path/db.sqlite. The path is everything
// after "sqlite://" (relative paths are tolerated; --data-dir always derives
// absolute ones). Parent directories are created; the DB file itself is
// created by the driver on first open.
func OpenSQLite(ctx context.Context, sqliteURL string, opts ...Option) (*SQLite, error) {
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
	o := resolveOptions(opts)
	return &SQLite{db: db, codec: o.codec, now: o.now}, nil
}

func (s *SQLite) Close() { _ = s.db.Close() }

// InitSchema creates the metadata tables (idempotent). No advisory lock
// (unlike PG): SQLite's file lock + busy_timeout serialize concurrent
// startups, and CREATE TABLE IF NOT EXISTS is race-free under that lock.
func (s *SQLite) InitSchema(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite init begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	// Column names match pg.go so the row SQL stays byte-comparable; all
	// timestamps are INTEGER unixnano written Go-side (see file comment).
	// AUTOINCREMENT keeps ids monotonic like PG identity columns, so
	// ORDER BY id remains registration order even after deletes.
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS orgs (
		    id         INTEGER PRIMARY KEY AUTOINCREMENT,
		    name       TEXT NOT NULL UNIQUE,
		    created_at INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS org_public_keys (
		    id         INTEGER PRIMARY KEY AUTOINCREMENT,
		    org_id     INTEGER NOT NULL REFERENCES orgs(id) ON DELETE CASCADE,
		    pem        TEXT NOT NULL,
		    created_at INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS org_public_keys_org_idx ON org_public_keys (org_id)`,
		// Commit-signing keys for verify-sig (docs/reference/authentication.md): SSH public key lines
		// or ASCII-armored PGP public key blocks, read fresh on every push.
		`CREATE TABLE IF NOT EXISTS org_signing_keys (
		    id         INTEGER PRIMARY KEY AUTOINCREMENT,
		    org_id     INTEGER NOT NULL REFERENCES orgs(id) ON DELETE CASCADE,
		    key_text   TEXT NOT NULL,
		    created_at INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS org_signing_keys_org_idx ON org_signing_keys (org_id)`,
		`CREATE TABLE IF NOT EXISTS repos (
		    repo_id        TEXT PRIMARY KEY,
		    org_id         INTEGER NOT NULL REFERENCES orgs(id),
		    url            TEXT NOT NULL,
		    default_branch TEXT NOT NULL,
		    created_at     INTEGER NOT NULL,
		    deleted_at     INTEGER,
		    purged_at      INTEGER,
		    CONSTRAINT repos_org_id_url_key UNIQUE (org_id, url)
		)`,
		// Keyset pagination index for ListRepos (live rows only).
		`CREATE INDEX IF NOT EXISTS repos_org_created_idx
		    ON repos (org_id, created_at DESC, repo_id DESC) WHERE deleted_at IS NULL`,
		// Phase-5 eventing/sync tables (eventing.go): webhook registrations,
		// git credentials, and repo upstream links. syncing is an INTEGER
		// 0/1 boolean; counters and flags are written Go-side like the
		// timestamps.
		`CREATE TABLE IF NOT EXISTS webhooks (
		    id         TEXT PRIMARY KEY,
		    org_id     INTEGER NOT NULL REFERENCES orgs(id) ON DELETE CASCADE,
		    url        TEXT NOT NULL,
		    secret     TEXT NOT NULL,
		    created_at INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS webhooks_org_idx ON webhooks (org_id)`,
		`CREATE TABLE IF NOT EXISTS git_credentials (
		    id         TEXT PRIMARY KEY,
		    repo_id    TEXT NOT NULL,
		    username   TEXT NOT NULL,
		    password   TEXT NOT NULL,
		    created_at INTEGER NOT NULL,
		    CONSTRAINT git_credentials_repo_id_key UNIQUE (repo_id)
		)`,
		`CREATE TABLE IF NOT EXISTS repo_upstreams (
		    repo_id       TEXT PRIMARY KEY,
		    provider      TEXT NOT NULL,
		    owner         TEXT NOT NULL,
		    name          TEXT NOT NULL,
		    upstream_host TEXT NOT NULL,
		    run_count     INTEGER NOT NULL,
		    success_count INTEGER NOT NULL,
		    syncing       INTEGER NOT NULL,
		    sync_pending  INTEGER NOT NULL DEFAULT 0,
		    sync_claim_holder     TEXT NOT NULL DEFAULT '',
		    sync_claim_expires_at INTEGER NOT NULL DEFAULT 0,
		    forward_on_push       INTEGER NOT NULL DEFAULT 0,
		    push_pending          INTEGER NOT NULL DEFAULT 0,
		    push_syncing          INTEGER NOT NULL DEFAULT 0,
		    push_claim_holder     TEXT NOT NULL DEFAULT '',
		    push_claim_expires_at INTEGER NOT NULL DEFAULT 0,
		    push_claim_force      INTEGER NOT NULL DEFAULT 0,
		    push_run_count        INTEGER NOT NULL DEFAULT 0,
		    push_success_count    INTEGER NOT NULL DEFAULT 0,
		    push_attempt          INTEGER NOT NULL DEFAULT 0,
		    push_due              INTEGER NOT NULL DEFAULT 0,
		    sync_due              INTEGER NOT NULL DEFAULT 0,
		    created_at    INTEGER NOT NULL
		)`,
	} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("sqlite init schema: %w", err)
		}
	}
	// Upgrade path for data dirs whose repo_upstreams predates the
	// sync_pending (#79) or claim-lease (#86) columns (SQLite has no ADD
	// COLUMN IF NOT EXISTS). Claims left at the empty-holder/zero-expiry
	// defaults are LEGACY claims: TryBeginSync adopts them under one
	// grace period before they become stealable (pre-existing wedges
	// heal one grace after the first post-upgrade sync attempt).
	for _, col := range []struct{ table, name, ddl string }{
		{"repo_upstreams", "sync_pending", `sync_pending INTEGER NOT NULL DEFAULT 0`},
		{"repo_upstreams", "sync_claim_holder", `sync_claim_holder TEXT NOT NULL DEFAULT ''`},
		{"repo_upstreams", "sync_claim_expires_at", `sync_claim_expires_at INTEGER NOT NULL DEFAULT 0`},
		// Outbound push-forwarding lane (issue #90) for data dirs
		// predating it: pending level 0/1/2, claim lease, counters.
		{"repo_upstreams", "forward_on_push", `forward_on_push INTEGER NOT NULL DEFAULT 0`},
		{"repo_upstreams", "push_pending", `push_pending INTEGER NOT NULL DEFAULT 0`},
		{"repo_upstreams", "push_syncing", `push_syncing INTEGER NOT NULL DEFAULT 0`},
		{"repo_upstreams", "push_claim_holder", `push_claim_holder TEXT NOT NULL DEFAULT ''`},
		{"repo_upstreams", "push_claim_expires_at", `push_claim_expires_at INTEGER NOT NULL DEFAULT 0`},
		{"repo_upstreams", "push_claim_force", `push_claim_force INTEGER NOT NULL DEFAULT 0`},
		{"repo_upstreams", "push_run_count", `push_run_count INTEGER NOT NULL DEFAULT 0`},
		{"repo_upstreams", "push_success_count", `push_success_count INTEGER NOT NULL DEFAULT 0`},
		// Local schedule state (issue #266, ADR-50): retry position and
		// due times owned by the row. 0 means "first attempt, due now",
		// so pending work recorded before the upgrade is due at once.
		{"repo_upstreams", "push_attempt", `push_attempt INTEGER NOT NULL DEFAULT 0`},
		{"repo_upstreams", "push_due", `push_due INTEGER NOT NULL DEFAULT 0`},
		{"repo_upstreams", "sync_due", `sync_due INTEGER NOT NULL DEFAULT 0`},
		// Purge tombstone column (issue #52) for data dirs predating it.
		{"repos", "purged_at", `purged_at INTEGER`},
		// Create state (issue #388): 0 legacy, 1 pending, 2 complete.
		{"repos", "create_state", `create_state INTEGER NOT NULL DEFAULT 0`},
	} {
		var present int
		if err := tx.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?`, col.table, col.name,
		).Scan(&present); err != nil {
			return fmt.Errorf("sqlite init schema: probe %s column: %w", col.name, err)
		}
		if present == 0 {
			if _, err := tx.ExecContext(ctx,
				`ALTER TABLE `+col.table+` ADD COLUMN `+col.ddl); err != nil {
				return fmt.Errorf("sqlite init schema: add %s column: %w", col.name, err)
			}
		}
	}
	// Retention-purge scan index (issue #52): PENDING purges only — see
	// the PG twin's rationale. Created after the column upgrades above
	// (the predicate needs purged_at on pre-upgrade data dirs).
	for _, stmt := range []string{
		`DROP INDEX IF EXISTS repos_deleted_idx`,
		`CREATE INDEX IF NOT EXISTS repos_purgeable_idx
		    ON repos (deleted_at, repo_id) WHERE deleted_at IS NOT NULL AND purged_at IS NULL`,
	} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("sqlite init schema: purgeable index: %w", err)
		}
	}
	return tx.Commit()
}

// nowNano returns the store's current instant as stored (unixnano) plus its
// exact time.Time rendering, so returned rows byte-match later reads. Reads
// the injected clock when set (WithNowFunc), time.Now otherwise.
func (s *SQLite) nowNano() (int64, time.Time) {
	now := time.Now()
	if s.now != nil {
		now = s.now()
	}
	nanos := now.UnixNano()
	return nanos, time.Unix(0, nanos).UTC()
}

// isUniqueViolation reports whether err is SQLITE_CONSTRAINT_UNIQUE (the
// modernc analogue of PG SQLSTATE 23505).
func isUniqueViolation(err error) bool {
	var serr *sqlite.Error
	return errors.As(err, &serr) && serr.Code() == sqlite3.SQLITE_CONSTRAINT_UNIQUE
}

// lookupOrgID resolves an org name to its id; ErrNotFound for unknown orgs.
func (s *SQLite) lookupOrgID(ctx context.Context, name string) (int64, error) {
	assertf(name != "", "lookupOrgID: empty org name")
	var id int64
	err := s.db.QueryRowContext(ctx, `SELECT id FROM orgs WHERE name = ?`, name).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("org %q: %w", name, ErrNotFound)
	}
	if err != nil {
		return 0, fmt.Errorf("lookup org %q: %w", name, err)
	}
	assertf(id > 0, "org %q resolved to non-positive id %d", name, id)
	return id, nil
}

// sqlRow is the shared Scan surface of *sql.Row and *sql.Rows.
type sqlRow interface{ Scan(dest ...any) error }

// scanRepoSQLite scans one row of repoCols, converting the INTEGER unixnano
// timestamps back to UTC time.Time per the Repo contract.
func scanRepoSQLite(row sqlRow) (Repo, error) {
	var r Repo
	var createdAt int64
	var deletedAt sql.NullInt64
	if err := row.Scan(&r.RepoID, &r.OrgID, &r.URL, &r.DefaultBranch, &createdAt, &deletedAt, &r.CreateState); err != nil {
		return Repo{}, err
	}
	r.CreateState = knownCreateState(r.RepoID, r.CreateState)
	r.CreatedAt = time.Unix(0, createdAt).UTC()
	if deletedAt.Valid {
		utc := time.Unix(0, deletedAt.Int64).UTC()
		r.DeletedAt = &utc
	}
	return r, nil
}

// UpsertOrgWithKeys idempotently creates the org and replaces its registered
// PEM public keys (startup bootstrap in single-org mode).
//
// PG locks the org row (SELECT ... FOR UPDATE) so concurrent upserts
// serialize; here _txlock=immediate makes the whole transaction a write
// transaction from BEGIN, and SQLite allows only one writer at a time, so
// the DELETE/INSERT replacement is last-writer-wins by construction.
func (s *SQLite) UpsertOrgWithKeys(ctx context.Context, name string, pemPublicKeys []string) (Org, error) {
	assertf(name != "", "UpsertOrgWithKeys: empty org name")
	assertf(len(pemPublicKeys) > 0, "UpsertOrgWithKeys: org %q needs at least one public key", name)
	for i, pem := range pemPublicKeys {
		assertf(strings.TrimSpace(pem) != "", "UpsertOrgWithKeys: blank PEM block %d for org %q", i, name)
	}
	nanos, _ := s.nowNano()
	var org Org
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO orgs (name, created_at) VALUES (?, ?) ON CONFLICT (name) DO NOTHING`,
			name, nanos); err != nil {
			return err
		}
		var createdAt int64
		if err := tx.QueryRowContext(ctx,
			`SELECT id, name, created_at FROM orgs WHERE name = ?`, name).
			Scan(&org.ID, &org.Name, &createdAt); err != nil {
			return err
		}
		org.CreatedAt = time.Unix(0, createdAt).UTC()
		if _, err := tx.ExecContext(ctx, `DELETE FROM org_public_keys WHERE org_id = ?`, org.ID); err != nil {
			return err
		}
		for _, pem := range pemPublicKeys {
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO org_public_keys (org_id, pem, created_at) VALUES (?, ?, ?)`,
				org.ID, pem, nanos); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return Org{}, fmt.Errorf("upsert org %q: %w", name, err)
	}
	return org, nil
}

// inTx runs fn inside one BEGIN IMMEDIATE transaction (via _txlock),
// committing on nil and rolling back on error.
func (s *SQLite) inTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// PublicKeysForOrg returns the org's registered PEM public key blocks in
// registration order.
func (s *SQLite) PublicKeysForOrg(ctx context.Context, name string) ([]string, error) {
	orgID, err := s.lookupOrgID(ctx, name)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT pem FROM org_public_keys WHERE org_id = ? ORDER BY id`, orgID)
	if err != nil {
		return nil, fmt.Errorf("keys for org %q: %w", name, err)
	}
	defer rows.Close()
	var pems []string
	for rows.Next() {
		var pem string
		if err := rows.Scan(&pem); err != nil {
			return nil, err
		}
		pems = append(pems, pem)
	}
	return pems, rows.Err()
}

// OrgNames returns every org row's name, ordered by name.
func (s *SQLite) OrgNames(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT name FROM orgs ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list org names: %w", err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

// CreateRepo inserts a live repo with a server-generated RepoID.
// ErrAlreadyExists when (org, url) is taken — even by a soft-deleted row.
func (s *SQLite) CreateRepo(ctx context.Context, org, url, defaultBranch string) (Repo, error) {
	assertf(url != "", "CreateRepo: empty url for org %q", org)
	assertf(defaultBranch != "", "CreateRepo: empty default branch for %q (caller defaults to main)", url)
	orgID, err := s.lookupOrgID(ctx, org)
	if err != nil {
		return Repo{}, err
	}
	return s.insertRepo(ctx, s.db, org, orgID, url, defaultBranch, CreateStateLegacy)
}

// CreateRepoWithState is CreateRepo with an explicit create state.
func (s *SQLite) CreateRepoWithState(ctx context.Context, org, url, defaultBranch string, state CreateState) (Repo, error) {
	assertf(url != "" && defaultBranch != "", "CreateRepoWithState: empty url %q or default branch %q", url, defaultBranch)
	assertf(state == CreateStatePending || state == CreateStateComplete, "CreateRepoWithState: state %d", state)
	orgID, err := s.lookupOrgID(ctx, org)
	if err != nil {
		return Repo{}, err
	}
	return s.insertRepo(ctx, s.db, org, orgID, url, defaultBranch, state)
}

// CompleteRepoCreate moves a pending repo to complete (meta.Store contract).
func (s *SQLite) CompleteRepoCreate(ctx context.Context, org, repoID string) (CreateOutcome, error) {
	return s.completeRepoCreate(ctx, org, repoID, false)
}

// CompleteSyncRepoCreate is CompleteRepoCreate for a sync create (issue
// #388): when the row is pending it also records the first-sync request
// (sync_pending, which the local schedule's sweep runs) in the same
// write. A complete or deleted row records nothing.
func (s *SQLite) CompleteSyncRepoCreate(ctx context.Context, org, repoID string) (CreateOutcome, error) {
	return s.completeRepoCreate(ctx, org, repoID, true)
}

func (s *SQLite) completeRepoCreate(ctx context.Context, org, repoID string, requestSync bool) (CreateOutcome, error) {
	assertf(repoID != "", "CompleteRepoCreate: empty repo id for org %q", org)
	orgID, err := s.lookupOrgID(ctx, org)
	if err != nil {
		return 0, err
	}
	var out CreateOutcome
	err = s.inTx(ctx, func(tx *sql.Tx) error { // BEGIN IMMEDIATE: one writer
		r, err := scanRepoSQLite(tx.QueryRowContext(ctx,
			`SELECT `+repoCols+` FROM repos WHERE org_id = ? AND repo_id = ?`, orgID, repoID))
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("repo id %q in org %q: %w", repoID, org, ErrNotFound)
		}
		if err != nil {
			return fmt.Errorf("read repo id %q: %w", repoID, err)
		}
		if r.DeletedAt != nil || r.CreateState != CreateStatePending {
			out, err = completeOutcome(r)
			return err
		}
		if requestSync {
			res, err := tx.ExecContext(ctx, `UPDATE repo_upstreams SET sync_pending = 1 WHERE repo_id = ?`, repoID)
			if err != nil {
				return fmt.Errorf("request first sync for repo %q: %w", repoID, err)
			}
			if n, err := res.RowsAffected(); err != nil || n != 1 {
				return fmt.Errorf("request first sync for repo %q: %d upstream rows, %v: %w", repoID, n, err, ErrNotFound)
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE repos SET create_state = ? WHERE repo_id = ?`,
			CreateStateComplete, repoID); err != nil {
			return fmt.Errorf("complete create of repo id %q: %w", repoID, err)
		}
		out = CreateCompleted
		return nil
	})
	return out, err
}

// sqliteExecer is the ExecContext method *sql.DB and *sql.Tx share, so an
// insert runs alone or inside CreateLinkedRepo's transaction.
type sqliteExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func (s *SQLite) insertRepo(ctx context.Context, q sqliteExecer, org string, orgID int64, url, defaultBranch string, state CreateState) (Repo, error) {
	nanos, createdAt := s.nowNano()
	r := Repo{RepoID: newRepoID(), OrgID: orgID, URL: url, DefaultBranch: defaultBranch, CreatedAt: createdAt, CreateState: state}
	_, err := q.ExecContext(ctx,
		`INSERT INTO repos (repo_id, org_id, url, default_branch, created_at, create_state) VALUES (?, ?, ?, ?, ?, ?)`,
		r.RepoID, orgID, url, defaultBranch, nanos, state)
	// A unique violation here can only be (org_id, url): a repo_id collision
	// would be SQLITE_CONSTRAINT_PRIMARYKEY, a different extended code.
	if isUniqueViolation(err) {
		return Repo{}, fmt.Errorf("repo %q in org %q: %w", url, org, ErrAlreadyExists)
	}
	if err != nil {
		return Repo{}, fmt.Errorf("create repo %q in org %q: %w", url, org, err)
	}
	return r, nil
}

// GetRepoByURL returns the live repo with the given url; ErrNotFound when
// absent or soft-deleted.
func (s *SQLite) GetRepoByURL(ctx context.Context, org, url string) (Repo, error) {
	assertf(url != "", "GetRepoByURL: empty url for org %q", org)
	orgID, err := s.lookupOrgID(ctx, org)
	if err != nil {
		return Repo{}, err
	}
	r, err := scanRepoSQLite(s.db.QueryRowContext(ctx,
		`SELECT `+repoCols+` FROM repos WHERE org_id = ? AND url = ? AND deleted_at IS NULL`,
		orgID, url))
	if errors.Is(err, sql.ErrNoRows) {
		return Repo{}, fmt.Errorf("repo %q in org %q: %w", url, org, ErrNotFound)
	}
	if err != nil {
		return Repo{}, fmt.Errorf("get repo %q in org %q: %w", url, org, err)
	}
	assertf(r.DeletedAt == nil, "live-repo query returned soft-deleted row %q", r.RepoID)
	return r, nil
}

// GetRepoByID returns the live repo with the given RepoID; ErrNotFound when
// absent or soft-deleted.
func (s *SQLite) GetRepoByID(ctx context.Context, org, repoID string) (Repo, error) {
	assertf(repoID != "", "GetRepoByID: empty repoID for org %q", org)
	orgID, err := s.lookupOrgID(ctx, org)
	if err != nil {
		return Repo{}, err
	}
	r, err := scanRepoSQLite(s.db.QueryRowContext(ctx,
		`SELECT `+repoCols+` FROM repos WHERE org_id = ? AND repo_id = ? AND deleted_at IS NULL`,
		orgID, repoID))
	if errors.Is(err, sql.ErrNoRows) {
		return Repo{}, fmt.Errorf("repo id %q in org %q: %w", repoID, org, ErrNotFound)
	}
	if err != nil {
		return Repo{}, fmt.Errorf("get repo id %q in org %q: %w", repoID, org, err)
	}
	assertf(r.DeletedAt == nil, "live-repo query returned soft-deleted row %q", r.RepoID)
	return r, nil
}

// UpdateRepoDefaultBranch CASes the live repo's default branch (Store
// contract). One UPDATE carries the whole CAS; zero rows affected then
// splits inside the same write transaction (BEGIN IMMEDIATE) into "no live
// repo" (ErrNotFound) and "default branch changed concurrently" (won=false).
func (s *SQLite) UpdateRepoDefaultBranch(ctx context.Context, org, url, oldBranch, newBranch string) (bool, error) {
	assertf(url != "", "UpdateRepoDefaultBranch: empty url for org %q", org)
	assertf(oldBranch != "" && newBranch != "",
		"UpdateRepoDefaultBranch: empty branch for %q (old %q, new %q)", url, oldBranch, newBranch)
	orgID, err := s.lookupOrgID(ctx, org)
	if err != nil {
		return false, err
	}
	won := false
	err = s.inTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`UPDATE repos SET default_branch = ?
			 WHERE org_id = ? AND url = ? AND deleted_at IS NULL AND default_branch = ?`,
			newBranch, orgID, url, oldBranch)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		assertf(n == 0 || n == 1, "default-branch CAS of %q/%q affected %d rows", org, url, n)
		if n == 1 {
			won = true
			return nil
		}
		// No row matched: absent/soft-deleted repo (404) or a concurrently
		// changed default branch (won=false -> caller's 409).
		var one int
		err = tx.QueryRowContext(ctx,
			`SELECT 1 FROM repos WHERE org_id = ? AND url = ? AND deleted_at IS NULL`,
			orgID, url).Scan(&one)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	})
	if err != nil {
		return false, fmt.Errorf("update default branch of %q in org %q: %w", url, org, err)
	}
	return won, nil
}

// SoftDeleteRepo sets deleted_at and returns the affected repo. ErrNotFound
// when no row exists; ErrAlreadyDeleted when already soft-deleted.
func (s *SQLite) SoftDeleteRepo(ctx context.Context, org, url string) (Repo, error) {
	assertf(url != "", "SoftDeleteRepo: empty url for org %q", org)
	orgID, err := s.lookupOrgID(ctx, org)
	if err != nil {
		return Repo{}, err
	}
	nanos, _ := s.nowNano()
	var repo Repo
	err = s.inTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`UPDATE repos SET deleted_at = ?
			 WHERE org_id = ? AND url = ? AND deleted_at IS NULL`, nanos, orgID, url)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		assertf(n == 0 || n == 1, "soft delete of %q/%q affected %d rows", org, url, n)
		if n == 1 {
			r, err := scanRepoSQLite(tx.QueryRowContext(ctx,
				`SELECT `+repoCols+` FROM repos WHERE org_id = ? AND url = ?`, orgID, url))
			if err != nil {
				return err
			}
			assertf(r.DeletedAt != nil, "soft delete of %q returned nil deleted_at", r.RepoID)
			repo = r
			return nil
		}
		// No live row matched: distinguish already-deleted (409) from absent
		// (404). The transaction is a write transaction (BEGIN IMMEDIATE), so
		// no live row can appear between the two statements.
		var deletedAt sql.NullInt64
		err = tx.QueryRowContext(ctx,
			`SELECT deleted_at FROM repos WHERE org_id = ? AND url = ?`, orgID, url).Scan(&deletedAt)
		if errors.Is(err, sql.ErrNoRows) || (err == nil && !deletedAt.Valid) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		return ErrAlreadyDeleted
	})
	if err != nil {
		return Repo{}, fmt.Errorf("delete repo %q in org %q: %w", url, org, err)
	}
	return repo, nil
}

// AddSigningKey registers a commit-signing key for the org and returns the
// stored row. Format validation (SSH line / armored PGP block) is the
// caller's responsibility per the Store contract; the text is stored
// verbatim.
func (s *SQLite) AddSigningKey(ctx context.Context, org, keyText string) (SigningKey, error) {
	assertf(strings.TrimSpace(keyText) != "", "AddSigningKey: blank key text for org %q", org)
	orgID, err := s.lookupOrgID(ctx, org)
	if err != nil {
		return SigningKey{}, err
	}
	nanos, createdAt := s.nowNano()
	k := SigningKey{OrgID: orgID, KeyText: keyText, CreatedAt: createdAt}
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO org_signing_keys (org_id, key_text, created_at) VALUES (?, ?, ?)`,
		orgID, keyText, nanos)
	if err != nil {
		return SigningKey{}, fmt.Errorf("add signing key for org %q: %w", org, err)
	}
	k.ID, err = res.LastInsertId()
	if err != nil {
		return SigningKey{}, fmt.Errorf("add signing key for org %q: %w", org, err)
	}
	assertf(k.ID > 0, "signing key insert returned non-positive id %d", k.ID)
	return k, nil
}

// SigningKeysForOrg returns the org's registered signing keys in
// registration order; empty slice when none are registered.
func (s *SQLite) SigningKeysForOrg(ctx context.Context, org string) ([]SigningKey, error) {
	orgID, err := s.lookupOrgID(ctx, org)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, org_id, key_text, created_at FROM org_signing_keys
		 WHERE org_id = ? ORDER BY id`, orgID)
	if err != nil {
		return nil, fmt.Errorf("signing keys for org %q: %w", org, err)
	}
	defer rows.Close()
	keys := []SigningKey{}
	for rows.Next() {
		var k SigningKey
		var createdAt int64
		if err := rows.Scan(&k.ID, &k.OrgID, &k.KeyText, &createdAt); err != nil {
			return nil, err
		}
		k.CreatedAt = time.Unix(0, createdAt).UTC()
		keys = append(keys, k)
	}
	return keys, rows.Err()
}

// RemoveSigningKey deletes the signing key with the given ID. ErrNotFound
// when the org is unknown or the ID does not belong to it.
func (s *SQLite) RemoveSigningKey(ctx context.Context, org string, id int64) error {
	assertf(id > 0, "RemoveSigningKey: non-positive id %d for org %q", id, org)
	orgID, err := s.lookupOrgID(ctx, org)
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM org_signing_keys WHERE org_id = ? AND id = ?`, orgID, id)
	if err != nil {
		return fmt.Errorf("remove signing key %d for org %q: %w", id, org, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("remove signing key %d for org %q: %w", id, org, err)
	}
	if n == 0 {
		return fmt.Errorf("signing key %d for org %q: %w", id, org, ErrNotFound)
	}
	assertf(n == 1, "DELETE by primary key affected %d rows", n)
	return nil
}

// ListRepos pages live repos for the org, keyset-ordered by
// (created_at DESC, repo_id DESC).
//
// PG's `url ILIKE ?` becomes `lower(url) LIKE lower(?) ESCAPE '\'`. SQLite's
// lower() only folds ASCII, so non-ASCII urls match case-sensitively here
// while PG ILIKE folds per locale — an accepted divergence for local mode.
func (s *SQLite) ListRepos(ctx context.Context, org string, params ListParams) (ListResult, error) {
	// Limit is validated/clamped by the HTTP handler per the Store contract
	// (default 20, max 100); a value outside [1,100] is a caller bug.
	assertf(params.Limit >= 1 && params.Limit <= 100,
		"ListRepos: limit %d outside contract range [1,100]", params.Limit)
	orgID, err := s.lookupOrgID(ctx, org)
	if err != nil {
		return ListResult{}, err
	}

	query := `SELECT ` + repoCols + ` FROM repos WHERE org_id = ? AND deleted_at IS NULL`
	args := []any{orgID}
	if params.Q != "" {
		args = append(args, "%"+escapeLike(params.Q)+"%")
		query += ` AND lower(url) LIKE lower(?) ESCAPE '\'`
	}
	if params.Cursor != "" {
		createdAt, repoID, err := decodeCursor(params.Cursor)
		if err != nil {
			return ListResult{}, err
		}
		// The cursor's unixnano compares exactly against the stored INTEGER.
		args = append(args, createdAt.UnixNano(), repoID)
		query += ` AND (created_at, repo_id) < (?, ?)`
	}
	// Fetch one extra row to compute HasMore without a second query.
	args = append(args, params.Limit+1)
	query += ` ORDER BY created_at DESC, repo_id DESC LIMIT ?`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return ListResult{}, fmt.Errorf("list repos for org %q: %w", org, err)
	}
	defer rows.Close()
	repos := make([]Repo, 0, params.Limit+1)
	for rows.Next() {
		r, err := scanRepoSQLite(rows)
		if err != nil {
			return ListResult{}, err
		}
		repos = append(repos, r)
	}
	if err := rows.Err(); err != nil {
		return ListResult{}, err
	}
	assertf(len(repos) <= params.Limit+1, "LIMIT %d query returned %d rows", params.Limit+1, len(repos))

	res := ListResult{Repos: repos}
	if len(repos) > params.Limit {
		res.Repos = repos[:params.Limit]
		last := res.Repos[params.Limit-1]
		res.NextCursor = encodeCursor(last.CreatedAt, last.RepoID)
		res.HasMore = true
	}
	return res, nil
}
