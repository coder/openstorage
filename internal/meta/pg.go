package meta

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/coder/openstorage/internal/secrets"
)

// assertf is a fail-fast invariant check (crash-and-burn style).
func assertf(cond bool, format string, args ...any) {
	if !cond {
		panic(fmt.Sprintf("meta assertion failed: "+format, args...))
	}
}

const (
	// pgUniqueViolation is SQLSTATE 23505.
	pgUniqueViolation = "23505"
	// repoURLConstraint is the named UNIQUE(org_id, url) constraint; it spans
	// soft-deleted rows on purpose (docs/explanation/domain-model.md: re-create of a soft-deleted url
	// must 409).
	repoURLConstraint = "repos_org_id_url_key"
	// repoCols is the scan column list matching scanRepo.
	repoCols = "repo_id, org_id, url, default_branch, created_at, deleted_at, create_state"
)

// PG implements Backend on Postgres.
type PG struct {
	pool *pgxpool.Pool
	// codec, when non-nil, envelope-encrypts the secret columns
	// (webhooks.secret, git_credentials.password) — see secrets.go.
	codec *secrets.Codec
}

var _ Backend = (*PG)(nil)

// OpenPG connects to pgURL and pings the database.
func OpenPG(ctx context.Context, pgURL string, opts ...Option) (*PG, error) {
	assertf(pgURL != "", "OpenPG: empty PG URL")
	pool, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		return nil, fmt.Errorf("pg connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pg ping: %w", err)
	}
	return &PG{pool: pool, codec: resolveOptions(opts).codec}, nil
}

func (s *PG) Close() { s.pool.Close() }

// InitSchema creates the metadata tables (idempotent). The advisory lock
// serializes concurrent node startups: two racing CREATE TABLE IF NOT EXISTS
// can fail with a duplicate pg_type key error (same pattern as internal/ref).
func (s *PG) InitSchema(ctx context.Context) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(0x4D455441)`); err != nil { // "META"
			return err
		}
		_, err := tx.Exec(ctx, `
CREATE TABLE IF NOT EXISTS orgs (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name       text NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS org_public_keys (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id     bigint NOT NULL REFERENCES orgs(id) ON DELETE CASCADE,
    pem        text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS org_public_keys_org_idx ON org_public_keys (org_id);
-- Commit-signing keys for verify-sig (docs/02): SSH public key lines or
-- ASCII-armored PGP public key blocks, read fresh from PG on every push.
CREATE TABLE IF NOT EXISTS org_signing_keys (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id     bigint NOT NULL REFERENCES orgs(id) ON DELETE CASCADE,
    key_text   text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS org_signing_keys_org_idx ON org_signing_keys (org_id);
CREATE TABLE IF NOT EXISTS repos (
    repo_id        text PRIMARY KEY,
    org_id         bigint NOT NULL REFERENCES orgs(id),
    url            text NOT NULL,
    default_branch text NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    deleted_at     timestamptz,
    -- purged_at marks a completed storage purge (issue #52). The row is
    -- KEPT as a permanent name tombstone: JWT repo claims are URL-scoped
    -- and can outlive any retention (Codex security round on #105), so
    -- releasing the burned (org, url) name would let a stale token
    -- authorize against a successor repository created at that URL.
    purged_at      timestamptz,
    CONSTRAINT repos_org_id_url_key UNIQUE (org_id, url)
);
-- Keyset pagination index for ListRepos (live rows only).
CREATE INDEX IF NOT EXISTS repos_org_created_idx
    ON repos (org_id, created_at DESC, repo_id DESC) WHERE deleted_at IS NULL;
-- Phase-5 eventing/sync tables (eventing.go): webhook registrations, git
-- credentials, and repo upstream links.
CREATE TABLE IF NOT EXISTS webhooks (
    id         text PRIMARY KEY,
    org_id     bigint NOT NULL REFERENCES orgs(id) ON DELETE CASCADE,
    url        text NOT NULL,
    secret     text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS webhooks_org_idx ON webhooks (org_id);
CREATE TABLE IF NOT EXISTS git_credentials (
    id         text PRIMARY KEY,
    repo_id    text NOT NULL,
    username   text NOT NULL,
    password   text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT git_credentials_repo_id_key UNIQUE (repo_id)
);
CREATE TABLE IF NOT EXISTS repo_upstreams (
    repo_id       text PRIMARY KEY,
    provider      text NOT NULL,
    owner         text NOT NULL,
    name          text NOT NULL,
    upstream_host text NOT NULL,
    run_count     integer NOT NULL DEFAULT 0,
    success_count integer NOT NULL DEFAULT 0,
    syncing       boolean NOT NULL DEFAULT false,
    sync_pending  boolean NOT NULL DEFAULT false,
    sync_claim_holder     text NOT NULL DEFAULT '',
    sync_claim_expires_at timestamptz NOT NULL DEFAULT 'epoch',
    forward_on_push       boolean NOT NULL DEFAULT false,
    push_pending          integer NOT NULL DEFAULT 0,
    push_syncing          boolean NOT NULL DEFAULT false,
    push_claim_holder     text NOT NULL DEFAULT '',
    push_claim_expires_at timestamptz NOT NULL DEFAULT 'epoch',
    push_claim_force      boolean NOT NULL DEFAULT false,
    push_run_count        integer NOT NULL DEFAULT 0,
    push_success_count    integer NOT NULL DEFAULT 0,
    default_branch_omitted boolean NOT NULL DEFAULT false,
    created_at    timestamptz NOT NULL DEFAULT now()
);
-- Upgrade path for deployments whose repo_upstreams predates the
-- sync_pending (#79) or claim-lease (#86) columns (idempotent; runs under
-- the META advisory lock). Claims with the empty-holder/epoch defaults are
-- LEGACY claims: TryBeginSync adopts them under one grace period before
-- they become stealable, so a rolling upgrade's live old-version runs are
-- never raced while pre-existing wedges still heal automatically.
ALTER TABLE repo_upstreams ADD COLUMN IF NOT EXISTS sync_pending boolean NOT NULL DEFAULT false;
ALTER TABLE repo_upstreams ADD COLUMN IF NOT EXISTS sync_claim_holder text NOT NULL DEFAULT '';
ALTER TABLE repo_upstreams ADD COLUMN IF NOT EXISTS sync_claim_expires_at timestamptz NOT NULL DEFAULT 'epoch';
-- Outbound push-forwarding lane (issue #90): pending level 0/1/2 (force
-- coalesces upward), its own claim lease, and per-direction counters.
ALTER TABLE repo_upstreams ADD COLUMN IF NOT EXISTS forward_on_push boolean NOT NULL DEFAULT false;
ALTER TABLE repo_upstreams ADD COLUMN IF NOT EXISTS push_pending integer NOT NULL DEFAULT 0;
ALTER TABLE repo_upstreams ADD COLUMN IF NOT EXISTS push_syncing boolean NOT NULL DEFAULT false;
ALTER TABLE repo_upstreams ADD COLUMN IF NOT EXISTS push_claim_holder text NOT NULL DEFAULT '';
ALTER TABLE repo_upstreams ADD COLUMN IF NOT EXISTS push_claim_expires_at timestamptz NOT NULL DEFAULT 'epoch';
ALTER TABLE repo_upstreams ADD COLUMN IF NOT EXISTS push_claim_force boolean NOT NULL DEFAULT false;
ALTER TABLE repo_upstreams ADD COLUMN IF NOT EXISTS push_run_count integer NOT NULL DEFAULT 0;
ALTER TABLE repo_upstreams ADD COLUMN IF NOT EXISTS push_success_count integer NOT NULL DEFAULT 0;
-- GitHub sync create without a default branch (issue #599): rows that
-- predate the column keep their branch.
ALTER TABLE repo_upstreams ADD COLUMN IF NOT EXISTS default_branch_omitted boolean NOT NULL DEFAULT false;
-- Upgrade path for repos predating the purge tombstone column (issue #52).
ALTER TABLE repos ADD COLUMN IF NOT EXISTS purged_at timestamptz;
-- Create state (issue #388): 0 legacy, 1 pending, 2 complete. Rows that
-- predate the column read legacy, which is never treated as complete.
ALTER TABLE repos ADD COLUMN IF NOT EXISTS create_state integer NOT NULL DEFAULT 0;
-- Retention-purge scan index (issue #52): PurgeableRepos pages PENDING
-- purges (soft-deleted, not yet stamped) oldest-deletion-first. Excluding
-- stamped tombstones keeps the scan O(pending) — completed purges keep
-- their repos rows forever, so an all-deleted index would grow without
-- bound. Created after the purged_at upgrade above (the predicate needs
-- the column on pre-upgrade databases); the drop clears this PR's earlier
-- all-deleted spelling from dev databases.
DROP INDEX IF EXISTS repos_deleted_idx;
CREATE INDEX IF NOT EXISTS repos_purgeable_idx
    ON repos (deleted_at, repo_id) WHERE deleted_at IS NOT NULL AND purged_at IS NULL;
-- Any write to repos.default_branch ends the wait for adoption (issue
-- #609). The clear lives in a trigger, not only in Go: during a rolling
-- upgrade a replica from before this rule still commits a plain UPDATE, and
-- its PATCH to "main" changes nothing the adopt write could see (adoption
-- requires default_branch = 'main'). An UPDATE OF trigger fires when the SET
-- list names the column, even for an unchanged value. The catalog check
-- installs it once, so a steady-state boot takes no lock on repos for it
-- (DROP TRIGGER would take ACCESS EXCLUSIVE on every boot).
CREATE OR REPLACE FUNCTION clear_default_branch_omitted() RETURNS trigger LANGUAGE plpgsql AS $clear$
BEGIN
    UPDATE repo_upstreams SET default_branch_omitted = false
    WHERE repo_id = NEW.repo_id AND default_branch_omitted;
    RETURN NULL;
END $clear$;
DO $install$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger
                   WHERE tgname = 'repos_clear_default_branch_omitted' AND tgrelid = 'repos'::regclass) THEN
        CREATE TRIGGER repos_clear_default_branch_omitted
            AFTER UPDATE OF default_branch ON repos
            FOR EACH ROW EXECUTE FUNCTION clear_default_branch_omitted();
    END IF;
END $install$;
`)
		return err
	})
}

// newRepoID generates a server-side repo id: "repo_<12 hex>" (48 random bits).
func newRepoID() string {
	var b [6]byte
	_, err := rand.Read(b[:])
	assertf(err == nil, "crypto/rand failed: %v", err)
	return "repo_" + hex.EncodeToString(b[:])
}

// lookupOrgID resolves an org name to its id; ErrNotFound for unknown orgs.
func (s *PG) lookupOrgID(ctx context.Context, name string) (int64, error) {
	assertf(name != "", "lookupOrgID: empty org name")
	var id int64
	err := s.pool.QueryRow(ctx, `SELECT id FROM orgs WHERE name = $1`, name).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("org %q: %w", name, ErrNotFound)
	}
	if err != nil {
		return 0, fmt.Errorf("lookup org %q: %w", name, err)
	}
	assertf(id > 0, "org %q resolved to non-positive id %d", name, id)
	return id, nil
}

// scanRepo scans one row of repoCols; timestamps normalized to UTC per the
// Repo contract.
func scanRepo(row pgx.Row) (Repo, error) {
	var r Repo
	if err := row.Scan(&r.RepoID, &r.OrgID, &r.URL, &r.DefaultBranch, &r.CreatedAt, &r.DeletedAt, &r.CreateState); err != nil {
		return Repo{}, err
	}
	r.CreateState = knownCreateState(r.RepoID, r.CreateState)
	r.CreatedAt = r.CreatedAt.UTC()
	if r.DeletedAt != nil {
		utc := r.DeletedAt.UTC()
		r.DeletedAt = &utc
	}
	return r, nil
}

// UpsertOrgWithKeys idempotently creates the org and replaces its registered
// PEM public keys (startup bootstrap in single-org mode).
//
// The org row is locked (SELECT ... FOR UPDATE) before the DELETE/INSERT of
// org_public_keys so concurrent upserts (e.g. two replicas booting) serialize:
// without the lock, a blocked DELETE re-evaluates against a statement snapshot
// that misses the winner's freshly inserted rows, committing a union of both
// key sets instead of last-writer-wins replacement.
func (s *PG) UpsertOrgWithKeys(ctx context.Context, name string, pemPublicKeys []string) (Org, error) {
	assertf(name != "", "UpsertOrgWithKeys: empty org name")
	assertf(len(pemPublicKeys) > 0, "UpsertOrgWithKeys: org %q needs at least one public key", name)
	for i, pem := range pemPublicKeys {
		assertf(strings.TrimSpace(pem) != "", "UpsertOrgWithKeys: blank PEM block %d for org %q", i, name)
	}
	var org Org
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO orgs (name) VALUES ($1) ON CONFLICT (name) DO NOTHING`, name); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx,
			`SELECT id, name, created_at FROM orgs WHERE name = $1 FOR UPDATE`, name).
			Scan(&org.ID, &org.Name, &org.CreatedAt); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM org_public_keys WHERE org_id = $1`, org.ID); err != nil {
			return err
		}
		for _, pem := range pemPublicKeys {
			if _, err := tx.Exec(ctx,
				`INSERT INTO org_public_keys (org_id, pem) VALUES ($1, $2)`, org.ID, pem); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return Org{}, fmt.Errorf("upsert org %q: %w", name, err)
	}
	org.CreatedAt = org.CreatedAt.UTC()
	return org, nil
}

// PublicKeysForOrg returns the org's registered PEM public key blocks in
// registration order.
func (s *PG) PublicKeysForOrg(ctx context.Context, name string) ([]string, error) {
	orgID, err := s.lookupOrgID(ctx, name)
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx,
		`SELECT pem FROM org_public_keys WHERE org_id = $1 ORDER BY id`, orgID)
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
func (s *PG) OrgNames(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT name FROM orgs ORDER BY name`)
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
func (s *PG) CreateRepo(ctx context.Context, org, url, defaultBranch string) (Repo, error) {
	assertf(url != "", "CreateRepo: empty url for org %q", org)
	assertf(defaultBranch != "", "CreateRepo: empty default branch for %q (caller defaults to main)", url)
	orgID, err := s.lookupOrgID(ctx, org)
	if err != nil {
		return Repo{}, err
	}
	return pgInsertRepo(ctx, s.pool, org, orgID, url, defaultBranch, CreateStateLegacy)
}

// CreateRepoWithState is CreateRepo with an explicit create state.
func (s *PG) CreateRepoWithState(ctx context.Context, org, url, defaultBranch string, state CreateState) (Repo, error) {
	assertf(url != "" && defaultBranch != "", "CreateRepoWithState: empty url %q or default branch %q", url, defaultBranch)
	assertf(state == CreateStatePending || state == CreateStateComplete, "CreateRepoWithState: state %d", state)
	orgID, err := s.lookupOrgID(ctx, org)
	if err != nil {
		return Repo{}, err
	}
	return pgInsertRepo(ctx, s.pool, org, orgID, url, defaultBranch, state)
}

// CompleteRepoCreate moves a pending repo to complete (meta.Store contract).
func (s *PG) CompleteRepoCreate(ctx context.Context, org, repoID string) (CreateOutcome, error) {
	var out CreateOutcome
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var err error
		out, err = PGCompleteRepoCreate(ctx, tx, org, repoID, nil)
		return err
	})
	return out, err
}

// PGCompleteRepoCreate is CompleteRepoCreate inside the caller's
// transaction on the metadata database. It locks the repo row; only when
// the row is live and pending does it run schedule(ctx) (nil = nothing)
// and mark the row complete, so both commit together or not at all
// (issue #388: River inserts a sync create's first-sync job in tx). A
// complete or deleted row runs nothing.
func PGCompleteRepoCreate(ctx context.Context, tx pgx.Tx, org, repoID string, schedule func(context.Context) error) (CreateOutcome, error) {
	assertf(tx != nil && repoID != "", "PGCompleteRepoCreate: nil tx or empty repo id for org %q", org)
	r, err := scanRepo(tx.QueryRow(ctx,
		`SELECT `+repoCols+` FROM repos
		 WHERE org_id = (SELECT id FROM orgs WHERE name = $1) AND repo_id = $2 FOR UPDATE`, org, repoID))
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("repo id %q in org %q: %w", repoID, org, ErrNotFound)
	}
	if err != nil {
		return 0, fmt.Errorf("lock repo id %q: %w", repoID, err)
	}
	if r.DeletedAt != nil || r.CreateState != CreateStatePending {
		return completeOutcome(r)
	}
	if schedule != nil {
		if err := schedule(ctx); err != nil {
			return 0, err
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE repos SET create_state = $2 WHERE repo_id = $1`,
		repoID, CreateStateComplete); err != nil {
		return 0, fmt.Errorf("complete create of repo id %q: %w", repoID, err)
	}
	return CreateCompleted, nil
}

// pgQueryRower is the QueryRow method *pgxpool.Pool and pgx.Tx share, so
// an insert runs alone or inside CreateLinkedRepo's transaction.
type pgQueryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func pgInsertRepo(ctx context.Context, q pgQueryRower, org string, orgID int64, url, defaultBranch string, state CreateState) (Repo, error) {
	r := Repo{RepoID: newRepoID(), OrgID: orgID, URL: url, DefaultBranch: defaultBranch, CreateState: state}
	err := q.QueryRow(ctx,
		`INSERT INTO repos (repo_id, org_id, url, default_branch, create_state) VALUES ($1, $2, $3, $4, $5)
		 RETURNING created_at`,
		r.RepoID, orgID, url, defaultBranch, state).Scan(&r.CreatedAt)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation && pgErr.ConstraintName == repoURLConstraint {
		return Repo{}, fmt.Errorf("repo %q in org %q: %w", url, org, ErrAlreadyExists)
	}
	if err != nil {
		return Repo{}, fmt.Errorf("create repo %q in org %q: %w", url, org, err)
	}
	r.CreatedAt = r.CreatedAt.UTC()
	return r, nil
}

// GetRepoByURL returns the live repo with the given url; ErrNotFound when
// absent or soft-deleted.
func (s *PG) GetRepoByURL(ctx context.Context, org, url string) (Repo, error) {
	assertf(url != "", "GetRepoByURL: empty url for org %q", org)
	orgID, err := s.lookupOrgID(ctx, org)
	if err != nil {
		return Repo{}, err
	}
	r, err := scanRepo(s.pool.QueryRow(ctx,
		`SELECT `+repoCols+` FROM repos WHERE org_id = $1 AND url = $2 AND deleted_at IS NULL`,
		orgID, url))
	if errors.Is(err, pgx.ErrNoRows) {
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
func (s *PG) GetRepoByID(ctx context.Context, org, repoID string) (Repo, error) {
	assertf(repoID != "", "GetRepoByID: empty repoID for org %q", org)
	orgID, err := s.lookupOrgID(ctx, org)
	if err != nil {
		return Repo{}, err
	}
	r, err := scanRepo(s.pool.QueryRow(ctx,
		`SELECT `+repoCols+` FROM repos WHERE org_id = $1 AND repo_id = $2 AND deleted_at IS NULL`,
		orgID, repoID))
	if errors.Is(err, pgx.ErrNoRows) {
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
// splits inside the same transaction into "no live repo" (ErrNotFound) and
// "default branch changed concurrently" (won=false).
func (s *PG) UpdateRepoDefaultBranch(ctx context.Context, org, url, oldBranch, newBranch string) (bool, error) {
	assertf(url != "", "UpdateRepoDefaultBranch: empty url for org %q", org)
	assertf(oldBranch != "" && newBranch != "",
		"UpdateRepoDefaultBranch: empty branch for %q (old %q, new %q)", url, oldBranch, newBranch)
	orgID, err := s.lookupOrgID(ctx, org)
	if err != nil {
		return false, err
	}
	won := false
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		ct, err := tx.Exec(ctx,
			`UPDATE repos SET default_branch = $1
			 WHERE org_id = $2 AND url = $3 AND deleted_at IS NULL AND default_branch = $4`,
			newBranch, orgID, url, oldBranch)
		if err != nil {
			return err
		}
		n := ct.RowsAffected()
		assertf(n == 0 || n == 1, "default-branch CAS of %q/%q affected %d rows", org, url, n)
		if n == 1 {
			won = true
			_, err := tx.Exec(ctx,
				`UPDATE repo_upstreams SET default_branch_omitted = false
				 WHERE default_branch_omitted AND repo_id = (SELECT repo_id FROM repos WHERE org_id = $1 AND url = $2 AND deleted_at IS NULL)`,
				orgID, url)
			return err
		}
		// No row matched: absent/soft-deleted repo (404) or a concurrently
		// changed default branch (won=false -> caller's 409).
		var one int
		err = tx.QueryRow(ctx,
			`SELECT 1 FROM repos WHERE org_id = $1 AND url = $2 AND deleted_at IS NULL`,
			orgID, url).Scan(&one)
		if errors.Is(err, pgx.ErrNoRows) {
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
func (s *PG) SoftDeleteRepo(ctx context.Context, org, url string) (Repo, error) {
	assertf(url != "", "SoftDeleteRepo: empty url for org %q", org)
	orgID, err := s.lookupOrgID(ctx, org)
	if err != nil {
		return Repo{}, err
	}
	var repo Repo
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		r, err := scanRepo(tx.QueryRow(ctx,
			`UPDATE repos SET deleted_at = now()
			 WHERE org_id = $1 AND url = $2 AND deleted_at IS NULL
			 RETURNING `+repoCols, orgID, url))
		if err == nil {
			assertf(r.DeletedAt != nil, "soft delete of %q returned nil deleted_at", r.RepoID)
			repo = r
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		// No live row matched: distinguish already-deleted (409) from absent
		// (404). A live row appearing between the two statements (create
		// racing delete under READ COMMITTED) linearizes as create-after-
		// delete, so ErrNotFound stays correct.
		var deletedAt *time.Time
		err = tx.QueryRow(ctx,
			`SELECT deleted_at FROM repos WHERE org_id = $1 AND url = $2`, orgID, url).Scan(&deletedAt)
		if errors.Is(err, pgx.ErrNoRows) || (err == nil && deletedAt == nil) {
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
func (s *PG) AddSigningKey(ctx context.Context, org, keyText string) (SigningKey, error) {
	assertf(strings.TrimSpace(keyText) != "", "AddSigningKey: blank key text for org %q", org)
	orgID, err := s.lookupOrgID(ctx, org)
	if err != nil {
		return SigningKey{}, err
	}
	k := SigningKey{OrgID: orgID, KeyText: keyText}
	err = s.pool.QueryRow(ctx,
		`INSERT INTO org_signing_keys (org_id, key_text) VALUES ($1, $2)
		 RETURNING id, created_at`, orgID, keyText).Scan(&k.ID, &k.CreatedAt)
	if err != nil {
		return SigningKey{}, fmt.Errorf("add signing key for org %q: %w", org, err)
	}
	k.CreatedAt = k.CreatedAt.UTC()
	return k, nil
}

// SigningKeysForOrg returns the org's registered signing keys in
// registration order; empty slice when none are registered.
func (s *PG) SigningKeysForOrg(ctx context.Context, org string) ([]SigningKey, error) {
	orgID, err := s.lookupOrgID(ctx, org)
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, org_id, key_text, created_at FROM org_signing_keys
		 WHERE org_id = $1 ORDER BY id`, orgID)
	if err != nil {
		return nil, fmt.Errorf("signing keys for org %q: %w", org, err)
	}
	defer rows.Close()
	keys := []SigningKey{}
	for rows.Next() {
		var k SigningKey
		if err := rows.Scan(&k.ID, &k.OrgID, &k.KeyText, &k.CreatedAt); err != nil {
			return nil, err
		}
		k.CreatedAt = k.CreatedAt.UTC()
		keys = append(keys, k)
	}
	return keys, rows.Err()
}

// RemoveSigningKey deletes the signing key with the given ID. ErrNotFound
// when the org is unknown or the ID does not belong to it.
func (s *PG) RemoveSigningKey(ctx context.Context, org string, id int64) error {
	assertf(id > 0, "RemoveSigningKey: non-positive id %d for org %q", id, org)
	orgID, err := s.lookupOrgID(ctx, org)
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM org_signing_keys WHERE org_id = $1 AND id = $2`, orgID, id)
	if err != nil {
		return fmt.Errorf("remove signing key %d for org %q: %w", id, org, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("signing key %d for org %q: %w", id, org, ErrNotFound)
	}
	assertf(tag.RowsAffected() == 1, "DELETE by primary key affected %d rows", tag.RowsAffected())
	return nil
}

// escapeLike backslash-escapes LIKE/ILIKE metacharacters so q matches as a
// literal substring (default ESCAPE character is backslash).
func escapeLike(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

// listURLPattern returns the LIKE/ILIKE pattern for the ListParams url
// filters, or "" when neither applies. `prefix%q%` anchors the literal
// prefix at the start, which consumes exactly the prefix's characters, so q
// can only match the rest of the url after it.
func listURLPattern(p ListParams) string {
	assertf(p.Q == strings.TrimSpace(p.Q) && p.Prefix == strings.TrimSpace(p.Prefix),
		"ListRepos: untrimmed filters q=%q prefix=%q", p.Q, p.Prefix)
	if p.Q == "" && p.Prefix == "" {
		return ""
	}
	pattern := escapeLike(p.Prefix) + "%"
	if p.Q != "" {
		pattern += escapeLike(p.Q) + "%"
	}
	return pattern
}

// ListRepos pages live repos for the org, keyset-ordered by
// (created_at DESC, repo_id DESC).
func (s *PG) ListRepos(ctx context.Context, org string, params ListParams) (ListResult, error) {
	// Limit is validated/clamped by the HTTP handler per the Store contract
	// (default 20, max 100); a value outside [1,100] is a caller bug.
	assertf(params.Limit >= 1 && params.Limit <= 100,
		"ListRepos: limit %d outside contract range [1,100]", params.Limit)
	orgID, err := s.lookupOrgID(ctx, org)
	if err != nil {
		return ListResult{}, err
	}

	sql := `SELECT ` + repoCols + ` FROM repos WHERE org_id = $1 AND deleted_at IS NULL`
	args := []any{orgID}
	if pattern := listURLPattern(params); pattern != "" {
		args = append(args, pattern)
		sql += fmt.Sprintf(` AND url ILIKE $%d`, len(args))
	}
	if params.Cursor != "" {
		createdAt, repoID, err := decodeCursor(params.Cursor)
		if err != nil {
			return ListResult{}, err
		}
		args = append(args, createdAt, repoID)
		sql += fmt.Sprintf(` AND (created_at, repo_id) < ($%d, $%d)`, len(args)-1, len(args))
	}
	// Fetch one extra row to compute HasMore without a second query.
	args = append(args, params.Limit+1)
	sql += fmt.Sprintf(` ORDER BY created_at DESC, repo_id DESC LIMIT $%d`, len(args))

	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return ListResult{}, fmt.Errorf("list repos for org %q: %w", org, err)
	}
	defer rows.Close()
	repos := make([]Repo, 0, params.Limit+1)
	for rows.Next() {
		r, err := scanRepo(rows)
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
