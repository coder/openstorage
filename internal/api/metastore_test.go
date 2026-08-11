package api

// pgMetaStore is a TEST-SCOPED implementation of the frozen meta.Store
// contract against real Postgres (pgx), used to drive the integration matrix
// until the sibling phase-1 slice lands the production internal/meta store.
// INTEGRATION NOTE: once that store exists, newTestDeps (env_test.go) should
// construct it instead and this file can be deleted — nothing else refers to
// pgMetaStore. The schema below mirrors the contract comment in
// internal/meta/meta.go exactly (UNIQUE(org_id, url) spans soft-deleted rows).

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"codestorage/internal/meta"
)

type pgMetaStore struct {
	pool *pgxpool.Pool
}

var _ meta.Store = (*pgMetaStore)(nil)

func openPGMetaStore(ctx context.Context, pgURL string) (*pgMetaStore, error) {
	pool, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		return nil, err
	}
	s := &pgMetaStore{pool: pool}
	if err := s.initSchema(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *pgMetaStore) Close() { s.pool.Close() }

// initSchema creates the metadata tables (idempotent), serialized with an
// advisory lock like internal/ref does for concurrent startups.
func (s *pgMetaStore) initSchema(ctx context.Context) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(0x4D455441)`); err != nil { // "META"
			return err
		}
		_, err := tx.Exec(ctx, `
CREATE TABLE IF NOT EXISTS orgs (
  id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name       text UNIQUE NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS org_public_keys (
  id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  org_id     bigint NOT NULL REFERENCES orgs(id),
  pem        text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS repos (
  repo_id        text PRIMARY KEY,
  org_id         bigint NOT NULL REFERENCES orgs(id),
  url            text NOT NULL,
  default_branch text NOT NULL,
  created_at     timestamptz NOT NULL DEFAULT now(),
  deleted_at     timestamptz,
  UNIQUE (org_id, url));
`)
		return err
	})
}

func (s *pgMetaStore) orgID(ctx context.Context, name string) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx, `SELECT id FROM orgs WHERE name = $1`, name).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("org %q: %w", name, meta.ErrNotFound)
	}
	return id, err
}

func (s *pgMetaStore) UpsertOrgWithKeys(ctx context.Context, name string, pemPublicKeys []string) (meta.Org, error) {
	var org meta.Org
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
INSERT INTO orgs (name) VALUES ($1)
ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
RETURNING id, name, created_at`, name).Scan(&org.ID, &org.Name, &org.CreatedAt)
		if err != nil {
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
	return org, err
}

func (s *pgMetaStore) PublicKeysForOrg(ctx context.Context, name string) ([]string, error) {
	orgID, err := s.orgID(ctx, name)
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx,
		`SELECT pem FROM org_public_keys WHERE org_id = $1 ORDER BY id`, orgID)
	if err != nil {
		return nil, err
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

func (s *pgMetaStore) CreateRepo(ctx context.Context, org, url, defaultBranch string) (meta.Repo, error) {
	orgID, err := s.orgID(ctx, org)
	if err != nil {
		return meta.Repo{}, err
	}
	var buf [6]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return meta.Repo{}, err
	}
	repo := meta.Repo{
		RepoID:        "repo_" + hex.EncodeToString(buf[:]),
		OrgID:         orgID,
		URL:           url,
		DefaultBranch: defaultBranch,
	}
	err = s.pool.QueryRow(ctx, `
INSERT INTO repos (repo_id, org_id, url, default_branch) VALUES ($1, $2, $3, $4)
RETURNING created_at`, repo.RepoID, orgID, url, defaultBranch).Scan(&repo.CreatedAt)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation on (org_id, url)
		return meta.Repo{}, fmt.Errorf("repo %q: %w", url, meta.ErrAlreadyExists)
	}
	if err != nil {
		return meta.Repo{}, err
	}
	return repo, nil
}

func (s *pgMetaStore) GetRepoByURL(ctx context.Context, org, url string) (meta.Repo, error) {
	orgID, err := s.orgID(ctx, org)
	if err != nil {
		return meta.Repo{}, err
	}
	var repo meta.Repo
	err = s.pool.QueryRow(ctx, `
SELECT repo_id, org_id, url, default_branch, created_at FROM repos
WHERE org_id = $1 AND url = $2 AND deleted_at IS NULL`, orgID, url).
		Scan(&repo.RepoID, &repo.OrgID, &repo.URL, &repo.DefaultBranch, &repo.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return meta.Repo{}, fmt.Errorf("repo %q: %w", url, meta.ErrNotFound)
	}
	return repo, err
}

func (s *pgMetaStore) SoftDeleteRepo(ctx context.Context, org, url string) (meta.Repo, error) {
	orgID, err := s.orgID(ctx, org)
	if err != nil {
		return meta.Repo{}, err
	}
	var repo meta.Repo
	var deletedAt time.Time
	err = s.pool.QueryRow(ctx, `
UPDATE repos SET deleted_at = now()
WHERE org_id = $1 AND url = $2 AND deleted_at IS NULL
RETURNING repo_id, org_id, url, default_branch, created_at, deleted_at`, orgID, url).
		Scan(&repo.RepoID, &repo.OrgID, &repo.URL, &repo.DefaultBranch, &repo.CreatedAt, &deletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		// Distinguish never-existed (404) from already soft-deleted (409).
		var exists bool
		if qErr := s.pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM repos WHERE org_id = $1 AND url = $2)`,
			orgID, url).Scan(&exists); qErr != nil {
			return meta.Repo{}, qErr
		}
		if exists {
			return meta.Repo{}, fmt.Errorf("repo %q: %w", url, meta.ErrAlreadyDeleted)
		}
		return meta.Repo{}, fmt.Errorf("repo %q: %w", url, meta.ErrNotFound)
	}
	if err != nil {
		return meta.Repo{}, err
	}
	repo.DeletedAt = &deletedAt
	return repo, nil
}

// encodeCursor/decodeCursor implement the opaque keyset cursor:
// base64url("<created_at_unixnano>|<repo_id>").
func encodeCursor(createdAt time.Time, repoID string) string {
	return base64.RawURLEncoding.EncodeToString(
		[]byte(strconv.FormatInt(createdAt.UnixNano(), 10) + "|" + repoID))
}

func decodeCursor(cursor string) (time.Time, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("%w: %v", meta.ErrInvalidCursor, err)
	}
	ns, repoID, found := strings.Cut(string(raw), "|")
	if !found || repoID == "" {
		return time.Time{}, "", meta.ErrInvalidCursor
	}
	nanos, err := strconv.ParseInt(ns, 10, 64)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("%w: %v", meta.ErrInvalidCursor, err)
	}
	return time.Unix(0, nanos), repoID, nil
}

// escapeLike escapes LIKE/ILIKE metacharacters in a user-supplied substring
// (backslash is the default ESCAPE character).
func escapeLike(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

func (s *pgMetaStore) ListRepos(ctx context.Context, org string, params meta.ListParams) (meta.ListResult, error) {
	if params.Limit < 1 {
		return meta.ListResult{}, fmt.Errorf("ListRepos called with unvalidated limit %d", params.Limit)
	}
	orgID, err := s.orgID(ctx, org)
	if err != nil {
		return meta.ListResult{}, err
	}

	sql := `
SELECT repo_id, org_id, url, default_branch, created_at FROM repos
WHERE org_id = $1 AND deleted_at IS NULL`
	args := []any{orgID}
	if params.Q != "" {
		args = append(args, "%"+escapeLike(params.Q)+"%")
		sql += fmt.Sprintf(` AND url ILIKE $%d`, len(args))
	}
	if params.Cursor != "" {
		createdAt, repoID, err := decodeCursor(params.Cursor)
		if err != nil {
			return meta.ListResult{}, err
		}
		args = append(args, createdAt, repoID)
		sql += fmt.Sprintf(` AND (created_at, repo_id) < ($%d, $%d)`, len(args)-1, len(args))
	}
	args = append(args, params.Limit+1) // one extra row decides HasMore
	sql += fmt.Sprintf(` ORDER BY created_at DESC, repo_id DESC LIMIT $%d`, len(args))

	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return meta.ListResult{}, err
	}
	defer rows.Close()

	repos := make([]meta.Repo, 0, params.Limit)
	for rows.Next() {
		var repo meta.Repo
		if err := rows.Scan(&repo.RepoID, &repo.OrgID, &repo.URL, &repo.DefaultBranch, &repo.CreatedAt); err != nil {
			return meta.ListResult{}, err
		}
		repos = append(repos, repo)
	}
	if err := rows.Err(); err != nil {
		return meta.ListResult{}, err
	}

	res := meta.ListResult{Repos: repos}
	if len(repos) > params.Limit {
		res.Repos = repos[:params.Limit]
		res.HasMore = true
		last := res.Repos[len(res.Repos)-1]
		res.NextCursor = encodeCursor(last.CreatedAt, last.RepoID)
	}
	return res, nil
}
