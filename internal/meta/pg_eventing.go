// Phase-5 eventing/sync persistence on Postgres (contract and semantics in
// eventing.go; sqlite_eventing.go mirrors this file 1:1).

package meta

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	// gitCredentialRepoConstraint is the named UNIQUE(repo_id) constraint:
	// one credential per repo (docs/03: 409).
	gitCredentialRepoConstraint = "git_credentials_repo_id_key"
	// Scan column lists matching scanWebhook / scanGitCredential /
	// scanRepoUpstream (shared with sqlite_eventing.go like repoCols).
	webhookCols       = "id, org_id, url, secret, created_at"
	gitCredentialCols = "id, repo_id, username, password, created_at"
	repoUpstreamCols  = "repo_id, provider, owner, name, upstream_host, run_count, success_count, syncing, created_at"
)

// scanWebhook scans one row of webhookCols; CreatedAt normalized to UTC.
func scanWebhook(row pgx.Row) (Webhook, error) {
	var w Webhook
	if err := row.Scan(&w.ID, &w.OrgID, &w.URL, &w.Secret, &w.CreatedAt); err != nil {
		return Webhook{}, err
	}
	w.CreatedAt = w.CreatedAt.UTC()
	return w, nil
}

// scanGitCredential scans one row of gitCredentialCols; CreatedAt in UTC.
func scanGitCredential(row pgx.Row) (GitCredential, error) {
	var c GitCredential
	if err := row.Scan(&c.ID, &c.RepoID, &c.Username, &c.Password, &c.CreatedAt); err != nil {
		return GitCredential{}, err
	}
	c.CreatedAt = c.CreatedAt.UTC()
	return c, nil
}

// scanRepoUpstream scans one row of repoUpstreamCols; CreatedAt in UTC.
func scanRepoUpstream(row pgx.Row) (RepoUpstream, error) {
	var up RepoUpstream
	if err := row.Scan(&up.RepoID, &up.Provider, &up.Owner, &up.Name, &up.UpstreamHost,
		&up.RunCount, &up.SuccessCount, &up.Syncing, &up.CreatedAt); err != nil {
		return RepoUpstream{}, err
	}
	up.CreatedAt = up.CreatedAt.UTC()
	return up, nil
}

// decryptWebhook maps w.Secret from its stored column form to plaintext.
// Legacy plaintext rows and rows under a retired key are lazily rewritten
// under the active key: the stored-value guard makes the UPDATE a
// compare-and-swap, so a concurrent writer wins and the rewrite no-ops.
// Rewrite errors are ignored on purpose — the read stays correct and the
// next read retries.
func (s *PG) decryptWebhook(ctx context.Context, w *Webhook) error {
	pt, rewrite, err := decodeSecret(s.codec, w.Secret, fmt.Sprintf("webhook %q secret", w.ID))
	if err != nil {
		return err
	}
	if rewrite {
		_, _ = s.pool.Exec(ctx,
			`UPDATE webhooks SET secret = $3 WHERE id = $1 AND secret = $2`,
			w.ID, w.Secret, s.codec.Encrypt(pt))
	}
	w.Secret = pt
	return nil
}

// decryptGitCredential is decryptWebhook's git_credentials.password twin.
func (s *PG) decryptGitCredential(ctx context.Context, c *GitCredential) error {
	pt, rewrite, err := decodeSecret(s.codec, c.Password, fmt.Sprintf("credential %q password", c.ID))
	if err != nil {
		return err
	}
	if rewrite {
		_, _ = s.pool.Exec(ctx,
			`UPDATE git_credentials SET password = $3 WHERE id = $1 AND password = $2`,
			c.ID, c.Password, s.codec.Encrypt(pt))
	}
	c.Password = pt
	return nil
}

// CreateWebhook inserts a webhook with a server-generated ID.
// ErrNotFound when the org is unknown; ErrReservedSecret when the secret
// starts with the reserved envelope prefix.
func (s *PG) CreateWebhook(ctx context.Context, org, url, secret string) (Webhook, error) {
	assertf(url != "", "CreateWebhook: empty url for org %q", org)
	stored, err := encodeSecret(s.codec, secret)
	if err != nil {
		return Webhook{}, fmt.Errorf("create webhook for org %q: %w", org, err)
	}
	orgID, err := s.lookupOrgID(ctx, org)
	if err != nil {
		return Webhook{}, err
	}
	w := Webhook{ID: NewEventingID("wh"), OrgID: orgID, URL: url, Secret: secret}
	err = s.pool.QueryRow(ctx,
		`INSERT INTO webhooks (id, org_id, url, secret) VALUES ($1, $2, $3, $4)
		 RETURNING created_at`, w.ID, orgID, url, stored).Scan(&w.CreatedAt)
	if err != nil {
		return Webhook{}, fmt.Errorf("create webhook for org %q: %w", org, err)
	}
	w.CreatedAt = w.CreatedAt.UTC()
	return w, nil
}

// ListWebhooks returns the org's webhooks in registration order.
// ErrNotFound when the org is unknown.
func (s *PG) ListWebhooks(ctx context.Context, org string) ([]Webhook, error) {
	orgID, err := s.lookupOrgID(ctx, org)
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx,
		`SELECT `+webhookCols+` FROM webhooks WHERE org_id = $1 ORDER BY created_at, id`, orgID)
	if err != nil {
		return nil, fmt.Errorf("list webhooks for org %q: %w", org, err)
	}
	defer rows.Close()
	hooks := []Webhook{}
	for rows.Next() {
		w, err := scanWebhook(rows)
		if err != nil {
			return nil, err
		}
		hooks = append(hooks, w)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close() // release the connection before the rewrite UPDATEs
	for i := range hooks {
		if err := s.decryptWebhook(ctx, &hooks[i]); err != nil {
			// Enumeration must not fail org-wide because one row's envelope
			// is corrupt or under a missing key: event fan-out and the list
			// API only need IDs, and hiding the bad row's ID would block
			// the delete-and-re-register recovery. Degrade this row to an
			// empty secret (never the raw envelope); per-delivery
			// GetWebhook still fails closed for it.
			hooks[i].Secret = ""
		}
	}
	return hooks, nil
}

// GetWebhook returns the webhook with the given ID regardless of org
// (delivery jobs carry only the ID); ErrNotFound when absent.
func (s *PG) GetWebhook(ctx context.Context, id string) (Webhook, error) {
	assertf(id != "", "GetWebhook: empty id")
	w, err := scanWebhook(s.pool.QueryRow(ctx,
		`SELECT `+webhookCols+` FROM webhooks WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Webhook{}, fmt.Errorf("webhook %q: %w", id, ErrNotFound)
	}
	if err != nil {
		return Webhook{}, fmt.Errorf("get webhook %q: %w", id, err)
	}
	if err := s.decryptWebhook(ctx, &w); err != nil {
		return Webhook{}, err
	}
	return w, nil
}

// DeleteWebhook removes the org's webhook; ErrNotFound when the org is
// unknown or the ID does not belong to it.
func (s *PG) DeleteWebhook(ctx context.Context, org, id string) error {
	assertf(id != "", "DeleteWebhook: empty id for org %q", org)
	orgID, err := s.lookupOrgID(ctx, org)
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM webhooks WHERE org_id = $1 AND id = $2`, orgID, id)
	if err != nil {
		return fmt.Errorf("delete webhook %q for org %q: %w", id, org, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("webhook %q for org %q: %w", id, org, ErrNotFound)
	}
	assertf(tag.RowsAffected() == 1, "DELETE by primary key affected %d rows", tag.RowsAffected())
	return nil
}

// CreateGitCredential stores cred (ID/CreatedAt assigned by the store).
// ErrAlreadyExists when the repo already has a credential; ErrReservedSecret
// when the password starts with the reserved envelope prefix.
func (s *PG) CreateGitCredential(ctx context.Context, cred GitCredential) (GitCredential, error) {
	assertf(cred.RepoID != "", "CreateGitCredential: empty repo id")
	stored, err := encodeSecret(s.codec, cred.Password)
	if err != nil {
		return GitCredential{}, fmt.Errorf("create credential for repo %q: %w", cred.RepoID, err)
	}
	cred.ID = NewEventingID("ggc")
	err = s.pool.QueryRow(ctx,
		`INSERT INTO git_credentials (id, repo_id, username, password) VALUES ($1, $2, $3, $4)
		 RETURNING created_at`,
		cred.ID, cred.RepoID, cred.Username, stored).Scan(&cred.CreatedAt)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation && pgErr.ConstraintName == gitCredentialRepoConstraint {
		return GitCredential{}, fmt.Errorf("credential for repo %q: %w", cred.RepoID, ErrAlreadyExists)
	}
	if err != nil {
		return GitCredential{}, fmt.Errorf("create credential for repo %q: %w", cred.RepoID, err)
	}
	cred.CreatedAt = cred.CreatedAt.UTC()
	return cred, nil
}

// GetGitCredential returns the credential by ID; ErrNotFound when absent.
func (s *PG) GetGitCredential(ctx context.Context, id string) (GitCredential, error) {
	assertf(id != "", "GetGitCredential: empty id")
	c, err := scanGitCredential(s.pool.QueryRow(ctx,
		`SELECT `+gitCredentialCols+` FROM git_credentials WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return GitCredential{}, fmt.Errorf("credential %q: %w", id, ErrNotFound)
	}
	if err != nil {
		return GitCredential{}, fmt.Errorf("get credential %q: %w", id, err)
	}
	if err := s.decryptGitCredential(ctx, &c); err != nil {
		return GitCredential{}, err
	}
	return c, nil
}

// GitCredentialMeta returns the row with Password "" (the password column
// is never read — see the interface comment: recovery for undecryptable
// rows). ErrNotFound when absent.
func (s *PG) GitCredentialMeta(ctx context.Context, id string) (GitCredential, error) {
	assertf(id != "", "GitCredentialMeta: empty id")
	var c GitCredential
	err := s.pool.QueryRow(ctx,
		`SELECT id, repo_id, username, created_at FROM git_credentials WHERE id = $1`, id).
		Scan(&c.ID, &c.RepoID, &c.Username, &c.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return GitCredential{}, fmt.Errorf("credential %q: %w", id, ErrNotFound)
	}
	if err != nil {
		return GitCredential{}, fmt.Errorf("get credential meta %q: %w", id, err)
	}
	c.CreatedAt = c.CreatedAt.UTC()
	return c, nil
}

// UpdateGitCredential replaces username/password; ErrNotFound when absent;
// ErrReservedSecret when the password starts with the reserved envelope
// prefix.
func (s *PG) UpdateGitCredential(ctx context.Context, id, username, password string) (GitCredential, error) {
	assertf(id != "", "UpdateGitCredential: empty id")
	stored, err := encodeSecret(s.codec, password)
	if err != nil {
		return GitCredential{}, fmt.Errorf("update credential %q: %w", id, err)
	}
	c, err := scanGitCredential(s.pool.QueryRow(ctx,
		`UPDATE git_credentials SET username = $2, password = $3 WHERE id = $1
		 RETURNING `+gitCredentialCols, id, username, stored))
	if errors.Is(err, pgx.ErrNoRows) {
		return GitCredential{}, fmt.Errorf("credential %q: %w", id, ErrNotFound)
	}
	if err != nil {
		return GitCredential{}, fmt.Errorf("update credential %q: %w", id, err)
	}
	// The RETURNING row carries the stored form we just wrote; hand the
	// caller its plaintext back (interface contract: plaintext in, plaintext out).
	c.Password = password
	return c, nil
}

// DeleteGitCredential removes the credential; ErrNotFound when absent.
func (s *PG) DeleteGitCredential(ctx context.Context, id string) error {
	assertf(id != "", "DeleteGitCredential: empty id")
	tag, err := s.pool.Exec(ctx, `DELETE FROM git_credentials WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete credential %q: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("credential %q: %w", id, ErrNotFound)
	}
	assertf(tag.RowsAffected() == 1, "DELETE by primary key affected %d rows", tag.RowsAffected())
	return nil
}

// GitCredentialForRepo returns the repo's credential; ErrNotFound when none
// is stored (public upstream).
func (s *PG) GitCredentialForRepo(ctx context.Context, repoID string) (GitCredential, error) {
	assertf(repoID != "", "GitCredentialForRepo: empty repo id")
	c, err := scanGitCredential(s.pool.QueryRow(ctx,
		`SELECT `+gitCredentialCols+` FROM git_credentials WHERE repo_id = $1`, repoID))
	if errors.Is(err, pgx.ErrNoRows) {
		return GitCredential{}, fmt.Errorf("credential for repo %q: %w", repoID, ErrNotFound)
	}
	if err != nil {
		return GitCredential{}, fmt.Errorf("get credential for repo %q: %w", repoID, err)
	}
	if err := s.decryptGitCredential(ctx, &c); err != nil {
		return GitCredential{}, err
	}
	return c, nil
}

// SetRepoUpstream inserts the upstream row (CreatedAt assigned by the store;
// counters/syncing zeroed). ErrAlreadyExists when the repo already has one.
func (s *PG) SetRepoUpstream(ctx context.Context, up RepoUpstream) (RepoUpstream, error) {
	assertf(up.RepoID != "", "SetRepoUpstream: empty repo id")
	up.RunCount, up.SuccessCount, up.Syncing = 0, 0, false
	err := s.pool.QueryRow(ctx,
		`INSERT INTO repo_upstreams (repo_id, provider, owner, name, upstream_host)
		 VALUES ($1, $2, $3, $4, $5) RETURNING created_at`,
		up.RepoID, up.Provider, up.Owner, up.Name, up.UpstreamHost).Scan(&up.CreatedAt)
	// The only unique constraint on repo_upstreams is the repo_id PK.
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
		return RepoUpstream{}, fmt.Errorf("upstream for repo %q: %w", up.RepoID, ErrAlreadyExists)
	}
	if err != nil {
		return RepoUpstream{}, fmt.Errorf("set upstream for repo %q: %w", up.RepoID, err)
	}
	up.CreatedAt = up.CreatedAt.UTC()
	return up, nil
}

// GetRepoUpstream returns the row; ErrNotFound when detached/never linked.
func (s *PG) GetRepoUpstream(ctx context.Context, repoID string) (RepoUpstream, error) {
	assertf(repoID != "", "GetRepoUpstream: empty repo id")
	up, err := scanRepoUpstream(s.pool.QueryRow(ctx,
		`SELECT `+repoUpstreamCols+` FROM repo_upstreams WHERE repo_id = $1`, repoID))
	if errors.Is(err, pgx.ErrNoRows) {
		return RepoUpstream{}, fmt.Errorf("upstream for repo %q: %w", repoID, ErrNotFound)
	}
	if err != nil {
		return RepoUpstream{}, fmt.Errorf("get upstream for repo %q: %w", repoID, err)
	}
	return up, nil
}

// DeleteRepoUpstream detaches the repo; ErrNotFound when already detached.
func (s *PG) DeleteRepoUpstream(ctx context.Context, repoID string) error {
	assertf(repoID != "", "DeleteRepoUpstream: empty repo id")
	tag, err := s.pool.Exec(ctx, `DELETE FROM repo_upstreams WHERE repo_id = $1`, repoID)
	if err != nil {
		return fmt.Errorf("delete upstream for repo %q: %w", repoID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("upstream for repo %q: %w", repoID, ErrNotFound)
	}
	assertf(tag.RowsAffected() == 1, "DELETE by primary key affected %d rows", tag.RowsAffected())
	return nil
}

// TryBeginSync atomically claims a sync run (syncing false -> true).
// claimed=false with the current row when a run is already in flight;
// ErrNotFound when detached.
func (s *PG) TryBeginSync(ctx context.Context, repoID string) (RepoUpstream, bool, error) {
	assertf(repoID != "", "TryBeginSync: empty repo id")
	up, err := scanRepoUpstream(s.pool.QueryRow(ctx,
		`UPDATE repo_upstreams SET syncing = true
		 WHERE repo_id = $1 AND NOT syncing
		 RETURNING `+repoUpstreamCols, repoID))
	if err == nil {
		assertf(up.Syncing, "claimed row %q returned syncing=false", repoID)
		return up, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return RepoUpstream{}, false, fmt.Errorf("begin sync for repo %q: %w", repoID, err)
	}
	// No claim: distinguish in-flight (return the current row) from
	// detached (ErrNotFound). A concurrent FinishSync or detach between the
	// two statements linearizes as happening before this call, so the
	// not-claimed answer stays correct.
	up, err = s.GetRepoUpstream(ctx, repoID)
	if err != nil {
		return RepoUpstream{}, false, err
	}
	return up, false, nil
}

// FinishSync completes the claimed run: increments RunCount (and
// SuccessCount when success), clears Syncing, returns the updated row.
// ErrNotFound when the repo was detached mid-run — or when no run is in
// flight: the `AND syncing` guard keeps a stale finisher (detached mid-run,
// then re-attached) from polluting the fresh row's counters.
func (s *PG) FinishSync(ctx context.Context, repoID string, success bool) (RepoUpstream, error) {
	assertf(repoID != "", "FinishSync: empty repo id")
	up, err := scanRepoUpstream(s.pool.QueryRow(ctx,
		`UPDATE repo_upstreams
		 SET syncing = false, run_count = run_count + 1,
		     success_count = success_count + CASE WHEN $2 THEN 1 ELSE 0 END
		 WHERE repo_id = $1 AND syncing = true
		 RETURNING `+repoUpstreamCols, repoID, success))
	if errors.Is(err, pgx.ErrNoRows) {
		return RepoUpstream{}, fmt.Errorf("upstream for repo %q: %w", repoID, ErrNotFound)
	}
	if err != nil {
		return RepoUpstream{}, fmt.Errorf("finish sync for repo %q: %w", repoID, err)
	}
	return up, nil
}
