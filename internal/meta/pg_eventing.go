// Phase-5 eventing/sync persistence on Postgres (contract and semantics in
// eventing.go; sqlite_eventing.go mirrors this file 1:1).

package meta

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	// gitCredentialRepoConstraint is the named UNIQUE(repo_id) constraint:
	// one credential per repo (docs/reference/http-api.md: 409).
	gitCredentialRepoConstraint = "git_credentials_repo_id_key"
	// Scan column lists matching scanWebhook / scanGitCredential /
	// scanRepoUpstream (shared with sqlite_eventing.go like repoCols).
	webhookCols       = "id, org_id, url, secret, created_at"
	gitCredentialCols = "id, repo_id, username, password, created_at"
	repoUpstreamCols  = "repo_id, provider, owner, name, upstream_host, run_count, success_count, syncing, sync_pending, sync_claim_holder, sync_claim_expires_at, forward_on_push, push_pending, push_syncing, push_claim_holder, push_claim_expires_at, push_claim_force, push_run_count, push_success_count, default_branch_omitted, created_at"
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

// scanRepoUpstream scans one row of repoUpstreamCols; timestamps in UTC.
func scanRepoUpstream(row pgx.Row) (RepoUpstream, error) {
	var up RepoUpstream
	if err := row.Scan(&up.RepoID, &up.Provider, &up.Owner, &up.Name, &up.UpstreamHost,
		&up.RunCount, &up.SuccessCount, &up.Syncing, &up.SyncPending,
		&up.SyncClaimHolder, &up.SyncClaimExpiresAt,
		&up.ForwardOnPush, &up.PushPending, &up.PushSyncing,
		&up.PushClaimHolder, &up.PushClaimExpiresAt, &up.PushClaimForce,
		&up.PushRunCount, &up.PushSuccessCount, &up.DefaultBranchOmitted, &up.CreatedAt); err != nil {
		return RepoUpstream{}, err
	}
	up.SyncClaimExpiresAt = up.SyncClaimExpiresAt.UTC()
	up.PushClaimExpiresAt = up.PushClaimExpiresAt.UTC()
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
	if err := pgUnknownText("webhook", id); err != nil {
		return Webhook{}, err
	}
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
	if err := pgUnknownText("webhook", id); err != nil {
		return err
	}
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
	return s.createGitCredential(ctx, s.pool, cred)
}

func (s *PG) createGitCredential(ctx context.Context, q pgQueryRower, cred GitCredential) (GitCredential, error) {
	assertf(cred.RepoID != "", "CreateGitCredential: empty repo id")
	stored, err := encodeSecret(s.codec, cred.Password)
	if err != nil {
		return GitCredential{}, fmt.Errorf("create credential for repo %q: %w", cred.RepoID, err)
	}
	cred.ID = NewEventingID("ggc")
	// The insert is guarded on a LIVE repos row, locked FOR UPDATE inside
	// the statement (issue #52, PR #105 round 6): a request that resolved
	// the repo before its soft-delete could otherwise insert after the
	// purge deleted this table's rows, permanently attaching a secret to
	// a purged repository. The row lock totally orders the insert against
	// SoftDeleteRepo/PurgeRepoMeta — it either commits while the repo is
	// live (the purge then deletes it) or re-evaluates to zero rows.
	err = q.QueryRow(ctx,
		`INSERT INTO git_credentials (id, repo_id, username, password)
		 SELECT $1, $2, $3, $4
		 WHERE EXISTS (SELECT 1 FROM repos WHERE repo_id = $2 AND deleted_at IS NULL FOR UPDATE)
		 RETURNING created_at`,
		cred.ID, cred.RepoID, cred.Username, stored).Scan(&cred.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return GitCredential{}, fmt.Errorf("credential for deleted or unknown repo %q: %w", cred.RepoID, ErrNotFound)
	}
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
	if err := pgUnknownText("credential", id); err != nil {
		return GitCredential{}, err
	}
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
	if err := pgUnknownText("credential", id); err != nil {
		return GitCredential{}, err
	}
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
	if err := pgUnknownText("credential", id); err != nil {
		return GitCredential{}, err
	}
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
	if err := pgUnknownText("credential", id); err != nil {
		return err
	}
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
// counters/syncing/sync_pending/claim lease zeroed). ErrAlreadyExists when
// the repo already has one.
func (s *PG) SetRepoUpstream(ctx context.Context, up RepoUpstream) (RepoUpstream, error) {
	return pgSetRepoUpstream(ctx, s.pool, up)
}

// CreateLinkedRepo runs CreateRepo, CreateGitCredential and
// SetRepoUpstream in one transaction (meta.EventingStore contract). A
// concurrent insert of the same (org, url) blocks on the unique index
// until this commits, so its conflict path reads the upstream too.
func (s *PG) CreateLinkedRepo(ctx context.Context, org, url, defaultBranch string, up RepoUpstream, cred *GitCredential) (Repo, error) {
	assertf(url != "" && defaultBranch != "", "CreateLinkedRepo: empty url %q or default branch %q", url, defaultBranch)
	orgID, err := s.lookupOrgID(ctx, org)
	if err != nil {
		return Repo{}, err
	}
	var repo Repo
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var err error
		if repo, err = pgInsertRepo(ctx, tx, org, orgID, url, defaultBranch, CreateStatePending); err != nil {
			return err
		}
		if cred != nil {
			c := *cred
			c.RepoID = repo.RepoID
			if _, err := s.createGitCredential(ctx, tx, c); err != nil {
				return err
			}
		}
		up.RepoID = repo.RepoID
		_, err = pgSetRepoUpstream(ctx, tx, up)
		return err
	})
	if err != nil {
		return Repo{}, err
	}
	return repo, nil
}

// AdoptUpstreamDefaultBranch (meta.EventingStore contract). The repos row
// is locked first, as UpdateRepoDefaultBranch does, so a concurrent PATCH
// either clears the flag before this reads it or waits for this commit.
func (s *PG) AdoptUpstreamDefaultBranch(ctx context.Context, repoID, branch string) (bool, error) {
	assertf(repoID != "" && branch != "", "AdoptUpstreamDefaultBranch: empty repo id %q or branch %q", repoID, branch)
	adopted := false
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var one int
		err := tx.QueryRow(ctx,
			`SELECT 1 FROM repos WHERE repo_id = $1 AND deleted_at IS NULL FOR UPDATE`, repoID).Scan(&one)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("adopt default branch of repo %q: %w", repoID, ErrNotFound)
		}
		if err != nil {
			return err
		}
		ct, err := tx.Exec(ctx,
			`UPDATE repo_upstreams SET default_branch_omitted = false WHERE repo_id = $1 AND default_branch_omitted`, repoID)
		if err != nil || ct.RowsAffected() == 0 {
			return err
		}
		ct, err = tx.Exec(ctx, `UPDATE repos SET default_branch = $2 WHERE repo_id = $1 AND default_branch = $3`,
			repoID, branch, OmittedDefaultBranch)
		if err != nil {
			return err
		}
		n := ct.RowsAffected()
		assertf(n == 0 || n == 1, "adopt default branch of locked repo %q affected %d rows", repoID, n)
		adopted = n == 1
		return nil
	})
	if err != nil {
		return false, err
	}
	return adopted, nil
}

func pgSetRepoUpstream(ctx context.Context, q pgQueryRower, up RepoUpstream) (RepoUpstream, error) {
	assertf(up.RepoID != "", "SetRepoUpstream: empty repo id")
	up.RunCount, up.SuccessCount, up.Syncing, up.SyncPending = 0, 0, false, false
	up.SyncClaimHolder, up.SyncClaimExpiresAt = "", time.Unix(0, 0).UTC()
	up.PushPending, up.PushSyncing, up.PushRunCount, up.PushSuccessCount = 0, false, 0, 0
	up.PushClaimHolder, up.PushClaimExpiresAt, up.PushClaimForce = "", time.Unix(0, 0).UTC(), false
	// Live-repo guard with FOR UPDATE — see CreateGitCredential (issue
	// #52, PR #105 round 6): no upstream link may attach to a deleted or
	// purged repository.
	err := q.QueryRow(ctx,
		`INSERT INTO repo_upstreams (repo_id, provider, owner, name, upstream_host, forward_on_push, default_branch_omitted)
		 SELECT $1, $2, $3, $4, $5, $6, $7
		 WHERE EXISTS (SELECT 1 FROM repos WHERE repo_id = $1 AND deleted_at IS NULL FOR UPDATE)
		 RETURNING created_at`,
		up.RepoID, up.Provider, up.Owner, up.Name, up.UpstreamHost, up.ForwardOnPush, up.DefaultBranchOmitted).Scan(&up.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return RepoUpstream{}, fmt.Errorf("upstream for deleted or unknown repo %q: %w", up.RepoID, ErrNotFound)
	}
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

// PGRequestSync is RequestSync inside the caller's transaction. It locks
// the upstream row, so a concurrent detach either waits for the commit or
// makes this fail with ErrNotFound (issue #388: a sync create must not
// complete for a detached repo).
func PGRequestSync(ctx context.Context, tx pgx.Tx, repoID string) error {
	assertf(tx != nil, "PGRequestSync: nil tx")
	return pgRequestSync(ctx, tx, repoID)
}

// RequestSync marks a pending sync request; ErrNotFound when detached.
// See the EventingStore contract (eventing.go) for the pending-flag
// protocol (issue #79).
func (s *PG) RequestSync(ctx context.Context, repoID string) error {
	return pgRequestSync(ctx, s.pool, repoID)
}

// pgExecer is the Exec method *pgxpool.Pool and pgx.Tx share.
type pgExecer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func pgRequestSync(ctx context.Context, q pgExecer, repoID string) error {
	assertf(repoID != "", "RequestSync: empty repo id")
	tag, err := q.Exec(ctx,
		`UPDATE repo_upstreams SET sync_pending = true WHERE repo_id = $1`, repoID)
	if err != nil {
		return fmt.Errorf("request sync for repo %q: %w", repoID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("upstream for repo %q: %w", repoID, ErrNotFound)
	}
	assertf(tag.RowsAffected() == 1, "UPDATE by primary key affected %d rows", tag.RowsAffected())
	return nil
}

// TryBeginSync atomically claims a sync run (syncing false -> true) under
// a lease and consumes sync_pending (the claimed run's fetch postdates
// every request marked before the claim). An EXPIRED claim is stolen in
// the same guarded UPDATE — the row lock taken by the initial SELECT FOR
// UPDATE makes the read-check-write atomic against concurrent claimants
// (issue #86: the only recovery for a holder that died mid-run). Both
// sides of the expiry comparison evaluate on the database clock, so node
// clock skew does not participate. claimed=false with the current row when
// an unexpired run is in flight; ErrNotFound when detached.
func (s *PG) TryBeginSync(ctx context.Context, repoID, holder string, lease time.Duration) (RepoUpstream, bool, string, error) {
	assertf(repoID != "", "TryBeginSync: empty repo id")
	assertf(holder != "", "TryBeginSync: empty holder")
	assertf(lease > 0, "TryBeginSync: non-positive lease %v", lease)
	var up RepoUpstream
	var claimed bool
	var stolenFrom string
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		prev, err := scanRepoUpstream(tx.QueryRow(ctx,
			`SELECT `+repoUpstreamCols+` FROM repo_upstreams WHERE repo_id = $1 FOR UPDATE`, repoID))
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("upstream for repo %q: %w", repoID, ErrNotFound)
		}
		if err != nil {
			return err
		}
		// The row is locked: the claim decision runs in Go on consistent
		// state, and every write below is unconditional on the locked
		// row. Expiry compares on the DATABASE clock, matching the
		// now()-derived expiry writes — node clock skew never participates.
		var dbNow time.Time
		if err := tx.QueryRow(ctx, `SELECT now()`).Scan(&dbNow); err != nil {
			return err
		}
		if prev.Syncing && prev.SyncClaimExpiresAt.After(dbNow) {
			up = prev // unexpired claim in flight: refused, current row
			return nil
		}
		if prev.Syncing && prev.SyncClaimHolder == LegacyClaimHolder && !legacyStealEnabled() {
			// A lapsed adoption grace still is not proof against a
			// paused pre-lease binary, which carries none of the holder
			// guards — stealing needs the operator's drained-fleet
			// assertion (EnvSyncStealLegacy).
			up = prev
			return nil
		}
		if prev.Syncing && prev.SyncClaimHolder == "" {
			// Legacy claim (pre-lease replica, mixed fleet): adopt with a
			// full grace instead of stealing what may be a LIVE run — the
			// old replica's FinishSync clears it if so; only an unclaimed
			// expiry after the grace proves the holder dead.
			row, err := scanRepoUpstream(tx.QueryRow(ctx,
				`UPDATE repo_upstreams
				 SET sync_claim_holder = $2, sync_claim_expires_at = now() + make_interval(secs => $3)
				 WHERE repo_id = $1
				 RETURNING `+repoUpstreamCols, repoID, LegacyClaimHolder, lease.Seconds()))
			if err != nil {
				return err
			}
			up = row
			return nil
		}
		row, err := scanRepoUpstream(tx.QueryRow(ctx,
			`UPDATE repo_upstreams
			 SET syncing = true, sync_pending = false,
			     sync_claim_holder = $2,
			     sync_claim_expires_at = now() + make_interval(secs => $3)
			 WHERE repo_id = $1
			 RETURNING `+repoUpstreamCols, repoID, holder, lease.Seconds()))
		if err != nil {
			return err
		}
		assertf(row.Syncing && row.SyncClaimHolder == holder,
			"claimed row %q returned holder %q syncing=%v", repoID, row.SyncClaimHolder, row.Syncing)
		up, claimed = row, true
		if prev.Syncing {
			stolenFrom = prev.SyncClaimHolder // expired lease (or lapsed grace) stolen
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return RepoUpstream{}, false, "", err
		}
		return RepoUpstream{}, false, "", fmt.Errorf("begin sync for repo %q: %w", repoID, err)
	}
	return up, claimed, stolenFrom, nil
}

// FinishSync completes the run claimed by holder: increments RunCount (and
// SuccessCount when success), clears Syncing and the claim lease, returns
// the updated row. ErrNotFound when the repo was detached mid-run — or
// when no run is in flight: the `AND syncing` guard keeps a stale finisher
// (detached mid-run, then re-attached) from polluting the fresh row's
// counters. ErrClaimLost when the row is claimed by a DIFFERENT holder:
// the caller's expired claim was stolen (issue #86), and the late
// finisher must not clear the thief's claim or touch the counters.
func (s *PG) FinishSync(ctx context.Context, repoID, holder string, success bool) (RepoUpstream, error) {
	assertf(repoID != "", "FinishSync: empty repo id")
	assertf(holder != "", "FinishSync: empty holder")
	up, err := scanRepoUpstream(s.pool.QueryRow(ctx,
		`UPDATE repo_upstreams
		 SET syncing = false, run_count = run_count + 1,
		     success_count = success_count + CASE WHEN $3 THEN 1 ELSE 0 END,
		     sync_claim_holder = '', sync_claim_expires_at = 'epoch'
		 WHERE repo_id = $1 AND syncing = true AND sync_claim_holder = $2
		 RETURNING `+repoUpstreamCols, repoID, holder, success))
	if err == nil {
		return up, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return RepoUpstream{}, fmt.Errorf("finish sync for repo %q: %w", repoID, err)
	}
	// Guard miss: classify. A concurrent steal between the UPDATE and this
	// read can only shift the answer between two abandon paths, so the
	// approximation is harmless.
	switch cur, err := s.GetRepoUpstream(ctx, repoID); {
	case errors.Is(err, ErrNotFound):
		return RepoUpstream{}, err
	case err != nil:
		return RepoUpstream{}, fmt.Errorf("finish sync for repo %q: %w", repoID, err)
	case cur.Syncing && cur.SyncClaimHolder != holder:
		return RepoUpstream{}, fmt.Errorf("upstream for repo %q now held by %q: %w",
			repoID, cur.SyncClaimHolder, ErrClaimLost)
	default:
		return RepoUpstream{}, fmt.Errorf("upstream for repo %q: %w", repoID, ErrNotFound)
	}
}

// RunRestoreStore ends runs that serve's shutdown cancelled without counting
// them (issue #343). Only *PG implements it: SQLite and S3WAL restore
// through LocalSchedule (ADR-50, ADR-51).
type RunRestoreStore interface {
	RestoreSync(ctx context.Context, repoID, holder string) (RepoUpstream, error)
	RestorePush(ctx context.Context, repoID, holder string, force bool) (RepoUpstream, error)
}

var _ RunRestoreStore = (*PG)(nil)

// RestoreSync ends the run claimed by holder without counting it (issue
// #343): serve's shutdown cancelled it. The claim is released and the
// request re-marked, so the next run redoes it. Errors as FinishSync.
func (s *PG) RestoreSync(ctx context.Context, repoID, holder string) (RepoUpstream, error) {
	assertf(repoID != "", "RestoreSync: empty repo id")
	assertf(holder != "", "RestoreSync: empty holder")
	up, err := scanRepoUpstream(s.pool.QueryRow(ctx,
		`UPDATE repo_upstreams
		 SET syncing = false, sync_pending = true,
		     sync_claim_holder = '', sync_claim_expires_at = 'epoch'
		 WHERE repo_id = $1 AND syncing = true AND sync_claim_holder = $2
		 RETURNING `+repoUpstreamCols, repoID, holder))
	if err == nil {
		return up, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return RepoUpstream{}, fmt.Errorf("restore sync for repo %q: %w", repoID, err)
	}
	return RepoUpstream{}, s.classifyGuardMiss(ctx, repoID, holder, "sync", func(cur RepoUpstream) (bool, string) {
		return cur.Syncing, cur.SyncClaimHolder
	})
}

// RestorePush is RestoreSync on the push_* lane: the claim is released
// and the run's consumed level (force when force) re-marked; levels only
// coalesce upward. The attempt lives in the River job, which snoozes.
func (s *PG) RestorePush(ctx context.Context, repoID, holder string, force bool) (RepoUpstream, error) {
	assertf(repoID != "", "RestorePush: empty repo id")
	assertf(holder != "", "RestorePush: empty holder")
	level := 1
	if force {
		level = 2
	}
	up, err := scanRepoUpstream(s.pool.QueryRow(ctx,
		`UPDATE repo_upstreams
		 SET push_syncing = false, push_pending = GREATEST(push_pending, $3),
		     push_claim_holder = '', push_claim_expires_at = 'epoch', push_claim_force = false
		 WHERE repo_id = $1 AND push_syncing = true AND push_claim_holder = $2
		 RETURNING `+repoUpstreamCols, repoID, holder, level))
	if err == nil {
		return up, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return RepoUpstream{}, fmt.Errorf("restore push for repo %q: %w", repoID, err)
	}
	return RepoUpstream{}, s.classifyGuardMiss(ctx, repoID, holder, "push", func(cur RepoUpstream) (bool, string) {
		return cur.PushSyncing, cur.PushClaimHolder
	})
}

// classifyGuardMiss explains a holder-guarded UPDATE that matched no row,
// as FinishSync does: ErrClaimLost when another holder owns the lane,
// ErrNotFound otherwise.
func (s *PG) classifyGuardMiss(ctx context.Context, repoID, holder, lane string, claimOf func(RepoUpstream) (bool, string)) error {
	cur, err := s.GetRepoUpstream(ctx, repoID)
	if errors.Is(err, ErrNotFound) {
		return err
	}
	if err != nil {
		return fmt.Errorf("restore %s for repo %q: %w", lane, repoID, err)
	}
	if held, by := claimOf(cur); held && by != holder {
		return fmt.Errorf("upstream for repo %q now held by %q: %w", repoID, by, ErrClaimLost)
	}
	return fmt.Errorf("upstream for repo %q: %w", repoID, ErrNotFound)
}

// RequestPush raises the outbound pending level (issue #90); levels only
// coalesce upward so force is never downgraded. ErrNotFound when detached.
func (s *PG) RequestPush(ctx context.Context, repoID string, force bool) error {
	assertf(repoID != "", "RequestPush: empty repo id")
	level := 1
	if force {
		level = 2
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE repo_upstreams SET push_pending = GREATEST(push_pending, $2) WHERE repo_id = $1`,
		repoID, level)
	if err != nil {
		return fmt.Errorf("request push for repo %q: %w", repoID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("upstream for repo %q: %w", repoID, ErrNotFound)
	}
	assertf(tag.RowsAffected() == 1, "UPDATE by primary key affected %d rows", tag.RowsAffected())
	return nil
}

// PGForwardsOnPush reports, inside the caller's transaction and without
// a lock, whether repoID has an upstream link with forward_on_push.
func PGForwardsOnPush(ctx context.Context, tx pgx.Tx, repoID string) (bool, error) {
	assertf(tx != nil, "PGForwardsOnPush: nil tx")
	assertf(repoID != "", "PGForwardsOnPush: empty repo id")
	var forwards bool
	err := tx.QueryRow(ctx, `SELECT forward_on_push FROM repo_upstreams WHERE repo_id = $1`, repoID).Scan(&forwards)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read forward_on_push for repo %q: %w", repoID, err)
	}
	return forwards, nil
}

// PGRequestForward is RequestPush(repoID, false) inside the caller's
// transaction for a repository that forwards on push. It reports whether
// the repository has an upstream link with forward_on_push; otherwise it
// writes nothing. The marked row stays locked until the caller commits.
func PGRequestForward(ctx context.Context, tx pgx.Tx, repoID string) (bool, error) {
	assertf(tx != nil, "PGRequestForward: nil tx")
	assertf(repoID != "", "PGRequestForward: empty repo id")
	tag, err := tx.Exec(ctx,
		`UPDATE repo_upstreams SET push_pending = GREATEST(push_pending, 1) WHERE repo_id = $1 AND forward_on_push`,
		repoID)
	if err != nil {
		return false, fmt.Errorf("request forward for repo %q: %w", repoID, err)
	}
	assertf(tag.RowsAffected() <= 1, "UPDATE by primary key affected %d rows", tag.RowsAffected())
	return tag.RowsAffected() == 1, nil
}

// pgPushWork and pgPullWork are the claim guards of TryBeginPush and
// TryBeginSync, evaluated now on the database clock: an unclaimed lane with
// a pending request, or an expired claim, which holds the request it
// consumed. A lapsed legacy grace is claimable only with the operator's
// steal assertion, so without it the lane is not work. They are
// ListLocalWork's guards without SQLite's due times: on PostgreSQL a
// backoff is a scheduled River job. $1 is LegacyClaimHolder and $2
// legacyStealEnabled().
const (
	pgPushWork = `((NOT u.push_syncing AND u.push_pending > 0)
		OR (u.push_syncing AND u.push_claim_expires_at <= now()))`
	pgPullWork = `((NOT u.syncing AND u.sync_pending)
		OR (u.syncing AND u.sync_claim_expires_at <= now()
		    AND (u.sync_claim_holder <> $1 OR $2)))`
)

// PGListLaneWork returns at most limit lanes of org's live repos whose
// claim guard holds now (pgPushWork, pgPullWork), ordered by RepoID
// strictly after after. A page shorter than limit ends a traversal. It
// takes no locks. River's lane scan checks each lane again with PGLaneWork
// after it inserts the lane's job.
func PGListLaneWork(ctx context.Context, tx pgx.Tx, org, after string, limit int) ([]LocalWork, error) {
	assertf(tx != nil && org != "" && limit > 0, "PGListLaneWork: nil tx, empty org %q or non-positive limit %d", org, limit)
	rows, err := tx.Query(ctx, `SELECT u.repo_id, `+pgPushWork+`, `+pgPullWork+`
		FROM repo_upstreams u
		JOIN repos r ON r.repo_id = u.repo_id AND r.deleted_at IS NULL
		JOIN orgs o ON o.id = r.org_id AND o.name = $3
		WHERE u.repo_id > $4 AND (`+pgPushWork+` OR `+pgPullWork+`)
		ORDER BY u.repo_id LIMIT $5`,
		LegacyClaimHolder, legacyStealEnabled(), org, after, limit)
	if err != nil {
		return nil, fmt.Errorf("list lane work: %w", err)
	}
	defer rows.Close()
	var work []LocalWork
	for rows.Next() {
		var w LocalWork
		if err := rows.Scan(&w.RepoID, &w.Push, &w.Pull); err != nil {
			return nil, fmt.Errorf("list lane work: %w", err)
		}
		assertf(w.Push || w.Pull, "listed %q without lane work", w.RepoID)
		work = append(work, w)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list lane work: %w", err)
	}
	assertf(len(work) <= limit, "PGListLaneWork returned %d > %d rows", len(work), limit)
	return work, nil
}

// PGLaneWork evaluates PGListLaneWork's guards for repoID inside the
// caller's transaction, without locks. A repo that is deleted or has no
// upstream link has no lane work.
func PGLaneWork(ctx context.Context, tx pgx.Tx, repoID string) (LocalWork, error) {
	assertf(tx != nil && repoID != "", "PGLaneWork: nil tx or empty repo id")
	w := LocalWork{RepoID: repoID}
	err := tx.QueryRow(ctx, `SELECT `+pgPushWork+`, `+pgPullWork+`
		FROM repo_upstreams u
		JOIN repos r ON r.repo_id = u.repo_id AND r.deleted_at IS NULL
		WHERE u.repo_id = $3`,
		LegacyClaimHolder, legacyStealEnabled(), repoID).Scan(&w.Push, &w.Pull)
	if errors.Is(err, pgx.ErrNoRows) {
		return w, nil
	}
	if err != nil {
		return LocalWork{}, fmt.Errorf("read lane work of repo %q: %w", repoID, err)
	}
	return w, nil
}

// TryBeginPush is TryBeginSync on the push_* lane (issue #90): row-locked
// read-check-write on the database clock, expired leases stolen atomically.
// No legacy adoption exists here — push claims only ever come from
// #90-aware binaries, so an expired lease is always provably dead. A steal
// inherits the dead claim's persisted force mandate (push_claim_force), so
// a crash between claim and finish cannot downgrade an accepted forced
// request.
func (s *PG) TryBeginPush(ctx context.Context, repoID, holder string, lease time.Duration) (RepoUpstream, bool, bool, string, error) {
	assertf(repoID != "", "TryBeginPush: empty repo id")
	assertf(holder != "", "TryBeginPush: empty holder")
	assertf(lease > 0, "TryBeginPush: non-positive lease %v", lease)
	var up RepoUpstream
	var claimed, force bool
	var stolenFrom string
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		prev, err := scanRepoUpstream(tx.QueryRow(ctx,
			`SELECT `+repoUpstreamCols+` FROM repo_upstreams WHERE repo_id = $1 FOR UPDATE`, repoID))
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("upstream for repo %q: %w", repoID, ErrNotFound)
		}
		if err != nil {
			return err
		}
		var dbNow time.Time
		if err := tx.QueryRow(ctx, `SELECT now()`).Scan(&dbNow); err != nil {
			return err
		}
		if prev.PushSyncing && prev.PushClaimExpiresAt.After(dbNow) {
			up = prev // unexpired claim in flight: refused, current row
			return nil
		}
		force = prev.PushPending == 2 || (prev.PushSyncing && prev.PushClaimForce)
		row, err := scanRepoUpstream(tx.QueryRow(ctx,
			`UPDATE repo_upstreams
			 SET push_syncing = true, push_pending = 0,
			     push_claim_holder = $2,
			     push_claim_expires_at = now() + make_interval(secs => $3),
			     push_claim_force = $4
			 WHERE repo_id = $1
			 RETURNING `+repoUpstreamCols, repoID, holder, lease.Seconds(), force))
		if err != nil {
			return err
		}
		assertf(row.PushSyncing && row.PushClaimHolder == holder,
			"claimed row %q returned holder %q push_syncing=%v", repoID, row.PushClaimHolder, row.PushSyncing)
		up, claimed = row, true
		if prev.PushSyncing {
			stolenFrom = prev.PushClaimHolder // expired lease stolen
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return RepoUpstream{}, false, false, "", err
		}
		return RepoUpstream{}, false, false, "", fmt.Errorf("begin push for repo %q: %w", repoID, err)
	}
	return up, claimed, force, stolenFrom, nil
}

// FinishPush is FinishSync on the push_* lane: holder-guarded completion,
// per-direction counters, ErrClaimLost for stolen claims.
func (s *PG) FinishPush(ctx context.Context, repoID, holder string, success bool) (RepoUpstream, error) {
	assertf(repoID != "", "FinishPush: empty repo id")
	assertf(holder != "", "FinishPush: empty holder")
	up, err := scanRepoUpstream(s.pool.QueryRow(ctx,
		`UPDATE repo_upstreams
		 SET push_syncing = false, push_run_count = push_run_count + 1,
		     push_success_count = push_success_count + CASE WHEN $3 THEN 1 ELSE 0 END,
		     push_claim_holder = '', push_claim_expires_at = 'epoch', push_claim_force = false
		 WHERE repo_id = $1 AND push_syncing = true AND push_claim_holder = $2
		 RETURNING `+repoUpstreamCols, repoID, holder, success))
	if err == nil {
		return up, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return RepoUpstream{}, fmt.Errorf("finish push for repo %q: %w", repoID, err)
	}
	// Guard miss: classify (same harmless approximation as FinishSync).
	switch cur, err := s.GetRepoUpstream(ctx, repoID); {
	case errors.Is(err, ErrNotFound):
		return RepoUpstream{}, err
	case err != nil:
		return RepoUpstream{}, fmt.Errorf("finish push for repo %q: %w", repoID, err)
	case cur.PushSyncing && cur.PushClaimHolder != holder:
		return RepoUpstream{}, fmt.Errorf("upstream for repo %q now held by %q: %w",
			repoID, cur.PushClaimHolder, ErrClaimLost)
	default:
		return RepoUpstream{}, fmt.Errorf("upstream for repo %q: %w", repoID, ErrNotFound)
	}
}
