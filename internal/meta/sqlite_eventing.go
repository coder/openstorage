// Phase-5 eventing/sync persistence on SQLite. Semantics mirror
// pg_eventing.go 1:1 (contract in eventing.go); storage conventions follow
// sqlite.go's file comment: INTEGER unixnano timestamps written Go-side,
// INTEGER 0/1 booleans, and BEGIN IMMEDIATE write transactions via _txlock
// where multi-statement atomicity is needed (TryBeginSync / FinishSync).

package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// isPrimaryKeyViolation reports SQLITE_CONSTRAINT_PRIMARYKEY (duplicate
// PRIMARY KEY insert — a distinct extended code from UNIQUE-column
// violations, see CreateRepo's comment).
func isPrimaryKeyViolation(err error) bool {
	var serr *sqlite.Error
	return errors.As(err, &serr) && serr.Code() == sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY
}

// scanWebhookSQLite scans one row of webhookCols, converting the INTEGER
// unixnano timestamp back to UTC time.Time.
func scanWebhookSQLite(row sqlRow) (Webhook, error) {
	var w Webhook
	var createdAt int64
	if err := row.Scan(&w.ID, &w.OrgID, &w.URL, &w.Secret, &createdAt); err != nil {
		return Webhook{}, err
	}
	w.CreatedAt = time.Unix(0, createdAt).UTC()
	return w, nil
}

// scanGitCredentialSQLite scans one row of gitCredentialCols.
func scanGitCredentialSQLite(row sqlRow) (GitCredential, error) {
	var c GitCredential
	var createdAt int64
	if err := row.Scan(&c.ID, &c.RepoID, &c.Username, &c.Password, &createdAt); err != nil {
		return GitCredential{}, err
	}
	c.CreatedAt = time.Unix(0, createdAt).UTC()
	return c, nil
}

// scanRepoUpstreamSQLite scans one row of repoUpstreamCols (syncing scans
// from INTEGER 0/1 into bool).
func scanRepoUpstreamSQLite(row sqlRow) (RepoUpstream, error) {
	var up RepoUpstream
	var createdAt int64
	if err := row.Scan(&up.RepoID, &up.Provider, &up.Owner, &up.Name, &up.UpstreamHost,
		&up.RunCount, &up.SuccessCount, &up.Syncing, &createdAt); err != nil {
		return RepoUpstream{}, err
	}
	up.CreatedAt = time.Unix(0, createdAt).UTC()
	return up, nil
}

// CreateWebhook inserts a webhook with a server-generated ID.
// ErrNotFound when the org is unknown.
func (s *SQLite) CreateWebhook(ctx context.Context, org, url, secret string) (Webhook, error) {
	assertf(url != "", "CreateWebhook: empty url for org %q", org)
	orgID, err := s.lookupOrgID(ctx, org)
	if err != nil {
		return Webhook{}, err
	}
	nanos, createdAt := nowNano()
	w := Webhook{ID: NewEventingID("wh"), OrgID: orgID, URL: url, Secret: secret, CreatedAt: createdAt}
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO webhooks (id, org_id, url, secret, created_at) VALUES (?, ?, ?, ?, ?)`,
		w.ID, orgID, url, secret, nanos); err != nil {
		return Webhook{}, fmt.Errorf("create webhook for org %q: %w", org, err)
	}
	return w, nil
}

// ListWebhooks returns the org's webhooks in registration order.
// ErrNotFound when the org is unknown.
func (s *SQLite) ListWebhooks(ctx context.Context, org string) ([]Webhook, error) {
	orgID, err := s.lookupOrgID(ctx, org)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+webhookCols+` FROM webhooks WHERE org_id = ? ORDER BY created_at, id`, orgID)
	if err != nil {
		return nil, fmt.Errorf("list webhooks for org %q: %w", org, err)
	}
	defer rows.Close()
	hooks := []Webhook{}
	for rows.Next() {
		w, err := scanWebhookSQLite(rows)
		if err != nil {
			return nil, err
		}
		hooks = append(hooks, w)
	}
	return hooks, rows.Err()
}

// GetWebhook returns the webhook with the given ID regardless of org
// (delivery jobs carry only the ID); ErrNotFound when absent.
func (s *SQLite) GetWebhook(ctx context.Context, id string) (Webhook, error) {
	assertf(id != "", "GetWebhook: empty id")
	w, err := scanWebhookSQLite(s.db.QueryRowContext(ctx,
		`SELECT `+webhookCols+` FROM webhooks WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Webhook{}, fmt.Errorf("webhook %q: %w", id, ErrNotFound)
	}
	if err != nil {
		return Webhook{}, fmt.Errorf("get webhook %q: %w", id, err)
	}
	return w, nil
}

// DeleteWebhook removes the org's webhook; ErrNotFound when the org is
// unknown or the ID does not belong to it.
func (s *SQLite) DeleteWebhook(ctx context.Context, org, id string) error {
	assertf(id != "", "DeleteWebhook: empty id for org %q", org)
	orgID, err := s.lookupOrgID(ctx, org)
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM webhooks WHERE org_id = ? AND id = ?`, orgID, id)
	if err != nil {
		return fmt.Errorf("delete webhook %q for org %q: %w", id, org, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete webhook %q for org %q: %w", id, org, err)
	}
	if n == 0 {
		return fmt.Errorf("webhook %q for org %q: %w", id, org, ErrNotFound)
	}
	assertf(n == 1, "DELETE by primary key affected %d rows", n)
	return nil
}

// CreateGitCredential stores cred (ID/CreatedAt assigned by the store).
// ErrAlreadyExists when the repo already has a credential.
func (s *SQLite) CreateGitCredential(ctx context.Context, cred GitCredential) (GitCredential, error) {
	assertf(cred.RepoID != "", "CreateGitCredential: empty repo id")
	nanos, createdAt := nowNano()
	cred.ID = NewEventingID("ggc")
	cred.CreatedAt = createdAt
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO git_credentials (id, repo_id, username, password, created_at) VALUES (?, ?, ?, ?, ?)`,
		cred.ID, cred.RepoID, cred.Username, cred.Password, nanos)
	// A unique violation here can only be repo_id: an id collision would be
	// SQLITE_CONSTRAINT_PRIMARYKEY, a different extended code.
	if isUniqueViolation(err) {
		return GitCredential{}, fmt.Errorf("credential for repo %q: %w", cred.RepoID, ErrAlreadyExists)
	}
	if err != nil {
		return GitCredential{}, fmt.Errorf("create credential for repo %q: %w", cred.RepoID, err)
	}
	return cred, nil
}

// GetGitCredential returns the credential by ID; ErrNotFound when absent.
func (s *SQLite) GetGitCredential(ctx context.Context, id string) (GitCredential, error) {
	assertf(id != "", "GetGitCredential: empty id")
	c, err := scanGitCredentialSQLite(s.db.QueryRowContext(ctx,
		`SELECT `+gitCredentialCols+` FROM git_credentials WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return GitCredential{}, fmt.Errorf("credential %q: %w", id, ErrNotFound)
	}
	if err != nil {
		return GitCredential{}, fmt.Errorf("get credential %q: %w", id, err)
	}
	return c, nil
}

// UpdateGitCredential replaces username/password; ErrNotFound when absent.
func (s *SQLite) UpdateGitCredential(ctx context.Context, id, username, password string) (GitCredential, error) {
	assertf(id != "", "UpdateGitCredential: empty id")
	var cred GitCredential
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`UPDATE git_credentials SET username = ?, password = ? WHERE id = ?`,
			username, password, id)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return ErrNotFound
		}
		assertf(n == 1, "UPDATE by primary key affected %d rows", n)
		c, err := scanGitCredentialSQLite(tx.QueryRowContext(ctx,
			`SELECT `+gitCredentialCols+` FROM git_credentials WHERE id = ?`, id))
		if err != nil {
			return err
		}
		cred = c
		return nil
	})
	if err != nil {
		return GitCredential{}, fmt.Errorf("update credential %q: %w", id, err)
	}
	return cred, nil
}

// DeleteGitCredential removes the credential; ErrNotFound when absent.
func (s *SQLite) DeleteGitCredential(ctx context.Context, id string) error {
	assertf(id != "", "DeleteGitCredential: empty id")
	res, err := s.db.ExecContext(ctx, `DELETE FROM git_credentials WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete credential %q: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete credential %q: %w", id, err)
	}
	if n == 0 {
		return fmt.Errorf("credential %q: %w", id, ErrNotFound)
	}
	assertf(n == 1, "DELETE by primary key affected %d rows", n)
	return nil
}

// GitCredentialForRepo returns the repo's credential; ErrNotFound when none
// is stored (public upstream).
func (s *SQLite) GitCredentialForRepo(ctx context.Context, repoID string) (GitCredential, error) {
	assertf(repoID != "", "GitCredentialForRepo: empty repo id")
	c, err := scanGitCredentialSQLite(s.db.QueryRowContext(ctx,
		`SELECT `+gitCredentialCols+` FROM git_credentials WHERE repo_id = ?`, repoID))
	if errors.Is(err, sql.ErrNoRows) {
		return GitCredential{}, fmt.Errorf("credential for repo %q: %w", repoID, ErrNotFound)
	}
	if err != nil {
		return GitCredential{}, fmt.Errorf("get credential for repo %q: %w", repoID, err)
	}
	return c, nil
}

// SetRepoUpstream inserts the upstream row (CreatedAt assigned by the store;
// counters/syncing zeroed). ErrAlreadyExists when the repo already has one.
func (s *SQLite) SetRepoUpstream(ctx context.Context, up RepoUpstream) (RepoUpstream, error) {
	assertf(up.RepoID != "", "SetRepoUpstream: empty repo id")
	nanos, createdAt := nowNano()
	up.RunCount, up.SuccessCount, up.Syncing = 0, 0, false
	up.CreatedAt = createdAt
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO repo_upstreams (repo_id, provider, owner, name, upstream_host,
		        run_count, success_count, syncing, created_at)
		 VALUES (?, ?, ?, ?, ?, 0, 0, 0, ?)`,
		up.RepoID, up.Provider, up.Owner, up.Name, up.UpstreamHost, nanos)
	// The only constraint on repo_upstreams is the repo_id PRIMARY KEY.
	if isPrimaryKeyViolation(err) {
		return RepoUpstream{}, fmt.Errorf("upstream for repo %q: %w", up.RepoID, ErrAlreadyExists)
	}
	if err != nil {
		return RepoUpstream{}, fmt.Errorf("set upstream for repo %q: %w", up.RepoID, err)
	}
	return up, nil
}

// GetRepoUpstream returns the row; ErrNotFound when detached/never linked.
func (s *SQLite) GetRepoUpstream(ctx context.Context, repoID string) (RepoUpstream, error) {
	assertf(repoID != "", "GetRepoUpstream: empty repo id")
	up, err := scanRepoUpstreamSQLite(s.db.QueryRowContext(ctx,
		`SELECT `+repoUpstreamCols+` FROM repo_upstreams WHERE repo_id = ?`, repoID))
	if errors.Is(err, sql.ErrNoRows) {
		return RepoUpstream{}, fmt.Errorf("upstream for repo %q: %w", repoID, ErrNotFound)
	}
	if err != nil {
		return RepoUpstream{}, fmt.Errorf("get upstream for repo %q: %w", repoID, err)
	}
	return up, nil
}

// DeleteRepoUpstream detaches the repo; ErrNotFound when already detached.
func (s *SQLite) DeleteRepoUpstream(ctx context.Context, repoID string) error {
	assertf(repoID != "", "DeleteRepoUpstream: empty repo id")
	res, err := s.db.ExecContext(ctx, `DELETE FROM repo_upstreams WHERE repo_id = ?`, repoID)
	if err != nil {
		return fmt.Errorf("delete upstream for repo %q: %w", repoID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete upstream for repo %q: %w", repoID, err)
	}
	if n == 0 {
		return fmt.Errorf("upstream for repo %q: %w", repoID, ErrNotFound)
	}
	assertf(n == 1, "DELETE by primary key affected %d rows", n)
	return nil
}

// TryBeginSync atomically claims a sync run (syncing false -> true).
// claimed=false with the current row when a run is already in flight;
// ErrNotFound when detached. The BEGIN IMMEDIATE transaction makes the
// claim-then-read pair atomic against concurrent claimers.
func (s *SQLite) TryBeginSync(ctx context.Context, repoID string) (RepoUpstream, bool, error) {
	assertf(repoID != "", "TryBeginSync: empty repo id")
	var up RepoUpstream
	var claimed bool
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`UPDATE repo_upstreams SET syncing = 1 WHERE repo_id = ? AND syncing = 0`, repoID)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		assertf(n == 0 || n == 1, "sync claim for %q affected %d rows", repoID, n)
		claimed = n == 1
		row, err := scanRepoUpstreamSQLite(tx.QueryRowContext(ctx,
			`SELECT `+repoUpstreamCols+` FROM repo_upstreams WHERE repo_id = ?`, repoID))
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		assertf(!claimed || row.Syncing, "claimed row %q returned syncing=false", repoID)
		up = row
		return nil
	})
	if err != nil {
		return RepoUpstream{}, false, fmt.Errorf("begin sync for repo %q: %w", repoID, err)
	}
	return up, claimed, nil
}

// FinishSync completes the claimed run: increments RunCount (and
// SuccessCount when success), clears Syncing, returns the updated row.
// ErrNotFound when the repo was detached mid-run.
func (s *SQLite) FinishSync(ctx context.Context, repoID string, success bool) (RepoUpstream, error) {
	assertf(repoID != "", "FinishSync: empty repo id")
	successInc := 0
	if success {
		successInc = 1
	}
	var up RepoUpstream
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`UPDATE repo_upstreams
			 SET syncing = 0, run_count = run_count + 1, success_count = success_count + ?
			 WHERE repo_id = ?`, successInc, repoID)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return ErrNotFound
		}
		assertf(n == 1, "finish sync for %q affected %d rows", repoID, n)
		row, err := scanRepoUpstreamSQLite(tx.QueryRowContext(ctx,
			`SELECT `+repoUpstreamCols+` FROM repo_upstreams WHERE repo_id = ?`, repoID))
		if err != nil {
			return err
		}
		up = row
		return nil
	})
	if err != nil {
		return RepoUpstream{}, fmt.Errorf("finish sync for repo %q: %w", repoID, err)
	}
	return up, nil
}
