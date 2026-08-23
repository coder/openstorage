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
// from INTEGER 0/1 into bool; timestamps from unixnano).
func scanRepoUpstreamSQLite(row sqlRow) (RepoUpstream, error) {
	var up RepoUpstream
	var claimExpires, createdAt int64
	if err := row.Scan(&up.RepoID, &up.Provider, &up.Owner, &up.Name, &up.UpstreamHost,
		&up.RunCount, &up.SuccessCount, &up.Syncing, &up.SyncPending,
		&up.SyncClaimHolder, &claimExpires, &createdAt); err != nil {
		return RepoUpstream{}, err
	}
	up.SyncClaimExpiresAt = time.Unix(0, claimExpires).UTC()
	up.CreatedAt = time.Unix(0, createdAt).UTC()
	return up, nil
}

// decryptWebhookSQLite mirrors PG.decryptWebhook (see pg_eventing.go for
// the CAS-rewrite rationale).
func (s *SQLite) decryptWebhookSQLite(ctx context.Context, w *Webhook) error {
	pt, rewrite, err := decodeSecret(s.codec, w.Secret, fmt.Sprintf("webhook %q secret", w.ID))
	if err != nil {
		return err
	}
	if rewrite {
		_, _ = s.db.ExecContext(ctx,
			`UPDATE webhooks SET secret = ? WHERE id = ? AND secret = ?`,
			s.codec.Encrypt(pt), w.ID, w.Secret)
	}
	w.Secret = pt
	return nil
}

// decryptGitCredentialSQLite mirrors PG.decryptGitCredential.
func (s *SQLite) decryptGitCredentialSQLite(ctx context.Context, c *GitCredential) error {
	pt, rewrite, err := decodeSecret(s.codec, c.Password, fmt.Sprintf("credential %q password", c.ID))
	if err != nil {
		return err
	}
	if rewrite {
		_, _ = s.db.ExecContext(ctx,
			`UPDATE git_credentials SET password = ? WHERE id = ? AND password = ?`,
			s.codec.Encrypt(pt), c.ID, c.Password)
	}
	c.Password = pt
	return nil
}

// CreateWebhook inserts a webhook with a server-generated ID.
// ErrNotFound when the org is unknown; ErrReservedSecret when the secret
// starts with the reserved envelope prefix.
func (s *SQLite) CreateWebhook(ctx context.Context, org, url, secret string) (Webhook, error) {
	assertf(url != "", "CreateWebhook: empty url for org %q", org)
	stored, err := encodeSecret(s.codec, secret)
	if err != nil {
		return Webhook{}, fmt.Errorf("create webhook for org %q: %w", org, err)
	}
	orgID, err := s.lookupOrgID(ctx, org)
	if err != nil {
		return Webhook{}, err
	}
	nanos, createdAt := nowNano()
	w := Webhook{ID: NewEventingID("wh"), OrgID: orgID, URL: url, Secret: secret, CreatedAt: createdAt}
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO webhooks (id, org_id, url, secret, created_at) VALUES (?, ?, ?, ?, ?)`,
		w.ID, orgID, url, stored, nanos); err != nil {
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	_ = rows.Close() // release the connection before the rewrite UPDATEs
	for i := range hooks {
		if err := s.decryptWebhookSQLite(ctx, &hooks[i]); err != nil {
			// Degrade the bad row to an empty secret instead of failing
			// the whole listing — see PG.ListWebhooks for the rationale.
			hooks[i].Secret = ""
		}
	}
	return hooks, nil
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
	if err := s.decryptWebhookSQLite(ctx, &w); err != nil {
		return Webhook{}, err
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
// ErrAlreadyExists when the repo already has a credential; ErrReservedSecret
// when the password starts with the reserved envelope prefix.
func (s *SQLite) CreateGitCredential(ctx context.Context, cred GitCredential) (GitCredential, error) {
	assertf(cred.RepoID != "", "CreateGitCredential: empty repo id")
	stored, err := encodeSecret(s.codec, cred.Password)
	if err != nil {
		return GitCredential{}, fmt.Errorf("create credential for repo %q: %w", cred.RepoID, err)
	}
	nanos, createdAt := nowNano()
	cred.ID = NewEventingID("ggc")
	cred.CreatedAt = createdAt
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO git_credentials (id, repo_id, username, password, created_at) VALUES (?, ?, ?, ?, ?)`,
		cred.ID, cred.RepoID, cred.Username, stored, nanos)
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
	if err := s.decryptGitCredentialSQLite(ctx, &c); err != nil {
		return GitCredential{}, err
	}
	return c, nil
}

// GitCredentialMeta returns the row with Password "" (the password column
// is never read — see the interface comment: recovery for undecryptable
// rows). ErrNotFound when absent.
func (s *SQLite) GitCredentialMeta(ctx context.Context, id string) (GitCredential, error) {
	assertf(id != "", "GitCredentialMeta: empty id")
	var c GitCredential
	var createdAt int64
	err := s.db.QueryRowContext(ctx,
		`SELECT id, repo_id, username, created_at FROM git_credentials WHERE id = ?`, id).
		Scan(&c.ID, &c.RepoID, &c.Username, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return GitCredential{}, fmt.Errorf("credential %q: %w", id, ErrNotFound)
	}
	if err != nil {
		return GitCredential{}, fmt.Errorf("get credential meta %q: %w", id, err)
	}
	c.CreatedAt = time.Unix(0, createdAt).UTC()
	return c, nil
}

// UpdateGitCredential replaces username/password; ErrNotFound when absent;
// ErrReservedSecret when the password starts with the reserved envelope
// prefix.
func (s *SQLite) UpdateGitCredential(ctx context.Context, id, username, password string) (GitCredential, error) {
	assertf(id != "", "UpdateGitCredential: empty id")
	stored, err := encodeSecret(s.codec, password)
	if err != nil {
		return GitCredential{}, fmt.Errorf("update credential %q: %w", id, err)
	}
	var cred GitCredential
	err = s.inTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`UPDATE git_credentials SET username = ?, password = ? WHERE id = ?`,
			username, stored, id)
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
	// The re-read carries the stored form we just wrote; hand the caller
	// its plaintext back (interface contract: plaintext in, plaintext out).
	cred.Password = password
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
	if err := s.decryptGitCredentialSQLite(ctx, &c); err != nil {
		return GitCredential{}, err
	}
	return c, nil
}

// SetRepoUpstream inserts the upstream row (CreatedAt assigned by the store;
// counters/syncing/sync_pending/claim lease zeroed). ErrAlreadyExists when
// the repo already has one.
func (s *SQLite) SetRepoUpstream(ctx context.Context, up RepoUpstream) (RepoUpstream, error) {
	assertf(up.RepoID != "", "SetRepoUpstream: empty repo id")
	nanos, createdAt := nowNano()
	up.RunCount, up.SuccessCount, up.Syncing, up.SyncPending = 0, 0, false, false
	up.SyncClaimHolder, up.SyncClaimExpiresAt = "", time.Unix(0, 0).UTC()
	up.CreatedAt = createdAt
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO repo_upstreams (repo_id, provider, owner, name, upstream_host,
		        run_count, success_count, syncing, sync_pending,
		        sync_claim_holder, sync_claim_expires_at, created_at)
		 VALUES (?, ?, ?, ?, ?, 0, 0, 0, 0, '', 0, ?)`,
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

// RequestSync marks a pending sync request; ErrNotFound when detached.
// See the EventingStore contract (eventing.go) for the pending-flag
// protocol (issue #79).
func (s *SQLite) RequestSync(ctx context.Context, repoID string) error {
	assertf(repoID != "", "RequestSync: empty repo id")
	res, err := s.db.ExecContext(ctx,
		`UPDATE repo_upstreams SET sync_pending = 1 WHERE repo_id = ?`, repoID)
	if err != nil {
		return fmt.Errorf("request sync for repo %q: %w", repoID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("request sync for repo %q: %w", repoID, err)
	}
	if n == 0 {
		return fmt.Errorf("upstream for repo %q: %w", repoID, ErrNotFound)
	}
	assertf(n == 1, "UPDATE by primary key affected %d rows", n)
	return nil
}

// TryBeginSync atomically claims a sync run (syncing false -> true) under
// a lease and consumes sync_pending (the claimed run's fetch postdates
// every request marked before the claim). An EXPIRED claim is stolen in
// the same guarded UPDATE (issue #86: the only recovery for a holder that
// died mid-run). The BEGIN IMMEDIATE transaction makes the
// read-check-write atomic against concurrent claimers; local mode is a
// single-host topology, so Go wall time is a consistent clock source.
// claimed=false with the current row when an unexpired run is in flight;
// ErrNotFound when detached.
func (s *SQLite) TryBeginSync(ctx context.Context, repoID, holder string, lease time.Duration) (RepoUpstream, bool, string, error) {
	assertf(repoID != "", "TryBeginSync: empty repo id")
	assertf(holder != "", "TryBeginSync: empty holder")
	assertf(lease > 0, "TryBeginSync: non-positive lease %v", lease)
	nowNanos, _ := nowNano()
	var up RepoUpstream
	var claimed bool
	var stolenFrom string
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		prev, err := scanRepoUpstreamSQLite(tx.QueryRowContext(ctx,
			`SELECT `+repoUpstreamCols+` FROM repo_upstreams WHERE repo_id = ?`, repoID))
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		// BEGIN IMMEDIATE holds the writer lock: the claim decision runs
		// in Go on consistent state, and every write below is
		// unconditional.
		now := time.Unix(0, nowNanos)
		if prev.Syncing && prev.SyncClaimExpiresAt.After(now) {
			up = prev // unexpired claim in flight: refused, current row
			return nil
		}
		reread := func() error {
			row, err := scanRepoUpstreamSQLite(tx.QueryRowContext(ctx,
				`SELECT `+repoUpstreamCols+` FROM repo_upstreams WHERE repo_id = ?`, repoID))
			if err != nil {
				return err
			}
			up = row
			return nil
		}
		if prev.Syncing && prev.SyncClaimHolder == "" {
			// Legacy claim (pre-lease replica, mixed fleet): adopt with a
			// full grace instead of stealing what may be a LIVE run — the
			// old replica's FinishSync clears it if so; only an unclaimed
			// expiry after the grace proves the holder dead.
			if _, err := tx.ExecContext(ctx,
				`UPDATE repo_upstreams SET sync_claim_holder = ?, sync_claim_expires_at = ? WHERE repo_id = ?`,
				LegacyClaimHolder, nowNanos+lease.Nanoseconds(), repoID); err != nil {
				return err
			}
			return reread()
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE repo_upstreams
			 SET syncing = 1, sync_pending = 0, sync_claim_holder = ?, sync_claim_expires_at = ?
			 WHERE repo_id = ?`,
			holder, nowNanos+lease.Nanoseconds(), repoID); err != nil {
			return err
		}
		if err := reread(); err != nil {
			return err
		}
		assertf(up.Syncing && up.SyncClaimHolder == holder,
			"claimed row %q returned holder %q syncing=%v", repoID, up.SyncClaimHolder, up.Syncing)
		claimed = true
		if prev.Syncing {
			stolenFrom = prev.SyncClaimHolder // expired lease (or lapsed grace) stolen
		}
		return nil
	})
	if err != nil {
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
func (s *SQLite) FinishSync(ctx context.Context, repoID, holder string, success bool) (RepoUpstream, error) {
	assertf(repoID != "", "FinishSync: empty repo id")
	assertf(holder != "", "FinishSync: empty holder")
	successInc := 0
	if success {
		successInc = 1
	}
	var up RepoUpstream
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`UPDATE repo_upstreams
			 SET syncing = 0, run_count = run_count + 1, success_count = success_count + ?,
			     sync_claim_holder = '', sync_claim_expires_at = 0
			 WHERE repo_id = ? AND syncing = 1 AND sync_claim_holder = ?`,
			successInc, repoID, holder)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			// Guard miss: classify inside the same transaction.
			cur, err := scanRepoUpstreamSQLite(tx.QueryRowContext(ctx,
				`SELECT `+repoUpstreamCols+` FROM repo_upstreams WHERE repo_id = ?`, repoID))
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			if err != nil {
				return err
			}
			if cur.Syncing && cur.SyncClaimHolder != holder {
				return fmt.Errorf("now held by %q: %w", cur.SyncClaimHolder, ErrClaimLost)
			}
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
