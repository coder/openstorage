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
	var claimExpires, pushClaimExpires, createdAt int64
	if err := row.Scan(&up.RepoID, &up.Provider, &up.Owner, &up.Name, &up.UpstreamHost,
		&up.RunCount, &up.SuccessCount, &up.Syncing, &up.SyncPending,
		&up.SyncClaimHolder, &claimExpires,
		&up.ForwardOnPush, &up.PushPending, &up.PushSyncing,
		&up.PushClaimHolder, &pushClaimExpires, &up.PushClaimForce,
		&up.PushRunCount, &up.PushSuccessCount, &up.DefaultBranchOmitted, &createdAt); err != nil {
		return RepoUpstream{}, err
	}
	up.SyncClaimExpiresAt = time.Unix(0, claimExpires).UTC()
	up.PushClaimExpiresAt = time.Unix(0, pushClaimExpires).UTC()
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
	nanos, createdAt := s.nowNano()
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
	return s.createGitCredential(ctx, s.db, cred)
}

func (s *SQLite) createGitCredential(ctx context.Context, q sqliteExecer, cred GitCredential) (GitCredential, error) {
	assertf(cred.RepoID != "", "CreateGitCredential: empty repo id")
	stored, err := encodeSecret(s.codec, cred.Password)
	if err != nil {
		return GitCredential{}, fmt.Errorf("create credential for repo %q: %w", cred.RepoID, err)
	}
	nanos, createdAt := s.nowNano()
	cred.ID = NewEventingID("ggc")
	cred.CreatedAt = createdAt
	// Live-repo guard (issue #52, PR #105 round 6): SQLite's single-writer
	// lock totally orders this statement against SoftDeleteRepo /
	// PurgeRepoMeta — see the PG twin's rationale.
	res, err := q.ExecContext(ctx,
		`INSERT INTO git_credentials (id, repo_id, username, password, created_at)
		 SELECT ?, ?, ?, ?, ?
		 WHERE EXISTS (SELECT 1 FROM repos WHERE repo_id = ? AND deleted_at IS NULL)`,
		cred.ID, cred.RepoID, cred.Username, stored, nanos, cred.RepoID)
	// A unique violation here can only be repo_id: an id collision would be
	// SQLITE_CONSTRAINT_PRIMARYKEY, a different extended code.
	if isUniqueViolation(err) {
		return GitCredential{}, fmt.Errorf("credential for repo %q: %w", cred.RepoID, ErrAlreadyExists)
	}
	if err != nil {
		return GitCredential{}, fmt.Errorf("create credential for repo %q: %w", cred.RepoID, err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return GitCredential{}, fmt.Errorf("create credential for repo %q: rows: %w", cred.RepoID, err)
	} else if n == 0 {
		return GitCredential{}, fmt.Errorf("credential for deleted or unknown repo %q: %w", cred.RepoID, ErrNotFound)
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
	return s.setRepoUpstream(ctx, s.db, up)
}

// CreateLinkedRepo runs CreateRepo, CreateGitCredential and
// SetRepoUpstream in one BEGIN IMMEDIATE transaction (meta.EventingStore
// contract).
func (s *SQLite) CreateLinkedRepo(ctx context.Context, org, url, defaultBranch string, up RepoUpstream, cred *GitCredential) (Repo, error) {
	assertf(url != "" && defaultBranch != "", "CreateLinkedRepo: empty url %q or default branch %q", url, defaultBranch)
	orgID, err := s.lookupOrgID(ctx, org)
	if err != nil {
		return Repo{}, err
	}
	var repo Repo
	err = s.inTx(ctx, func(tx *sql.Tx) error {
		var err error
		if repo, err = s.insertRepo(ctx, tx, org, orgID, url, defaultBranch, CreateStatePending); err != nil {
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
		_, err = s.setRepoUpstream(ctx, tx, up)
		return err
	})
	if err != nil {
		return Repo{}, err
	}
	return repo, nil
}

// AdoptUpstreamDefaultBranch (meta.EventingStore contract) in one BEGIN
// IMMEDIATE transaction.
func (s *SQLite) AdoptUpstreamDefaultBranch(ctx context.Context, repoID, branch string) (bool, error) {
	assertf(repoID != "" && branch != "", "AdoptUpstreamDefaultBranch: empty repo id %q or branch %q", repoID, branch)
	adopted := false
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		var one int
		err := tx.QueryRowContext(ctx,
			`SELECT 1 FROM repos WHERE repo_id = ? AND deleted_at IS NULL`, repoID).Scan(&one)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("adopt default branch of repo %q: %w", repoID, ErrNotFound)
		}
		if err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx,
			`UPDATE repo_upstreams SET default_branch_omitted = 0 WHERE repo_id = ? AND default_branch_omitted`, repoID)
		if err != nil {
			return err
		}
		if n, err := res.RowsAffected(); err != nil || n == 0 {
			return err
		}
		res, err = tx.ExecContext(ctx, `UPDATE repos SET default_branch = ? WHERE repo_id = ? AND default_branch = ?`,
			branch, repoID, OmittedDefaultBranch)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		assertf(n == 0 || n == 1, "adopt default branch of live repo %q affected %d rows", repoID, n)
		adopted = n == 1
		return nil
	})
	if err != nil {
		return false, err
	}
	return adopted, nil
}

func (s *SQLite) setRepoUpstream(ctx context.Context, q sqliteExecer, up RepoUpstream) (RepoUpstream, error) {
	assertf(up.RepoID != "", "SetRepoUpstream: empty repo id")
	nanos, createdAt := s.nowNano()
	up.RunCount, up.SuccessCount, up.Syncing, up.SyncPending = 0, 0, false, false
	up.SyncClaimHolder, up.SyncClaimExpiresAt = "", time.Unix(0, 0).UTC()
	up.PushPending, up.PushSyncing, up.PushRunCount, up.PushSuccessCount = 0, false, 0, 0
	up.PushClaimHolder, up.PushClaimExpiresAt, up.PushClaimForce = "", time.Unix(0, 0).UTC(), false
	up.CreatedAt = createdAt
	// Live-repo guard — see CreateGitCredential (issue #52, PR #105
	// round 6).
	res, err := q.ExecContext(ctx,
		`INSERT INTO repo_upstreams (repo_id, provider, owner, name, upstream_host,
		        run_count, success_count, syncing, sync_pending,
		        sync_claim_holder, sync_claim_expires_at, forward_on_push, default_branch_omitted, created_at)
		 SELECT ?, ?, ?, ?, ?, 0, 0, 0, 0, '', 0, ?, ?, ?
		 WHERE EXISTS (SELECT 1 FROM repos WHERE repo_id = ? AND deleted_at IS NULL)`,
		up.RepoID, up.Provider, up.Owner, up.Name, up.UpstreamHost, up.ForwardOnPush, up.DefaultBranchOmitted, nanos, up.RepoID)
	// The only constraint on repo_upstreams is the repo_id PRIMARY KEY.
	if isPrimaryKeyViolation(err) {
		return RepoUpstream{}, fmt.Errorf("upstream for repo %q: %w", up.RepoID, ErrAlreadyExists)
	}
	if err != nil {
		return RepoUpstream{}, fmt.Errorf("set upstream for repo %q: %w", up.RepoID, err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return RepoUpstream{}, fmt.Errorf("set upstream for repo %q: rows: %w", up.RepoID, err)
	} else if n == 0 {
		return RepoUpstream{}, fmt.Errorf("upstream for deleted or unknown repo %q: %w", up.RepoID, ErrNotFound)
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
	c, err := s.beginSync(ctx, repoID, holder, lease, false)
	if err != nil {
		return RepoUpstream{}, false, "", err
	}
	return c.Up, c.Claimed, c.StolenFrom, nil
}

// beginSync is TryBeginSync's transaction. With requireDue (the local
// schedule's ClaimDueSync, issue #266) an unclaimed lane is claimable only
// when sync_pending is set and sync_due has passed; claimed lanes follow
// the unchanged lease/steal/legacy rules.
func (s *SQLite) beginSync(ctx context.Context, repoID, holder string, lease time.Duration, requireDue bool) (LocalClaim, error) {
	assertf(repoID != "", "TryBeginSync: empty repo id")
	assertf(holder != "", "TryBeginSync: empty holder")
	assertf(lease > 0, "TryBeginSync: non-positive lease %v", lease)
	nowNanos, _ := s.nowNano()
	var up RepoUpstream
	var claimed, busy bool
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
			up, busy = prev, true // unexpired claim in flight: refused, current row
			return nil
		}
		if prev.Syncing && prev.SyncClaimHolder == LegacyClaimHolder && !legacyStealEnabled() {
			// A lapsed adoption grace still is not proof against a
			// paused pre-lease binary, which carries none of the holder
			// guards — stealing needs the operator's drained-fleet
			// assertion (EnvSyncStealLegacy).
			up, busy = prev, true
			return nil
		}
		if requireDue && !prev.Syncing {
			sched, err := readLocalScheduleTx(ctx, tx, repoID)
			if err != nil {
				return err
			}
			if !prev.SyncPending || sched.syncDue > nowNanos {
				up = prev // no durable work due: refused, nothing written
				return nil
			}
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
			busy = true
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
		return LocalClaim{}, fmt.Errorf("begin sync for repo %q: %w", repoID, err)
	}
	assertf(!(claimed && busy), "begin sync for %q: claimed and busy", repoID)
	return LocalClaim{Up: up, Claimed: claimed, Busy: busy, StolenFrom: stolenFrom}, nil
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

// RequestPush raises the outbound pending level (issue #90); levels only
// coalesce upward so force is never downgraded. ErrNotFound when detached.
func (s *SQLite) RequestPush(ctx context.Context, repoID string, force bool) error {
	assertf(repoID != "", "RequestPush: empty repo id")
	level := 1
	if force {
		level = 2
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE repo_upstreams SET push_pending = MAX(push_pending, ?) WHERE repo_id = ?`,
		level, repoID)
	if err != nil {
		return fmt.Errorf("request push for repo %q: %w", repoID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("request push for repo %q: %w", repoID, err)
	}
	if n == 0 {
		return fmt.Errorf("upstream for repo %q: %w", repoID, ErrNotFound)
	}
	assertf(n == 1, "UPDATE by primary key affected %d rows", n)
	return nil
}

// TryBeginPush is TryBeginSync on the push_* lane (issue #90): BEGIN
// IMMEDIATE read-check-write on Go wall time (single-host topology). No
// legacy adoption exists here — push claims only ever come from #90-aware
// binaries, so an expired lease is always provably dead. A steal inherits
// the dead claim's persisted force mandate (push_claim_force), so a crash
// between claim and finish cannot downgrade an accepted forced request.
func (s *SQLite) TryBeginPush(ctx context.Context, repoID, holder string, lease time.Duration) (RepoUpstream, bool, bool, string, error) {
	c, err := s.beginPush(ctx, repoID, holder, lease, false)
	if err != nil {
		return RepoUpstream{}, false, false, "", err
	}
	return c.Up, c.Claimed, c.Force, c.StolenFrom, nil
}

// beginPush is TryBeginPush's transaction. With requireDue (the local
// schedule's ClaimDuePush, issue #266) an unclaimed lane is claimable only
// when push_pending is set and push_due has passed; an expired claim is
// stolen as before. The claimed run's attempt stays in the row.
func (s *SQLite) beginPush(ctx context.Context, repoID, holder string, lease time.Duration, requireDue bool) (LocalClaim, error) {
	assertf(repoID != "", "TryBeginPush: empty repo id")
	assertf(holder != "", "TryBeginPush: empty holder")
	assertf(lease > 0, "TryBeginPush: non-positive lease %v", lease)
	nowNanos, _ := s.nowNano()
	var up RepoUpstream
	var claimed, force, busy bool
	var stolenFrom string
	var attempt int
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		prev, err := scanRepoUpstreamSQLite(tx.QueryRowContext(ctx,
			`SELECT `+repoUpstreamCols+` FROM repo_upstreams WHERE repo_id = ?`, repoID))
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		now := time.Unix(0, nowNanos)
		if prev.PushSyncing && prev.PushClaimExpiresAt.After(now) {
			up, busy = prev, true // unexpired claim in flight: refused, current row
			return nil
		}
		if requireDue {
			sched, err := readLocalScheduleTx(ctx, tx, repoID)
			if err != nil {
				return err
			}
			if !prev.PushSyncing && (prev.PushPending == 0 || sched.pushDue > nowNanos) {
				up = prev // no durable work due: refused, nothing written
				return nil
			}
			attempt = sched.pushAttempt
		}
		force = prev.PushPending == 2 || (prev.PushSyncing && prev.PushClaimForce)
		forceInt := 0
		if force {
			forceInt = 1
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE repo_upstreams
			 SET push_syncing = 1, push_pending = 0, push_claim_holder = ?, push_claim_expires_at = ?, push_claim_force = ?
			 WHERE repo_id = ?`,
			holder, nowNanos+lease.Nanoseconds(), forceInt, repoID); err != nil {
			return err
		}
		if requireDue {
			// While a scheduled run holds the lane, push_due carries its
			// claim time (never 0). RequestPushNow resets it to 0, which
			// marks an explicit request made during the run; the run's
			// retry/restore then keeps that request due now.
			assertf(nowNanos > 0, "claim time %d is not positive", nowNanos)
			if _, err := tx.ExecContext(ctx,
				`UPDATE repo_upstreams SET push_due = ? WHERE repo_id = ?`, nowNanos, repoID); err != nil {
				return err
			}
		}
		row, err := scanRepoUpstreamSQLite(tx.QueryRowContext(ctx,
			`SELECT `+repoUpstreamCols+` FROM repo_upstreams WHERE repo_id = ?`, repoID))
		if err != nil {
			return err
		}
		up = row
		assertf(up.PushSyncing && up.PushClaimHolder == holder,
			"claimed row %q returned holder %q push_syncing=%v", repoID, up.PushClaimHolder, up.PushSyncing)
		claimed = true
		if prev.PushSyncing {
			stolenFrom = prev.PushClaimHolder // expired lease stolen
		}
		return nil
	})
	if err != nil {
		return LocalClaim{}, fmt.Errorf("begin push for repo %q: %w", repoID, err)
	}
	assertf(!(claimed && busy), "begin push for %q: claimed and busy", repoID)
	return LocalClaim{Up: up, Claimed: claimed, Busy: busy, Force: claimed && force, Attempt: attempt, StolenFrom: stolenFrom}, nil
}

// FinishPush is FinishSync on the push_* lane: holder-guarded completion,
// per-direction counters, ErrClaimLost for stolen claims.
func (s *SQLite) FinishPush(ctx context.Context, repoID, holder string, success bool) (RepoUpstream, error) {
	assertf(repoID != "", "FinishPush: empty repo id")
	assertf(holder != "", "FinishPush: empty holder")
	successInc := 0
	if success {
		successInc = 1
	}
	var up RepoUpstream
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`UPDATE repo_upstreams
			 SET push_syncing = 0, push_run_count = push_run_count + 1,
			     push_success_count = push_success_count + ?,
			     push_claim_holder = '', push_claim_expires_at = 0, push_claim_force = 0
			 WHERE repo_id = ? AND push_syncing = 1 AND push_claim_holder = ?`,
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
			if cur.PushSyncing && cur.PushClaimHolder != holder {
				return fmt.Errorf("now held by %q: %w", cur.PushClaimHolder, ErrClaimLost)
			}
			return ErrNotFound
		}
		assertf(n == 1, "finish push for %q affected %d rows", repoID, n)
		row, err := scanRepoUpstreamSQLite(tx.QueryRowContext(ctx,
			`SELECT `+repoUpstreamCols+` FROM repo_upstreams WHERE repo_id = ?`, repoID))
		if err != nil {
			return err
		}
		up = row
		return nil
	})
	if err != nil {
		return RepoUpstream{}, fmt.Errorf("finish push for repo %q: %w", repoID, err)
	}
	return up, nil
}
