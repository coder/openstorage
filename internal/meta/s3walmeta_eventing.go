// Phase-5 eventing/sync persistence on the s3wal meta document. Semantics
// mirror sqlite_eventing.go 1:1 (contract in eventing.go) with two
// documented deviations:
//
//   - No legacy claim adoption: an empty-holder or LegacyClaimHolder claim
//     can only be written by a pre-lease SQL replica, and no pre-s3walmeta
//     binary exists (the same rationale as the ref store's absent legacy
//     reservation spellings) — such states are asserted impossible instead
//     of adopted.
//
//   - Sync-claim write-through (plan §2.8): with a SyncClaimSink wired
//     (WithSyncClaimSink), TryBeginSync commits the meta claim FIRST and
//     then installs the holder token in the base repo group's ref manifest
//     so a fenced CASRef checks the holder in the same object it CASes.
//     Meta-first ordering means only the meta claim's winner ever touches
//     the fence. If the manifest write fails, the meta claim is rolled
//     back best-effort (restoring a consumed sync_pending marker) and the
//     claim is refused — a failed rollback merely leaves the claim to
//     expire, and the missing manifest token keeps every fenced write
//     failing CLOSED either way. After the fence lands, the claim is
//     revalidated against the meta store: a steal that raced the fence
//     install (our manifest write landing after the thief's) is detected,
//     the fence is cleared owner-guarded, and the claim is refused — so a
//     stolen holder can never write under a late-reinstalled fence.
//     FinishSync clears the manifest token (owner-guarded, so a thief's
//     token is never touched) BEFORE finalizing the meta row; a failure
//     there aborts the finish (retryable; expiry recovers the lease).
//     The push lane has no write-through: the outbound claim protects
//     upstream refs, which no local CASRef fences.
package meta

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"
)

// SyncClaimSink mirrors inbound sync-claim transitions into the ref
// authority so CASRef's §2.8 fence reads the holder from the object it
// CASes. ref.S3WAL implements it (SetSyncClaim/ClearSyncClaim); SQL modes
// run without one (nil sink = no-op) because their fence reads the shared
// repo_upstreams row directly.
type SyncClaimSink interface {
	// SetSyncClaim installs holder as repoID's fence token, superseding
	// any previous token (the caller won the meta claim lease).
	SetSyncClaim(ctx context.Context, repoID, holder string) error
	// ClearSyncClaim removes repoID's fence token iff it still belongs
	// to holder.
	ClearSyncClaim(ctx context.Context, repoID, holder string) error
}

// lookupUpstream returns the upstream row; ErrNotFound when detached.
func lookupUpstream(doc *s3walMetaDoc, repoID string) (*s3walMetaUpstream, error) {
	assertf(repoID != "", "lookupUpstream: empty repo id")
	u := doc.Upstreams[repoID]
	if u == nil {
		return nil, fmt.Errorf("upstream for repo %q: %w", repoID, ErrNotFound)
	}
	return u, nil
}

// CreateWebhook inserts a webhook with a server-generated ID.
// ErrNotFound when the org is unknown; ErrReservedSecret when the secret
// starts with the reserved envelope prefix. Recovery class PRED: the
// per-run random ID proves OUR insert.
func (s *S3WAL) CreateWebhook(ctx context.Context, org, url, secret string) (Webhook, error) {
	assertf(url != "", "CreateWebhook: empty url for org %q", org)
	for what, v := range map[string]string{"webhook url": url, "webhook secret": secret} {
		if err := utf8OK(what, v); err != nil {
			return Webhook{}, fmt.Errorf("create webhook for org %q: %w", org, err)
		}
	}
	stored, err := encodeSecret(s.codec, secret)
	if err != nil {
		return Webhook{}, fmt.Errorf("create webhook for org %q: %w", org, err)
	}
	nanos, createdAt := s.nowNano()
	var hook Webhook
	var id string
	err = s.mutate(ctx, s3walMetaMutation{
		op: "create webhook",
		fn: func(doc *s3walMetaDoc) (bool, error) {
			orgRow, err := lookupOrg(doc, org)
			if err != nil {
				return false, err
			}
			id = NewEventingID("wh")
			if doc.Webhooks == nil {
				doc.Webhooks = make(map[string]*s3walMetaWebhook)
			}
			doc.Webhooks[id] = &s3walMetaWebhook{OrgID: orgRow.ID, URL: url, Secret: stored, CreatedAt: nanos}
			hook = Webhook{ID: id, OrgID: orgRow.ID, URL: url, Secret: secret, CreatedAt: createdAt}
			return true, nil
		},
		recover: func(doc *s3walMetaDoc) s3walMetaVerdict {
			if id != "" && doc.Webhooks[id] != nil {
				return s3walMetaApplied
			}
			return s3walMetaRerun
		},
	})
	if err != nil {
		return Webhook{}, err
	}
	return hook, nil
}

// decodeStoredSecret decrypts one stored secret value, CAS-rewriting the
// row under the active key when decodeSecret asks for it (issue #54; the
// rewrite is best-effort and guarded on the old stored value, exactly
// like the SQL twins' conditional UPDATE).
func (s *S3WAL) decodeStoredSecret(ctx context.Context, stored, what string, rewriteRow func(doc *s3walMetaDoc, oldStored, newStored string) bool) (string, error) {
	pt, rewrite, err := decodeSecret(s.codec, stored, what)
	if err != nil {
		return "", err
	}
	if rewrite {
		newStored := s.codec.Encrypt(pt)
		_ = s.mutate(ctx, s3walMetaMutation{ // best-effort, like the SQL twins
			op: "rewrite secret",
			fn: func(doc *s3walMetaDoc) (bool, error) {
				return rewriteRow(doc, stored, newStored), nil
			},
		})
	}
	return pt, nil
}

// decryptWebhook resolves w's stored secret to plaintext (see
// decodeStoredSecret for the lazy rewrite).
func (s *S3WAL) decryptWebhook(ctx context.Context, w *Webhook) error {
	pt, err := s.decodeStoredSecret(ctx, w.Secret, fmt.Sprintf("webhook %q secret", w.ID),
		func(doc *s3walMetaDoc, oldStored, newStored string) bool {
			row := doc.Webhooks[w.ID]
			if row == nil || row.Secret != oldStored {
				return false
			}
			row.Secret = newStored
			return true
		})
	if err != nil {
		return err
	}
	w.Secret = pt
	return nil
}

// ListWebhooks returns the org's webhooks in registration order
// (created_at, id). ErrNotFound when the org is unknown; undecryptable
// rows degrade to Secret == "" (see the EventingStore contract).
func (s *S3WAL) ListWebhooks(ctx context.Context, org string) ([]Webhook, error) {
	doc, err := s.viewDoc(ctx)
	if err != nil {
		return nil, err
	}
	orgRow, err := lookupOrg(doc, org)
	if err != nil {
		return nil, err
	}
	hooks := []Webhook{}
	for id, w := range doc.Webhooks {
		if w.OrgID != orgRow.ID {
			continue
		}
		hooks = append(hooks, Webhook{ID: id, OrgID: w.OrgID, URL: w.URL, Secret: w.Secret, CreatedAt: time.Unix(0, w.CreatedAt).UTC()})
	}
	sort.Slice(hooks, func(i, j int) bool {
		if !hooks[i].CreatedAt.Equal(hooks[j].CreatedAt) {
			return hooks[i].CreatedAt.Before(hooks[j].CreatedAt)
		}
		return hooks[i].ID < hooks[j].ID
	})
	for i := range hooks {
		if err := s.decryptWebhook(ctx, &hooks[i]); err != nil {
			// Degrade the bad row to an empty secret instead of failing
			// the whole listing — see PG.ListWebhooks for the rationale.
			hooks[i].Secret = ""
		}
	}
	return hooks, nil
}

// GetWebhook returns the webhook with the given ID regardless of org
// (delivery jobs carry only the ID); ErrNotFound when absent.
func (s *S3WAL) GetWebhook(ctx context.Context, id string) (Webhook, error) {
	assertf(id != "", "GetWebhook: empty id")
	doc, err := s.viewDoc(ctx)
	if err != nil {
		return Webhook{}, err
	}
	row := doc.Webhooks[id]
	if row == nil {
		return Webhook{}, fmt.Errorf("webhook %q: %w", id, ErrNotFound)
	}
	w := Webhook{ID: id, OrgID: row.OrgID, URL: row.URL, Secret: row.Secret, CreatedAt: time.Unix(0, row.CreatedAt).UTC()}
	if err := s.decryptWebhook(ctx, &w); err != nil {
		return Webhook{}, err
	}
	return w, nil
}

// DeleteWebhook removes the org's webhook; ErrNotFound when the org is
// unknown or the ID does not belong to it. Recovery class PRED on the
// semantic effect: webhook IDs are random and never reused, so an absent
// ID proves the deletion holds.
func (s *S3WAL) DeleteWebhook(ctx context.Context, org, id string) error {
	assertf(id != "", "DeleteWebhook: empty id for org %q", org)
	return s.mutate(ctx, s3walMetaMutation{
		op: "delete webhook",
		fn: func(doc *s3walMetaDoc) (bool, error) {
			orgRow, err := lookupOrg(doc, org)
			if err != nil {
				return false, err
			}
			row := doc.Webhooks[id]
			if row == nil || row.OrgID != orgRow.ID {
				return false, fmt.Errorf("webhook %q for org %q: %w", id, org, ErrNotFound)
			}
			delete(doc.Webhooks, id)
			return true, nil
		},
		recover: func(doc *s3walMetaDoc) s3walMetaVerdict {
			if doc.Webhooks[id] == nil && doc.Orgs[org] != nil {
				return s3walMetaApplied
			}
			return s3walMetaRerun
		},
	})
}

// CreateGitCredential stores cred (ID/CreatedAt assigned by the store).
// ErrAlreadyExists when the repo already has a credential; ErrNotFound
// for deleted or unknown repos (the live-repo guard, issue #52 — the
// single-object CAS is the atomicity the SQL twins buy with statement
// serialization); ErrReservedSecret for reserved-prefix passwords.
// Recovery class PRED: the per-run random ID proves OUR insert.
func (s *S3WAL) CreateGitCredential(ctx context.Context, cred GitCredential) (GitCredential, error) {
	assertf(cred.RepoID != "", "CreateGitCredential: empty repo id")
	for what, v := range map[string]string{"credential username": cred.Username, "credential password": cred.Password} {
		if err := utf8OK(what, v); err != nil {
			return GitCredential{}, fmt.Errorf("create credential for repo %q: %w", cred.RepoID, err)
		}
	}
	stored, err := encodeSecret(s.codec, cred.Password)
	if err != nil {
		return GitCredential{}, fmt.Errorf("create credential for repo %q: %w", cred.RepoID, err)
	}
	nanos, createdAt := s.nowNano()
	var id string
	err = s.mutate(ctx, s3walMetaMutation{
		op: "create credential",
		fn: func(doc *s3walMetaDoc) (bool, error) {
			repo := doc.Repos[cred.RepoID]
			if repo == nil || repo.DeletedAt != 0 {
				return false, fmt.Errorf("credential for deleted or unknown repo %q: %w", cred.RepoID, ErrNotFound)
			}
			for _, c := range doc.Credentials {
				if c.RepoID == cred.RepoID {
					return false, fmt.Errorf("credential for repo %q: %w", cred.RepoID, ErrAlreadyExists)
				}
			}
			id = NewEventingID("ggc")
			if doc.Credentials == nil {
				doc.Credentials = make(map[string]*s3walMetaCredential)
			}
			doc.Credentials[id] = &s3walMetaCredential{RepoID: cred.RepoID, Username: cred.Username, Password: stored, CreatedAt: nanos}
			return true, nil
		},
		recover: func(doc *s3walMetaDoc) s3walMetaVerdict {
			if id != "" && doc.Credentials[id] != nil {
				return s3walMetaApplied
			}
			return s3walMetaRerun
		},
	})
	if err != nil {
		return GitCredential{}, err
	}
	cred.ID = id
	cred.CreatedAt = createdAt
	return cred, nil
}

// decryptCredential resolves c's stored password to plaintext (see
// decodeStoredSecret for the lazy rewrite).
func (s *S3WAL) decryptCredential(ctx context.Context, c *GitCredential) error {
	pt, err := s.decodeStoredSecret(ctx, c.Password, fmt.Sprintf("credential %q password", c.ID),
		func(doc *s3walMetaDoc, oldStored, newStored string) bool {
			row := doc.Credentials[c.ID]
			if row == nil || row.Password != oldStored {
				return false
			}
			row.Password = newStored
			return true
		})
	if err != nil {
		return err
	}
	c.Password = pt
	return nil
}

// GetGitCredential returns the credential by ID; ErrNotFound when absent.
func (s *S3WAL) GetGitCredential(ctx context.Context, id string) (GitCredential, error) {
	assertf(id != "", "GetGitCredential: empty id")
	doc, err := s.viewDoc(ctx)
	if err != nil {
		return GitCredential{}, err
	}
	row := doc.Credentials[id]
	if row == nil {
		return GitCredential{}, fmt.Errorf("credential %q: %w", id, ErrNotFound)
	}
	c := GitCredential{ID: id, RepoID: row.RepoID, Username: row.Username, Password: row.Password, CreatedAt: time.Unix(0, row.CreatedAt).UTC()}
	if err := s.decryptCredential(ctx, &c); err != nil {
		return GitCredential{}, err
	}
	return c, nil
}

// GitCredentialMeta returns the row with Password "" (the password field
// is never read — recovery for undecryptable rows, see the interface
// comment). ErrNotFound when absent.
func (s *S3WAL) GitCredentialMeta(ctx context.Context, id string) (GitCredential, error) {
	assertf(id != "", "GitCredentialMeta: empty id")
	doc, err := s.viewDoc(ctx)
	if err != nil {
		return GitCredential{}, err
	}
	row := doc.Credentials[id]
	if row == nil {
		return GitCredential{}, fmt.Errorf("credential %q: %w", id, ErrNotFound)
	}
	return GitCredential{ID: id, RepoID: row.RepoID, Username: row.Username, CreatedAt: time.Unix(0, row.CreatedAt).UTC()}, nil
}

// UpdateGitCredential replaces username/password; ErrNotFound when
// absent; ErrReservedSecret for reserved-prefix passwords. Recovery class
// PRED: with encryption on, the stored envelope is unique per call (fresh
// GCM nonce), so field equality proves OUR write; with plaintext storage
// an identical competing update is indistinguishable and identical in
// effect.
func (s *S3WAL) UpdateGitCredential(ctx context.Context, id, username, password string) (GitCredential, error) {
	assertf(id != "", "UpdateGitCredential: empty id")
	for what, v := range map[string]string{"credential username": username, "credential password": password} {
		if err := utf8OK(what, v); err != nil {
			return GitCredential{}, fmt.Errorf("update credential %q: %w", id, err)
		}
	}
	stored, err := encodeSecret(s.codec, password)
	if err != nil {
		return GitCredential{}, fmt.Errorf("update credential %q: %w", id, err)
	}
	var cred GitCredential
	err = s.mutate(ctx, s3walMetaMutation{
		op: "update credential",
		fn: func(doc *s3walMetaDoc) (bool, error) {
			row := doc.Credentials[id]
			if row == nil {
				return false, fmt.Errorf("credential %q: %w", id, ErrNotFound)
			}
			row.Username, row.Password = username, stored
			cred = GitCredential{ID: id, RepoID: row.RepoID, Username: username, Password: password, CreatedAt: time.Unix(0, row.CreatedAt).UTC()}
			return true, nil
		},
		recover: func(doc *s3walMetaDoc) s3walMetaVerdict {
			if row := doc.Credentials[id]; row != nil && row.Username == username && row.Password == stored {
				return s3walMetaApplied
			}
			return s3walMetaRerun
		},
	})
	if err != nil {
		return GitCredential{}, err
	}
	return cred, nil
}

// DeleteGitCredential removes the credential; ErrNotFound when absent.
// Recovery class PRED: credential IDs are never reused.
func (s *S3WAL) DeleteGitCredential(ctx context.Context, id string) error {
	assertf(id != "", "DeleteGitCredential: empty id")
	return s.mutate(ctx, s3walMetaMutation{
		op: "delete credential",
		fn: func(doc *s3walMetaDoc) (bool, error) {
			if doc.Credentials[id] == nil {
				return false, fmt.Errorf("credential %q: %w", id, ErrNotFound)
			}
			delete(doc.Credentials, id)
			return true, nil
		},
		recover: func(doc *s3walMetaDoc) s3walMetaVerdict {
			if doc.Credentials[id] == nil {
				return s3walMetaApplied
			}
			return s3walMetaRerun
		},
	})
}

// GitCredentialForRepo returns the repo's credential; ErrNotFound when
// none is stored (public upstream).
func (s *S3WAL) GitCredentialForRepo(ctx context.Context, repoID string) (GitCredential, error) {
	assertf(repoID != "", "GitCredentialForRepo: empty repo id")
	doc, err := s.viewDoc(ctx)
	if err != nil {
		return GitCredential{}, err
	}
	for id, row := range doc.Credentials {
		if row.RepoID == repoID {
			c := GitCredential{ID: id, RepoID: repoID, Username: row.Username, Password: row.Password, CreatedAt: time.Unix(0, row.CreatedAt).UTC()}
			if err := s.decryptCredential(ctx, &c); err != nil {
				return GitCredential{}, err
			}
			return c, nil
		}
	}
	return GitCredential{}, fmt.Errorf("credential for repo %q: %w", repoID, ErrNotFound)
}

// SetRepoUpstream inserts the upstream row (CreatedAt assigned by the
// store; counters/flags/leases zeroed). ErrAlreadyExists when the repo
// already has one; ErrNotFound for deleted or unknown repos (live-repo
// guard, issue #52). Recovery class PRED: the per-run stamp plus the full
// identity fields prove OUR insert (rows are created at most once per
// repo, so a matching row under our stamp cannot predate this call).
func (s *S3WAL) SetRepoUpstream(ctx context.Context, up RepoUpstream) (RepoUpstream, error) {
	assertf(up.RepoID != "", "SetRepoUpstream: empty repo id")
	for what, v := range map[string]string{
		"upstream provider": up.Provider, "upstream owner": up.Owner,
		"upstream name": up.Name, "upstream host": up.UpstreamHost,
	} {
		if err := utf8OK(what, v); err != nil {
			return RepoUpstream{}, fmt.Errorf("set upstream for repo %q: %w", up.RepoID, err)
		}
	}
	nanos, createdAt := s.nowNano()
	up.RunCount, up.SuccessCount, up.Syncing, up.SyncPending = 0, 0, false, false
	up.SyncClaimHolder, up.SyncClaimExpiresAt = "", time.Unix(0, 0).UTC()
	up.PushPending, up.PushSyncing, up.PushRunCount, up.PushSuccessCount = 0, false, 0, 0
	up.PushClaimHolder, up.PushClaimExpiresAt, up.PushClaimForce = "", time.Unix(0, 0).UTC(), false
	up.CreatedAt = createdAt
	row := &s3walMetaUpstream{
		Provider: up.Provider, Owner: up.Owner, Name: up.Name, UpstreamHost: up.UpstreamHost,
		ForwardOnPush: up.ForwardOnPush, CreatedAt: nanos,
	}
	err := s.mutate(ctx, s3walMetaMutation{
		op: "set upstream",
		fn: func(doc *s3walMetaDoc) (bool, error) {
			repo := doc.Repos[up.RepoID]
			if repo == nil || repo.DeletedAt != 0 {
				return false, fmt.Errorf("upstream for deleted or unknown repo %q: %w", up.RepoID, ErrNotFound)
			}
			if doc.Upstreams[up.RepoID] != nil {
				return false, fmt.Errorf("upstream for repo %q: %w", up.RepoID, ErrAlreadyExists)
			}
			if doc.Upstreams == nil {
				doc.Upstreams = make(map[string]*s3walMetaUpstream)
			}
			cp := *row
			doc.Upstreams[up.RepoID] = &cp
			return true, nil
		},
		recover: func(doc *s3walMetaDoc) s3walMetaVerdict {
			if got := doc.Upstreams[up.RepoID]; got != nil && got.CreatedAt == nanos &&
				got.Provider == row.Provider && got.Owner == row.Owner &&
				got.Name == row.Name && got.UpstreamHost == row.UpstreamHost &&
				got.ForwardOnPush == row.ForwardOnPush {
				return s3walMetaApplied
			}
			return s3walMetaRerun
		},
	})
	if err != nil {
		return RepoUpstream{}, err
	}
	return up, nil
}

// GetRepoUpstream returns the row; ErrNotFound when detached/never linked.
func (s *S3WAL) GetRepoUpstream(ctx context.Context, repoID string) (RepoUpstream, error) {
	doc, err := s.viewDoc(ctx)
	if err != nil {
		return RepoUpstream{}, err
	}
	u, err := lookupUpstream(doc, repoID)
	if err != nil {
		return RepoUpstream{}, err
	}
	return u.toRepoUpstream(repoID), nil
}

// DeleteRepoUpstream detaches the repo; ErrNotFound when already
// detached. Recovery class PRED: the deleted row is identified by its
// creation stamp, so a row re-linked after our landed delete is never
// re-deleted by a recovery re-run.
func (s *S3WAL) DeleteRepoUpstream(ctx context.Context, repoID string) error {
	assertf(repoID != "", "DeleteRepoUpstream: empty repo id")
	var targetCreatedAt int64
	targeted := false
	return s.mutate(ctx, s3walMetaMutation{
		op: "delete upstream",
		fn: func(doc *s3walMetaDoc) (bool, error) {
			u := doc.Upstreams[repoID]
			if u == nil || (targeted && u.CreatedAt != targetCreatedAt) {
				return false, fmt.Errorf("upstream for repo %q: %w", repoID, ErrNotFound)
			}
			targetCreatedAt, targeted = u.CreatedAt, true
			delete(doc.Upstreams, repoID)
			return true, nil
		},
		recover: func(doc *s3walMetaDoc) s3walMetaVerdict {
			if u := doc.Upstreams[repoID]; u == nil || u.CreatedAt != targetCreatedAt {
				return s3walMetaApplied
			}
			return s3walMetaRerun
		},
	})
}

// RequestSync marks a pending sync request (issue #79 protocol);
// ErrNotFound when detached. Recovery class ID: the flag is a monotone
// upsert, re-applied harmlessly.
func (s *S3WAL) RequestSync(ctx context.Context, repoID string) error {
	return s.mutate(ctx, s3walMetaMutation{
		op: "request sync",
		fn: func(doc *s3walMetaDoc) (bool, error) {
			u, err := lookupUpstream(doc, repoID)
			if err != nil {
				return false, err
			}
			if u.SyncPending {
				return false, nil // already marked: identical terminal state
			}
			u.SyncPending = true
			return true, nil
		},
	})
}

// PendingSyncRepoIDs lists repos with the sync_pending marker set (boot
// recovery, issue #86), ordered by repo id.
func (s *S3WAL) PendingSyncRepoIDs(ctx context.Context) ([]string, error) {
	doc, err := s.viewDoc(ctx)
	if err != nil {
		return nil, err
	}
	var ids []string
	for id, u := range doc.Upstreams {
		if u.SyncPending {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids, nil
}

// assertNoLegacyClaim crashes on claim shapes only pre-lease SQL replicas
// can write: no pre-s3walmeta binary exists, so hitting one means the
// document was corrupted or edited out-of-band.
func assertNoLegacyClaim(repoID string, u *s3walMetaUpstream) {
	assertf(!(u.Syncing && u.SyncClaimHolder == ""),
		"s3walmeta: repo %q claim has no holder (only pre-lease SQL replicas write that shape)", repoID)
	assertf(u.SyncClaimHolder != LegacyClaimHolder,
		"s3walmeta: repo %q claim held by %q (no legacy adoption lane exists here)", repoID, LegacyClaimHolder)
}

// TryBeginSync atomically claims a sync run under a lease, consuming
// sync_pending; an EXPIRED claim is stolen in the same CAS (the
// read-check-write atomicity the SQL twins buy with row locks is the
// object CAS here). claimed=false with the current row when an unexpired
// run is in flight; ErrNotFound when detached. No legacy adoption lane
// exists (see the file comment).
//
// Recovery class PRED for the claim write: the (holder, expiry-stamp)
// pair is unique to this attempt — holder tokens are per-run and the
// expiry is this call's clock reading — so finding both on the fresh
// snapshot proves OUR claim landed (holder equality alone would not,
// the Phase 1b same-field-rewrite lesson).
//
// With a SyncClaimSink wired, the §2.8 write-through runs after the meta
// claim commits — see the file comment for the ordering and fail-closed
// argument.
func (s *S3WAL) TryBeginSync(ctx context.Context, repoID, holder string, lease time.Duration) (RepoUpstream, bool, string, error) {
	assertf(repoID != "", "TryBeginSync: empty repo id")
	assertf(holder != "", "TryBeginSync: empty holder")
	assertf(lease > 0, "TryBeginSync: non-positive lease %v", lease)
	if err := utf8OK("holder", holder); err != nil {
		return RepoUpstream{}, false, "", fmt.Errorf("begin sync for repo %q: %w", repoID, err)
	}
	nowNanos, _ := s.nowNano()
	expiry := nowNanos + lease.Nanoseconds()
	var up RepoUpstream
	var claimed bool
	var stolenFrom string
	var prevPending bool
	err := s.mutate(ctx, s3walMetaMutation{
		op: "begin sync",
		fn: func(doc *s3walMetaDoc) (bool, error) {
			claimed, stolenFrom, prevPending = false, "", false
			u, err := lookupUpstream(doc, repoID)
			if err != nil {
				return false, err
			}
			assertNoLegacyClaim(repoID, u)
			if u.Syncing && u.SyncClaimExpiresAt > nowNanos {
				up = u.toRepoUpstream(repoID) // unexpired claim in flight: refused
				return false, nil
			}
			if u.Syncing {
				stolenFrom = u.SyncClaimHolder // expired lease stolen
			}
			prevPending = u.SyncPending
			u.Syncing, u.SyncPending = true, false
			u.SyncClaimHolder, u.SyncClaimExpiresAt = holder, expiry
			up = u.toRepoUpstream(repoID)
			claimed = true
			return true, nil
		},
		recover: func(doc *s3walMetaDoc) s3walMetaVerdict {
			if u := doc.Upstreams[repoID]; u != nil && u.Syncing &&
				u.SyncClaimHolder == holder && u.SyncClaimExpiresAt == expiry {
				return s3walMetaApplied // fn's outputs from the landed run stand
			}
			return s3walMetaRerun
		},
	})
	if err != nil {
		return RepoUpstream{}, false, "", fmt.Errorf("begin sync for repo %q: %w", repoID, err)
	}
	if !claimed || s.sink == nil {
		return up, claimed, stolenFrom, nil
	}

	// §2.8 write-through: install the fence token in the ref manifest.
	if serr := s.sink.SetSyncClaim(ctx, repoID, holder); serr != nil {
		// Fail closed: without the manifest token every fenced write is
		// refused, so the claim must not be handed out. Roll the meta
		// claim back best-effort (a failed rollback leaves it to expire —
		// the crashed-holder shape the lease already recovers from) and
		// clear any token the failed write may still have landed.
		s.rollbackSyncClaim(ctx, repoID, holder, expiry, prevPending)
		if cerr := s.sink.ClearSyncClaim(ctx, repoID, holder); cerr != nil {
			slog.Warn("s3walmeta: fence cleanup after failed write-through also failed; a stale token may linger (fail-closed for every other holder)",
				"repo", repoID, "err", cerr)
		}
		return RepoUpstream{}, false, "", fmt.Errorf("begin sync for repo %q: sync-claim write-through: %w", repoID, serr)
	}
	// Revalidate the meta claim now that the fence is installed: if the
	// claim was stolen while our manifest write was in flight (our token
	// landing AFTER the thief's), the thief's fence was just overwritten
	// — clear our token (owner-guarded: the thief's own re-install is
	// never touched) and refuse the claim, so a stolen holder can never
	// write under a late-reinstalled fence.
	fresh, gerr := s.GetRepoUpstream(ctx, repoID)
	if gerr == nil && fresh.Syncing && fresh.SyncClaimHolder == holder &&
		fresh.SyncClaimExpiresAt.Equal(time.Unix(0, expiry).UTC()) {
		return up, true, stolenFrom, nil
	}
	if cerr := s.sink.ClearSyncClaim(ctx, repoID, holder); cerr != nil {
		slog.Warn("s3walmeta: fence clear after lost claim revalidation failed; the stale token blocks no thief (their claim re-installs it) but fails our writes closed",
			"repo", repoID, "err", cerr)
	}
	if gerr != nil {
		return RepoUpstream{}, false, "", fmt.Errorf("begin sync for repo %q: claim revalidation: %w", repoID, gerr)
	}
	return fresh, false, "", nil
}

// rollbackSyncClaim undoes a just-committed claim after a failed fence
// write-through: holder+expiry-guarded (a thief's claim is never
// touched), counters untouched, and the consumed sync_pending marker is
// restored (OR-ed — a request that arrived meanwhile is never lowered).
// Best-effort: on failure the claim simply expires.
func (s *S3WAL) rollbackSyncClaim(ctx context.Context, repoID, holder string, expiry int64, prevPending bool) {
	err := s.mutate(ctx, s3walMetaMutation{
		op: "rollback sync claim",
		fn: func(doc *s3walMetaDoc) (bool, error) {
			u := doc.Upstreams[repoID]
			if u == nil || !u.Syncing || u.SyncClaimHolder != holder || u.SyncClaimExpiresAt != expiry {
				return false, nil // not our claim anymore: nothing to undo
			}
			u.Syncing = false
			u.SyncClaimHolder, u.SyncClaimExpiresAt = "", 0
			u.SyncPending = u.SyncPending || prevPending
			return true, nil
		},
		recover: func(doc *s3walMetaDoc) s3walMetaVerdict {
			if u := doc.Upstreams[repoID]; u == nil || !u.Syncing ||
				u.SyncClaimHolder != holder || u.SyncClaimExpiresAt != expiry {
				return s3walMetaApplied // our claim is gone either way
			}
			return s3walMetaRerun
		},
	})
	if err != nil {
		slog.Warn("s3walmeta: sync-claim rollback failed; the claim will expire on its lease",
			"repo", repoID, "holder", holder, "err", err)
	}
}

// FinishSync completes the run claimed by holder: increments RunCount
// (and SuccessCount when success), clears Syncing and the lease, returns
// the updated row; SyncPending is NOT touched. ErrNotFound / ErrClaimLost
// per the EventingStore contract. With a sink wired, the manifest fence
// token is cleared (owner-guarded) BEFORE the meta row is finalized; a
// clear failure aborts the finish (retryable — the meta claim stays held
// until retried or expired, and the manifest token keeps fencing only
// this holder's writes).
//
// Recovery class PRED on counter advancement: our finish landed iff the
// claim is no longer ours AND both counters advanced by exactly our
// increment from the claimed snapshot. A thief that stole our expired
// claim and finished with the SAME success bit is indistinguishable — and
// produces the identical terminal row, so reporting success is harmless;
// a thief with a different bit fails the predicate and re-runs into the
// SQL twins' late-finisher classification (ErrClaimLost / ErrNotFound).
func (s *S3WAL) FinishSync(ctx context.Context, repoID, holder string, success bool) (RepoUpstream, error) {
	assertf(repoID != "", "FinishSync: empty repo id")
	assertf(holder != "", "FinishSync: empty holder")
	if s.sink != nil {
		if err := s.sink.ClearSyncClaim(ctx, repoID, holder); err != nil {
			return RepoUpstream{}, fmt.Errorf("finish sync for repo %q: clear sync-claim fence: %w", repoID, err)
		}
	}
	successInc := 0
	if success {
		successInc = 1
	}
	var up RepoUpstream
	var prevRun, prevSuccess int
	err := s.mutate(ctx, s3walMetaMutation{
		op: "finish sync",
		fn: func(doc *s3walMetaDoc) (bool, error) {
			u := doc.Upstreams[repoID]
			if u == nil {
				return false, fmt.Errorf("upstream for repo %q: %w", repoID, ErrNotFound)
			}
			if !u.Syncing || u.SyncClaimHolder != holder {
				if u.Syncing && u.SyncClaimHolder != holder {
					return false, fmt.Errorf("finish sync for repo %q: now held by %q: %w", repoID, u.SyncClaimHolder, ErrClaimLost)
				}
				return false, fmt.Errorf("upstream for repo %q: %w", repoID, ErrNotFound)
			}
			prevRun, prevSuccess = u.RunCount, u.SuccessCount
			u.Syncing = false
			u.RunCount++
			u.SuccessCount += successInc
			u.SyncClaimHolder, u.SyncClaimExpiresAt = "", 0
			up = u.toRepoUpstream(repoID)
			return true, nil
		},
		recover: func(doc *s3walMetaDoc) s3walMetaVerdict {
			u := doc.Upstreams[repoID]
			if u == nil {
				return s3walMetaRerun // fn classifies as ErrNotFound
			}
			if u.Syncing && u.SyncClaimHolder == holder {
				return s3walMetaRerun // our claim intact: the finish never landed
			}
			if u.RunCount == prevRun+1 && u.SuccessCount == prevSuccess+successInc {
				return s3walMetaApplied
			}
			return s3walMetaRerun // fn classifies (ErrClaimLost / ErrNotFound)
		},
	})
	if err != nil {
		return RepoUpstream{}, fmt.Errorf("finish sync for repo %q: %w", repoID, err)
	}
	return up, nil
}

// RequestPush raises the outbound pending level (issue #90); levels only
// coalesce upward. ErrNotFound when detached. Recovery class ID: the
// MAX-merge is monotone.
func (s *S3WAL) RequestPush(ctx context.Context, repoID string, force bool) error {
	level := 1
	if force {
		level = 2
	}
	return s.mutate(ctx, s3walMetaMutation{
		op: "request push",
		fn: func(doc *s3walMetaDoc) (bool, error) {
			u, err := lookupUpstream(doc, repoID)
			if err != nil {
				return false, err
			}
			if u.PushPending >= level {
				return false, nil // already at (or above) the requested level
			}
			u.PushPending = level
			return true, nil
		},
	})
}

// PendingPushRepoIDs lists repos owing an outbound run at boot — pending
// marker set OR orphaned in-flight claim (see the EventingStore
// contract), ordered by repo id.
func (s *S3WAL) PendingPushRepoIDs(ctx context.Context) ([]string, error) {
	doc, err := s.viewDoc(ctx)
	if err != nil {
		return nil, err
	}
	var ids []string
	for id, u := range doc.Upstreams {
		if u.PushPending > 0 || u.PushSyncing {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids, nil
}

// TryBeginPush is TryBeginSync on the push_* lane (issue #90): claims the
// outbound run under a lease, consumes PushPending whole, and inherits a
// dead claim's persisted force mandate on steal. There is no legacy
// adoption and no write-through — the outbound claim protects upstream
// refs, which no local CASRef fences. Recovery class PRED: the
// (holder, expiry-stamp) pair proves OUR claim, as in TryBeginSync.
func (s *S3WAL) TryBeginPush(ctx context.Context, repoID, holder string, lease time.Duration) (RepoUpstream, bool, bool, string, error) {
	assertf(repoID != "", "TryBeginPush: empty repo id")
	assertf(holder != "", "TryBeginPush: empty holder")
	assertf(lease > 0, "TryBeginPush: non-positive lease %v", lease)
	if err := utf8OK("holder", holder); err != nil {
		return RepoUpstream{}, false, false, "", fmt.Errorf("begin push for repo %q: %w", repoID, err)
	}
	nowNanos, _ := s.nowNano()
	expiry := nowNanos + lease.Nanoseconds()
	var up RepoUpstream
	var claimed, force bool
	var stolenFrom string
	err := s.mutate(ctx, s3walMetaMutation{
		op: "begin push",
		fn: func(doc *s3walMetaDoc) (bool, error) {
			claimed, force, stolenFrom = false, false, ""
			u, err := lookupUpstream(doc, repoID)
			if err != nil {
				return false, err
			}
			assertf(!(u.PushSyncing && u.PushClaimHolder == ""),
				"s3walmeta: repo %q push claim has no holder", repoID)
			if u.PushSyncing && u.PushClaimExpiresAt > nowNanos {
				up = u.toRepoUpstream(repoID) // unexpired claim in flight: refused
				return false, nil
			}
			force = u.PushPending == 2 || (u.PushSyncing && u.PushClaimForce)
			if u.PushSyncing {
				stolenFrom = u.PushClaimHolder // expired lease stolen
			}
			u.PushSyncing, u.PushPending = true, 0
			u.PushClaimHolder, u.PushClaimExpiresAt, u.PushClaimForce = holder, expiry, force
			up = u.toRepoUpstream(repoID)
			claimed = true
			return true, nil
		},
		recover: func(doc *s3walMetaDoc) s3walMetaVerdict {
			if u := doc.Upstreams[repoID]; u != nil && u.PushSyncing &&
				u.PushClaimHolder == holder && u.PushClaimExpiresAt == expiry {
				return s3walMetaApplied
			}
			return s3walMetaRerun
		},
	})
	if err != nil {
		return RepoUpstream{}, false, false, "", fmt.Errorf("begin push for repo %q: %w", repoID, err)
	}
	return up, claimed, force, stolenFrom, nil
}

// FinishPush is FinishSync on the push_* lane: holder-guarded completion,
// per-direction counters, force mandate cleared, ErrClaimLost for stolen
// claims. Same counter-advancement recovery predicate as FinishSync.
func (s *S3WAL) FinishPush(ctx context.Context, repoID, holder string, success bool) (RepoUpstream, error) {
	assertf(repoID != "", "FinishPush: empty repo id")
	assertf(holder != "", "FinishPush: empty holder")
	successInc := 0
	if success {
		successInc = 1
	}
	var up RepoUpstream
	var prevRun, prevSuccess int
	err := s.mutate(ctx, s3walMetaMutation{
		op: "finish push",
		fn: func(doc *s3walMetaDoc) (bool, error) {
			u := doc.Upstreams[repoID]
			if u == nil {
				return false, fmt.Errorf("upstream for repo %q: %w", repoID, ErrNotFound)
			}
			if !u.PushSyncing || u.PushClaimHolder != holder {
				if u.PushSyncing && u.PushClaimHolder != holder {
					return false, fmt.Errorf("finish push for repo %q: now held by %q: %w", repoID, u.PushClaimHolder, ErrClaimLost)
				}
				return false, fmt.Errorf("upstream for repo %q: %w", repoID, ErrNotFound)
			}
			prevRun, prevSuccess = u.PushRunCount, u.PushSuccessCount
			u.PushSyncing = false
			u.PushRunCount++
			u.PushSuccessCount += successInc
			u.PushClaimHolder, u.PushClaimExpiresAt, u.PushClaimForce = "", 0, false
			up = u.toRepoUpstream(repoID)
			return true, nil
		},
		recover: func(doc *s3walMetaDoc) s3walMetaVerdict {
			u := doc.Upstreams[repoID]
			if u == nil {
				return s3walMetaRerun
			}
			if u.PushSyncing && u.PushClaimHolder == holder {
				return s3walMetaRerun
			}
			if u.PushRunCount == prevRun+1 && u.PushSuccessCount == prevSuccess+successInc {
				return s3walMetaApplied
			}
			return s3walMetaRerun
		},
	})
	if err != nil {
		return RepoUpstream{}, fmt.Errorf("finish push for repo %q: %w", repoID, err)
	}
	return up, nil
}
