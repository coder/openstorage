// Phase-5 eventing/sync persistence contract (docs/06 webhooks, docs/03
// git-credentials + upstream sync): webhook registrations, git credentials,
// and repo upstream links live in the meta database next to the repo rows
// they reference.
//
// INTEGRATION CONTRACT: both backends (pg_eventing.go / sqlite_eventing.go)
// implement EventingStore, and Store embeds it once the implementations
// land. Consumers never use these methods directly — the events and sync
// packages consume them through thin adapters (events.MetaWebhookStore,
// sync.MetaStores) that translate types and sentinel errors.
package meta

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"
)

// Webhook is one registered delivery endpoint (docs/06). Secret is the HMAC
// signing key, stored plaintext because every delivery signs with it
// (ADR: at-rest secret encryption is out of scope for phase 5). Never log
// Secret.
type Webhook struct {
	ID        string // server-generated "wh_<16 hex>"
	OrgID     int64
	URL       string
	Secret    string
	CreatedAt time.Time // UTC
}

// GitCredential is one stored HTTPS credential for a repo's git upstream
// (docs/03 "Git credentials"). Password is stored verbatim (plaintext ADR,
// as for Webhook.Secret) and must never be logged.
type GitCredential struct {
	ID        string // server-generated "ggc_<16 hex>"
	RepoID    string
	Username  string // optional; token-only providers omit it
	Password  string
	CreatedAt time.Time // UTC
}

// RepoUpstream is one repo's upstream mirror link plus its sync-run state
// (docs/01 invariant 11). is_first_sync := SuccessCount == 0; detach =
// delete the row.
type RepoUpstream struct {
	RepoID       string
	Provider     string
	Owner        string
	Name         string
	UpstreamHost string // resolved at create time: provider default or explicit upstream_host
	RunCount     int    // completed sync runs (success + failure)
	SuccessCount int    // completed successful runs
	Syncing      bool   // one run in flight (TryBeginSync..FinishSync)
	CreatedAt    time.Time
}

// NewEventingID returns "<prefix>_<16 hex>" — the shared ID shape for
// webhooks (wh_) and git credentials (ggc_, openapi example
// ggc_1234567890abcdef).
func NewEventingID(prefix string) string {
	assertf(prefix != "", "NewEventingID: empty prefix")
	var buf [8]byte
	_, err := rand.Read(buf[:])
	assertf(err == nil, "NewEventingID: crypto/rand failed: %v", err)
	return prefix + "_" + hex.EncodeToString(buf[:])
}

// EventingStore is the phase-5 persistence surface. Org-scoped methods key
// by org name (JWT `iss`) and return ErrNotFound for unknown orgs, matching
// the Store conventions above.
type EventingStore interface {
	// CreateWebhook inserts a webhook with a server-generated ID.
	// ErrNotFound when the org is unknown.
	CreateWebhook(ctx context.Context, org, url, secret string) (Webhook, error)
	// ListWebhooks returns the org's webhooks in registration order.
	// ErrNotFound when the org is unknown.
	ListWebhooks(ctx context.Context, org string) ([]Webhook, error)
	// GetWebhook returns the webhook with the given ID regardless of org
	// (delivery jobs carry only the ID); ErrNotFound when absent.
	GetWebhook(ctx context.Context, id string) (Webhook, error)
	// DeleteWebhook removes the org's webhook; ErrNotFound when the org is
	// unknown or the ID does not belong to it.
	DeleteWebhook(ctx context.Context, org, id string) error

	// CreateGitCredential stores cred (ID/CreatedAt assigned by the store;
	// caller-set ID/CreatedAt are ignored). ErrAlreadyExists when the repo
	// already has a credential (docs/03: 409).
	CreateGitCredential(ctx context.Context, cred GitCredential) (GitCredential, error)
	// GetGitCredential returns the credential by ID; ErrNotFound when absent.
	GetGitCredential(ctx context.Context, id string) (GitCredential, error)
	// UpdateGitCredential replaces username/password; ErrNotFound when absent.
	UpdateGitCredential(ctx context.Context, id, username, password string) (GitCredential, error)
	// DeleteGitCredential removes the credential; ErrNotFound when absent.
	DeleteGitCredential(ctx context.Context, id string) error
	// GitCredentialForRepo returns the repo's credential; ErrNotFound when
	// none is stored (public upstream).
	GitCredentialForRepo(ctx context.Context, repoID string) (GitCredential, error)

	// SetRepoUpstream inserts the upstream row (CreatedAt assigned by the
	// store; RunCount/SuccessCount/Syncing start zeroed). ErrAlreadyExists
	// when the repo already has one (base_repo is set exactly once).
	SetRepoUpstream(ctx context.Context, up RepoUpstream) (RepoUpstream, error)
	// GetRepoUpstream returns the row; ErrNotFound when detached/never linked.
	GetRepoUpstream(ctx context.Context, repoID string) (RepoUpstream, error)
	// DeleteRepoUpstream detaches the repo; ErrNotFound when already detached.
	DeleteRepoUpstream(ctx context.Context, repoID string) error
	// TryBeginSync atomically claims a sync run (syncing false -> true) and
	// returns the row as of the claim. claimed=false (row returned) when a
	// run is already in flight; ErrNotFound when detached.
	TryBeginSync(ctx context.Context, repoID string) (up RepoUpstream, claimed bool, err error)
	// FinishSync completes the claimed run: increments RunCount (and
	// SuccessCount when success), clears Syncing, returns the updated row.
	// ErrNotFound when the repo was detached mid-run.
	FinishSync(ctx context.Context, repoID string, success bool) (RepoUpstream, error)
}
