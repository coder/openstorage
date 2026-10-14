// Phase-5 eventing/sync persistence contract (docs/reference/webhooks.md webhooks, docs/reference/http-api.md
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
	"strconv"
	"time"

	"github.com/coder/openstorage/internal/envconfig"
)

// Webhook is one registered delivery endpoint (docs/reference/webhooks.md). Secret is the HMAC
// signing key — plaintext at this interface; at rest it is stored plaintext
// by default and envelope-encrypted when the store carries a secrets codec
// (issue #54, OPENSTORAGE_SECRETS_KEY; see secrets.go). Never log Secret.
type Webhook struct {
	ID        string // server-generated "wh_<16 hex>"
	OrgID     int64
	URL       string
	Secret    string
	CreatedAt time.Time // UTC
}

// GitCredential is one stored HTTPS credential for a repo's git upstream
// (docs/reference/http-api.md "Git credentials"). Password is plaintext at this interface and
// gets the same optional at-rest envelope encryption as Webhook.Secret;
// it must never be logged.
type GitCredential struct {
	ID        string // server-generated "ggc_<16 hex>"
	RepoID    string
	Username  string // optional; token-only providers omit it
	Password  string
	CreatedAt time.Time // UTC
}

// RepoUpstream is one repo's upstream mirror link plus its sync-run state
// (docs/explanation/domain-model.md invariant 11). is_first_sync := SuccessCount == 0; detach =
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
	// SyncPending marks a sync request (RequestSync) not yet covered by a
	// claimed run: TryBeginSync consumes it, so a flag still set when
	// FinishSync returns means requests arrived AFTER the finished run
	// claimed (its fetch may predate them) and a follow-up run is due
	// (issue #79 liveness).
	SyncPending bool
	// SyncClaimHolder/SyncClaimExpiresAt lease the Syncing claim (issue
	// #86): every claim records a per-run holder token and an expiry, and
	// a conflicting claimant may STEAL a claim whose lease has provably
	// expired — the only recovery for a holder process that died between
	// TryBeginSync and FinishSync. FinishSync clears both (holder "" and
	// expiry at the zero instant, epoch/0 in storage).
	SyncClaimHolder    string
	SyncClaimExpiresAt time.Time
	// ForwardOnPush opts the repo into forwarding won local pushes to the
	// upstream (issue #90, base_repo.forward_pushes; default off).
	ForwardOnPush bool
	// PushPending is the outbound lane's pending marker (issue #90): a
	// three-level value (0 none / 1 push / 2 force) that coalesces upward
	// via RequestPush and is consumed by TryBeginPush. The level lives on
	// the durable row — not the queue job — because job deduplication is
	// keyed by repo and a crash-rescued job must still remember force.
	PushPending int
	// PushSyncing/PushClaimHolder/PushClaimExpiresAt lease the outbound
	// claim exactly like the inbound triple. The lanes are separate on
	// purpose: the inbound claim protects local refs (the loopback
	// force-push), the outbound claim protects upstream refs. No legacy
	// adoption exists here — no pre-#90 binary ever holds a push claim,
	// so an expired push lease is always stealable.
	PushSyncing        bool
	PushClaimHolder    string
	PushClaimExpiresAt time.Time
	// PushClaimForce persists the force mandate the in-flight claim
	// consumed from the marker, until FinishPush clears it: a claim whose
	// holder crashed still owes the accepted forced request, so stealing
	// an expired claim inherits its force (OR-ed with the thief's own
	// consumed level) instead of silently downgrading the rescue.
	PushClaimForce   bool
	PushRunCount     int // completed outbound runs (success + failure)
	PushSuccessCount int // completed successful outbound runs
	CreatedAt        time.Time
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

// LegacyClaimHolder marks a sync claim taken by a pre-lease replica and
// adopted into the lease regime by a new one (TryBeginSync mixed-fleet
// staging). The '!' prefix cannot occur in real holder tokens, whose
// process tag is hostname-pid (RFC 1123 hostnames have no '!').
//
// Adopted legacy claims are NEVER stolen by default: a pre-lease binary
// has neither the ref-update fence nor a holder-guarded FinishSync, so no
// finite grace can make stealing from a possibly-alive one safe. Stealing
// them requires EnvSyncStealLegacy — the operator's assertion that no
// pre-lease replica remains (same fleet-prerequisite pattern as
// delete-mode sweeping), after which legacy rows can only be pre-upgrade
// wedges. Claims from lease-aware replicas heal automatically regardless.
const LegacyClaimHolder = "!legacy-grace"

// EnvSyncStealLegacy enables stealing adopted legacy claims whose grace
// has lapsed. Any value strconv.ParseBool accepts as true enables it;
// malformed values stay disabled (the safe direction).
const EnvSyncStealLegacy = "OPENSTORAGE_SYNC_STEAL_LEGACY"

// legacyStealEnabled reads EnvSyncStealLegacy per decision (cheap; tests
// flip it with t.Setenv).
func legacyStealEnabled() bool {
	v, err := strconv.ParseBool(envconfig.Getenv(EnvSyncStealLegacy))
	return err == nil && v
}

// EventingStore is the phase-5 persistence surface. Org-scoped methods key
// by org name (JWT `iss`) and return ErrNotFound for unknown orgs, matching
// the Store conventions above.
type EventingStore interface {
	// CreateWebhook inserts a webhook with a server-generated ID.
	// ErrNotFound when the org is unknown.
	CreateWebhook(ctx context.Context, org, url, secret string) (Webhook, error)
	// ListWebhooks returns the org's webhooks in registration order.
	// ErrNotFound when the org is unknown. ListWebhooks is an enumeration
	// surface: a row whose stored secret cannot be decrypted (issue #54)
	// is returned with Secret == "" rather than failing the listing, so
	// event fan-out and the list API (which never expose secrets) keep
	// working and the bad row's ID stays discoverable for deletion.
	// Consumers that need the secret must use GetWebhook, which fails
	// closed per webhook.
	ListWebhooks(ctx context.Context, org string) ([]Webhook, error)
	// GetWebhook returns the webhook with the given ID regardless of org
	// (delivery jobs carry only the ID); ErrNotFound when absent.
	GetWebhook(ctx context.Context, id string) (Webhook, error)
	// DeleteWebhook removes the org's webhook; ErrNotFound when the org is
	// unknown or the ID does not belong to it.
	DeleteWebhook(ctx context.Context, org, id string) error

	// CreateGitCredential stores cred (ID/CreatedAt assigned by the store;
	// caller-set ID/CreatedAt are ignored). ErrAlreadyExists when the repo
	// already has a credential (docs/reference/http-api.md: 409).
	CreateGitCredential(ctx context.Context, cred GitCredential) (GitCredential, error)
	// GetGitCredential returns the credential by ID; ErrNotFound when absent.
	GetGitCredential(ctx context.Context, id string) (GitCredential, error)
	// GitCredentialMeta returns the credential row with Password always ""
	// — the password column is never read, so this works even when the
	// stored envelope can no longer be decrypted (issue #54). Ownership
	// prechecks for update/delete use it so operators can always replace or
	// remove a credential whose key was retired prematurely or whose row is
	// corrupt. ErrNotFound when absent.
	GitCredentialMeta(ctx context.Context, id string) (GitCredential, error)
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
	// CreateLinkedRepo is CreateRepo plus SetRepoUpstream (and
	// CreateGitCredential when cred is non-nil) in one atomic write, so no
	// reader sees the repo before its upstream link (issue #387: a
	// concurrent create of the same name compares upstreams). The store
	// assigns every ID; caller-set RepoIDs are ignored. ErrAlreadyExists
	// when (org, url) is taken, and then nothing is written. The repo is
	// CreateStatePending until CompleteRepoCreate.
	CreateLinkedRepo(ctx context.Context, org, url, defaultBranch string, up RepoUpstream, cred *GitCredential) (Repo, error)
	// GetRepoUpstream returns the row; ErrNotFound when detached/never linked.
	GetRepoUpstream(ctx context.Context, repoID string) (RepoUpstream, error)
	// DeleteRepoUpstream detaches the repo; ErrNotFound when already detached.
	DeleteRepoUpstream(ctx context.Context, repoID string) error
	// RequestSync marks a pending sync request (sync_pending = true).
	// Callers that promise a sync (POST /repos/pull-upstream 202) set it
	// BEFORE enqueueing: if the enqueue coalesces with a run that already
	// claimed (whose upstream fetch may predate this request), the flag
	// survives to that run's FinishSync and the finisher schedules a
	// follow-up run. ErrNotFound when detached.
	RequestSync(ctx context.Context, repoID string) error
	// TryBeginSync atomically claims a sync run (syncing false -> true)
	// under a lease and returns the row as of the claim. holder is the
	// caller's per-run claim token; the claim records it with an expiry
	// of now+lease. A row whose current claim has EXPIRED is stolen
	// atomically (read-check-write inside one transaction/row lock — no
	// read-then-write window): stolenFrom then carries the previous
	// holder for observability, and the previous holder's late
	// FinishSync fails with ErrClaimLost.
	//
	// Mixed-fleet staging (rolling upgrades from pre-lease versions): a
	// claim with an EMPTY holder was taken by an old replica that writes
	// no lease — it may be a live run, not a crashed one. Such a claim
	// is never stolen on sight; it is ADOPTED instead (holder set to
	// LegacyClaimHolder, expiry now+lease, claimed=false) and becomes
	// stealable only when that grace passes without the old replica's
	// FinishSync clearing it. The grace equals the caller's lease, which
	// strictly dominates any live pre-lease run window.
	//
	// The claim consumes sync_pending: every request marked before the
	// claim is observed by the claimed run's upstream fetch, which
	// happens after it. claimed=false (row returned) when an unexpired
	// run is in flight or a legacy claim was just adopted; ErrNotFound
	// when detached.
	TryBeginSync(ctx context.Context, repoID, holder string, lease time.Duration) (up RepoUpstream, claimed bool, stolenFrom string, err error)
	// FinishSync completes the run claimed by holder: increments RunCount
	// (and SuccessCount when success), clears Syncing and the claim
	// lease, returns the updated row. SyncPending is NOT touched: a set
	// flag in the returned row tells the finisher to schedule a follow-up
	// run (the follow-up's claim consumes it). ErrNotFound when the repo
	// was detached mid-run or no run is in flight; ErrClaimLost when the
	// row is claimed by a DIFFERENT holder — the caller's expired claim
	// was stolen, and the late finisher must abandon without touching the
	// thief's claim or the counters.
	FinishSync(ctx context.Context, repoID, holder string, success bool) (RepoUpstream, error)

	// RequestPush raises PushPending to at least 1 (2 when force) —
	// levels only coalesce upward (issue #90). Same mark-before-enqueue
	// protocol as RequestSync. ErrNotFound when detached.
	RequestPush(ctx context.Context, repoID string, force bool) error
	// TryBeginPush is TryBeginSync on the push_* lane: it claims the
	// outbound run under a lease and consumes PushPending whole,
	// reporting whether the run must force. There is no legacy adoption —
	// an expired push lease is stolen outright, and the steal INHERITS
	// the dead claim's persisted force mandate (PushClaimForce), so a
	// crash between claim and finish cannot silently downgrade an
	// accepted forced request.
	TryBeginPush(ctx context.Context, repoID, holder string, lease time.Duration) (up RepoUpstream, claimed bool, force bool, stolenFrom string, err error)
	// FinishPush is FinishSync on the push_* lane.
	FinishPush(ctx context.Context, repoID, holder string, success bool) (RepoUpstream, error)
}
