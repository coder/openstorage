// Package sync implements upstream mirror synchronization (docs/explanation/domain-model.md
// invariant 11, docs/reference/http-api.md "Upstream sync", docs/reference/webhooks.md repo.sync.* events): a
// worker mirror-fetches from an upstream git provider and loopback-pushes
// into this server's own git smart-HTTP endpoint, reusing pack storage, ref
// policy, CAS, and push-event emission (phase-5 Decision 4).
//
// The package defines the consumer-side contracts the worker and the HTTP
// handlers need (CredentialStore, UpstreamStore, EventSink, Queue) plus an
// in-memory implementation (MemStore) used by tests and available as the
// local-mode stand-in until the durable meta-backed rows land in the
// foundation slice. Credential values are stored as given and never logged.
package sync

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// assertf is a fail-fast invariant check (crash-and-burn style). Never used
// on untrusted input — only on programming errors.
func assertf(cond bool, format string, args ...any) {
	if !cond {
		panic(fmt.Sprintf("sync assertion failed: "+format, args...))
	}
}

// Sentinel errors. Store implementations return them (possibly wrapped);
// callers match with errors.Is.
var (
	// ErrNotFound: unknown credential/upstream/repo row.
	ErrNotFound = errors.New("not found")
	// ErrCredentialExists: CreateCredential when the repo already has one
	// (docs/reference/http-api.md: POST /repos/git-credentials 409 = exists).
	ErrCredentialExists = errors.New("credential already exists")
	// ErrUpstreamExists: PutUpstream when the repo already has an upstream
	// row (base_repo is set exactly once at repo creation).
	ErrUpstreamExists = errors.New("upstream already exists")
	// ErrSyncInProgress: BeginSync while a run is already in flight —
	// callers skip silently (invariant 11: no overlapping runs).
	ErrSyncInProgress = errors.New("sync already in progress")
	// ErrPushInProgress: BeginPush while an outbound run is already in
	// flight — callers skip silently (issue #90: at most one active
	// forward per repo; the claim's finisher covers pending requests).
	ErrPushInProgress = errors.New("push already in progress")
	// ErrClaimLost: FinishSync by a holder whose expired claim was stolen
	// (issue #86) — the late finisher abandons silently; the thief owns
	// the run lifecycle now.
	ErrClaimLost = errors.New("sync claim lost")
)

// Credential is one stored HTTPS credential for a repo's generic git
// upstream (docs/reference/http-api.md "Git credentials"). Password is the upstream secret
// (token or password) verbatim; it must never be logged.
type Credential struct {
	ID        string // server-generated "ggc_<16 hex>"
	RepoID    string
	Username  string // optional; token-only providers omit it
	Password  string
	CreatedAt time.Time // UTC
}

// Upstream is one repo's upstream mirror link (the repo_upstreams row).
type Upstream struct {
	RepoID       string
	Provider     string
	Owner        string
	Name         string
	UpstreamHost string // resolved at create time: provider default or explicit upstream_host
	RunCount     int    // completed sync runs (success + failure)
	SuccessCount int    // completed successful runs; is_first_sync := SuccessCount == 0
	Syncing      bool   // one run in flight (BeginSync..FinishSync)
	// SyncPending marks a sync request (RequestSync) not yet covered by a
	// claimed run: BeginSync consumes it, so a flag still set on the row
	// FinishSync returns means requests arrived after the finished run
	// claimed (its upstream fetch may predate them) and a follow-up run
	// is due (issue #79 liveness).
	SyncPending bool
	// SyncClaimHolder/SyncClaimExpiresAt lease the Syncing claim (issue
	// #86): each claim records the run's holder token and an expiry, and
	// BeginSync steals a claim whose lease has provably expired — the
	// only recovery when the holder process died mid-run.
	SyncClaimHolder    string
	SyncClaimExpiresAt time.Time
	// ForwardOnPush opts the repo into scheduling an outbound
	// push-upstream run for every won local push (issue #90,
	// base_repo.forward_pushes). Default off: forwarding stays
	// explicit-trigger.
	ForwardOnPush bool
	// PushPending is the outbound analogue of SyncPending (issue #90): a
	// three-level durable marker (PushPendingNone/Normal/Force) set by
	// RequestPush and consumed by BeginPush. Levels coalesce by MAX so a
	// forced request is never downgraded by a later plain one; the level
	// must ride the durable marker (not the queue job) because queue
	// deduplication is keyed by repo only and a crash-rescued job must
	// still remember force.
	PushPending int
	// PushSyncing/PushClaimHolder/PushClaimExpiresAt lease the outbound
	// claim exactly like the inbound triple leases Syncing. The lanes are
	// deliberately SEPARATE: the inbound claim protects local refs (the
	// loopback force-push), the outbound claim protects upstream refs —
	// sharing one claim would couple the two lanes' liveness without
	// protecting anything the non-force outbound push does not already
	// protect. No legacy adoption exists for this lane: no pre-#90 binary
	// ever holds a push claim.
	PushSyncing        bool
	PushClaimHolder    string
	PushClaimExpiresAt time.Time
	PushRunCount       int // completed outbound runs (success + failure)
	PushSuccessCount   int // completed successful outbound runs
	CreatedAt          time.Time
}

// PushPending levels (issue #90). Force must dominate: markers coalesce by
// MAX at RequestPush and are consumed whole by BeginPush.
const (
	PushPendingNone  = 0
	PushPendingPush  = 1
	PushPendingForce = 2
)

// CloneURL builds the upstream fetch URL, https://{host}/{owner}/{name}.git
// (docs/reference/http-api.md). A host that already carries a scheme (self-host upstreams,
// tests) is used verbatim as the base.
func (u Upstream) CloneURL() string {
	assertf(u.Owner != "" && u.Name != "" && u.UpstreamHost != "",
		"CloneURL on incomplete upstream row (repo %q)", u.RepoID)
	base := u.UpstreamHost
	if !strings.Contains(base, "://") {
		base = "https://" + base
	}
	return strings.TrimRight(base, "/") + "/" + u.Owner + "/" + u.Name + ".git"
}

// providerHosts maps the docs/reference/http-api.md sync providers to their default hosts.
// Empty string = no default; upstream_host is required (self-hosted forges).
var providerHosts = map[string]string{
	"github":    "github.com",
	"gitlab":    "gitlab.com",
	"bitbucket": "bitbucket.org",
	"codeberg":  "codeberg.org",
	"sr.ht":     "git.sr.ht",
	"sourcehut": "git.sr.ht", // openapi enum alias for sr.ht (docs/archive/phases/14-phase5-implementation-notes.md deferred #1)
	"gitea":     "",
	"forgejo":   "",
}

// ProviderHost resolves a sync provider name to its default host.
// known=false for providers this backend cannot mirror-sync (including
// "code"/"code.storage", which are fork-only).
func ProviderHost(provider string) (host string, known bool) {
	host, known = providerHosts[provider]
	return host, known
}

// NewCredentialID generates a fresh credential ID in the documented shape
// (openapi example: ggc_1234567890abcdef).
func NewCredentialID() string {
	var buf [8]byte
	_, err := rand.Read(buf[:])
	assertf(err == nil, "crypto/rand failed: %v", err)
	return "ggc_" + hex.EncodeToString(buf[:])
}

// CredentialStore persists git credentials (docs/reference/http-api.md "Git credentials").
// The durable implementation is meta-backed (foundation slice); MemStore
// implements it in-memory.
type CredentialStore interface {
	// CreateCredential stores cred (ID/CreatedAt assigned by the store).
	// ErrCredentialExists when the repo already has a credential.
	CreateCredential(ctx context.Context, cred Credential) (Credential, error)
	// GetCredential returns the credential by ID; ErrNotFound when absent.
	GetCredential(ctx context.Context, id string) (Credential, error)
	// CredentialMeta returns the credential with Password always "" — the
	// stored password is never read or decrypted, so ownership prechecks
	// for update/delete work even when an encrypted row's key is gone
	// (issue #54 recovery: replace or remove without the old value).
	// ErrNotFound when absent.
	CredentialMeta(ctx context.Context, id string) (Credential, error)
	// UpdateCredential replaces username/password; ErrNotFound when absent.
	UpdateCredential(ctx context.Context, id, username, password string) (Credential, error)
	// DeleteCredential removes the credential; ErrNotFound when absent.
	DeleteCredential(ctx context.Context, id string) error
	// CredentialForRepo returns the repo's credential; ErrNotFound when the
	// upstream is public (no credential stored).
	CredentialForRepo(ctx context.Context, repoID string) (Credential, error)
}

// UpstreamStore persists upstream mirror links and their sync-run state.
type UpstreamStore interface {
	// PutUpstream inserts the row (CreatedAt assigned by the store).
	// ErrUpstreamExists when the repo already has one.
	PutUpstream(ctx context.Context, up Upstream) (Upstream, error)
	// GetUpstream returns the row; ErrNotFound when detached/never linked.
	GetUpstream(ctx context.Context, repoID string) (Upstream, error)
	// DeleteUpstream detaches the repo; ErrNotFound when already detached
	// (DELETE /repos/base is idempotent — callers treat that as success).
	DeleteUpstream(ctx context.Context, repoID string) error
	// RequestSync marks a pending sync request (SyncPending = true).
	// Callers that promise a sync (POST /repos/pull-upstream 202) set it
	// BEFORE enqueueing: if the enqueue coalesces with a run that already
	// claimed, the flag survives to that run's FinishSync and the
	// finisher schedules a follow-up run. ErrNotFound when detached.
	RequestSync(ctx context.Context, repoID string) error
	// BeginSync atomically claims a sync run (syncing false -> true)
	// under a lease and returns the row as of the claim (RunCount =
	// completed runs). holder is the run's claim token, recorded with an
	// expiry of now+lease; a claim whose lease has EXPIRED is stolen
	// atomically, and stolenFrom then carries the previous holder for
	// observability (issue #86). The claim consumes SyncPending: every
	// request marked before the claim is observed by the claimed run's
	// upstream fetch, which happens after it. ErrNotFound when detached;
	// ErrSyncInProgress when an unexpired run is already syncing.
	BeginSync(ctx context.Context, repoID, holder string, lease time.Duration) (up Upstream, stolenFrom string, err error)
	// FinishSync completes the run claimed by holder: increments RunCount
	// (and SuccessCount when success), clears Syncing and the claim
	// lease, returns the updated row. SyncPending is NOT touched: a set
	// flag on the returned row tells the finisher to schedule a follow-up
	// run (issue #79 liveness). ErrNotFound when the repo was detached
	// mid-run — the run then ends silently (docs/reference/webhooks.md:
	// detach cancels without a failed event). ErrClaimLost when the
	// caller's expired claim was stolen: the late finisher abandons
	// without touching the thief's claim or the counters.
	FinishSync(ctx context.Context, repoID, holder string, success bool) (Upstream, error)

	// RequestPush marks a pending outbound push request (issue #90),
	// raising PushPending to at least PushPendingPush (PushPendingForce
	// when force) — levels only ever coalesce upward, mirroring the
	// RequestSync-before-enqueue protocol: callers that promise a push
	// (POST /repos/push-upstream 202, the forward-on-push poke) mark
	// BEFORE enqueueing so a request that coalesces with a run that
	// already claimed survives to that run's FinishPush. ErrNotFound
	// when detached.
	RequestPush(ctx context.Context, repoID string, force bool) error
	// BeginPush atomically claims an outbound run under a lease —
	// TryBeginSync semantics on the push_* lane — and consumes
	// PushPending, returning whether the consumed marker demanded force.
	// ErrNotFound when detached; ErrPushInProgress when an unexpired
	// outbound run is already in flight.
	BeginPush(ctx context.Context, repoID, holder string, lease time.Duration) (up Upstream, force bool, stolenFrom string, err error)
	// FinishPush completes the outbound run claimed by holder —
	// FinishSync semantics on the push_* lane. PushPending is NOT
	// touched; a set level on the returned row means requests arrived
	// after this run claimed and a follow-up run is due.
	FinishPush(ctx context.Context, repoID, holder string, success bool) (Upstream, error)
}

// EventSink receives one repo.sync.* event for fan-out to the org's
// registered webhooks (docs/reference/webhooks.md). payload is the exact JSON body to sign and
// deliver; implementations must not mutate it. The webhook-delivery slice
// provides the production sink; tests use recorders.
type EventSink interface {
	Emit(ctx context.Context, org, eventType string, payload []byte)
}

// Job is one queued sync request. Ref is the optional pull-upstream hint
// (docs/reference/http-api.md {ref?}); the worker currently performs a full mirror sync — a
// superset of any single-ref request — so Ref is carried for logging only.
// Retry counts consecutive transient-failure retries of an outbound push
// run (issue #90): it selects the backoff delay and bounds the retry chain,
// and deliberately does NOT participate in queue deduplication.
type Job struct {
	Org    string // org name (JWT iss)
	RepoID string
	Ref    string
	Retry  int
}

// Queue schedules sync runs. The Postgres deployment backs this with the
// durable jobs seam (foundation slice); InProcessQueue is the in-process
// local-mode implementation.
type Queue interface {
	EnqueueSync(ctx context.Context, job Job) error
	// EnqueuePush schedules one outbound push-upstream run (issue #90).
	// Deduplication mirrors EnqueueSync: callers that promise the push
	// mark it first via UpstreamStore.RequestPush.
	EnqueuePush(ctx context.Context, job Job) error
}
