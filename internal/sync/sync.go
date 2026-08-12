// Package sync implements upstream mirror synchronization (docs/01
// invariant 11, docs/03 "Upstream sync", docs/06 repo.sync.* events): a
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
	// (docs/03: POST /repos/git-credentials 409 = exists).
	ErrCredentialExists = errors.New("credential already exists")
	// ErrUpstreamExists: PutUpstream when the repo already has an upstream
	// row (base_repo is set exactly once at repo creation).
	ErrUpstreamExists = errors.New("upstream already exists")
	// ErrSyncInProgress: BeginSync while a run is already in flight —
	// callers skip silently (invariant 11: no overlapping runs).
	ErrSyncInProgress = errors.New("sync already in progress")
)

// Credential is one stored HTTPS credential for a repo's generic git
// upstream (docs/03 "Git credentials"). Password is the upstream secret
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
	CreatedAt    time.Time
}

// CloneURL builds the upstream fetch URL, https://{host}/{owner}/{name}.git
// (docs/03). A host that already carries a scheme (self-host upstreams,
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

// providerHosts maps the docs/03 sync providers to their default hosts.
// Empty string = no default; upstream_host is required (self-hosted forges).
var providerHosts = map[string]string{
	"github":    "github.com",
	"gitlab":    "gitlab.com",
	"bitbucket": "bitbucket.org",
	"codeberg":  "codeberg.org",
	"sr.ht":     "git.sr.ht",
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

// CredentialStore persists git credentials (docs/03 "Git credentials").
// The durable implementation is meta-backed (foundation slice); MemStore
// implements it in-memory.
type CredentialStore interface {
	// CreateCredential stores cred (ID/CreatedAt assigned by the store).
	// ErrCredentialExists when the repo already has a credential.
	CreateCredential(ctx context.Context, cred Credential) (Credential, error)
	// GetCredential returns the credential by ID; ErrNotFound when absent.
	GetCredential(ctx context.Context, id string) (Credential, error)
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
	// BeginSync atomically claims a sync run (syncing false -> true) and
	// returns the row as of the claim (RunCount = completed runs).
	// ErrNotFound when detached; ErrSyncInProgress when already syncing.
	BeginSync(ctx context.Context, repoID string) (Upstream, error)
	// FinishSync completes the claimed run: increments RunCount (and
	// SuccessCount when success), clears Syncing, returns the updated row.
	// ErrNotFound when the repo was detached mid-run — the run then ends
	// silently (docs/06: detach cancels without a failed event).
	FinishSync(ctx context.Context, repoID string, success bool) (Upstream, error)
}

// EventSink receives one repo.sync.* event for fan-out to the org's
// registered webhooks (docs/06). payload is the exact JSON body to sign and
// deliver; implementations must not mutate it. The webhook-delivery slice
// provides the production sink; tests use recorders.
type EventSink interface {
	Emit(ctx context.Context, org, eventType string, payload []byte)
}

// Job is one queued sync request. Ref is the optional pull-upstream hint
// (docs/03 {ref?}); the worker currently performs a full mirror sync — a
// superset of any single-ref request — so Ref is carried for logging only.
type Job struct {
	Org    string // org name (JWT iss)
	RepoID string
	Ref    string
}

// Queue schedules sync runs. The Postgres deployment backs this with the
// durable jobs seam (foundation slice); InProcessQueue is the in-process
// local-mode implementation.
type Queue interface {
	EnqueueSync(ctx context.Context, job Job) error
}
