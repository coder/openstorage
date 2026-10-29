// Package meta declares the metadata-store contract for organizations, org
// public keys, and repositories (docs/explanation/domain-model.md domain model, docs/reference/http-api.md HTTP API).
//
// FROZEN CONTRACT: parallel phase-1 slices implement Store against Postgres
// (schema: orgs / org_public_keys / repos with UNIQUE(org_id, url) spanning
// soft-deleted rows) and build HTTP handlers on top of these exact
// signatures. Do not change them without coordinating across slices.
package meta

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"time"
)

// Sentinel errors implementations must return (possibly wrapped; callers
// match with errors.Is) so HTTP handlers can map storage outcomes onto the
// docs/reference/http-api.md status contract.
var (
	// ErrNotFound: unknown org, or repo absent/soft-deleted → 404.
	ErrNotFound = errors.New("not found")
	// ErrAlreadyExists: CreateRepo unique violation on (org, url), including
	// against a soft-deleted row → 409.
	ErrAlreadyExists = errors.New("repository already exists")
	// ErrAlreadyDeleted: SoftDeleteRepo on an already soft-deleted repo → 409.
	ErrAlreadyDeleted = errors.New("repository already deleted")
	// ErrClaimLost: FinishSync by a holder whose expired sync claim was
	// stolen (issue #86) — the late finisher must abandon silently.
	ErrClaimLost = errors.New("sync claim lost")
	// ErrInvalidCursor: ListRepos received an undecodable cursor → 400.
	ErrInvalidCursor = errors.New("invalid cursor")
)

// Org is one registered organization (single-org self-host mode seeds
// exactly one at startup).
type Org struct {
	ID        int64
	Name      string // JWT `iss` claim value; unique
	CreatedAt time.Time
}

// Repo is one repository row.
type Repo struct {
	RepoID        string // server-generated "repo_<12 hex>"
	OrgID         int64
	URL           string     // JWT `repo` claim value at create time
	DefaultBranch string     // validated ref component, default "main"
	CreatedAt     time.Time  // UTC; rendered as RFC 3339 in responses
	DeletedAt     *time.Time // nil while live (soft delete per docs/explanation/domain-model.md)
	CreateState   CreateState
}

// CreateState records whether a repo's create finished (issue #388). A
// repeat POST /repos may answer 200 only for CreateStateComplete.
type CreateState int

const (
	// CreateStateLegacy: the create is not tracked. The row predates create
	// tracking (on S3-WAL, a metadata v1 row), or an untracked path created
	// it (CreateRepo). A crashed fork can look like this, so the row is
	// never treated as complete.
	CreateStateLegacy CreateState = 0
	// CreateStatePending: inserted, and the create has not finished.
	CreateStatePending CreateState = 1
	// CreateStateComplete: the create finished.
	CreateStateComplete CreateState = 2
)

// knownCreateState reads a value this binary does not know (written by a
// newer binary before a downgrade) as legacy, which is never treated as
// complete, and logs a warning instead of failing the read.
func knownCreateState(repoID string, s CreateState) CreateState {
	if s == CreateStateLegacy || s == CreateStatePending || s == CreateStateComplete {
		return s
	}
	slog.Warn("meta: unknown repos.create_state, reading it as legacy", "repo_id", repoID, "create_state", int(s))
	return CreateStateLegacy
}

// completeOutcome answers CompleteRepoCreate for a row its conditional
// update did not change (any row, deleted included).
func completeOutcome(r Repo) (CreateOutcome, error) {
	switch {
	case r.DeletedAt != nil:
		return CreateDeleted, nil
	case r.CreateState == CreateStateComplete:
		return CreateAlreadyComplete, nil
	default:
		// A live pending row would have been updated; a legacy row was
		// never inserted pending, so no create can be finishing it.
		return 0, fmt.Errorf("complete create of repo id %q: create_state %d is not pending", r.RepoID, r.CreateState)
	}
}

// CreateOutcome is the result of CompleteRepoCreate.
type CreateOutcome int

const (
	CreateCompleted       CreateOutcome = iota + 1 // this call finished the create
	CreateAlreadyComplete                          // an earlier call finished it
	CreateDeleted                                  // the repo was deleted first; the create must not report success
)

// ListParams are the GET /repos query parameters after handler-side
// validation: handlers reject bad `limit` with 400 and pass the resolved
// value here (default 20, max 100); the opaque cursor is validated by the
// store (ErrInvalidCursor).
type ListParams struct {
	Q      string // case-insensitive substring filter on URL, pre-trimmed; "" = no filter
	Cursor string // opaque cursor from a previous ListResult; "" = first page
	Limit  int    // page size, already clamped/validated by the caller
}

// ListResult is one page of repos ordered by (created_at DESC, repo_id DESC).
type ListResult struct {
	Repos      []Repo // never nil; empty slice when no matches
	NextCursor string // opaque; "" when exhausted
	HasMore    bool
}

// SigningKey is one registered commit-signing key for an org (docs/reference/authentication.md
// verify-sig; the OSS stand-in for the upstream dashboard key registry).
// KeyText is either a single SSH public key line ("ssh-ed25519 AAAA...")
// or an ASCII-armored PGP public key block; callers validate the format
// before AddSigningKey — the store persists the text verbatim.
type SigningKey struct {
	ID        int64
	OrgID     int64
	KeyText   string
	CreatedAt time.Time // UTC
}

// Store is the metadata persistence contract. All org-scoped methods key by
// org name (the JWT `iss` claim); implementations resolve it internally and
// return ErrNotFound for unknown orgs.
type Store interface {
	// UpsertOrgWithKeys idempotently creates the org and replaces its
	// registered PEM public keys (startup bootstrap in single-org mode).
	UpsertOrgWithKeys(ctx context.Context, name string, pemPublicKeys []string) (Org, error)

	// PublicKeysForOrg returns the org's registered PEM public key blocks
	// for JWT verification.
	PublicKeysForOrg(ctx context.Context, name string) ([]string, error)

	// OrgNames returns every org row's name, ordered by name. Org rows are
	// retained forever (single-org mode never deletes them), so the serve
	// command uses this at startup to warn about rows that no longer match
	// the configured org (issue #47).
	OrgNames(ctx context.Context) ([]string, error)

	// CreateRepo inserts a live repo with a server-generated RepoID.
	// ErrAlreadyExists when (org, url) is taken — even by a soft-deleted row.
	// The row's CreateState is CreateStateLegacy.
	CreateRepo(ctx context.Context, org, url, defaultBranch string) (Repo, error)

	// CreateRepoWithState is CreateRepo with an explicit CreateState
	// (pending or complete).
	CreateRepoWithState(ctx context.Context, org, url, defaultBranch string, state CreateState) (Repo, error)

	// CompleteRepoCreate moves a pending repo to complete. A soft-deleted
	// repo returns CreateDeleted, even if it was complete. ErrNotFound when
	// no row exists. On a legacy row it returns an error.
	CompleteRepoCreate(ctx context.Context, org, repoID string) (CreateOutcome, error)

	// GetRepoByURL returns the live repo with the given url;
	// ErrNotFound when absent or soft-deleted.
	GetRepoByURL(ctx context.Context, org, url string) (Repo, error)

	// GetRepoByID returns the live repo with the given server-generated
	// RepoID; ErrNotFound when absent or soft-deleted. Used by the git
	// smart-HTTP path, whose URLs address repos by repo_id (docs/explanation/git-protocol-and-namespaces.md) while
	// JWT `repo` claims carry the url (docs/reference/authentication.md).
	GetRepoByID(ctx context.Context, org, repoID string) (Repo, error)

	// UpdateRepoDefaultBranch compare-and-swaps the live repo's default
	// branch from oldBranch to newBranch (openapi.yaml PATCH repo: the
	// update is "a transactional compare-and-swap" against the value the
	// caller read). won=false (no error) when the live row's default
	// branch is no longer oldBranch — someone else changed it first;
	// ErrNotFound when no live repo matches. A won swap also clears the
	// upstream's DefaultBranchOmitted: an explicit choice wins (#599).
	UpdateRepoDefaultBranch(ctx context.Context, org, url, oldBranch, newBranch string) (won bool, err error)

	// SoftDeleteRepo sets deleted_at and returns the affected repo.
	// ErrNotFound when no row exists; ErrAlreadyDeleted when already
	// soft-deleted.
	SoftDeleteRepo(ctx context.Context, org, url string) (Repo, error)

	// ListRepos pages live repos for the org per ListParams.
	ListRepos(ctx context.Context, org string, params ListParams) (ListResult, error)

	// AddSigningKey registers a commit-signing key for the org and returns
	// the stored row (ID for later removal). keyText must be non-blank; see
	// SigningKey for the accepted formats (callers validate before adding).
	AddSigningKey(ctx context.Context, org, keyText string) (SigningKey, error)

	// SigningKeysForOrg returns the org's registered signing keys in
	// registration order. Empty slice when none are registered — the
	// verify-sig hook reads this fresh from the store on every push and
	// rejects on an empty keyring.
	SigningKeysForOrg(ctx context.Context, org string) ([]SigningKey, error)

	// RemoveSigningKey deletes the signing key with the given ID.
	// ErrNotFound when the org is unknown or the ID does not belong to it.
	RemoveSigningKey(ctx context.Context, org string, id int64) error

	// EventingStore is the phase-5 webhook / git-credential / repo-upstream
	// persistence surface (eventing.go).
	EventingStore
}

// Backend extends Store with the lifecycle the wire seam needs: serve runs
// InitSchema once at startup and Close on shutdown. Consumers keep coding
// against Store; hooks never run DDL.
type Backend interface {
	Store
	// InitSchema creates the metadata tables (idempotent, safe under
	// concurrent startups).
	InitSchema(ctx context.Context) error
	Close()
}

// Open dispatches on the URL scheme to the matching Backend:
//
//	postgres:// or postgresql://  → OpenPG
//	sqlite:///abs/path/db.sqlite  → pure-Go SQLite store
//	s3wal://bucket?endpoint=…     → single-object CAS store over conditional S3
//
// Open never runs InitSchema: hooks reconstruct stores from env URLs and
// must not run DDL — the serve/wire seam owns schema init (for s3wal that
// means the conditional-write probe). Options (WithSecretsCodec) apply to
// any backend.
func Open(ctx context.Context, dbURL string, opts ...Option) (Backend, error) {
	assertf(dbURL != "", "Open: empty metadata store URL")
	u, err := url.Parse(dbURL)
	if err != nil {
		return nil, fmt.Errorf("parse metadata store URL: %w", err)
	}
	switch u.Scheme {
	case "postgres", "postgresql":
		return OpenPG(ctx, dbURL, opts...)
	case "sqlite":
		return OpenSQLite(ctx, dbURL, opts...)
	case "s3wal":
		return OpenS3WAL(ctx, dbURL, opts...)
	default:
		return nil, fmt.Errorf("metadata store URL scheme %q unsupported (want postgres, postgresql, sqlite, or s3wal)", u.Scheme)
	}
}
