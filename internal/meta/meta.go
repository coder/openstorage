// Package meta declares the metadata-store contract for organizations, org
// public keys, and repositories (docs/01 domain model, docs/03 HTTP API).
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
	"net/url"
	"time"

	// Registers the pure-Go "sqlite" database/sql driver the sqlite backend
	// opens (cgo-free cross-compiled single binaries).
	_ "modernc.org/sqlite"
)

// Sentinel errors implementations must return (possibly wrapped; callers
// match with errors.Is) so HTTP handlers can map storage outcomes onto the
// docs/03 status contract.
var (
	// ErrNotFound: unknown org, or repo absent/soft-deleted → 404.
	ErrNotFound = errors.New("not found")
	// ErrAlreadyExists: CreateRepo unique violation on (org, url), including
	// against a soft-deleted row → 409.
	ErrAlreadyExists = errors.New("repository already exists")
	// ErrAlreadyDeleted: SoftDeleteRepo on an already soft-deleted repo → 409.
	ErrAlreadyDeleted = errors.New("repository already deleted")
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
	DeletedAt     *time.Time // nil while live (soft delete per docs/01)
}

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

// SigningKey is one registered commit-signing key for an org (docs/02
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

	// CreateRepo inserts a live repo with a server-generated RepoID.
	// ErrAlreadyExists when (org, url) is taken — even by a soft-deleted row.
	CreateRepo(ctx context.Context, org, url, defaultBranch string) (Repo, error)

	// GetRepoByURL returns the live repo with the given url;
	// ErrNotFound when absent or soft-deleted.
	GetRepoByURL(ctx context.Context, org, url string) (Repo, error)

	// GetRepoByID returns the live repo with the given server-generated
	// RepoID; ErrNotFound when absent or soft-deleted. Used by the git
	// smart-HTTP path, whose URLs address repos by repo_id (docs/05) while
	// JWT `repo` claims carry the url (docs/02).
	GetRepoByID(ctx context.Context, org, repoID string) (Repo, error)

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
//
// Open never runs InitSchema: hooks reconstruct stores from env URLs and
// must not run DDL — the serve/wire seam owns schema init.
func Open(ctx context.Context, dbURL string) (Backend, error) {
	assertf(dbURL != "", "Open: empty metadata store URL")
	u, err := url.Parse(dbURL)
	if err != nil {
		return nil, fmt.Errorf("parse metadata store URL: %w", err)
	}
	switch u.Scheme {
	case "postgres", "postgresql":
		return OpenPG(ctx, dbURL)
	case "sqlite":
		return nil, fmt.Errorf("metadata store scheme %q not implemented yet", u.Scheme)
	default:
		return nil, fmt.Errorf("metadata store URL scheme %q unsupported (want postgres, postgresql, or sqlite)", u.Scheme)
	}
}
