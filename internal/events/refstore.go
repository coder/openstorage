package events

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/coder/code-storage/internal/auth"
	"github.com/coder/code-storage/internal/meta"
	"github.com/coder/code-storage/internal/ref"
)

// RepoSource resolves a live repo row by RepoID for payload construction
// (repository.url, customer_id). meta.Store satisfies it as-is.
type RepoSource interface {
	GetRepoByID(ctx context.Context, org, repoID string) (meta.Repo, error)
}

// RefStoreOptions configures NewRefStore. Org, Repos, Webhooks, and
// EnqueueDelivery are required; the rest default sensibly.
type RefStoreOptions struct {
	// Org is the org name owning every repo this store serves (single-org
	// self-host mode, same value the serve config / hook env carries).
	Org string
	// Repos resolves RepoIDs to repo rows (meta.Store satisfies this).
	Repos RepoSource
	// Webhooks lists the org's registered delivery endpoints.
	Webhooks WebhookLister
	// EnqueueDelivery hands one delivery to the jobs queue. Adapters wrap
	// jobs.Queue.EnqueueWebhookDelivery; failures are logged, never
	// propagated (the ref flip already committed).
	EnqueueDelivery func(ctx context.Context, d Delivery) error
	// EnqueueSync, when non-nil, is invoked once per won non-ephemeral ref
	// update with the base RepoID (docs/explanation/domain-model.md invariant 11: pushes to a repo
	// with an upstream schedule a sync). Implementations decide whether a
	// sync is actually due — upstream row present AND no run currently in
	// progress (which also prevents the sync worker's own loopback pushes
	// from re-triggering themselves) — and enqueue accordingly.
	EnqueueSync func(ctx context.Context, repoID string) error
	// Now supplies pushed_at; defaults to time.Now.
	Now func() time.Time
	// Logger records emission failures; defaults to slog.Default().
	Logger *slog.Logger
}

// RefStore decorates a ref.RefStore so every won CASRef on a persistent
// (non-ephemeral) ref emits one `push` event per registered webhook and
// pokes the sync seam — the single choke point covering both write
// surfaces (API handlers and the git receive-pack update hook), which both
// flow their ref flips through CASRef. Emission happens strictly AFTER the
// CAS commits and never fails the update: enqueue errors are logged and
// swallowed (docs/reference/webhooks.md "emit after the ref update commits").
type RefStore struct {
	ref.RefStore // inner store; every method except CASRef delegates

	org             string
	repos           RepoSource
	webhooks        WebhookLister
	enqueueDelivery func(ctx context.Context, d Delivery) error
	enqueueSync     func(ctx context.Context, repoID string) error
	now             func() time.Time
	logger          *slog.Logger
}

var _ ref.RefStore = (*RefStore)(nil)

// NewRefStore wraps inner with push-event emission per opts. inner is
// additionally wrapped in ref.IdempotentCAS, so an ambiguous CAS outcome
// whose update actually committed is reconciled to a win BELOW this
// decorator — the push event for it is emitted, not silently dropped.
func NewRefStore(inner ref.RefStore, opts RefStoreOptions) *RefStore {
	assertf(inner != nil, "NewRefStore: nil inner store")
	assertf(opts.Org != "", "NewRefStore: empty org")
	assertf(opts.Repos != nil, "NewRefStore: nil RepoSource")
	assertf(opts.Webhooks != nil, "NewRefStore: nil WebhookLister")
	assertf(opts.EnqueueDelivery != nil, "NewRefStore: nil EnqueueDelivery")
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &RefStore{
		RefStore:        ref.IdempotentCAS{RefStore: inner},
		org:             opts.Org,
		repos:           opts.Repos,
		webhooks:        opts.Webhooks,
		enqueueDelivery: opts.EnqueueDelivery,
		enqueueSync:     opts.EnqueueSync,
		now:             now,
		logger:          logger,
	}
}

// CASRef delegates to the inner store and, on a won update outside the
// ephemeral namespace, emits the push event + sync poke. Emission uses a
// cancellation-detached context: the CAS already committed, so a client
// hanging up mid-response must not lose the event. The inner store is
// ref.IdempotentCAS-wrapped (see NewRefStore), so a returned error or lost
// CAS here is already reconciled — skipping emission on it is safe.
func (s *RefStore) CASRef(ctx context.Context, repo, name, old, new string) (bool, error) {
	won, err := s.RefStore.CASRef(ctx, repo, name, old, new)
	if err != nil || !won {
		return won, err
	}
	s.emitPush(context.WithoutCancel(ctx), repo, name, old, new)
	return true, nil
}

// CASRefGen mirrors CASRef exactly (an explicit override: interface
// embedding would silently forward past the push-event emission): a won
// swap emits the push event + sync poke, and the inner store's generation
// attribution passes through untouched.
func (s *RefStore) CASRefGen(ctx context.Context, repo, name, old, new string) (bool, int64, error) {
	won, gen, err := s.RefStore.CASRefGen(ctx, repo, name, old, new)
	if err != nil || !won {
		return won, gen, err
	}
	s.emitPush(context.WithoutCancel(ctx), repo, name, old, new)
	return true, gen, nil
}

// emitPush fans one push event out to the org's webhooks and pokes the
// sync seam. Never returns an error: the ref flip is already durable.
func (s *RefStore) emitPush(ctx context.Context, repoKey, refName, before, after string) {
	if EphemeralUpdate(repoKey, refName) {
		return // invariant 7: ephemeral updates emit nothing
	}
	repoID := BaseRepoID(repoKey)

	hooks, err := s.webhooks.ListWebhooks(ctx, s.org)
	if err != nil {
		s.logger.Error("push event lost: list webhooks", "repo", repoID, "ref", refName, "err", err)
		hooks = nil // still poke the sync seam below
	}
	if len(hooks) > 0 {
		repo, err := s.repos.GetRepoByID(ctx, s.org, repoID)
		if err != nil {
			// Repo row gone (e.g. soft-deleted concurrently): no payload
			// can be built; the update itself already succeeded.
			s.logger.Error("push event lost: resolve repo", "repo", repoID, "ref", refName, "err", err)
		} else {
			payload := NewPushPayload(s.org, repo, refName, before, after, s.now())
			body, err := json.Marshal(payload)
			assertf(err == nil, "marshal push payload: %v", err)
			for _, wh := range hooks {
				d := Delivery{WebhookID: wh.ID, Event: EventPush, Payload: body}
				if err := s.enqueueDelivery(ctx, d); err != nil {
					s.logger.Error("push event lost: enqueue delivery",
						"repo", repoID, "ref", refName, "webhook_id", wh.ID, "err", err)
				}
			}
		}
	}

	if s.enqueueSync != nil {
		if err := s.enqueueSync(ctx, repoID); err != nil {
			s.logger.Error("sync poke lost: enqueue sync", "repo", repoID, "ref", refName, "err", err)
		}
	}
}

// EphemeralUpdate reports whether a CASRef targets the ephemeral namespace
// on either write surface: the git surface keys ephemeral pushes by the
// `<RepoID>+ephemeral` storage key (plain ref names), the API surface uses
// the bare RepoID with `refs/namespaces/ephemeral/`-prefixed ref names.
func EphemeralUpdate(repoKey, refName string) bool {
	assertf(repoKey != "", "EphemeralUpdate: empty repo key")
	assertf(refName != "", "EphemeralUpdate: empty ref name")
	return strings.HasSuffix(repoKey, "+ephemeral") ||
		strings.HasPrefix(refName, auth.EphemeralNamespacePrefix)
}

// BaseRepoID strips the optional `+ephemeral`/`+import` namespace suffix
// from a storage key (same shape githttp.BaseRepo handles for URL path
// elements), yielding the RepoID that keys repo metadata.
func BaseRepoID(repoKey string) string {
	assertf(repoKey != "", "BaseRepoID: empty repo key")
	base, _, _ := strings.Cut(repoKey, "+")
	assertf(base != "", "BaseRepoID: key %q has empty base", repoKey)
	return base
}

// RepositoryRef is the `repository` object embedded in event payloads.
type RepositoryRef struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

// PushPayload is the docs/reference/webhooks.md `push` event body. All seven fields are
// required non-empty — vendor SDKs reject payloads with any empty field.
type PushPayload struct {
	Repository RepositoryRef `json:"repository"`
	Ref        string        `json:"ref"`
	Before     string        `json:"before"` // 40 zeros for ref creation
	After      string        `json:"after"`  // 40 zeros for ref deletion
	CustomerID string        `json:"customer_id"`
	PushedAt   string        `json:"pushed_at"` // RFC 3339 UTC
}

// NewPushPayload builds the push body for one won CAS. before/after come
// straight from CASRef's old/new, which already use ref.ZeroSHA for
// create/delete — exactly the docs/reference/webhooks.md wire convention. customer_id is the
// owning org's name — the same string the JWT `iss` claim carries (docs/reference/webhooks.md
// "org/customer id"; vendor SDKs compare it against iss).
func NewPushPayload(org string, repo meta.Repo, refName, before, after string, pushedAt time.Time) PushPayload {
	assertf(org != "", "NewPushPayload: empty org")
	assertf(repo.RepoID != "", "NewPushPayload: empty RepoID")
	assertf(repo.URL != "", "NewPushPayload: repo %s has empty URL", repo.RepoID)
	assertf(refName != "", "NewPushPayload: empty ref name")
	assertf(ref.ValidSHA(before), "NewPushPayload: malformed before sha %q", before)
	assertf(ref.ValidSHA(after), "NewPushPayload: malformed after sha %q", after)
	assertf(!pushedAt.IsZero(), "NewPushPayload: zero pushed_at")
	return PushPayload{
		Repository: RepositoryRef{ID: repo.RepoID, URL: repo.URL},
		Ref:        refName,
		Before:     before,
		After:      after,
		CustomerID: org,
		PushedAt:   pushedAt.UTC().Format(time.RFC3339),
	}
}
