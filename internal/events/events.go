// Package events implements the docs/06 outbound-webhook surface: HMAC
// delivery signing, the per-attempt delivery worker (re-signing on every
// retry), the storage contract for registered webhooks, and the
// ref.RefStore decorator that turns won non-ephemeral CAS ref flips into
// `push` events (docs/01 invariant 7).
//
// The package deliberately owns only the eventing seam: durable queueing
// (internal/jobs) and webhook persistence (internal/meta) plug in through
// the narrow interfaces declared here; wiring them together is the serve
// command's job.
package events

import (
	"context"
	"fmt"
	"time"
)

// assertf is a fail-fast invariant check (crash-and-burn style). Never used
// on untrusted input (webhook bodies, receiver responses) — only on
// internal invariants.
func assertf(cond bool, format string, args ...any) {
	if !cond {
		panic(fmt.Sprintf("events assertion failed: "+format, args...))
	}
}

// Delivery headers and event names (docs/06 "Delivery headers").
const (
	// HeaderEvent carries the event type on every delivery.
	HeaderEvent = "X-Pierre-Event"
	// HeaderSignature carries the `t=<unix>,sha256=<hex>` HMAC signature.
	HeaderSignature = "X-Pierre-Signature"
	// UserAgent is the fixed delivery User-Agent.
	UserAgent = "Pierre-Webhook/1.0"

	// EventPush fires on any persistent-namespace ref update (API commits,
	// merges, branch/tag/notes writes, git push).
	EventPush = "push"
	// Sync lifecycle events (emitted by the upstream-sync worker).
	EventSyncStarted   = "repo.sync.started"
	EventSyncSucceeded = "repo.sync.succeeded"
	EventSyncFailed    = "repo.sync.failed"
)

// Webhook is one registered delivery endpoint. Secret is the HMAC signing
// key, stored plaintext because every delivery signs with it (ADR: at-rest
// secret encryption is out of scope for phase 5). Never log Secret.
type Webhook struct {
	ID        string // server-generated "wh_<hex>"
	OrgID     int64
	URL       string
	Secret    string
	CreatedAt time.Time // UTC
}

// WebhookLookup resolves one webhook by ID at delivery time, so every
// attempt sees the current URL/secret and deliveries for deleted webhooks
// are dropped. Implementations return meta.ErrNotFound (possibly wrapped)
// when the webhook no longer exists.
type WebhookLookup interface {
	GetWebhook(ctx context.Context, id string) (Webhook, error)
}

// WebhookLister returns the org's registered webhooks (fan-out targets for
// one event). Org-scoped by org name, following the meta.Store convention.
type WebhookLister interface {
	ListWebhooks(ctx context.Context, org string) ([]Webhook, error)
}

// WebhookStore is the full persistence contract behind the webhook CRUD
// surface (internal/api/handlers_webhooks.go) and the delivery/fan-out
// paths. internal/meta implements it on both backends; org-scoped methods
// key by org name and return meta.ErrNotFound for unknown orgs/IDs,
// mirroring the frozen meta.Store conventions.
type WebhookStore interface {
	// CreateWebhook inserts a webhook with a server-generated ID.
	CreateWebhook(ctx context.Context, org, url, secret string) (Webhook, error)
	// ListWebhooks returns the org's webhooks in registration order.
	ListWebhooks(ctx context.Context, org string) ([]Webhook, error)
	// GetWebhook returns the webhook with the given ID regardless of org
	// (delivery jobs carry only the ID); meta.ErrNotFound when absent.
	GetWebhook(ctx context.Context, id string) (Webhook, error)
	// DeleteWebhook removes the org's webhook; meta.ErrNotFound when the
	// org is unknown or the ID does not belong to it.
	DeleteWebhook(ctx context.Context, org, id string) error
}
