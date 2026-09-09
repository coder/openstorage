package events

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/coder/openstorage/internal/meta"
)

// ErrWebhookGone marks a delivery whose webhook was deleted between
// enqueue and attempt: terminal, never retried, dropped silently (the
// registration is gone, so nobody is listening).
var ErrWebhookGone = errors.New("webhook no longer exists")

// Delivery is one queued webhook delivery: the payload is the fully
// rendered JSON body, frozen at enqueue time; only the signature is
// recomputed per attempt. Queue implementations (internal/jobs) persist
// exactly these three fields.
type Delivery struct {
	WebhookID string
	Event     string // EventPush, EventSync*
	Payload   []byte // JSON body as sent on the wire
}

// Deliverer is the webhook delivery worker (docs/reference/webhooks.md). Both queue backends
// share it: River workers call Attempt once per job attempt (River owns
// backoff/attempt accounting), the in-process local-mode queue calls
// Deliver which loops attempts with backoff itself. Every attempt reloads
// the webhook and signs with the current time, so retries always carry a
// fresh signature (receivers reject signatures older than ~300s).
type Deliverer struct {
	Webhooks WebhookLookup // required

	// Client posts deliveries; defaults to the TargetGuard-enforcing
	// client (issue #53): private-range targets denied at dial time,
	// redirects refused. Only tests override this — production wiring
	// must keep the guard and steer it through AllowPrivate.
	Client *http.Client
	// AllowPrivate selects the guard's private-range escape hatch
	// (OPENSTORAGE_WEBHOOK_ALLOW_PRIVATE) when Client is nil. Redirects
	// are refused either way.
	AllowPrivate bool
	// Now supplies the signing timestamp; defaults to time.Now.
	Now func() time.Time
	// MaxAttempts bounds Deliver's retry loop (default 10, then discard).
	MaxAttempts int
	// Backoff returns the sleep before retrying after the given 1-based
	// failed attempt; defaults to 1s<<(attempt-1) capped at 64s.
	Backoff func(attempt int) time.Duration
	// Logger records drops and retries; defaults to slog.Default().
	Logger *slog.Logger
}

// The default clients are guard-enforcing so a Deliverer constructed
// without an explicit Client can only fail CLOSED (issue #53); both bound
// a whole attempt at 30s — a receiver that never responds must not wedge
// a delivery worker.
var (
	defaultClient        = TargetGuard{}.NewClient()
	defaultPrivateClient = TargetGuard{AllowPrivate: true}.NewClient()
)

func defaultBackoff(attempt int) time.Duration {
	assertf(attempt >= 1, "defaultBackoff: non-positive attempt %d", attempt)
	if attempt > 7 { // 1s<<6 = 64s cap
		return 64 * time.Second
	}
	return time.Second << (attempt - 1)
}

func (d *Deliverer) client() *http.Client {
	if d.Client != nil {
		return d.Client
	}
	if d.AllowPrivate {
		return defaultPrivateClient
	}
	return defaultClient
}

func (d *Deliverer) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

func (d *Deliverer) logger() *slog.Logger {
	if d.Logger != nil {
		return d.Logger
	}
	return slog.Default()
}

func (d *Deliverer) maxAttempts() int {
	if d.MaxAttempts > 0 {
		return d.MaxAttempts
	}
	return 10
}

func (d *Deliverer) backoff(attempt int) time.Duration {
	if d.Backoff != nil {
		return d.Backoff(attempt)
	}
	return defaultBackoff(attempt)
}

// validate checks the deliverer's own configuration and the delivery's
// internal shape — both produced by our code, never by receivers.
func (d *Deliverer) validate(del Delivery) {
	assertf(d.Webhooks != nil, "Deliverer.Webhooks is required")
	assertf(del.WebhookID != "", "Delivery.WebhookID is empty")
	assertf(del.Event != "", "Delivery.Event is empty")
	assertf(len(del.Payload) > 0, "Delivery.Payload is empty")
}

// Attempt performs exactly one delivery attempt: reload the webhook, sign
// the payload with the current time, POST it with the docs/reference/webhooks.md headers.
// A non-2xx status or transport error returns an error (the caller
// retries); ErrWebhookGone (wrapped) is terminal.
func (d *Deliverer) Attempt(ctx context.Context, del Delivery) error {
	d.validate(del)
	wh, err := d.Webhooks.GetWebhook(ctx, del.WebhookID)
	if errors.Is(err, meta.ErrNotFound) {
		return fmt.Errorf("webhook %s: %w", del.WebhookID, ErrWebhookGone)
	}
	if err != nil {
		return fmt.Errorf("load webhook %s: %w", del.WebhookID, err)
	}
	assertf(wh.URL != "" && wh.Secret != "", "stored webhook %s has empty url or secret", wh.ID)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, wh.URL, bytes.NewReader(del.Payload))
	if err != nil {
		return fmt.Errorf("build delivery request for %s: %w", del.WebhookID, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", UserAgent)
	req.Header.Set(HeaderEvent, del.Event)
	req.Header.Set(HeaderSignature, Sign(wh.Secret, d.now().Unix(), del.Payload))

	resp, err := d.client().Do(req)
	if err != nil {
		return fmt.Errorf("deliver to webhook %s: %w", del.WebhookID, err)
	}
	// Receiver bodies are untrusted and irrelevant; drain a bounded amount
	// to release transport resources, then close. Guarded clients disable
	// keep-alives so every attempt gets a fresh authoritative DNS check.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	_ = resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook %s: receiver returned status %d", del.WebhookID, resp.StatusCode)
	}
	return nil
}

// Deliver runs Attempt with backoff until success, a terminal drop
// (webhook deleted), MaxAttempts exhaustion (delivery discarded, per the
// docs/reference/webhooks.md at-least-once + bounded-retries guidance), or ctx cancellation.
// Used by the in-process local-mode queue; River jobs use Attempt directly.
func (d *Deliverer) Deliver(ctx context.Context, del Delivery) error {
	d.validate(del)
	max := d.maxAttempts()
	var lastErr error
	for attempt := 1; attempt <= max; attempt++ {
		lastErr = d.Attempt(ctx, del)
		if lastErr == nil {
			return nil
		}
		if errors.Is(lastErr, ErrWebhookGone) {
			d.logger().Info("dropping delivery for deleted webhook",
				"webhook_id", del.WebhookID, "event", del.Event)
			return lastErr
		}
		if attempt == max {
			break
		}
		d.logger().Warn("webhook delivery attempt failed; retrying",
			"webhook_id", del.WebhookID, "event", del.Event,
			"attempt", attempt, "err", lastErr)
		timer := time.NewTimer(d.backoff(attempt))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	d.logger().Error("webhook delivery discarded after max attempts",
		"webhook_id", del.WebhookID, "event", del.Event,
		"attempts", max, "err", lastErr)
	return fmt.Errorf("webhook %s: giving up after %d attempts: %w", del.WebhookID, max, lastErr)
}
