package events

import (
	"context"

	"github.com/coder/open-storage/internal/meta"
)

// MetaWebhookStore adapts the durable meta.EventingStore webhook rows to
// this package's WebhookStore contract (type translation only — meta's
// sentinel errors, notably meta.ErrNotFound, already match what consumers
// here and in the CRUD handlers expect, so errors pass through untouched).
type MetaWebhookStore struct{ Store meta.EventingStore }

var _ WebhookStore = MetaWebhookStore{}

func fromMetaWebhook(w meta.Webhook) Webhook {
	return Webhook{ID: w.ID, OrgID: w.OrgID, URL: w.URL, Secret: w.Secret, CreatedAt: w.CreatedAt}
}

func (s MetaWebhookStore) CreateWebhook(ctx context.Context, org, url, secret string) (Webhook, error) {
	w, err := s.Store.CreateWebhook(ctx, org, url, secret)
	if err != nil {
		return Webhook{}, err
	}
	return fromMetaWebhook(w), nil
}

func (s MetaWebhookStore) ListWebhooks(ctx context.Context, org string) ([]Webhook, error) {
	rows, err := s.Store.ListWebhooks(ctx, org)
	if err != nil {
		return nil, err
	}
	out := make([]Webhook, 0, len(rows))
	for _, w := range rows {
		out = append(out, fromMetaWebhook(w))
	}
	return out, nil
}

func (s MetaWebhookStore) GetWebhook(ctx context.Context, id string) (Webhook, error) {
	w, err := s.Store.GetWebhook(ctx, id)
	if err != nil {
		return Webhook{}, err
	}
	return fromMetaWebhook(w), nil
}

func (s MetaWebhookStore) DeleteWebhook(ctx context.Context, org, id string) error {
	return s.Store.DeleteWebhook(ctx, org, id)
}
