package main

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/coder/openstorage/internal/events"
	"github.com/coder/openstorage/internal/jobs"
	"github.com/coder/openstorage/internal/ref"
	upsync "github.com/coder/openstorage/internal/sync"
)

// forwardIntentStore records a forward_on_push repository's forward intent
// inside each PostgreSQL ref CAS (ref.WithCASTxHook), so no accepted write
// leaves a window in which a crash or a lost connection drops the intent.
// Metadata and refs share the one PostgreSQL database. It sits under
// events.RefStore and covers the writes that the decorator's sync poke
// (enqueueSyncOrForward) forwards: refs outside the ephemeral namespace,
// written by a caller that may forward. The poke still runs after the CAS;
// it is a duplicate for these writes, and it covers a win that
// ref.IdempotentCAS recovered from another caller's identical write.
type forwardIntentStore struct {
	ref.RefStore
	queue         *jobs.River
	org           string
	forwardDenied func(ctx context.Context) bool
}

func newForwardIntentStore(inner ref.RefStore, queue *jobs.River, org string, forwardDenied func(ctx context.Context) bool) forwardIntentStore {
	assertf(inner != nil && queue != nil && org != "" && forwardDenied != nil, "newForwardIntentStore: missing deps")
	return forwardIntentStore{RefStore: inner, queue: queue, org: org, forwardDenied: forwardDenied}
}

// CASRef and CASRefGen are explicit overrides: interface embedding would
// forward past the intent.
func (s forwardIntentStore) CASRef(ctx context.Context, repo, name, old, new string) (bool, error) {
	return s.RefStore.CASRef(s.withIntent(ctx, repo, name), repo, name, old, new)
}

func (s forwardIntentStore) CASRefGen(ctx context.Context, repo, name, old, new string) (bool, int64, error) {
	return s.RefStore.CASRefGen(s.withIntent(ctx, repo, name), repo, name, old, new)
}

func (s forwardIntentStore) withIntent(ctx context.Context, repo, name string) context.Context {
	if events.EphemeralUpdate(repo, name) || s.forwardDenied(ctx) {
		return ctx
	}
	job := upsync.Job{Org: s.org, RepoID: events.BaseRepoID(repo)}
	return ref.WithCASTxHook(ctx, func(ctx context.Context, tx pgx.Tx) error {
		return s.queue.RecordForwardTx(ctx, tx, job)
	})
}
