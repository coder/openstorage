package main

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/coder/openstorage/internal/events"
	"github.com/coder/openstorage/internal/jobs"
	"github.com/coder/openstorage/internal/ref"
	upsync "github.com/coder/openstorage/internal/sync"
)

// newEventsStore is events.NewRefStore with the signature of
// newPGEventsStore's first two parameters.
func newEventsStore(inner ref.RefStore, opts events.RefStoreOptions) ref.RefStore {
	return events.NewRefStore(inner, opts)
}

// newPGEventsStore is events.NewRefStore for PostgreSQL, where metadata
// and refs share one database: each ref CAS records a forward_on_push
// repository's forward intent in its own transaction, so no crash or lost
// connection after the CAS can drop it. The intent covers the writes that
// the decorator's sync poke (enqueueSyncOrForward) forwards: refs outside
// the ephemeral namespace, by a caller that may forward. The poke skips a
// forward that the CAS already recorded, so it neither re-marks a level
// that a run has just consumed nor schedules a second run. It still runs
// for a win that ref.IdempotentCAS recovered, which may be another
// caller's identical write.
func newPGEventsStore(inner ref.RefStore, opts events.RefStoreOptions, queue *jobs.River, forwardDenied func(ctx context.Context) bool) ref.RefStore {
	assertf(queue != nil && forwardDenied != nil, "newPGEventsStore: missing deps")
	if poke := opts.EnqueueSync; poke != nil {
		opts.EnqueueSync = func(ctx context.Context, repoID string) error {
			if rec, ok := ctx.Value(forwardRecordKey{}).(*forwardRecord); ok && rec.recorded {
				return nil
			}
			return poke(ctx, repoID)
		}
	}
	return forwardIntentStore{RefStore: events.NewRefStore(inner, opts), queue: queue, org: opts.Org, forwardDenied: forwardDenied}
}

// forwardRecord tells the sync poke of one CAS that the CAS's committed
// transaction recorded the forward intent.
type forwardRecord struct{ recorded bool }

type forwardRecordKey struct{}

// forwardIntentStore wraps the events decorator and gives each CAS its
// intent hook and record.
type forwardIntentStore struct {
	ref.RefStore
	queue         *jobs.River
	org           string
	forwardDenied func(ctx context.Context) bool
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
	rec := &forwardRecord{}
	var recorded bool
	return ref.WithCASTxHook(context.WithValue(ctx, forwardRecordKey{}, rec), ref.CASTxHook{
		InTx: func(ctx context.Context, tx pgx.Tx) error {
			var err error
			recorded, err = s.queue.RecordForwardTx(ctx, tx, job)
			return err
		},
		Committed: func() { rec.recorded = recorded },
	})
}
