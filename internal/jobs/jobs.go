// Package jobs is the phase-5 background-work seam (plan Decision 1): one
// Queue interface with two implementations sharing the same worker code —
// River (durable Postgres jobs, at-least-once with backoff retries and
// unique-by-repo sync jobs) for postgres deployments, and an in-process
// goroutine queue for local/sqlite mode, whose queued work is lost on
// process restart (the documented ADR trade-off; local mode keeps working,
// no silent breakage).
//
// The queue deliberately reuses the payload types its two producers and
// workers already share: events.Delivery (webhook fan-out, delivered via
// events.Deliverer) and sync.Job (upstream sync runs, executed by
// sync.(*Worker).Run).
package jobs

import (
	"context"
	"fmt"

	"github.com/coder/open-storage/internal/events"
	"github.com/coder/open-storage/internal/sync"
)

// assertf is a fail-fast invariant check (crash-and-burn style). Never used
// on untrusted input — only on programming errors.
func assertf(cond bool, format string, args ...any) {
	if !cond {
		panic(fmt.Sprintf("jobs assertion failed: "+format, args...))
	}
}

// MaxDeliveryAttempts bounds webhook delivery retries on both backends
// (docs/reference/webhooks.md: capped exponential backoff, then discard).
const MaxDeliveryAttempts = 10

// Queue schedules background work. Enqueue methods are safe for concurrent
// use; Close stops accepting work and, for the local queue, waits for
// in-flight jobs.
type Queue interface {
	// EnqueueWebhookDelivery schedules one signed webhook POST. Retries and
	// re-signing per attempt are the worker's job (events.Deliverer).
	EnqueueWebhookDelivery(ctx context.Context, d events.Delivery) error
	// EnqueueSync schedules one upstream sync run for job.RepoID. Runs for
	// the same repo are deduplicated while queued (River unique jobs /
	// in-process coalescing); overlap with an in-flight run is handled by
	// the store's TryBeginSync claim (invariant 11). Deduplication against
	// a run that already claimed can drop the enqueue (issue #79): callers
	// that promise the sync (pull-upstream 202) mark the request first via
	// UpstreamStore.RequestSync, and the in-flight run's finisher schedules
	// the coalesced follow-up run.
	EnqueueSync(ctx context.Context, job sync.Job) error
	// EnqueuePush schedules one outbound push-upstream run (issue #90).
	// Deduplication and the mark-before-enqueue protocol mirror
	// EnqueueSync on the push lane (UpstreamStore.RequestPush).
	EnqueuePush(ctx context.Context, job sync.Job) error
	// EnqueueRepack schedules one background repack of repo's registered
	// packs (issue #1). Jobs for the same repo are deduplicated while
	// active (River unique-by-args jobs / in-process in-flight skip); a
	// fold that loses its install race is not retried eagerly — the next
	// write past the threshold re-triggers.
	EnqueueRepack(ctx context.Context, repo string) error
	Close()
}

// SyncRunner executes one sync run; production wiring passes
// sync.(*Worker).Run. Its error return is for logging only — documented
// outcomes (detach, overlapping run, failed sync with an emitted
// repo.sync.failed event) are fully handled inside the run.
type SyncRunner func(ctx context.Context, job sync.Job) error
