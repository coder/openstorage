# ADR-27: Jobs are a queue seam — River on Postgres, in-process goroutines in local mode

> Extracted verbatim from [`14-phase5-implementation-notes.md`](../archive/phases/14-phase5-implementation-notes.md)
> (“Phase-5 Implementation Notes & ADRs (eventing, upstream sync, credentials, fork)”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

Webhook deliveries and upstream syncs are background work. Decision: **the queue is a
narrow seam, not a fixed implementation.** The consumer-side contracts
(`events.Delivery` + enqueue funcs, `sync.Queue`/`sync.Job`) are owned by the eventing and
sync packages; the Postgres deployment plugs in a durable River-backed jobs package, while
local mode ships an in-process implementation (`sync.InProcessQueue`, and
`events.Deliverer.Deliver` looping attempts with backoff on a goroutine).
`events.Deliverer` is deliberately shaped for both backends: River workers call `Attempt`
once per job attempt (River owns backoff/attempt accounting and durability), the local
queue calls `Deliver` which owns the retry loop itself. Every attempt re-signs with the
current time, so retries never carry stale signatures.

Accepted trade-off: **local-mode queued work is lost on process restart.** Every queued
action is re-derivable — a lost sync re-enqueues on the next push or `pull-upstream`, and
webhook delivery is best-effort with bounded retries (docs/06) — so local mode trades
durability for zero infrastructure. `InProcessQueue` also coalesces per repo (a job queued
while one is still waiting is dropped; the waiting run observes the same upstream state),
which is exactly the semantics invariant 11 needs.
