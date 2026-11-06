# ADR-44: Outbound push forwarding rides a separate push-claim lane

> Decision record for issue #90 (upstream sync was pull-only). Context:
> upstream's GitHub App mode forwards every `git push` to the provider
> implicitly; this backend adds the outbound direction as an explicit
> `POST /repos/push-upstream` trigger plus an opt-in per-repo
> `base_repo.forward_pushes` flag — the trigger-semantics difference is a
> recorded [wire-compatibility deviation](../explanation/wire-compatibility.md#known-deviations-from-the-vendored-spec).
>
> **Amended by #354:** outbound runs now emit `repo.push_upstream.{started,succeeded,failed}`
> instead of `repo.sync.*` with `direction: "push"`, so the `repo.sync.*` bodies match
> upstream's schemas. Mentions of `repo.sync.failed` for outbound runs below predate that
> change.
>
> **Amended by [#451](https://github.com/coder/openstorage/pull/451) and
> [#474](https://github.com/coder/openstorage/pull/474):** local-mode boot recovery and
> `PendingPushRepoIDs` no longer exist. SQLite and S3WAL serves recover through the local
> schedule's sweep, which lists expired claims as due work (`ListLocalWork`, ADR-50,
> ADR-51), and the steal still inherits the dead claim's force mandate. The boot-recovery
> text below predates that change.

An outbound run is [ADR-30](030-upstream-sync-mirror-fetch-loopback-push-through-our-own-front-door.md)'s
mirror image: mirror-fetch the LOCAL refs through this server's own
smart-HTTP loopback (reads go through the front door too — hydration and
auth are reused by construction), then a real `git push` of `refs/heads/*` +
`refs/tags/*` to the linked upstream with the stored envelope-encrypted
credential via `GIT_ASKPASS`. Non-force refspecs by default: a
non-fast-forward is refused **per ref by the upstream itself**, parsed from
`git push --porcelain` output, and reported in the `repo.sync.failed`
event's `rejected_refs`; `force: true` opts into `+refs`. Ref deletions are
not propagated in either direction (the inbound mirror deliberately avoids
`--mirror` for the same reason).

**Separate claim lane.** The outbound run leases `push_syncing` /
`push_claim_holder` / `push_claim_expires_at` — parallel columns to the
inbound claim, same steal-on-expiry semantics (issue #86), no legacy
adoption (no pre-#90 binary ever holds a push claim). The lanes are
deliberately NOT mutually exclusive: the inbound claim protects **local
refs** (the loopback force-push must not interleave), the outbound claim
protects **upstream refs** (at most one active forward per repo). Sharing
one claim would couple the lanes' liveness — a stuck pull starving all
forwarding — while protecting nothing the non-force outbound push does not
already protect: concurrent pull‖push interleavings converge because a push
of a stale snapshot is refused as a non-fast-forward by the upstream, and
the next trigger republishes current state. A run re-checks the row between
the local fetch and the upstream push, and the push itself runs under a
detach/steal watchdog that kills the subprocess mid-transfer — a
`DELETE /repos/base` that lands while a slow upstream is still receiving
refs cancels the transfer within one poll interval instead of after it.

**Force rides durable state, not the queue job.** `push_pending` is a
three-level marker (none/push/force) coalescing upward via `RequestPush`;
`BeginPush` consumes it whole and returns the force bit. Queue
deduplication is keyed by repo only, so a forced enqueue can coalesce into
a queued plain job — the marker is the only place force survives that. The
claim then PERSISTS its consumed mandate (`push_claim_force`) until
`FinishPush` clears it: a holder that crashes mid-run still owes the
accepted forced request, so stealing an expired claim inherits its force
(OR-ed with the thief's own consumed level) instead of silently downgrading
the rescue. Local-mode boot recovery closes the same crash window from the
other side: `PendingPushRepoIDs` lists orphaned in-flight claims (always
dead in a single-process deployment) alongside marked rows, and recovery
re-marks before re-enqueueing so the deferred retry can steal the dead
lease once it expires; the pg deployment recovers through River's durable
job rescue, whose default stuck-job window comfortably exceeds the claim
lease.

**forward_on_push reroutes the push→pull poke.** Without forwarding, a won
local push schedules an *inbound* mirror — which force-resets local refs to
upstream state ("upstream wins"). With `forward_pushes` enabled that order
would be a data-loss race: pull-first rewinds the local push before the
forward's snapshot sees it. So the poke schedules the FORWARD, and the
worker schedules the mirror-back pull strictly after a successful forward —
upstream's own documented model ("pushes are proxied to the provider and
changes mirrored back"). Rerouting alone cannot serialize a pull that was
ALREADY in flight when the local push won its CAS, so the conflict rule is
structural: on forward_on_push repos every inbound loopback push is
fast-forward-only (LOCAL wins) — a stale snapshot's refused refs are the
converged outcome, genuine upstream additions still land, and upstream
rewinds surface on the next forward as its documented FailRejected for a
human force decision. Residual: a FORCE-accepted local rewind to an
ancestor of a concurrently in-flight pull's snapshot is itself
fast-forwardable and can still be undone by that pull.
The mirror-back is marked durable (`RequestSync`)
before it is enqueued: a request that coalesces with an in-flight pull run
survives to that run's completion gate instead of being swallowed by queue
deduplication, so the pull cannot strand a pre-forward snapshot locally.
Every successful outbound run schedules it — explicit `push-upstream`
included — because the same pull‖push interleaving exists there: a pull
that fetched before the forward would otherwise force-write its
pre-forward snapshot back with nothing left to converge the divergence.
A sync run's own loopback pushes carry the sync
claim in the hook environment and poke nothing, which terminates the
forward↔pull cycle. Full-mirror forwarding (not just the pushed ref) is
deliberate: converging to local state is the contract, and per-ref pending
queues would be unbounded state for no correctness gain. A policed or
write-only caller cannot trigger either direction: the explicit endpoint
fails closed (ref-policy ops like pull-upstream, plus a required git:read
— the run exports every local head/tag, so a write-only token must not
exfiltrate content it cannot clone to an upstream it can read), and the
automatic poke applies the same denial to the pushing token (serve-path
claims; hook-path env, including the scope list) — the denied write stays
local until the next authorized trigger converges the mirror. The
`forward_pushes` decision itself is made at repo creation by a
`repo:write` holder.

The reroute and the local-wins rule are replica-local code, so they are
only as strong as the fleet is uniform: a pre-#90 replica that accepts a
push on a `forward_pushes` repo still runs the legacy poke — an inbound
force-mirror ("upstream wins") that an old worker executes as a force
rewind of the accepted write (a new worker's local-wins preserves it, but
no forward is scheduled either way). The claim-lane columns need no
adoption story (no old binary ever holds a push claim), but the poke
cannot be gated in the database, so the prerequisite is
documentation-enforced like the project's other mixed-fleet upgrade
orderings: roll every replica to a #90-capable binary before creating
repositories with `forward_pushes: true`. Repositories without the flag
are unaffected — old and new replicas agree on their pull-only behavior.

**Egress is guarded at dial time.** `upstream_host` is caller-linked data,
and outbound pushes are a WRITE primitive into whatever network the server
can reach — strictly stronger than the pull lane's read/credential-echo
exposure — so every http(s) forward dials through a short-lived loopback
vetting proxy applying the webhook TargetGuard policy (issue #53): deny
loopback/private/link-local/ULA/metadata/special-purpose destinations,
re-resolving and vetting at connect time (no DNS-rebinding TOCTOU).
`OPENSTORAGE_SYNC_PUSH_ALLOW_PRIVATE` relaxes only the private receiver
classes for internal-forge deployments; a denied destination fails the run
permanently as configuration. The pull lane's egress predates this ADR;
[ADR-45](045-sync-egress-policy-is-unified-behind-one-dial-time-guard.md)
extends the same dial-time guard to it behind the unified
`OPENSTORAGE_SYNC_ALLOW_PRIVATE` policy (this flag became the deprecated
push-only spelling).

**Failure semantics.** The forward itself is atomic (`git push --atomic`):
a batch with one refused ref leaves the upstream entirely untouched, so a
failed run never strands a partially-updated upstream that no mirror-back
would converge (failed runs schedule none). A local push never fails
because the upstream is down — forwarding is asynchronous. Transient
failures (upstream
unreachable) re-mark the durable pending level and retry with exponential
backoff (1m…16m, 5 attempts per trigger; River schedules delayed
successors transactionally, local mode arms single-flight timers, and the
marker survives restarts via boot recovery). Permanent failures (auth,
config, rejected refs) emit `repo.sync.failed` and stop — hammering a 401
is abuse-shaped, and a rejected non-fast-forward needs a human decision.

**Amendment: PostgreSQL records the intent with the ref.** The poke runs
after the ref CAS commits, and River's job rescue covers only jobs that
exist, so a hook or serve process that died between the CAS and
`RequestPush`, or a failed `RequestPush` or enqueue, lost the forward
until the next trigger. On PostgreSQL the CAS transaction now writes the
intent itself: for a write that the poke would forward, it inserts the
River push job and raises `push_pending` before COMMIT (metadata and refs
share one database). The poke then skips that forward: marking it again
after a run had claimed the new job would schedule a second run. The poke
still forwards a win that `IdempotentCAS` recovered, which can be another
caller's identical write. The hook's job also exists before receive-pack
publishes the cache ref, so a River push run waits for the repository's
publication on its own node (holds are per node, and its loopback fetch
reads that node's cache) after its claim, as local runs do. A failed wait
restores the request (`RestorePush`), and the job's completion gate
snoozes it. The gate also never completes the job while a push claim is
held: a run whose finish or restore write failed leaves its claim holding
the request it consumed, so the job waits for the lease, and its re-run
steals the claim with its force level. Only a job whose repository was
deleted completes over a held claim. A hook that cannot set up its eventing
on PostgreSQL refuses the ref update instead of accepting it without the
intent (ADR-33's amendment).

**Egress.** The push targets exactly the `CloneURL` the inbound fetch
already dials — a host validated at link time (`validUpstreamHost`,
provider defaults) — under the same hermetic git environment and
`GIT_ALLOW_PROTOCOL` pin (default `http:https`). No new egress class is
introduced; like the inbound fetch, this is a *git* egress governed by the
upstream-host validation at the API boundary, not by the webhook
deliverer's dial-time SSRF guard (which continues to govern webhook HTTP).

**Testing decision (vercel-labs/emulate).** Evaluated and NOT adopted for
this slice: emulate covers GitHub's REST API but not the git wire protocol
(`info/refs`/`receive-pack`), which is the only surface these runs touch —
the hermetic bare-repo/CGI `git-http-backend` harness covers every
acceptance case (clean push, non-fast-forward rejection, force, missing and
invalid credentials) without a Node dependency in `go test ./...`. No test
reaches api.github.com. Revisit if a GitHub App mode (installation-token
minting) or a `/webhooks/github` receiver lands — those are REST surfaces
emulate does cover.
