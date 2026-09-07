# ADR-46: s3wal — refs and metadata ride one conditional-CAS object per repo group

> Decision record for issue #133 (experimental zero-infrastructure serve
> mode) and its phased implementation
> ([design plan](../plans/s3wal-plan.md), PRs #142/#156/#159 and the
> Phase 3 serve wiring). Context: local mode (ADR-13) already removed
> Postgres, but its SQLite file and `file://` blob dir still make the
> node's disk a durable authority. Issue #133 asks whether an
> S3-compatible bucket can be the **sole** authority — process dies, disk
> burns, service reconstructs from the bucket alone.

**One manifest object per base repo group is the ref authority; one meta
object is the metadata authority.** The `s3wal://bucket?endpoint=…` URL
scheme (dispatched by `ref.Open` and `meta.Open` — ADR-12 factory
discipline; the same URL drives both stores) stores the ENTIRE state of a
base repository group `{base, +ephemeral, +import}` — refs, pack registry,
quota ledger, reservations, delete locks, LFS registry, sync-claim fence —
in one JSON object `_wal/repo/<baseRepoID>`, advanced atomically with
`PutObject + If-Match` (create: `If-None-Match: *`). Every multi-entity
transaction the SQL backends run is a single-object compare-and-swap here.
All meta tables live in one further object, `_wal/meta.json`, with the
same CAS discipline and a process-local cache revalidated through
`If-None-Match` 304s. Packs and LFS bytes stay on the unchanged gocloud
path (`packs/`, `lfs/`); the raw AWS SDK client is used **only** for the
`_wal/` objects, because gocloud's portable writer cannot express
`If-Match` writes trustworthily.

**ETag-ABA is designed out, ambiguity is recovered per method.** Every CAS
write carries a strictly monotonic `seq` and a fresh 128-bit writer nonce,
so identical logical content still produces distinct bytes and therefore
distinct ETags. A response lost after a conditional PUT (timeout, 5xx) is
recovered by re-reading and applying a per-method predicate — writer-nonce
match, semantic effect, or membership of the operation's ID in a bounded
`ops` ring — and non-idempotent mutations whose predicate is inconclusive
**fail closed** and leave TTL machinery to collect the leak. A store
serving an older `seq` than the process has observed is a stale read and
crashes the process.

**Clock deviation (the plan's §2.4): a manifest logical clock plus
fail-closed skew guards replace the DB clock.** PG/SQLite stamp every
lease and grace window with the database clock so node skew cannot corrupt
expiry. s3wal has no server clock, so: (1) every CAS writes
`clk = max(localNow, clk_prev + 1ms)` — monotone across restarts and
backward local jumps — and all stamps use it; (2) verdicts that permit
destruction or steal fail closed (demote to report-only, log, never expire
early) when the manifest clock runs ahead of local time or the wall clock
drifts from the process's monotonic clock by more than
`min(15 min, ttl/4)`; (3) destructive verdicts additionally cross-check
the S3 HTTP `Date` response header — the only arm that catches a clock
that was wrong at boot — and a store that supplies no usable `Date`
witness is refused at startup. Over-retention and false-deny are the only
reachable failure directions. This is a bounded, documented deviation from
the DB-clock invariant, valid only under the single-node decree below.

**Conditional-store requirements are probed, not assumed.** Serving
requires: conditional create (`If-None-Match: *` → 412 on existing),
conditional advance (`If-Match` → 412 on stale), correct 412/409
discipline under races, byte-ordered `ListObjectsV2` pagination, and a
usable `Date` header. A five-step boot probe (`_wal/probe`) verifies the
create/duplicate/advance/stale sequence and **refuses to serve** any store
that fails — header-*ignoring* stores (accepting the write unconditionally)
are the silent-corruption case the probe exists to catch. Qualified
stores: AWS S3 and SeaweedFS (the tested self-hosted reference, pinned in
CI); MinIO was dropped as the reference store (the prior CI pin predates
conditional-PUT support, and modern releases still ignore `If-Match` on
DELETE).

**No conditional DELETE, anywhere (§2.7).** The protocol never guards a
DELETE with a precondition: AWS honors `If-Match` DELETE only on some
bucket classes and MinIO silently ignores it — a guard that sometimes does
not guard is worse than none. Deletion safety comes from ordering
(tombstone-before-delete), short-TTL delete locks recorded in the
manifest, and the sweeper's quarantine-first handshake. Manifest purge
tombstones (`purged_at`) are terminal and never deleted — a purged
repository's name stays burned in the bucket exactly like the SQL modes'
tombstone rows.

**Single-node decree, detected — not enforced — by the instance lease
(§2.11).** s3wal serving is intentionally single-node (the ADR-13 analog).
Correctness never depends on that: every state change is CAS-serialized
regardless, and the multi-node DST lane exercises the shared-store
topology. What a second writer *does* degrade is the §2.4 clock guards
(cross-machine skew) and cache behavior. The `_wal/instance-lease`
CAS-heartbeat (30 s cadence, 2 min TTL) turns the accidental dual-serve
topology into a **detected** condition: serve refuses to start over a
fresh foreign lease, steals expired ones, and fail-stops (crash, not limp)
on heartbeat loss. It is explicitly **best-effort**: freshness compares
local clocks against a stamp another machine wrote, so pathological skew
delays detection — it never weakens the CAS serialization that correctness
actually rests on. `OPENSTORAGE_S3WAL_LEASE_TTL=0` disables it (test
suites that deliberately run several processes against one bucket), with a
loud warning.

**Serve-mode consequences.** The s3wal scheme pairs only with an `s3://`
blob URL (same bucket expected: `packs/`, `lfs/`, and `_wal/` prefixes
coexist) — a `file://` blob store would silently reintroduce a local-disk
authority, so `resolveServeConfig` refuses it. Jobs ride the local
in-process queue and tickers (River requires Postgres). The sweeper and
purge paths carry fail-fast asserts that no disposal ever touches a
`_wal/` key. The meta backend's sync-claim writes flow through to the ref
manifest (`meta.SyncClaimSink`) so a fenced `CASRef` checks the claim
holder in the very object it CASes.

**Non-goals** (per issue #133): multi-tenant scale; hot-repo write
concurrency beyond the measured envelope (one CAS point per repo group);
multi-replica s3wal serving; River/queue emulation over S3; replacing the
PG or SQLite defaults; batch multi-ref CAS (git receive hooks still CAS
one ref at a time, so a losing ref in a multi-ref push can leave earlier
refs committed — identical to today's per-ref hook semantics). Manifest
size is a documented boundary: soft warning at 4 MiB, new registrations
refused at 16 MiB.
