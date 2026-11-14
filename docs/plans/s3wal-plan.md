# s3wal RefStore backend — design & phased implementation plan (issue #133)

Status: DRAFT for maintainer-desk approval — no implementation until approved.
Branch: `s3wal-refstore` (plan committed as `docs/plans/s3wal-plan.md`). Base: origin/main `e750ecd5`.
Nature: **experiment with explicit kill criteria**, not a rewrite. PG remains the multi-tenant standard; SQLite remains the local default.

## 0. Executive summary

A third `RefStore` backend, `s3wal`: the entire authority state of one base repo group
{base, +ephemeral, +import} lives in **one JSON manifest object** in S3, advanced by
`If-Match` CAS (`If-None-Match: *` for creation). Immutable packs/LFS bytes stay where they
are today. Target: zero-infra deployment — single binary + one bucket, no PG, no
local-disk authority. Single-node by decree (the ADR-13 analog), jobs on the existing
ticker path.

**Core design decision — one object per base group, not manifest+ledger pairs.** The
interface audit (§1) found 9 multi-entity atomic operations (e.g. `RegisterPacks` =
insert packs + consume reservations + credit usage + bump adoption gen + bump repo
generation, all in one tx). Splitting state across a refs-manifest and a ledger object
would reintroduce cross-object atomicity (two CAS points, crash windows between them, a
quota oracle violation on unlucky crashes). Putting the whole group's refs, packs,
retained tombstones, generations, quota ledger, reservations, LFS registry, adoption
claims/gens, purge fence, and sync-claim token into **one object** makes every audited
transaction a **single-object CAS** — the crash-window analysis collapses to "each op
either happened or didn't, plus one ambiguous-response window that `IdempotentCAS`-style
re-read recovery already handles." The cost is one CAS point per repo group (namespace
members contend with each other) and manifest-size growth; both are measured and bounded
(§3, §5-R5/R6).

**Feasibility already verified in this checkout (no new deps needed):**
- Vendored `aws-sdk-go-v2/service/s3 v1.107.1` exposes `IfMatch`/`IfNoneMatch` on
  `PutObjectInput` (`vendor/.../api_op_PutObject.go:497,513`).
- gocloud v0.46.0's portable API has only `IfNotExist` (If-None-Match: *); full If-Match
  is reachable only via the `BeforeWrite` as-hook (`**transfermanager.UploadObjectInput`,
  which has `IfMatch` — `vendor/.../transfermanager/api_op_UploadObject.go:303`). We use
  the **raw `s3.Client` for the CAS core** (clean 412/409 taxonomy, no transfer-manager
  indirection) and keep gocloud for bulk pack/LFS I/O unchanged. See §5-R1.
- Self-hosted store: **SeaweedFS replaces MinIO** as the repo's self-hosted S3
  implementation (user directive 2026-08-25; §5-R7), with the repo-wide swap landing
  only after Phase 1a empirical qualification (qualification-first ordering, §4).
  SeaweedFS implements conditional PUT with real per-object CAS serialization
  (seaweedfs#7154); our MinIO CI pin `minio/minio:RELEASE.2024-01-16T16-07-38Z`
  predates known conditional-write support and must be assumed non-compliant until
  the Phase 1a matrix documents its actual behavior (header-ignoring is the worst,
  silent failure mode), and even modern MinIO ignores `If-Match` on DELETE ⇒ the
  protocol **never relies on conditional DELETE** (§2.7, §5-R2). A mandatory
  boot-time conditional-write probe proves actual behavior on any store either way
  (§2.6). s3wal additionally requires strong read-after-write and list consistency of
  the store (real S3 guarantees both since 2020; documented as a hard requirement for
  compatible stores).

**Kill criteria (measured, §3):** single-writer push p50 > 500 ms or p99 > 2 s on real
S3; sustained hot-repo CAS-advance throughput < 3/s at N=8 pushers or any pusher starved
> 30 s; error-rate cliff or livelock at N=32; conditional-write infidelity detected by
the probe/harness on a supported store. Any of these ⇒ report and close #133 as rejected.

---

## 1. Interface-fit audit

Full method inventory from `internal/ref/ref.go:87-336` plus the job sub-interfaces
(`internal/jobs/{sweep,purge,lfsreclaim,absence}.go`). Fit classes: **M** = single
manifest CAS/read, **L** = bucket prefix listing, **S** = side object, **N** = non-goal.

| Group | Methods | Fit | s3wal mapping |
|---|---|---|---|
| Ref reads | `ListRefs`, `ListRefsWithMeta`, `ListRefsAfter`, `GetRef` | M | GET manifest; `ListRefsAfter` pages the in-memory refs map in byte order (contract preserved; the whole set is already in hand) |
| Ref CAS | `CASRef` | M | read-mutate-CAS; old-SHA mismatch ⇒ `won=false` (never retried); purge fence + sync-claim fence are fields of the same object ⇒ same-atom checks |
| Pack registry | `RegisterPacks`, `ListPacks`, `HasPack`, `ReplacePacks`, `IsRetainedPack`, `ReclaimRetainedPack` | M | packs list + `pack_retained` tombstones + usage counter + reservations all in-manifest; `ReplacePacks` precondition (“all old packs still present”) evaluated on the read snapshot, enforced by CAS |
| Quota ledger | `ReservePackBytes`, `QuotaLedgerState`, `RetirePackAttempts`, | M | reservations = in-manifest map keyed by blob key with `{owner, attempts, gen, created_at}`; admission sum computed over the single object — the per-base serialization PG buys with `lockBaseLedger` is inherent to single-object CAS |
| Reservation janitor | `SweepExpiredPackReservations` | L+M | list `_wal/repo/` prefix, per-group CAS-prune expired entries (ticker sweeper already rotates; bounded per pass) |
| LFS registry | `RegisterLFSObject`, `RegisterAdoptedLFSObject`, `HasLFSObject`, `TouchLFSObject`, `LFSObjectRegisteredAt`, `CountLFSObjects`, `GuardedLFSDelete`, `RecordLFSMismatch`, `LFSMismatchFingerprint`, `ClearLFSMismatch` | M | LFS map keyed by canonical/legacy key with `{bytes, created_at, unreferenced_since}`; counts maintained in-object; `GuardedLFSDelete` uses a **CAS delete-lease** (`delete_locks`, §2.5 W6) so delete-vs-register exclusion is *prevented*, matching the SQL lock guarantee |
| ADR-41 janitor | `ReconcileLegacyLFSObjects` | L+M | iterate groups via prefix list; per-group single-CAS twin-drop with count/bytes refund |
| Adoption | `TryAdoptionClaim`, `RenewAdoptionClaim`, `ReleaseAdoptionClaim`, `BumpLFSAdoptionGen`, `LFSAdoptionGen` | M | claims are reservation-map entries under the `/adopt-claim` suffix keys (exactly the PG spelling); steal-on-expiry compares writer clock to stored `created_at` (§2.4) |
| Generation | `GetGeneration` | M | per-member generation counters in-manifest, bumped by the driver inside every mutating CAS (replaces DB triggers); cheap polling via conditional GET `If-None-Match: <etag>` ⇒ 304 (§2.3) |
| Purge | `FenceRepoPurge`, `IsRepoPurged`, `DropRepoRefs`, `RetireRepoPacks`, `DropRepoLFSObjects`, `CountRepoRows`, `PurgeRepoLedger` | M | fence = terminal flag in the manifest; each purge phase is one CAS; the manifest is **never deleted** (burned-name invariant: the tombstone object persists, ~200 B) |
| LFS reclaim | `LFSReposWithObjects`, `CountRefs`, `ListRepoLFSObjects`, `MarkLFSObjectsUnreferenced`, `ClearLFSObjectsUnreferenced`, `RetireUnreferencedLFSObject` | L/M | repo rotation via prefix list; per-repo ops in-manifest; `RetireUnreferencedLFSObject`’s pinned-generation fence across namespace members is a same-object check (all members’ gens live together) |
| Absence scan | `ListFleetLFSObjects`, `ListFleetPacks` | L+M | prefix listing over `_wal/repo/` + in-memory member paging; correctness argument and cursor-resume procedure in §2.9. Cost: one GET per group per page — the documented scaling boundary |
| Scan state (PR #132, in flight) | `GetJobScanState`, `PutJobScanState`, `DeleteJobScanState` | S | side objects `_wal/scan/<job>/<scope>.json`, CAS-advanced; the in-transaction watermark check SQL performs cannot be replicated cross-object — s3wal’s concrete substitute rule is §2.10. Absence covers are reservation-map entries ⇒ in-manifest; `ListAbsenceCovers` via prefix walk. If #132 lands mid-experiment we rebase and implement the mapping in Phase 1/2; if not, the methods don’t exist to implement — either way no schema conflict |
| Lifecycle | `InitSchema`, `Close` | — | `InitSchema` = bucket reachability + **conditional-write probe** (create/advance/expect-412 on `_wal/probe`; refuse to serve on infidelity) |
| River queue | (jobs) | N | non-goal — `isPGURL` already routes non-PG to `jobs.NewLocal` + tickers (`cmd/codestorage/wire_eventing.go:141,209,241`) |
| Cross-repo wire-compat pagination, webhooks/credentials/orgs/repos | `meta.Store` | S/N | separate store; zero-infra story in §5-Q4 (resolved) |

**Honest misfit summary:** fleet-wide scans become prefix-list + per-group GETs (O(groups)
instead of O(rows) keyset pages) — acceptable at target scale (≤ ~2k repos), stated in
docs as the scaling boundary. River is a non-goal. Quota aggregation is *better* fitted
than feared (single object). The genuinely awkward residents are TTL semantics without a
DB clock (§2.4) and meta (§5-Q4).

## 2. Manifest & CAS protocol spec

### 2.1 Object layout

```
_wal/probe                          # boot-time conditional-write probe scratch
_wal/instance-lease                 # optional dual-serve guard (Phase 3)
_wal/repo/<baseRepoID>              # THE manifest (one per base group) — NO file
                                    # extension: a suffix like ".json" would corrupt
                                    # listing order for prefix-related IDs ("abc-1.json"
                                    # sorts before "abc.json" although abc < abc-1)
_wal/scan/<job>/<scope>             # PR #132 job scan state (scope path-escaped)
packs/..., lfs/...                  # unchanged (gocloud path)
```

RepoIDs are server-generated and URL-safe today; the driver asserts the safe alphabet at
key-build time (defensive) so S3’s lexicographic listing == byte-ordered keyset paging.
The sweeper’s classifying prefixes are `packs/` and `lfs/` — `_wal/` is invisible to it;
Phase 3 adds an explicit assertion + test that sweep never touches `_wal/`.

### 2.2 Manifest schema (v1, sketch)

```jsonc
{
  "v": 1,
  "seq": 412,                  // strictly monotonic, bumped every CAS
  "writer": "9f3a…",           // 128-bit random nonce, fresh per write — with seq,
                               // guarantees distinct bytes per logical version ⇒
                               // distinct ETag (kills ETag-ABA; §5-R4) and identifies
                               // the last writer for ambiguous-PUT recovery (§2.3)
  "ops": ["op-…", "…"],        // bounded ring (K=64) of recent mutation op-IDs so
                               // ambiguous retries of NON-idempotent ops (e.g.
                               // RetirePackAttempts decrements) can test membership
                               // instead of re-applying (§2.3)
  "clk": "2026-08-25T…",       // manifest logical clock: every CAS writes
                               // max(localNow, clk_prev + 1ms) — monotone across
                               // restarts and local clock regressions (§2.4)
  "purged_at": null,           // non-null ⇒ terminal tombstone (fence + stamp)
  "members": {                 // keyed by repo key: base, base+ephemeral, base+import
    "<repoKey>": {
      "gen": 91,               // repo mutation generation (driver-bumped)
      "refs":  { "<name>": {"sha": "…", "created_at": "…"} },
      "packs": [ {"id": "…", "key": "…", "bytes": 123, "created_at": "…"} ],
      "retained": { "<key>": {"bytes": 123, "superseded_at": "…"} }
    }                          // pack bytes persisted (absence scan + ListFleetPacks
  },                           // need them; fresh backend ⇒ no 0-means-unknown rows)
  "usage_bytes": 123456,       // pack_usage counter (group scope)
  "reservations": {            // pack_reservations incl. /adopt-claim entries and
    "<blobKey>": {"owner":"…","bytes":1,"attempts":2,"gen":"…","created_at":"…"}
  },                           // absence covers (absence-lost/<base>/<class>-e<n>)
  "delete_locks": {            // short-TTL delete leases (§2.5 W6)
    "<blobKey>": {"owner":"…","created_at":"…"}
  },
  "lfs": {
    "count": 42,
    "objects": { "<key>": {"bytes":1,"created_at":"…","unreferenced_since":null} },
    "mismatches": { "<key>": "<fingerprint>" },
    "adoption_gen": 7
  },
  "sync_claim": {"holder": "…", "created_at": "…"}   // §2.8
}
```

Size accounting: ~120 B/ref + ~150 B/pack + ~130 B/LFS object. 5k refs + 500 packs +
5k LFS ≈ 1.4 MB. Soft warn at 4 MB, hard refuse new registrations at 16 MB
(configurable) — a documented boundary, not a silent failure.

### 2.3 CAS discipline

- **Read**: `GetObject` → (body, ETag). Verify `seq` monotonicity against the last seq
  this process observed for the key (defensive assert: a regression means the store
  served a stale read — fail loudly).
- **Create**: `PutObject` + `If-None-Match: *`. 412/409 ⇒ someone else created; re-read.
- **Advance**: `PutObject` + `If-Match: <etag>`; body carries `seq+1`, fresh nonce.
- **Generation polling** (hydration checks): `GetObject` with `If-None-Match: <etag>` ⇒
  304 when unchanged — cheap hot-path reads without a coherence-risky cache. A
  process-local `(etag, decoded manifest)` cache keyed by validation-via-304 is safe
  even multi-process, because it never serves without revalidation. Serving-path reads
  beyond generation checks may reuse the validated snapshot within one request.
- **Error taxonomy** (mirrors PG's `notExecuted` contract for `IdempotentCAS`):
  - 412 `PreconditionFailed` on advance = **our write did NOT happen** (someone else
    advanced the object first): re-read fresh state and re-evaluate the operation on the
    new snapshot. For `CASRef`, if the ref no longer matches `old` ⇒ `won=false` (never
    retried); otherwise re-attempt the CAS. For state mutations (registrations, marks)
    re-apply on the fresh snapshot.
  - 409 `ConditionalRequestConflict` = concurrent conditional write in flight: retry the
    same attempt.
  - Timeout / connection loss / 5xx **after the request was sent** = **ambiguous**: the
    write may have landed. Recovery = re-read and apply a per-method predicate:
    (a) if `writer == our nonce`, our write landed and is the latest ⇒ won;
    (b) if `writer != ours`, our write may still have landed and been superseded — test
    the **semantic effect** (ref now equals `new`; pack id present; reservation gone) or,
    for non-idempotent effects with no stable predicate (`RetirePackAttempts`
    decrements), test our op-ID's membership in the bounded `ops` ring **and record the
    op-ID on the touched reservation/ledger entry itself** (cleared with the entry), so
    eviction from the global ring cannot forge a "not applied" verdict for a live entry.
    If membership is absent AND the semantic predicate is inconclusive, **fail closed**:
    return the ambiguity as a retryable error and let TTL/sweep machinery collect the
    leaked entry — a non-idempotent mutation is never blindly re-applied. Idempotent
    re-apply is the fallback only where re-applying is provably harmless — the driver
    classifies every method explicitly (Appendix A carries the per-method recovery
    class). This is the simtest "ambiguous-but-committed CAS" oracle contract
    (`scenario.go:605`), now exercised against the manifest.
  - Failures **before the request was sent** (dial, ctx expiry while queued) = *not
    executed* (never recovered as a win) — the PG `notExecuted` twin.
- **Retry/backoff**: contention retries use full-jitter exponential backoff, 25 ms → 2 s
  cap, ≤ 16 attempts, bounded by ctx deadline. Exhaustion returns a typed
  `ErrCASContention` — surfaced, never silently dropped.

### 2.4 TTL semantics without a server clock

PG/SQLite stamp `created_at` with the DB clock so multi-replica clock skew can't corrupt
reservation expiry, adoption-claim steals, unreferenced-grace, or legacy-reap grace.
s3wal substitutes a **manifest logical clock plus fail-closed skew guards** — not a bare
trust in the local wall clock:
1. Every CAS writes `clk = max(localNow, clk_prev + 1ms)` (§2.2): `clk` is monotone
   across restarts and immune to backward local jumps. All stamps written into the
   manifest (`created_at`, `unreferenced_since`, …) use this write-time `clk`.
2. Expiry comparisons that *permit destruction or steal* (reservation expiry, claim
   steal, delete-lock expiry, unreferenced/legacy-reap grace) compute elapsed time as
   `min(localNow, clk_candidate) − stamp`, where `clk_candidate = max(localNow, clk)` is
   what this write would set: a **forward** local-clock leap therefore cannot shorten a
   grace window past the manifest's own history. Destructive/steal checks additionally
   fail closed (demote to report-only / retry later, log loudly — never expire early)
   when `|localNow − clk| > min(15 min, ttl/4)` — the bound scales with the lease: a
   2 min delete lock tolerates ~30 s of skew, an 1 h grace tolerates 15 min.
   Over-retention and false-deny are the only reachable failure directions.
3. Cheap cross-check: the driver compares `localNow` against the S3 HTTP response `Date`
   header on every CAS and asserts agreement within the same bound (defensive
   crash-and-burn on grossly wrong clocks).
4. Grace floors keep their conservative minimums (1 h quarantine floor, reap grace ≥
   max(sweep threshold, 1 h)) — residual skew inside the bound is noise against them.
5. The instance lease (§2.11) turns accidental dual-writer topologies — the only source
   of *cross-machine* skew — into a detected fail-stop.
The ADR records this as a bounded, fail-closed deviation from the DB-clock invariant,
valid only under the single-node decree.

### 2.5 Crash-window analysis

Notation: `W#` = process dies (or the response is lost) at that point. Invariants
checked: **P** publish-before-CAS (committed refs never point at unavailable packs),
**T** tombstone-before-delete, **Q** quota-oracle (usage == registered + retained),
**A** absence-cover behavior.

**Push publish** (hook: upload pack blob → `RegisterPacks` → `CASRef` per ref):
- W1 after pack blob PUT, before `RegisterPacks` CAS: orphan blob; unreferenced by any
  manifest ⇒ sweeper reaps after age threshold. P holds (no ref moved). Q holds
  (reservation still counted until TTL).
- W2 `RegisterPacks` CAS ambiguous: re-read; pack present ⇒ done (reservation consumed +
  usage credited in the same landed write — Q holds atomically); absent ⇒ re-apply.
  Duplicate re-apply is idempotent (same pack id ⇒ no-op by set semantics, reservation
  already consumed ⇒ no double credit — same contract as the SQL upsert).
- W3 between `RegisterPacks` and `CASRef`: registered pack, refs not flipped — identical
  to today's SQL window; retry re-runs; abandoned state is reclaimed by LFS/pack
  reclamation exactly as on SQLite. P holds (refs never moved).
- W4 `CASRef` ambiguous: `IdempotentCAS` recovery re-reads the ref (`GetRef` is a
  manifest read) — the existing decorator works unchanged on top of the driver's
  executed/not-executed error discipline (§2.3).
- Multi-ref atomic pushes: policy is evaluated before publication (unchanged); the ref
  flips are sequential `CASRef` calls exactly as on SQL backends today — s3wal neither
  improves nor regresses partial-failure semantics of the existing interface. (A future
  batch-CAS extension is a natural single-manifest win — explicitly out of scope.)

**Repack** (`ReplacePacks`): new blob uploaded first (unchanged), then ONE CAS that
drops old pack rows, inserts the fold, tombstones old keys into `retained` (bytes still
counted), bumps gen. W5 ambiguous CAS ⇒ re-read: fold present ⇒ won; old packs gone but
fold absent is **impossible** (single object write). T and Q hold by atomicity; losing
fold blobs are swept (P unaffected — refs don't reference pack rows directly).

**Sweep / reclaim** (job code unchanged, runs against the interface):
- Superseded-pack reap: sweeper quarantine-first `safeDelete` blob, then
  `ReclaimRetainedPack` CAS (debit + drop tombstone). W6 between them: tombstone + bytes
  remain counted, blob gone ⇒ conservative over-count, healed by the next sweep pass
  re-running reclaim (idempotent, returns 0 after first success). T holds: the row-level
  tombstone exists strictly before any blob delete (created atomically by ReplacePacks).
- `GuardedLFSDelete` (W6): SQL serializes delete-vs-register under the ledger lock;
  check-then-delete against an object store cannot be fixed by a *post-hoc* CAS (the
  blob would already be gone when the conflict is detected). s3wal instead **prevents**
  the race with a CAS **delete-lease** whose lifetime provably covers the delete I/O:
  - Lock entry: `delete_locks[key] = {owner, op_id, created_at}`; effective TTL is a
    protocol constant with `TTL ≥ deleteIOTimeout + safetyMargin + skewMargin`.
  - (i) CAS-insert the lock only if the key is unregistered AND no live lock exists —
    the serialization point; (ii) run `del` under a **hard `deleteIOTimeout`**
    (context-enforced; note a request already sent may still complete server-side after
    the deadline — the blocking margin below is what fences late completion); a renewal
    loop CAS-refreshes `created_at` every TTL/3 while deletion is in progress;
    (iii) CAS-clear the lock.
  - Register/adopt/restore paths treat a lock as blocking until
    `created_at + TTL + deleteIOTimeout + safetyMargin` (not merely TTL), so even a
    lock that missed its final renewal still fences the worst-case in-flight DELETE;
    they retry after backoff — the same shape as losing the adoption claim.
  - Renewal lost **before** the DELETE request is sent ⇒ abort the delete (never send).
    Renewal lost **after** it was sent ⇒ report failure and rely on the blocking margin
    above; the caller may not treat the key as deleted.
  - Crash windows: die before `del` ⇒ stale lock briefly blocks registration, expires
    (safe direction); die between `del` and clear ⇒ key unregistered + stale lock, same
    expiry. Phase 2 tests include stalled-delete / lock-expiry / late-register races.
  No window can delete a registered object's bytes.

**Purge pipeline** (each phase = one CAS; ordering preserved from the SQL sequence):
fence-first (W7 after fence: all row-creating writes reject — fence is in the same
object every write CASes, total order for free), refs drop (W8: fenced, partial state
invisible to writers, re-run continues), packs→retained, LFS drops, blob reap (job,
unchanged), zero-row verify = manifest field check, ledger cleanup + `purged_at` stamp
= terminal CAS. The manifest object persists forever as the tombstone (burned name).
Every phase is idempotently re-runnable; `purge-repo` re-runs rescan as today.

**LFS register/adopt**: reservation consume + row insert + count + usage + adoption-gen
bump = one CAS (W9 ambiguous ⇒ re-read/reapply, idempotent). Adoption copy-then-register
keeps its claim-renewal protocol unchanged (claims are manifest entries; renewal CAS
extends `created_at`; a lost renewal CAS ⇒ `renewed=false` exactly as SQL).

**ADR-41 key duality**: canonical-only trust, legacy grace anchored on the registry
`created_at` (manifest clock, §2.4), size-aware sparing (`LFSObjectRegisteredAt` returns
in-manifest bytes) — all registry reads/writes are manifest fields; no protocol change.
**Absence covers (ADR-43)**: cover reservations are manifest reservation entries with
incarnation keys; renewal/shrink/re-mint are per-group CAS ops; fleet rotation via
prefix listing. Charge/renewal atomicity actually *improves* (cover + gen bump in one
CAS).

**Meta/ref cross-store windows (W10, zero-infra mode with S3 meta — §5-Q4):**
- Repo creation: meta CAS first (repo row exists), manifest created lazily by the first
  authority write via `If-None-Match: *` — a crash between leaves a virgin repo (no
  manifest = empty refs), which is today's semantics for a never-pushed repo.
- Soft-delete → purge: meta soft-delete stamps `deleted_at`; the purge job later fences
  the manifest (W7). A crash between leaves a soft-deleted repo pending purge — exactly
  the SQL steady state; purge re-runs are idempotent.
- Restart-from-bucket-only: both stores live in the bucket ⇒ the Phase 3 drill (§4)
  proves a cold process reconstructs full service from `_wal/` + `packs/` + `lfs/`
  alone.

### 2.6 Conditional-write probe & store requirements

`InitSchema` proves the store's conditional-write fidelity before serving (a lying
store that ignores the headers corrupts silently — the worst failure mode):
1. `PUT _wal/probe` with `If-None-Match: *` (create) — must succeed.
2. Duplicate create with `If-None-Match: *` — **must fail 412**.
3. `PUT` with correct `If-Match: <etag>` (advance) — must succeed, new ETag.
4. `PUT` with the stale (pre-advance) ETag — **must fail 412**.
5. Two racing advances from the same snapshot (goroutines) — exactly one must win.
Any deviation ⇒ refuse to serve with an explicit diagnosis. The probe also documents the
store requirements: conditional PUT (If-Match / If-None-Match), strong read-after-write
and list-after-write consistency. The bench harness's contention run doubles as a
sustained lost-update detector on both SeaweedFS and real S3.

### 2.7 No conditional DELETE, anywhere

MinIO ignores `If-Match` on DELETE (verified during adoption work; unchanged). AWS S3
has since added conditional deletes on general-purpose buckets, but self-hosted stores
have not followed — so the portability verdict stands. The protocol therefore never
guards a DELETE with a precondition: manifests are never deleted (purge tombstone is
terminal state, not deletion); scan-state objects are deleted only by their single
writer; blob deletes remain guarded by the delete-lease (§2.5 W6) exactly like today's
mutual-exclusion designs.

### 2.8 Sync-claim fence

PG/SQLite piggyback CASRef's sync fence on `repo_upstreams` in the **shared** DB
(`ref.go:798`: "deployment invariant: meta and ref share one database"). s3wal has no
shared DB, so the claim token moves **into the manifest** (`sync_claim` field):
`TryBeginSync`/`RenewSync`/`FinishSync` in zero-infra mode write through to the base
manifest, and a fenced `CASRef` checks the holder **in the same object it CASes** —
steal-vs-write serialization is the manifest CAS itself, strictly stronger than the SQL
FOR-UPDATE argument. Wiring detail for Phase 3 (the meta seam owns those methods today).

### 2.9 Fleet pagination: contiguity argument & cursor resume

SQL fleet scans keyset-page by `(repo, key)` byte order across ALL repos including
namespace members. s3wal lists `_wal/repo/<baseID>.json` (base groups only) and pages
members from each group's object in memory. This reproduces the exact SQL order given
one lemma: **a base's member keys are contiguous in byte order** — i.e. no *other* base
ID sorts between `<base>` and its members `<base>+<suffix>`. Proof sketch: member keys
extend the base ID with `+` (0x2B), which sorts **below** every character in the
asserted repo-ID alphabet (`repo_<12 hex>`, all ≥ 0x30); therefore any distinct base ID
sharing the prefix `<base>` continues with a character > `+` and sorts after all
members, and any not sharing the prefix sorts entirely before/after the whole family.
The driver *asserts* the alphabet at key-build time (§2.1), making the lemma checked,
not assumed. Manifest object keys carry **no extension** (§2.1) so the bucket listing
order of `_wal/repo/<base>` equals the byte order of base IDs — extension suffixes
would corrupt ordering between prefix-related IDs (`abc` vs `abc-1`); conformance tests
cover exactly these prefix families (`abc`, `abc-1`, `abc+ephemeral`). Cursor resume
for `(afterRepo, afterKey)`: derive `base(afterRepo)` by stripping the `+suffix`, GET
that group's manifest, emit member rows `> (afterRepo, afterKey)`, then continue
listing with `StartAfter = _wal/repo/<base>`. Fallback if the lemma or ordering
argument ever breaks (alphabet change): scan-all-manifests → materialize rows → sort by
`(repo, key)` → return `limit` — O(fleet) but unconditionally correct, acceptable at
zero-infra scale and kept implemented behind a flag as the degraded path. Phase 1 ports
the existing `absence_test.go` pagination-order conformance tests unchanged; they are
the oracle.

### 2.10 Scan state & the destructive-reap watermark (PR #132)

SQL re-reads the `sweep/lfs-destructive-reap` watermark **inside the retirement
transaction** (both ends on the DB clock). s3wal cannot join a side object and a
manifest in one atom. Substitute rule, concrete and fail-closed:
- Scan-state side objects (`_wal/scan/<job>/<scope>.json`) are CAS-advanced; their
  single-writer discipline is the single-process decree, and a CAS conflict on them is
  treated as evidence of dual-serve ⇒ loud error + fail-stop (§2.11).
- Because sweeper and reclaimer run in the **same process** (ticker path), the
  watermark's authoritative copy during a run is in-process state backed by the durable
  side object; `RetireUnreferencedLFSObject` requires a watermark freshly re-read (or
  written) within `max(grace, 1 m)/2` of the retire CAS, else the pass **demotes to
  report-only** — mirroring the existing pre-demotion behavior rather than inventing a
  new atomicity mechanism. Timestamps compare on the §2.4 clock rules.
- If #132's interface lands mid-experiment, this mapping implements it verbatim; if the
  desk prefers, s3wal may alternatively ship with destructive LFS reclamation demoted to
  report-only entirely (delete flags inert), deferring the watermark story — flagged as
  an explicit Phase 2 decision point.

### 2.11 Instance lease (dual-serve guard, Phase 3)

`_wal/instance-lease` CAS-heartbeat (30 s cadence, 2 min TTL): serve refuses to
start over a fresh foreign lease; on heartbeat loss the process fail-stops (crash, not
limp). Correctness does **not** depend on it — every state change is CAS-serialized
regardless; the lease only converts "two writers cause TTL-clock weirdness and cache
staleness" (§2.4) into a detected condition. Defensive, cheap, honest about being
best-effort.

## 3. Benchmark prototype plan (go/no-go BEFORE deep investment)

The bench prototype is part of **Phase 1** — measured numbers reach the desk before
Phase 2 begins. Harness follows the existing micro-bench patterns
(`internal/ref/ledger_bench_test.go`, nightly `bench-nightly.yml` conventions), plus a
small standalone runner for real-S3 runs.

**Measured (all: SeaweedFS testcontainer AND real S3; optionally S3 Express one-zone):**
1. Manifest CAS advance latency, p50/p95/p99 (1 KB and 1 MB manifests) — the primitive.
2. Simulated push = GET + pack PUT (8 MB) + 2 CAS advances, p50/p99 — single writer.
3. Hot-repo contention: N = 2/8/32 concurrent pushers on ONE repo group, measuring
   per-push success latency distribution, aggregate CAS-advance throughput, retry
   counts, and max starvation gap, under the §2.3 backoff policy.
4. Generation-read latency: unconditional GET vs `If-None-Match` 304 path, p50/p99.
5. Baseline comparison: same op mix on SQLite (existing backend) for an honest "what
   zero-infra costs you" table.

**Kill thresholds (any ⇒ recommend closing #133):**
- (2) p50 > 500 ms or p99 > 2 s on real S3 from same-region compute.
- (3) at N=8: aggregate successful CAS advance < 3/s sustained, or any pusher starved
  > 30 s; at N=32: throughput collapse below the N=8 figure (negative scaling) or
  unbounded retry growth (livelock).
- (4) 304-path p99 > 100 ms on real S3 (hydration checks ride every git request).
- (1) SeaweedFS-vs-S3 semantic divergence caught by the harness (lost update under
  contention = disqualifying for the self-hosted story, not just slow).

**Method & environment (falsifiability):** real-S3 runs from same-region compute
(region recorded), ≥ 200 samples per cell after a 20-op warmup, ≥ 3 runs on separate
days for the headline numbers; contention runs 60 s sustained per N; raw per-op CSVs
published alongside the summary tables. SeaweedFS runs on the pinned CI container
image for reproducibility.

**Published artifacts:** raw tables (SeaweedFS + S3) + the SQLite baseline in the PR body
and `docs/plans/s3wal-bench-results.md`; the harness lands in-tree
(`internal/ref/s3wal/bench_test.go`, real-S3 lane gated on `CODESTORAGE_BENCH_S3_URL`).

## 4. Phased implementation plan

Every phase captures reviewer-verifiable dogfooding evidence: terminal recordings
(agent-tty) of the drills, screenshots/artifacts of bench tables and CI runs, attached
to the phase's PR or gate report.

**Phase 1a — store qualification + CAS primitive + bench go/no-go (before the full
driver). Ordering is qualification-first: SeaweedFS is empirically qualified BEFORE
the repo-wide container swap lands.**
- Step 1 — neutral S3 compatibility harness (store-agnostic, endpoint-parameterized;
  runnable against SeaweedFS, MinIO old/new, real AWS S3, optionally LocalStack).
  "Existing suites green" is NOT sufficient swap validation; the harness explicitly
  covers the surfaces the existing gocloud-based suites under-exercise:
  - basic `PutObject`/`GetObject`/`HeadObject`/`DeleteObject` round-trips;
  - gocloud writer behavior at small AND multipart sizes (verify readback size +
    digest; never assume multipart ETag == MD5);
  - gocloud `IfNotExist` (`If-None-Match: *`) duplicate-write rejection;
  - the raw-SDK §2.6 conditional-write probe (create / duplicate-412 / matched
    advance / stale-412 / racing writers, exactly one winner);
  - `ListObjectsV2` with >1000 keys, `Prefix`, `StartAfter`, lexicographic order,
    including the §1 `_wal/repo/<base>` member-contiguity assumptions;
  - presigned GET (the LFS download path);
  - path-style endpoint + SigV4 compatibility, bucket bootstrap/health-check startup.
- Step 2 — qualify SeaweedFS: harness + §3 contention lost-update detector. If
  SeaweedFS fails, **gate-stopping escalation back to the desk before any swap**
  (fallback ranking in §5-R7).
- Step 3 — repo-wide swap PR (mechanical, lands only after step 2 passes): replace
  MinIO testcontainers with SeaweedFS (`internal/githttp/e2e_test.go:158`,
  `internal/blob/minio_test.go`); pin the exact SeaweedFS image+version and
  `weed server -s3` command/flags; versioning + object-lock disabled (seaweedfs#8073
  avoidance); health-check + bucket bootstrap wired; container logs archived on
  failure. Validated by the full existing e2e/blob suites green PLUS the step-1
  harness running in CI.
- Files: `internal/ref/s3wal/casobj.go` (raw `s3.Client` conditional GET/PUT + §2.3
  error taxonomy; ~200 lines), `probe.go` (§2.6 probe), `qualify_test.go` (step-1
  harness), `bench_test.go` (§3 harness).
- Deliverable: the §3 tables + the §5-R7 store-qualification matrix (SeaweedFS
  validated; old+new MinIO documented — whether each rejects, ignores, or mishandles
  conditional headers — proving the probe refuses non-compliant stores).
- **Gate 1a-A (self-hosted correctness):** SeaweedFS passes harness + probe +
  contention detector ⇒ unblocks the step-3 swap PR and Phase 1b driver work.
- **Gate 1a-B (real-S3 performance):** the §3 kill thresholds measured on real S3.
  Required before any promote/keep verdict (Phase 4) — NOT required to continue
  implementation. Known dependency: **no AWS credentials in the dev environment**
  (verified 2026-08-25); needs a scoped test bucket + credentials from the desk. If
  absent at reporting time, the gate report says "provisionally continue; real-S3
  gate blocked" — never "gate passed".

**Phase 1b — driver skeleton + conformance on SeaweedFS.**
- Files: `internal/ref/s3wal/` — `memcas.go` (in-memory CAS fake with per-op fault
  hooks — the unit/DST substrate), `manifest.go` (schema, seq/writer/ops/clk, size
  guards), `store.go` (RefStore + job sub-interfaces over read-mutate-CAS).
  `internal/ref/ref.go`: `Open` dispatch for `s3wal://bucket?endpoint=…&region=…`
  (ADR-12 URL-scheme factory).
- Audit item: verify the hook's multi-ref atomic-push semantics against sequential
  `CASRef` (`internal/githttp/hook.go:604`) and record the finding in the ADR — batch
  single-manifest CAS stays a non-goal unless a safe seam already exists.
- go.mod: aws-sdk-go-v2/service/s3 is already vendored; promoting it to a direct import
  requires `go mod tidy && go mod vendor` **in the same commit** (vendored-deps rule).
- Tests: `runRefBackends` gains an `s3wal` lane (`generation_test.go:91`) — all 56
  conformance tests green against a **SeaweedFS** testcontainer (shared
  package-wide like `pgTestStore`); probe tests against the old MinIO image
  documenting its actual conditional-PUT behavior and proving the probe refuses any
  store that fails §2.6.
- **Gate: conformance green.**
- Dogfooding: terminal transcript/recording (agent-tty) of the conformance + probe
  runs; SeaweedFS container logs archived.

**Phase 2 — crash windows + DST.**
- Files: `memcas.go` fault classes wired to the simtest fault plan (clean / ambiguous /
  stale-read on GET, PUT-if-match, PUT-if-none-match, LIST); `internal/simtest/cluster.go`
  gains a `RefBackend` config knob (sqlite | s3wal-over-memcas); oracles untouched.
- Tests: table-driven crash-window unit tests enumerating §2.5 W1–W9 (kill at each
  point, restart, assert P/T/Q/A); ambiguous-CAS recovery tests under `IdempotentCAS`;
  DST: `TestDSTFast` + determinism + a seeded sweep lane on the s3wal backend, all
  existing oracles (incl. quota ledger and CAS-recovery) green; multi-node DST runs
  s3wal *shared-fake* topology — stricter than the single-node decree, catching
  protocol (not deployment) races.
- **Gate: DST green over ≥ the nightly seed budget; crash matrix reviewed.**
- Dogfooding: seed corpus + failure-replay instructions in the PR; one recorded
  kill-9-mid-push drill against SeaweedFS showing clean recovery.
- *Amendment (issue #216, 2026-09):* the stale-read GET class listed above was
  retired from the DST fault plan. It contradicts the §1/§2.6 hard requirement
  the driver is written against — every unconditional read is trusted, and the
  §2.3 seq watermark only catches a read regressing below the process's own last
  observation — so a served-stale manifest yields a silently wrong *read-only*
  verdict (a reservation sweep that "finds nothing to prune", a CAS that "lost")
  with no conditional PUT to expose it; under that fault the exact-state oracles
  are unsatisfiable by any read-only path. Seed 833534285 is pinned in
  `TestDSTRegressions`. The 412 lost-race path the arm targeted is exercised
  34–59× per 300-step seed by the multi-node seed cache (issue #182) alone; the
  in-package `armStaleGet` recovery tests and the seq-watermark panic test remain.

**Phase 3 — zero-infra serve mode + docs.**
- Files: `cmd/codestorage/config.go` (resolve `s3wal://` ref URL; refuse `file://` blob
  pairing; jobs auto-route to ticker via existing non-PG check), sync-claim write-through
  seam (§2.8), instance lease (§2.11), sweeper `_wal/` exclusion assertion, S3 meta
  backend per §5-Q4 resolution.
- Docs: new ADR-44 (s3wal manifest CAS — decision, §2.4 clock deviation, non-goals),
  deployment guide zero-infra section with the honest scaling boundaries table,
  `docs/reference/configuration.md`, storage-lifecycle notes (`_wal/` prefix, purge
  tombstone permanence).
- Tests: serve-mode e2e (githttp suite parameterized lane on SeaweedFS): push/clone/LFS
  round-trip, restart-from-bucket-only drill, dual-serve refusal test.
- **Gate: e2e lane green + docs reviewed.**
- Dogfooding: recorded end-to-end session — `codestorage serve` in s3wal mode against
  SeaweedFS **and** real S3: create repo, push (incl. LFS), kill -9 mid-push, restart on a
  *different* machine/dir with only the bucket, clone + fsck clean. Terminal recordings
  attached to the PR.

**Phase 4 — benchmark publication + final report.**
- Full push-path e2e bench (real `git push` over loopback, N = 1/2/8/32) vs SQLite mode;
  SeaweedFS + real S3 (+ S3 Express if available); publish
  `docs/plans/s3wal-bench-results.md` + PR-body tables; optional nightly bench lane
  entry (`-p 1` conventions). Final experiment verdict against issue #133's acceptance
  criteria, recommending promote/keep-experimental/kill.

## 5. Risks & open questions (with resolutions)

- **R1 gocloud vs raw SDK — RESOLVED.** gocloud portable API lacks If-Match writes
  (only `IfNotExist`); the `BeforeWrite`→`UploadObjectInput.IfMatch` escape hatch exists
  in our vendored versions but routes through the transfer manager (multipart ambiguity,
  murky 412 surfacing through `writer.Close`). Decision: raw vendored `s3.Client` for
  `_wal/` objects only; gocloud unchanged for packs/LFS. No new module; go.mod
  directness bump + `go mod vendor` same-commit.
- **R2 self-hosted store fidelity — RESOLVED with teeth.** Header-ignoring (accepting
  the write!) is the silent-corruption failure mode and must be assumed of any store
  until the Phase 1 probe documents actual behavior. Three layers: SeaweedFS as the
  decided self-hosted store (§5-R7); mandatory boot probe (§2.6, refuse to serve); the
  bench contention run doubles as a sustained lost-update detector on both stores.
  MinIO's disqualifying quirks (why it was dropped): the CI pin predates
  conditional-PUT support entirely; If-Match DELETE is ignored even on modern releases
  (§2.7 — designed out regardless, for portability); quorum-degraded conditional
  bypass (minio#21603).
- **R3 PR #132 — mapped, not blocked.** Scan-state interface → `_wal/scan/` side
  objects (§1). Plan rebases if #132 merges first (expected); otherwise the methods
  don't exist yet and Phase 1 proceeds without them.
- **R4 ETag ABA — designed out** via seq+nonce distinct-bytes rule (§2.2); unit test
  asserts identical logical content still produces distinct ETags across writes.
- **R5 hot-group contention** (namespace members + jobs share one CAS point) — measured
  (§3.3); jobs back off under `ErrCASContention` (they already tolerate bounded errors
  per pass).
- **R6 manifest growth** — warn/refuse thresholds (§2.2), documented boundary; the
  target deployment (small/single-tenant) sits far below them.
- **Q4 meta store scope — RESOLVED in-plan.** True zero-infra ("no local-disk
  authority") requires repos/orgs/webhooks/credentials/upstreams (meta.Store) off local
  disk too; refs-in-S3 + meta-in-SQLite has *inconsistent durability* (ref authority
  survives disk loss; the repo-URL→ID mapping doesn't) — worse than plain SQLite mode,
  and it breaks the issue's "S3 as sole authority" premise. **Resolution: Phase 3
  includes a minimal S3 meta backend** — one `_wal/meta.json` CAS object (all meta
  tables; low write rate; encrypted credentials unchanged) + process-local
  304-validated cache; sync-claim methods write through to base manifests (§2.8);
  cross-store crash windows enumerated in §2.5 W10. The SQLite-meta-on-a-volume
  fallback exists only as an explicit **de-scope lever the desk may pull**, and pulling
  it means the experiment ships without satisfying the zero-infra acceptance criterion
  (relabeled "low-infra") — it is not an equal option. The kill criteria (§3) are
  unaffected either way.
- **R7 CI/dev store selection — DECIDED: SeaweedFS replaces MinIO (user directive,
  2026-08-25).** Building a CAS-authority product on a store that silently ignores the
  CAS headers we depend on makes every deployment a latent correctness hazard that
  must be perpetually re-validated; MinIO's community edition has also stagnated.
  SeaweedFS is the self-hosted store of record for CI, dev, and the deployment guide,
  and the repo-wide testcontainer replacement is **in scope** (Phase 1a step-3 swap
  PR — landing only AFTER step-2 empirical qualification passes; touches
  `internal/githttp/e2e_test.go:158` and `internal/blob/minio_test.go`).
  Phase 1a still runs the compatibility harness + probe (§2.6) + sustained contention
  lost-update detector against the plausible stores and publishes the qualification
  matrix — now as empirical validation of the decision and deployment-guide input,
  not as an open selection. Evidence:
  - **SeaweedFS — decided:** implemented conditional PUT (seaweedfs#7154) with
    per-object owner-filer serialization (a real CAS design, not a best-effort check);
    known defect only with versioning+object-lock enabled (seaweedfs#8073) — a
    configuration s3wal does not use; single lightweight binary (`weed server -s3`),
    easy testcontainer. Implementation freshness is the residual risk; the probe +
    contention harness validates it empirically at the Phase 1a gate, and a failure
    there is a gate-stopping escalation back to the desk.
  - **MinIO — dropped:** our CI pin predates known conditional-write support and
    must be assumed non-compliant until the Phase 1a matrix documents whether it
    rejects, ignores, or mishandles the headers (header-ignoring being the
    silent-corruption case); modern (≥ 2025) releases support conditional PUT but
    still ignore If-Match on DELETE (verified during adoption work) and carry the
    quorum-degraded conditional bypass minio#21603; community-edition stagnation
    makes betting on fixes a strategic risk. Kept in the Phase 1a matrix only to
    document behavior and prove the probe refuses non-compliant stores.
  - **Garage:** no conditional-PUT support — unimplemented surface returns 501, and
    ecosystem workarounds exist specifically to strip If-None-Match for it; its
    CRDT/AP metadata design makes linearizable CAS architecturally unlikely soon.
    Disqualified for s3wal (fine for plain blob storage).
  - **RustFS:** multiple open conditional-request defects through 2026 (304 handling
    kills the connection, ETag parse strictness, copy-source If-Match rejected).
    Disqualified for now.
  - **Ceph RGW:** too heavy for testcontainers CI regardless of fidelity; not pursued.
  - **LocalStack:** implemented If-Match conditional writes (localstack#11941);
    an emulator, not a deployment target — optional cheap third CI cross-check lane.
  The zero-infra deployment guide will document *qualified* stores (probe-passing:
  SeaweedFS and AWS S3) and explicitly warn that the serve-time probe refuses
  unqualified ones.
  **Support posture (explicit):** plain blob storage (packs/LFS under PG/SQLite
  modes) remains generic S3-compatible via gocloud — no intentional break for
  existing PG/SQLite blob-store MinIO deployments, though MinIO is now
  best-effort/untested — and MinIO ceases to be the tested reference store: release
  notes and
  deployment docs state SeaweedFS is the tested self-hosted store, MinIO moves to
  untested/best-effort status for plain blob storage, and s3wal authority mode
  refuses any store that fails the boot probe regardless of vendor. The swap PR's
  release-note entry carries this statement.
- **Q5 CI cost** — the s3wal conformance lane adds one SeaweedFS container to
  `internal/ref` tests (today PG-only + sqlite). Mitigation: share one container
  package-wide like `pgTestStore` does; elsewhere the swap replaces MinIO one-for-one,
  so the repo's total container count is unchanged.

## 6. Non-goals (explicit, per issue #133)

Multi-tenant scale; hot-repo write concurrency beyond the measured envelope;
multi-replica s3wal serving (single-node decree, ADR-13 analog, lease-guarded); River/S3
queue emulation; wire-compat cross-repo pagination performance parity; replacing PG or
SQLite as defaults; batch multi-ref CAS interface extensions.

---

## Appendix A — exhaustive method map (signatures, fit, ambiguous-PUT recovery class)

Recovery classes (§2.3): **RO** read-only (no recovery), **ID** idempotent re-apply,
**PRED** semantic-effect predicate on re-read, **OPS** op-ID membership (ring +
per-entry record; fail closed on inconclusive — §2.3), **CAS** the CASRef won/lost
protocol, **LEASE** delete-lease protocol (§2.5 W6). Method shapes are condensed for
tabulation — every row elides the leading `ctx context.Context` parameter and Go type
keywords; the exact Go signatures are the interface text at `internal/ref/ref.go:87-336`
plus the job sub-interfaces, which this table mirrors row-for-row with no method
omitted.

| Method (exact name & shape) | Fit | Recovery |
|---|---|---|
| `ListRefs(repo) (map[string]string, error)` | M | RO |
| `ListRefsWithMeta(repo) ([]RefInfo, error)` | M | RO |
| `ListRefsAfter(repo, after, limit) ([]RefEntry, error)` | M | RO (in-memory byte-order paging) |
| `GetRef(repo, name) (string, error)` | M | RO |
| `CASRef(repo, name, old, new) (bool, error)` | M | CAS (fences: purge + sync-claim, same object) |
| `RegisterPacks(repo, packs) error` | M | PRED (pack ids present ∧ reservations gone) / ID |
| `ListPacks(repo) ([]Pack, error)` | M | RO |
| `HasPack(repo, key) (bool, error)` | M | RO |
| `ReplacePacks(repo, old, newPack) (bool, error)` | M | PRED (fold present) |
| `IsRetainedPack(repo, key) (bool, error)` | M | RO |
| `ReclaimRetainedPack(repo, key) (int64, error)` | M | PRED (retained entry gone; debit rides same write) |
| `ReservePackBytes(group, packs, owner, usageFloor, floorGen, quota, ttl) (ReserveDecision, error)` | M | OPS (attempt increments are non-idempotent) |
| `QuotaLedgerState(group, ttl, excludeKeys...) (PackQuotaLedgerState, error)` | M | RO |
| `RetirePackAttempts(group, gens) error` | M | OPS (decrement-to-delete, token-fenced) |
| `SweepExpiredPackReservations(olderThan) (int64, error)` | L+M | ID (per-group prune, §2.4 clock) |
| `ReconcileLegacyLFSObjects() (int64, bool, error)` | L+M | ID (twin-drop + refund per group) |
| `RegisterLFSObject(repo, key, bytes) error` | M | PRED / ID |
| `RegisterAdoptedLFSObject(repo, key, bytes) error` | M | PRED / ID (gen over-bump harmless) |
| `RecordLFSMismatch(repo, key, fingerprint) error` | M | ID (upsert) |
| `LFSMismatchFingerprint(repo, key) (string, bool, error)` | M | RO |
| `ClearLFSMismatch(repo, key) error` | M | ID |
| `HasLFSObject(repo, key) (bool, error)` | M | RO |
| `TouchLFSObject(repo, key) (bool, error)` | M | PRED (row exists ∧ mark cleared) |
| `LFSObjectRegisteredAt(repo, key) (time.Time, int64, bool, error)` | M | RO |
| `BumpLFSAdoptionGen(repo) error` | M | ID (over-bump = spurious cache invalidation, safe) |
| `LFSAdoptionGen(repo) (int64, error)` | M | RO |
| `CountLFSObjects(repo) (int64, error)` | M | RO |
| `GuardedLFSDelete(repo, key, del) (bool, error)` | M | LEASE |
| `TryAdoptionClaim(repo, key) (bool, string, error)` | M | PRED (owner token == ours) |
| `RenewAdoptionClaim(key, owner) (bool, error)` | M | PRED (stamp advanced under our ownership) |
| `ReleaseAdoptionClaim(key, owner) error` | M | ID |
| `GetGeneration(repo) (int64, error)` | M | RO |
| `InitSchema() error` | — | probe (§2.6), idempotent |
| `Close()` | — | — |
| `FenceRepoPurge(base) error` | M | ID |
| `IsRepoPurged(base) (bool, error)` | M | RO |
| `DropRepoRefs(group) (int64, error)` | M | ID (retry count drift matches SQL re-runs) |
| `RetireRepoPacks(group) (int64, error)` | M | ID |
| `DropRepoLFSObjects(group) (int64, error)` | M | ID |
| `CountRepoRows(group) (int64, error)` | M | RO |
| `PurgeRepoLedger(group) (int64, error)` | M | ID |
| `LFSReposWithObjects(after, limit) ([]string, error)` | L | RO (§2.9) |
| `CountRefs(repo, limit) (int64, error)` | M | RO |
| `ListRepoLFSObjects(repo, after, limit) ([]LFSObjectRow, error)` | M | RO |
| `MarkLFSObjectsUnreferenced(repo, keys) (int64, error)` | M | ID (only stamps NULL rows) |
| `ClearLFSObjectsUnreferenced(repo, keys) (int64, error)` | M | ID |
| `RetireUnreferencedLFSObject(repo, key, pinnedGens, grace) (bool, error)` | M | PRED + watermark rule (§2.10) |
| `ListFleetLFSObjects(afterRepo, afterKey, limit) ([]AbsenceLFSRow, error)` | L+M | RO (§2.9) |
| `ListFleetPacks(afterRepo, afterID, limit) ([]AbsencePackRow, error)` | L+M | RO (§2.9) |
| PR #132 (if merged): `GetJobScanState`, `PutJobScanState`, `DeleteJobScanState`; cover listing helpers | S / L+M | ID on side objects (§2.10) |

`meta.Store` (Phase 3, §5-Q4): `_wal/meta.json` single-object CAS — same taxonomy;
low-rate CRUD is ID/PRED; `TryBeginSync`/`FinishSync` write through to base manifests
(§2.8). Note: `DeletePacks` does **not** exist on any store interface at `e750ecd5`
(verified by repo-wide grep) — an earlier survey draft mentioned it in error.

---

## Appendix B — advisor review log (6 rounds, endorsed)

The plan was iterated through a mandatory strong-model advisor loop; each objection and
its resolution:

**Round 1 (10 objections → all fixed):** non-exhaustive interface audit → Appendix A
row-for-row method map; fleet pagination correctness risk → §2.9 contiguity lemma +
cursor resume + degraded fallback; unsafe post-hoc GuardedLFSDelete CAS → delete-lease
redesign (§2.5 W6); under-specified PR #132 watermark atomicity → §2.10 concrete
fail-closed rule; hand-wavy TTL/clock story → §2.4 manifest logical clock + fail-closed
skew guards; unresolved meta scope → §5-Q4 resolved (Phase 3 S3 meta backend; SQLite
fallback = acceptance-relaxing de-scope lever) + W10 cross-store windows; missing pack
bytes → schema fix; wrong 412 taxonomy ("executed-and-lost") → corrected to
not-executed lost race + per-method ambiguity predicates + writer nonce + ops ring;
multi-ref push claim → Phase 1b hook-semantics audit item; overclaimed MinIO
"silently ignores" → softened to assumed-until-probed + §2.6 5-step probe spec.

**Round 2 (3 blockers → all fixed):** `.json` extension corrupts listing order between
prefix-related repo IDs (`abc-1.json` < `abc.json`) → extension-free manifest keys +
prefix-family tests + scan-all-sort fallback; 15-min skew bound too loose for 2-min
leases → per-TTL bound `min(15 min, ttl/4)`, never early expiry; bounded ops-ring
eviction could double-apply non-idempotent ops → per-entry op-ID records + fail-closed
on inconclusive.

**Round 3 (1 blocker → fixed):** delete-lease lifetime did not provably cover stalled
delete I/O → TTL ≥ deleteIOTimeout + margins, hard I/O timeout, TTL/3 renewal loop,
blockers wait past `TTL + deleteIOTimeout + safetyMargin`, renewal-loss abort rules,
stalled-delete/late-register race tests.

**Round 4: endorsed — "confident in the plan with no major objections remaining."**
Final polish applied: honest wording that a sent DELETE may complete server-side after
the client deadline (the blocking margin fences it), this log, and the per-phase
dogfooding-evidence rule.

**Round 5 (SeaweedFS amendment review; 6 required edits → all applied):** desk
directive made SeaweedFS the store of record; advisor directionally endorsed but
required: qualification-first ordering (harness/probe before the repo-wide swap, not
after) → Phase 1a restructured into steps 1–3; "existing suites green" insufficient →
explicit neutral compatibility harness (`qualify_test.go`) covering multipart/ETag,
IfNotExist, conditional probe, ListObjectsV2 >1000-key pagination + `_wal` contiguity,
presigned GET, path-style/SigV4, bootstrap; missing MinIO support posture → R7
statement (no intentional break for existing PG/SQLite blob-store MinIO deployments;
best-effort/untested; release-note entry); single gate hides blocked real-S3 kill
criteria → split Gates 1a-A (self-hosted correctness) / 1a-B (real-S3 performance,
required before any promote/keep verdict, "provisionally continue" wording when
credentials are absent); unverified "lies/accepts the write" claims → softened to
assumed-non-compliant-until-probed; missing operational pinning → exact image/version,
`weed server -s3` flags, versioning+object-lock disabled, health-check/bootstrap,
archived logs.

**Round 6: amendment approved — no blocking objections remaining.** One non-blocking
precision tweak applied (support posture states "no intentional break", not an
unverified compatibility guarantee).
