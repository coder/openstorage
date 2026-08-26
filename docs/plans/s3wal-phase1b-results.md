# s3wal Phase 1b results — RefStore conformance, recovery suite, deviations (issue #133)

Phase 1b wires the s3wal manifest-CAS store (built in
[s3wal-plan.md](s3wal-plan.md) §2, Phase 1a qualification in
`internal/ref/s3wal/qualify_test.go`) into `ref.Open`, runs the ENTIRE
existing RefStore conformance corpus against it as a third backend lane,
and pins every §2.3/Appendix A ambiguous-PUT recovery class with a
Docker-free unit suite over the MemCAS fault fake. This document records
the conformance matrix, the recovery-suite coverage, and every deliberate
deviation from the SQL backends.

## 1. Conformance matrix

`go test ./internal/ref/ -count=1` — 271 tests/subtests, exit 0, with the
s3wal lane added to `runRefBackends` (one package-wide SeaweedFS container,
`InitSchema` = the §2.6 conditional-write probe at boot, exactly like the
serve path). **Zero store changes were needed to pass the corpus.**

| Lane | Result |
|---|---|
| s3wal lane subtests | 53 pass |
| s3wal lane skips | 4, each justified below |
| pg / sqlite lanes | unchanged, all pass |

Skips (all "the SQL-era feature does not exist on s3wal", never "the store
behaves differently"):

| Test | Reason |
|---|---|
| `TestGenerationLegacyWriterStillBumps` | No SQL schema ⇒ no triggers and no legacy binary that could write raw DML around the driver; the driver's per-row bumps are the only write path, so the trigger rollout-safety property has no s3wal analog. |
| `TestLegacyStatementsCannotEndTargetReservations` | No v0.1.0 legacy era rows or raw-key statements exist on s3wal; reservations key on plain blob keys by design (§4 below). |
| `TestSettlementConsumesBothEraRows` | Same; plain-key settlement stays covered on s3wal by `TestRegisterConsumesReservation` and `TestRegisterLFSObject_SettlesReservationAndCredits`. |
| `TestRetireLeavesLegacyTwinRow` | Same; the gen-token retire semantics stay covered on s3wal by `TestRetireIsGenerationBound`. |

Deliberate pass-not-skip deviation: **`TestRetireIsGenerationBound` runs on
s3wal** although it sits amid the legacy-era tests — it is interface-only
(reservation gen tokens, owner rotation, stale-retire no-ops) and passes
unmodified, so skipping it would have discarded real coverage.

One backend-specific probe was extended rather than skipped:
`TestReservePrunesExpiredPhysicalRows` counts physical reservation rows,
and the s3wal arm reads `len(m.Reservations)` from the group manifest —
the `pack_reservations` table twin (the manifest is repo-scoped, so the
count is exact on the shared store).

## 2. Recovery-predicate suite

`internal/ref/s3walstore_recovery_test.go` — Docker-free, over the MemCAS
fault fake; competing writers are a second `S3WAL` driver sharing the same
MemCAS, interleaved between a faulted PUT and the recovery read via the
`AfterPut` hook (§8). 61 passing cases + 1 justified skip (grown across
review rounds; the class rows below list the load-bearing cases):

| Appendix A class | Cases |
|---|---|
| CAS (`CASRef`) | 4 — ambiguous applied ⇒ nonce win; applied-then-superseded ⇒ fail closed (ambiguity surfaced for the `IdempotentCAS` point-read recheck); real 412 (stale-read-induced) + ref moved ⇒ clean `won=false`, never retried; 412 + still-matching ⇒ retried win. |
| PRED | 11 — `RegisterPacks` applied/dropped (zero-growth rerun; single credit and gen bump); `ReplacePacks` applied ⇒ recovered won, dropped + old intact ⇒ rerun win, inconclusive (competing fold consumed the old set) ⇒ fail closed; `ReclaimRetainedPack` nonce win reports bytes, applied-then-superseded reports 0 with no double debit (the documented degraded report); `TryAdoptionClaim` applied ⇒ recovered claim, dropped + competitor claimed ⇒ clean loss; `RenewAdoptionClaim` dropped ⇒ rerun until the stamp provably advances (owner match alone is no proof — renewal changes only `CreatedAt`), stolen-while-ambiguous ⇒ clean lease-lost. |
| OPS | 5 — `ReservePackBytes` ring hit (decision stands, attempts stay 1) and both-miss ⇒ fail-closed retryable error, entry never blindly re-applied; `RetirePackAttempts` ring hit (single decrement), entry-op-ID hit after FORCED ring eviction (>64 foreign ops; asserts the full ring), both-miss ⇒ fail closed with the row intact. |
| LEASE (§2.5 W6) | 3 + 1 skip — registration blocked at `TTL + deleteIOTimeout + safety − 1ms` and unblocked at exactly the window (stale-lock expiry, no unconditional sleep); renewal loss while the DELETE is in flight refuses `deleted=true`; persistently dropped renewal PUTs are proven un-landed (stamp advancement, not owner match) and likewise refuse `deleted=true`; **skip**: renewal-loss-before-dispatch — the pre-dispatch check sits microseconds after lease acquisition with no seam the fake can reach without production-code scaffolding; the rule it double-checks is pinned by the in-flight case. |
| Fences | 9 purge-fence cases (every row-creating method incl. a namespace member; `CASRef` rejection carries the not-executed marker; every rejection is write-free, asserted via unchanged manifest seq) + 3 sync-claim fence outcomes (no claim ⇒ fail closed, foreign holder ⇒ loss, matching holder ⇒ win). |
| Protocol | 6 — §2.4 skew guard demotes an otherwise-due claim steal to report-only (write-free) and allows it once clocks agree, via the store's `now`-injection knob; §2.3 seq-monotonicity panic on an injected stale read; §2.2 hard-cap refusal on growing mutations with shrinking mutations passing, plus the once-per-base soft warn; scan-state stamps ahead of the local clock beyond a ±2s tolerance are unverifiable-stale (a backward-stepped clock must not read a stale destructive-reap watermark as fresh — fail closed, sub-tolerance offsets stay fresh); `QuotaLedgerState` mirrors the armed admission-fence marker into `RenewalDegraded` like the SQL twins' EXISTS subquery; the fleet-global lapse-evidence marker retries raced multi-writer upserts instead of failing them as dual-serve evidence. |

No store bugs were found: every class behaved exactly per the Appendix A
contract, and the only non-test change was the MemCAS hook (§8).

## 3. Package-layout deviation

The plan sketched the driver as `s3wal.Store`; it landed as `ref.S3WAL`
(`internal/ref/s3walstore*.go`). The driver needs `ref`'s unexported
machinery — `casFence`, `notExecuted`, the ledger knobs
(`maxActiveReservations`, `reconcileLegacyLFSBatch`), `baseRepoOf`,
`lfsClaimTwin` — the internal conformance tests must inspect it, and
`ref.Open` must dispatch to it. A store inside `package s3wal` would be an
import cycle in both directions, so `s3wal` stays a leaf substrate
(manifest codec, conditional clients, probe, fakes) and the RefStore
implementation lives in `package ref`, mirroring how the PG/SQLite
backends live there. Rationale is also recorded in the `s3walstore.go`
header.

## 4. Divergences from the SQL backends (all documented in code)

- **No era spellings.** SQL reservations carry the `reservKeySuffix` era
  fence because v0.1.0 binaries write the same tables with raw blob keys.
  s3wal predates no manifest-era binary, so reservations key on PLAIN blob
  keys (`s3walstore_quota.go` header) — hence the three legacy-era test
  skips above.
- **Alphabet relaxation.** §2.9's member-contiguity lemma needs every base
  key byte to sort above `'+'` (0x2B) with no `'+'` or non-ASCII bytes.
  `s3wal.ManifestKey`/`ValidBaseRepoKey` enforce exactly that ordering
  invariant, NOT the production `[0-9a-z-]` server-generated alphabet —
  deliberately, so the conformance corpus can run raw `t.Name()`-derived
  repo IDs (slashes, underscores, uppercase — all > 0x2B). Production IDs
  remain a strict subset; the lemma is checked at key-build time either way.
- **Sync-claim write-through deferred to Phase 3.** Phase 1b implements
  only the READ side: `CASRef` under `WithSyncClaimFence` evaluates
  `Manifest.SyncClaim` same-atom with the swap and fails CLOSED when the
  claim is absent or foreign (issue #86 semantics). Nothing writes
  `SyncClaim` yet — `TryBeginSync`/`FinishSync` write-through is the §2.8
  Phase 3 seam; the recovery suite injects claims by direct manifest edit.

## 5. Generation-bump matrix (as implemented)

s3wal has no schema triggers; the driver bumps `Member.Gen` inside the
same CAS as the row change — same invalidation-token contract, same
per-changed-row granularity:

| Mutation | Bump |
|---|---|
| `CASRef` won (create/update/delete) | +1; lost race/fence rejection: 0 |
| `RegisterPacks` | +1 per INSERTED pack row; idempotent retry: 0; reservation-only settlement: 0 (reservations are not refs/packs rows — no SQL trigger fires there either) |
| `ReplacePacks` | +1 per deleted old row, +1 for the fold insert |
| `DropRepoRefs` / `RetireRepoPacks` (purge) | +1 per dropped/retired row |
| `PurgeRepoLedger` | resets `Gen` to 0 (the `repo_generations` row deletion, counted once) |
| LFS registrations / marks / claims | 0 member bumps; adoption-generation bumps ride the same CAS where the SQL backends bump them (adopted registration, twin reconcile, retirement, `ReplacePacks`' adopted credit) |

Verified per-delta (not just "strictly increases") by
`TestGenerationBumpsOnEveryMutation` on the s3wal lane.

## 6. Hook audit (read-only; githttp untouched)

`internal/githttp/hook.go:604` — the git update hook reconstructs the
store via `ref.Open` (which now dispatches `s3wal://`) and issues ONE
`CASRef` per ref (`hook.go:611`), since git invokes the update hook once
per ref. Multi-ref pushes gate POLICY atomically in pre-receive, but a
per-ref CAS conflict mid-push leaves earlier refs committed — the existing
documented residual of the per-ref hook protocol on every backend. s3wal
neither improves nor regresses this: each `CASRef` is one manifest CAS,
exactly as atomic as one SQL statement. Folding a multi-ref push into ONE
manifest CAS (which the single-object design could express) remains a
non-goal (plan §6) — it would change wire-visible semantics relative to
the SQL backends, and compatibility outranks the improvement.

## 7. Gate battery

All exit codes captured unpiped; see the Phase 1b PR for the run log:
`go build ./...`, `gofmt -l` (non-vendor), `go vet ./internal/ref/...`,
golangci-lint (CI pin), `go test ./internal/ref/ -count=1` (the gate),
`go test ./internal/ref/s3wal/ -count=1`, `go test -short ./internal/ref/...
-count=1`, full `go test ./... -count=1`, plus `-race` on the recovery
suite.

## 8. Test-fake extension

`MemCASHooks.AfterPut func(op, key string)` — invoked after every
conditional PUT attempt, OUTSIDE the store lock and regardless of outcome,
so recovery tests can interleave a competing writer between a faulted PUT
and the driver's recovery read (unlike `PutFault`, it may call back into
the MemCAS). This is the minimal seam that makes every recovery verdict
above deterministic; no production code carries test scaffolding.

## 9. Post-#132 addendum — scan-state, absence-cover, and reclamation surfaces

Commit `36be9e63` (rebased over #145's SeaweedFS swap, #132's
fleet-persistent scan state, and #154's read-deadline disarm) implements
the post-#132 store surface on `ref.S3WAL`:

- **`ScanStateStore`** (`s3walstore_scanstate.go`): `GetJobScanState`,
  `JobScanStateAge`, `PutJobScanState`, `DeleteJobScanState`,
  `ParkRepoJobScanState`, `ListJobScanStateScopes`, `BeginLFSReapEpoch`,
  `RecordLFSReapComplete`, plus `ArmAbsenceAdmissionFence` — one
  CAS-advanced side object per (job, scope).
- **Absence covers** (`s3walstore_absence.go`): `ListAbsenceCovers`,
  `AbsenceCoverHead`, `RenewAbsenceCovers`, `LapsedAbsenceCoverExists`,
  `CASAbsenceCoverOwner`, `DeleteAbsenceCover` — a fleet `_wal/repo/`
  prefix walk over the manifests' ordinary reservation entries, never a
  separate table.
- **Reclamation growth**: `RetireUnreferencedLFSObject` gains the
  `reapMaxAge` destructive-reap watermark gate, legacy certification, the
  epoch pin, and deferred-refund parking; `SettleLFSRefunds` lands;
  `ReservePackBytes` fails finite admissions closed under the armed
  absence admission fence; expired un-folded covers consumed by
  reservation GC (and `SweepExpiredPackReservations`) record lapse
  evidence; `PurgeRepoLedger` sweeps the base's settlement cursor and
  refund buckets after the manifest CAS.

Design decisions, in the same spirit as §3–§4:

- **Keys are `_wal/scan/<job>/<hex(scope)>`.** The plan's "path-escaped"
  sketch is deliberately replaced: `url.PathEscape` reorders `%` (0x25)
  against literal bytes, while lowercase hex is a strictly monotone,
  fixed-width (2:1), reversible injection — bucket listing order over
  encoded keys IS byte order over the unencoded scopes, which is exactly
  the `ORDER BY scope COLLATE "C"` contract `ListJobScanStateScopes`
  promises on the SQL backends. No `.json` extension, per the §2.1
  ordering argument.
- **Clock story.** The SQL backends stamp `updated_at` with the DATABASE
  clock so freshness verdicts never mix application clocks. Under the
  single-process decree every scan-state reader and writer shares ONE
  process clock, so "DB clock at both ends" collapses to the local clock
  honestly: stamps are ClkNext-monotone per object (a backward clock jump
  never regresses `updated_at`) and ages clamp at zero.
- **Conflict policy (plan §2.10).** Per-key in-process locks serialize
  same-process writers of one scan key; a CAS conflict that still reaches
  the store is dual-serve evidence — loud error, never retried. Ambiguous
  PUTs resolve via a fresh per-write writer nonce, which also makes the
  non-idempotent `BeginLFSReapEpoch` increment exactly-once.
- **Non-atomic splits, ordered fail-closed.** Retire deletes the registry
  row in one manifest CAS, THEN parks the refund in the per-epoch bucket
  (a crash between the two leaks a slot conservatively — capacity stays
  charged); settle drains buckets FIRST as side-object CASes, then applies
  the accumulated count/usage refund and adoption-generation bump in ONE
  manifest CAS (a crash between loses the drained refund, again
  conservative; the reverse order could refund twice). `reapEpochMu` and
  `absenceFenceMu` mirror PG's FOR SHARE epoch pin and the fleet-global
  fence advisory-lock pair within the one process.
- **RawKey spelling.** Manifests key reservations on PLAIN blob keys (the
  §4 divergence), but the absence-cover surface still emits the
  fleet-uniform decorated `RawKey` spelling and strips it at the boundary:
  RawKey is a persisted cursor and fold-CAS identity, so its observable
  shape must not depend on which backend minted it.

Conformance-matrix update: the 15 tests the post-#132 corpus added that
previously failed on the s3wal lane — the 12 `TestLFSReclaim_*` tests,
`TestJobScanState_RoundTrip`, `TestJobScanState_ScopesIsolated`, and
`TestAbsenceCovers_ListHeadDelete` — now pass, with **zero test changes
needed**; the pg/sqlite lanes are unchanged.
