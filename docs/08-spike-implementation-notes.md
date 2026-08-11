# Spike Implementation Notes & ADRs

Records the load-bearing decisions from the phase-0 de-risk spike (`cmd/codestorage`,
`internal/spike/`). The spike code itself is throwaway-quality; the decisions below are the
part meant to survive into the real backend. Acceptance is covered by container-based tests
(`internal/spike/spike_test.go`: real Postgres + MinIO via testcontainers, nodes as separate
OS processes).

## What the spike proves

1. `git push` lands refs in Postgres and pack bytes in S3 (MinIO).
2. A second node with an empty cache directory serves a correct `git clone` (statelessness).
3. Two concurrent pushes to the same ref: exactly one wins; the loser gets a per-ref `ng`.
4. A crash between S3 upload and PG commit leaves garbage, never a broken ref.
5. JWT ref policies (`no-push`, `no-force-push`, first-match-wins) are enforced per ref.

## ADR-1: Postgres is the ref authority

**Context.** Multiple stateless nodes must agree on refs. On-disk bare repos cannot provide
cross-node atomicity, and replicating git's own ref store adds a consensus problem the
metadata DB already solves.

**Decision.** Refs live in a PG table `(repo, name, sha)`. Every ref update is a
compare-and-swap executed from git's `update` hook as a **single SQL statement**
(`INSERT … ON CONFLICT DO NOTHING` for create, conditional `UPDATE`/`DELETE` otherwise).
Bare repos on disk are read-through caches only: `Hydrate` rebuilds them from PG + S3, and
`SyncRefs` forces the local ref store to match PG exactly (PG wins, local strays deleted).

**Consequences.** A partial or broken ref state is impossible by construction — refs only
ever change inside those single statements. Concurrent pushes resolve to one winner with
git-native per-ref error reporting (hook exit ≠ 0 → `ng`), and git only updates the cache
ref for winners. The cost: every advertisement must sync refs from PG first, and the local
git ref store must never be trusted as truth.

## ADR-2: S3 upload strictly before the ref flip

**Context.** Pack bytes and ref pointers must never disagree; a crash can happen between
any two steps of a push.

**Decision.** Fixed ordering per push: quarantine → S3 upload (`pre-receive`) → PG pack-row
commit in one transaction (`pre-receive`) → per-ref PG CAS (`update`). Refs are never
touched in `pre-receive`. Cache repos set `receive.unpackLimit=0` (every push yields a
pack) and `gc.auto=0` (packs stay immutable); S3 keys are content-addressed
(`packs/{repo}/pack-{hash}.pack`), so a retried identical push is idempotent.

**Consequences.** The two crash windows (validated by test knobs) are:

| Crash point | Result |
|---|---|
| after S3 upload, before PG commit | orphaned S3 object only |
| after PG commit, before ref CAS | registered-but-unreferenced pack |

Both are invisible garbage that a later sweep can collect; neither is user-visible
corruption. A ref can never point at objects that are not durably in S3.

## ADR-3: Namespace mapping via separate ref stores

**Context.** docs/05 requires that `+ephemeral` remotes see/update only the ephemeral ref
space and persistent remotes never see ephemeral refs, and explicitly allows either git
namespaces or separate ref stores.

**Decision.** The full namespaced name (`{repo}+ephemeral`, `{repo}+import`) is the storage
key *everywhere*: PG ref rows, S3 pack prefix, and cache directory. Authentication strips
the suffix — the JWT `repo` claim carries the base repo id and is valid for its namespaces.

**Consequences.** Isolation comes for free: there is no refspec-mapping layer and no way
for ephemeral refs to leak into a persistent advertisement. The trade-off is that
namespaces do not share objects, so ephemeral→persistent promotion is a copy, not O(1).
This is a spike-local choice; the roadmap's open item (ref-prefix namespace for O(1)
promotion) remains open for the real backend.

## Smaller notes

- **Hooks re-exec the binary.** `pre-receive`/`update` are two-line shell scripts running
  `codestorage hook <name>`; request context (repo, refs policy) flows via env vars set by
  the HTTP handler around `git receive-pack`. No shell/psql dependencies inside hooks.
- **Schema init takes a PG advisory lock.** Two nodes starting concurrently can race
  `CREATE TABLE IF NOT EXISTS` into a duplicate `pg_type` key error (SQLSTATE 23505);
  `pg_advisory_xact_lock` before the DDL serializes startup. Found via dogfooding.
- **Ref-sync failures are asymmetric.** Hydration ref-sync failure is fatal on the
  upload-pack path (a stale advertisement breaks clone correctness) but tolerated on the
  receive-pack path, where a concurrent receive may hold ref locks and git re-validates
  client old-SHAs itself.
- **No-pack pushes must be accepted.** Deletes and branch creates from existing commits may
  arrive without a quarantined pack; whether git sends an empty pack is version/client
  dependent. The hook asserts the target objects exist instead of rejecting.
- **Policy enforcement lives in the `update` hook**, first-match-wins over normalized
  patterns, evaluated before the CAS. Unknown ops and `verify-sig` fail closed.
