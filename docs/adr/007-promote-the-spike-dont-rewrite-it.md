# ADR-7: Promote the spike, don't rewrite it

> Extracted verbatim from [`10-phase2-implementation-notes.md`](../archive/phases/10-phase2-implementation-notes.md)
> (“Phase-2 Implementation Notes & ADRs”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

**Context.** The p0/p1 spike (`internal/spike`) already carried a working git path: smart
HTTP server, pre-receive/update hooks with PG-CAS + S3-before-ref-flip, cache hydration,
`+ephemeral`/`+import` routing, and a real-container e2e suite (two-replica cold-cache
clone, concurrent CAS, crash-after-S3 ordering).

**Decision.** Phase 2 is a promotion: `git mv internal/spike internal/githttp` in its own
mechanical commit (reviewable diffs), then harden in place. Runtime env vars the server
passes to hooks are renamed (`SPIKE_REPO` → `CODESTORAGE_GIT_REPO`, `SPIKE_REFS_POLICY` →
`CODESTORAGE_GIT_REFS_POLICY`). Test/fault-injection knobs become `CODESTORAGE_TEST_*` /
`X-Codestorage-Test-Crash` and are honored **only** when `CODESTORAGE_TEST_MODE=1` — the
server refuses the crash header otherwise; they exist for the crash-ordering and CAS e2e
tests, never for production use.

**Consequences.** Every spike acceptance test (push/PG/MinIO, two-replica cold-cache
clone, namespace isolation, force-push/deletion matrix, crash ordering) must stay green
through the refactor — the suite is the regression harness for the promotion itself.
