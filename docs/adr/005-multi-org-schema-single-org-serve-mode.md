# ADR-5: Multi-org schema, single-org serve mode

> Extracted verbatim from [`09-phase1-implementation-notes.md`](../archive/phases/09-phase1-implementation-notes.md)
> (“Phase-1 Implementation Notes & ADRs”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

**Context.** The vendor model is multi-tenant — JWTs are customer-signed and `iss` names
the org — but a self-hosted deployment has exactly one tenant. The auth path must stay
wire-identical either way.

**Decision.** Schema and verifier are fully multi-org (orgs, per-org public keys, `iss`
resolves the key set). `serve` provisions exactly one org at startup: `CODESTORAGE_ORG`
names it and `CODESTORAGE_ORG_PUBKEYS` supplies 1..n PEM public-key blocks (idempotent
upsert). `/api/v1` is mounted only when both are set; without them the binary serves the
phase-0 git path only.

**Consequences.** No single-tenant special cases in auth or the store: verification runs
the same `iss` → key set → algorithm-allow-list path a multi-tenant deployment would.
Growing to real multi-org later is provisioning work (org CRUD), not an auth change.
