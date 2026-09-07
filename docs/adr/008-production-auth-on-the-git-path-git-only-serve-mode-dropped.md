# ADR-8: Production auth on the git path; git-only serve mode dropped

> Extracted verbatim from [`10-phase2-implementation-notes.md`](../archive/phases/10-phase2-implementation-notes.md)
> (“Phase-2 Implementation Notes & ADRs”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

**Context.** The spike verified git-path JWTs against a single ECDSA key with no `iss`
validation, while `/api/v1` already used `auth.NewVerifier` (multi-algorithm, org keys
resolved from the meta store).

**Decision.** The git server authenticates with the same `auth.Verifier` as the HTTP API,
keeping the docs/05 wire format (basic auth user `t`, password = JWT): verification
failures map to 401; a valid token whose `iss` is a *different* registered org, a repo
claim that does not resolve (repo_id → url via the meta store), or a missing scope
(`git:read` for upload-pack, `git:write` for receive-pack) map to 403. The phase-0
"git-only, single key" serve mode and `--jwt-pubkey` are dropped: `serve` requires
`OPENSTORAGE_ORG` + `OPENSTORAGE_ORG_PUBKEYS`. This is a breaking CLI change confined to
this repo. The CLI `keygen`/`mint` helpers moved off the spike's single-ECDSA code onto
`internal/auth` (`keygen --alg`, `mint --iss` required, algorithm inferred from the key).
