# ADR-10: Signing-key registry = meta table + CLI (dashboard stand-in)

> Extracted verbatim from [`10-phase2-implementation-notes.md`](../archive/phases/10-phase2-implementation-notes.md)
> (“Phase-2 Implementation Notes & ADRs”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

**Context.** Upstream manages commit-signing keys in its dashboard; no public REST
endpoint exists in the spec, so wire compatibility imposes nothing here.

**Decision.** The OSS equivalent is a metadata table plus an operator CLI:
`org_signing_keys` in `internal/meta` (`AddSigningKey` / `SigningKeysForOrg` /
`RemoveSigningKey`) and `openstorage signing-key add|list|remove --org <org>`, which talks
to Postgres directly via `OPENSTORAGE_PG_URL` — deliberately not an HTTP endpoint. Key
material is validated at add time (a single SSH public key line with an allow-listed type
whose base64 blob echoes the declared algorithm, or an ASCII-armored PGP public key
block); the hook trusts registry contents verbatim and reads the keyring **fresh from
Postgres on every push**, so add/remove takes effect immediately with no cache to
invalidate.
