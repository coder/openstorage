# ADR-32: Upstream credentials and webhook secrets are stored plaintext

> Extracted verbatim from [`14-phase5-implementation-notes.md`](../archive/phases/14-phase5-implementation-notes.md)
> (“Phase-5 Implementation Notes & ADRs (eventing, upstream sync, credentials, fork)”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

`sync.Credential.Password` (upstream git credentials) and `events.Webhook.Secret` (HMAC
signing keys) are stored as given. Decision: **at-rest secret encryption is out of scope
for phase 5.** Both secrets are *used* in cleartext on every consumption — basic-auth
material for upstream fetches, per-delivery HMAC signing — so reversible encryption
without a KMS/keystore seam would be obfuscation, not a security boundary, and this
self-host deployment has no such seam yet. Compensating rules that ARE enforced: secrets
are never logged (worker and delivery code paths keep them out of errors and slog
fields), webhook secrets are shown exactly once at registration (ADR-29), and credential
reads are scoped to the sync worker. Revisit when a KMS/keystore seam exists.

**Amended by [#54](https://github.com/coder/code-storage/issues/54):** the environment is
now that seam. Optional
[`CODESTORAGE_SECRETS_KEY`](../reference/configuration.md#codestorage_secrets_key)
envelope-encrypts both columns (AES-256-GCM, `enc:v1:` marker, comma-separated key list
with first-key-encrypts rotation) inside `internal/meta` — encrypt-on-write, fail-closed
decrypt-on-read, lazy re-encryption of legacy plaintext rows. Unset preserves this ADR's
plaintext behavior; the never-log and show-once rules stand.
