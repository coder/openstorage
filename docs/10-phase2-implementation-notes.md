# Phase-2 Implementation Notes & ADRs

Records the load-bearing decisions from phase 2 (git smart HTTP: promoting the phase-0
spike to the production git surface). The phase-0/1 decisions — PG as ref authority
(ADR-1), S3 upload before the ref flip (ADR-2), namespace mapping via separate ref stores
(ADR-3), frozen contracts + one integration seam (ADR-4), single-org serve mode (ADR-5),
soft delete (ADR-6) — carry into phase 2 unchanged. Acceptance stays container-based
(real `git` CLI + testcontainers) with live two-replica dogfood transcripts at the gate.

## ADR-7: Promote the spike, don't rewrite it

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

## ADR-8: Production auth on the git path; git-only serve mode dropped

**Context.** The spike verified git-path JWTs against a single ECDSA key with no `iss`
validation, while `/api/v1` already used `auth.NewVerifier` (multi-algorithm, org keys
resolved from the meta store).

**Decision.** The git server authenticates with the same `auth.Verifier` as the HTTP API,
keeping the docs/05 wire format (basic auth user `t`, password = JWT): verification
failures map to 401; a valid token whose `iss` is a *different* registered org, a repo
claim that does not resolve (repo_id → url via the meta store), or a missing scope
(`git:read` for upload-pack, `git:write` for receive-pack) map to 403. The phase-0
"git-only, single key" serve mode and `--jwt-pubkey` are dropped: `serve` requires
`CODESTORAGE_ORG` + `CODESTORAGE_ORG_PUBKEYS`. This is a breaking CLI change confined to
this repo. The CLI `keygen`/`mint` helpers moved off the spike's single-ECDSA code onto
`internal/auth` (`keygen --alg`, `mint --iss` required, algorithm inferred from the key).

## ADR-9: verify-sig delegates to git's own verification machinery

**Context.** docs/02 and the branch-protection guide require `verify-sig` to reject pushes
introducing commits without a valid signature from a registered key, for "the signature
formats Git supports" (SSH and OpenPGP). Reimplementing SSHSIG/OpenPGP verification in Go
is high-risk, low-value.

**Decision.** The update hook computes the introduced set (`git rev-list <new> ^<old>`;
branch creation checks everything reachable, deletions introduce nothing) and verifies
each commit by delegating to git inside the cache repo: a temp `allowed_signers` file
(`* namespaces="git" <ssh-key>` lines) plus a temp `GNUPGHOME` with the org's armored PGP
keys imported, then `git verify-commit <sha>` — git dispatches SSH vs OpenPGP by signature
type. Unsigned commits, signatures from unregistered keys, and an empty keyring all fail
closed, surfacing as per-ref `[remote rejected] ... (signature verification failed)`.

**Consequences.** Verification quality tracks git itself. On hosts without a `gpg` binary,
SSH verification still works (`ssh-keygen`); PGP-signed commits then fail closed with a
clear reason, and the GPG e2e test skips when `gpg` is absent.

## ADR-10: Signing-key registry = meta table + CLI (dashboard stand-in)

**Context.** Upstream manages commit-signing keys in its dashboard; no public REST
endpoint exists in the spec, so wire compatibility imposes nothing here.

**Decision.** The OSS equivalent is a metadata table plus an operator CLI:
`org_signing_keys` in `internal/meta` (`AddSigningKey` / `SigningKeysForOrg` /
`RemoveSigningKey`) and `codestorage signing-key add|list|remove --org <org>`, which talks
to Postgres directly via `CODESTORAGE_PG_URL` — deliberately not an HTTP endpoint. Key
material is validated at add time (a single SSH public key line with an allow-listed type
whose base64 blob echoes the declared algorithm, or an ASCII-armored PGP public key
block); the hook trusts registry contents verbatim and reads the keyring **fresh from
Postgres on every push**, so add/remove takes effect immediately with no cache to
invalidate.

## ADR-11: Ephemeral namespace policy quirk

**Context.** The branch-protection guide documents that ref policies written for
persistent branches do not automatically police ephemeral pushes.

**Decision.** When a push targets a `+ephemeral` repo, the hook matches policies against
`refs/namespaces/ephemeral/<refname>` instead of `<refname>`; a rule for `refs/heads/main`
therefore does not fire on an ephemeral `main` push unless the token uses `*` or an
explicit namespaced pattern. `+import` and persistent pushes match verbatim — the guide
documents the quirk for ephemeral only, so it is not extrapolated to `+import`. Pattern
syntax is tightened to the upstream contract at the same time: exact ref name, standalone
`*`, or a single trailing `*` after a `/` (prefix match); no mid-pattern globs. The
deprecated repo-wide `ops` claim is enforced as a fold-into-`*` fallback (`refs` and `ops`
are not additive), applied once at parse time.

## Backlog fixes landed in phase 2

- **`CODESTORAGE_BASE_URL`** — overrides the `http://<bound-addr>` base used for
  `http_url` in `POST /repos` responses (the derived base is unroutable when binding
  `0.0.0.0`). Validated at startup: http(s), host required, no query/fragment.
- **500 bodies sanitized** — `/api/v1` handlers log the underlying store error via
  `slog` (op, method, path) and return a generic `{"error":"internal error"}`; raw
  DB/driver strings no longer reach clients.
- **`statusRecorder.Unwrap()`** — the request-log wrapper exposes the underlying
  `http.ResponseWriter` so `http.ResponseController` flushing works through it; this is
  load-bearing for git smart HTTP streaming through the `api.New` fallback handler.
- **`UpsertOrgWithKeys` race** — the org row is locked (`SELECT ... FOR UPDATE`) before
  the key DELETE/INSERT so concurrent `PublicKeysForOrg` readers cannot observe an empty
  keyring mid-replace (spurious 401s under two-replica load).
- **CLI `keygen`/`mint` migration** — see ADR-8.

## Post-integration hardening

A review pass after the slices merged closed the phase; each item corrects a non-obvious
assumption made during the parallel build.

- **Cache repos are repaired on every hydrate, not only at init.** `EnsureCacheRepo`
  returned early once HEAD existed, so a crash between `git init` and hook installation
  (or a deleted hook file) left a repo whose receive-pack ran *without* the hooks that own
  S3 upload, PG-CAS, policy enforcement, and verify-sig. Config and hooks are now verified
  on every call — config via read-check-then-set (no `config.lock` contention in steady
  state), hooks via content/mode compare with temp-file+rename writes so a concurrent
  receive-pack never execs a partial script. The hook script embeds the binary path, so
  this also picks up redeployments.
- **`SyncRefs` runs deletions first, deterministically.** Updates-before-deletions broke
  when PG replaced `refs/heads/foo` with `refs/heads/foo/bar`: git's files backend rejects
  the directory/file conflict even within a single `update-ref --stdin` transaction.
  Deletions now run first in their own sorted invocation; partial application is safe
  because `SyncRefs` is idempotent over a rebuildable cache and callers retry.
- **Git endpoints reject empty and `"org"` repo claims.** A token minted with the
  org-level sentinel repo claim passed the git repo gate for a repo whose path element is
  literally `org` (`BaseRepo("org") == claims.Repo`). Git endpoints are always repo-scoped
  (docs/02), so both claims now map to 403, mirroring the API middleware's repo-scope gate.
- **Ephemeral policy matching normalizes shorthand refs.** The update hook built the
  ADR-11 match name by string concatenation (`refs/namespaces/ephemeral/` + refname),
  bypassing the docs/02 shorthand rule: a shorthand ref (`main`) became
  `refs/namespaces/ephemeral/main`, which `NormalizeRefName` leaves alone (leading
  `refs/`), so `refs/namespaces/ephemeral/refs/heads/*` rules never fired and enforcement
  could be bypassed. The hook now uses `auth.EphemeralRefName`, which normalizes before
  prefixing and is idempotent for fully-qualified refs. (Caught during the
  local-backends integration review.)
- **Ref policies apply to the API write endpoints.** docs/02 makes JWT ref policies bind
  every write surface, but the API handlers ignored `refs`/`ops`. Both write handlers now
  parse the policy claims exactly like the git update hook (including the fold-once `ops`
  fallback) and fail closed with 403 on malformed claims. Repo deletion destroys every ref
  and the API has no ref inventory, so any rule carrying an op that could deny a ref
  deletion (`no-push`, `no-force-push`, unknown ops) denies the whole-repo delete;
  `verify-sig` alone never does (deletions introduce no commits). Creating an empty repo
  touches no refs, so any well-formed policy allows it.

## Still deferred

- **S3 orphan GC**: packs uploaded by pushes that later lose the PG CAS race (or crash
  before the ref flip) remain in S3 unreferenced. Correctness is unaffected — S3 is
  written strictly before the ref flip (ADR-2) and unreferenced packs are never served —
  so garbage collection stays a cost concern for a later phase.
