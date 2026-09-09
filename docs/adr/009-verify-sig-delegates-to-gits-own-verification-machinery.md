# ADR-9: verify-sig delegates to git's own verification machinery

> Extracted verbatim from [`10-phase2-implementation-notes.md`](../archive/phases/10-phase2-implementation-notes.md)
> (“Phase-2 Implementation Notes & ADRs”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

**Context.** docs/02 and the branch-protection guide require `verify-sig` to reject pushes
introducing commits without a valid signature from a registered key, for "the signature
formats Git supports" (SSH and OpenPGP). Reimplementing SSHSIG/OpenPGP verification in Go
is high-risk, low-value.

**Decision.** The update hook computes the introduced set (`git rev-list <new> ^<old>`;
branch creation checks everything reachable — the branch-protection contract compares
against the ref's own prior tip only, never other refs — and deletions introduce
nothing; [#59](https://github.com/coder/openstorage/issues/59) bounded the cost with a
per-push verdict cache and a fail-closed verification budget) and verifies
each commit by delegating to git inside the cache repo: a temp `allowed_signers` file
(`* namespaces="git" <ssh-key>` lines) plus a temp `GNUPGHOME` with the org's armored PGP
keys imported, then `git verify-commit <sha>` — git dispatches SSH vs OpenPGP by signature
type. Unsigned commits, signatures from unregistered keys, and an empty keyring all fail
closed, surfacing as per-ref `[remote rejected] ... (signature verification failed)`.

**Consequences.** Verification quality tracks git itself. On hosts without a `gpg` binary,
SSH verification still works (`ssh-keygen`); PGP-signed commits then fail closed with a
clear reason, and the GPG e2e test skips when `gpg` is absent.
