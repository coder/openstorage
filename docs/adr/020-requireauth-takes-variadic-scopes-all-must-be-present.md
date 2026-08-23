# ADR-20: `requireAuth` takes variadic scopes; all must be present

> Extracted verbatim from [`12-phase3-implementation-notes.md`](../archive/phases/12-phase3-implementation-notes.md)
> (“Phase-3 Implementation Notes & ADRs (read API)”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

`requireAuth(verifier, allowExpired, scopes...)` now checks that **every** listed scope is
in the token (docs/02 quirk: `DELETE /repos/tags` sends both `git:read` and `git:write`).
All twelve read routes pass exactly `auth.ScopeGitRead`; 401 (missing/bad token) and 403
(scope or repo-claim mismatch) keep the docs/03 envelope, and `WWW-Authenticate` is emitted
on 401 only (RFC 9110 §11.6.1) on both the API and githttp surfaces.
