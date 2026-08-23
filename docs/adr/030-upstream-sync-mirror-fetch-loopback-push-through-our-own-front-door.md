# ADR-30: Upstream sync = mirror fetch + loopback push through our own front door

> Extracted verbatim from [`14-phase5-implementation-notes.md`](../archive/phases/14-phase5-implementation-notes.md)
> (“Phase-5 Implementation Notes & ADRs (eventing, upstream sync, credentials, fork)”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

The sync worker fetches from the upstream provider and then performs a **real `git push`
against this server's own smart-HTTP endpoint** (the loopback), authenticated with a
short-lived minted `git:write` JWT passed via `GIT_ASKPASS` (never logged). Rationale:
the receive path already implements pack upload/registration, ref policies, CAS ref
flips, and push-event emission; a direct-to-store sync writer would be a second,
divergence-prone implementation of all four. The loopback reuses them by construction.
Overlap is prevented by `BeginSync`'s atomic claim (`ErrSyncInProgress` → silent skip,
invariant 11), and the run lifecycle emits the documented `repo.sync.started/succeeded/
failed` events with the exact docs/06 error strings.

Two consequences of the loopback design are enforced explicitly. The loopback push runs
with an internally minted token that carries no ref policies, so `handlePullUpstream`
checks the *caller's* policies before enqueuing — any restrictive policy op denies
pull-upstream fail-closed rather than letting the worker bypass it. And because
`upstream_host` becomes a fetch URL, `validateSyncBaseRepo` accepts only a bare
`hostname[:port]` or an http(s) base URL, and the worker pins `GIT_ALLOW_PROTOCOL`
(default `http:https`) so `file://`/`ssh://` and friends cannot be smuggled in.
