# ADR-26: Result envelope statuses; 499 is not emittable

> Extracted verbatim from [`13-phase4-implementation-notes.md`](../archive/phases/13-phase4-implementation-notes.md)
> (“Phase-4 Implementation Notes & ADRs (write API + p3 read burn-down)”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

The write envelope implements the docs/04 status list (`ok, precondition_failed, conflict,
not_found, invalid, timeout, unauthorized, forbidden, unavailable, internal, failed`) with
its HTTP mapping table, including envelope-shaped 401/403 from the auth middleware on write
routes (recorded here as the contract; not yet wired — see the p4 review burn-down below).
`499 client closed request` appears in the docs/04 table but is an nginx-ism: Go's
`net/http` cannot emit a status for a connection the client already abandoned, so 499 is
recorded as non-emittable (docs/12 deviation 7). `POST /repos/restore-commit` takes plain
JSON per docs/03+04; openapi's NDJSON `reset-commits` alias is a recorded deviation
(docs/12 deviation 6).
