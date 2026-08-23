# ADR-19: Hydrate serializes per pack with an flock; temp-download + atomic rename

> Extracted verbatim from [`12-phase3-implementation-notes.md`](../archive/phases/12-phase3-implementation-notes.md)
> (“Phase-3 Implementation Notes & ADRs (read API)”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

Read handlers hydrate per request, so the p2 hydrate cleanup race (serve process vs
re-exec'd hooks both downloading and `os.Remove`-ing pack/idx files) became hot. Each pack
is now materialized under a blocking exclusive flock on `<pack>.pack.lock`
(`internal/githttp/hydrate.go`): download to a uniquely-named temp file, `git index-pack`
the temp, rename into place atomically, remove only our own temp names, and treat
"already present after acquiring the lock" as success. Lock files are left in place —
they are tiny and re-locked on the next hydrate.
