# ADR-15: Cursor formats — keyset everywhere, tip-anchored for commits

> Extracted verbatim from [`12-phase3-implementation-notes.md`](../archive/phases/12-phase3-implementation-notes.md)
> (“Phase-3 Implementation Notes & ADRs (read API)”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

All cursors reuse the p1 ADR-6 wire format (unpadded base64url of a `|`-separated keyset).
Branches and tags key on ref name (lexical order), files on the last returned path, grep on
`commit|path|line`. Commit listing cannot keyset on commit date (not unique, not stable
under concurrent pushes), so its cursor is **tip-anchored**: the first page resolves the
requested branch to an anchor SHA and later pages replay `git log <anchor> --skip=<offset>`
(`gitread.Log` exposes `Skip`). Pages stay newest-first (invariant 9) and immutable even
while the branch advances, because the anchor never moves. Malformed cursors are a 400 at
decode time on every endpoint.
