# ADR-6: Soft delete via a unique constraint spanning deleted rows

> Extracted verbatim from [`09-phase1-implementation-notes.md`](../archive/phases/09-phase1-implementation-notes.md)
> (“Phase-1 Implementation Notes & ADRs”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

**Context.** docs/01 requires deletion to be soft-first, and docs/03 maps re-creating an
existing url onto 409 — including while a soft-deleted row still holds the url.

**Decision.** `repos` keeps one named `UNIQUE(org_id, url)` that deliberately spans
soft-deleted rows, so `CreateRepo` gets its 409 (`ErrAlreadyExists`) from the constraint
for free, with no live-vs-deleted casing. Reads exclude deleted rows (`deleted_at IS
NULL`), backed by a partial keyset index on `(org_id, created_at DESC, repo_id DESC)`.

**Consequences.** A re-create can never silently resurrect or shadow a deleted repo. The
trade-off: a url stays occupied until its row is hard-deleted, so the async cleanup that
docs/01 prescribes after a soft delete is what will free names for reuse.
