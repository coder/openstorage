# ADR-16: `refs.created_at` column for `branches[].created_at`

> Extracted verbatim from [`12-phase3-implementation-notes.md`](../archive/phases/12-phase3-implementation-notes.md)
> (“Phase-3 Implementation Notes & ADRs (read API)”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

openapi.yaml requires `branches[].created_at` ("ISO 8601 creation timestamp"). The `refs`
table (pg + sqlite) gained a `created_at` column set on first insert and **preserved on SHA
updates** (`ON CONFLICT ... DO UPDATE` keeps the original), surfaced through
`ListRefsWithMeta`. Schema change is an idempotent additive `ALTER TABLE` in `InitSchema` —
pre-release, so no migration framework; existing dev DBs upgrade in place.
