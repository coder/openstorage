# ADR-4: Frozen contracts first, one integration seam

> Extracted verbatim from [`09-phase1-implementation-notes.md`](../archive/phases/09-phase1-implementation-notes.md)
> (“Phase-1 Implementation Notes & ADRs”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

**Context.** Phase 1 was built as parallel slices (meta store, auth, API service, CLI
wiring) that could not see each other's code while in flight.

**Decision.** The `meta.Store` and `auth.Verifier` interfaces — including sentinel errors
mapping onto the docs/03 status contract and the docs/02 scope/claim constants — were
declared and frozen *before* implementation started. `cmd/openstorage/wire.go` is the
single integration seam: the only file referencing sibling-slice constructors. An opt-in
`openstorage_stub` (then `codestorage_stub`) build tag swapped in `wire_stub.go` so the CLI
could build and test before the slices landed; the stub was deleted once they did — it was
scaffolding for the parallel build, not a supported mode.

**Consequences.** The frozen interfaces held and the slices merged without cross-slice
edits. The one drift was on the *unfrozen* constructor surface — the API slice shipped
`api.New(api.Config{...})` where the CLI anticipated `api.NewHandler(store, verifier)` —
and was fixed in `wire.go` alone, as designed. Lesson: freeze constructor signatures along
with the interfaces.
