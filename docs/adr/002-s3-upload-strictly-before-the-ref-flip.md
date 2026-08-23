# ADR-2: S3 upload strictly before the ref flip

> Extracted verbatim from [`08-spike-implementation-notes.md`](../archive/phases/08-spike-implementation-notes.md)
> (“Spike Implementation Notes & ADRs”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

**Context.** Pack bytes and ref pointers must never disagree; a crash can happen between
any two steps of a push.

**Decision.** Fixed ordering per push: quarantine → S3 upload (`pre-receive`) → PG pack-row
commit in one transaction (`pre-receive`) → per-ref PG CAS (`update`). Refs are never
touched in `pre-receive`. Cache repos set `receive.unpackLimit=0` (every push yields a
pack) and `gc.auto=0` (packs stay immutable); S3 keys are content-addressed
(`packs/{repo}/pack-{hash}.pack`), so a retried identical push is idempotent.

**Consequences.** The two crash windows (validated by test knobs) are:

| Crash point | Result |
|---|---|
| after S3 upload, before PG commit | orphaned S3 object only |
| after PG commit, before ref CAS | registered-but-unreferenced pack |

Both are invisible garbage that a later sweep can collect; neither is user-visible
corruption. A ref can never point at objects that are not durably in S3.
