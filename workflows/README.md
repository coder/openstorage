# workflows

## Overview

This directory holds one Xum (mux) durable workflow script, `build-phase.js`. It is an agent-orchestration conductor, not part of the Go service. No Go code, script, or CI job in the repository imports or runs it. An operator starts it by hand from a Xum workspace:

```js
workflow_run({
  script_path: "./workflows/build-phase.js",
  args: { phase: "p0-spike", isolated: true, brief: "optional operator guidance" },
});
```

One run builds one roadmap phase of the open.storage backend. The run spawns agents for each step and ends with a go/no-go gate report. The human gate is between runs: the operator reads the report and then launches the next phase. Step order of `workflow`, as named by its `phase(...)` calls:

1. `plan`
2. `slice-plan`
3. `implement-foundation` (only when the slice plan names a foundation)
4. `implement-slices`
5. `integrate-slices`
6. `verify`, then `fix-round-N` and `verify-round-N` until validation passes or the round limit is reached
7. `code-review`
8. `fix-review-findings` (only when P0 or P1 findings exist)
9. `dogfood`
10. `document`
11. `gate-report`

The `specRefs` strings in `[build-phase.js:PHASES]` cite the living docs [`docs/README.md`](../docs/README.md), [`docs/reference/http-api.md`](../docs/reference/http-api.md), [`docs/reference/authentication.md`](../docs/reference/authentication.md), [`docs/reference/webhooks.md`](../docs/reference/webhooks.md), [`docs/explanation/git-protocol-and-namespaces.md`](../docs/explanation/git-protocol-and-namespaces.md), [`docs/explanation/domain-model.md`](../docs/explanation/domain-model.md), and [`docs/explanation/streaming-commit-protocols.md`](../docs/explanation/streaming-commit-protocols.md). The `p0-spike` spec also cites the archive page `docs/archive/phases/07-implementation-roadmap.md` as historical context only. No `specRefs` string cites `docs/upstream/openapi.yaml`. That file appears in the module-private `[build-phase.js:SPEC_POINTER]` text that the `plan` prompt includes, in the `wire-conformance` lane of `[build-phase.js:REVIEW_LANES]`, and as `openapi.yaml` in one `p3-read-api` acceptance string.

## Interface

- `[build-phase.js:meta]` Exported workflow metadata: `description` and `argsSchema` (built with the global `mux.schema`, `additionalProperties: false`).
- `[build-phase.js:meta]` Run arguments (`meta.argsSchema`):

  | Arg | Type | Required | Notes |
  |---|---|---|---|
  | `phase` | enum | yes | `p0-spike`, `p1-skeleton-auth`, `p2-git-http`, `p3-read-api`, `p4-write-api`, `p5-eventing-sync`, `p6-lfs-polish`, `custom` |
  | `spec` | object | when `phase` is `custom` | Fields `id`, `title`, `specRefs`, `deliverables`, `acceptance` (string array, at least 1), `dogfood` |
  | `brief` | string | no | Appended as operator guidance to the plan, foundation, and slice prompts |
  | `maxFixRounds` | number | no | Default 2, clamped to 1..4 |
  | `isolated` | boolean | yes, no default | `true`: worktree-isolated children, patch fan-in. `false`: shared working tree |

- `[build-phase.js:PHASES]` Built-in phase specs. Each has `title`, `specRefs`, `deliverables`, `acceptance`, and `dogfood`:
  - `p0-spike`: de-risk spike (git push to S3, Postgres CAS, clone from a second node).
  - `p1-skeleton-auth`: service skeleton, JWT middleware, repo CRUD, error envelope.
  - `p2-git-http`: git smart HTTP, namespaces, ref-policy hooks.
  - `p3-read-api`: `git:read` endpoints (files, branches, tags, commits, diff, blame, grep, archive).
  - `p4-write-api`: NDJSON commit-pack and diff-commit, merge, branches, tags, notes.
  - `p5-eventing-sync`: webhooks, upstream mirrors, forking.
  - `p6-lfs-polish`: LFS batch API, named-repo routes, repo-urls, rate limits.
- `[build-phase.js:REVIEW_LANES]` Four parallel review lanes: `correctness-concurrency`, `wire-conformance`, `security`, `tests-and-slop`.
- `[build-phase.js:workflow]` Default export. Takes the runtime context `{ args, phase, log, agent, parallel, applyPatch }`. Returns `{ reportMarkdown, structuredOutput }`.
- `[build-phase.js:gateReport]` Builds the return value. `structuredOutput` has `phase`, `gate` (`"go"` or `"no-go"`), `reason`, `verifyPassed`, `dogfoodPassed`, `openBlocking` (the recheck agent's unresolved issue list), and `evidencePaths`.
- `[build-phase.js:sliceSchema]`, `[build-phase.js:verifySchema]`, `[build-phase.js:reviewSchema]`, `[build-phase.js:recheckSchema]`, `[build-phase.js:dogfoodSchema]` Module-private structured-output schemas for the slice, verify, review, recheck, and dogfood agents. Review severity is the enum `P0`, `P1`, `P2`, `P3`.

## Invariants

- `[build-phase.js:workflow]` The run throws when `phase` is `custom` and `args.spec` is missing.
- `[build-phase.js:workflow]` The run throws unless `args.isolated` is a boolean. The code comment records why: a missing value once dropped a whole phase's implementation (p6, 2026-08-12).
- `[build-phase.js:workflow]` `phaseId` is `args.spec.id` for `custom`, else `args.phase`. The code does not sanitize a custom `id` before it uses it in the evidence path.
- `[build-phase.js:workflow]` `maxFix` is `Math.max(1, Math.min(args.maxFixRounds ?? 2, 4))`. The verify loop runs rounds `0..maxFix`. It stops on the first pass or after the last round. A fix agent runs only between failed rounds.
- `[build-phase.js:workflow]` When validation still fails after the last round, the run returns a no-go gate report at once. Code review, dogfood, and document steps do not run.
- `[build-phase.js:sliceSchema]` The slice plan has 1 to 4 slices. `[build-phase.js:sanitizeName]` turns each slice name into an ID `s<N>-<name>` (lowercase, non-alphanumerics to `-`).
- `[build-phase.js:workflow]` Disjoint slice file ownership is a prompt rule only. The code does not check `ownedPaths` for overlap.
- `[build-phase.js:workflow]` Slice implementation runs with `maxParallel: 4` when `isolated` is `true`, else `maxParallel: 1` (sequential). Review lanes run with `maxParallel: 4`.
- `[build-phase.js:workflow]` Only P0 and P1 review issues go to the fix agent. P2 and P3 issues appear in the gate report but are not fixed.
- `[build-phase.js:workflow]` The gate is go only when `verify.passed && dogfood.passed && unresolvedBlocking === 0`. After review fixes, `verify` holds the `verify-after-findings` result.
- `[build-phase.js:workflow]` (`applyChildPatch`) Patch fan-in is a no-op when `isolated` is `false`. When `isolated` is `true`, a failed `applyPatch` is logged and the run continues.

## Side Effects

- `[build-phase.js:workflow]` Spawns agents with these IDs: `plan` (agent `plan`), `slice-plan` (agent `explore`), `implement-foundation` (conditional), `implement-slice-<id>` (one per slice), `integrate-slices`, `verify-<round>`, `fix-<round>`, `review-<lane>` (agent `explore`), `fix-findings`, `verify-after-findings`, `recheck-findings` (agent `explore`), `dogfood`, and `document`. `[build-phase.js:gateReport]` spawns `gate-report`.
- `[build-phase.js:workflow]` Writing agents (foundation, slices, integrate, fix, document) change repository files. When `isolated` is `false`, they write into the shared checkout. When `isolated` is `true`, they write in their own forks, and `applyPatch` applies their work with IDs `apply-foundation`, `apply-slice-<id>`, `apply-integrate`, `apply-fix-<round>`, `apply-fix-findings`, and `apply-document`.
- `[build-phase.js:commitRule]` When `isolated` is `true`, the prompt tells each writing agent to commit all changes on the current branch (`git add -A && git commit`) and never push. Uncommitted child work is lost at fan-in.
- `[build-phase.js:workflow]` When `isolated` is `false`, the slice prompt tells agents to stay on the current branch, never `git reset`, rebase, or switch branches, and keep scratch files in `/tmp`.
- `[build-phase.js:workflow]` The dogfood prompt tells the agent to bring up Docker Compose with Postgres and MinIO and to save evidence under `/tmp/openstorage-evidence/${phaseId}/`.
- `[build-phase.js:workflow]` The document prompt tells the agent to update living pages under `docs/` and the README quickstart (if setup changed), and never edit `docs/archive/`.
- `[build-phase.js:workflow]` Calls `log` with `"Slicing decision"` and `"patch not applied cleanly: <agentId>"`.

## Verification

- No automated check exists for this directory.
- The repository has no test, lint, or CI command for JavaScript files. `go test ./...` does not cover this directory. A real check is a `workflow_run` of the script in a Xum workspace.
