# Phase-4 Implementation Notes & ADRs (write API + p3 read burn-down)

Phase 4 delivers the `git:write` surface on `/api/v1` (docs/07 §4): NDJSON streaming
commit-pack and diff-commit, restore-commit, merge + read-only merge preview, branch
create/delete, tags, and notes — CAS everywhere, ref policies on every new write path, the
docs/04 result envelope on the documented status list — plus the p3 review burn-down on the
read API (P2 fixes below). Contract sources: docs/04-streaming-commit-protocols.md,
docs/03-http-api.md, docs/01-domain-model.md (invariants 1–6, 10),
docs/upstream/openapi.yaml. Deviations discovered this phase are recorded in the docs/12
deviation list (entries 4–7), keeping one authoritative list for conformance runs.

## ADR-21: Repos are keyed by `repo_id` everywhere; URL path elements resolve via meta

The p3 gate surfaced a repo-key split: `internal/githttp` keyed packs/refs/hook env by the
git URL path element verbatim while `internal/api` keyed by `RepoID`, so a push to
`/{name}.git` was accepted but invisible to every read endpoint. Decision: **`RepoID` (plus
the `+ephemeral`/`+import` namespace suffix) is the only storage key.** githttp resolves the
path element (after stripping the namespace suffix) through the metadata store — try
`GetRepoByID`, then `GetRepoByURL` (`meta.ResolveRepoKey`, shared with the API's claim
resolution) — and canonicalizes before any pack/ref/hook-env use.
Path elements that resolve to no repo record are a **404 on both surfaces**: repos must be
created via `POST /api/v1/repos` first (docs/05 addresses git URLs by `repo_id`; auth
already accepted both spellings).

## ADR-22: CAS mismatch = 412, lost race / merge conflict = 409

`expected_head_sha` / `expected_target_sha` / `expected_ref_sha` guard failures detected by
comparing against the current ref value return **412 `precondition_failed`**. A CAS lost at
publish time (no guard given, or the guard passed but the ref moved concurrently) and merge
conflicts return **409 `conflict`** (the merge body adds `conflict_paths` and
`merge_base_sha`). Root commits on an unborn branch report `old_sha` as 40 zeros
(invariant 2). Write successes are **200** (docs/03+04 win over openapi's 201; SDKs accept
both — docs/12 deviation 5).

## ADR-23: Write pipeline = per-request scratch odb over the cache, then publish

Every write materializes the cache repo read-only (alternates), creates new objects in a
per-request scratch object directory (`GIT_OBJECT_DIRECTORY` +
`GIT_ALTERNATE_OBJECT_DIRECTORIES`), and publishes in the same order as the push hook:
`git pack-objects` over the new-tip reachability frontier, `blob.UploadFile(PackKey(...))`,
`RegisterPacks`, `CASRef`. A lost CAS leaves only an unreferenced immutable pack — never a
dangling ref. Reachability-based packing also publishes ephemeral→persistent promotions on
merge for free (`promoted_commits`). Merge preview (invariant 6) runs entirely in the
scratch odb and deletes it per request, so preview can never mutate stores; the merge engine
is `git merge-tree --write-tree` (requires git ≥ 2.38, asserted at first use).

## ADR-24: NDJSON ingestion is incremental with hard per-frame caps

commit-pack/diff-commit bodies are parsed line by line (`bufio` reader, hard line cap sized
for 4 MiB raw chunks after base64 + JSON overhead) so chunked uploads without
`Content-Length` stream without body buffering. The first frame must be `metadata`;
per-file `blob_chunk` frames must be contiguous and end with `eof:true`; decoded chunks are
capped at 4 MiB. Chunk interleaving across files is rejected as `invalid` — the SDKs send
contiguous chunks and docs/04 implies a sequence, so the stricter read keeps the parser
single-pass. Malformed frames, unknown `content_id`s, duplicate/missing eof, and oversize
chunks map to `invalid` envelopes, never asserts (untrusted input).

## ADR-25: Ref policies enforced API-side through the hook's engine

The receive-hook policy core (`auth.ParseRefPolicies`/`FoldOps`/`MatchPolicy` + the
enforce loop) is shared with the write handlers, called with full ref names
(`refs/heads/…`, `refs/tags/…`, `refs/notes/…`): `no-push` blocks all writes/deletes,
`no-force-push` blocks non-fast-forward updates and deletes (is-ancestor callback),
`verify-sig` rejects unsigned server-created commits — parity with the push path
(invariant 5). Default-branch deletion is refused in the persistent namespace
(invariant 4) and exempted for `+ephemeral` (invariant 7).

## ADR-26: Result envelope statuses; 499 is not emittable

The write envelope implements the docs/04 status list (`ok, precondition_failed, conflict,
not_found, invalid, timeout, unauthorized, forbidden, unavailable, internal, failed`) with
its HTTP mapping table, including envelope-shaped 401/403 from the auth middleware on write
routes (recorded here as the contract; not yet wired — see the p4 review burn-down below).
`499 client closed request` appears in the docs/04 table but is an nginx-ism: Go's
`net/http` cannot emit a status for a connection the client already abandoned, so 499 is
recorded as non-emittable (docs/12 deviation 7). `POST /repos/restore-commit` takes plain
JSON per docs/03+04; openapi's NDJSON `reset-commits` alias is a recorded deviation
(docs/12 deviation 6).

## p3 review burn-down (read API)

Externally visible P2 fixes, each with regression coverage:

1. **`GET /repos/file` Content-Type.** openapi pins "Always `application/octet-stream`";
   the handler now sets it before `http.ServeContent`, which previously sniffed
   extension/content (`handlers_read_core.go`). Asserted on 200, 206, and HEAD.
2. **files/metadata date field.** openapi: "RFC 3339 **author** date"; the handler used the
   committer date. The core test fixture now pins distinct author/committer dates so the
   wiring stays assertable (commit listings keep the committer date — invariant 9).
3. **grep `paths` narrowing.** `body.Paths` were validated and post-filtered but never
   passed to git, so `git grep` always scanned the whole tree. They now become
   `:(literal)<prefix>` pathspecs — literal semantics match the documented "path prefixes"
   (no glob dialect, no pathspec-magic smuggling); the server-side filter remains the
   authoritative output shape.
4. **Invalid grep regex → 400.** git's exit-128 fatal for a bad pattern was a 500; with the
   commit pre-resolved and pathspecs literal-only, exit 128 now maps to
   `400 invalid query.pattern`.
5. **Archive resolves before streaming.** `handleArchive` committed the 200 before
   `git log`/`ls-tree` ran, turning resolution failures into mid-stream aborts.
   `gitread.PrepareArchive` (commit lookup + tree walk + filtering) now runs before headers;
   only blob streaming remains abortable (`PreparedArchive.Stream`). The archive `prefix`
   is normalized with a trailing slash ("snap" → "snap/") so bare folder names cannot glue
   onto entry names.
6. **Root-diff deletion assert removed.** `buildDiffPayload` asserted deletions never
   appear when the diff base is the empty tree; the state is git-output-derived, so it is
   now handled by `statDiffBlob`'s error path (worst case a per-request 500) instead of a
   process-killing panic.
7. **`gitread.Materialize` unit tests** over the zero-container local backends: hydrate
   from registered packs, DB-authority ref moves on re-materialization, unborn repos, and
   refs naming objects missing from every pack (fatal, not silently dropped).
8. **Malformed grep cursors** (wrong field count, non-hex/unknown commit, empty path, bad
   line numbers, positions not in the result set) are all pinned as 400s.

P3 nits closed in the same files: `PeeledTags` peels to **commit** objects only —
tags on trees/blobs are skipped, nested tags peel through (invariant 10);
`mergeBaseSHA`'s input assert became an error return; `queryFlag` was deduplicated into
`coreBoolParam`; the tar-slip `archive.prefix` validation is unchanged and still asserted
inside `gitread`. `CatBlobTo` already wrapped errors with `%w`; files/metadata's
`recursive` param stays accepted-and-ignored (openapi: "Accepted for API symmetry").

## Integration notes (fan-in)

- **API ephemeral keying follows the read surface, not the plan text.** The plan said the
  write endpoints' `ephemeral` flags "select the `+ephemeral` storage key" (githttp
  namespace parity), but the frozen p3 read surface stores ephemeral refs as
  `refs/namespaces/ephemeral/…` (`auth.EphemeralRefName`) under the **base RepoID key**.
  The write endpoints follow the read surface so API-created ephemeral refs are visible to
  `GET /repos/branches?ephemeral=true` and merge promotion works cross-endpoint. The git
  smart-HTTP `+ephemeral` URL namespace remains a fully separate storage key
  (`RepoID+ephemeral`), so ephemeral state written over git push and over the API live in
  different ref spaces — a known cross-surface split inherited from p2/p3.
- **`GET /repos/merge/preview` has no `allow_unrelated_histories` parameter** (per the
  upstream contract), so unrelated histories preview as if the flag were set; the actual
  `POST /repos/merge` still rejects them without the flag.
- **verify-sig on non-deletion API ref writes fails closed** (403): the API surface can
  neither produce signed commits nor verify third-party signatures (the hook's keyring
  verification is githttp-internal), so allowing the write would bypass the policy.
  Deletions and pure fast-forwards (no server-created commit) pass — hook parity (ADR-25).
- **The write-path policy helpers are deliberately not consolidated.** The commit
  pipeline's `wcCheckRefPolicy` and the ref/merge handlers' `wrefPolicyCheck` look like
  duplicates but encode different call-site contracts: commit-pack/diff-commit build on the
  current head, so their updates are fast-forward by construction (no old/new SHAs, no
  ancestry callback, writes the response itself); the ref/merge helper must handle
  creations, deletions, and non-fast-forward detection (zero-SHA conventions + is-ancestor
  callback). Folding both into an `internal/gitwrite` package now would re-churn ~3000
  validated lines for no behavior change — revisit if phase 5 adds another writer. The
  merge endpoints already reuse the commit pipeline's scratch/publish plumbing
  (`wcPublish`, scratch odb, `mergeBaseSHA`), so there is no third publish path.

## p4 review burn-down (write path)

The p4 review found seven externally visible P2s. Landed so far:

1. **Notes read endpoints** (`GET /repos/notes`, `GET /repos/notes/refs`) — the docs/03
   read surface the review flagged as missing entirely. Non-obvious choices
   (`internal/api/handlers_read_notes.go`, `gitread.Note`):
   - Notes refs are listed **from the ref authority** (`ListRefsWithMeta`, already in
     lexical order), not the materialized repo — same source and keyset-cursor shape as
     `GET /repos/branches`. `GET /repos/notes` also checks the notes-ref tip against the
     ref authority first, so an unborn notes ref 404s without materializing anything;
     `ref_sha` in the response is that authoritative tip.
   - `git notes show` exits 1 for an unborn notes ref, an unknown object, and an object
     without a note alike; `gitread.Note` collapses all three into `ErrNotFound` → 404
     (any other failure is internal). Notes are stored stripspace'd by `git notes
     add/append`, so the read echoes the posted text by trimming the one trailing newline.
   - Custom notes refs are always enabled on this server (the write surface accepts
     them), so upstream's "custom notes refs disabled" 400 has no equivalent. `sha` and
     `ref` reuse the write path's validation and normalization (`ref.ValidSHA`,
     `wrefNotesRefName`).

All items below are now closed (re-gate burn-down; regression tests apiece):

1. **Empty-pack publish 500s** — fixed: `wcPublish`/`wrefPublish` skip the blob upload
   for a zero-object `pack-objects` result and proceed to the ref CAS.
2. **Merge preview 500 on submodule conflicts** — fixed: `wmRenderConflicts` carries
   mode+oid from merge-tree records and never `cat-file blob`s gitlink (160000) stages.
3. **Merge preview cannot see ephemeral branches** — fixed: optional
   `source_ephemeral`/`target_ephemeral` query params (mirrors `handleMerge`'s body
   flags; the vendor SDK sends neither, so its behavior is unchanged).
4. **Note author CR/LF injection** — fixed: notes author fields go through the same
   `\n`/`\r` rejection as the commit-pack path.
5. **ADR-26's envelope-shaped 401/403 on write routes** — implemented:
   `requireAuthWrite` (all docs/04 write routes) emits `{"result": {success:false,
   status: unauthorized|forbidden, message}}` with `WWW-Authenticate: Bearer` on 401;
   read routes keep the `{"error"}` body per docs/02.
6. **`wrefIsAncestor` coverage** — added: fast-forward notes ops under `no-force-push`
   run the real ancestry check end-to-end; the non-fast-forward denial branch (an
   unreachable transition through the notes API) is unit-tested via `wrefPolicyCheck`
   with a stubbed ancestry function.

## Follow-ups

- Extend the vendor-SDK compat harness (`internal/api/sdkcompat_test.go` +
  `testdata/sdkharness`) to drive SDK write methods and assert envelope statuses
  round-trip — unblocked now that the write routes are wired into `api.New`.
- Produce the phase-4 gate evidence at this HEAD: the vendor `full-workflow.js` drive, a
  `just-code-storage` consumer session, and the README quickstart transcript (docs/07 §4).
- files-metadata per-path `git log -1` and the temp-file staging in `GET /repos/file`
  remain the known read-path perf follow-ups from p3 (ADR-17).
