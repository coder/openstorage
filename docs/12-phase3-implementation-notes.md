# Phase-3 Implementation Notes & ADRs (read API)

Phase 3 delivers every `git:read` endpoint on the `/api/v1` surface (docs/07 §3): file /
files / files-metadata, branches, tags, commits / commit, diff / branches-diff, blame, grep,
and archive — with cursor pagination everywhere, conditional/range file streaming, and the
docs/03 status-code contract enforced on both backend families (pg+s3 and sqlite+file)
through the `runBackends` fan-out. It also closes the p2 and local-backends carry-overs
(assertf-on-external-input, multi-scope auth, hydrate locking, `WWW-Authenticate` scoping,
CLI `--data-dir` derivation). Contract sources: docs/03-http-api.md, docs/01-domain-model.md
(invariants 2, 3, 9, 10), docs/02-authentication.md, docs/upstream/openapi.yaml.

## ADR-14: Read plumbing lives in `internal/gitread`, still `os/exec git`

All read-side plumbing (ls-tree, log, cat-file, diff, blame porcelain, grep, archive walk)
is a dedicated package of small wrappers over a repo path, each with a focused parser and
unit tests against real fixture repos built in `t.TempDir()`. API handlers stay thin:
`gitread.Materialize` runs `githttp.Hydrate` (which `SyncRefs`s the cache repo) before any
plumbing, so the ref store remains the single ref authority (ADR-1) and the bare-repo cache
is only ever a materialization. `ResolveRef` maps branch / tag / short- or full-SHA (and the
p2 ephemeral namespace) to a commit, with missing refs and unborn branches surfacing as a
typed not-found the handlers translate to the docs/03 404s (invariant 3). No go-git —
consistent with every prior phase.

## ADR-15: Cursor formats — keyset everywhere, tip-anchored for commits

All cursors reuse the p1 ADR-6 wire format (unpadded base64url of a `|`-separated keyset).
Branches and tags key on ref name (lexical order), files on the last returned path, grep on
`commit|path|line`. Commit listing cannot keyset on commit date (not unique, not stable
under concurrent pushes), so its cursor is **tip-anchored**: the first page resolves the
requested branch to an anchor SHA and later pages replay `git log <anchor> --skip=<offset>`
(`gitread.Log` exposes `Skip`). Pages stay newest-first (invariant 9) and immutable even
while the branch advances, because the anchor never moves. Malformed cursors are a 400 at
decode time on every endpoint.

## ADR-16: `refs.created_at` column for `branches[].created_at`

openapi.yaml requires `branches[].created_at` ("ISO 8601 creation timestamp"). The `refs`
table (pg + sqlite) gained a `created_at` column set on first insert and **preserved on SHA
updates** (`ON CONFLICT ... DO UPDATE` keeps the original), surfaced through
`ListRefsWithMeta`. Schema change is an idempotent additive `ALTER TABLE` in `InitSchema` —
pre-release, so no migration framework; existing dev DBs upgrade in place.

## ADR-17: File streaming = `http.ServeContent` over a request-scoped temp file

`GET/HEAD /repos/file` resolves the blob, streams it to a request-scoped temp file, and
hands it to `http.ServeContent`, which implements the RFC 9110 conditional/range matrix
(200/206/304/412/416, `If-Range` both arms, `Content-Range`, HEAD body suppression) once,
correctly, for free. Handlers pre-set the contract headers: strong `ETag` = quoted blob SHA,
`Last-Modified` = last commit touching the path, `X-Blob-Sha`, `X-Last-Commit-Sha`,
`Accept-Ranges: bytes`. Cost is O(blob) temp disk per request — accepted for p3 (the cache
dir is LRU/rebuildable anyway) and recorded as a perf follow-up alongside files-metadata's
per-path `git log -1` (O(page) subprocesses at ≤ page-size limits).

## ADR-18: Archive is a custom walk into `archive/tar` + `gzip`, not `git archive`

`git archive` cannot filter by blob size, so `POST /repos/archive` walks
`ls-tree -r --long`, applies include/exclude globs and `max_blob_size` in one code path, and
streams `cat-file blob` per entry into a tar.gz with the requested prefix
(`internal/gitread/archive.go`). Modes map 100755 → 0755, symlink entries come from blob
content, submodules are skipped. Response is `Content-Type: application/gzip` with
`Content-Disposition` and `Cache-Control: no-store`.

## ADR-19: Hydrate serializes per pack with an flock; temp-download + atomic rename

Read handlers hydrate per request, so the p2 hydrate cleanup race (serve process vs
re-exec'd hooks both downloading and `os.Remove`-ing pack/idx files) became hot. Each pack
is now materialized under a blocking exclusive flock on `<pack>.pack.lock`
(`internal/githttp/hydrate.go`): download to a uniquely-named temp file, `git index-pack`
the temp, rename into place atomically, remove only our own temp names, and treat
"already present after acquiring the lock" as success. Lock files are left in place —
they are tiny and re-locked on the next hydrate.

## ADR-20: `requireAuth` takes variadic scopes; all must be present

`requireAuth(verifier, allowExpired, scopes...)` now checks that **every** listed scope is
in the token (docs/02 quirk: `DELETE /repos/tags` sends both `git:read` and `git:write`).
All twelve read routes pass exactly `auth.ScopeGitRead`; 401 (missing/bad token) and 403
(scope or repo-claim mismatch) keep the docs/03 envelope, and `WWW-Authenticate` is emitted
on 401 only (RFC 9110 §11.6.1) on both the API and githttp surfaces.

## Carry-over closures (p2 gate + local-backends P2s)

- `assertf` on external input replaced with validation on the receive path
  (`internal/githttp/hook.go`: malformed receive-pack stdin, hook args, SHA shapes from
  subprocess output → stderr message + non-zero exit, never a panic) and in
  `internal/auth/policy.go` (empty ref → explicit no-match, not a crash). Asserts remain
  only for internally-produced impossible states.
- `signing-key` and `list-packs` honor `--data-dir` / `CODESTORAGE_DATA_DIR` via the same
  URL-derivation helper `serve` uses (`resolveStorageURLs`), so local mode works for all
  subcommands (verified live in the Lane-B transcript below).
- Relative paths can no longer silently split sqlite state between `serve` and re-exec'd
  hooks: the shared derivation makes the data dir absolute up front, and explicit relative
  `sqlite://` URLs are normalized rather than trusted as-is. The githttp e2e local lane pins
  the `--data-dir`-derived on-disk contract (one `codestorage.db`, plain-file packs under
  `blobs/`, rebuildable `cache/`).

## docs-vs-openapi deviations honored (docs/03 wins)

Recorded here so the schemathesis conformance run can be read correctly; checks that trip
solely on these are accepted:

1. **Error envelope.** docs/03 + docs/02:95 mandate `{"error":"<msg>"}` and clients branch
   on status codes only. openapi.yaml instead declares RFC 9457 problem-details (requires a
   `type` member) for 4xx responses. Schemathesis flags this as
   `Response violates schema: "type" is a required property` — accepted deviation.
2. **`ephemeral_base` on `GET /repos/file`.** Supported per docs/03; absent from the
   openapi parameter list for that path.
3. **501 during phased rollout.** openapi documents no 501, but routes are registered with
   501 stubs until their endpoint slice lands (`Undocumented HTTP status code` /
   `Server error` findings against a pre-integration build are the stub, not a conformance
   bug). At the integrated HEAD no read route answers 501.

## Acceptance evidence index

Evidence root: `/tmp/codestorage-evidence/p3-read-api/` (transcripts are **never
committed**; keys are runtime-generated). Layout:

| Path | What it shows |
|---|---|
| `full-stack/01-signing-key.txt` | `signing-key add/list` against live Postgres |
| `full-stack/02-seed-push.txt` | repo create + real `git push` seed (3 commits on main, diverged `feature`, annotated `v1.0.0` + lightweight `v0.9.0`, rename, exec file, symlink) |
| `full-stack/03-sdk-read-drive.txt` | vendor **Go SDK driver** exercising all read methods (ls / log / show / diff / branch-diff / blame / grep / file stream + conditional matrix / branches / tags / archive) |
| `full-stack/04-curl-conditional.txt` | raw-HTTP matrix: 401/403 triad, 200+`X-Blob-Sha`/`ETag`, `Range`→206, beyond-EOF→416, `If-None-Match`→304, stale `If-Match`→412, HEAD parity, unborn-branch 404, camelCase `GET /repos/diff?sha&baseSha&gitApplyCompatible` |
| `local/01-seed-push.txt`, `local/03-sdk-read-drive.txt`, `local/04-curl-conditional.txt` | the same seed + SDK drive + curl matrix against `serve --data-dir` (sqlite+file, **zero containers**) — backend-family parity |
| `local/02-signing-key-datadir.txt` | `signing-key --data-dir` derivation (local-backends P2 closure) live |
| `schemathesis/run.txt`, `schemathesis/rerun.sh` | schemathesis (Docker, `--network host`) run of the 12 `/api/v1` read paths from `docs/upstream/openapi.yaml` against the live server, scoped via `--include-path-regex`; deviations above pre-recorded |
| `sdk/main.go`, `sdk/go.mod` | the SDK driver source (`/tmp/p3-sdk-driver`, `replace` → `/tmp/sdk-copy/packages/code-storage-go`; vendor SDK itself never modified) |
| `p3-seed.sh`, `p3-curl-matrix.sh` | the reusable seed + matrix scripts the lanes ran |

Both lanes run the **same discovery-driven driver binary** (it learns branches, commits,
and paths from the server), so the identical-behavior acceptance dimension is checked by
construction: any divergence between the pg+s3 and sqlite+file stacks fails the same
assertion in both transcripts. The harness re-runs unmodified at any later HEAD:
Lane A = `docker compose -p <proj> up -d --wait` + `serve` with PG/S3 URLs; Lane B =
`serve --data-dir`; then `p3-seed.sh`, the driver, `p3-curl-matrix.sh`, and
`schemathesis/rerun.sh`.
