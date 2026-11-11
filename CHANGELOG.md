# Changelog

## [0.20.0](https://github.com/coder/openstorage/compare/v0.19.0...v0.20.0) (2026-11-11)


### ⚠ BREAKING CHANGES

* **api:** `archive` answers 422 for a repository that holds a symlink whose target is longer than 4,095 bytes, before any response byte, and the body names the symlink. Before, a target of 4,096 bytes up to about 1 MiB was in the archive, and a longer one cut the stream after its headers. To get the archive, pass the symlink in `exclude_globs`, or fetch the target with git.
* **api:** `diff-commit` answers 400 `invalid` for a patch that gives a file a directory mode (`new file mode 040000`, or a mode change to `040000`). Before, such a patch answered 412 `no changes to commit`, 409 `conflict`, or 200 with a commit that lacked the file.
* **api:** `diff-commit` answers 400 `invalid` instead of 409 `conflict` for a patch that only git refuses (`.git./x`, `git~1/x`, a `.gitmodules` symlink, `new mode 100664`, a submodule patch without a commit or with a null commit). It answers 500 instead of 409 when `git apply --cached` fails for any other reason (a held index lock, a full disk, an unreadable index), and 500 instead of 400 when git fails while it parses the patch. A file name with a newline and a `fatal:` line makes git's message look like a server fault, so the client's own 400 or 409 then answers 500. A patch that makes git abort and a header without a file name answer 400 instead of 500.
* **api:** the REST API answers a token that fails verification (malformed, bad signature, expired or unknown issuer) with 403 `forbidden` instead of 401 `unauthorized`. SDK 0.1.0 raises `PermissionDeniedError` instead of `AuthenticationError` for such a token. SDK 1.17.1 reports status 403 instead of 401, for example `ApiError.status` in TypeScript. A request without a Bearer token still gets 401.
* **api:** a key store failure during REST token verification answers 403 `authentication_unavailable` instead of 401 (or instead of 403 `forbidden` for a fork source token; the Git and LFS transports keep 401; during a key store outage a caller with any well-formed token sees this 403 instead of 401), and unmatched `/api/` paths answer problem bodies (404 `route_not_found`, or 405 `method_not_allowed` with `Allow`) instead of ServeMux's plain text.
* **api:** merge failures no longer carry the `result` envelope. File conflicts answer 409 with upstream's `MergeConflict` body (`error`, `conflict_type`, `conflict_paths`, `merge_base_sha`), and every other merge failure answers the plain problem body. A non-fast-forward `ff_only` merge and a merge of unrelated histories answer 412 `precondition_failed` instead of 409. Note-write failures answer the plain problem body instead of the note envelope, except the 409 guard body. SDK 1.17.1 Go reads the note envelope only for `application/json` (`util.go:154`), so it now reports these note-write failures as a generic `APIError` instead of a `RefUpdateError`. I did not check the 1.17.1 TypeScript and Python clients.
* **api:** commit-pack, diff-commit and restore-commit answer the failures for which upstream documents ProblemDetails with the plain problem body instead of the result envelope: 403 (ref policy, quota), 404, 429 and 500 on all three, and 400 and 409 on restore-commit. Merge's repository-not-found 404 and its 500 also answer the plain problem body. The 404s carry `repository_not_found`, `branch_not_found` or `ref_not_found`. SDK 1.17.1 Go reports these restore-commit failures as `RefUpdateError` with status `404` (or the HTTP status) and a generic message, and builds the commit-pack and diff-commit `RefUpdateError` with status `failed`. I did not check the 1.17.1 TypeScript and Python clients.
* **api:** write routes (commit-pack, diff-commit, restore-commit, merge, notes, branches, tags) answer auth failures (401, 403), the REST rate limiter's 429 and the path-style alias 404 with the plain problem body instead of the result envelope, and so do the path-id git-credentials routes for every failure. SDK 1.17.1 Go builds the commit-pack, diff-commit and restore-commit `RefUpdateError` from that body with `Status` `failed` instead of `unauthorized`, `forbidden`, `not_found` or `unavailable`. I did not check the 1.17.1 TypeScript and Python clients.
* **api:** a stale `expected_target_sha` or `expected_head_sha` on `commit-pack`, `diff-commit` and `restore-commit`, and a stale `expected_notes_ref_sha` or `expected_ref_sha` on `POST .../notes` and the `DELETE .../notes` body, now answer `409` with `code: ref_guard_failed`, `guard`, `current_sha` and `result.status: conflict`, instead of `412` `precondition_failed`. Go SDK 1.17.1 (`code-storage-go`) callers now get a `RefUpdateError` with status `conflict` on the three commit routes. On a note create they get an `APIError` with status 409 whose message is the raw body, because that SDK reads note bodies only for `application/json`. Go SDK 0.1.0 (`github.com/pierrecomputer/sdk/go`) callers get a `RefGuardError` on every form.
* **api:** error bodies with problem members (`code`, `detail`, ...) are now sent as `Content-Type: application/problem+json`, as upstream documents. SDK 1.17.1 Go and TypeScript clients parse error JSON only for `application/json`, so their error message becomes the raw body text and the parsed body becomes a string. SDK 0.1.0 and the 1.17.1 Python client are not affected.

### Features

* **api:** base64 siblings for non-UTF-8 paths in files, diff, blame and grep responses ([#721](https://github.com/coder/openstorage/issues/721)) ([ccdd961](https://github.com/coder/openstorage/commit/ccdd961d5f4c563230342fe9a2a7527cd24f63e6))
* **api:** filter GET /repos by prefix (upstream spec 34ac6de1) ([#652](https://github.com/coder/openstorage/issues/652)) ([86e6fe7](https://github.com/coder/openstorage/commit/86e6fe7fe290287b5facaf146c781db53d09a9c0))
* **api:** send application/problem+json on problem bodies ([#660](https://github.com/coder/openstorage/issues/660)) ([3c6bfde](https://github.com/coder/openstorage/commit/3c6bfde36e3261aef45578985f732fc127914404))
* **api:** send problem bodies and MergeConflict for merge and note-write failures ([#670](https://github.com/coder/openstorage/issues/670)) ([3c4b2b5](https://github.com/coder/openstorage/commit/3c4b2b575e6cb281136d5ef8216e6ae6da6fad8b))
* **api:** send problem bodies for commit-write failures upstream documents as ProblemDetails ([#669](https://github.com/coder/openstorage/issues/669)) ([74c9684](https://github.com/coder/openstorage/commit/74c9684799fdbf9b9a86ecfe2e42c9df83c29f41))
* **api:** send problem bodies for write-route auth, rate-limit and alias failures ([#663](https://github.com/coder/openstorage/issues/663)) ([e0a48fb](https://github.com/coder/openstorage/commit/e0a48fbe4e2d4ce5a9e71fd63616b56c3ee8dabb))
* **api:** send route_not_found and authentication_unavailable ([#672](https://github.com/coder/openstorage/issues/672)) ([8638fa9](https://github.com/coder/openstorage/commit/8638fa95603bd5fb9f09853143b94a3ee56bd6f4))
* **api:** serve GET/POST /api/repos and the tag-in-path delete that SDK 0.1.0 calls ([#662](https://github.com/coder/openstorage/issues/662)) ([2176fc1](https://github.com/coder/openstorage/commit/2176fc1459ebe47871b317b92ee9e3d4d0097668))
* **ref:** write a scan-state row only when its generation is the expected one ([#698](https://github.com/coder/openstorage/issues/698)) ([1815795](https://github.com/coder/openstorage/commit/1815795056e5e9dc1cc2426aa360266f21ac7221))


### Bug Fixes

* **api:** answer 400 or 404, not 500, for a repo claim or key with a NUL byte ([#792](https://github.com/coder/openstorage/issues/792)) ([d5ce6f0](https://github.com/coder/openstorage/commit/d5ce6f038334941f17c9f28b0214262988938ae4))
* **api:** answer 404 or 400 for a git check only when git says no ([#760](https://github.com/coder/openstorage/issues/760)) ([b3add73](https://github.com/coder/openstorage/commit/b3add73a8c70ef2316a137b58c44a2c42f72355a))
* **api:** answer 404 or 400, not 500, for request text with a NUL byte or invalid UTF-8 ([#793](https://github.com/coder/openstorage/issues/793)) ([490caa8](https://github.com/coder/openstorage/commit/490caa8048226d376ef54a30bf6acfa9ed2d2edf))
* **api:** answer a git-credentials repo_id of "org" with 404 instead of an assertion ([#667](https://github.com/coder/openstorage/issues/667)) ([8ebc5ee](https://github.com/coder/openstorage/commit/8ebc5ee4272fb76408d19a9656671f81b78f006e))
* **api:** answer a token that fails verification with 403 forbidden ([#674](https://github.com/coder/openstorage/issues/674)) ([4e30915](https://github.com/coder/openstorage/commit/4e30915ceb0fe663f80cb0730721fdf28152c1a6))
* **api:** answer stale commit and notes guards with 409 ref_guard_failed ([#661](https://github.com/coder/openstorage/issues/661)) ([29688af](https://github.com/coder/openstorage/commit/29688af92328606dece78eb5a2bfad4cd26d0f4b))
* **api:** cap diff-commit's patch, and read git apply's stderr as it streams ([#778](https://github.com/coder/openstorage/issues/778)) ([797dcd5](https://github.com/coder/openstorage/commit/797dcd55013a475e6514105c11904dd56a377871))
* **api:** clip long client values in git error names and log lines ([#757](https://github.com/coder/openstorage/issues/757)) ([44df4a4](https://github.com/coder/openstorage/commit/44df4a40ad7a8fe9ad9f647cc0dfc8ec616c1c88))
* **api:** preview merge conflicts at paths longer than one git argument ([#739](https://github.com/coder/openstorage/issues/739)) ([a8b0e34](https://github.com/coder/openstorage/commit/a8b0e34d45d5a4e88e1b88c08a90974360b352cd))
* **api:** put three rare diff-commit patch failures in the right status ([#686](https://github.com/coder/openstorage/issues/686)) ([a439727](https://github.com/coder/openstorage/commit/a439727489af6cb8394470b073ae7953ae7e1cf6))
* **api:** refuse a diff-commit patch that gives a file a directory mode ([#722](https://github.com/coder/openstorage/issues/722)) ([c94f17a](https://github.com/coder/openstorage/commit/c94f17a7529b060106b003c95ff198c80a2772ff))
* **api:** refuse an archive that holds a symlink target over 4095 bytes ([#756](https://github.com/coder/openstorage/issues/756)) ([77efbe3](https://github.com/coder/openstorage/commit/77efbe3ac195b21698d0267f827a1c23c9f7c324))
* **api:** refuse an ephemeral branch named HEAD ([#797](https://github.com/coder/openstorage/issues/797)) ([1331621](https://github.com/coder/openstorage/commit/1331621c9c9deb9614dcca82a46ca644d0bbbff3))
* **api:** restore only a commit, and answer 500 for one git cannot read ([#771](https://github.com/coder/openstorage/issues/771)) ([94f4341](https://github.com/coder/openstorage/commit/94f43418582b88244e671a28a328593b66d0cf42))
* **api:** send target_branch and every RefUpdateResult member in commit-write results ([#668](https://github.com/coder/openstorage/issues/668)) ([50b4071](https://github.com/coder/openstorage/commit/50b4071e157ccaa0a2bc5ccc4aaaa4bc4a903118))
* **auth:** accept any non-empty sub and treat a token without repo as org-wide ([#666](https://github.com/coder/openstorage/issues/666)) ([137ba2c](https://github.com/coder/openstorage/commit/137ba2cc4eff12dc4a700ff1887b8012efa6cca6))
* **blob:** list file-store keys in key order and add ListPagesAfter ([#697](https://github.com/coder/openstorage/issues/697)) ([093c231](https://github.com/coder/openstorage/commit/093c2317bef3006d1f064141f5543cc69a0df36e))
* **deps:** bump github.com/jackc/pgx/v5 to v5.12.0 ([#710](https://github.com/coder/openstorage/issues/710)) ([caa296e](https://github.com/coder/openstorage/commit/caa296e617b917972fc2d9cbb9404e3ee5f6c04d))
* **deps:** bump River to v0.51.1 ([#720](https://github.com/coder/openstorage/issues/720)) ([59cf665](https://github.com/coder/openstorage/commit/59cf6659fea79dd81721c15d16620955aeb7a4a7))
* **githttp:** keep the hook's store errors off the pusher's screen and give them to the operator ([#742](https://github.com/coder/openstorage/issues/742)) ([9cdb2fa](https://github.com/coder/openstorage/commit/9cdb2fab4f5a90a4e5f7a9a87db0f0fe8aa4f881))
* **githttp:** report a signature check that ends without a verdict as a failure ([#765](https://github.com/coder/openstorage/issues/765)) ([9c7aa98](https://github.com/coder/openstorage/commit/9c7aa98ab3ff7d14405867ddca49f3073023ba21))
* **githttp:** show the pusher only refusals from a hook ([#755](https://github.com/coder/openstorage/issues/755)) ([d9095f3](https://github.com/coder/openstorage/commit/d9095f3aef90b1e706576fbdd30b4d30d35d9c39))
* **gitlimit:** keep only the tail of git's stderr in git errors ([#759](https://github.com/coder/openstorage/issues/759)) ([6d4b1bb](https://github.com/coder/openstorage/commit/6d4b1bb7c9a80761f113146b51a8be75ac3afb55))
* **gitlimit:** signal only process groups whose leader is unreaped ([#696](https://github.com/coder/openstorage/issues/696)) ([50f97fd](https://github.com/coder/openstorage/commit/50f97fd30bae26c8eb4626d8b9ce49f2fcbb276e))
* **gitread:** answer 500, not 404, for a ref whose commit git cannot read ([#782](https://github.com/coder/openstorage/issues/782)) ([e1bfbbf](https://github.com/coder/openstorage/commit/e1bfbbfae884d0a79545e351a92198d34df6d5f1))
* **gitread:** answer a diff with a type change ([#784](https://github.com/coder/openstorage/issues/784)) ([4a505d0](https://github.com/coder/openstorage/commit/4a505d085b9ac5eff7dd45c26ab1836e562cfd9e))
* **gitread:** decline a tree walk whose cat-file ended before the request ([#774](https://github.com/coder/openstorage/issues/774)) ([3c4337d](https://github.com/coder/openstorage/commit/3c4337d6c9f54be5bb352f14f6e1eda9824e97e5))
* **gitread:** report a canceled request as a cancellation, not a git failure ([#787](https://github.com/coder/openstorage/issues/787)) ([171a4cc](https://github.com/coder/openstorage/commit/171a4cc7bed417ef6388b71dda6b329d1c3e5a89))
* **gitread:** report a failed git call in ResolveRef, never a missing ref ([#749](https://github.com/coder/openstorage/issues/749)) ([c4ce89f](https://github.com/coder/openstorage/commit/c4ce89faf3d4bfe0593f24e31a4dcf42ba5a717f))
* **gitread:** run blame and grep without the repository's attributes ([#781](https://github.com/coder/openstorage/issues/781)) ([5326d70](https://github.com/coder/openstorage/commit/5326d70db35d97a58b00df5e277a5ff37db8c29b))
* **image:** bump alpine runtime base to 3.24.4 (apk-tools 3.0.9 security fix) ([#711](https://github.com/coder/openstorage/issues/711)) ([2b8f86b](https://github.com/coder/openstorage/commit/2b8f86be9e1c680ed590565ca533c8e5d6f52335))
* **jobs:** give the orphan sweep and LFS reclaim their own River queues ([#691](https://github.com/coder/openstorage/issues/691)) ([5279b5b](https://github.com/coder/openstorage/commit/5279b5b5aa5fdee7fc5435053f86f61ca04001d6))
* **jobs:** give the repository purge its own budget in a sweep job ([#692](https://github.com/coder/openstorage/issues/692)) ([96a88ad](https://github.com/coder/openstorage/commit/96a88ad3d60c6e6c6b0b584d3c6f145d5212fbf7))
* **jobs:** never complete a sync job while its sync claim is held ([#716](https://github.com/coder/openstorage/issues/716)) ([d42b6c7](https://github.com/coder/openstorage/commit/d42b6c79b7c87c4fc3f337948ca37cc733fbbc94))
* **jobs:** requeue PostgreSQL lanes whose durable work has no River job ([#726](https://github.com/coder/openstorage/issues/726)) ([2c1c74f](https://github.com/coder/openstorage/commit/2c1c74fe20c44122607101527e0b1eb60b05d270))
* **jobs:** return the sweep job shortly after its context ends ([#730](https://github.com/coder/openstorage/issues/730)) ([bcb2444](https://github.com/coder/openstorage/commit/bcb2444df17dedb2b1417053353092a0a315698d))
* **jobs:** run each quarantine heal entry under its own deadline ([#693](https://github.com/coder/openstorage/issues/693)) ([570f2ae](https://github.com/coder/openstorage/commit/570f2ae5a42925894b8f01720b8136b4fb89652c))
* **jobs:** warn when a reclaim run is skipped and pin the guard's reset and order ([#731](https://github.com/coder/openstorage/issues/731)) ([cb51ae2](https://github.com/coder/openstorage/commit/cb51ae28db29507c91e9f0be2ca5f4067ff98942))
* keep only the tail of git's stderr in every git runner's error ([#776](https://github.com/coder/openstorage/issues/776)) ([50df23a](https://github.com/coder/openstorage/commit/50df23a2d2020b2e66e2a662938a1ade75572716))
* list repos for a q or prefix of any length, and refuse NUL and invalid UTF-8 ([#783](https://github.com/coder/openstorage/issues/783)) ([e6be273](https://github.com/coder/openstorage/commit/e6be2732f8aa577043f46a494ff3f6514c7bbeec))
* **ref:** make ReplacePacks honor the purge fence ([#704](https://github.com/coder/openstorage/issues/704)) ([d7896ef](https://github.com/coder/openstorage/commit/d7896ef4488ced6c2238f100f4ecea38cfb0a687))
* refuse read paths outside the repository, and read blame and grep failures right ([#772](https://github.com/coder/openstorage/issues/772)) ([85b7d83](https://github.com/coder/openstorage/commit/85b7d836eb9b65879c4faa681b924e850ae21362))
* run every git child without replacement objects ([#777](https://github.com/coder/openstorage/issues/777)) ([f6a8329](https://github.com/coder/openstorage/commit/f6a83290bbfe0cf0559f97b00a53cfd204d38405))
* **s3wal:** log the quarantine cause once and tighten the meta.json fuzz harness ([#682](https://github.com/coder/openstorage/issues/682)) ([1a93067](https://github.com/coder/openstorage/commit/1a93067ba8465a8f37001a5d937b48ada51a9739))
* **scripts:** keep tab stops when a Markdown container prefix is dropped ([#751](https://github.com/coder/openstorage/issues/751)) ([2c07fca](https://github.com/coder/openstorage/commit/2c07fca2e420c4e7c4423084318ed030db1f1ca7))
* **scripts:** lex JavaScript in the citation checker and stop where it is unsure ([#723](https://github.com/coder/openstorage/issues/723)) ([aa48d35](https://github.com/coder/openstorage/commit/aa48d3583594a16d1e80f8d7f1e26a986d437c1e))
* **scripts:** read line ends, index flags and list items as their tools do ([#738](https://github.com/coder/openstorage/issues/738)) ([0b3a1e2](https://github.com/coder/openstorage/commit/0b3a1e2003594a6a27736fe228ee5db6dfd28d06))
* **serve:** refuse an intent-recording ref update when the hook cannot wire its eventing ([#724](https://github.com/coder/openstorage/issues/724)) ([03cc9f2](https://github.com/coder/openstorage/commit/03cc9f236c96780532cfd79b5a670e66ae452d7d))
* **sync:** hold the ref and record forward intent before a REST write's CAS ([#737](https://github.com/coder/openstorage/issues/737)) ([dc0dcfe](https://github.com/coder/openstorage/commit/dc0dcfe68dc67afddfd3c3f2e854da4826e2b6a0))
* **sync:** never lose a PostgreSQL forward between the ref CAS and its push run ([#679](https://github.com/coder/openstorage/issues/679)) ([faea6e1](https://github.com/coder/openstorage/commit/faea6e1b5f1637895970679875447f17f2c8882d))
* **sync:** record git forward intent before the CAS and refuse stale held snapshots ([#678](https://github.com/coder/openstorage/issues/678)) ([1996cc3](https://github.com/coder/openstorage/commit/1996cc32f37800afe260e2b20e6d856c67643cf4))
* **sync:** refuse stale held snapshots on PostgreSQL and restore River push runs ([#762](https://github.com/coder/openstorage/issues/762)) ([a0635de](https://github.com/coder/openstorage/commit/a0635de0a9c3dbc1f46ede8f1832e867f921f1fe))
* **sync:** run the worker's git without replacement objects ([#779](https://github.com/coder/openstorage/issues/779)) ([ffd0e9f](https://github.com/coder/openstorage/commit/ffd0e9f46922e1a90c7d007f06de038503ebeb28))


### Performance Improvements

* **api:** resume a recursive files page at its cursor ([#715](https://github.com/coder/openstorage/issues/715)) ([3ad97da](https://github.com/coder/openstorage/commit/3ad97da3ee5e152425391c1b2327e667c9479b04))
* **gitread:** join a git call's output once ([#750](https://github.com/coder/openstorage/issues/750)) ([df18c1b](https://github.com/coder/openstorage/commit/df18c1b9e9f5d12de90624700b9eeac6aabc2fc9))
* **meta:** lower a repo list filter once per query on PostgreSQL ([#785](https://github.com/coder/openstorage/issues/785)) ([3ceb600](https://github.com/coder/openstorage/commit/3ceb600e83b8a91d7d0e13a4aa90d66d423a552c))

## [0.19.0](https://github.com/coder/openstorage/compare/v0.18.0...v0.19.0) (2026-10-30)


### ⚠ BREAKING CHANGES

* **api:** a diff-commit patch that does not apply now gets 409 conflict instead of 422 invalid, and a diff payload that is not a valid patch or that names an invalid path now gets 400 invalid instead of 422.
* **api:** `GET .../files` without `recursive` lists every file in the tree instead of the direct children. It answers 422 when that listing passes the 64 MiB git output budget. Send `recursive=false` to get the direct children.
* **api:** when another writer moves the branch after the server wrote the commit, commit-pack and diff-commit now answer 409 with result.status failed instead of conflict, and the body carries the written commit in commit and result.new_sha.
* **api:** GitHub sync creates during a rolling upgrade. Only an upgraded worker adopts the GitHub default branch for a sync create that omits default_branch. A sync that an older worker runs leaves the repo on main until the next sync by an upgraded worker. An older replica's PATCH /repo to main before that sync does not stop the adoption, so the repo still moves to the GitHub branch. Upgrade every replica before you send GitHub sync creates that omit default_branch, or send default_branch explicitly. Tracked in #609.
* **api:** a commit-pack or diff-commit request that changes nothing now gets 412 precondition_failed "no changes to commit" instead of an empty commit. A commit-pack or diff-commit stream that ends before its eof chunk now gets 412 instead of 400.
* **api:** GET /api/v1/repos/files/metadata and GET /api/repos/{repo_name}/files/metadata now send only the subject line of each commit message in the commits map (the first paragraph as one line, the same as git's %s). Clients that need the body or the trailers must read GET /api/v1/repos/commit.
* **api:** GET /api/v1/repos/commits and GET /api/repos/{repo_name}/commits now send only the subject line of each commit message (the first paragraph as one line, the same as git's %s). Clients that need the body or the trailers must read GET /api/v1/repos/commit.
* **api:** `files` and `files/metadata` calls without `limit` and `cursor` return every entry instead of one page, and `files/metadata` answers 429 when the files under `path` have more than 50,000 distinct last commits.
* **api:** a failed merge guard on POST /api/v1/repos/merge and POST /api/repos/{repo_name}/merge now answers code ref_guard_failed instead of conflict, with guard and current_sha members. A merge with expected_source_sha whose source branch does not exist answers 409 ref_guard_failed with current_sha 40 zeros instead of 404. Clients that branch on code conflict for a merge guard must check ref_guard_failed.
* **api:** a stale expected_target_sha on POST /api/v1/repos/merge and POST /api/repos/{repo_name}/merge now answers 409 with a problem body, code conflict, instead of 412 with result.status precondition_failed. Raw HTTP callers that check for 412 must check for 409 and the conflict code. SDK callers keep the same error type.
* **api:** `POST /repos` now answers 200 with the existing repo instead of 409 for a plain, copy or fork create of a name that a finished repo without an upstream holds. A repeat copy or fork answers before it reads its source.
* **api:** `POST /repos` now answers 503 with `Retry-After` instead of 409 for a plain, copy or fork create of a name whose fork or copy create has not finished, including one that crashed or whose last step failed. A sync create pending after its upstream link was removed gets the same answer. `DELETE /repos/delete` clears such a name.
* **s3wal:** S3WAL only, PostgreSQL and SQLite are not affected. An S3WAL bucket written by v0.16.0 through v0.18.0 must be converted before this release serves it. Stop `serve` and wait for it to exit, take a backup, replace the binary, run `openstorage s3wal-meta convert --to 2`, then start `serve`. Upgrade an older bucket to v0.18.0 first. To go back, stop `serve`, take a backup, and run `openstorage s3wal-meta convert --to 1` with this release before starting the older release. See docs/how-to/deploy-production.md#upgrading-the-s3wal-metadata-format.
* **api:** `POST /repos` with `upstream` or a `base_repo` sync, for a name that a repo with the same upstream and the same `forward_pushes` holds, now answers 200 with the existing repo instead of 409. A repeat create of a pending sync repo by a token with only `repo:write` now answers 403 instead of 409.

### Features

* **api:** a fork or copy is pending until its ref write ([#450](https://github.com/coder/openstorage/issues/450)) ([49bc1ac](https://github.com/coder/openstorage/commit/49bc1acd7cdb1579fa34e76e09b741673f20649d))
* **api:** a repeat create of a finished repo answers 200 ([#453](https://github.com/coder/openstorage/issues/453)) ([295de82](https://github.com/coder/openstorage/commit/295de82d0eb8ec9da1f30dc6ce230bdb300ac910))
* **api:** a repeat sync create of the same upstream answers 200 ([#438](https://github.com/coder/openstorage/issues/438)) ([0a4994f](https://github.com/coder/openstorage/commit/0a4994f2b6f92a34cc94af4304b572edbfc48917))
* **api:** answer a stale merge expected_target_sha with 409 conflict ([#536](https://github.com/coder/openstorage/issues/536)) ([198cc45](https://github.com/coder/openstorage/commit/198cc4554547f63247f35eef94e65f7478df07f4))
* **api:** answer failed merge guards with 409 ref_guard_failed ([#571](https://github.com/coder/openstorage/issues/571)) ([d7c1eb8](https://github.com/coder/openstorage/commit/d7c1eb806b70957a0544701c9f7f731e7175d559))
* **api:** honor expected_source_sha on merge ([#535](https://github.com/coder/openstorage/issues/535)) ([0341af9](https://github.com/coder/openstorage/commit/0341af91c465ffb3e979929846a0996cd95adfd6))
* **api:** name invalid fields from the single-field body validators ([#443](https://github.com/coder/openstorage/issues/443)) ([e1beeeb](https://github.com/coder/openstorage/commit/e1beeeb81f8d291bb5d2f35fbc6574621dd136c3))
* **api:** name the invalid field in invalid_params on body decode failures ([#436](https://github.com/coder/openstorage/issues/436)) ([66f5c32](https://github.com/coder/openstorage/commit/66f5c3293c17e8ef5a5bc1e29e0d6f7580347866))
* **api:** send a Deprecation header on the deprecated pull-upstream and base routes ([#460](https://github.com/coder/openstorage/issues/460)) ([ea43a01](https://github.com/coder/openstorage/commit/ea43a01c041a9f7545fa8da4c9115755a5beafe4))
* **api:** send only the subject line in commit list entries ([#575](https://github.com/coder/openstorage/issues/575)) ([f0c975a](https://github.com/coder/openstorage/commit/f0c975ad87dadee2ee76873dc94c0432ba3755d4))
* **api:** send only the subject line in files/metadata commits ([#579](https://github.com/coder/openstorage/issues/579)) ([5e9bbe6](https://github.com/coder/openstorage/commit/5e9bbe655c5233ea65cead4663854f1cd1625935))
* **meta:** add the ADR-50 schedule transitions for S3WAL metadata v2 ([#448](https://github.com/coder/openstorage/issues/448)) ([27b24bd](https://github.com/coder/openstorage/commit/27b24bd35caf2cba631c68c579b9f7e0ca091bb0))
* **meta:** add the S3WAL metadata v1/v2 conversion (ADR-51) ([#441](https://github.com/coder/openstorage/issues/441)) ([ba8365a](https://github.com/coder/openstorage/commit/ba8365a78e420a088deb53440d2d58750345df98))
* **meta:** complete the S3WAL local schedule ([#449](https://github.com/coder/openstorage/issues/449)) ([ae6e3a0](https://github.com/coder/openstorage/commit/ae6e3a03b0271ec312ca9dcefdd3e27361e634ea))
* **s3wal:** repair or refuse unfinished creates on convert --to 1 ([#489](https://github.com/coder/openstorage/issues/489)) ([5b06087](https://github.com/coder/openstorage/commit/5b0608718a1fe2b123af47d838cc868ca745f308))
* **s3wal:** run S3WAL upstream syncs on the durable local schedule ([#451](https://github.com/coder/openstorage/issues/451)) ([f0153d4](https://github.com/coder/openstorage/commit/f0153d41b0415f3d53bfeb0c9b0a145bc1f97f09))
* **s3wal:** schedule upstream syncs from S3WAL git-surface pushes ([#454](https://github.com/coder/openstorage/issues/454)) ([f5d362a](https://github.com/coder/openstorage/commit/f5d362ab0dff6de49df68b78cbc7b372cc50c36e))
* **s3wal:** serve reads metadata format v2; add s3wal-meta convert ([#444](https://github.com/coder/openstorage/issues/444)) ([116175f](https://github.com/coder/openstorage/commit/116175fc28c6581b0b402f49b0b760a55c75fa19))
* **s3wal:** store create state in metadata v2; release-pair harness covers conversion and downgrade ([#446](https://github.com/coder/openstorage/issues/446)) ([cb885a7](https://github.com/coder/openstorage/commit/cb885a78482ab9c352eab8108551d1dadb627543))


### Bug Fixes

* **api:** add ref_guard_failed problem members to the notes 409s ([#573](https://github.com/coder/openstorage/issues/573)) ([0e6133b](https://github.com/coder/openstorage/commit/0e6133beb9f1bfb9738b2f81bec344a81dbee147))
* **api:** answer "repository already detached" on DELETE .../upstream without an upstream ([#468](https://github.com/coder/openstorage/issues/468)) ([6005169](https://github.com/coder/openstorage/commit/60051699a3128b57c0f0fba700b45e8178b48f04))
* **api:** answer 409 conflict when a diff-commit patch does not apply ([#611](https://github.com/coder/openstorage/issues/611)) ([77424c8](https://github.com/coder/openstorage/commit/77424c86a21facfc163ce320e9f96cc2279714b8))
* **api:** answer 412 no changes to commit on commit writes that change nothing ([#605](https://github.com/coder/openstorage/issues/605)) ([54f9bca](https://github.com/coder/openstorage/commit/54f9bca65684b0daaa92c674fbb393e6fd63b99e))
* **api:** answer 413 when a commit-pack passes 262144000 decoded bytes ([#610](https://github.com/coder/openstorage/issues/610)) ([e491cf4](https://github.com/coder/openstorage/commit/e491cf45542e365a854b75ffdc820ecb5ea1f5d2))
* **api:** answer a lost commit CAS with 409 failed and the written commit ([#606](https://github.com/coder/openstorage/issues/606)) ([9cec7ea](https://github.com/coder/openstorage/commit/9cec7ea3dbba6731297f41fc1ca1fe4ba1e7735d))
* **api:** answer the pack-reservation 429 with status unavailable ([#600](https://github.com/coder/openstorage/issues/600)) ([dc40992](https://github.com/coder/openstorage/commit/dc40992ea2403053df9960a5676d0517613e20c6))
* **api:** list files recursively when the call sends no recursive ([#602](https://github.com/coder/openstorage/issues/602)) ([8cb2588](https://github.com/coder/openstorage/commit/8cb258898844280a300d15fa8063daccba7b8f96))
* **api:** page recursive files listings without listing the whole tree ([#614](https://github.com/coder/openstorage/issues/614)) ([5620494](https://github.com/coder/openstorage/commit/5620494ca88650825618a0bbb731000ee586bb8f))
* **api:** read path params and tree paths literally in git pathspecs ([#578](https://github.com/coder/openstorage/issues/578)) ([0a39336](https://github.com/coder/openstorage/commit/0a39336e234504fc059deecc2ef3bcd6d1967af1))
* **api:** return every entry from file listings without limit or cursor ([#572](https://github.com/coder/openstorage/issues/572)) ([3ff6012](https://github.com/coder/openstorage/commit/3ff6012568312778e822e8168b6d07fc4dd4747f))
* **api:** take the GitHub default branch when a sync create omits default_branch ([#607](https://github.com/coder/openstorage/issues/607)) ([f6d96da](https://github.com/coder/openstorage/commit/f6d96da578938e63576328fecac4cf81de813fc9))
* **compose:** give serve 30 s to drain and tear down on docker stop ([#493](https://github.com/coder/openstorage/issues/493)) ([a09d656](https://github.com/coder/openstorage/commit/a09d6563ff383e94bb34e964f4b2258b5927a5b7))
* **deploy:** stop the AWS systemd unit with KillMode=mixed ([#479](https://github.com/coder/openstorage/issues/479)) ([a4be26e](https://github.com/coder/openstorage/commit/a4be26e562d773e8bc03a8fbda8800da5deed886))
* **deps:** bump Go toolchain to 1.27.3 for GO-2026-6809 ([#546](https://github.com/coder/openstorage/issues/546)) ([f58a6f0](https://github.com/coder/openstorage/commit/f58a6f0575780396b6f9aafac01c649c1bd77147))
* **deps:** bump gocloud.dev to v0.47.0 for the fileblob root-key fix ([#510](https://github.com/coder/openstorage/issues/510)) ([0274c7e](https://github.com/coder/openstorage/commit/0274c7ed8e74efd312be19e511264ccbe7715b95))
* **deps:** bump golang.org/x/net to v0.62.0 (GO-2026-6860, GO-2026-6866) ([#622](https://github.com/coder/openstorage/issues/622)) ([7a11b9c](https://github.com/coder/openstorage/commit/7a11b9ce193f2bd13d7f780e85941ab814d57bc7))
* **githttp:** report stale ref locks and document their manual removal ([#485](https://github.com/coder/openstorage/issues/485)) ([dda15af](https://github.com/coder/openstorage/commit/dda15aff526e5a12071dfa80a15edeb66370fbd2))
* **gitread:** frame git log fields with NUL bytes ([#582](https://github.com/coder/openstorage/issues/582)) ([6458ece](https://github.com/coder/openstorage/commit/6458eced58babd55fc01d1b083f6da5a5d85af1e))
* **gitread:** read commit dates that git cannot parse ([#584](https://github.com/coder/openstorage/issues/584)) ([fca3943](https://github.com/coder/openstorage/commit/fca3943cb0a6e7f4566d38c056e01606ea3a0dc9))
* **gitread:** read grep paths that hold a newline ([#585](https://github.com/coder/openstorage/issues/585)) ([e42338e](https://github.com/coder/openstorage/commit/e42338ed81eb07b16b4f9d392b5ece85db9e4a0c))
* **gitread:** return blame original_path unquoted ([#591](https://github.com/coder/openstorage/issues/591)) ([d283ba4](https://github.com/coder/openstorage/commit/d283ba45419c5b0d6a9c2519d120519e1a27be77))
* **image:** bump the alpine runtime base to 3.24.3 ([#551](https://github.com/coder/openstorage/issues/551)) ([302960e](https://github.com/coder/openstorage/commit/302960e15a2277bda22569fc07cb46b99624de31))
* **jobs:** bound River.Close when no drain stopped the jobs ([#642](https://github.com/coder/openstorage/issues/642)) ([23b715e](https://github.com/coder/openstorage/commit/23b715ebf9075c42905415e6a2c513ad18ea7914))
* **jobs:** give River jobs a 2-hour deadline instead of River's 1-minute default ([#637](https://github.com/coder/openstorage/issues/637)) ([47cc9bc](https://github.com/coder/openstorage/commit/47cc9bc7920899307d46ffb3eac39e2adfeaf3dc))
* **jobs:** rescue crashed sync and push jobs once their claim lease expires ([#644](https://github.com/coder/openstorage/issues/644)) ([c82e7dc](https://github.com/coder/openstorage/commit/c82e7dccd19f0cbcd1ed7e1509c32830f8dc27b1))
* **s3wal:** complete the --force help and share the op-log scope names ([#500](https://github.com/coder/openstorage/issues/500)) ([f69d4f6](https://github.com/coder/openstorage/commit/f69d4f6c942683ab8b84a4ca62d17615834dabe6))
* **s3wal:** give the meta.json quarantine one clear error and document it at the hook ([#528](https://github.com/coder/openstorage/issues/528)) ([e182cba](https://github.com/coder/openstorage/commit/e182cba77792ac958f4ce7d31ff8d69ab80532a3))
* **s3wal:** keep serve running when meta.json goes back to an older seq ([#496](https://github.com/coder/openstorage/issues/496)) ([287cda6](https://github.com/coder/openstorage/commit/287cda69db83eb0361e03f27cb853cd56e826eb6))
* **s3wal:** latch the rollback refusal when a cached meta.json stops decoding ([#514](https://github.com/coder/openstorage/issues/514)) ([5cbaba6](https://github.com/coder/openstorage/commit/5cbaba62359c872ec034ebef02c7ec21c2feac46))
* **s3wal:** quarantine meta.json until restart when an edit is found after adoption ([#523](https://github.com/coder/openstorage/issues/523)) ([d45ad2a](https://github.com/coder/openstorage/commit/d45ad2a6ff85466a6586733b3813a2bfb6211d43))
* **s3wal:** refuse a hook ref update on a corrupt meta.json ([#476](https://github.com/coder/openstorage/issues/476)) ([aab68a4](https://github.com/coder/openstorage/commit/aab68a4d3c42ece6b5aa6afd141f3e9cfb30ff35))
* **s3wal:** refuse a hook ref update on a metadata format mismatch ([#465](https://github.com/coder/openstorage/issues/465)) ([c3f6d8c](https://github.com/coder/openstorage/commit/c3f6d8c984a1694ab6a8fdac6cee1e7d9f6a2d77))
* **s3wal:** reject every row content the background loops rely on at meta.json decode ([#518](https://github.com/coder/openstorage/issues/518)) ([3fb638d](https://github.com/coder/openstorage/commit/3fb638deb798cfba47d025dce844f92b14aec6f3))
* **s3wal:** reject impossible id counters in meta.json instead of panicking ([#486](https://github.com/coder/openstorage/issues/486)) ([48076b6](https://github.com/coder/openstorage/commit/48076b620b7491225dff5c2663c29a4af3f5e2b4))
* **s3wal:** route every meta.json latch through one refusal choke point ([#522](https://github.com/coder/openstorage/issues/522)) ([c49b99b](https://github.com/coder/openstorage/commit/c49b99b1c77e54b61e961a17a3a998e3a3c228dc))
* **s3wal:** tag org-key refresh round trips in the S3 op log ([#498](https://github.com/coder/openstorage/issues/498)) ([8ae1314](https://github.com/coder/openstorage/commit/8ae13145cea7daf57fdf80da464545faa4fe306d))
* **s3wal:** turn meta.json lane-shape and counter panics into corrupt-document errors ([#509](https://github.com/coder/openstorage/issues/509)) ([2c70de2](https://github.com/coder/openstorage/commit/2c70de2f84d6e82064a6f8aa551c00656315e54c))
* **serve:** stop git children when the shutdown drain times out ([#472](https://github.com/coder/openstorage/issues/472)) ([66fedf1](https://github.com/coder/openstorage/commit/66fedf187cf716ca0c1fe764b2f98fea78a87609))
* **serve:** stop REST and hydration git children on drain timeout ([#483](https://github.com/coder/openstorage/issues/483)) ([84394f6](https://github.com/coder/openstorage/commit/84394f6573a3e00d36afe6aa83f4b5d2fce41650))


### Performance Improvements

* **gitread:** diff only what can change a files/metadata answer ([#586](https://github.com/coder/openstorage/issues/586)) ([38fb0be](https://github.com/coder/openstorage/commit/38fb0be2c6412ff9d3306940e32d8a9887e6f31d))
* **gitread:** send last-commit diffs ahead within a window ([#589](https://github.com/coder/openstorage/issues/589)) ([e5d34fd](https://github.com/coder/openstorage/commit/e5d34fdb4041c25b51c1d2ce02efa76c7a8345fa))
* **s3wal:** reuse the decoded meta.json on a 304 for read-only callers ([#495](https://github.com/coder/openstorage/issues/495)) ([1a1a201](https://github.com/coder/openstorage/commit/1a1a2016b228f87c27a6c62e7ebecc2dededaf94))

## [0.18.0](https://github.com/coder/openstorage/compare/v0.17.0...v0.18.0) (2026-10-13)


### ⚠ BREAKING CHANGES

* **sync:** a REST write or git push from a token whose ref policies carry any op no longer schedules the inbound upstream sync on PostgreSQL, or for REST writes on SQLite and S3WAL. The write stays local, and the repository converges on the next write, pull-upstream or push-upstream from a token without policy ops. In a mixed fleet, replicas on an older binary still schedule the sync for such writes until they are upgraded.

### Features

* **api:** add RFC 9457 instance to problem-details error bodies ([#420](https://github.com/coder/openstorage/issues/420)) ([6b23ae9](https://github.com/coder/openstorage/commit/6b23ae9f9b06f22e0f8260f8a9f269a6b945b362))
* **api:** emit upstream's specific not-found and conflict problem codes ([#435](https://github.com/coder/openstorage/issues/435)) ([5cadc73](https://github.com/coder/openstorage/commit/5cadc73d849630a2ef3555792275177268e46b72))
* **api:** emit upstream's specific problem codes from the central helpers ([#428](https://github.com/coder/openstorage/issues/428)) ([b14fa58](https://github.com/coder/openstorage/commit/b14fa588d162d7881fb34507046af822bcb73b6b))
* **api:** pull-upstream finishes a pending sync create ([#432](https://github.com/coder/openstorage/issues/432)) ([deef8b1](https://github.com/coder/openstorage/commit/deef8b10c2f3a3ff9d2cc86a51dbe56108ec6bd0))


### Bug Fixes

* **deps:** bump Go toolchain to 1.27.2 for stdlib security fixes ([#434](https://github.com/coder/openstorage/issues/434)) ([04a3eea](https://github.com/coder/openstorage/commit/04a3eea2b54e1d4201bb57d15479b5157abe990d))
* **jobs:** a PostgreSQL sync create completes only while its upstream is linked ([#423](https://github.com/coder/openstorage/issues/423)) ([c034b20](https://github.com/coder/openstorage/commit/c034b20ef9f80af73705b914c8cc7edbac1055d0))
* **release:** keep release binaries' VCS stamp clean ([#415](https://github.com/coder/openstorage/issues/415)) ([3bd4897](https://github.com/coder/openstorage/commit/3bd4897d7f5c9c0b4d41e20b0fdfebe11345a7ea))
* **s3wal:** restart a pull that crashed holding its claim ([#427](https://github.com/coder/openstorage/issues/427)) ([464c1c2](https://github.com/coder/openstorage/commit/464c1c2a7ceba485b578d2ed7e8419f7707d26eb))
* **sync:** deny the implicit sync poke for ref-policy-restricted tokens ([#426](https://github.com/coder/openstorage/issues/426)) ([dacd7cd](https://github.com/coder/openstorage/commit/dacd7cd9076336565730311e1999f1e4c0400438))

## [0.17.0](https://github.com/coder/openstorage/compare/v0.16.0...v0.17.0) (2026-10-12)


### ⚠ BREAKING CHANGES

* **config:** openstorage no longer reads CODESTORAGE_* environment variables. Rename each one to its OPENSTORAGE_* name before you upgrade. A process that still sets an old name without its replacement refuses to start and names the variables to rename.

### Features

* **compose:** accept a registry image in the Docker-only quickstart ([#406](https://github.com/coder/openstorage/issues/406)) ([22346a4](https://github.com/coder/openstorage/commit/22346a40905590c7da16aa71ddd708fea7df597a))
* **config:** remove the CODESTORAGE_* environment fallback ([#413](https://github.com/coder/openstorage/issues/413)) ([f70ee39](https://github.com/coder/openstorage/commit/f70ee3949e1dfc1782c69e62e15b98b855d41fe8))

## [0.16.0](https://github.com/coder/openstorage/compare/v0.15.0...v0.16.0) (2026-10-09)


### ⚠ BREAKING CHANGES

* **api:** Commit.date now means the author date. Read the new committer_date field for the committer date.
* **api:** POST /api/v1/repos returns 201 instead of 200 for a new repository, the success message text changes, and a create of a deleted repo's name returns 404 instead of 409. The Go and TypeScript SDKs accept any 2xx.
* **auth:** RSA organization public keys must be at least 2048 bits. The server checks this at key ingestion and on every request, including keys already stored, so rotate weaker keys before you upgrade. ([#253](https://github.com/coder/openstorage/issues/253))
* **webhooks:** send outbound push-upstream runs as repo.push_upstream.* events ([#358](https://github.com/coder/openstorage/issues/358))
* The Go module path is now github.com/coder/openstorage (was github.com/coder/open-storage). Update go install commands, go.mod requirements and imports. ([#225](https://github.com/coder/openstorage/issues/225))

### Features

* **api:** accept optional repo_name in POST /repos ([#299](https://github.com/coder/openstorage/issues/299)) ([3a68bc9](https://github.com/coder/openstorage/commit/3a68bc98851c823a0cfd64648837cfd83a6a07cb))
* **api:** accept restore-commit base_ref with the spec's 412 preconditions ([#383](https://github.com/coder/openstorage/issues/383)) ([b0ddce7](https://github.com/coder/openstorage/commit/b0ddce709856aa56ff2f0c5cd9e87d68e473e9cf))
* **api:** accept upstream on repo create and expose it on repo GET ([#361](https://github.com/coder/openstorage/issues/361)) ([f6a6d33](https://github.com/coder/openstorage/commit/f6a6d336266a2730e71ce17789354c34b000088b))
* **api:** add organization metadata endpoint ([#286](https://github.com/coder/openstorage/issues/286)) ([536e096](https://github.com/coder/openstorage/commit/536e096a32db4a0f76a3d163f2075d7d4984164f))
* **api:** add RFC 9457 problem-details fields to the error envelope ([#351](https://github.com/coder/openstorage/issues/351)) ([1f392ac](https://github.com/coder/openstorage/commit/1f392ac38e330e1966a8a9a31a023f2ba09cdfe7))
* **api:** add topological order rank to files/metadata commits ([#337](https://github.com/coder/openstorage/issues/337)) ([516ee3d](https://github.com/coder/openstorage/commit/516ee3dcbed62c4a15da6642980a5bdbbbe921bd))
* **api:** allocate omitted branch targets ([#239](https://github.com/coder/openstorage/issues/239)) ([3663fae](https://github.com/coder/openstorage/commit/3663faef017a7c51392933d3f5e11a0a52d68520))
* **api:** answer POST /repos 201 on create and 404 for a deleted name ([#390](https://github.com/coder/openstorage/issues/390)) ([127be7d](https://github.com/coder/openstorage/commit/127be7debbd9f9ee39ce0bf9b3eeb2d368e7b86a))
* **api:** bound memory, time and concurrency of read-side git subprocesses ([#324](https://github.com/coder/openstorage/issues/324)) ([0fec21e](https://github.com/coder/openstorage/commit/0fec21eb42be719c5843924d69b0015eea774516))
* **api:** serve the path-style notes/append, upstream/pull and DELETE upstream routes ([#382](https://github.com/coder/openstorage/issues/382)) ([718d3a5](https://github.com/coder/openstorage/commit/718d3a52b6c5403f7f4956320aae1066ebc7583a))
* **cli:** add fail-closed keygen bootstrap mode ([e32d58f](https://github.com/coder/openstorage/commit/e32d58f349d9b7fa41d052488db0347a4647695a))
* **compose:** add a local-build Docker-only quickstart ([#256](https://github.com/coder/openstorage/issues/256)) ([e374aad](https://github.com/coder/openstorage/commit/e374aad49eef85d5142612b56795d0995503ea12))
* **deploy:** add Terraform AWS EC2 reference ([#254](https://github.com/coder/openstorage/issues/254)) ([d94125f](https://github.com/coder/openstorage/commit/d94125f63c0bec1ea4bdf833dd19deaaa22e4a9d))
* **githttp:** bounded push-publication wait primitive ([#332](https://github.com/coder/openstorage/issues/332)) ([474791e](https://github.com/coder/openstorage/commit/474791ea36db75e53eab305664c974e3ad948c8c))
* **githttp:** commit-graph refreshes join the shared git chokepoint; outcome log contract and metric ([#338](https://github.com/coder/openstorage/issues/338)) ([cb8b33a](https://github.com/coder/openstorage/commit/cb8b33a8313ffbabf50f0671c8d92b94e5657e20))
* **meta:** add CreateLinkedRepo, an atomic repo + upstream (+ credential) create ([#389](https://github.com/coder/openstorage/issues/389)) ([c3af22a](https://github.com/coder/openstorage/commit/c3af22a501cb4aa404950b2231f208c5d4660b92))
* **meta:** record a per-repository create state ([#391](https://github.com/coder/openstorage/issues/391)) ([f677626](https://github.com/coder/openstorage/commit/f6776265b658d46bdc7916cd5a8a4179ed34fef3))
* **meta:** SQLite local schedule state and transitions ([#336](https://github.com/coder/openstorage/issues/336)) ([549d37c](https://github.com/coder/openstorage/commit/549d37cb0131a93f270d2fbcc03424748e308e94))
* **metrics:** add opt-in Prometheus metrics listener ([#327](https://github.com/coder/openstorage/issues/327)) ([372a4b3](https://github.com/coder/openstorage/commit/372a4b3031821c0a3d9653a06eccd869c6d59dcb))
* **sync:** schedule a sync create's first sync with its create mark ([#392](https://github.com/coder/openstorage/issues/392)) ([0c6c2c2](https://github.com/coder/openstorage/commit/0c6c2c2f1222080b48dd87425b8a15945b20b368))
* **sync:** SQLite git-surface pushes schedule sync and forwarding ([#345](https://github.com/coder/openstorage/issues/345)) ([897ccd1](https://github.com/coder/openstorage/commit/897ccd1054a86f1142fbd156281a4b40a648ffbb))
* **sync:** SQLite local worker and schedule sweep ([#341](https://github.com/coder/openstorage/issues/341)) ([edb0eeb](https://github.com/coder/openstorage/commit/edb0eeb064dbd1025ad2d607283a099bd44fc10d))
* **webhooks:** send org, type and repository.repo_name ([#368](https://github.com/coder/openstorage/issues/368)) ([05f083a](https://github.com/coder/openstorage/commit/05f083a8a94d98a983fbaed165b36484d4449e8d))
* **webhooks:** send outbound push-upstream runs as repo.push_upstream.* events ([#358](https://github.com/coder/openstorage/issues/358)) ([c69bcc7](https://github.com/coder/openstorage/commit/c69bcc737fb998f73fbbaac8d6598b27b5decd3d))


### Bug Fixes

* **api:** accept current upstream field names and DELETE query params ([#379](https://github.com/coder/openstorage/issues/379)) ([a26fcc3](https://github.com/coder/openstorage/commit/a26fcc3aef61684930b8c4c36b505c7b8f1d7692))
* **api:** accept uppercase full SHAs and reject duplicate strict-JSON members ([#384](https://github.com/coder/openstorage/issues/384)) ([87b06ec](https://github.com/coder/openstorage/commit/87b06ec90d029349913e716321b80b0e5efc28b4))
* **api:** accept uppercase guard SHAs and walk strict request types ([#386](https://github.com/coder/openstorage/issues/386)) ([9efd9d7](https://github.com/coder/openstorage/commit/9efd9d70126bce1f49425bab0a45e8bc221b409a))
* **api:** deny sync repo creates for ref-policy-restricted tokens ([#395](https://github.com/coder/openstorage/issues/395)) ([db465a2](https://github.com/coder/openstorage/commit/db465a2594aecbf88094c3688e8db30eff38d5d8))
* **api:** omit exhausted pagination cursors and refresh upstream spec ([#377](https://github.com/coder/openstorage/issues/377)) ([6851ff9](https://github.com/coder/openstorage/commit/6851ff93f6ad34b3576669812896f5a1f5788cc0))
* **api:** report the author date as Commit.date and add committer_date ([#401](https://github.com/coder/openstorage/issues/401)) ([cf05eff](https://github.com/coder/openstorage/commit/cf05eff6c7852112ffe14b2a3bbc6784077a1217))
* **api:** resolve a relative cache dir once at startup and in api.New ([#366](https://github.com/coder/openstorage/issues/366)) ([b590932](https://github.com/coder/openstorage/commit/b59093209ae15a35db392d8930a39e6339be9d37))
* **api:** stage the fork pack on the pack dir's filesystem ([#362](https://github.com/coder/openstorage/issues/362)) ([fcb7816](https://github.com/coder/openstorage/commit/fcb7816243c4c8aa14c937d94404311b46f9ee44))
* **auth:** reject non-canonical base64url in JWT segments ([#323](https://github.com/coder/openstorage/issues/323)) ([2ee1cfb](https://github.com/coder/openstorage/commit/2ee1cfb169d6be7b87f2db003ba9dd074e937cd6))
* **auth:** require 2048-bit RSA organization public keys ([5b17f02](https://github.com/coder/openstorage/commit/5b17f0261a5b81a61430d02fab25dc03a54b750b))
* **ci:** make the legacy MinIO qualification lane opt-in ([#308](https://github.com/coder/openstorage/issues/308)) ([c01ad82](https://github.com/coder/openstorage/commit/c01ad8268f4f7f74df842bab4e8c2e0f42bb6e38))
* **ci:** restore the legacy MinIO fixture source ([#251](https://github.com/coder/openstorage/issues/251)) ([5f172c5](https://github.com/coder/openstorage/commit/5f172c5e4e72568dcdf0f344d65f75163b2f34c6))
* **deps:** update x/crypto to address SSH advisories ([#233](https://github.com/coder/openstorage/issues/233)) ([de71d60](https://github.com/coder/openstorage/commit/de71d6061da7e4afb306254b731f756c3dd9340a))
* **githttp:** serialize cold cache initialization ([#252](https://github.com/coder/openstorage/issues/252)) ([915d4be](https://github.com/coder/openstorage/commit/915d4beafaaa313f282a6ea08eb99970ad4d9908))
* **gitread:** pin git's message locale so heap-cap failures classify as 422 ([#340](https://github.com/coder/openstorage/issues/340)) ([870a647](https://github.com/coder/openstorage/commit/870a64786dfd1e85a981a0adf13ed8cbd901d591))
* **jobs:** keep River sync runs uncounted across serve shutdown ([#347](https://github.com/coder/openstorage/issues/347)) ([b224898](https://github.com/coder/openstorage/commit/b224898af6c778717bda8a576e43fb5b79ddd421))
* **s3wal:** name missing lease holder in corruption diagnostics ([#302](https://github.com/coder/openstorage/issues/302)) ([6bf383a](https://github.com/coder/openstorage/commit/6bf383a272fc81b695e5db5d0630dc031ac8ba12))
* **s3wal:** preserve evidence in lease refusal diagnostics ([#270](https://github.com/coder/openstorage/issues/270)) ([84101df](https://github.com/coder/openstorage/commit/84101df0e27770f4808735fa41485bb7f94deaf6))
* **sync:** keep in-flight local runs' requests across serve shutdown ([#344](https://github.com/coder/openstorage/issues/344)) ([bba1986](https://github.com/coder/openstorage/commit/bba1986913e5f0dc5ee2c90fd47e0cc9aa7f9870))
* **sync:** send upstream's default username when none is stored ([#402](https://github.com/coder/openstorage/issues/402)) ([ed1b409](https://github.com/coder/openstorage/commit/ed1b409ce7c53f03a98a8d3fb4dd6f37859a01b0))
* **sync:** send x-token-auth for Bitbucket credentials without a username ([#398](https://github.com/coder/openstorage/issues/398)) ([6cb11d3](https://github.com/coder/openstorage/commit/6cb11d301be0e5bb920c7c3b81d98403bb3601ad))
* **test:** accept trailing slash on fake S3 bucket requests ([#330](https://github.com/coder/openstorage/issues/330)) ([347f59d](https://github.com/coder/openstorage/commit/347f59d6ff3d1841bb7f1e07c87c33ae74fafbb5))
* use the canonical openstorage repository and Go module ([#225](https://github.com/coder/openstorage/issues/225)) ([c877535](https://github.com/coder/openstorage/commit/c8775353a7ce469fcebad75ee95ff471a458bea8))


### Performance Improvements

* **githttp:** write split commit-graphs for cache repositories ([#321](https://github.com/coder/openstorage/issues/321)) ([84e2125](https://github.com/coder/openstorage/commit/84e212568cbb8f66431b7912d3d3acb87dc42638))

## [0.15.0](https://github.com/coder/openstorage/compare/v0.14.6...v0.15.0) (2026-09-07)


### ⚠ BREAKING CHANGES

* rename project to open.storage ([#220](https://github.com/coder/openstorage/issues/220))

### Features

* rename project to open.storage ([#220](https://github.com/coder/openstorage/issues/220)) ([9ce88d2](https://github.com/coder/openstorage/commit/9ce88d2056775c8e169fbe4405a53cf1a79e8e4c))

## [0.14.6](https://github.com/coder/openstorage/compare/v0.14.5...v0.14.6) (2026-08-29)


### Bug Fixes

* **githttp:** stop the boot sweep from contaminating the s3wal push op census ([#210](https://github.com/coder/openstorage/issues/210)) ([8325954](https://github.com/coder/openstorage/commit/8325954f159d929476913c37fc16ffca6eb28159))

## [0.14.5](https://github.com/coder/openstorage/compare/v0.14.4...v0.14.5) (2026-08-28)


### Bug Fixes

* **githttp:** make holdFileID stat folding portable across platforms ([#199](https://github.com/coder/openstorage/issues/199)) ([fb29408](https://github.com/coder/openstorage/commit/fb29408cfceb28fc7d2ef3f467277e2694c3cd38))

## [0.14.4](https://github.com/coder/openstorage/compare/v0.14.3...v0.14.4) (2026-08-28)


### Bug Fixes

* **githttp:** coalesce held-marker validation across claimed push generations ([#197](https://github.com/coder/openstorage/issues/197)) ([950995d](https://github.com/coder/openstorage/commit/950995dcbd4485c1e9620c4329177d751382f2e7))

## [0.14.3](https://github.com/coder/openstorage/compare/v0.14.2...v0.14.3) (2026-08-28)


### Bug Fixes

* **githttp:** release committed and denied push-hold refs via append-only tombstones ([#195](https://github.com/coder/openstorage/issues/195)) ([aa24498](https://github.com/coder/openstorage/commit/aa2449882400ccc3ceac1d38dc7b7d1e97cfea63))

## [0.14.2](https://github.com/coder/openstorage/compare/v0.14.1...v0.14.2) (2026-08-28)


### Bug Fixes

* **githttp:** make identical concurrent pack publication idempotent ([#187](https://github.com/coder/openstorage/issues/187)) ([#190](https://github.com/coder/openstorage/issues/190)) ([c878adb](https://github.com/coder/openstorage/commit/c878adb59016c2f1830e9d54eb9336cc065210b0))
* **githttp:** persist pack-path JIT verdicts, kernel-lock hold reaping, ABA-safe hold fingerprints ([#189](https://github.com/coder/openstorage/issues/189)) ([71f514b](https://github.com/coder/openstorage/commit/71f514b87eccf2d6583cbb129490fc0c1acd3cc7))

## [0.14.1](https://github.com/coder/openstorage/compare/v0.14.0...v0.14.1) (2026-08-28)


### Bug Fixes

* **githttp:** skip in-flight push refs during hydration to stop spurious receive-pack rejections ([#170](https://github.com/coder/openstorage/issues/170)) ([#173](https://github.com/coder/openstorage/issues/173)) ([c7c291b](https://github.com/coder/openstorage/commit/c7c291bc48572fe4224bf0e4363d13a8f4f1e67c))

## [0.14.0](https://github.com/coder/openstorage/compare/v0.13.0...v0.14.0) (2026-08-27)


### Features

* **bench:** s3wal push-path e2e benchmark + Phase 4 results publication ([#171](https://github.com/coder/openstorage/issues/171)) ([dea8da0](https://github.com/coder/openstorage/commit/dea8da04f5aa2ee26220cb54259f6fdec0de645b))
* **s3wal:** Phase 3 — zero-infra serve mode with S3 meta backend, sync-claim write-through, and instance lease ([#168](https://github.com/coder/openstorage/issues/168)) ([7ea41d2](https://github.com/coder/openstorage/commit/7ea41d26da87ba76fd434231b1ebf69edd62d970))


### Bug Fixes

* **blob:** carry LocationConstraint on CreateBucket and probe on constraint rejection ([#180](https://github.com/coder/openstorage/issues/180)) ([520916d](https://github.com/coder/openstorage/commit/520916d6a80ec0ad9ce6fdb2c5ea43b260e15219))
* **blob:** gate SeaweedFS test harness readiness on the shared write roundtrip ([#178](https://github.com/coder/openstorage/issues/178)) ([ceb0847](https://github.com/coder/openstorage/commit/ceb08470d7755c9436b548f40f4d6a548585c580))
* **s3wal:** align lost-race backoff with §2.3 and make the contention runner faithful ([#181](https://github.com/coder/openstorage/issues/181)) ([#184](https://github.com/coder/openstorage/issues/184)) ([c230b79](https://github.com/coder/openstorage/commit/c230b79f978e776358cda65c2c5784ccaf16fdce))
* **s3wal:** gate SeaweedFS harness readiness on a write roundtrip, hardening qualification against boot-window 5xx ([#172](https://github.com/coder/openstorage/issues/172)) ([d0b4f6b](https://github.com/coder/openstorage/commit/d0b4f6b9a3bd0a0ba1157cd7bdf722c8e4e3edfc))
* **serve:** release s3wal instance lease on SIGTERM/SIGINT via graceful shutdown ([#176](https://github.com/coder/openstorage/issues/176)) ([74aa93b](https://github.com/coder/openstorage/commit/74aa93bf2719ebb48afaa018e9b14e67fd33297d))


### Performance Improvements

* **s3wal:** trim per-push S3 round trips ([#182](https://github.com/coder/openstorage/issues/182)) ([#185](https://github.com/coder/openstorage/issues/185)) ([6a55e45](https://github.com/coder/openstorage/commit/6a55e450ef8bd94a8c31b136cdaa678facbf4e3a))

## [0.13.0](https://github.com/coder/openstorage/compare/v0.12.0...v0.13.0) (2026-08-26)


### Features

* **api:** sync upstream drift — diff base_sha and notes object_ref revisions ([#163](https://github.com/coder/openstorage/issues/163)) ([f88cdf3](https://github.com/coder/openstorage/commit/f88cdf3c1db5fb6232f25dfca7dffda602e52f79))
* **ref:** s3wal RefStore — full driver and conformance over single-manifest CAS ([#156](https://github.com/coder/openstorage/issues/156)) ([96c12e4](https://github.com/coder/openstorage/commit/96c12e4bcce3e66518d57165e3b53e147a2419d1))

## [0.12.0](https://github.com/coder/openstorage/compare/v0.11.1...v0.12.0) (2026-08-26)


### Features

* **blob:** SeaweedFS replaces MinIO as the tested self-hosted S3 store ([#145](https://github.com/coder/openstorage/issues/145)) ([0eb03e0](https://github.com/coder/openstorage/commit/0eb03e06fb42a685a75596fa2133af169ace295c))

## [0.11.1](https://github.com/coder/openstorage/compare/v0.11.0...v0.11.1) (2026-08-26)


### Bug Fixes

* **githttp:** disarm LFS proxy-upload body read deadlines after body consumption ([#154](https://github.com/coder/openstorage/issues/154)) ([0f888e7](https://github.com/coder/openstorage/commit/0f888e76695545ea175aed46c90ed9a7dc7f3e17)), closes [#141](https://github.com/coder/openstorage/issues/141)

## [0.11.0](https://github.com/coder/openstorage/compare/v0.10.0...v0.11.0) (2026-08-26)


### Features

* **jobs:** fleet-persistent scan state — observed reap watermarks and resumable cursors ([#132](https://github.com/coder/openstorage/issues/132)) ([daa59c3](https://github.com/coder/openstorage/commit/daa59c33cfa9e273559325abfad80451cca27286))

## [0.10.0](https://github.com/coder/openstorage/compare/v0.9.0...v0.10.0) (2026-08-25)


### Features

* **api:** emit the IETF RateLimit header alongside Retry-After on every 429 ([#151](https://github.com/coder/openstorage/issues/151)) ([d1e8d71](https://github.com/coder/openstorage/commit/d1e8d71acd04a4899c616023fe1d9e72602d2410)), closes [#150](https://github.com/coder/openstorage/issues/150)
* **s3wal:** store qualification harness, conditional CAS primitive, and Phase 1a benches ([#142](https://github.com/coder/openstorage/issues/142)) ([39f1ba7](https://github.com/coder/openstorage/commit/39f1ba7d567cc0a916a75f6e383f53069d6ec84f))

## [0.9.0](https://github.com/coder/openstorage/compare/v0.8.0...v0.9.0) (2026-08-25)


### Features

* **api:** add repo_name to the delete-repo response; refresh vendored OpenAPI snapshot ([#148](https://github.com/coder/openstorage/issues/148)) ([8a8e25d](https://github.com/coder/openstorage/commit/8a8e25d7d7c5af426921b0c3643e3f45026da612)), closes [#147](https://github.com/coder/openstorage/issues/147)

## [0.8.0](https://github.com/coder/openstorage/compare/v0.7.0...v0.8.0) (2026-08-25)


### Features

* **sync:** unify sync egress policy behind one dial-time destination guard ([#144](https://github.com/coder/openstorage/issues/144)) ([5ce9f8f](https://github.com/coder/openstorage/commit/5ce9f8ff946a55a627da1bcd9a087759b6cdafc0)), closes [#139](https://github.com/coder/openstorage/issues/139)

## [0.7.0](https://github.com/coder/openstorage/compare/v0.6.2...v0.7.0) (2026-08-25)


### Features

* **sync:** forward local pushes to the linked upstream (push-upstream) ([#137](https://github.com/coder/openstorage/issues/137)) ([3f985b5](https://github.com/coder/openstorage/commit/3f985b5be5dde9ee031e726d96f0d9aeba584c1f))

## [0.6.2](https://github.com/coder/openstorage/compare/v0.6.1...v0.6.2) (2026-08-25)


### Bug Fixes

* **jobs:** heal truncated registered originals from quarantine and never purge unverifiable backups ([#135](https://github.com/coder/openstorage/issues/135)) ([ca608f0](https://github.com/coder/openstorage/commit/ca608f0eb9810488732ef0cfb59fcaa59dcf4eba))

## [0.6.1](https://github.com/coder/openstorage/compare/v0.6.0...v0.6.1) (2026-08-25)


### Bug Fixes

* **jobs:** spare legacy LFS source when canonical copy is size-deficient ([#130](https://github.com/coder/openstorage/issues/130)) ([fdf9c01](https://github.com/coder/openstorage/commit/fdf9c01c0ab52e8a26a768fe3d076e718973b56f)), closes [#127](https://github.com/coder/openstorage/issues/127)

## [0.6.0](https://github.com/coder/openstorage/compare/v0.5.1...v0.6.0) (2026-08-25)


### Features

* **jobs:** reconcile registered-but-missing blobs against the quota ledger ([#123](https://github.com/coder/openstorage/issues/123)) ([d73e087](https://github.com/coder/openstorage/commit/d73e08746aaeca3cf1dd3fbd546f657856abd80c))

## [0.5.1](https://github.com/coder/openstorage/compare/v0.5.0...v0.5.1) (2026-08-25)


### Bug Fixes

* **githttp:** bound ref materialization in the shared hydration path ([#124](https://github.com/coder/openstorage/issues/124)) ([366d609](https://github.com/coder/openstorage/commit/366d609600a54e1eec4942d8af1078ce189b70c2)), closes [#119](https://github.com/coder/openstorage/issues/119)

## [0.5.0](https://github.com/coder/openstorage/compare/v0.4.0...v0.5.0) (2026-08-24)


### Features

* **lfs:** reclaim registered-but-unreferenced objects ([#117](https://github.com/coder/openstorage/issues/117)) ([c428e82](https://github.com/coder/openstorage/commit/c428e828ffa82a17fcd9f39bcb30a86070238fb7))

## [0.4.0](https://github.com/coder/openstorage/compare/v0.3.1...v0.4.0) (2026-08-24)


### Features

* **blob:** optional expected-bucket-owner enforcement across all S3 operations ([#115](https://github.com/coder/openstorage/issues/115)) ([fe39b73](https://github.com/coder/openstorage/commit/fe39b739324e7379ebc213818f553297dc651f65))

## [0.3.1](https://github.com/coder/openstorage/compare/v0.3.0...v0.3.1) (2026-08-24)


### Bug Fixes

* **lfs:** canonical o2 object keys immune to legacy presigned-PUT replay ([#104](https://github.com/coder/openstorage/issues/104)) ([3cda233](https://github.com/coder/openstorage/commit/3cda233fc435f10220200501dd5b7100bfe3bc92))

## [0.3.0](https://github.com/coder/openstorage/compare/v0.2.0...v0.3.0) (2026-08-23)


### Features

* **storage:** reclaim soft-deleted repositories — retention purge + purge-repo CLI ([#105](https://github.com/coder/openstorage/issues/105)) ([eb057d2](https://github.com/coder/openstorage/commit/eb057d2e28d1159789e0bbfca40a64f5b101e428))

## [0.2.0](https://github.com/coder/openstorage/compare/v0.1.4...v0.2.0) (2026-08-23)


### Features

* **lfs:** quota-metered, digest-verified uploads and lfs/ sweep reconciliation ([#51](https://github.com/coder/openstorage/issues/51)) ([583f369](https://github.com/coder/openstorage/commit/583f369aafbc26d31a6b95d3d13755674cf4cdb0))

## [0.1.4](https://github.com/coder/openstorage/compare/v0.1.3...v0.1.4) (2026-08-23)


### Performance Improvements

* **githttp:** eliminate git subprocess forks from the hydration fast path ([#98](https://github.com/coder/openstorage/issues/98)) ([cab32ca](https://github.com/coder/openstorage/commit/cab32ca5c0ebe431167758312dedf3144000eeda))

## [0.1.3](https://github.com/coder/openstorage/compare/v0.1.2...v0.1.3) (2026-08-23)


### Bug Fixes

* **sync:** lease the sync claim so a dead holder can never wedge syncing ([#89](https://github.com/coder/openstorage/issues/89)) ([8c7ba19](https://github.com/coder/openstorage/commit/8c7ba1943d1bf465c67203970a4491a42e5aced1))

## [0.1.2](https://github.com/coder/openstorage/compare/v0.1.1...v0.1.2) (2026-08-23)


### Bug Fixes

* **sync:** never drop pull-upstream requests accepted while a sync claim is held ([#85](https://github.com/coder/openstorage/issues/85)) ([5ef8da6](https://github.com/coder/openstorage/commit/5ef8da66ba549dc2f4192bf249e98772c0e43b69))

## [0.1.1](https://github.com/coder/openstorage/compare/v0.1.0...v0.1.1) (2026-08-23)


### Bug Fixes

* **githttp:** bound upload-pack gzip negotiation bodies ([f5e6c8f](https://github.com/coder/openstorage/commit/f5e6c8f872b5d3b621aae4cff697a6f2fc7e959c))

## 0.1.0 (2026-08-22)


### Features

* **release:** adopt release-please and ship cross-platform release binaries ([#65](https://github.com/coder/openstorage/issues/65), [#66](https://github.com/coder/openstorage/issues/66)) ([#72](https://github.com/coder/openstorage/issues/72)) ([0af923f](https://github.com/coder/openstorage/commit/0af923fcb0a18ba80f49e605ffbd6e0bdbab703c))
