# Changelog

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
