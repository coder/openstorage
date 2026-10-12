# Changelog

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
