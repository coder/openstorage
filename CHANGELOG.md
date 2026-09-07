# Changelog

## [0.14.6](https://github.com/coder/open-storage/compare/v0.14.5...v0.14.6) (2026-08-29)


### Bug Fixes

* **githttp:** stop the boot sweep from contaminating the s3wal push op census ([#210](https://github.com/coder/open-storage/issues/210)) ([8325954](https://github.com/coder/open-storage/commit/8325954f159d929476913c37fc16ffca6eb28159))

## [0.14.5](https://github.com/coder/open-storage/compare/v0.14.4...v0.14.5) (2026-08-28)


### Bug Fixes

* **githttp:** make holdFileID stat folding portable across platforms ([#199](https://github.com/coder/open-storage/issues/199)) ([fb29408](https://github.com/coder/open-storage/commit/fb29408cfceb28fc7d2ef3f467277e2694c3cd38))

## [0.14.4](https://github.com/coder/open-storage/compare/v0.14.3...v0.14.4) (2026-08-28)


### Bug Fixes

* **githttp:** coalesce held-marker validation across claimed push generations ([#197](https://github.com/coder/open-storage/issues/197)) ([950995d](https://github.com/coder/open-storage/commit/950995dcbd4485c1e9620c4329177d751382f2e7))

## [0.14.3](https://github.com/coder/open-storage/compare/v0.14.2...v0.14.3) (2026-08-28)


### Bug Fixes

* **githttp:** release committed and denied push-hold refs via append-only tombstones ([#195](https://github.com/coder/open-storage/issues/195)) ([aa24498](https://github.com/coder/open-storage/commit/aa2449882400ccc3ceac1d38dc7b7d1e97cfea63))

## [0.14.2](https://github.com/coder/open-storage/compare/v0.14.1...v0.14.2) (2026-08-28)


### Bug Fixes

* **githttp:** make identical concurrent pack publication idempotent ([#187](https://github.com/coder/open-storage/issues/187)) ([#190](https://github.com/coder/open-storage/issues/190)) ([c878adb](https://github.com/coder/open-storage/commit/c878adb59016c2f1830e9d54eb9336cc065210b0))
* **githttp:** persist pack-path JIT verdicts, kernel-lock hold reaping, ABA-safe hold fingerprints ([#189](https://github.com/coder/open-storage/issues/189)) ([71f514b](https://github.com/coder/open-storage/commit/71f514b87eccf2d6583cbb129490fc0c1acd3cc7))

## [0.14.1](https://github.com/coder/open-storage/compare/v0.14.0...v0.14.1) (2026-08-28)


### Bug Fixes

* **githttp:** skip in-flight push refs during hydration to stop spurious receive-pack rejections ([#170](https://github.com/coder/open-storage/issues/170)) ([#173](https://github.com/coder/open-storage/issues/173)) ([c7c291b](https://github.com/coder/open-storage/commit/c7c291bc48572fe4224bf0e4363d13a8f4f1e67c))

## [0.14.0](https://github.com/coder/open-storage/compare/v0.13.0...v0.14.0) (2026-08-27)


### Features

* **bench:** s3wal push-path e2e benchmark + Phase 4 results publication ([#171](https://github.com/coder/open-storage/issues/171)) ([dea8da0](https://github.com/coder/open-storage/commit/dea8da04f5aa2ee26220cb54259f6fdec0de645b))
* **s3wal:** Phase 3 — zero-infra serve mode with S3 meta backend, sync-claim write-through, and instance lease ([#168](https://github.com/coder/open-storage/issues/168)) ([7ea41d2](https://github.com/coder/open-storage/commit/7ea41d26da87ba76fd434231b1ebf69edd62d970))


### Bug Fixes

* **blob:** carry LocationConstraint on CreateBucket and probe on constraint rejection ([#180](https://github.com/coder/open-storage/issues/180)) ([520916d](https://github.com/coder/open-storage/commit/520916d6a80ec0ad9ce6fdb2c5ea43b260e15219))
* **blob:** gate SeaweedFS test harness readiness on the shared write roundtrip ([#178](https://github.com/coder/open-storage/issues/178)) ([ceb0847](https://github.com/coder/open-storage/commit/ceb08470d7755c9436b548f40f4d6a548585c580))
* **s3wal:** align lost-race backoff with §2.3 and make the contention runner faithful ([#181](https://github.com/coder/open-storage/issues/181)) ([#184](https://github.com/coder/open-storage/issues/184)) ([c230b79](https://github.com/coder/open-storage/commit/c230b79f978e776358cda65c2c5784ccaf16fdce))
* **s3wal:** gate SeaweedFS harness readiness on a write roundtrip, hardening qualification against boot-window 5xx ([#172](https://github.com/coder/open-storage/issues/172)) ([d0b4f6b](https://github.com/coder/open-storage/commit/d0b4f6b9a3bd0a0ba1157cd7bdf722c8e4e3edfc))
* **serve:** release s3wal instance lease on SIGTERM/SIGINT via graceful shutdown ([#176](https://github.com/coder/open-storage/issues/176)) ([74aa93b](https://github.com/coder/open-storage/commit/74aa93bf2719ebb48afaa018e9b14e67fd33297d))


### Performance Improvements

* **s3wal:** trim per-push S3 round trips ([#182](https://github.com/coder/open-storage/issues/182)) ([#185](https://github.com/coder/open-storage/issues/185)) ([6a55e45](https://github.com/coder/open-storage/commit/6a55e450ef8bd94a8c31b136cdaa678facbf4e3a))

## [0.13.0](https://github.com/coder/open-storage/compare/v0.12.0...v0.13.0) (2026-08-26)


### Features

* **api:** sync upstream drift — diff base_sha and notes object_ref revisions ([#163](https://github.com/coder/open-storage/issues/163)) ([f88cdf3](https://github.com/coder/open-storage/commit/f88cdf3c1db5fb6232f25dfca7dffda602e52f79))
* **ref:** s3wal RefStore — full driver and conformance over single-manifest CAS ([#156](https://github.com/coder/open-storage/issues/156)) ([96c12e4](https://github.com/coder/open-storage/commit/96c12e4bcce3e66518d57165e3b53e147a2419d1))

## [0.12.0](https://github.com/coder/open-storage/compare/v0.11.1...v0.12.0) (2026-08-26)


### Features

* **blob:** SeaweedFS replaces MinIO as the tested self-hosted S3 store ([#145](https://github.com/coder/open-storage/issues/145)) ([0eb03e0](https://github.com/coder/open-storage/commit/0eb03e06fb42a685a75596fa2133af169ace295c))

## [0.11.1](https://github.com/coder/open-storage/compare/v0.11.0...v0.11.1) (2026-08-26)


### Bug Fixes

* **githttp:** disarm LFS proxy-upload body read deadlines after body consumption ([#154](https://github.com/coder/open-storage/issues/154)) ([0f888e7](https://github.com/coder/open-storage/commit/0f888e76695545ea175aed46c90ed9a7dc7f3e17)), closes [#141](https://github.com/coder/open-storage/issues/141)

## [0.11.0](https://github.com/coder/open-storage/compare/v0.10.0...v0.11.0) (2026-08-26)


### Features

* **jobs:** fleet-persistent scan state — observed reap watermarks and resumable cursors ([#132](https://github.com/coder/open-storage/issues/132)) ([daa59c3](https://github.com/coder/open-storage/commit/daa59c33cfa9e273559325abfad80451cca27286))

## [0.10.0](https://github.com/coder/open-storage/compare/v0.9.0...v0.10.0) (2026-08-25)


### Features

* **api:** emit the IETF RateLimit header alongside Retry-After on every 429 ([#151](https://github.com/coder/open-storage/issues/151)) ([d1e8d71](https://github.com/coder/open-storage/commit/d1e8d71acd04a4899c616023fe1d9e72602d2410)), closes [#150](https://github.com/coder/open-storage/issues/150)
* **s3wal:** store qualification harness, conditional CAS primitive, and Phase 1a benches ([#142](https://github.com/coder/open-storage/issues/142)) ([39f1ba7](https://github.com/coder/open-storage/commit/39f1ba7d567cc0a916a75f6e383f53069d6ec84f))

## [0.9.0](https://github.com/coder/open-storage/compare/v0.8.0...v0.9.0) (2026-08-25)


### Features

* **api:** add repo_name to the delete-repo response; refresh vendored OpenAPI snapshot ([#148](https://github.com/coder/open-storage/issues/148)) ([8a8e25d](https://github.com/coder/open-storage/commit/8a8e25d7d7c5af426921b0c3643e3f45026da612)), closes [#147](https://github.com/coder/open-storage/issues/147)

## [0.8.0](https://github.com/coder/open-storage/compare/v0.7.0...v0.8.0) (2026-08-25)


### Features

* **sync:** unify sync egress policy behind one dial-time destination guard ([#144](https://github.com/coder/open-storage/issues/144)) ([5ce9f8f](https://github.com/coder/open-storage/commit/5ce9f8ff946a55a627da1bcd9a087759b6cdafc0)), closes [#139](https://github.com/coder/open-storage/issues/139)

## [0.7.0](https://github.com/coder/open-storage/compare/v0.6.2...v0.7.0) (2026-08-25)


### Features

* **sync:** forward local pushes to the linked upstream (push-upstream) ([#137](https://github.com/coder/open-storage/issues/137)) ([3f985b5](https://github.com/coder/open-storage/commit/3f985b5be5dde9ee031e726d96f0d9aeba584c1f))

## [0.6.2](https://github.com/coder/open-storage/compare/v0.6.1...v0.6.2) (2026-08-25)


### Bug Fixes

* **jobs:** heal truncated registered originals from quarantine and never purge unverifiable backups ([#135](https://github.com/coder/open-storage/issues/135)) ([ca608f0](https://github.com/coder/open-storage/commit/ca608f0eb9810488732ef0cfb59fcaa59dcf4eba))

## [0.6.1](https://github.com/coder/open-storage/compare/v0.6.0...v0.6.1) (2026-08-25)


### Bug Fixes

* **jobs:** spare legacy LFS source when canonical copy is size-deficient ([#130](https://github.com/coder/open-storage/issues/130)) ([fdf9c01](https://github.com/coder/open-storage/commit/fdf9c01c0ab52e8a26a768fe3d076e718973b56f)), closes [#127](https://github.com/coder/open-storage/issues/127)

## [0.6.0](https://github.com/coder/open-storage/compare/v0.5.1...v0.6.0) (2026-08-25)


### Features

* **jobs:** reconcile registered-but-missing blobs against the quota ledger ([#123](https://github.com/coder/open-storage/issues/123)) ([d73e087](https://github.com/coder/open-storage/commit/d73e08746aaeca3cf1dd3fbd546f657856abd80c))

## [0.5.1](https://github.com/coder/open-storage/compare/v0.5.0...v0.5.1) (2026-08-25)


### Bug Fixes

* **githttp:** bound ref materialization in the shared hydration path ([#124](https://github.com/coder/open-storage/issues/124)) ([366d609](https://github.com/coder/open-storage/commit/366d609600a54e1eec4942d8af1078ce189b70c2)), closes [#119](https://github.com/coder/open-storage/issues/119)

## [0.5.0](https://github.com/coder/open-storage/compare/v0.4.0...v0.5.0) (2026-08-24)


### Features

* **lfs:** reclaim registered-but-unreferenced objects ([#117](https://github.com/coder/open-storage/issues/117)) ([c428e82](https://github.com/coder/open-storage/commit/c428e828ffa82a17fcd9f39bcb30a86070238fb7))

## [0.4.0](https://github.com/coder/open-storage/compare/v0.3.1...v0.4.0) (2026-08-24)


### Features

* **blob:** optional expected-bucket-owner enforcement across all S3 operations ([#115](https://github.com/coder/open-storage/issues/115)) ([fe39b73](https://github.com/coder/open-storage/commit/fe39b739324e7379ebc213818f553297dc651f65))

## [0.3.1](https://github.com/coder/open-storage/compare/v0.3.0...v0.3.1) (2026-08-24)


### Bug Fixes

* **lfs:** canonical o2 object keys immune to legacy presigned-PUT replay ([#104](https://github.com/coder/open-storage/issues/104)) ([3cda233](https://github.com/coder/open-storage/commit/3cda233fc435f10220200501dd5b7100bfe3bc92))

## [0.3.0](https://github.com/coder/open-storage/compare/v0.2.0...v0.3.0) (2026-08-23)


### Features

* **storage:** reclaim soft-deleted repositories — retention purge + purge-repo CLI ([#105](https://github.com/coder/open-storage/issues/105)) ([eb057d2](https://github.com/coder/open-storage/commit/eb057d2e28d1159789e0bbfca40a64f5b101e428))

## [0.2.0](https://github.com/coder/open-storage/compare/v0.1.4...v0.2.0) (2026-08-23)


### Features

* **lfs:** quota-metered, digest-verified uploads and lfs/ sweep reconciliation ([#51](https://github.com/coder/open-storage/issues/51)) ([583f369](https://github.com/coder/open-storage/commit/583f369aafbc26d31a6b95d3d13755674cf4cdb0))

## [0.1.4](https://github.com/coder/open-storage/compare/v0.1.3...v0.1.4) (2026-08-23)


### Performance Improvements

* **githttp:** eliminate git subprocess forks from the hydration fast path ([#98](https://github.com/coder/open-storage/issues/98)) ([cab32ca](https://github.com/coder/open-storage/commit/cab32ca5c0ebe431167758312dedf3144000eeda))

## [0.1.3](https://github.com/coder/open-storage/compare/v0.1.2...v0.1.3) (2026-08-23)


### Bug Fixes

* **sync:** lease the sync claim so a dead holder can never wedge syncing ([#89](https://github.com/coder/open-storage/issues/89)) ([8c7ba19](https://github.com/coder/open-storage/commit/8c7ba1943d1bf465c67203970a4491a42e5aced1))

## [0.1.2](https://github.com/coder/open-storage/compare/v0.1.1...v0.1.2) (2026-08-23)


### Bug Fixes

* **sync:** never drop pull-upstream requests accepted while a sync claim is held ([#85](https://github.com/coder/open-storage/issues/85)) ([5ef8da6](https://github.com/coder/open-storage/commit/5ef8da66ba549dc2f4192bf249e98772c0e43b69))

## [0.1.1](https://github.com/coder/open-storage/compare/v0.1.0...v0.1.1) (2026-08-23)


### Bug Fixes

* **githttp:** bound upload-pack gzip negotiation bodies ([f5e6c8f](https://github.com/coder/open-storage/commit/f5e6c8f872b5d3b621aae4cff697a6f2fc7e959c))

## 0.1.0 (2026-08-22)


### Features

* **release:** adopt release-please and ship cross-platform release binaries ([#65](https://github.com/coder/open-storage/issues/65), [#66](https://github.com/coder/open-storage/issues/66)) ([#72](https://github.com/coder/open-storage/issues/72)) ([0af923f](https://github.com/coder/open-storage/commit/0af923fcb0a18ba80f49e605ffbd6e0bdbab703c))
