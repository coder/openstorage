# Changelog

## [0.5.1](https://github.com/coder/code-storage/compare/v0.5.0...v0.5.1) (2026-08-25)


### Bug Fixes

* **githttp:** bound ref materialization in the shared hydration path ([#124](https://github.com/coder/code-storage/issues/124)) ([366d609](https://github.com/coder/code-storage/commit/366d609600a54e1eec4942d8af1078ce189b70c2)), closes [#119](https://github.com/coder/code-storage/issues/119)

## [0.5.0](https://github.com/coder/code-storage/compare/v0.4.0...v0.5.0) (2026-08-24)


### Features

* **lfs:** reclaim registered-but-unreferenced objects ([#117](https://github.com/coder/code-storage/issues/117)) ([c428e82](https://github.com/coder/code-storage/commit/c428e828ffa82a17fcd9f39bcb30a86070238fb7))

## [0.4.0](https://github.com/coder/code-storage/compare/v0.3.1...v0.4.0) (2026-08-24)


### Features

* **blob:** optional expected-bucket-owner enforcement across all S3 operations ([#115](https://github.com/coder/code-storage/issues/115)) ([fe39b73](https://github.com/coder/code-storage/commit/fe39b739324e7379ebc213818f553297dc651f65))

## [0.3.1](https://github.com/coder/code-storage/compare/v0.3.0...v0.3.1) (2026-08-24)


### Bug Fixes

* **lfs:** canonical o2 object keys immune to legacy presigned-PUT replay ([#104](https://github.com/coder/code-storage/issues/104)) ([3cda233](https://github.com/coder/code-storage/commit/3cda233fc435f10220200501dd5b7100bfe3bc92))

## [0.3.0](https://github.com/coder/code-storage/compare/v0.2.0...v0.3.0) (2026-08-23)


### Features

* **storage:** reclaim soft-deleted repositories — retention purge + purge-repo CLI ([#105](https://github.com/coder/code-storage/issues/105)) ([eb057d2](https://github.com/coder/code-storage/commit/eb057d2e28d1159789e0bbfca40a64f5b101e428))

## [0.2.0](https://github.com/coder/code-storage/compare/v0.1.4...v0.2.0) (2026-08-23)


### Features

* **lfs:** quota-metered, digest-verified uploads and lfs/ sweep reconciliation ([#51](https://github.com/coder/code-storage/issues/51)) ([583f369](https://github.com/coder/code-storage/commit/583f369aafbc26d31a6b95d3d13755674cf4cdb0))

## [0.1.4](https://github.com/coder/code-storage/compare/v0.1.3...v0.1.4) (2026-08-23)


### Performance Improvements

* **githttp:** eliminate git subprocess forks from the hydration fast path ([#98](https://github.com/coder/code-storage/issues/98)) ([cab32ca](https://github.com/coder/code-storage/commit/cab32ca5c0ebe431167758312dedf3144000eeda))

## [0.1.3](https://github.com/coder/code-storage/compare/v0.1.2...v0.1.3) (2026-08-23)


### Bug Fixes

* **sync:** lease the sync claim so a dead holder can never wedge syncing ([#89](https://github.com/coder/code-storage/issues/89)) ([8c7ba19](https://github.com/coder/code-storage/commit/8c7ba1943d1bf465c67203970a4491a42e5aced1))

## [0.1.2](https://github.com/coder/code-storage/compare/v0.1.1...v0.1.2) (2026-08-23)


### Bug Fixes

* **sync:** never drop pull-upstream requests accepted while a sync claim is held ([#85](https://github.com/coder/code-storage/issues/85)) ([5ef8da6](https://github.com/coder/code-storage/commit/5ef8da66ba549dc2f4192bf249e98772c0e43b69))

## [0.1.1](https://github.com/coder/code-storage/compare/v0.1.0...v0.1.1) (2026-08-23)


### Bug Fixes

* **githttp:** bound upload-pack gzip negotiation bodies ([f5e6c8f](https://github.com/coder/code-storage/commit/f5e6c8f872b5d3b621aae4cff697a6f2fc7e959c))

## 0.1.0 (2026-08-22)


### Features

* **release:** adopt release-please and ship cross-platform release binaries ([#65](https://github.com/coder/code-storage/issues/65), [#66](https://github.com/coder/code-storage/issues/66)) ([#72](https://github.com/coder/code-storage/issues/72)) ([0af923f](https://github.com/coder/code-storage/commit/0af923fcb0a18ba80f49e605ffbd6e0bdbab703c))
