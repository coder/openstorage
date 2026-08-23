# Changelog

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
