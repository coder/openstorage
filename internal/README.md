# `internal`

## Overview
The Go packages behind the openstorage server (module `github.com/coder/openstorage`). This directory has no Go files of its own. Requests enter through `api` (REST) and `githttp` (git smart HTTP, Git LFS, and the git hooks). Both rely on `auth` for JWT verification, on `meta` for org, repo, and upstream metadata, on `ref` for the ref authority, pack registry, and quota ledger, and on `blob` for pack and LFS bytes. `gitread` and `gitlimit` run bounded git subprocesses. `jobs` and `sync` run background work: sweeps, purges, repacks, LFS reclamation, webhook delivery through `events`, and upstream mirroring. `simtest` is the deterministic simulation harness over these packages.

## Subdirectories & Boundaries
- `api`: [./api/README.md](./api/README.md) - REST `/api/v1` routes and the `/api/repos/{repo_name}` aliases: auth, rate limits, git-read budgets, repo and ref CRUD, reads, commit, merge, fork, sync, credential, and webhook handlers
- `auth`: [./auth/README.md](./auth/README.md) - JWT claims, scopes, per-org key verifier, token minting, and ref-policy parsing and matching
- `blob`: [./blob/README.md](./blob/README.md) - `BlobStore` contract with S3 and filesystem backends, pack and LFS key layout, usage listing, and restore covers
- `envconfig`: [./envconfig/README.md](./envconfig/README.md) - `OPENSTORAGE_*` environment lookup. It never reads the old `CODESTORAGE_*` names: `[envconfig/envconfig.go:CheckLegacy]` rejects a process that sets one without its `OPENSTORAGE_*` replacement
- `events`: [./events/README.md](./events/README.md) - signed webhook delivery with an SSRF dial guard, and a ref-store decorator that emits push events
- `githttp`: [./githttp/README.md](./githttp/README.md) - git smart-HTTP and LFS server, cache-repo hydration, push holds, and the pre-receive, update, and reference-transaction hooks
- `gitlimit`: [./gitlimit/README.md](./gitlimit/README.md) - admission slots, tracked git process groups, and the Linux heap cap
- `gitread`: [./gitread/README.md](./gitread/README.md) - budgeted read-side git plumbing: trees, blobs, logs, blame, diff, grep, archives, tags, notes
- `jobs`: [./jobs/README.md](./jobs/README.md) - River and in-process job queues, orphan sweeper, repo purge, repack, LFS reclamation, and absence reconciler
- `lfsscan`: [./lfsscan/README.md](./lfsscan/README.md) - LFS pointer parser and bounded reachable-OID walk
- `meta`: [./meta/README.md](./meta/README.md) - metadata store (Postgres, SQLite, S3WAL) for orgs, keys, repos, webhooks, credentials, upstream claim lanes, and the local schedule
- `metrics`: [./metrics/README.md](./metrics/README.md) - Prometheus metrics on a private registry, HTTP instrumentation, and the `/metrics` listener
- `ref`: [./ref/README.md](./ref/README.md) - ref authority (Postgres, SQLite, S3WAL): CAS refs, pack registry, quota ledger, LFS registry, purge, scan state; child `s3wal` holds the conditional-object substrate
- `s3oplog`: [./s3oplog/README.md](./s3oplog/README.md) - opt-in per-process S3 round-trip log
- `secrets`: [./secrets/README.md](./secrets/README.md) - AES-256-GCM envelope encryption with key rotation
- `simtest`: [./simtest/README.md](./simtest/README.md) - deterministic simulation testing of a multi-node cluster with fault injection and invariant oracles
- `sync`: [./sync/README.md](./sync/README.md) - upstream pull sync and push-forward worker, egress proxy, queues, and local scheduler
- `testutil`: [./testutil/README.md](./testutil/README.md) - test helpers: pinned container images and container log capture
- `version`: [./version/README.md](./version/README.md) - one-line build version string
