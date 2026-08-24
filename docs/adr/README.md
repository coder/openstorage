# Architecture decision records

Standalone, individually addressable copies of the ADRs recorded in the
phase implementation notes (now archived under
[`../archive/phases/`](../archive/phases)). Extraction was mechanical: each
record is verbatim from its source file, plus a provenance header. ADRs are
historical — they record why a decision was made at the time; where reality
has moved on, the linked issues/PRs in each record's text are the trail.

| ADR | Title | Source |
|---|---|---|
| ADR-1 | [Postgres is the ref authority](001-postgres-is-the-ref-authority.md) | [`08-spike-implementation-notes.md`](../archive/phases/08-spike-implementation-notes.md) |
| ADR-2 | [S3 upload strictly before the ref flip](002-s3-upload-strictly-before-the-ref-flip.md) | [`08-spike-implementation-notes.md`](../archive/phases/08-spike-implementation-notes.md) |
| ADR-3 | [Namespace mapping via separate ref stores](003-namespace-mapping-via-separate-ref-stores.md) | [`08-spike-implementation-notes.md`](../archive/phases/08-spike-implementation-notes.md) |
| ADR-4 | [Frozen contracts first, one integration seam](004-frozen-contracts-first-one-integration-seam.md) | [`09-phase1-implementation-notes.md`](../archive/phases/09-phase1-implementation-notes.md) |
| ADR-5 | [Multi-org schema, single-org serve mode](005-multi-org-schema-single-org-serve-mode.md) | [`09-phase1-implementation-notes.md`](../archive/phases/09-phase1-implementation-notes.md) |
| ADR-6 | [Soft delete via a unique constraint spanning deleted rows](006-soft-delete-via-a-unique-constraint-spanning-deleted-rows.md) | [`09-phase1-implementation-notes.md`](../archive/phases/09-phase1-implementation-notes.md) |
| ADR-7 | [Promote the spike, don't rewrite it](007-promote-the-spike-dont-rewrite-it.md) | [`10-phase2-implementation-notes.md`](../archive/phases/10-phase2-implementation-notes.md) |
| ADR-8 | [Production auth on the git path; git-only serve mode dropped](008-production-auth-on-the-git-path-git-only-serve-mode-dropped.md) | [`10-phase2-implementation-notes.md`](../archive/phases/10-phase2-implementation-notes.md) |
| ADR-9 | [verify-sig delegates to git's own verification machinery](009-verify-sig-delegates-to-gits-own-verification-machinery.md) | [`10-phase2-implementation-notes.md`](../archive/phases/10-phase2-implementation-notes.md) |
| ADR-10 | [Signing-key registry = meta table + CLI (dashboard stand-in)](010-signing-key-registry-meta-table-cli-dashboard-stand-in.md) | [`10-phase2-implementation-notes.md`](../archive/phases/10-phase2-implementation-notes.md) |
| ADR-11 | [Ephemeral namespace policy quirk](011-ephemeral-namespace-policy-quirk.md) | [`10-phase2-implementation-notes.md`](../archive/phases/10-phase2-implementation-notes.md) |
| ADR-12 | [URL-scheme factories with lifecycle interfaces](012-url-scheme-factories-with-lifecycle-interfaces.md) | [`11-local-backends-implementation-notes.md`](../archive/phases/11-local-backends-implementation-notes.md) |
| ADR-13 | [Local mode is intentionally single-node](013-local-mode-is-intentionally-single-node.md) | [`11-local-backends-implementation-notes.md`](../archive/phases/11-local-backends-implementation-notes.md) |
| ADR-14 | [Read plumbing lives in `internal/gitread`, still `os/exec git`](014-read-plumbing-lives-in-internal-gitread-still-os-exec-git.md) | [`12-phase3-implementation-notes.md`](../archive/phases/12-phase3-implementation-notes.md) |
| ADR-15 | [Cursor formats — keyset everywhere, tip-anchored for commits](015-cursor-formats-keyset-everywhere-tip-anchored-for-commits.md) | [`12-phase3-implementation-notes.md`](../archive/phases/12-phase3-implementation-notes.md) |
| ADR-16 | [`refs.created_at` column for `branches[].created_at`](016-refs-created-at-column-for-branches-created-at.md) | [`12-phase3-implementation-notes.md`](../archive/phases/12-phase3-implementation-notes.md) |
| ADR-17 | [File streaming = `http.ServeContent` over a request-scoped temp file](017-file-streaming-http-servecontent-over-a-request-scoped-temp-file.md) | [`12-phase3-implementation-notes.md`](../archive/phases/12-phase3-implementation-notes.md) |
| ADR-18 | [Archive is a custom walk into `archive/tar` + `gzip`, not `git archive`](018-archive-is-a-custom-walk-into-archive-tar-gzip-not-git-archive.md) | [`12-phase3-implementation-notes.md`](../archive/phases/12-phase3-implementation-notes.md) |
| ADR-19 | [Hydrate serializes per pack with an flock; temp-download + atomic rename](019-hydrate-serializes-per-pack-with-an-flock-temp-download-atomic-rename.md) | [`12-phase3-implementation-notes.md`](../archive/phases/12-phase3-implementation-notes.md) |
| ADR-20 | [`requireAuth` takes variadic scopes; all must be present](020-requireauth-takes-variadic-scopes-all-must-be-present.md) | [`12-phase3-implementation-notes.md`](../archive/phases/12-phase3-implementation-notes.md) |
| ADR-21 | [Repos are keyed by `repo_id` everywhere; URL path elements resolve via meta](021-repos-are-keyed-by-repo-id-everywhere-url-path-elements-resolve-via-meta.md) | [`13-phase4-implementation-notes.md`](../archive/phases/13-phase4-implementation-notes.md) |
| ADR-22 | [CAS mismatch = 412, lost race / merge conflict = 409](022-cas-mismatch-412-lost-race-merge-conflict-409.md) | [`13-phase4-implementation-notes.md`](../archive/phases/13-phase4-implementation-notes.md) |
| ADR-23 | [Write pipeline = per-request scratch odb over the cache, then publish](023-write-pipeline-per-request-scratch-odb-over-the-cache-then-publish.md) | [`13-phase4-implementation-notes.md`](../archive/phases/13-phase4-implementation-notes.md) |
| ADR-24 | [NDJSON ingestion is incremental with hard per-frame caps](024-ndjson-ingestion-is-incremental-with-hard-per-frame-caps.md) | [`13-phase4-implementation-notes.md`](../archive/phases/13-phase4-implementation-notes.md) |
| ADR-25 | [Ref policies enforced API-side through the hook's engine](025-ref-policies-enforced-api-side-through-the-hooks-engine.md) | [`13-phase4-implementation-notes.md`](../archive/phases/13-phase4-implementation-notes.md) |
| ADR-26 | [Result envelope statuses; 499 is not emittable](026-result-envelope-statuses-499-is-not-emittable.md) | [`13-phase4-implementation-notes.md`](../archive/phases/13-phase4-implementation-notes.md) |
| ADR-27 | [Jobs are a queue seam — River on Postgres, in-process goroutines in local mode](027-jobs-are-a-queue-seam-river-on-postgres-in-process-goroutines-in-local-mode.md) | [`14-phase5-implementation-notes.md`](../archive/phases/14-phase5-implementation-notes.md) |
| ADR-28 | [Push events are emitted by a CASRef decorator, not by handlers](028-push-events-are-emitted-by-a-casref-decorator-not-by-handlers.md) | [`14-phase5-implementation-notes.md`](../archive/phases/14-phase5-implementation-notes.md) |
| ADR-29 | [Webhook endpoint management is a self-defined CRUD surface](029-webhook-endpoint-management-is-a-self-defined-crud-surface.md) | [`14-phase5-implementation-notes.md`](../archive/phases/14-phase5-implementation-notes.md) |
| ADR-30 | [Upstream sync = mirror fetch + loopback push through our own front door](030-upstream-sync-mirror-fetch-loopback-push-through-our-own-front-door.md) | [`14-phase5-implementation-notes.md`](../archive/phases/14-phase5-implementation-notes.md) |
| ADR-31 | [Forking is synchronous — a one-shot deterministic clone inside POST /repos](031-forking-is-synchronous-a-one-shot-deterministic-clone-inside-post-repos.md) | [`14-phase5-implementation-notes.md`](../archive/phases/14-phase5-implementation-notes.md) |
| ADR-32 | [Upstream credentials and webhook secrets are stored plaintext](032-upstream-credentials-and-webhook-secrets-are-stored-plaintext.md) | [`14-phase5-implementation-notes.md`](../archive/phases/14-phase5-implementation-notes.md) |
| ADR-33 | [Git-push eventing crosses processes — durable River inserts on pg, synchronous deliveries in local mode](033-git-push-eventing-crosses-processes-durable-river-inserts-on-pg-synchronous-deliveries-in-local-mode.md) | [`14-phase5-implementation-notes.md`](../archive/phases/14-phase5-implementation-notes.md) |
| ADR-34 | [LFS lives on the storage host — presigned transfers, proxy fallback](034-lfs-lives-on-the-storage-host-presigned-transfers-proxy-fallback.md) | [`15-phase6-implementation-notes.md`](../archive/phases/15-phase6-implementation-notes.md) |
| ADR-35 | [LFS objects are keyed by base RepoID — namespaces share one object space](035-lfs-objects-are-keyed-by-base-repoid-namespaces-share-one-object-space.md) | [`15-phase6-implementation-notes.md`](../archive/phases/15-phase6-implementation-notes.md) |
| ADR-36 | [Named-repo aliases re-dispatch to the v1 handlers — one implementation, two path styles](036-named-repo-aliases-re-dispatch-to-the-v1-handlers-one-implementation-two-path-styles.md) | [`15-phase6-implementation-notes.md`](../archive/phases/15-phase6-implementation-notes.md) |
| ADR-37 | [Rate limiting is a pre-auth, per-node, per-client-IP token bucket](037-rate-limiting-is-a-pre-auth-per-node-per-client-ip-token-bucket.md) | [`15-phase6-implementation-notes.md`](../archive/phases/15-phase6-implementation-notes.md) |
| ADR-38 | [Runtime image is alpine + external git; the binary is static](038-runtime-image-is-alpine-external-git-the-binary-is-static.md) | [`16-phase7-implementation-notes.md`](../archive/phases/16-phase7-implementation-notes.md) |
| ADR-39 | [Replicas are an opt-in profile of the dev compose file, symmetric by construction](039-replicas-are-an-opt-in-profile-of-the-dev-compose-file-symmetric-by-construction.md) | [`16-phase7-implementation-notes.md`](../archive/phases/16-phase7-implementation-notes.md) |
| ADR-40 | [River migrations run under a session-level advisory lock](040-river-migrations-run-under-a-session-level-advisory-lock.md) | [`16-phase7-implementation-notes.md`](../archive/phases/16-phase7-implementation-notes.md) |
| ADR-41 | [Registered LFS objects live at a canonical key legacy presigned URLs cannot address](041-registered-lfs-objects-live-at-a-canonical-key-legacy-presigned-urls-cannot-address.md) | issue #51, PR #61 |
