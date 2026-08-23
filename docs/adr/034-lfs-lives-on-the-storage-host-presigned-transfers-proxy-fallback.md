# ADR-34: LFS lives on the storage host — presigned transfers, proxy fallback

> Extracted verbatim from [`15-phase6-implementation-notes.md`](../archive/phases/15-phase6-implementation-notes.md)
> (“Phase-6 Implementation Notes & ADRs (LFS, named-repo aliases, repo-urls, rate limiting)”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

docs/05 puts LFS on the git remote itself: git-lfs discovers
`POST /{repo}.git/info/lfs/objects/batch` relative to the clone URL. Decision: **the
smart-HTTP server implements the Batch API directly — no separate LFS server, no extra
client config** — and transfer actions are **presigned blob URLs when the backend can
sign them, else proxy endpoints back through this server**.

- **Auth.** The batch body is parsed (size-bounded, gzip-aware) *before* authentication
  because the `operation` field selects the required scope: `download` → `git:read`,
  `upload` → `git:write`, with the docs/02 401/403 failure mapping and the
  `LFS-Authenticate: Basic` discovery challenge on 401. Credentials arrive as Basic with
  username `t` (the git remote-URL form) or as a Bearer token; the shared claim check was
  split out of `authenticate()` into `verifyClaims()` so both paths use it.
- **Presigned path (S3/MinIO).** `blob.SignedURL(ctx, key, method, 15m)` presigns GET/PUT
  via gocloud. Content-Type is deliberately left unsigned: aws-sdk-v2 cannot sign it for
  presigned PUTs, and an unsigned Content-Type lets git-lfs's
  `application/octet-stream` PUT verify against MinIO SigV4. The server never touches
  object bytes — transfers go client↔S3.
- **Proxy path (local mode).** `fileblob` cannot presign and returns the
  `blob.ErrSignedURLUnsupported` sentinel; the action then points at
  `GET/PUT /{repo}.git/info/lfs/objects/{oid}` on this server, echoing the caller's
  `Authorization` header in the action headers so git-lfs re-presents the same
  credentials. Proxy uploads recompute the sha256 while streaming to a scratch file and
  **refuse to publish on mismatch** — the oid IS the content address, and publishing
  wrong bytes under it would corrupt every future download of that oid. This is ADR-2's
  ordering applied to LFS: verified content lands in the blob store atomically
  (tmp+rename), or not at all.
- **Batch semantics.** Only the `basic` transfer adapter (422 otherwise) and `sha256`
  (409 per the spec's hash_algo negotiation). Existence is a `blob.Stat` probe: download
  of a missing oid is a per-object 404 error entry; upload of a present oid returns no
  actions, which tells the client to skip the transfer. Download sizes are the
  authoritative stored sizes. Unknown `info/lfs/*` subpaths (e.g. the optional locking
  API's `locks/verify` probe) answer 404, which git-lfs treats as "not supported".
