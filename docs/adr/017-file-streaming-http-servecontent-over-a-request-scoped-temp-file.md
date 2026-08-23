# ADR-17: File streaming = `http.ServeContent` over a request-scoped temp file

> Extracted verbatim from [`12-phase3-implementation-notes.md`](../archive/phases/12-phase3-implementation-notes.md)
> (“Phase-3 Implementation Notes & ADRs (read API)”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

`GET/HEAD /repos/file` resolves the blob, streams it to a request-scoped temp file, and
hands it to `http.ServeContent`, which implements the RFC 9110 conditional/range matrix
(200/206/304/412/416, `If-Range` both arms, `Content-Range`, HEAD body suppression) once,
correctly, for free. Handlers pre-set the contract headers: strong `ETag` = quoted blob SHA,
`Last-Modified` = last commit touching the path, `X-Blob-Sha`, `X-Last-Commit-Sha`,
`Accept-Ranges: bytes`. Cost is O(blob) temp disk per request — accepted for p3 (the cache
dir is LRU/rebuildable anyway) and recorded as a perf follow-up alongside files-metadata's
per-path `git log -1` (O(page) subprocesses at ≤ page-size limits).
