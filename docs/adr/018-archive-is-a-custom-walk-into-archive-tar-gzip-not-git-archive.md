# ADR-18: Archive is a custom walk into `archive/tar` + `gzip`, not `git archive`

> Extracted verbatim from [`12-phase3-implementation-notes.md`](../archive/phases/12-phase3-implementation-notes.md)
> (“Phase-3 Implementation Notes & ADRs (read API)”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

`git archive` cannot filter by blob size, so `POST /repos/archive` walks
`ls-tree -r --long`, applies include/exclude globs and `max_blob_size` in one code path, and
streams `cat-file blob` per entry into a tar.gz with the requested prefix
(`internal/gitread/archive.go`). Modes map 100755 → 0755, symlink entries come from blob
content, submodules are skipped. Response is `Content-Type: application/gzip` with
`Content-Disposition` and `Cache-Control: no-store`. Because the prefix is prepended to
every tar entry name, the untrusted `archive.prefix` is validated at the API boundary
(400 for absolute paths, `.`/`..` segments, backslashes, and NUL bytes) and the same
invariant is asserted in `gitread.Archive`, so no caller can produce tar-slip entry names.
