# ADR-24: NDJSON ingestion is incremental with hard per-frame caps

> Extracted verbatim from [`13-phase4-implementation-notes.md`](../archive/phases/13-phase4-implementation-notes.md)
> (“Phase-4 Implementation Notes & ADRs (write API + p3 read burn-down)”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

commit-pack/diff-commit bodies are parsed line by line (`bufio` reader, hard line cap sized
for 4 MiB raw chunks after base64 + JSON overhead) so chunked uploads without
`Content-Length` stream without body buffering. The first frame must be `metadata`;
per-file `blob_chunk` frames must be contiguous and end with `eof:true`; decoded chunks are
capped at 4 MiB. Chunk interleaving across files is rejected as `invalid` — the SDKs send
contiguous chunks and docs/04 implies a sequence, so the stricter read keeps the parser
single-pass. Malformed frames, unknown `content_id`s, duplicate/missing eof, and oversize
chunks map to `invalid` envelopes, never asserts (untrusted input).
