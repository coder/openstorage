# ADR-38: Runtime image is alpine + external git; the binary is static

> Extracted verbatim from [`16-phase7-implementation-notes.md`](../archive/phases/16-phase7-implementation-notes.md)
> (“Phase-7 Implementation Notes & ADRs (deployment packaging + p6 burn-down)”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

Decision: **multi-stage build — `golang:1.26-alpine` with `CGO_ENABLED=0 -trimpath` and
module/build cache mounts, then an `alpine:3.21` runtime stage with `git` and
`ca-certificates`, a non-root user, `ENTRYPOINT ["codestorage"]` / `CMD ["serve"]`.**

- **Alpine, not distroless/scratch.** The server has a hard runtime dependency on
  external `git` (smart-HTTP upload/receive-pack, merges, hook re-execs); a
  binary-only image cannot serve the git surface at all. Alpine is the smallest base
  that carries a packaged git.
- **`CGO_ENABLED=0` is safe and desired.** Both storage stacks are pure Go (pgx,
  gocloud, `modernc.org/sqlite`), so disabling cgo costs nothing and yields a static
  binary that is independent of the runtime image's libc.
- **`CMD ["serve"]` only defaults the subcommand**: the same image runs `keygen`,
  `mint`, `signing-key`, etc. via `docker run <img> <subcommand>` — the compose
  quickstart deliberately mints tokens with the *host* binary instead, so tokens and
  private keys never depend on a running container.
