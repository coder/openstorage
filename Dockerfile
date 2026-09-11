# Multi-stage build for the openstorage server (see README quickstart and the
# docker-compose.yml `replicas` profile). The runtime stage is alpine rather
# than distroless because the server has a hard runtime dependency on external
# git (smart-HTTP upload/receive-pack, merges, hook re-execs).
# Keep the Go version in sync with mise.toml.
FROM golang:1.27.1-alpine AS build
WORKDIR /src
# Dependencies come from the committed vendor/ tree (issue #114): Go auto-uses
# -mod=vendor when vendor/modules.txt is consistent, so no `go mod download`
# layer or module cache is needed and the build works offline.
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -o /out/openstorage ./cmd/openstorage

FROM alpine:3.24.1@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b
# gnupg + openssh-keygen: verify-sig ref policies shell out through git to
# `gpg` (PGP) and `ssh-keygen` (SSH) and fail closed per ref when the binary
# is absent (ADR-9, #50) — without them every signed push on a protected ref
# is rejected. CI smoke-tests their presence in the built image.
RUN apk add --no-cache git ca-certificates gnupg openssh-keygen \
    && adduser -D -H openstorage
COPY --from=build /out/openstorage /usr/local/bin/openstorage
USER openstorage
ENTRYPOINT ["openstorage"]
CMD ["serve"]
