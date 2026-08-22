# Multi-stage build for the codestorage server (see README quickstart and the
# docker-compose.yml `replicas` profile). The runtime stage is alpine rather
# than distroless because the server has a hard runtime dependency on external
# git (smart-HTTP upload/receive-pack, merges, hook re-execs).
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -o /out/codestorage ./cmd/codestorage

FROM alpine:3.21
# gnupg + openssh-keygen: verify-sig ref policies shell out through git to
# `gpg` (PGP) and `ssh-keygen` (SSH) and fail closed per ref when the binary
# is absent (ADR-9, #50) — without them every signed push on a protected ref
# is rejected. CI smoke-tests their presence in the built image.
RUN apk add --no-cache git ca-certificates gnupg openssh-keygen \
    && adduser -D -H codestorage
COPY --from=build /out/codestorage /usr/local/bin/codestorage
USER codestorage
ENTRYPOINT ["codestorage"]
CMD ["serve"]
