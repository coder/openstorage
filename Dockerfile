# Multi-stage build for the openstorage server (see README quickstart and the
# docker-compose.yml `replicas` profile). The runtime stage is alpine rather
# than distroless because the server has a hard runtime dependency on external
# git (smart-HTTP upload/receive-pack, merges, hook re-execs).
# Keep the Go version in sync with mise.toml. Both base images are pinned by
# multi-platform index digest: release images must not depend on a mutable tag.
# The build stage runs on the builder's native platform and cross-compiles
# (pure Go, CGO off), so a multi-platform build emulates only the runtime stage.
FROM --platform=$BUILDPLATFORM golang:1.27.3-alpine@sha256:d530cf60723abf1ffae0cd16bb546b379ec0550d2f3815e5b16ed69dbc00fba4 AS build
ARG TARGETOS TARGETARCH
# Release builds (.github/workflows/image.yml) stamp `openstorage --version`
# like scripts/release-archives.sh does. .git is not in the build context, so
# unstamped builds report "devel" without a commit.
ARG VERSION=devel COMMIT= BUILD_DATE=
WORKDIR /src
# Dependencies come from the committed vendor/ tree (issue #114): Go auto-uses
# -mod=vendor when vendor/modules.txt is consistent, so no `go mod download`
# layer or module cache is needed and the build works offline.
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath \
    -ldflags "-X github.com/coder/openstorage/internal/version.version=$VERSION -X github.com/coder/openstorage/internal/version.commit=$COMMIT -X github.com/coder/openstorage/internal/version.date=$BUILD_DATE" \
    -o /out/openstorage ./cmd/openstorage

FROM alpine:3.24.3@sha256:b3413572228e34bd1c9245f5565e6cd127005655e8a7899aef33d8e83255c9e6
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
