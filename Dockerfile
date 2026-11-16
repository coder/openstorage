# Multi-stage build for the openstorage server (see README quickstart and the
# docker-compose.yml `replicas` profile). The runtime stage is alpine rather
# than distroless because the server has a hard runtime dependency on external
# git (smart-HTTP upload/receive-pack, merges, hook re-execs).
# Keep the Go version in sync with mise.toml. Both base images are pinned by
# multi-platform index digest: release images must not depend on a mutable tag.
# The build stage runs on the builder's native platform and cross-compiles
# (pure Go, CGO off), so a multi-platform build emulates only the runtime stage.
FROM --platform=$BUILDPLATFORM golang:1.27.4-alpine@sha256:0a5f7fe796599ff487605987432e7619ae45beeb2fba4c2ce0b907879932aebf AS build
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

FROM alpine:3.24.4@sha256:93b41ef40b1677fce8fcc64ff4af09daddfacf5040afde618bc46c1e2d1cc7d4
# gnupg + openssh-keygen: verify-sig ref policies shell out through git to
# `gpg` (PGP) and `ssh-keygen` (SSH) and fail closed per ref when the binary
# is absent (ADR-9, #50) — without them every signed push on a protected ref
# is rejected. CI smoke-tests their presence in the built image.
# git is not pinned to a version. An Alpine branch serves only the current build
# of a package, so an exact pin would make a release tag unbuildable after the
# next update, and image.yml rebuilds a tag from its own Dockerfile. The image
# jobs run scripts/check-apply-messages.sh against the git that the build got.
# The pinned base holds zlib 1.3.2-r1 (ALPINE-CVE-2026-4738, fixed in 1.4-r0)
# and `apk add` does not upgrade a package that the base already holds. The same
# layer upgrades zlib, without a version pin (see git above). Remove the
# `apk upgrade` line once the pinned alpine base ships zlib 1.4-r0 or newer.
RUN apk add --no-cache git ca-certificates gnupg openssh-keygen \
    && apk upgrade --no-cache zlib \
    && adduser -D -H openstorage
COPY --from=build /out/openstorage /usr/local/bin/openstorage
USER openstorage
ENTRYPOINT ["openstorage"]
CMD ["serve"]
