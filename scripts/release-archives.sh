#!/usr/bin/env bash
# Builds the openstorage release archives: cross-compiles ./cmd/openstorage
# for every supported platform, packages each binary and LICENSE at the root of
# openstorage_<version>_<os>_<arch>.tar.gz, and writes a sha256 checksums.txt
# next to them. Used by .github/workflows/release.yml and runnable locally so
# the tested path is the shipped path.
#
# Usage: scripts/release-archives.sh <tag> <outdir>
#   tag     release tag, e.g. v0.1.0 (stamped into `openstorage --version`)
#   outdir  directory to write archives + checksums.txt into (created)
set -euo pipefail

tag="${1:?usage: release-archives.sh <tag> <outdir>}"
outdir="${2:?usage: release-archives.sh <tag> <outdir>}"

[[ "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+ ]] || {
	echo "release-archives.sh: tag must look like v1.2.3, got: $tag" >&2
	exit 1
}

version="${tag#v}"
commit="$(git rev-parse HEAD)"
date="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

# The build must stay pure Go (modernc.org/sqlite): exporting CGO_ENABLED=0
# is the assertion — any transitive cgo requirement fails the darwin
# cross-compiles below instead of silently linking against host libc.
export CGO_ENABLED=0

module=github.com/coder/open-storage
ldflags="-s -w
	-X ${module}/internal/version.version=${tag}
	-X ${module}/internal/version.commit=${commit}
	-X ${module}/internal/version.date=${date}"

mkdir -p "$outdir"

for platform in linux/amd64 linux/arm64 darwin/amd64 darwin/arm64; do
	goos="${platform%/*}"
	goarch="${platform#*/}"
	workdir="$(mktemp -d)"
	GOOS="$goos" GOARCH="$goarch" go build -trimpath -ldflags "$ldflags" \
		-o "$workdir/openstorage" ./cmd/openstorage
	archive="openstorage_${version}_${goos}_${goarch}.tar.gz"
	# Do not add AppleDouble metadata entries when building archives on macOS.
	COPYFILE_DISABLE=1 tar -czf "$outdir/$archive" -C "$workdir" openstorage -C "$PWD" LICENSE
	rm -rf "$workdir"
	# Layout assertion: the binary and canonical license at the archive root.
	# Keep the binary at the root for mise/ubi-style installers.
	entries="$(tar -tzf "$outdir/$archive")"
	[ "$entries" = $'openstorage\nLICENSE' ] || {
		echo "release-archives.sh: unexpected layout in $archive: $entries" >&2
		exit 1
	}
	tar -xOzf "$outdir/$archive" LICENSE | cmp - LICENSE
	echo "built $archive"
done

# GNU sha256sum format; verifiable with `sha256sum -c` (linux) and
# `shasum -a 256 -c` (macOS).
(cd "$outdir" && sha256sum openstorage_"${version}"_*.tar.gz >checksums.txt)
echo "wrote checksums.txt"
