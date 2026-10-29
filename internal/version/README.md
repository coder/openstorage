# `internal/version`

## Overview
Reports the openstorage build version as one line. It combines linker-stamped package variables with Go module build info from `runtime/debug`.

## Interface
- `String` — returns `openstorage <version> (commit <rev>, built <time>, <go version>)` [version.go:String]
- Unexported `version`, `commit`, `date` — set by release `-ldflags -X`; defaults `"devel"`, `""`, `""` [version.go:version]

## Invariants
- `version` is replaced by `bi.Main.Version` only while it is still `"devel"` and the module version is non-empty and not `"(devel)"` [version.go:String]
- `commit` and `date` come from the `vcs.revision` and `vcs.time` build settings only when the linker left them empty, so linker values win [version.go:String]
- Empty commit or built details are omitted; the Go runtime version is always appended [version.go:String]

## Side Effects
- Calls `debug.ReadBuildInfo()` and `runtime.Version()` [version.go:String]

## Verification
- Run: `go test ./internal/version/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m`
