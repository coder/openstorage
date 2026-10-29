# `internal/envconfig`

## Overview
Resolves `OPENSTORAGE_*` environment settings. It does not read the legacy `CODESTORAGE_*` names as values [envconfig.go:CheckLegacy]. It reports legacy variables that are set without their replacement, and it strips or sets both spellings in child-process environments.

## Interface
- `LookupEnv` — injectable wrapper around `os.LookupEnv`; never logs [envconfig.go:LookupEnv]
- `Getenv` — value-only form; returns `""` when unset [envconfig.go:Getenv]
- `First` — returns the value of the first present name in an alias chain [envconfig.go:First]
- `CheckLegacy` — returns an error listing sorted `CODESTORAGE_*` to `OPENSTORAGE_*` pairs set without the replacement, without values [envconfig.go:CheckLegacy]
- `Strip` — removes both spellings of each canonical name from an env slice [envconfig.go:Strip]
- `Set` — replaces both spellings of a name with one canonical `name=value` entry [envconfig.go:Set]
- Prefix literals `OPENSTORAGE_` and `CODESTORAGE_` [envconfig.go:prefix]

## Invariants
- `legacyName` panics unless the name starts with `OPENSTORAGE_` [envconfig.go:legacyName]
- `First` stops at the first present name, including an explicitly empty one [envconfig.go:First]
- `CheckLegacy` reports only legacy names whose replacement is unset, deduplicated and sorted, and returns nil when there are none [envconfig.go:CheckLegacy]
- `Set` removes both spellings before it appends the canonical value [envconfig.go:Set]

## Side Effects
- Reads the process environment through `os.LookupEnv` and `os.Environ` [envconfig.go:CheckLegacy]
- `CheckLegacy` must not run in hook subprocesses, because git relays hook stderr to the pushing client [envconfig.go:CheckLegacy]
- `Strip` mutates the caller's slice in place; `Set` appends to it [envconfig.go:Strip]

## Verification
- Run: `go test ./internal/envconfig/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m`
