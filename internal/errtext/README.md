# `internal/errtext`

## Overview
Bounds the request text that an error answer repeats, so that an answer stays small whatever the request holds. `api` and `githttp` share it: `githttp` cannot import `api`.

## Interface
- `MaxBytes` — 16384, the most bytes of a text that an answer repeats [errtext.go:MaxBytes]
- `Bound` — cuts a longer text at a rune boundary and ends it with `…`; a text of up to `MaxBytes` bytes comes back unchanged [errtext.go:Bound]

## Invariants
- The cut counts bytes before JSON escapes them, so the answer can hold up to six bytes for each byte kept [errtext.go:MaxBytes]
- The cut never splits a rune of valid UTF-8 [errtext.go:Bound]

## Callers
- `api` cuts `error`, `detail` and each `invalid_params` name of a problem body (`newProblem`), and the `result.message` of a commit write (`wcRefUpdate`) [../api/errors.go:newProblem] [../api/handlers_write_commit.go:wcRefUpdate]
- `githttp` cuts the error text of the git and LFS routes [../githttp/server.go:httpError] [../githttp/lfs.go:lfsError]

## Verification
- Run: `go test ./internal/errtext/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m`
