# `internal/errtext`

## Overview
Bounds the request text that an error answer repeats, so that an answer stays small whatever the request holds. `api` and `githttp` share it: `githttp` cannot import `api`.

## Interface
- `MaxBytes` — 16384, the most bytes of a text that an answer repeats [errtext.go:MaxBytes]
- `Bound` — cuts a longer text at a rune boundary and ends it with `…`; a text of up to `MaxBytes` bytes comes back unchanged [errtext.go:Bound]
- `Cut` — the same cut at a limit that the caller gives [errtext.go:Cut]
- `MaxQuotedBytes` — 256, the most bytes of one request value that an answer quotes [errtext.go:MaxQuotedBytes]
- `Quote` — quotes a server prefix and a request value; a value of more than `MaxQuotedBytes` bytes is cut and followed by its length, `"xx…" (1000000 bytes)` [errtext.go:Quote]

## Invariants
- The cut counts bytes before JSON escapes them, so the answer can hold up to six bytes for each byte kept [errtext.go:MaxBytes]
- The cut never splits a rune of valid UTF-8 [errtext.go:Cut]

## Callers
- `api` cuts `error`, `detail` and each `invalid_params` name of a problem body (`newProblem`), and the `result.message` of a commit write (`wcRefUpdate`) [../api/errors.go:newProblem] [../api/handlers_write_commit.go:wcRefUpdate]
- `api` quotes an unknown member name in an answer that names one unknown member (`unknownNameText`), and cuts each unknown member name at 32 bytes in the answer of a strictly decoded body that names several members (`Cut`) [../api/decode_body.go:unknownNameText] [../api/handlers_write_refs.go:strictProblem.err]
- `api` quotes a refused sync provider, operation, access or host (`upstreamConfig.validate`, `validateSyncBaseRepo`) and the ref policy op of a token in a 403 (`refPolicyOpDenies`) [../api/handlers_sync.go:upstreamConfig.validate] [../api/handlers_sync.go:refPolicyOpDenies]
- `api` quotes a ref policy op that a ref, commit or merge write does not know (`unknown policy op "x" for <ref>`) [../api/handlers_write_refs.go:wrefPolicyCheck] [../api/handlers_write_commit.go:wcCheckRefPolicy] [../api/handlers_write_merge.go:server.wmCheckRefPolicy]
- `events` quotes a webhook URL or its host in a registration refusal, and cuts the parse error it adds after a URL that does not parse (`CheckRegistration`) [../events/guard.go:CheckRegistration]
- `githttp` cuts the error text of the git and LFS routes [../githttp/server.go:httpError] [../githttp/lfs.go:lfsError]
- `githttp` quotes a ref policy op that the update hook does not know, in the refusal that git shows the pusher (`evalRefPolicy`) [../githttp/hook.go:evalRefPolicy]

## Verification
- Run: `go test ./internal/errtext/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m`
