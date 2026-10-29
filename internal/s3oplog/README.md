# `internal/s3oplog`

## Overview
Opt-in per-process S3 round-trip log for diagnostics. When `OPENSTORAGE_S3_OP_LOG` names an absolute file, a smithy Deserialize middleware appends one line per HTTP attempt, tagged with the process role and an optional scope. When the variable is unset, nothing is installed.

## Interface
- `Env` — env var name `OPENSTORAGE_S3_OP_LOG` [s3oplog.go:Env]
- `APIOption` — returns the smithy API option that adds the `openstorageS3OpLog` middleware, plus an enabled flag; nil, false when disabled [s3oplog.go:APIOption]
- `WithScope` — tags a context so its S3 round trips log as `role:scope` [s3oplog.go:WithScope]
- `Scopes` — returns a copy of the registered scope list [s3oplog.go:Scopes]
- `ScopeSchedule`, `ScopeKeyRefresh` — scope constants `schedule` and `keyrefresh` [s3oplog.go:ScopeSchedule]

## Invariants
- `WithScope` panics on an unknown scope [s3oplog.go:WithScope]
- A relative log path is refused and logging stays disabled; an open failure is reported to stderr and also leaves logging disabled [s3oplog.go:setup]
- `setup` runs at most once per process through `sync.Once` [s3oplog.go:APIOption]
- `logOp` drops write errors, logs status 0 when there is no HTTP response, and `-` when there is no URL [s3oplog.go:logOp]
- `procRole` stops at the first flag argument and after three parts, and squashes whitespace so lines stay field-splittable [s3oplog.go:procRole]

## Side Effects
- Reads `OPENSTORAGE_S3_OP_LOG` through `envconfig.Getenv` [s3oplog.go:setup]
- Opens the log file with `O_CREATE|O_WRONLY|O_APPEND`, mode 0644, and keeps it for the process lifetime [s3oplog.go:setup]
- Appends one line per S3 attempt: timestamp, pid, role, operation, status, duration, path [s3oplog.go:logOp]
- Reads `os.Args` [s3oplog.go:procRole] and `os.Getpid` [s3oplog.go:logOp]

## Verification
- Run: `go test ./internal/s3oplog/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m`
