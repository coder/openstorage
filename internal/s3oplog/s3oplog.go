// Package s3oplog is the per-request S3 round-trip log seam (issue #182):
// when OPENSTORAGE_S3_OP_LOG names a file, every S3 HTTP attempt made
// through the openstorage SDK clients — the s3wal conditional client (ref
// and meta authority) and the blob client — appends one whitespace-
// separated line:
//
//	<unixnano> <pid> <role> <operation> <status> <dur_us> <path>
//
// role is derived from the process argv (serve vs hook subprocesses), so a
// log shared by one push's processes attributes each round trip to its
// origin. Background work tags its context with WithScope, and its round
// trips log as role:scope, so a census can tell them from request work.
// The seam exists to census and regression-guard the per-push S3
// round-trip count; it is diagnostics, not telemetry — unset (the default)
// it installs nothing and costs nothing.
//
// The middleware sits in the Deserialize step (the captureStoreDate
// precedent in internal/ref/s3wal): it wraps only the wire exchange, runs
// once per HTTP attempt (inside the retry loop), and sees the raw response
// status uniformly for every operation.
package s3oplog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/coder/openstorage/internal/envconfig"
)

// Env names the op-log file. Empty (the default) disables the seam.
const Env = "OPENSTORAGE_S3_OP_LOG"

var (
	once    sync.Once
	logFile *os.File
	role    string
)

// setup resolves the env knob once per process. Open failure logs to
// stderr and leaves the seam disabled rather than failing the caller: a
// diagnostics knob must never take down serving (unlike boot-validated
// configuration such as the expected-bucket-owner).
func setup() {
	path := envconfig.Getenv(Env)
	if path == "" {
		return
	}
	if !filepath.IsAbs(path) {
		// The env is inherited by hook subprocesses whose working
		// directory is the cache repo, not the serve process's cwd: a
		// relative path would silently split the census across
		// per-process files. Refuse it outright.
		fmt.Fprintf(os.Stderr, "s3oplog: %s=%q is not an absolute path (op logging disabled)\n", Env, path)
		return
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "s3oplog: open %s: %v (op logging disabled)\n", path, err)
		return
	}
	logFile = f
	role = procRole()
}

// procRole renders this process's identity from argv: the binary base name
// plus up to two leading non-flag arguments — "openstorage:serve",
// "openstorage:hook:pre-receive", "openstorage:hook:update". Whitespace is
// squashed so the line stays field-splittable.
func procRole() string {
	parts := []string{filepath.Base(os.Args[0])}
	for _, a := range os.Args[1:] {
		if strings.HasPrefix(a, "-") || len(parts) == 3 {
			break
		}
		parts = append(parts, a)
	}
	return strings.Join(strings.Fields(strings.Join(parts, ":")), "_")
}

type scopeKey struct{}

// WithScope returns ctx tagged with scope: S3 round trips made under it log
// their role as role:scope. scope must be one non-empty word (no
// whitespace or colon), so the line stays field-splittable.
func WithScope(ctx context.Context, scope string) context.Context {
	if scope == "" || strings.ContainsAny(scope, ": \t\n") {
		panic(fmt.Sprintf("s3oplog.WithScope: invalid scope %q", scope))
	}
	return context.WithValue(ctx, scopeKey{}, scope)
}

// APIOption returns the smithy API option installing the op-log middleware
// and whether the seam is enabled. Callers append it to s3.Options
// .APIOptions exactly like the expected-bucket-owner option.
func APIOption() (func(*middleware.Stack) error, bool) {
	once.Do(setup)
	if logFile == nil {
		return nil, false
	}
	return func(stack *middleware.Stack) error {
		return stack.Deserialize.Add(middleware.DeserializeMiddlewareFunc("openstorageS3OpLog", logOp), middleware.Before)
	}, true
}

// logOp times one wire exchange and appends its line. The write is a
// single O_APPEND syscall, atomic enough for concurrent processes sharing
// one log file; errors are deliberately dropped (diagnostics only).
func logOp(ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler) (middleware.DeserializeOutput, middleware.Metadata, error) {
	start := time.Now()
	out, md, err := next.HandleDeserialize(ctx, in)
	dur := time.Since(start)

	key := "-"
	if req, ok := in.Request.(*smithyhttp.Request); ok && req.URL != nil {
		key = req.URL.Path
		if p := req.URL.Query().Get("prefix"); p != "" {
			key += "?prefix=" + p
		}
	}
	status := 0
	if resp, ok := out.RawResponse.(*smithyhttp.Response); ok && resp != nil && resp.Response != nil {
		status = resp.StatusCode
	}
	r := role
	if scope, ok := ctx.Value(scopeKey{}).(string); ok {
		r += ":" + scope
	}
	line := fmt.Sprintf("%d %d %s %s %d %d %s\n",
		start.UnixNano(), os.Getpid(), r, awsmiddleware.GetOperationName(ctx), status, dur.Microseconds(), key)
	_, _ = logFile.WriteString(line)
	return out, md, err
}
