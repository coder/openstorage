package githttp

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/coder/openstorage/internal/envconfig"
)

// EnvHookErrors carries the path of this push's hook-errors file. git
// relays a hook's stderr and its error to the pusher, and a store error
// names the database host, user and name, or the S3 endpoint and bucket.
// So the hooks write such errors to this file instead, and serve logs them
// for the operator after receive-pack ends. serve creates the file in the
// push's private state directory and strips the variable from inherited
// env like the other per-push inputs.
const EnvHookErrors = "OPENSTORAGE_GIT_HOOK_ERRORS"

// hookErrorsName is the hook-errors file's name in the push's state
// directory.
const hookErrorsName = "hook-errors"

// maxLoggedHookErrors bounds how many hook-errors records serve logs for
// one push: a push of many refs while the store is down writes one record
// per update hook.
const maxLoggedHookErrors = 100

// NewHookLogHandler returns the slog handler of a hook process. stderr
// (which reaches the pusher) gets each record without its error-valued
// attributes. The hook-errors file (EnvHookErrors) gets the whole record.
// Without that file, as for a hook that runs outside a push, the error
// attributes are dropped: they never fall back to stderr.
func NewHookLogHandler(stderr io.Writer, opts *slog.HandlerOptions) slog.Handler {
	assertf(stderr != nil, "NewHookLogHandler: nil stderr")
	h := hookLogHandler{pusher: slog.NewTextHandler(stderr, opts)}
	if path := envconfig.Getenv(EnvHookErrors); path != "" {
		h.operator = slog.NewTextHandler(hookErrorsWriter(path), opts)
	}
	return h
}

type hookLogHandler struct {
	pusher   slog.Handler
	operator slog.Handler // nil without a hook-errors file
}

func (h hookLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.pusher.Enabled(ctx, level)
}

func (h hookLogHandler) Handle(ctx context.Context, r slog.Record) error {
	if h.operator != nil {
		// A lost record must not fail the hook: the pusher's line below
		// still names the failed step.
		_ = h.operator.Handle(ctx, r.Clone())
	}
	pr := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	r.Attrs(func(a slog.Attr) bool {
		if a, ok := withoutErrors(a); ok {
			pr.AddAttrs(a)
		}
		return true
	})
	return h.pusher.Handle(ctx, pr)
}

func (h hookLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	var kept []slog.Attr
	for _, a := range attrs {
		if a, ok := withoutErrors(a); ok {
			kept = append(kept, a)
		}
	}
	out := hookLogHandler{pusher: h.pusher.WithAttrs(kept)}
	if h.operator != nil {
		out.operator = h.operator.WithAttrs(attrs)
	}
	return out
}

func (h hookLogHandler) WithGroup(name string) slog.Handler {
	out := hookLogHandler{pusher: h.pusher.WithGroup(name)}
	if h.operator != nil {
		out.operator = h.operator.WithGroup(name)
	}
	return out
}

// withoutErrors returns a without its error values, and false when nothing
// is left of it.
func withoutErrors(a slog.Attr) (slog.Attr, bool) {
	a.Value = a.Value.Resolve()
	switch a.Value.Kind() {
	case slog.KindAny:
		if _, isErr := a.Value.Any().(error); isErr {
			return slog.Attr{}, false
		}
	case slog.KindGroup:
		var kept []slog.Attr
		for _, g := range a.Value.Group() {
			if g, ok := withoutErrors(g); ok {
				kept = append(kept, g)
			}
		}
		if len(kept) == 0 {
			return slog.Attr{}, false
		}
		a.Value = slog.GroupValue(kept...)
	}
	return a, true
}

// hookErrorsWriter appends each write to the hook-errors file at its path.
// It writes only to an existing regular file: it never creates one, and it
// does not follow a symlink. The file lives in serve's private 0700 state
// directory, so no other user can swap it between the check and the open.
// A write that cannot reach the file is dropped.
type hookErrorsWriter string

func (w hookErrorsWriter) Write(b []byte) (int, error) {
	if fi, err := os.Lstat(string(w)); err != nil || !fi.Mode().IsRegular() {
		return len(b), nil
	}
	f, err := os.OpenFile(string(w), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return len(b), nil
	}
	defer f.Close()
	return f.Write(b)
}

// createHookErrors creates the empty hook-errors file (mode 0600) in dir,
// the push's private state directory, and returns its path. O_EXCL
// refuses a file or symlink that is already there.
func createHookErrors(dir string) (string, error) {
	path := filepath.Join(dir, hookErrorsName)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", err
	}
	return path, f.Close()
}

// logHookErrors logs each record of the hook-errors file at path, prefixed
// with prefix, up to maxLoggedHookErrors records and a count of the rest.
// It reads only a regular file, so it does not follow a symlink.
func logHookErrors(prefix, path string) {
	fi, err := os.Lstat(path)
	if err == nil && !fi.Mode().IsRegular() {
		err = fmt.Errorf("%s is not a regular file", path)
	}
	var f *os.File
	if err == nil {
		f, err = os.Open(path)
	}
	if err != nil {
		log.Printf("%s: hook errors: %v", prefix, err)
		return
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1<<20) // one record is one slog line
	logged, dropped := 0, 0
	for sc.Scan() {
		if logged == maxLoggedHookErrors {
			dropped++
			continue
		}
		log.Printf("%s: hook: %s", prefix, sc.Text())
		logged++
	}
	if err := sc.Err(); err != nil {
		log.Printf("%s: hook errors: %v", prefix, err)
	}
	if dropped > 0 {
		log.Printf("%s: hook: %d more records not logged", prefix, dropped)
	}
}
