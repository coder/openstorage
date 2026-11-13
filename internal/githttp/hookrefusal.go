package githttp

import (
	"fmt"
	"runtime/debug"
	"slices"

	"github.com/coder/openstorage/internal/envconfig"
	"github.com/coder/openstorage/internal/ref"
)

// git relays a hook's error to the pusher, and an error from a store or its
// transport names the database host, user and name, or the S3 endpoint and
// bucket. So RunHook shows the pusher only refusals: errors whose text the
// code that made them vouches for. Every other error reaches the pusher as
// a fixed line, and its text goes to the push's hook-errors file
// (EnvHookErrors), which serve logs for the operator.

// Refuse marks err as a refusal whose whole text the pusher may read. The
// caller vouches that the text names no store or transport detail: build
// it from fixed text, ref names, SHAs and numbers, never from a store
// error. errors.Is and errors.As still see err.
func Refuse(err error) error {
	assertf(err != nil, "Refuse: nil error")
	return refusal{err}
}

// refusef is Refuse(fmt.Errorf(format, a...)).
func refusef(format string, a ...any) error {
	return Refuse(fmt.Errorf(format, a...))
}

type refusal struct{ err error }

func (r refusal) Error() string { return r.err.Error() }
func (r refusal) Unwrap() error { return r.err }

// pusherSafeSentinels are other packages' leaf errors whose text the
// pusher may read.
var pusherSafeSentinels = []error{ref.ErrRepoPurged, ref.ErrLFSObjectCapExceeded, ref.ErrRefNameNotUTF8}

// ShownToPusher reports whether RunHook returns err to git, which shows it
// to the pusher, unchanged: err is a refusal or one of
// pusherSafeSentinels, or it wraps only errors that are. A wrapper adds
// only its own format text, so a wrapper must not format a store error
// with %v next to a %w.
func ShownToPusher(err error) bool {
	switch e := err.(type) {
	case refusal:
		return true
	case interface{ Unwrap() []error }:
		errs := e.Unwrap()
		if len(errs) == 0 {
			return false
		}
		for _, c := range errs {
			if c == nil || !ShownToPusher(c) {
				return false
			}
		}
		return true
	case interface{ Unwrap() error }:
		if c := e.Unwrap(); c != nil {
			return ShownToPusher(c)
		}
	}
	return slices.Contains(pusherSafeSentinels, err)
}

// hookFailure is what the pusher sees for a hook error that is not
// ShownToPusher. It writes the error to the push's hook-errors file only.
func hookFailure(hook string, err error) error {
	assertf(err != nil, "hookFailure: nil error")
	switch hook {
	case "pre-receive", "update", "post-receive", "reference-transaction":
	default:
		hook = "hook"
	}
	hookErrorsLogger().Error(hook+" failed", "err", err)
	return fmt.Errorf("%s failed; push again later (the server log has the cause)", hook)
}

// recoverHook is deferred by RunHook. A panic in a hook reaches the pusher
// as Go's panic output, which git relays and which names server paths. So
// the pusher gets the fixed hookFailure line, and the panic and its stack
// go to the hook-errors file. serve gives every push that file. A hook
// without it, such as one that hydration starts, has no pusher, and its
// panic keeps going to the operator's stderr. It cannot catch a panic in
// another goroutine.
func recoverHook(hook string, retErr *error) {
	r := recover()
	if r == nil {
		return
	}
	if envconfig.Getenv(EnvHookErrors) == "" {
		panic(r)
	}
	*retErr = hookFailure(hook, fmt.Errorf("panic: %v\n%s", r, debug.Stack()))
}
