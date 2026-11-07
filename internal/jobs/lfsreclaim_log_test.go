package jobs

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/ref"
)

// A run that is not turned away by the one-run-at-a-time guard does not say it
// was: a normal run, a run whose walk fails, a run whose walk panics and the
// run after the panic all log no "already running" line, so the line means an
// overlap and nothing else.
func TestLFSReclaim_OnlyATurnedAwayRunLogsTheOverlapWarning(t *testing.T) {
	ctx := context.Background()
	store, reg := newReclaimStore(t)
	base := "quiet-repo"
	key := "lfs/" + base + "/o2/" + strings.Repeat("ab", 32)
	if err := store.RegisterLFSObject(ctx, base, key, 10); err != nil {
		t.Fatal(err)
	}
	if won, err := store.CASRef(ctx, base, "refs/heads/main", ref.ZeroSHA, strings.Repeat("88", 20)); err != nil || !won {
		t.Fatalf("CASRef = (%v, %v)", won, err)
	}
	instantHydrate := func(context.Context, string, string) (string, error) { return t.TempDir(), nil }
	mode := "ok"
	walk := func(context.Context, string, int64) (map[string]struct{}, int64, error) {
		switch mode {
		case "fail":
			return nil, 0, errors.New("simulated walk failure")
		case "panic":
			panic("simulated walk crash")
		}
		return map[string]struct{}{}, 0, nil
	}
	var logs lockedBuffer
	r := NewLFSReclaimer(reg, instantHydrate, walk, alwaysMain,
		time.Hour, 1_000, true, true, slog.New(slog.NewTextHandler(&logs, nil)))

	run := func() (panicked bool) {
		defer func() { panicked = recover() != nil }()
		_, _ = r.Run(ctx)
		return false
	}
	for _, step := range []struct {
		mode      string
		wantPanic bool
	}{{"ok", false}, {"fail", false}, {"panic", true}, {"ok", false}} {
		mode = step.mode
		if got := run(); got != step.wantPanic {
			t.Fatalf("run in mode %q panicked = %v, want %v", step.mode, got, step.wantPanic)
		}
		if n := strings.Count(logs.String(), "a pass is already running"); n != 0 {
			t.Fatalf("a run in mode %q logged the overlap warning %d times:\n%s", step.mode, n, logs.String())
		}
	}
}
