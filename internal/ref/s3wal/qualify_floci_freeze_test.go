//go:build unix

package s3wal_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// freezeSelf stops this whole process for about d, as a paused runner
// would. A stopped process cannot wake itself, so a child shell resumes it.
// No other test runs meanwhile: the tests that call it are not parallel.
func freezeSelf(d time.Duration) error {
	pid := os.Getpid()
	cmd := exec.Command("sh", "-c", fmt.Sprintf("sleep %.1f; kill -CONT %d", d.Seconds(), pid))
	if err := cmd.Start(); err != nil {
		return err
	}
	defer func() { _ = cmd.Wait() }()
	return syscall.Kill(pid, syscall.SIGSTOP)
}

// TestWatchStallsSeesAFreezeOfTheProcess runs the heartbeat as the surface
// does. The process is awake for 500 ms on each side of its freeze, so a
// heartbeat that stopped ticking would show one span from the start of the
// run, not a span at the freeze.
func TestWatchStallsSeesAFreezeOfTheProcess(t *testing.T) {
	stop := watchStalls(time.Now(), stallTick)
	time.Sleep(500 * time.Millisecond)
	if err := freezeSelf(400 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	spans := stop()
	if len(spans) != 1 {
		t.Fatalf("spans = %v, want the one freeze", spans)
	}
	if got := spans[0].to - spans[0].from; spans[0].from < 450*time.Millisecond || got < 300*time.Millisecond || got > 800*time.Millisecond {
		t.Fatalf("span = %v, want a freeze of about 400 ms that starts after the first 450 ms", spans[0])
	}
}

// TestOverwriteRaceStampsReadsOnTheHeartbeatClock: a read that the process
// freezes inside is stamped on the same clock as the freeze, so the read
// shows how long it was frozen.
func TestOverwriteRaceStampsReadsOnTheHeartbeatClock(t *testing.T) {
	r := memRace(time.Second)
	ok := r.get
	var calls atomic.Int64
	r.get = func(ctx context.Context) ([]byte, string, error) {
		if calls.Add(1) == 20 {
			if err := freezeSelf(400 * time.Millisecond); err != nil {
				t.Error(err)
			}
			return nil, "", fmt.Errorf("read: %w", context.DeadlineExceeded)
		}
		return ok(ctx)
	}
	o := r.run(t.Context())
	if len(o.failures) != 1 {
		t.Fatalf("failures = %v, want the one read that the process froze inside", o.failures)
	}
	f := o.failures[0]
	if f.elapsed() < 350*time.Millisecond || f.stalledFor(o.spans) < 300*time.Millisecond {
		t.Fatalf("read %+v over spans %v, want about 400 ms elapsed and frozen", f, o.spans)
	}
}
