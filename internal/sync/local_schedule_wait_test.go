package sync

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"
)

// The fixture's runs get the production budget, and its run-bound waits
// last until the frozen store clock refuses a run's token, cut by the test
// binary's -timeout and never shorter than idleMinWait.
func TestLocalSchedFixtureRunBounds(t *testing.T) {
	f := newLocalSchedFixture(t)
	if got := f.wf.worker.Timeout; got != defaultRunTimeout {
		t.Errorf("fixture run Timeout = %v, want defaultRunTimeout %v", got, defaultRunTimeout)
	}
	start := time.Unix(1_000_000_000, 0)
	tokenLife := claimLease(defaultRunTimeout) - claimFenceMargin // 14 min
	for _, tc := range []struct {
		name          string
		created, want time.Time
		testDeadline  time.Time
	}{
		{"the store clock's token life", start.Add(-time.Minute), start.Add(tokenLife - time.Minute), time.Time{}},
		{"a sooner -timeout", start, start.Add(2 * time.Minute), start.Add(3 * time.Minute)},
		{"a later -timeout", start, start.Add(tokenLife), start.Add(time.Hour)},
		{"token life already over", start.Add(-time.Hour), start.Add(idleMinWait), time.Time{}},
		{"-timeout less than the margin away", start, start.Add(idleMinWait), start.Add(30 * time.Second)},
	} {
		if got := runWaitDeadline(start, tc.created, defaultRunTimeout, tc.testDeadline, !tc.testDeadline.IsZero()); !got.Equal(tc.want) {
			t.Errorf("%s: wait ends %v after its start, want %v", tc.name, got.Sub(start), tc.want.Sub(start))
		}
	}
}

// fakeWaitT is a waitT whose Fatal records the message and unwinds.
type fakeWaitT struct {
	deadline time.Time
	msg      string
}

func (*fakeWaitT) Helper()                       {}
func (*fakeWaitT) Name() string                  { return "fake" }
func (w *fakeWaitT) Deadline() (time.Time, bool) { return w.deadline, true }
func (w *fakeWaitT) Fatal(args ...any)           { w.msg = fmt.Sprint(args...); panic(w) }

// A run stuck in its upstream push: idle gives up, writes its message to
// stderr first (go test without -v prints a test's own messages only when
// the test ends), and cancels the run, so that the cleanup's Close can
// return and the test ends with that message.
func TestLocalScheduleIdleFailsWithItsMessage(t *testing.T) {
	f := newLocalSchedFixture(t)
	up := newStallUpstream(t)
	bare := addEmptyUpstream(t, up.root, "own", "wedge")
	testGit(t, bare, "config", "http.receivepack", "true")
	repo, _ := f.addRepo(t, "wedge", up.url)
	if err := f.ms.RequestPush(t.Context(), repo, false); err != nil {
		t.Fatal(err)
	}
	up.hold.Store(true)
	f.sched.Start(0, time.Hour)
	f.held(t, up)
	var stderr bytes.Buffer
	f.stderr = &stderr
	// A -timeout one second past the margin: idle waits idleMinWait.
	ft := &fakeWaitT{deadline: time.Now().Add(idleDeadlineMargin + time.Second)}
	func() {
		defer func() {
			if r := recover(); r != ft {
				panic(r)
			}
		}()
		f.idle(ft)
	}()
	const prefix = "scheduler still has 1 jobs in flight after "
	waited, err := time.ParseDuration(strings.TrimPrefix(ft.msg, prefix))
	if !strings.HasPrefix(ft.msg, prefix) || err != nil || waited < idleMinWait {
		t.Errorf("idle's failure = %q, want %q and at least %v", ft.msg, prefix, idleMinWait)
	}
	if got := stderr.String(); got != "fake: "+ft.msg+"\n" {
		t.Errorf("stderr = %q, want the message first", got)
	}
	f.sched.mu.Lock()
	n := len(f.sched.inflight)
	f.sched.mu.Unlock()
	if n != 0 {
		t.Errorf("%d jobs still in flight after idle failed, want the run cancelled", n)
	}
}
