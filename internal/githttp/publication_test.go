package githttp

// Regression coverage for the bounded push-publication wait (issue #331).
// Every test drives the real primitive (publicationWait.wait, or the
// exported WaitForPushPublication) over hold files written by the real
// hold writers; the only seams are the per-wait limits and two
// observation hooks (before a file read, after a poll).

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// pubHold mints a hold in repo and records names through the real
// reservation writer (appendPushHolds bypasses reservePushHolds' name
// validation so fixtures can use tiny or duplicate names).
func pubHold(t *testing.T, repo string, names ...string) string {
	t.Helper()
	hold, err := mintPushHold(repo)
	if err != nil {
		t.Fatalf("mintPushHold: %v", err)
	}
	if err := appendPushHolds(hold, names); err != nil {
		t.Fatalf("appendPushHolds: %v", err)
	}
	return hold
}

func pubRelease(t *testing.T, hold string, names ...string) {
	t.Helper()
	if err := appendPushReleased(hold, names); err != nil {
		t.Fatalf("appendPushReleased: %v", err)
	}
}

func pubAppend(t *testing.T, path string, b []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.Write(b); err != nil {
		t.Fatalf("append %s: %v", path, err)
	}
}

func pubLimits(bytes, records int64) publicationLimits {
	return publicationLimits{passBytes: bytes, passRecords: records, waitBytes: 64 * bytes, waitRecords: 64 * records}
}

var pubRoomy = pubLimits(1<<20, 1<<16)

// pubNames returns n distinct short ref-like names.
func pubNames(prefix string, n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = prefix + strconv.Itoa(i)
	}
	return names
}

func pubSize(names []string) int64 {
	var n int64
	for _, name := range names {
		n += int64(len(name)) + 1
	}
	return n
}

// pubFirstVerdict runs the wait until its first completed poll and
// reports that poll's verdict: a held first poll is cancelled right away,
// so "held" is observed deterministically instead of by timeout.
func pubFirstVerdict(t *testing.T, w *publicationWait) (held bool, err error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	polled := false
	w.afterPass = func(h bool) {
		if !polled {
			polled, held = true, h
		}
		cancel()
	}
	err = w.wait(ctx)
	if polled && held {
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("held poll then cancel: err = %v, want context.Canceled", err)
		}
		return true, nil
	}
	return false, err
}

func pubRequireLimit(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrPushPublicationUnverified) || !errors.Is(err, errPublicationScanLimit) {
		t.Fatalf("err = %v, want ErrPushPublicationUnverified wrapping errPublicationScanLimit", err)
	}
}

func TestPushPublicationWait_AbsentEmptyAndReleasedProceed(t *testing.T) {
	ctx := t.Context()

	// Absent hold directory: nothing was ever pushed through this cache.
	repo := t.TempDir()
	dir := filepath.Join(repo, pushHoldDir)
	w := newPublicationWait(dir, pubRoomy)
	if err := w.wait(ctx); err != nil || w.passes != 1 {
		t.Fatalf("absent dir: err=%v passes=%d, want nil after one poll", err, w.passes)
	}

	// Empty directory, an empty hold file, orphan sidecars (an unreserved
	// marker directory and a tombstone file), and a hold whose every name
	// was released: no live held name, so publication is complete.
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := newPublicationWait(dir, pubRoomy).wait(ctx); err != nil {
		t.Fatalf("empty dir: %v", err)
	}
	pubHold(t, repo)
	if err := os.MkdirAll(filepath.Join(dir, "push-orphan"+pushHoldUnreservedSuffix), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "push-orphan"+pushHoldReleasedSuffix), []byte("refs/heads/x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	released := pubHold(t, repo, "refs/heads/a", "refs/heads/b", "refs/heads/a")
	pubRelease(t, released, "refs/heads/b", "refs/heads/a")
	if err := newPublicationWait(dir, pubRoomy).wait(ctx); err != nil {
		t.Fatalf("empty/released holds: %v", err)
	}

	// Blank lines are records but not names.
	pubAppend(t, released, []byte("\n\n\n"))
	if err := newPublicationWait(dir, pubRoomy).wait(ctx); err != nil {
		t.Fatalf("blank lines: %v", err)
	}
}

func TestPushPublicationWait_HeldUntilPublished(t *testing.T) {
	for _, tc := range []struct {
		name    string
		publish func(t *testing.T, hold string)
	}{
		{"hold removed", func(t *testing.T, hold string) { removePushHoldFiles(hold) }},
		{"last name tombstoned", func(t *testing.T, hold string) { pubRelease(t, hold, "refs/heads/main") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := t.TempDir()
			hold := pubHold(t, repo, "refs/heads/done", "refs/heads/main")
			pubRelease(t, hold, "refs/heads/done")

			// Lockstep: afterPass hands the verdict over, then blocks
			// until the next nextPoll resumes it, so every change the test
			// makes between two nextPoll calls lands strictly between
			// passes. Without the resume, the wait ran on into its next
			// pass while the test was still publishing (issue #356).
			polls := make(chan bool)
			resume := make(chan struct{})
			w := newPublicationWait(filepath.Join(repo, pushHoldDir), pubRoomy)
			w.afterPass = func(held bool) {
				polls <- held
				<-resume
			}
			done := make(chan error, 1)
			go func() { done <- w.wait(t.Context()) }()
			paused := false // the wait is blocked in afterPass
			nextPoll := func() bool {
				t.Helper()
				if paused {
					resume <- struct{}{}
				}
				select {
				case held := <-polls:
					paused = true
					return held
				case err := <-done:
					t.Fatalf("wait returned %v before the expected poll", err)
					return false
				}
			}

			for i := range 3 {
				if held := nextPoll(); !held {
					t.Fatalf("poll %d: not held before publication", i)
				}
			}
			select {
			case err := <-done:
				t.Fatalf("wait returned %v while the accepted update was unpublished", err)
			default:
			}
			// The quarantine name used by the TTL reaper is still a hold.
			if err := os.Rename(hold, hold+".reaping"); err != nil {
				t.Fatal(err)
			}
			if held := nextPoll(); !held {
				t.Fatal("a quarantined (.reaping) hold stopped holding")
			}
			if err := os.Rename(hold+".reaping", hold); err != nil {
				t.Fatal(err)
			}
			if held := nextPoll(); !held {
				t.Fatal("restored hold stopped holding")
			}
			// Outlast the poll interval before publishing: a lockstep
			// that let the wait run ahead would now report a stale held
			// verdict every time instead of rarely under load.
			time.Sleep(2 * publicationPollInterval)
			tc.publish(t, hold)
			if held := nextPoll(); held {
				t.Fatal("still held after publication")
			}
			resume <- struct{}{} // let the final pass return
			if err := <-done; err != nil {
				t.Fatalf("wait after publication: %v", err)
			}
		})
	}
}

func TestPushPublicationWait_CancellationAndDeadline(t *testing.T) {
	repo := t.TempDir()
	pubHold(t, repo, "refs/heads/main")
	dir := filepath.Join(repo, pushHoldDir)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	w := newPublicationWait(dir, pubRoomy)
	if err := w.wait(ctx); !errors.Is(err, context.Canceled) || w.passes != 0 {
		t.Fatalf("pre-cancelled: err=%v passes=%d, want Canceled before any poll", err, w.passes)
	}

	held, err := pubFirstVerdict(t, newPublicationWait(dir, pubRoomy))
	if err != nil || !held {
		t.Fatalf("cancel while held: held=%v err=%v", held, err)
	}

	ctx, cancel = context.WithTimeout(t.Context(), 120*time.Millisecond)
	defer cancel()
	err = newPublicationWait(dir, pubRoomy).wait(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrPushPublicationUnverified) {
		t.Fatalf("deadline while held: err = %v, want bare DeadlineExceeded", err)
	}
}

// TestPushPublicationWait_ByteBudget pins the per-poll byte budget below,
// at, and above the exact bytes a fully released hold needs (hold plus
// tombstone sidecar), and that an over-budget poll reads at most one
// probe byte past the limit.
func TestPushPublicationWait_ByteBudget(t *testing.T) {
	repo := t.TempDir()
	names := pubNames("refs/heads/b", 200)
	hold := pubHold(t, repo, names...)
	pubRelease(t, hold, names...)
	need := 2 * pubSize(names)
	dir := filepath.Join(repo, pushHoldDir)

	for _, limit := range []int64{need + 1, need} {
		w := newPublicationWait(dir, pubLimits(limit, 1<<16))
		if err := w.wait(t.Context()); err != nil {
			t.Fatalf("limit %d (need %d): %v", limit, need, err)
		}
		if w.bytes != need {
			t.Fatalf("limit %d: read %d bytes, want exactly %d", limit, w.bytes, need)
		}
	}
	w := newPublicationWait(dir, pubLimits(need-1, 1<<16))
	pubRequireLimit(t, w.wait(t.Context()))
	if w.bytes > need {
		t.Fatalf("over-budget poll read %d bytes, limit %d (+1 probe)", w.bytes, need-1)
	}
}

// TestPushPublicationWait_RecordCap pins the per-poll record cap with
// tiny names: directory entries, every tombstone line, and every hold
// line up to the verdict count, including duplicates and blank lines.
func TestPushPublicationWait_RecordCap(t *testing.T) {
	t.Run("duplicates", func(t *testing.T) {
		repo := t.TempDir()
		dups := make([]string, 500)
		for i := range dups {
			dups[i] = "x"
		}
		hold := pubHold(t, repo, dups...)
		pubRelease(t, hold, "x")
		need := int64(2 + 1 + len(dups)) // hold + sidecar entries, one tombstone, every hold line
		dir := filepath.Join(repo, pushHoldDir)
		w := newPublicationWait(dir, pubLimits(1<<20, need))
		if err := w.wait(t.Context()); err != nil {
			t.Fatalf("cap %d: %v", need, err)
		}
		w = newPublicationWait(dir, pubLimits(1<<20, need-1))
		pubRequireLimit(t, w.wait(t.Context()))
	})
	t.Run("distinct tombstones", func(t *testing.T) {
		repo := t.TempDir()
		names := pubNames("", 1000) // "0".."999": one to three bytes each
		hold := pubHold(t, repo, "refs/heads/live")
		pubRelease(t, hold, names...)
		// Held after two entries, every tombstone, and the first hold line.
		need := int64(2 + len(names) + 1)
		dir := filepath.Join(repo, pushHoldDir)
		held, err := pubFirstVerdict(t, newPublicationWait(dir, pubLimits(1<<20, need)))
		if err != nil || !held {
			t.Fatalf("cap %d: held=%v err=%v", need, held, err)
		}
		w := newPublicationWait(dir, pubLimits(1<<20, need-1))
		pubRequireLimit(t, w.wait(t.Context()))
		// The cap is enforced before each record is retained: the failing
		// poll charged no more records than the cap allows.
		if w.records > need-1 {
			t.Fatalf("charged %d content records past a cap of %d", w.records, need-1)
		}
	})
	t.Run("blank lines", func(t *testing.T) {
		repo := t.TempDir()
		hold := pubHold(t, repo)
		pubAppend(t, hold, bytes.Repeat([]byte("\n"), 300))
		dir := filepath.Join(repo, pushHoldDir)
		if err := newPublicationWait(dir, pubLimits(1<<20, 1+300)).wait(t.Context()); err != nil {
			t.Fatalf("at cap: %v", err)
		}
		pubRequireLimit(t, newPublicationWait(dir, pubLimits(1<<20, 300)).wait(t.Context()))
	})
}

// TestPushPublicationWait_DefaultLimits drives the exported entry point
// with the production limits: a released hold at exactly the default
// record cap passes, one tiny name more fails closed.
func TestPushPublicationWait_DefaultLimits(t *testing.T) {
	lim := defaultPublicationLimits
	perSide := int((lim.passRecords - 2) / 2) // two entries, then tombstones + hold lines
	for _, tc := range []struct {
		n    int
		fail bool
	}{{perSide, false}, {perSide + 1, true}} {
		cache := t.TempDir()
		repo := RepoPath(cache, "r")
		names := pubNames("", tc.n)
		hold := pubHold(t, repo, names...)
		pubRelease(t, hold, names...)
		err := WaitForPushPublication(t.Context(), cache, "r")
		if tc.fail {
			pubRequireLimit(t, err)
		} else if err != nil {
			t.Fatalf("%d names per side under the default cap: %v", tc.n, err)
		}
	}
}

// TestPushPublicationWait_LargeRecord pins single-record bounds: a record
// is at most pushRefPathMax bytes (what the git files backend can
// materialize), and a record larger than the remaining byte budget fails
// without being buffered whole.
func TestPushPublicationWait_LargeRecord(t *testing.T) {
	name := "refs/heads/" + strings.Repeat("n", pushRefPathMax-len("refs/heads/"))
	for _, tc := range []struct {
		name string
		fail bool
	}{{name, false}, {name + "n", true}} {
		repo := t.TempDir()
		hold := pubHold(t, repo, tc.name)
		pubRelease(t, hold, tc.name)
		w := newPublicationWait(filepath.Join(repo, pushHoldDir), pubRoomy)
		err := w.wait(t.Context())
		if tc.fail {
			pubRequireLimit(t, err)
		} else if err != nil {
			t.Fatalf("%d-byte record: %v", len(tc.name), err)
		}
	}

	repo := t.TempDir()
	pubHold(t, repo, "refs/heads/"+strings.Repeat("h", 1000))
	w := newPublicationWait(filepath.Join(repo, pushHoldDir), pubLimits(256, 1<<16))
	pubRequireLimit(t, w.wait(t.Context()))
	if w.bytes > 257 {
		t.Fatalf("read %d bytes of one oversized record under a 256-byte budget", w.bytes)
	}
}

// TestPushPublicationWait_GrowthAfterStat grows a hold file or its
// tombstone sidecar between the listing's stat and the read: growth
// within the budget is honored, growth past it fails closed with the
// read bounded at the limit.
func TestPushPublicationWait_GrowthAfterStat(t *testing.T) {
	const limit = 4096
	for _, target := range []string{"hold", "sidecar"} {
		t.Run(target, func(t *testing.T) {
			repo := t.TempDir()
			hold := pubHold(t, repo, "x")
			pubRelease(t, hold, "x")
			path := hold
			if target == "sidecar" {
				path = hold + pushHoldReleasedSuffix
			}
			dir := filepath.Join(repo, pushHoldDir)

			w := newPublicationWait(dir, pubLimits(limit, 1<<20))
			w.beforeRead = func(p string) {
				if p == path {
					pubAppend(t, path, bytes.Repeat([]byte("x\n"), 10*limit))
				}
			}
			pubRequireLimit(t, w.wait(t.Context()))
			if w.bytes > limit+1 {
				t.Fatalf("read %d bytes after growth, limit %d (+1 probe)", w.bytes, limit)
			}
		})
	}

	// Small growth: a name reserved after the stat is still seen.
	repo := t.TempDir()
	hold := pubHold(t, repo, "x")
	pubRelease(t, hold, "x")
	w := newPublicationWait(filepath.Join(repo, pushHoldDir), pubLimits(limit, 1<<20))
	grew := false
	w.beforeRead = func(p string) {
		if p == hold && !grew {
			grew = true
			pubAppend(t, hold, []byte("refs/heads/late\n"))
		}
	}
	if held, err := pubFirstVerdict(t, w); err != nil || !held {
		t.Fatalf("name appended after stat: held=%v err=%v", held, err)
	}
}

// TestPushPublicationWait_ChurnBounded pins rebuild work: an unchanged
// held hold is read once however long the wait polls, and tombstone
// churn — every poll changing the sidecar — rereads at most the per-poll
// budget each time and fails closed once the per-wait budget is spent.
func TestPushPublicationWait_ChurnBounded(t *testing.T) {
	repo := t.TempDir()
	dir := filepath.Join(repo, pushHoldDir)
	names := pubNames("refs/heads/c", 100)
	hold := pubHold(t, repo, append(names, "refs/heads/live")...)
	pubRelease(t, hold, names...)

	// Unchanged: 20 polls, one read.
	w := newPublicationWait(dir, pubRoomy)
	ctx, cancel := context.WithCancel(t.Context())
	w.afterPass = func(held bool) {
		if !held || w.passes == 20 {
			cancel()
		}
	}
	if err := w.wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("unchanged held hold: %v", err)
	}
	cancel()
	if w.passes != 20 || w.reads != 1 {
		t.Fatalf("unchanged hold: %d polls, %d reads; want 20 polls, 1 read", w.passes, w.reads)
	}

	// Churn: one more tombstone after every poll. Each reread costs
	// roughly the whole hold plus sidecar; the per-wait budget allows
	// only a few before failing closed.
	perRead := 2*pubSize(names) + int64(len("refs/heads/live\n"))
	lim := publicationLimits{passBytes: 2 * perRead, passRecords: 1 << 16, waitBytes: 5 * perRead, waitRecords: 1 << 20}
	w = newPublicationWait(dir, lim)
	var last int64
	i := 0
	w.afterPass = func(held bool) {
		if !held {
			t.Fatal("churned hold stopped holding")
		}
		if d := w.bytes - last; d > lim.passBytes {
			t.Fatalf("poll %d read %d bytes, per-poll limit %d", w.passes, d, lim.passBytes)
		}
		last = w.bytes
		pubRelease(t, hold, "refs/heads/churn"+strconv.Itoa(i))
		i++
	}
	pubRequireLimit(t, w.wait(t.Context()))
	if w.bytes > lim.waitBytes+1 || w.reads > 6 {
		t.Fatalf("churn: read %d bytes over %d reads, per-wait limit %d", w.bytes, w.reads, lim.waitBytes)
	}
}

// TestPushPublicationWait_ReadFailuresFailClosed pins the fail-closed
// result: unreadable holds (or hold directory) never return permission to
// proceed, and a later wait recovers once the hold can be checked.
// Unreadable tombstones subtract nothing (over-holding).
func TestPushPublicationWait_ReadFailuresFailClosed(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("file permissions do not bind root")
	}
	repo := t.TempDir()
	dir := filepath.Join(repo, pushHoldDir)
	hold := pubHold(t, repo, "refs/heads/main")

	if err := os.Chmod(hold, 0); err != nil {
		t.Fatal(err)
	}
	err := newPublicationWait(dir, pubRoomy).wait(t.Context())
	if !errors.Is(err, ErrPushPublicationUnverified) || !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("unreadable hold: err = %v, want unverified permission error", err)
	}
	if err := os.Chmod(hold, 0o600); err != nil {
		t.Fatal(err)
	}

	if err := os.Chmod(dir, 0); err != nil {
		t.Fatal(err)
	}
	err = newPublicationWait(dir, pubRoomy).wait(t.Context())
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(err, ErrPushPublicationUnverified) {
		t.Fatalf("unreadable hold dir: err = %v, want unverified", err)
	}

	pubRelease(t, hold, "refs/heads/main")
	if err := os.Chmod(hold+pushHoldReleasedSuffix, 0); err != nil {
		t.Fatal(err)
	}
	w := newPublicationWait(dir, pubRoomy)
	if held, err := pubFirstVerdict(t, w); err != nil || !held {
		t.Fatalf("unreadable tombstones: held=%v err=%v, want held", held, err)
	}
	if err := os.Chmod(hold+pushHoldReleasedSuffix, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := newPublicationWait(dir, pubRoomy).wait(t.Context()); err != nil {
		t.Fatalf("recovery after the hold became checkable: %v", err)
	}
}

// TestPushPublicationWait_TornRecords pins the torn-append rules shared
// with readPushHolds: an unterminated tombstone never releases (its bytes
// may be a prefix of a longer released name), while an unterminated hold
// line still holds.
func TestPushPublicationWait_TornRecords(t *testing.T) {
	repo := t.TempDir()
	dir := filepath.Join(repo, pushHoldDir)
	hold := pubHold(t, repo, "refs/heads/foo")
	pubRelease(t, hold, "refs/heads/other")
	pubAppend(t, hold+pushHoldReleasedSuffix, []byte("refs/heads/foo"))
	if held, err := pubFirstVerdict(t, newPublicationWait(dir, pubRoomy)); err != nil || !held {
		t.Fatalf("torn tombstone released a name: held=%v err=%v", held, err)
	}

	repo = t.TempDir()
	dir = filepath.Join(repo, pushHoldDir)
	hold = pubHold(t, repo)
	pubAppend(t, hold, []byte("refs/heads/tor"))
	if held, err := pubFirstVerdict(t, newPublicationWait(dir, pubRoomy)); err != nil || !held {
		t.Fatalf("torn hold line not held: held=%v err=%v", held, err)
	}
}

// FuzzPushPublicationScan checks one poll over arbitrary hold and
// tombstone bytes against a straightforward reference: the verdict must
// match, the only possible failure is a scan limit, and the limits are
// never exceeded (bytes by at most one probe byte).
func FuzzPushPublicationScan(f *testing.F) {
	f.Add([]byte("refs/heads/a\n"), []byte(""), false)
	f.Add([]byte("refs/heads/a\nrefs/heads/b\n"), []byte("refs/heads/a\nrefs/heads/b\n"), true)
	f.Add([]byte("a\n\n\na"), []byte("a"), true)
	f.Add([]byte("x\nx\nx\nlive"), []byte("x\n"), true)
	f.Add([]byte(strings.Repeat("r", pushRefPathMax+1)), []byte(""), false)
	f.Add([]byte("\n"), []byte(strings.Repeat("t", pushRefPathMax)+"\n"), true)
	lim := publicationLimits{passBytes: 3000, passRecords: 64, waitBytes: 3000, waitRecords: 64}
	f.Fuzz(func(t *testing.T, holdBytes, relBytes []byte, hasRel bool) {
		dir := t.TempDir()
		hold := filepath.Join(dir, "push-f")
		if err := os.WriteFile(hold, holdBytes, 0o600); err != nil {
			t.Fatal(err)
		}
		if hasRel {
			if err := os.WriteFile(hold+pushHoldReleasedSuffix, relBytes, 0o600); err != nil {
				t.Fatal(err)
			}
		}
		w := newPublicationWait(dir, lim)
		held, err := w.pass()
		if w.bytes > lim.passBytes+1 || w.records > lim.passRecords {
			t.Fatalf("exceeded limits: %d bytes, %d records", w.bytes, w.records)
		}
		if err != nil {
			if !errors.Is(err, errPublicationScanLimit) {
				t.Fatalf("unexpected error class: %v", err)
			}
			return
		}
		// Reference: accepted polls fit every bound, so the full split
		// semantics apply.
		released := map[string]bool{}
		if hasRel {
			recs := strings.Split(string(relBytes), "\n")
			for _, r := range recs[:len(recs)-1] { // drop the unterminated tail
				released[r] = true
			}
		}
		want := false
		for r := range strings.SplitSeq(string(holdBytes), "\n") {
			if len(r) > pushRefPathMax {
				t.Fatalf("accepted a %d-byte record", len(r))
			}
			if r != "" && !released[r] {
				want = true
				break
			}
		}
		if held != want {
			t.Fatalf("held = %v, reference %v", held, want)
		}
	})
}

// holdRefreshers counts the running push-hold refresher goroutines
// (startPushHoldRefresh's ticker loop).
func holdRefreshers(t *testing.T) int {
	t.Helper()
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	count := 0
	for _, g := range bytes.Split(buf, []byte("\n\n")) {
		if bytes.Contains(g, []byte("githttp.startPushHoldRefresh.func")) {
			count++
		}
	}
	return count
}

// TestHoldRefHoldsItsRefUntilRelease: HoldRef holds exactly the ref it
// names, so a push or write to that ref waits and one to another ref does
// not, and release removes the hold and stops its refresher.
func TestHoldRefHoldsItsRefUntilRelease(t *testing.T) {
	prev := pushHoldWaitMax
	pushHoldWaitMax = 100 * time.Millisecond
	t.Cleanup(func() { pushHoldWaitMax = prev })
	cacheDir, repo := t.TempDir(), "repo_hold"
	before := holdRefreshers(t)
	release, err := HoldRef(context.Background(), cacheDir, repo, "refs/heads/a")
	if err != nil {
		t.Fatal(err)
	}
	repoPath := RepoPath(cacheDir, repo)
	other, err := mintPushHold(repoPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := reservePushHolds(context.Background(), other, []string{"refs/heads/a"}); !errors.Is(err, ErrRefHeld) {
		t.Fatalf("reserving the held ref = %v, want a denial that matches ErrRefHeld", err)
	}
	if err := reservePushHolds(context.Background(), other, []string{"refs/heads/b"}); err != nil {
		t.Fatalf("reserving another ref = %v, want it reserved", err)
	}
	removePushHoldFiles(other)
	release()
	if n := holdRefreshers(t); n != before {
		t.Fatalf("%d hold refreshers after release, want %d: release must stop the refresher", n, before)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := WaitForPushPublication(waitCtx, cacheDir, repo); err != nil {
		t.Fatalf("publication wait after release: %v", err)
	}
}

// TestHoldRefRefreshesPastItsTTL: a REST write can hold its ref longer
// than pushHoldTTL (a slow intent write or CAS), so HoldRef's refresher
// must keep the hold live. Without it the hold lapses after about one and
// a half TTLs, and a push run could snapshot the ref before the CAS.
func TestHoldRefRefreshesPastItsTTL(t *testing.T) {
	prev := pushHoldTTL
	pushHoldTTL = 2 * time.Second // the refresher touches it every 500 ms
	t.Cleanup(func() { pushHoldTTL = prev })
	cacheDir, repo := t.TempDir(), "repo_ttl"
	release, err := HoldRef(context.Background(), cacheDir, repo, "refs/heads/a")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	time.Sleep(4500 * time.Millisecond) // past the lapse of an unrefreshed hold
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := WaitForPushPublication(ctx, cacheDir, repo); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("publication wait 4.5 s into a hold with a 2 s TTL = %v, want it still blocked: the hold lapsed", err)
	}
}

// TestHoldRefReleasesWhenItCannotReserve: a HoldRef that gives up on a
// conflicting hold leaves no hold file and no refresher behind.
func TestHoldRefReleasesWhenItCannotReserve(t *testing.T) {
	prev := pushHoldWaitMax
	pushHoldWaitMax = 100 * time.Millisecond
	t.Cleanup(func() { pushHoldWaitMax = prev })
	cacheDir, repo := t.TempDir(), "repo_hold"
	repoPath, err := EnsureCacheRepoCtx(context.Background(), cacheDir, repo)
	if err != nil {
		t.Fatal(err)
	}
	blocker := mintHold(t, repoPath, "refs/heads/a")
	holds := func() []string {
		entries, err := os.ReadDir(filepath.Join(repoPath, pushHoldDir))
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		return names
	}
	want := holds()
	before := holdRefreshers(t)
	if release, err := HoldRef(context.Background(), cacheDir, repo, "refs/heads/a"); !errors.Is(err, ErrRefHeld) || release != nil {
		t.Fatalf("HoldRef past a conflicting hold = (release %v, err %v), want no release and an error that matches ErrRefHeld", release != nil, err)
	}
	if got := holds(); !slices.Equal(got, want) {
		t.Fatalf("hold files after the refused HoldRef = %v, want only the blocker's %v", got, want)
	}
	if n := holdRefreshers(t); n != before {
		t.Fatalf("%d hold refreshers after the refused HoldRef, want %d", n, before)
	}
	removePushHoldFiles(blocker)
}
