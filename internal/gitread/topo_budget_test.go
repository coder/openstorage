package gitread

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The #292 rank walk runs through Run (the #313 chokepoint) as a Heavy
// streaming child. These tests pin that it inherits the request budget:
// admission before git starts, the heap cap of the Heavy class, the time
// budget, early stop with reaping, and the non-Linux no-heap-cap policy.

func TestTopoRanksUsesHeavyHeapCap(t *testing.T) {
	if !HeapLimitSupported() {
		t.Skip("heap cap is Linux-only")
	}
	// Without a commit-graph, --topo-order reads the whole history before
	// its first line, so its heap grows with history length.
	t.Setenv("GIT_CONFIG_COUNT", "1")
	t.Setenv("GIT_CONFIG_KEY_0", "core.commitGraph")
	t.Setenv("GIT_CONFIG_VALUE_0", "false")
	dir, tip, root := deepHistory(t, 20000)

	small := Limits{HeavyHeap: 4 << 20, LightHeap: LightHeapBytes, MaxBlob: MaxBlobBytes}
	b := testBudget(NewSlots(1, 1, 0), time.Minute, DefaultOutputBytes).WithLimits(small)
	if _, err := TopoRanks(WithBudget(context.Background(), b), dir, tip, []string{tip}); !errors.Is(err, ErrMemoryLimit) {
		t.Fatalf("4 MiB Heavy cap: err = %v, want ErrMemoryLimit", err)
	}
	assertNoChildren(t)
	b.Close()

	// The walk is Heavy, not Light: a tiny Light cap does not apply to it.
	lightOnly := Limits{HeavyHeap: HeavyHeapBytes, LightHeap: 64 << 10, MaxBlob: MaxBlobBytes}
	b2 := testBudget(NewSlots(1, 1, 0), time.Minute, DefaultOutputBytes).WithLimits(lightOnly)
	ranks, err := TopoRanks(WithBudget(context.Background(), b2), dir, tip, []string{root, tip})
	if err != nil || ranks[tip] != 0 || ranks[root] != 19999 {
		t.Fatalf("default Heavy cap: ranks = %v, err = %v", ranks, err)
	}
	b2.Close()
}

func TestTopoRanksBusyFailsBeforeStartingGit(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "ran")
	fakeGit(t, "echo x >> "+marker)
	slots := NewSlots(1, 1, 10*time.Millisecond)
	holder := testBudget(slots, time.Minute, DefaultOutputBytes)
	if _, err := Run(WithBudget(context.Background(), holder), Cmd{Class: Heavy, Dir: t.TempDir(), Args: []string{"x"}}); err != nil {
		t.Fatal(err)
	}
	other := testBudget(slots, time.Minute, DefaultOutputBytes)
	sha := strings.Repeat("a", 40)
	if _, err := TopoRanks(WithBudget(context.Background(), other), t.TempDir(), sha, []string{sha}); !errors.Is(err, ErrBusy) {
		t.Fatalf("err = %v, want ErrBusy", err)
	}
	if raw, _ := os.ReadFile(marker); strings.Count(string(raw), "x") != 1 {
		t.Fatalf("git ran %d times, want 1 (a busy walk must not start git)", strings.Count(string(raw), "x"))
	}
	holder.Close()
	other.Close()
}

func TestTopoRanksTimeBudgetKillsAndReapsGit(t *testing.T) {
	fakeGit(t, "exec sleep 30")
	b := testBudget(NewSlots(1, 1, 0), 200*time.Millisecond, DefaultOutputBytes)
	sha := strings.Repeat("a", 40)
	start := time.Now()
	if _, err := TopoRanks(WithBudget(context.Background(), b), t.TempDir(), sha, []string{sha}); !errors.Is(err, ErrTimeBudget) {
		t.Fatalf("err = %v, want ErrTimeBudget", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("200 ms time budget took %v", d)
	}
	assertNoChildren(t)
	b.Close()
}

// Once every wanted commit is ranked the walk ends git early (ErrStop):
// the fake prints the wanted SHA, then would sleep for 30 s.
func TestTopoRanksStopsEarlyAndReaps(t *testing.T) {
	sha := strings.Repeat("b", 40)
	fakeGit(t, "echo "+strings.Repeat("a", 40)+"; echo "+sha+"; exec sleep 30")
	b := testBudget(NewSlots(1, 1, 0), time.Minute, 1) // streaming output is not counted
	start := time.Now()
	ranks, err := TopoRanks(WithBudget(context.Background(), b), t.TempDir(), sha, []string{sha})
	if err != nil || ranks[sha] != 1 {
		t.Fatalf("ranks = %v, err = %v", ranks, err)
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("walk did not stop early: %v", d)
	}
	assertNoChildren(t)
	b.Close()
}

// Non-Linux policy (ADR-049), exercised on Linux: without an enforceable
// heap cap the walk still runs under the other limits and returns exact
// ranks, so files/metadata keeps working on every platform.
func TestTopoRanksWithoutHeapCapSupportStillRanks(t *testing.T) {
	saved := heapLimitAvailable
	heapLimitAvailable = func() bool { return false }
	t.Cleanup(func() { heapLimitAvailable = saved })
	f := newFixture(t)
	tiny := Limits{HeavyHeap: 64 << 10, LightHeap: 64 << 10, MaxBlob: MaxBlobBytes} // would fail if applied
	b := testBudget(NewSlots(1, 1, 0), time.Minute, DefaultOutputBytes).WithLimits(tiny)
	ranks, err := TopoRanks(WithBudget(context.Background(), b), f.dir, f.c2, []string{f.c1, f.c2})
	if err != nil || ranks[f.c2] != 0 || ranks[f.c1] != 1 {
		t.Fatalf("ranks = %v, err = %v", ranks, err)
	}
	b.Close()
}
