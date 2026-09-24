//go:build linux

package gitread

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestDeepHistoryQualification runs every history-scaled read class under
// the production limits on a synthetic linear history (#313). It is
// opt-in: OPENSTORAGE_TEST_DEEP_HISTORY=<commits> (the issue's scale is
// 300000; building it takes seconds, the walks take a few seconds each).
//
// For each class it records the outcome and the peak VmData of the git
// child (sampled from /proc every 2 ms), and asserts that the outcome is
// success or a typed limit error, that no child exceeded its heap cap, and
// that no child outlived its call.
func TestDeepHistoryQualification(t *testing.T) {
	raw := os.Getenv("OPENSTORAGE_TEST_DEEP_HISTORY")
	if raw == "" {
		t.Skip("set OPENSTORAGE_TEST_DEEP_HISTORY=<commits> to run")
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 10 {
		t.Fatalf("OPENSTORAGE_TEST_DEEP_HISTORY=%q: want a commit count >= 10", raw)
	}
	start := time.Now()
	dir, tip, root := deepHistory(t, n)
	t.Logf("built %d-commit history in %v", n, time.Since(start))

	classes := []struct {
		name  string
		class Class
		run   func(ctx context.Context) error
	}{
		{"log -1 -- <root-commit file> (files/metadata, file)", Heavy, func(ctx context.Context) error {
			_, err := Log(ctx, dir, tip, LogOptions{Path: "cold.txt", MaxCount: 1})
			return err
		}},
		{"log --skip=n-10 -n 5 (commits deep page)", Heavy, func(ctx context.Context) error {
			_, err := Log(ctx, dir, tip, LogOptions{Skip: n - 10, MaxCount: 5})
			return err
		}},
		{"blame <root-commit file>", Heavy, func(ctx context.Context) error {
			_, err := Blame(ctx, dir, tip, "cold.txt", nil, false)
			return err
		}},
		{"merge-base <tip> <root> (diff, merge/preview)", Heavy, func(ctx context.Context) error {
			_, err := Run(ctx, Cmd{Class: Heavy, Dir: dir, Args: []string{"merge-base", tip, root}})
			return err
		}},
		{"log -n 1 (commit)", Light, func(ctx context.Context) error {
			_, err := GetCommit(ctx, dir, tip)
			return err
		}},
		{"grep (one thread)", Heavy, func(ctx context.Context) error {
			_, err := Grep(ctx, dir, tip, GrepOptions{Pattern: "cold"})
			return err
		}},
	}
	for _, c := range classes {
		t.Run(c.name, func(t *testing.T) {
			b := NewBudget(NewSlots(1, 0), 10*time.Minute, DefaultOutputBytes)
			ctx := WithBudget(context.Background(), b)
			sampler := sampleChildVmData()
			began := time.Now()
			err := c.run(ctx)
			elapsed := time.Since(began)
			peakKiB := sampler.stop()
			b.Close()
			assertNoChildren(t)
			limit := HeavyHeapBytes
			if c.class == Light {
				limit = LightHeapBytes
			}
			outcome := "ok"
			if err != nil {
				if !IsBudgetError(err) {
					t.Fatalf("untyped failure: %v", err)
				}
				outcome = "limit: " + firstLine(err.Error())
			}
			t.Logf("n=%d %-55s %8.2fs peak VmData %6d MiB (cap %d MiB) -> %s",
				n, c.name, elapsed.Seconds(), peakKiB>>10, limit>>20, outcome)
			if peakKiB<<10 > limit {
				t.Fatalf("git child reached %d KiB VmData, over its %d-byte cap", peakKiB, limit)
			}
		})
	}
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 160 {
		s = s[:160] + "..."
	}
	return s
}

// vmDataSampler records the peak VmData (KiB) of this process's git
// children.
type vmDataSampler struct {
	done chan struct{}
	wg   sync.WaitGroup
	peak int64
}

func sampleChildVmData() *vmDataSampler {
	s := &vmDataSampler{done: make(chan struct{})}
	self := strconv.Itoa(os.Getpid())
	s.wg.Go(func() {
		for {
			select {
			case <-s.done:
				return
			default:
			}
			ents, _ := os.ReadDir("/proc")
			for _, e := range ents {
				if _, err := strconv.Atoi(e.Name()); err != nil {
					continue
				}
				status, err := os.ReadFile(filepath.Join("/proc", e.Name(), "status"))
				if err != nil {
					continue
				}
				var name, ppid string
				var vm int64
				for line := range strings.SplitSeq(string(status), "\n") {
					if v, ok := strings.CutPrefix(line, "Name:"); ok {
						name = strings.TrimSpace(v)
					} else if v, ok := strings.CutPrefix(line, "PPid:"); ok {
						ppid = strings.TrimSpace(v)
					} else if v, ok := strings.CutPrefix(line, "VmData:"); ok {
						vm, _ = strconv.ParseInt(strings.Fields(v)[0], 10, 64)
					}
				}
				// Only git itself: between fork and exec a child still
				// shares (and reports) the test process's own memory.
				if name == "git" && ppid == self && vm > s.peak {
					s.peak = vm
				}
			}
			time.Sleep(2 * time.Millisecond)
		}
	})
	return s
}

// stop ends sampling and returns the peak; the sampler goroutine is joined
// first, so the read cannot race it.
func (s *vmDataSampler) stop() int64 {
	close(s.done)
	s.wg.Wait()
	return s.peak
}
