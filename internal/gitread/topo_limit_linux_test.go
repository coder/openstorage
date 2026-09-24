//go:build linux

package gitread

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestTopoRanksHeapLimitAppliedBeforeExec proves the limit is set before
// git starts: a fake `git` on PATH records the RLIMIT_DATA (KiB) it
// inherited.
func TestTopoRanksHeapLimitAppliedBeforeExec(t *testing.T) {
	bin := t.TempDir()
	out := filepath.Join(bin, "limit")
	script := "#!/bin/sh\nulimit -d > " + out + "\n"
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	sha := strings.Repeat("a", 40)
	// The fake prints nothing, so TopoRanks reports the SHA unreachable.
	if _, err := TopoRanks(context.Background(), t.TempDir(), sha, []string{sha}, 48<<20); err == nil {
		t.Fatal("want unreachable error from the silent fake git")
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("fake git did not run: %v", err)
	}
	if strings.TrimSpace(string(got)) != "49152" {
		t.Fatalf("git inherited RLIMIT_DATA %q KiB, want 49152", got)
	}
}

// TestTopoRanksHeapLimitExceeded: a walk that needs more heap than the
// limit fails with ErrMemoryLimit instead of growing without bound. A
// commit object larger than the limit forces the allocation failure
// deterministically.
func TestTopoRanksHeapLimitExceeded(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--bare")
	tree := strings.TrimSpace(mustGit(t, dir, "hash-object", "-t", "tree", "-w", "/dev/null"))
	raw := filepath.Join(t.TempDir(), "commit")
	body := "tree " + tree + "\nauthor A <a@example.com> 1700000000 +0000\ncommitter A <a@example.com> 1700000000 +0000\n\n" +
		strings.Repeat("x", 32<<20) + "\n"
	if err := os.WriteFile(raw, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	big := strings.TrimSpace(mustGit(t, dir, "hash-object", "-t", "commit", "-w", raw))

	_, err := TopoRanks(context.Background(), dir, big, []string{big}, 16<<20)
	if !errors.Is(err, ErrMemoryLimit) {
		t.Fatalf("err = %v, want ErrMemoryLimit", err)
	}
	// A cap too small for git to even start crashes it (SIGSEGV in the
	// loader) rather than printing "Out of memory": still ErrMemoryLimit.
	if _, err := TopoRanks(context.Background(), dir, big, []string{big}, 64<<10); !errors.Is(err, ErrMemoryLimit) {
		t.Fatalf("64 KiB cap: err = %v, want ErrMemoryLimit", err)
	}
	// The same walk succeeds with enough headroom.
	ranks, err := TopoRanks(context.Background(), dir, big, []string{big}, 256<<20)
	if err != nil || ranks[big] != 0 {
		t.Fatalf("ranks = %v, err = %v with a 256 MiB limit", ranks, err)
	}
}
