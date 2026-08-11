package gitread

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func diffByPath(files []FileDiff) map[string]FileDiff {
	m := make(map[string]FileDiff, len(files))
	for _, f := range files {
		m[f.Path] = f
	}
	return m
}

func TestDiffTwoCommits(t *testing.T) {
	f := newFixture(t)
	files, stats, err := Diff(context.Background(), f.dir, f.c1, f.c2, DiffOptions{})
	if err != nil {
		t.Fatal(err)
	}
	m := diffByPath(files)
	if len(m) != 4 {
		t.Fatalf("changed files = %v, want 4", files)
	}

	if d := m["README.md"]; d.Status != "M" || d.Additions != 1 || d.Deletions != 0 || d.Binary {
		t.Errorf("README.md = %+v", d)
	}
	if d := m["dir/renamed.txt"]; d.Status != "R" || d.OldPath != "dir/data.txt" || d.Additions != 0 || d.Deletions != 0 {
		t.Errorf("rename = %+v", d)
	}
	if d := m["extra.txt"]; d.Status != "A" || d.Additions != 1 {
		t.Errorf("extra.txt = %+v", d)
	}
	if d := m["bin.dat"]; d.Status != "M" || !d.Binary || d.Additions != -1 || d.Deletions != -1 {
		t.Errorf("bin.dat = %+v", d)
	}
	for _, d := range files {
		if !strings.HasPrefix(d.Patch, "diff --git ") {
			t.Errorf("%s patch does not start with a diff --git header: %.60q", d.Path, d.Patch)
		}
	}
	if stats.Files != 4 || stats.Additions != 2 || stats.Deletions != 0 || stats.Changes != 2 {
		t.Errorf("stats = %+v", stats)
	}
}

func TestDiffRootAgainstEmptyTree(t *testing.T) {
	f := newFixture(t)
	files, stats, err := Diff(context.Background(), f.dir, EmptyTreeSHA, f.c1, DiffOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Files != 5 {
		t.Fatalf("root diff files = %+v", files)
	}
	for _, d := range files {
		if d.Status != "A" {
			t.Errorf("%s status = %q, want A", d.Path, d.Status)
		}
	}
}

func TestDiffPathFilterAndEmpty(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	files, stats, err := Diff(ctx, f.dir, f.c1, f.c2, DiffOptions{Paths: []string{"README.md"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Path != "README.md" || stats.Files != 1 {
		t.Fatalf("filtered diff = %v", files)
	}

	// Identical trees: empty diff.
	files, stats, err = Diff(ctx, f.dir, f.c1, f.c1, DiffOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 || stats.Files != 0 {
		t.Fatalf("self diff = %v %+v", files, stats)
	}
}

// TestDiffPatchesApplyCleanly is the docs/01:93-95 invariant seed: the
// concatenated per-file patches of a text-only diff must pass
// `git apply --check` against the base tree.
func TestDiffPatchesApplyCleanly(t *testing.T) {
	f := newFixture(t)
	// bin.dat's binary change has no textual patch body, so exclude it.
	files, _, err := Diff(context.Background(), f.dir, f.c1, f.c2, DiffOptions{
		Paths: []string{"README.md", "dir", "extra.txt"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var concat strings.Builder
	for _, d := range files {
		concat.WriteString(d.Patch)
	}

	// Fresh worktree at c1 to check application against.
	work := t.TempDir()
	mustGit(t, work, "init", "-q")
	mustGit(t, work, "fetch", "-q", f.dir, f.c1)
	mustGit(t, work, "checkout", "-q", "FETCH_HEAD")
	patch := filepath.Join(t.TempDir(), "all.patch")
	if err := os.WriteFile(patch, []byte(concat.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("git", "apply", "--check", patch)
	cmd.Dir = work
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git apply --check failed: %v\n%s\npatch:\n%s", err, out, concat.String())
	}
}
