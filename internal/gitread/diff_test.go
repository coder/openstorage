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

// TestDiffPatchesApplyCleanly is the docs/explanation/domain-model.md:93-95 invariant seed: the
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

// A type change (T) is one --raw and one --numstat record, and two
// --patch sections, a deletion and an addition: the file's patch holds
// both, the file after it keeps its own, and the patches apply.
func TestDiffTypeChange(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--initial-branch=main")
	writeFile(t, dir, "typefile", "one\n", 0o644)
	writeFile(t, dir, "filetosub", "two\n", 0o644)
	writeFile(t, dir, "keep", "x\n", 0o644)
	mustGit(t, dir, "add", "-A")
	mustGit(t, dir, "commit", "-q", "-m", "files")
	files := strings.TrimSpace(mustGit(t, dir, "rev-parse", "HEAD"))
	if err := os.Remove(filepath.Join(dir, "typefile")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("keep", filepath.Join(dir, "typefile")); err != nil {
		t.Fatal(err)
	}
	mustGit(t, dir, "rm", "-q", "--cached", "filetosub")
	if err := os.Remove(filepath.Join(dir, "filetosub")); err != nil {
		t.Fatal(err)
	}
	mustGit(t, dir, "update-index", "--add", "--cacheinfo", "160000,"+files+",filetosub")
	writeFile(t, dir, "keep", "x\ny\n", 0o644)
	mustGit(t, dir, "add", "typefile", "keep")
	mustGit(t, dir, "commit", "-q", "-m", "a symlink and a submodule")
	other := strings.TrimSpace(mustGit(t, dir, "rev-parse", "HEAD"))
	for _, tc := range []struct {
		name, base, target string
		oldMode, newMode   [2]string // typefile's, filetosub's
	}{
		{"to a symlink and a submodule", files, other, [2]string{"100644", "100644"}, [2]string{"120000", "160000"}},
		{"back to files", other, files, [2]string{"120000", "160000"}, [2]string{"100644", "100644"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, stats, err := Diff(context.Background(), dir, tc.base, tc.target, DiffOptions{})
			if err != nil {
				t.Fatal(err)
			}
			m := diffByPath(got)
			if len(got) != 3 || stats.Files != 3 || m["keep"].Status != "M" || !strings.HasPrefix(m["keep"].Patch, "diff --git a/keep b/keep\n") || strings.Count(m["keep"].Patch, "diff --git ") != 1 {
				t.Fatalf("files = %+v, want filetosub, keep (M, its own patch) and typefile", got)
			}
			for i, path := range []string{"typefile", "filetosub"} {
				d := m[path]
				if d.Status != "T" || d.Additions != 1 || d.Deletions != 1 || d.Binary {
					t.Errorf("%s = %+v, want T with 1 addition and 1 deletion", path, d)
				}
				header := "diff --git a/" + path + " b/" + path + "\n"
				del := header + "deleted file mode " + tc.oldMode[i] + "\n"
				add := header + "new file mode " + tc.newMode[i] + "\n"
				if !strings.HasPrefix(d.Patch, del) || strings.Count(d.Patch, header) != 2 || !strings.Contains(d.Patch, "\n"+add) {
					t.Errorf("%s patch = %q, want its deletion, then its addition", path, d.Patch)
				}
			}
			var all strings.Builder
			for _, d := range got {
				all.WriteString(d.Patch)
			}
			work := t.TempDir()
			mustGit(t, work, "init", "-q")
			mustGit(t, work, "fetch", "-q", dir, tc.base)
			mustGit(t, work, "read-tree", "FETCH_HEAD")
			cmd := exec.Command("git", "apply", "--cached", "--check")
			cmd.Dir, cmd.Stdin = work, strings.NewReader(all.String())
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("git apply --cached --check: %v\n%s", err, out)
			}
		})
	}
}
