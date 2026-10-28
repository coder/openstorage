package gitread

import (
	"context"
	"slices"
	"strings"
	"testing"
)

// TestUserPathsAreLiteral pins #577: LsTree, Log and Diff read a path as a
// literal name, never as git pathspec magic or a wildcard. Each name below
// is a directory that one commit changes; a sibling commit changes the
// names that magic or a wildcard would match instead.
func TestUserPathsAreLiteral(t *testing.T) {
	names := []string{":(attr:x)p", ":(exclude)q", ":(glob)g", ":(icase)I", ":(top)t", "a*"}
	siblings := []string{"g", "i", "t", "ab", "q2"}
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--initial-branch=main")
	commit := func(paths []string, content, msg string) string {
		for _, p := range paths {
			writeFile(t, dir, p+"/f", content, 0o644)
		}
		mustGit(t, dir, "add", "-A")
		mustGit(t, dir, "commit", "-q", "-m", msg)
		return strings.TrimSpace(mustGit(t, dir, "rev-parse", "HEAD"))
	}
	root := commit(append(slices.Clone(names), siblings...), "0\n", "root")
	changed := map[string]string{}
	for _, n := range names {
		changed[n] = commit([]string{n}, "1\n", "change "+n)
	}
	beforeSiblings := strings.TrimSpace(mustGit(t, dir, "rev-parse", "HEAD"))
	tip := commit(siblings, "1\n", "change siblings")
	ctx := context.Background()

	for _, n := range names {
		t.Run(n, func(t *testing.T) {
			contents, err := LsTree(ctx, dir, tip, n+"/", false)
			if err != nil {
				t.Fatalf("LsTree(%q): %v", n+"/", err)
			}
			if len(contents) != 1 || contents[0].Path != n+"/f" {
				t.Errorf("LsTree(%q) = %+v, want only %s/f", n+"/", contents, n)
			}
			self, err := LsTree(ctx, dir, tip, n, false)
			if err != nil {
				t.Fatalf("LsTree(%q): %v", n, err)
			}
			if len(self) != 1 || self[0].Path != n || self[0].Type != EntryTree {
				t.Errorf("LsTree(%q) = %+v, want the tree entry itself", n, self)
			}

			log, err := Log(ctx, dir, tip, LogOptions{Path: n})
			if err != nil {
				t.Fatalf("Log(path %q): %v", n, err)
			}
			var got []string
			for _, c := range log {
				got = append(got, c.SHA)
			}
			if want := []string{changed[n], root}; !slices.Equal(got, want) {
				t.Errorf("Log(path %q) = %v, want %v", n, got, want)
			}

			for _, base := range []string{root, beforeSiblings} {
				files, _, err := Diff(ctx, dir, base, tip, DiffOptions{Paths: []string{n}})
				if err != nil {
					t.Fatalf("Diff(path %q): %v", n, err)
				}
				var paths []string
				for _, f := range files {
					paths = append(paths, f.Path)
				}
				want := []string{n + "/f"}
				if base == beforeSiblings {
					want = nil // only the siblings changed after beforeSiblings
				}
				if !slices.Equal(paths, want) {
					t.Errorf("Diff(%s..tip, path %q) = %v, want %v", base[:7], n, paths, want)
				}
			}
		})
	}
}
