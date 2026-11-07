package api

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// restoreTargetTree answers "not found" only for a target that names no
// commit. A commit whose tree, or a tag whose commit, git cannot read
// gives rev-parse the same exit 1, and is the repository's failure.
func TestRestoreTargetTreeTellsMissingFromBroken(t *testing.T) {
	dir := t.TempDir()
	coreGit(t, dir, "init", "-q", "-b", "main", ".")
	coreWriteFile(t, dir, "a.txt", "one\n")
	coreGit(t, dir, "add", ".")
	coreGit(t, dir, "commit", "-q", "-m", "c1")
	c1 := coreRevParse(t, dir, "HEAD")
	tree1 := coreRevParse(t, dir, "HEAD^{tree}")
	blob := coreRevParse(t, dir, "HEAD:a.txt")
	tag := func(name, target string) string {
		coreGit(t, dir, "tag", "-a", "-m", name, name, target)
		return coreRevParse(t, dir, "refs/tags/"+name)
	}
	tagC1 := tag("c1", c1)
	tagTag := tag("c1-again", tagC1)
	tagTree := tag("tree", tree1)

	// c2's tree and c3 are unique objects, removed below.
	coreWriteFile(t, dir, "b.txt", "two\n")
	coreGit(t, dir, "add", ".")
	coreGit(t, dir, "commit", "-q", "-m", "c2")
	c2 := coreRevParse(t, dir, "HEAD")
	tree2 := coreRevParse(t, dir, "HEAD^{tree}")
	coreGit(t, dir, "commit", "-q", "--allow-empty", "-m", "c3")
	c3 := coreRevParse(t, dir, "HEAD")
	tagC3 := tag("c3", c3)
	tagTagC3 := tag("c3-again", tagC3)
	for _, oid := range []string{tree2, c3} {
		if err := os.Remove(filepath.Join(dir, ".git", "objects", oid[:2], oid[2:])); err != nil {
			t.Fatal(err)
		}
	}

	sc, err := wcNewScratch(filepath.Join(dir, ".git"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sc.close)
	for _, tc := range []struct {
		name, target, tree string
		found, broken      bool
	}{
		{"commit", c1, tree1, true, false},
		{"tag of a commit", tagC1, tree1, true, false},
		{"tag of a tag of a commit", tagTag, tree1, true, false},
		{"unknown id", strings.Repeat("1", 40), "", false, false},
		{"tree", tree1, "", false, false},
		{"blob", blob, "", false, false},
		{"tag of a tree", tagTree, "", false, false},
		{"commit whose tree is missing", c2, "", false, true},
		{"tag of a missing commit", tagC3, "", false, true},
		{"tag of a tag of a missing commit", tagTagC3, "", false, true},
	} {
		tree, found, err := sc.restoreTargetTree(context.Background(), tc.target)
		if tree != tc.tree || found != tc.found || (err != nil) != tc.broken {
			t.Errorf("%s: tree %q found %v err %v, want tree %q found %v broken %v", tc.name, tree, found, err, tc.tree, tc.found, tc.broken)
		}
	}
}
