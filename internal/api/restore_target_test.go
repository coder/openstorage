package api

import (
	"bytes"
	"compress/zlib"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// restoreTargetTree answers "not found" only for a target that names no
// commit. A commit whose tree, or a tag whose commit, git cannot read
// gives rev-parse the same exit 1, and is the repository's failure, and
// so is a tag git cannot parse.
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
	// Tag objects git writes only with --literally: headers out of order,
	// which git cannot parse, and an uppercase object id, which it can.
	rawTag := func(body string) string {
		t.Helper()
		cmd := exec.Command("git", "hash-object", "-t", "tag", "-w", "--stdin", "--literally")
		cmd.Dir = dir
		cmd.Stdin = strings.NewReader(body)
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("hash-object: %v", err)
		}
		return strings.TrimSpace(string(out))
	}
	badTag := rawTag("type tree\nobject " + tree1 + "\ntag bad\ntagger a <a@b> 0 +0000\n\nm\n")
	upperTag := rawTag("object " + strings.ToUpper(tree1) + "\ntype tree\ntag up\ntagger a <a@b> 0 +0000\n\nm\n")

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
	// A tree that no commit has, which a replacement (any pusher can write
	// refs/replace/*) makes read as c1: it is still no commit, because git
	// reads every object as itself (gitlimit sets GIT_NO_REPLACE_OBJECTS).
	mk := exec.Command("git", "mktree")
	mk.Dir = dir
	mk.Stdin = strings.NewReader("100644 blob " + blob + "\tother.txt\n")
	out, err := mk.Output()
	if err != nil {
		t.Fatalf("mktree: %v", err)
	}
	replacedTree := strings.TrimSpace(string(out))
	coreGit(t, dir, "update-ref", "refs/replace/"+replacedTree, c1)
	// A commit that a replacement makes read as a tree: it is still a
	// commit, with its own tree.
	c4 := strings.TrimSpace(coreGit(t, dir, "commit-tree", tree1, "-p", c1, "-m", "c4"))
	coreGit(t, dir, "update-ref", "refs/replace/"+c4, tree1)
	for _, oid := range []string{tree2, c3} {
		if err := os.Remove(filepath.Join(dir, ".git", "objects", oid[:2], oid[2:])); err != nil {
			t.Fatal(err)
		}
	}
	// Commits whose object file is there but whose header git cannot
	// read: cat-file -e finds them, and rev-parse exits 128 for them.
	badHeader := strings.TrimSpace(coreGit(t, dir, "commit-tree", tree1, "-p", c1, "-m", "bad header"))
	tagBadHeader := tag("bad-header", badHeader)
	notZlib := strings.TrimSpace(coreGit(t, dir, "commit-tree", tree1, "-p", c1, "-m", "not zlib"))
	rewriteLoose := func(oid string, data func(body []byte) []byte) {
		t.Helper()
		p := filepath.Join(dir, ".git", "objects", oid[:2], oid[2:])
		f, err := os.Open(p)
		if err != nil {
			t.Fatal(err)
		}
		zr, err := zlib.NewReader(f)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := io.ReadAll(zr)
		_ = f.Close()
		if err != nil {
			t.Fatal(err)
		}
		_, body, ok := bytes.Cut(raw, []byte{0})
		if !ok {
			t.Fatalf("loose object %s has no header", oid)
		}
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, data(body), 0o444); err != nil {
			t.Fatal(err)
		}
	}
	rewriteLoose(badHeader, func(body []byte) []byte {
		var z bytes.Buffer
		zw := zlib.NewWriter(&z)
		_, _ = zw.Write(append([]byte("commit 12x\x00"), body...))
		_ = zw.Close()
		return z.Bytes()
	})
	rewriteLoose(notZlib, func([]byte) []byte { return []byte("not zlib data\n") })

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
		{"tag of a tree by an uppercase id", upperTag, "", false, false},
		{"tag that git cannot parse", badTag, "", false, true},
		{"tree that a replacement makes a commit", replacedTree, "", false, false},
		{"commit that a replacement makes a tree", c4, tree1, true, false},
		{"commit whose tree is missing", c2, "", false, true},
		{"tag of a missing commit", tagC3, "", false, true},
		{"tag of a tag of a missing commit", tagTagC3, "", false, true},
		{"commit with a header git cannot parse", badHeader, "", false, true},
		{"tag of a commit with a header git cannot parse", tagBadHeader, "", false, true},
		{"commit whose object file is not zlib data", notZlib, "", false, true},
	} {
		tree, found, err := sc.restoreTargetTree(context.Background(), tc.target)
		if tree != tc.tree || found != tc.found || (err != nil) != tc.broken {
			t.Errorf("%s: tree %q found %v err %v, want tree %q found %v broken %v", tc.name, tree, found, err, tc.tree, tc.found, tc.broken)
		}
	}
	// resolveTree, which commit-pack, diff-commit and merge read a parent's
	// tree with, does not peel ^{commit}: a tip that a replacement makes
	// read as a tree still resolves, as on main.
	if _, err := sc.resolveTree(context.Background(), c4); err != nil {
		t.Errorf("resolveTree(commit that a replacement makes a tree): %v", err)
	}
	// Only a full object id reaches restoreTargetTree: for an abbreviated
	// one, cat-file -e exit 1 can also mean an ambiguous name.
	t.Run("abbreviated target", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("restoreTargetTree(abbreviated id) did not panic")
			}
		}()
		_, _, _ = sc.restoreTargetTree(context.Background(), c1[:12])
	})
	// git peels the tags: nothing reads a tag object's message, which
	// can be as large as the repository allows.
	t.Run("no tag body read", func(t *testing.T) {
		gitFailsOnArg(t, "tag", "kill")
		if tree, found, err := sc.restoreTargetTree(context.Background(), tagTree); tree != "" || found || err != nil {
			t.Fatalf("tag of a tree: tree %q found %v err %v, want not found", tree, found, err)
		}
	})
}

// A git run in restoreTargetTree that fails other than with its "no such
// object" exit is the server's failure, an error, never "not found". So
// is a commit at the end of the chain that rev-parse ^{commit} cannot
// read.
func TestRestoreTargetTreeGitFailures(t *testing.T) {
	dir := t.TempDir()
	coreGit(t, dir, "init", "-q", "-b", "main", ".")
	coreWriteFile(t, dir, "a.txt", "one\n")
	coreGit(t, dir, "add", ".")
	coreGit(t, dir, "commit", "-q", "-m", "c1")
	c1 := coreRevParse(t, dir, "HEAD")
	tree1 := coreRevParse(t, dir, "HEAD^{tree}")
	sc, err := wcNewScratch(filepath.Join(dir, ".git"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sc.close)
	for _, tc := range []struct{ name, target, arg, how string }{
		{"rev-parse ^{commit} of a tree exits 128", tree1, tree1 + "^{commit}", "128"},
		{"rev-parse ^{commit} of a tree is killed", tree1, tree1 + "^{commit}", "kill"},
		{"cat-file -e exits 128", tree1, "-e", "128"},
		{"cat-file -e is killed", tree1, "-e", "kill"},
		{"cat-file -t exits 128", tree1, "-t", "128"},
		{"cat-file -t is killed", tree1, "-t", "kill"},
		{"rev-parse ^{commit} of a commit exits 1", c1, c1 + "^{commit}", "1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gitFailsOnArg(t, tc.arg, tc.how)
			if tree, found, err := sc.restoreTargetTree(context.Background(), tc.target); tree != "" || found || err == nil {
				t.Fatalf("tree %q found %v err %v, want an error", tree, found, err)
			}
		})
	}
}
