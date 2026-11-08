package gitread

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

// A repository's own .gitattributes make git print warnings that quote
// their paths unescaped, and a directory name can hold a newline and the
// text of git's diagnostics. blame and grep read no tree attributes, so a
// good repository answers as one: in a work tree, and in a bare repository
// told to read HEAD's attributes (GIT_ATTR_SOURCE, attr.tree), as git 2.43
// to 2.45 did by default.
func TestBlameAndGrepReadNoTreeAttributes(t *testing.T) {
	oid := strings.Repeat("c", 40)
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--initial-branch=main")
	for _, d := range []string{"e': unable to read " + oid + "\nz", "d\nfatal: -L zz"} {
		writeFile(t, dir, d+"/.gitattributes", "foo error:\n", 0o644)
		writeFile(t, dir, d+"/f.txt", "one\n", 0o644)
	}
	writeFile(t, dir, "a.txt", "one\ntwo\n", 0o644)
	mustGit(t, dir, "add", "-A")
	mustGit(t, dir, "commit", "-q", "-m", "c")
	commit := strings.TrimSpace(mustGit(t, dir, "rev-parse", "HEAD"))
	bare := filepath.Join(t.TempDir(), "bare.git")
	mustGit(t, t.TempDir(), "clone", "-q", "--bare", dir, bare)
	ctx := context.Background()
	for _, v := range []struct {
		name, repo string
		env        map[string]string
	}{
		{"work tree", dir, nil},
		{"bare, GIT_ATTR_SOURCE=HEAD", bare, map[string]string{"GIT_ATTR_SOURCE": "HEAD"}},
		{"bare, attr.tree=HEAD", bare, map[string]string{"GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": "attr.tree", "GIT_CONFIG_VALUE_0": "HEAD"}},
	} {
		t.Run(v.name, func(t *testing.T) {
			for k, val := range v.env {
				t.Setenv(k, val)
			}
			files, err := Grep(ctx, v.repo, commit, GrepOptions{Pattern: "one"})
			if err != nil || len(files) != 3 {
				t.Errorf("grep one = %d files, %v, want 3 files", len(files), err)
			}
			if _, err := Grep(ctx, v.repo, commit, GrepOptions{Pattern: "zzz"}); err != nil {
				t.Errorf("grep zzz = %v, want no match", err)
			}
			if _, err := Blame(ctx, v.repo, commit, "nope.txt", []string{"1,2"}, false); !errors.Is(err, ErrNotFound) {
				t.Errorf("blame of a missing path = %v, want ErrNotFound", err)
			}
			if _, err := Blame(ctx, v.repo, commit, "a.txt", []string{"9,10"}, false); !errors.Is(err, ErrBadRange) {
				t.Errorf("blame past the end = %v, want ErrBadRange", err)
			}
		})
	}
}
