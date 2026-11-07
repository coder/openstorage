package gitread

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Only git's own "no" (exit 1 from check-ref-format or from rev-parse
// --verify --quiet) makes ResolveRef answer ErrNotFound. A fatal exit, a
// killed git or a corrupt repository says nothing about the ref.
func TestResolveRefOnlyExitOneIsNotFound(t *testing.T) {
	f := newFixture(t)
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	for _, sub := range []string{"check-ref-format", "rev-parse"} {
		for _, tc := range []struct {
			name, sh string
			notFound bool
		}{
			{"exit 1", "exit 1", true},
			{"exit 128", "echo 'fatal: bad' >&2; exit 128", false},
			{"SIGKILL", "kill -KILL $$", false},
		} {
			t.Run(sub+"/"+tc.name, func(t *testing.T) {
				fakeGit(t, "if [ \"$1\" = "+sub+" ]; then "+tc.sh+"; fi\nexec '"+realGit+"' \"$@\"")
				sha, err := ResolveRef(context.Background(), f.dir, "main", false)
				if err == nil || errors.Is(err, ErrNotFound) != tc.notFound {
					t.Fatalf("ResolveRef(main) = (%q, %v), want an error that is ErrNotFound: %v", sha, err, tc.notFound)
				}
			})
		}
	}
	t.Run("corrupt packed-refs", func(t *testing.T) {
		dir := t.TempDir()
		mustGit(t, dir, "clone", "-q", "--bare", "--no-local", f.dir, ".")
		if err := os.WriteFile(filepath.Join(dir, "packed-refs"), []byte("garbage line\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if sha, err := ResolveRef(context.Background(), dir, "main", false); err == nil || errors.Is(err, ErrNotFound) {
			t.Fatalf("ResolveRef(main) = (%q, %v), want git's failure, not ErrNotFound", sha, err)
		}
	})
}

// ResolveRef answers a spec over maxRefSpecBytes (4096) before it runs
// git, and leaves the spec out of the error: a spec can be request-sized.
func TestResolveRefLongSpecSkipsGit(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "ran")
	fakeGit(t, "echo x >> '"+marker+"'\nexit 1")
	spec := strings.Repeat("x", 4097)
	_, err := ResolveRef(context.Background(), t.TempDir(), spec, false)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("4097-byte spec: err = %v, want ErrNotFound", err)
	}
	if strings.Contains(err.Error(), spec[:64]) {
		t.Fatalf("4097-byte spec: the error quotes the spec: %.80q", err.Error())
	}
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("git ran for a 4097-byte spec")
	}
	if _, err := ResolveRef(context.Background(), t.TempDir(), spec[:4096], false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("4096-byte spec: err = %v, want git's ErrNotFound", err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatal("git did not run for a 4096-byte spec")
	}
}

// rev-parse --verify --quiet <ref>^{commit} exits 1 for a ref that does
// not exist, and also for one whose commit git cannot read. Only the
// first is ErrNotFound: an existing branch or tag whose commit object is
// missing is the repository's failure. A ref or id that names a tree is
// still no commit.
func TestResolveRefUnreadableCommitIsAnError(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--initial-branch=main")
	writeFile(t, dir, "a.txt", "one\n", 0o644)
	mustGit(t, dir, "add", "-A")
	mustGit(t, dir, "commit", "-q", "-m", "c1")
	tree := strings.TrimSpace(mustGit(t, dir, "rev-parse", "HEAD^{tree}"))
	mustGit(t, dir, "tag", "-a", "-m", "tree", "tree-tag", tree)
	writeFile(t, dir, "a.txt", "two\n", 0o644)
	mustGit(t, dir, "commit", "-q", "-am", "c2")
	c2 := strings.TrimSpace(mustGit(t, dir, "rev-parse", "HEAD"))
	mustGit(t, dir, "branch", "gone", c2)
	mustGit(t, dir, "tag", "-a", "-m", "gone", "gone-tag", c2)
	mustGit(t, dir, "tag", "-a", "-m", "gone again", "gone-tag-tag", "gone-tag")
	tagObj := strings.TrimSpace(mustGit(t, dir, "rev-parse", "refs/tags/gone-tag"))
	mustGit(t, dir, "reset", "-q", "--hard", "HEAD~1")
	if err := os.Remove(filepath.Join(dir, ".git", "objects", c2[:2], c2[2:])); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, spec := range []string{"gone", "refs/heads/gone", "gone-tag", "gone-tag-tag", tagObj, strings.ToUpper(tagObj)} {
		if sha, err := ResolveRef(ctx, dir, spec, false); err == nil || errors.Is(err, ErrNotFound) {
			t.Errorf("ResolveRef(%q) = (%q, %v), want an error that is not ErrNotFound", spec, sha, err)
		}
	}
	for _, spec := range []string{"nope", "refs/heads/nope", "tree-tag", tree, c2, c2[:12], strings.Repeat("1", 40)} {
		if sha, err := ResolveRef(ctx, dir, spec, false); !errors.Is(err, ErrNotFound) {
			t.Errorf("ResolveRef(%q) = (%q, %v), want ErrNotFound", spec, sha, err)
		}
	}
	if _, err := ResolveRef(ctx, dir, "main", false); err != nil {
		t.Errorf("ResolveRef(main): %v", err)
	}
}
