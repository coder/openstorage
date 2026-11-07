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
