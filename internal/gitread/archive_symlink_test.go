package gitread

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// gitCalls puts a git on PATH that logs each call's arguments and runs the
// real one, and returns the log's path.
func gitCalls(t *testing.T) string {
	t.Helper()
	real, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	log := filepath.Join(t.TempDir(), "git.log")
	fakeGit(t, "echo \"$*\" >> "+log+"\nexec "+real+" \"$@\"")
	return log
}

func catFileCalls(t *testing.T, log string) int {
	t.Helper()
	out, err := os.ReadFile(log)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	n := 0
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, "cat-file") {
			n++
		}
	}
	return n
}

// A symlink target of 4095 bytes, the longest that symlink(2) accepts on
// Linux, is carried by the archive. One of 4096 bytes fails PrepareArchive
// with ErrSymlinkTooLong, a limit error, before git reads the target and
// before any archive byte exists. The sizes are numbers, not MaxSymlinkTarget,
// so a changed constant turns this red.
func TestPrepareArchiveCapsASymlinkTarget(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		name string
		size int
		ok   bool
	}{{"4095 bytes", 4095, true}, {"4096 bytes", 4096, false}, {"1 MiB", 1 << 20, false}} {
		t.Run(c.name, func(t *testing.T) {
			target := strings.Repeat("t", c.size)
			dir, commit := fiRepo(t, []fiFile{{"100644", "a.txt", "a"}, {"120000", "link", target}})
			log := gitCalls(t)
			prepared, err := PrepareArchive(ctx, dir, commit, ArchiveOptions{})
			if !c.ok {
				if !errors.Is(err, ErrSymlinkTooLong) || !IsBudgetError(err) || errors.Is(err, ErrBlobTooLarge) {
					t.Fatalf("PrepareArchive err = %v, want a budget error ErrSymlinkTooLong", err)
				}
				if msg := err.Error(); !strings.Contains(msg, `"link"`) || !strings.Contains(msg, "4095") {
					t.Fatalf("the error does not name the path and the limit: %v", err)
				}
				if n := catFileCalls(t, log); n != 0 {
					t.Fatalf("git cat-file ran %d times for a target over the limit", n)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			if err := prepared.Stream(ctx, &out); err != nil {
				t.Fatal(err)
			}
			zr, err := gzip.NewReader(&out)
			if err != nil {
				t.Fatal(err)
			}
			tr := tar.NewReader(zr)
			for {
				h, err := tr.Next()
				if err == io.EOF {
					t.Fatal("the archive has no entry for the symlink")
				}
				if err != nil {
					t.Fatal(err)
				}
				if h.Name == "link" {
					if h.Typeflag != tar.TypeSymlink || h.Linkname != target {
						t.Fatalf("link entry = type %c, %d bytes of target; want a symlink of %d bytes", h.Typeflag, len(h.Linkname), len(target))
					}
					return
				}
			}
		})
	}
}

// The filters come first: a symlink that the globs leave out is not checked.
func TestPrepareArchiveDoesNotCheckAFilteredOutSymlink(t *testing.T) {
	dir, commit := fiRepo(t, []fiFile{{"100644", "a.txt", "a"}, {"120000", "link", strings.Repeat("t", 5000)}})
	if _, err := PrepareArchive(context.Background(), dir, commit, ArchiveOptions{ExcludeGlobs: []string{"link"}}); err != nil {
		t.Fatalf("PrepareArchive with the symlink excluded: %v", err)
	}
	if _, err := PrepareArchive(context.Background(), dir, commit, ArchiveOptions{}); !errors.Is(err, ErrSymlinkTooLong) {
		t.Fatalf("PrepareArchive without the exclusion: err = %v, want ErrSymlinkTooLong", err)
	}
}

// ReadSymlinkTarget refuses a target over the limit before it starts git, and
// refuses a blob that is longer than the tree said it was.
func TestReadSymlinkTargetRefusesALongTarget(t *testing.T) {
	ctx := context.Background()
	long := strings.Repeat("t", 5000)
	dir, commit := fiRepo(t, []fiFile{{"120000", "short", "a.txt"}, {"120000", "long", long}})
	entries, err := LsTree(ctx, dir, commit, "", true)
	if err != nil {
		t.Fatal(err)
	}
	sha := map[string]TreeEntry{}
	for _, e := range entries {
		sha[e.Path] = e
	}
	log := gitCalls(t)
	if got, err := ReadSymlinkTarget(ctx, dir, sha["short"].SHA, sha["short"].Size); err != nil || got != "a.txt" {
		t.Fatalf("a short target: %q, %v", got, err)
	}
	before := catFileCalls(t, log)
	if _, err := ReadSymlinkTarget(ctx, dir, sha["long"].SHA, sha["long"].Size); !errors.Is(err, ErrSymlinkTooLong) {
		t.Fatalf("a target of 5000 bytes: err = %v, want ErrSymlinkTooLong", err)
	}
	if n := catFileCalls(t, log); n != before {
		t.Fatalf("git cat-file ran %d times for a target over the limit", n-before)
	}
	// The size came from a tree that listed a short blob: the blob git prints
	// is longer, as with a replacement object.
	if _, err := ReadSymlinkTarget(ctx, dir, sha["long"].SHA, 5); !errors.Is(err, ErrSymlinkTooLong) {
		t.Fatalf("a blob longer than its listed size: err = %v, want ErrSymlinkTooLong", err)
	}
}
