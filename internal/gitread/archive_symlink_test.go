package gitread

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
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
// so a changed constant turns this red. The regular file is longer than a
// target may be, and the archive keeps it: only symlinks have this limit.
func TestPrepareArchiveCapsASymlinkTarget(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		name string
		size int
		ok   bool
	}{{"4095 bytes", 4095, true}, {"4096 bytes", 4096, false}, {"1 MiB", 1 << 20, false}} {
		t.Run(c.name, func(t *testing.T) {
			target := strings.Repeat("t", c.size)
			dir, commit := fiRepo(t, []fiFile{{"100644", "a.txt", strings.Repeat("a", 5000)}, {"120000", "link", target}})
			log := gitCalls(t)
			prepared, err := PrepareArchive(ctx, dir, commit, ArchiveOptions{})
			if !c.ok {
				if !errors.Is(err, ErrSymlinkTooLong) || !IsBudgetError(err) || errors.Is(err, ErrBlobTooLarge) {
					t.Fatalf("PrepareArchive err = %v, want a budget error ErrSymlinkTooLong", err)
				}
				if msg := err.Error(); !strings.Contains(msg, `"link"`) || !strings.Contains(msg, "4095") {
					t.Fatalf("the error does not name the path and the limit: %v", err)
				}
				if path, ok := LimitPath(err); !ok || path != "link" {
					t.Fatalf("LimitPath(%v) = %q, %v; want \"link\"", err, path, ok)
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

// A replacement object that appears after the tree was listed makes git print
// a longer blob than the listed size. The read stops at one byte over the
// limit: git is ended, and the process does not hold the replacement (8 MiB
// here; up to the output allowance in a request).
func TestReadSymlinkTargetBoundsAReplacementBlob(t *testing.T) {
	ctx := context.Background()
	big := strings.Repeat("t", 8<<20)
	dir, commit := fiRepo(t, []fiFile{{"120000", "link", "a.txt"}, {"100644", "big", big}})
	entries, err := LsTree(ctx, dir, commit, "", true)
	if err != nil {
		t.Fatal(err)
	}
	sha := map[string]TreeEntry{}
	for _, e := range entries {
		sha[e.Path] = e
	}
	if sha["link"].Size != 5 {
		t.Fatalf("link is listed as %d bytes, want 5", sha["link"].Size)
	}
	gitIn(t, dir, "", "replace", sha["link"].SHA, sha["big"].SHA) // after the listing

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err = ReadSymlinkTarget(ctx, dir, sha["link"].SHA, sha["link"].Size)
	runtime.ReadMemStats(&after)
	if !errors.Is(err, ErrSymlinkTooLong) {
		t.Fatalf("a replacement of 8 MiB: err = %v, want ErrSymlinkTooLong", err)
	}
	if n := after.TotalAlloc - before.TotalAlloc; n > 1<<20 {
		t.Fatalf("the read allocated %d KiB for a replacement of 8 MiB, want under 1 MiB", n>>10)
	}
	assertNoChildren(t)
}

// The read ends git once it has more than the limit: a git that prints without
// end (what a huge replacement blob looks like to the reader) fails the read
// at once, and no git is left running.
func TestReadSymlinkTargetEndsGitAtTheLimit(t *testing.T) {
	dir, commit := fiRepo(t, []fiFile{{"120000", "link", "a.txt"}})
	entries, err := LsTree(context.Background(), dir, commit, "", true)
	if err != nil {
		t.Fatal(err)
	}
	fakeGit(t, "for a in \"$@\"; do case \"$a\" in cat-file) exec yes t ;; esac; done\nexit 1")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	start := time.Now()
	_, err = ReadSymlinkTarget(ctx, dir, entries[0].SHA, entries[0].Size)
	if !errors.Is(err, ErrSymlinkTooLong) || time.Since(start) > 3*time.Second {
		t.Fatalf("a git that never stops: err = %v after %v, want ErrSymlinkTooLong at once", err, time.Since(start))
	}
	assertNoChildren(t)
}

// The writer keeps 4096 bytes at most (one over the limit of 4095, which is
// what tells a long target from one at the limit) and ends git with ErrStop on
// the 4096th. The sizes are numbers, not the constant.
func TestTargetWriterKeepsOneByteOverTheLimit(t *testing.T) {
	var w targetWriter
	if n, err := w.Write(make([]byte, 4000)); n != 4000 || err != nil {
		t.Fatalf("Write(4000) = %d, %v", n, err)
	}
	if n, err := w.Write(make([]byte, 95)); n != 95 || err != nil || len(w.buf) != 4095 {
		t.Fatalf("Write(95) = %d, %v with %d bytes held; a target of 4095 bytes is whole", n, err, len(w.buf))
	}
	if n, err := w.Write(make([]byte, 10<<10)); n != 10<<10 || !errors.Is(err, ErrStop) || len(w.buf) != 4096 {
		t.Fatalf("Write(10 KiB) = %d, %v with %d bytes held; want ErrStop and 4096 bytes", n, err, len(w.buf))
	}
}

// A limit error keeps the path it was given, through the wrapping that
// callers add, so that a response can name the entry. An error that came
// from neither check names none.
func TestLimitErrorsNameTheirPath(t *testing.T) {
	big := CheckBlobSize(context.Background(), "dir/big.bin", MaxBlobBytes+1)
	long := CheckSymlinkTarget("dir/link", MaxSymlinkTarget+1)
	for _, c := range []struct {
		name string
		err  error
		is   error
		path string
	}{
		{"blob", big, ErrBlobTooLarge, "dir/big.bin"},
		{"symlink", long, ErrSymlinkTooLong, "dir/link"},
		{"wrapped", fmt.Errorf("archive: %w", long), ErrSymlinkTooLong, "dir/link"},
	} {
		if !errors.Is(c.err, c.is) || !IsBudgetError(c.err) {
			t.Fatalf("%s: %v is not a budget error %v", c.name, c.err, c.is)
		}
		if path, ok := LimitPath(c.err); !ok || path != c.path {
			t.Fatalf("%s: LimitPath = %q, %v; want %q", c.name, path, ok, c.path)
		}
	}
	if path, ok := LimitPath(ErrBlobTooLarge); ok {
		t.Fatalf("the bare error names %q", path)
	}
}
