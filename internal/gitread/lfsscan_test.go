package gitread

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func lfsPointer(oid string, size int) string {
	return fmt.Sprintf("version https://git-lfs.github.com/spec/v1\noid sha256:%s\nsize %d\n", oid, size)
}

const (
	oidA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	oidB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	oidC = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
)

func TestParseLFSPointer(t *testing.T) {
	valid := lfsPointer(oidA, 12345)
	cases := []struct {
		name string
		in   string
		oid  string
		ok   bool
	}{
		{"canonical", valid, oidA, true},
		{"hawser legacy version", "version https://hawser.github.com/spec/v1\noid sha256:" + oidB + "\nsize 5\n", oidB, true},
		{"no trailing newline", strings.TrimSuffix(valid, "\n"), oidA, true},
		{"crlf line endings", strings.ReplaceAll(valid, "\n", "\r\n"), oidA, true},
		{"extension keys ignored", "version https://git-lfs.github.com/spec/v1\next-0-foo sha256:" + oidC + "\noid sha256:" + oidA + "\nsize 1\n", oidA, true},
		{"missing size still parses", "version https://git-lfs.github.com/spec/v1\noid sha256:" + oidA + "\n", oidA, true},
		{"empty", "", "", false},
		{"no version line", "oid sha256:" + oidA + "\nsize 5\n", "", false},
		{"version not first", "size 5\nversion https://git-lfs.github.com/spec/v1\noid sha256:" + oidA + "\n", "", false},
		{"missing oid", "version https://git-lfs.github.com/spec/v1\nsize 5\n", "", false},
		{"short oid", "version https://git-lfs.github.com/spec/v1\noid sha256:abcd\n", "", false},
		{"uppercase oid", "version https://git-lfs.github.com/spec/v1\noid sha256:" + strings.ToUpper(oidA) + "\n", "", false},
		{"non-hex oid", "version https://git-lfs.github.com/spec/v1\noid sha256:" + strings.Repeat("z", 64) + "\n", "", false},
		{"sha1 oid scheme", "version https://git-lfs.github.com/spec/v1\noid sha1:" + oidA + "\n", "", false},
		{"nul byte", "version https://git-lfs.github.com/spec/v1\n\x00oid sha256:" + oidA + "\n", "", false},
		{"oversized", valid + strings.Repeat("#", LFSPointerMaxSize), "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			oid, ok := ParseLFSPointer([]byte(tc.in))
			if ok != tc.ok || oid != tc.oid {
				t.Fatalf("ParseLFSPointer(%q) = (%q, %v), want (%q, %v)", tc.in, oid, ok, tc.oid, tc.ok)
			}
		})
	}
}

func FuzzLFSPointerParse(f *testing.F) {
	f.Add([]byte(lfsPointer(oidA, 5)))
	f.Add([]byte("version https://hawser.github.com/spec/v1\noid sha256:" + oidB + "\nsize 0\n"))
	f.Add([]byte("version https://git-lfs.github.com/spec/v1\noid sha256:short\n"))
	f.Add([]byte("version https://git-lfs.github.com/spec/v1"))
	f.Add([]byte("oid sha256:" + oidC))
	f.Add([]byte(""))
	f.Add([]byte("\x00\x01\x02"))
	f.Add([]byte(strings.Repeat("version https://git-lfs.github.com/spec/v1\n", 30)))
	f.Fuzz(func(t *testing.T, data []byte) {
		oid, ok := ParseLFSPointer(data)
		if ok && !validLFSOID(oid) {
			t.Fatalf("accepted invalid oid %q", oid)
		}
		if !ok && oid != "" {
			t.Fatalf("rejected input returned oid %q", oid)
		}
	})
}

func TestReachableLFSOIDs(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--initial-branch=main")
	writeFile(t, dir, "a.bin", lfsPointer(oidA, 100), 0o644)
	writeFile(t, dir, "small.txt", "just a small file\n", 0o644)
	// Pointer-shaped prefix but oversized: excluded by the size cutoff.
	writeFile(t, dir, "big.txt", lfsPointer(oidC, 1)+strings.Repeat("x", 2000), 0o644)
	mustGit(t, dir, "add", "-A")
	mustGit(t, dir, "commit", "-q", "-m", "root")

	// Pointer B reachable only from a feature branch.
	mustGit(t, dir, "checkout", "-q", "-b", "feature")
	writeFile(t, dir, "b.bin", lfsPointer(oidB, 200), 0o644)
	mustGit(t, dir, "add", "b.bin")
	mustGit(t, dir, "commit", "-q", "-m", "feature")
	mustGit(t, dir, "checkout", "-q", "main")

	// Pointer C becomes unreachable when its branch is deleted.
	mustGit(t, dir, "checkout", "-q", "-b", "doomed")
	writeFile(t, dir, "c.bin", lfsPointer(oidC, 300), 0o644)
	mustGit(t, dir, "add", "c.bin")
	mustGit(t, dir, "commit", "-q", "-m", "doomed")
	mustGit(t, dir, "checkout", "-q", "main")
	mustGit(t, dir, "branch", "-q", "-D", "doomed")

	ctx := context.Background()
	oids, walked, err := ReachableLFSOIDs(ctx, dir, 1_000)
	if err != nil {
		t.Fatal(err)
	}
	if walked <= 0 {
		t.Fatalf("walked = %d, want > 0", walked)
	}
	if _, ok := oids[oidA]; !ok {
		t.Errorf("main-branch pointer OID missing: %v", oids)
	}
	if _, ok := oids[oidB]; !ok {
		t.Errorf("feature-branch pointer OID missing: %v", oids)
	}
	if _, ok := oids[oidC]; ok {
		t.Errorf("unreachable pointer OID collected: %v", oids)
	}

	// The budget aborts the walk with the sentinel and no OID set.
	if _, _, err := ReachableLFSOIDs(ctx, dir, 2); !errors.Is(err, ErrLFSScanBudget) {
		t.Fatalf("budget walk err = %v, want ErrLFSScanBudget", err)
	}
}

func TestReachableLFSOIDs_EmptyRepo(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--initial-branch=main")
	oids, walked, err := ReachableLFSOIDs(context.Background(), dir, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(oids) != 0 || walked != 0 {
		t.Fatalf("empty repo walk = (%v, %d)", oids, walked)
	}
}
