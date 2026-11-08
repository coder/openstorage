package lfsscan

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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
	oidD = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
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
		// git-lfs decodes pointers with leading blank lines (Codex security
		// round 8) and whitespace-indented version lines (Codex rounds 9):
		// rejecting one would retire the live object it names.
		{"leading blank line", "\n" + valid, oidA, true},
		{"leading whitespace lines", " \r\n\t\n" + valid, oidA, true},
		{"only blank lines", "\n \n\t\n", "", false},
		{"space-indented version line", " " + valid, oidA, true},
		{"tab-indented version line", "\t" + valid, oidA, true},
		// git-lfs rejects an indented oid line, but sparing MORE is the
		// safe direction for a deleter's parser.
		{"indented oid line", "version https://git-lfs.github.com/spec/v1\n  oid sha256:" + oidA + "\n", oidA, true},
		{"missing size still parses", "version https://git-lfs.github.com/spec/v1\noid sha256:" + oidA + "\n", oidA, true},
		// git-lfs accepts ext-* metadata lines BEFORE version (Codex
		// security round 10) — ordering can never be load-bearing for a
		// deleter's parser, so any line order parses.
		{"ext line before version", "ext-0-foo sha256:" + oidC + "\nversion https://git-lfs.github.com/spec/v1\noid sha256:" + oidA + "\nsize 1\n", oidA, true},
		{"version not first still parses", "size 5\nversion https://git-lfs.github.com/spec/v1\noid sha256:" + oidA + "\n", oidA, true},
		{"empty", "", "", false},
		{"no version line", "oid sha256:" + oidA + "\nsize 5\n", "", false},
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
	f.Add([]byte("\n \n" + lfsPointer(oidA, 5)))
	f.Add([]byte("\t" + lfsPointer(oidA, 5)))
	f.Add([]byte("ext-0-foo sha256:" + oidB + "\n" + lfsPointer(oidA, 5)))
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
	// Pointer-shaped prefix on an oversized blob: COLLECTED (Codex
	// security round 19) — the scan decodes every blob's first
	// LFSPointerMaxSize bytes, sparing on the prefix verdict, because a
	// total-size filter hid padded pointers git-lfs itself smudges.
	writeFile(t, dir, "big.txt", lfsPointer(oidD, 1)+strings.Repeat("x", 2000), 0o644)
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
	if _, ok := oids[oidD]; !ok {
		t.Errorf("oversized pointer-prefixed blob's OID missing (round 19): %v", oids)
	}

	// The budget aborts the walk with the sentinel and no OID set.
	if _, _, err := ReachableLFSOIDs(ctx, dir, 2); !errors.Is(err, ErrLFSScanBudget) {
		t.Fatalf("budget walk err = %v, want ErrLFSScanBudget", err)
	}
}

// TestReachableLFSOIDs_PaddedOversizedPointer (Codex security round 19):
// git-lfs decodes only the first blobSizeCutoff bytes and smudges a pointer
// of ANY total size whose window decodes — verified against git-lfs 3.7.1
// with this exact newline-padded 10,000-byte fixture. A total-size filter
// hid the live OID from every scan, so the row retired after grace and
// downloads 404'd. The blob must be collected AND charged by its full size.
func TestReachableLFSOIDs_PaddedOversizedPointer(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--initial-branch=main")
	padded := lfsPointer(oidA, 12345)
	padded += strings.Repeat("\n", 10_000-len(padded))
	writeFile(t, dir, "padded.bin", padded, 0o644)
	mustGit(t, dir, "add", "-A")
	mustGit(t, dir, "commit", "-q", "-m", "root")

	oids, walked, err := ReachableLFSOIDs(context.Background(), dir, 1_000)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := oids[oidA]; !ok {
		t.Fatalf("padded oversized pointer's OID missing: %v", oids)
	}
	// 1 commit + 1 tree + 1 blob, plus size units: the 10,000-byte blob
	// charges at least 10000/4096 = 2 extra units.
	if walked < 5 {
		t.Fatalf("walked = %d, want the padded blob charged by size", walked)
	}
}

// TestReachableLFSOIDs_LargeBlobsChargeBudget (Codex round 2): a blob:limit
// prefilter on rev-list would omit oversized blobs from its output while git
// still traverses every one of them, so a writer could stuff trees with large
// blobs that consume unbounded walk work the budget never sees. Every
// reachable object — including blobs too big to be pointer files — must
// charge one budget unit.
func TestReachableLFSOIDs_LargeBlobsChargeBudget(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--initial-branch=main")
	writeFile(t, dir, "a.bin", lfsPointer(oidA, 100), 0o644)
	for i := range 8 {
		writeFile(t, dir, fmt.Sprintf("big-%d.dat", i), strings.Repeat("x", 5000)+fmt.Sprint(i), 0o644)
	}
	mustGit(t, dir, "add", "-A")
	mustGit(t, dir, "commit", "-q", "-m", "root")

	// 1 commit + 1 tree + 1 pointer blob + 8 large blobs = 11 objects, and
	// each 5001-byte blob charges an extra size unit: 19 work units. A
	// size-prefiltered listing would emit only 3 objects and sail past a
	// budget of 6; charging every reachable object must trip it.
	if _, _, err := ReachableLFSOIDs(context.Background(), dir, 6); !errors.Is(err, ErrLFSScanBudget) {
		t.Fatalf("budget walk err = %v, want ErrLFSScanBudget", err)
	}

	// With enough budget the walk succeeds, the pointer is still collected,
	// and the charged units prove the large blobs were metered by size.
	oids, walked, err := ReachableLFSOIDs(context.Background(), dir, 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := oids[oidA]; !ok {
		t.Errorf("pointer OID missing: %v", oids)
	}
	if walked != 19 {
		t.Fatalf("walked = %d, want 19 (large blobs must charge size units)", walked)
	}
}

// TestReachableLFSOIDs_WideTreesChargeBudget (Codex security round 5): one
// git-valid tree can name the same blob under arbitrarily many entries, and
// rev-list expands every entry while emitting each unique OID only once — a
// flat per-object charge would meter a 100k-entry tree as 3 objects while
// git inflates megabytes. Objects charge by raw size too (a tree grows ~40
// bytes per entry), so entry stuffing pays for the expansion it causes.
func TestReachableLFSOIDs_WideTreesChargeBudget(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--initial-branch=main")
	writeFile(t, dir, "a.bin", lfsPointer(oidA, 100), 0o644)
	writeFile(t, dir, "junk.dat", "wide junk\n", 0o644)
	mustGit(t, dir, "add", "-A")
	mustGit(t, dir, "commit", "-q", "-m", "root")

	// Graft 12k index entries, all naming the junk blob, and commit them
	// on a second branch: one ~480KB raw tree, no new unique blobs.
	junkSHA := strings.TrimSpace(mustGit(t, dir, "rev-parse", "HEAD:junk.dat"))
	var entries strings.Builder
	for i := range 12_000 {
		fmt.Fprintf(&entries, "100644 %s\twide/entry-%06d\n", junkSHA, i)
	}
	mustGitStdin(t, dir, entries.String(), "update-index", "--add", "--index-info")
	treeSHA := strings.TrimSpace(mustGit(t, dir, "write-tree"))
	commitSHA := strings.TrimSpace(mustGitStdin(t, dir, "wide", "commit-tree", treeSHA, "-p", "HEAD"))
	mustGit(t, dir, "update-ref", "refs/heads/wide", commitSHA)

	// Flat charging would count ~8 objects and sail far under 60; the
	// wide subtree alone must charge its raw bytes' worth of units.
	if _, _, err := ReachableLFSOIDs(context.Background(), dir, 60); !errors.Is(err, ErrLFSScanBudget) {
		t.Fatalf("budget walk err = %v, want ErrLFSScanBudget", err)
	}

	oids, walked, err := ReachableLFSOIDs(context.Background(), dir, 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := oids[oidA]; !ok {
		t.Errorf("pointer OID missing: %v", oids)
	}
	if walked < 100 {
		t.Fatalf("walked = %d, want >= 100 (wide tree must charge size units)", walked)
	}
}

// TestReachableLFSOIDs_IgnoresReplacementRefs (Codex security round 1): a
// git:write pusher can mint refs/replace/* substitutions WITHOUT moving the
// protected refs they shadow — replacing a commit with a doctored twin
// whose tree omits the pointer files would hide their OIDs from a
// replacement-honoring walk and retire live objects. The walk must read
// the TRUE objects.
func TestReachableLFSOIDs_IgnoresReplacementRefs(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--initial-branch=main")
	writeFile(t, dir, "a.bin", lfsPointer(oidA, 100), 0o644)
	mustGit(t, dir, "add", "-A")
	mustGit(t, dir, "commit", "-q", "-m", "pointer commit")
	victim := strings.TrimSpace(mustGit(t, dir, "rev-parse", "HEAD"))
	pointerBlob := strings.TrimSpace(mustGit(t, dir, "rev-parse", victim+":a.bin"))

	// Doctored twin: same history shape, no pointer file — then a
	// replacement ref shadowing the victim commit, main unmoved.
	mustGit(t, dir, "rm", "-q", "a.bin")
	writeFile(t, dir, "cover.txt", "nothing to see\n", 0o644)
	mustGit(t, dir, "add", "-A")
	mustGit(t, dir, "commit", "-q", "-m", "doctored twin")
	doctored := strings.TrimSpace(mustGit(t, dir, "rev-parse", "HEAD"))
	mustGit(t, dir, "reset", "-q", "--hard", victim)
	mustGit(t, dir, "replace", victim, doctored)

	// Sanity: a replacement-honoring walk hides the pointer blob.
	if honored := mustGit(t, dir, "rev-list", "--objects", "refs/heads/main"); strings.Contains(honored, pointerBlob) {
		t.Fatalf("fixture broken: replacement did not shadow the pointer blob\n%s", honored)
	}

	oids, _, err := ReachableLFSOIDs(context.Background(), dir, 1_000)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := oids[oidA]; !ok {
		t.Fatalf("replacement ref hid a reachable pointer OID: %v", oids)
	}
}

// A refs/replace/<pointer blob> can also swap the pointer file's content
// for a blob that is no pointer, which cat-file --batch would read, or for
// an empty blob, which cat-file --batch-check would report as size 0, so
// that the walk never reads it. Every stage reads the true blob.
func TestReachableLFSOIDs_IgnoresReplacedBlobs(t *testing.T) {
	for _, tc := range []struct{ name, content string }{
		{"by text", "not a pointer\n"},
		{"by an empty blob", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			mustGit(t, dir, "init", "-q", "--initial-branch=main")
			writeFile(t, dir, "a.bin", lfsPointer(oidA, 100), 0o644)
			writeFile(t, dir, "other.txt", tc.content, 0o644)
			mustGit(t, dir, "add", "-A")
			mustGit(t, dir, "commit", "-q", "-m", "pointer commit")
			pointerBlob := strings.TrimSpace(mustGit(t, dir, "rev-parse", "HEAD:a.bin"))
			other := strings.TrimSpace(mustGit(t, dir, "rev-parse", "HEAD:other.txt"))
			mustGit(t, dir, "replace", pointerBlob, other)
			// Sanity: a replacement-honoring read gets the other content.
			if got := mustGit(t, dir, "cat-file", "-p", pointerBlob); got != tc.content {
				t.Fatalf("fixture broken: replacement did not swap the pointer blob (%d bytes)", len(got))
			}
			oids, _, err := ReachableLFSOIDs(context.Background(), dir, 1_000)
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := oids[oidA]; !ok {
				t.Fatalf("a replaced pointer blob hid its OID: %v", oids)
			}
		})
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

// gitEnv pins identity and neuters host config so fixture commits are
// hermetic.
var gitEnv = []string{
	"GIT_AUTHOR_NAME=Alice Author",
	"GIT_AUTHOR_EMAIL=alice@example.com",
	"GIT_COMMITTER_NAME=Bob Committer",
	"GIT_COMMITTER_EMAIL=bob@example.com",
	"GIT_CONFIG_GLOBAL=/dev/null",
	"GIT_CONFIG_SYSTEM=/dev/null",
}

func mustGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), gitEnv...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

func mustGitStdin(t *testing.T, dir, stdin string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), gitEnv...)
	cmd.Stdin = strings.NewReader(stdin)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

func writeFile(t *testing.T, dir, name, content string, mode os.FileMode) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
}
