package gitread

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

// Only git's own "no" (exit 1 from rev-parse --verify --quiet) makes
// ResolveRef answer ErrNotFound. Every failure of cat-file --batch-check
// is an error. A fatal exit, a killed git or a corrupt repository says
// nothing about the ref.
func TestResolveRefOnlyExitOneIsNotFound(t *testing.T) {
	f := newFixture(t)
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	// rev-parse peels an abbreviation that no object and no ref has, and
	// cat-file reads main's object.
	const noObject = "0000000000ab"
	for _, tc := range []struct {
		sub, spec, name, sh string
		notFound            bool
	}{
		{"cat-file", "main", "exit 1", "exit 1", false},
		{"cat-file", "main", "exit 128", "echo 'fatal: bad' >&2; exit 128", false},
		{"cat-file", "main", "SIGKILL", "kill -KILL $$", false},
		{"rev-parse", noObject, "exit 1", "exit 1", true},
		{"rev-parse", noObject, "exit 128", "echo 'fatal: bad' >&2; exit 128", false},
		{"rev-parse", noObject, "SIGKILL", "kill -KILL $$", false},
	} {
		t.Run(tc.sub+"/"+tc.name, func(t *testing.T) {
			fakeGit(t, "if [ \"$1\" = "+tc.sub+" ]; then "+tc.sh+"; fi\nexec '"+realGit+"' \"$@\"")
			sha, err := ResolveRef(context.Background(), f.dir, tc.spec, false)
			if err == nil || errors.Is(err, ErrNotFound) != tc.notFound {
				t.Fatalf("ResolveRef(%s) = (%q, %v), want an error that is ErrNotFound: %v", tc.spec, sha, err, tc.notFound)
			}
		})
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
	// A git that resolves no ref.
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
	blob := strings.TrimSpace(mustGit(t, dir, "rev-parse", "HEAD:a.txt"))
	mustGit(t, dir, "tag", "-a", "-m", "blob", "blob-tag", blob)
	writeFile(t, dir, "a.txt", "two\n", 0o644)
	mustGit(t, dir, "commit", "-q", "-am", "c2")
	c2 := strings.TrimSpace(mustGit(t, dir, "rev-parse", "HEAD"))
	mustGit(t, dir, "branch", "gone", c2)
	mustGit(t, dir, "tag", "-a", "-m", "gone", "gone-tag", c2)
	mustGit(t, dir, "tag", "-a", "-m", "gone again", "gone-tag-tag", "gone-tag")
	tagObj := strings.TrimSpace(mustGit(t, dir, "rev-parse", "refs/tags/gone-tag"))
	mustGit(t, dir, "reset", "-q", "--hard", "HEAD~1")
	c1 := strings.TrimSpace(mustGit(t, dir, "rev-parse", "HEAD"))
	// A broken branch is not shadowed by a good tag of its name, and a
	// broken branch named like an id not by that id.
	mustGit(t, dir, "branch", "dup", c2)
	mustGit(t, dir, "tag", "dup", c1)
	mustGit(t, dir, "update-ref", "refs/heads/"+c1, c2)
	if err := os.Remove(filepath.Join(dir, ".git", "objects", c2[:2], c2[2:])); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, spec := range []string{"gone", "refs/heads/gone", "gone-tag", "gone-tag-tag", tagObj, strings.ToUpper(tagObj), tagObj[:12], tagObj[:7], "dup", c1} {
		if sha, err := ResolveRef(ctx, dir, spec, false); err == nil || errors.Is(err, ErrNotFound) {
			t.Errorf("ResolveRef(%q) = (%q, %v), want an error that is not ErrNotFound", spec, sha, err)
		}
	}
	for _, spec := range []string{"nope", "refs/heads/nope", "tree-tag", "blob-tag", tree, c2, c2[:12], strings.Repeat("1", 40)} {
		if sha, err := ResolveRef(ctx, dir, spec, false); !errors.Is(err, ErrNotFound) {
			t.Errorf("ResolveRef(%q) = (%q, %v), want ErrNotFound", spec, sha, err)
		}
	}
	if _, err := ResolveRef(ctx, dir, "main", false); err != nil {
		t.Errorf("ResolveRef(main): %v", err)
	}
}

// The batch-check is a structured answer to three fixed requests: the
// object's type, its peel to a commit, and its peel through any tag
// chain. Every failed run and every incomplete or unexpected answer is
// git's failure, not "not found".
func TestResolveCommitObjectGitFailures(t *testing.T) {
	f := newFixture(t)
	tagObj := strings.TrimSpace(mustGit(t, f.dir, "rev-parse", "refs/tags/v1"))
	tree := strings.TrimSpace(mustGit(t, f.dir, "rev-parse", f.c1+"^{tree}"))
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	q := func(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }
	for _, tc := range []struct{ name, spec, script string }{
		{"batch-check exits 128", "main", "exit 128"},
		{"batch-check is killed", "main", "kill -KILL $$"},
		{"batch-check for a full id exits 128", f.c1, "exit 128"},
		{"batch-check for an abbreviation exits 128", f.c1[:12], "exit 128"},
		{"batch-check answers for another object", "main", "printf '%s' " + q(tree+" tree\n"+f.c1+" commit\n"+tree+" tree\n")},
		{"no batch answer", "main", "exit 0"},
		{"invalid peel answer", "main", "printf '%s' " + q(f.c2+" commit\nzz commit\n"+f.c2+" commit\n")},
		{"invalid peel answer for a tree", tree, "printf '%s' " + q(tree+" tree\nzz commit\n"+tree+" tree\n")},
		{"a commit with no readable commit peel", "main", "printf '%s' " + q(f.c2+" commit\n"+f.c2+"^{commit} missing\n"+f.c2+" commit\n")},
		// A survivor in the earlier design: cat-file -t named a commit
		// at the end of a tag chain, but the code read it as not found.
		{"tag chain ends at a commit but commit peel failed", "v1", "printf '%s' " + q(tagObj+" tag\n"+tagObj+"^{commit} missing\n"+f.c1+" commit\n")},
		{"tag chain cannot be read", "v1", "printf '%s' " + q(tagObj+" tag\n"+tagObj+"^{commit} missing\n"+tagObj+"^{} missing\n")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fakeGit(t, "if [ \"$1\" = cat-file ]; then "+tc.script+"; exit 0; fi\nexec '"+realGit+"' \"$@\"")
			if sha, err := ResolveRef(context.Background(), f.dir, tc.spec, false); err == nil || errors.Is(err, ErrNotFound) {
				t.Fatalf("ResolveRef(%q) = (%q, %v), want an error that is not ErrNotFound", tc.spec, sha, err)
			}
		})
	}
}

// ResolveRef reads one candidate at a time with rev-parse (no peel, so
// a ref's missing object still resolves), and the object's type and peel
// with one batch-check. Each read below takes at most main's git runs.
func TestResolveRefGitRuns(t *testing.T) {
	f := newFixture(t)
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	tree := strings.TrimSpace(mustGit(t, f.dir, "rev-parse", f.c1+"^{tree}"))
	blob := strings.TrimSpace(mustGit(t, f.dir, "rev-parse", f.c1+":README.md"))
	for _, tc := range []struct {
		name, spec string
		runs, main int
		found      bool
	}{
		{"branch", "main", 2, 2, true},
		{"tag", "v1", 3, 3, true},
		{"full id", f.c1, 4, 4, true},
		{"unknown name", "nope", 2, 3, false},
		{"abbreviation of a commit", f.c1[:12], 4, 4, true},
		{"tree id", tree, 4, 4, false},
		{"abbreviation of a blob", blob[:12], 4, 4, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := filepath.Join(t.TempDir(), "runs")
			fakeGit(t, "echo \"$1\" >> '"+log+"'\nexec '"+realGit+"' \"$@\"")
			if _, err := ResolveRef(context.Background(), f.dir, tc.spec, false); (err == nil) != tc.found || (err != nil && !errors.Is(err, ErrNotFound)) {
				t.Fatalf("ResolveRef(%s): %v, want found %v", tc.spec, err, tc.found)
			}
			got, err := os.ReadFile(log)
			if err != nil {
				t.Fatal(err)
			}
			if n := strings.Count(string(got), "\n"); n != tc.runs || n > tc.main {
				t.Fatalf("ResolveRef(%s) ran git %d times (%q), want %d (main ran %d)", tc.spec, n, got, tc.runs, tc.main)
			}
		})
	}
}

// Refs under a name that is no ref (refs/heads/pfx/a for "pfx") are
// not candidates. No candidate resolution lists ref directories.
func TestResolveRefListsNoRefsUnderTheName(t *testing.T) {
	f := newFixture(t)
	for _, r := range []string{"refs/heads/pfx/a", "refs/heads/pfx/b", "refs/tags/pfx/c"} {
		mustGit(t, f.dir, "update-ref", r, f.c1)
	}
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	log := filepath.Join(t.TempDir(), "for-each-ref")
	fakeGit(t, "if [ \"$1\" = for-each-ref ]; then echo listing >> '"+log+"'; exit 128; fi\nexec '"+realGit+"' \"$@\"")
	if sha, err := ResolveRef(context.Background(), f.dir, "pfx", false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ResolveRef(pfx) = (%q, %v), want ErrNotFound", sha, err)
	}
	if got, err := os.ReadFile(log); !os.IsNotExist(err) || len(got) != 0 {
		t.Fatalf("a ref-listing command ran: %q (%v)", got, err)
	}
	if sha, err := ResolveRef(context.Background(), f.dir, "pfx/a", false); err != nil || sha != f.c1 {
		t.Fatalf("ResolveRef(pfx/a) = (%q, %v), want c1", sha, err)
	}
	// A glob character cannot be in a ref's name: a spec with one names
	// no ref, though as a pattern it would match main.
	for _, spec := range []string{"ma?n", "m*", "[m]ain", "mai\\n"} {
		if sha, err := ResolveRef(context.Background(), f.dir, spec, false); !errors.Is(err, ErrNotFound) {
			t.Errorf("ResolveRef(%q) = (%q, %v), want ErrNotFound", spec, sha, err)
		}
	}
}

// An abbreviation that more than one object has names no one object:
// ErrNotFound, as an unknown one.
func TestResolveRefAmbiguousAbbreviationIsNotFound(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--initial-branch=main")
	// 400 blobs: two of them share a 4-character prefix.
	var paths []string
	for i := range 400 {
		p := filepath.Join(dir, fmt.Sprintf("b%d", i))
		if err := os.WriteFile(p, []byte(fmt.Sprintf("blob %d\n", i)), 0o644); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, p)
	}
	ids := strings.Fields(mustGit(t, dir, append([]string{"hash-object", "-w"}, paths...)...))
	seen := map[string]bool{}
	prefix := ""
	for _, id := range ids {
		if seen[id[:4]] {
			prefix = id[:4]
			break
		}
		seen[id[:4]] = true
	}
	if prefix == "" {
		t.Fatal("no two of 400 blobs share a 4-character prefix")
	}
	if sha, err := ResolveRef(context.Background(), dir, prefix, false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ResolveRef(%s) = (%q, %v), want ErrNotFound", prefix, sha, err)
	}
}

// ResolveRef must read candidate names directly, not list the cache's
// loose-ref directories. The ref debug trace records both direct reads
// and iterator steps. 20,000 irrelevant loose refs must add neither.
func TestResolveRefManyLooseRefsDoNotAddWork(t *testing.T) {
	f := newFixture(t)
	trace := func() string {
		t.Helper()
		log := filepath.Join(t.TempDir(), "refs.trace")
		t.Setenv("GIT_TRACE_REFS", log)
		for _, spec := range []string{"main", "v1", f.c1} {
			if _, err := ResolveRef(context.Background(), f.dir, spec, false); err != nil {
				t.Fatalf("ResolveRef(%s): %v", spec, err)
			}
		}
		got, err := os.ReadFile(log)
		if err != nil {
			t.Fatalf("read GIT_TRACE_REFS: %v", err)
		}
		if !strings.Contains(string(got), "read_raw_ref:") {
			t.Fatal("git did not trace ref reads")
		}
		return string(got)
	}
	before := trace()
	dir := filepath.Join(f.dir, ".git", "refs", "heads", "unrelated")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for i := range 20_000 {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("r%05d", i)), []byte(f.c1+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	after := trace()
	if steps := strings.Count(after, "iterator_advance:"); steps != 0 {
		t.Fatalf("with 20,000 loose refs: %d ref iterator steps, want 0 (no listing)", steps)
	}
	if b, a := strings.Count(before, "read_raw_ref:"), strings.Count(after, "read_raw_ref:"); a != b {
		t.Fatalf("20,000 loose refs changed direct ref reads from %d to %d", b, a)
	}
}

// Keep rev-parse's short-name DWIM rules, also for a "refs/" spec: main
// resolved refs/r to refs/heads/refs/r. No name is a revision expression.
func TestResolveRefKeepsGitDWIM(t *testing.T) {
	f := newFixture(t)
	for name, sha := range map[string]string{
		"refs/heads/refs/r":       f.c1,
		"refs/heads/refs/heads/r": f.c2,
		"refs/tags/refs/t":        f.c1,
	} {
		mustGit(t, f.dir, "update-ref", name, sha)
	}
	for _, tc := range []struct{ spec, sha string }{{"refs/r", f.c1}, {"r", f.c2}, {"refs/t", f.c1}} {
		if got, err := ResolveRef(context.Background(), f.dir, tc.spec, false); err != nil || got != tc.sha {
			t.Errorf("ResolveRef(%s) = (%q, %v), want %s", tc.spec, got, err, tc.sha)
		}
	}
	for _, spec := range []string{"refs/r~1", "refs/r^{tree}", "refs/r:a.txt", "refs/r@{0}", "refs/r\nrefs/heads/main", "refs/r\r", "refs/r\x01"} {
		if got, err := ResolveRef(context.Background(), f.dir, spec, false); !errors.Is(err, ErrNotFound) {
			t.Errorf("ResolveRef(%q) = (%q, %v), want ErrNotFound", spec, got, err)
		}
	}
}

// Name lookups must not interpret a request as a revision expression or
// a batch frame. Ref names contain none of these bytes, so they can be
// refused before git starts. An ordinary name still reaches git.
func TestResolveRefRevisionSyntaxSkipsGit(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "ran")
	fakeGit(t, "echo x >> '"+marker+"'\nexit 1")
	for _, spec := range []string{"refs/r~1", "refs/r^{}", "refs/r:path", "refs/r@{0}", "refs/r..refs/t", "r?", "r*", "[r]", "r\\x", "refs/r\nmain", "refs/r\x01", "refs/r "} {
		if _, err := ResolveRef(context.Background(), t.TempDir(), spec, false); !errors.Is(err, ErrNotFound) {
			t.Errorf("ResolveRef(%q): %v, want ErrNotFound", spec, err)
		}
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("git started for a revision operator or control byte: %v", err)
	}
	if _, err := ResolveRef(context.Background(), t.TempDir(), "refs/r", false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ordinary name: %v, want ErrNotFound from git", err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("ordinary name did not reach git: %v", err)
	}
}
