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
// ResolveRef answer ErrNotFound. for-each-ref, which reads the refs, has
// no such answer: every failure of it is an error. A fatal exit, a killed
// git or a corrupt repository says nothing about the ref.
func TestResolveRefOnlyExitOneIsNotFound(t *testing.T) {
	f := newFixture(t)
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	// rev-parse peels an abbreviation that no object and no ref has, and
	// for-each-ref reads main.
	const noObject = "0000000000ab"
	for _, tc := range []struct {
		sub, spec, name, sh string
		notFound            bool
	}{
		{"for-each-ref", "main", "exit 1", "exit 1", false},
		{"for-each-ref", "main", "exit 128", "echo 'fatal: bad' >&2; exit 128", false},
		{"for-each-ref", "main", "SIGKILL", "kill -KILL $$", false},
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
	// A git that lists no ref.
	fakeGit(t, "echo x >> '"+marker+"'\nexit 0")
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
	for _, spec := range []string{"nope", "refs/heads/nope", "tree-tag", tree, c2, c2[:12], strings.Repeat("1", 40)} {
		if sha, err := ResolveRef(ctx, dir, spec, false); !errors.Is(err, ErrNotFound) {
			t.Errorf("ResolveRef(%q) = (%q, %v), want ErrNotFound", spec, sha, err)
		}
	}
	if _, err := ResolveRef(ctx, dir, "main", false); err != nil {
		t.Errorf("ResolveRef(main): %v", err)
	}
}

// commitUnreadable reads each of its git runs by its exit: only git's own
// "no" (exit 1) leaves a candidate unknown. Any other failure of a git
// run, and a commit at the end of the chain that rev-parse ^{commit}
// could not read, is an error, never ErrNotFound.
func TestCommitUnreadableGitFailures(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--initial-branch=main")
	writeFile(t, dir, "a.txt", "one\n", 0o644)
	mustGit(t, dir, "add", "-A")
	mustGit(t, dir, "commit", "-q", "-m", "c1")
	tree := strings.TrimSpace(mustGit(t, dir, "rev-parse", "HEAD^{tree}"))
	mustGit(t, dir, "tag", "-a", "-m", "tree", "tree-tag", tree)
	tagObj := strings.TrimSpace(mustGit(t, dir, "rev-parse", "refs/tags/tree-tag"))
	c1 := strings.TrimSpace(mustGit(t, dir, "rev-parse", "HEAD"))
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	// rev-parse peels the object id that for-each-ref read for main.
	noCommit := "[ \"$1\" = rev-parse ] && [ \"$a\" = '" + c1 + "^{commit}' ] && exit 1\n"
	const batchCheck = "[ \"$1\" = cat-file ] && [ \"$a\" = '--batch-check=%(objectname) %(objecttype)' ] && "
	for _, tc := range []struct {
		name, spec, rules string
	}{
		{"batch-check exits 128", "tree-tag", batchCheck + "exit 128\n"},
		{"batch-check is killed", "tree-tag", batchCheck + "kill -KILL $$\n"},
		{"batch-check of a full id exits 128", tagObj, batchCheck + "exit 128\n"},
		{"batch-check of an abbreviation exits 128", tagObj[:12], batchCheck + "exit 128\n"},
		{"batch-check answers for another object", "tree-tag", batchCheck + "{ echo \"" + tree + " tree\"; exit 0; }\n"},
		{"peel to the end exits 128", "tree-tag", "[ \"$1\" = rev-parse ] && [ \"$a\" = '" + tagObj + "^{}' ] && exit 128\n"},
		{"cat-file -t exits 128", "tree-tag", "[ \"$1\" = cat-file ] && [ \"$a\" = -t ] && exit 128\n"},
		{"a commit at the end of the chain", "main", noCommit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fakeGit(t, "for a in \"$@\"; do\n"+tc.rules+"done\nexec '"+realGit+"' \"$@\"")
			if sha, err := ResolveRef(context.Background(), dir, tc.spec, false); err == nil || errors.Is(err, ErrNotFound) {
				t.Fatalf("ResolveRef(%q) = (%q, %v), want an error that is not ErrNotFound", tc.spec, sha, err)
			}
		})
	}
	// The same fake git with no rule that fires: main resolves, and the
	// tag of a tree is no commit.
	fakeGit(t, "for a in \"$@\"; do\n[ \"$a\" = never-an-argument ] && exit 128\ndone\nexec '"+realGit+"' \"$@\"")
	if _, err := ResolveRef(context.Background(), dir, "main", false); err != nil {
		t.Fatalf("ResolveRef(main) through the fake git: %v", err)
	}
	if sha, err := ResolveRef(context.Background(), dir, "tree-tag", false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ResolveRef(tree-tag) = (%q, %v), want ErrNotFound", sha, err)
	}
}

// ResolveRef reads every ref candidate with one for-each-ref, peels only
// what exists, and reads an object that is no commit with one
// batch-check. Each read below takes at most the git runs that main took
// (one check-ref-format, then one rev-parse per candidate:
// refs/heads/<spec>, refs/tags/<spec>, the id).
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
		{"tag", "v1", 2, 3, true},
		{"full id", f.c1, 2, 4, true},
		{"unknown name", "nope", 1, 3, false},
		{"abbreviation of a commit", f.c1[:12], 2, 4, true},
		{"tree id", tree, 3, 4, false},
		{"abbreviation of a blob", blob[:12], 3, 4, false},
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

// for-each-ref gets each ref candidate as a whole name: refs under a
// name that is no ref (refs/heads/pfx/a for "pfx") stay out of its output,
// however many there are.
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
	fakeGit(t, "if [ \"$1\" = for-each-ref ]; then out=$('"+realGit+"' \"$@\"); rc=$?; printf '%s' \"$out\" >> '"+log+"'; printf '%s\\n' \"$out\"; exit $rc; fi\nexec '"+realGit+"' \"$@\"")
	if sha, err := ResolveRef(context.Background(), f.dir, "pfx", false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ResolveRef(pfx) = (%q, %v), want ErrNotFound", sha, err)
	}
	if got, err := os.ReadFile(log); err != nil || len(got) != 0 {
		t.Fatalf("for-each-ref printed %q (%v), want nothing", got, err)
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

// for-each-ref's patterns each match one whole name: a line for any other
// ref, or one that is no "<name> <id>", is git's failure, not an answer.
func TestResolveRefRefusesOtherForEachRefLines(t *testing.T) {
	f := newFixture(t)
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{"refs/heads/other " + f.c1, "refs/heads/main", "refs/heads/main zz"} {
		fakeGit(t, "if [ \"$1\" = for-each-ref ]; then echo '"+line+"'; exit 0; fi\nexec '"+realGit+"' \"$@\"")
		if sha, err := ResolveRef(context.Background(), f.dir, "main", false); err == nil || errors.Is(err, ErrNotFound) {
			t.Errorf("for-each-ref printing %q: ResolveRef(main) = (%q, %v), want an error that is not ErrNotFound", line, sha, err)
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
