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
	// rev-parse peels an abbreviation of c1 (no ref has its name), and
	// for-each-ref reads main.
	for _, tc := range []struct {
		sub, spec, name, sh string
		notFound            bool
	}{
		{"for-each-ref", "main", "exit 1", "exit 1", false},
		{"for-each-ref", "main", "exit 128", "echo 'fatal: bad' >&2; exit 128", false},
		{"for-each-ref", "main", "SIGKILL", "kill -KILL $$", false},
		{"rev-parse", f.c1[:12], "exit 1", "exit 1", true},
		{"rev-parse", f.c1[:12], "exit 128", "echo 'fatal: bad' >&2; exit 128", false},
		{"rev-parse", f.c1[:12], "SIGKILL", "kill -KILL $$", false},
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
	for _, tc := range []struct {
		name, spec, rules string
	}{
		{"object of an abbreviation exits 128", tagObj[:12], "[ \"$1\" = rev-parse ] && [ \"$a\" = " + tagObj[:12] + " ] && exit 128\n"},
		{"cat-file -e exits 128", "tree-tag", "[ \"$1\" = cat-file ] && [ \"$a\" = -e ] && exit 128\n"},
		{"cat-file -e is killed", "tree-tag", "[ \"$1\" = cat-file ] && [ \"$a\" = -e ] && kill -KILL $$\n"},
		{"cat-file -e of a full id exits 128", tagObj, "[ \"$1\" = cat-file ] && [ \"$a\" = -e ] && exit 128\n"},
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

// ResolveRef reads every ref candidate with one for-each-ref, and peels
// only what exists: a branch, a tag and a full id each take two git runs.
// main took 2, 3 and 4: one check-ref-format, then one rev-parse per
// candidate (refs/heads/<spec>, refs/tags/<spec>, the id).
func TestResolveRefGitRuns(t *testing.T) {
	f := newFixture(t)
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, spec string
		runs       int
	}{
		{"branch", "main", 2},
		{"tag", "v1", 2},
		{"full id", f.c1, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := filepath.Join(t.TempDir(), "runs")
			fakeGit(t, "echo \"$1\" >> '"+log+"'\nexec '"+realGit+"' \"$@\"")
			if _, err := ResolveRef(context.Background(), f.dir, tc.spec, false); err != nil {
				t.Fatalf("ResolveRef(%s): %v", tc.spec, err)
			}
			got, err := os.ReadFile(log)
			if err != nil {
				t.Fatal(err)
			}
			if n := strings.Count(string(got), "\n"); n != tc.runs {
				t.Fatalf("ResolveRef(%s) ran git %d times (%q), want %d", tc.spec, n, got, tc.runs)
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
}
