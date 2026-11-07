package gitread

// Fixture-based unit tests: every test builds (or reuses) a tiny real git
// repo in a temp dir and asserts parser outputs against known content —
// no containers, no network.

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// gitEnv pins author/committer identity and timestamps so parsed fields are
// deterministic.
var gitEnv = []string{
	"GIT_AUTHOR_NAME=Alice Author",
	"GIT_AUTHOR_EMAIL=alice@example.com",
	"GIT_COMMITTER_NAME=Bob Committer",
	"GIT_COMMITTER_EMAIL=bob@example.com",
	"GIT_AUTHOR_DATE=2024-01-02T03:04:05+02:00",
	"GIT_COMMITTER_DATE=2024-01-02T04:05:06+02:00",
	"GIT_CONFIG_GLOBAL=/dev/null",
	"GIT_CONFIG_SYSTEM=/dev/null",
}

func mustGit(t testing.TB, dir string, args ...string) string {
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

// fixture is the shared multi-commit test repo:
//
//	c1 (root, main): README.md, dir/data.txt, script.sh (0755),
//	                 link -> README.md, bin.dat (binary)
//	c2 (main tip):   README.md modified, dir/data.txt -> dir/renamed.txt,
//	                 extra.txt added, bin.dat modified
//	c3 (feature):    feature.txt added on top of c1
//	tags:            v1 (annotated, on c1), v2-light (lightweight, on c2)
//	ephemeral:       refs/namespaces/ephemeral/refs/heads/main -> c1
type fixture struct {
	dir        string
	c1, c2, c3 string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	dir := t.TempDir()
	f := &fixture{dir: dir}
	mustGit(t, dir, "init", "-q", "--initial-branch=main")

	writeFile(t, dir, "README.md", "# Title\nline two\nline three\n", 0o644)
	writeFile(t, dir, "dir/data.txt", "data line 1\ndata line 2\n", 0o644)
	writeFile(t, dir, "script.sh", "#!/bin/sh\necho hi\n", 0o755)
	writeFile(t, dir, "bin.dat", "\x00\x01\x02binary\x00", 0o644)
	if err := os.Symlink("README.md", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	mustGit(t, dir, "add", "-A")
	mustGit(t, dir, "commit", "-q", "-m", "root commit\n\nbody of root")
	f.c1 = strings.TrimSpace(mustGit(t, dir, "rev-parse", "HEAD"))

	mustGit(t, dir, "tag", "-a", "v1", "-m", "release v1", f.c1)
	mustGit(t, dir, "update-ref", "refs/namespaces/ephemeral/refs/heads/main", f.c1)

	// feature branch on top of c1.
	mustGit(t, dir, "checkout", "-q", "-b", "feature", f.c1)
	writeFile(t, dir, "feature.txt", "feature work\n", 0o644)
	mustGit(t, dir, "add", "feature.txt")
	mustGit(t, dir, "commit", "-q", "-m", "feature commit")
	f.c3 = strings.TrimSpace(mustGit(t, dir, "rev-parse", "HEAD"))

	// second commit on main: modify, rename, add, binary change.
	mustGit(t, dir, "checkout", "-q", "main")
	writeFile(t, dir, "README.md", "# Title\nline two\nline three\nline four\n", 0o644)
	mustGit(t, dir, "mv", "dir/data.txt", "dir/renamed.txt")
	writeFile(t, dir, "extra.txt", "extra\n", 0o644)
	writeFile(t, dir, "bin.dat", "\x00\x03\x04binary2\x00", 0o644)
	mustGit(t, dir, "add", "-A")
	mustGit(t, dir, "commit", "-q", "-m", "second commit")
	f.c2 = strings.TrimSpace(mustGit(t, dir, "rev-parse", "HEAD"))

	mustGit(t, dir, "tag", "v2-light", f.c2)
	return f
}

func TestResolveRef(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	cases := []struct {
		name      string
		spec      string
		ephemeral bool
		want      string
	}{
		{"branch", "main", false, f.c2},
		{"feature branch", "feature", false, f.c3},
		{"full ref", "refs/heads/feature", false, f.c3},
		{"annotated tag peels to commit", "v1", false, f.c1},
		{"lightweight tag", "v2-light", false, f.c2},
		{"full sha", f.c1, false, f.c1},
		{"uppercase full sha", strings.ToUpper(f.c1), false, f.c1},
		{"short sha", f.c1[:8], false, f.c1},
		{"ephemeral branch", "main", true, f.c1},
		{"ephemeral full ref", "refs/heads/main", true, f.c1},
		{"sha under ephemeral still resolves", f.c2, true, f.c2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := ResolveRef(ctx, f.dir, c.spec, c.ephemeral)
			if err != nil {
				t.Fatalf("ResolveRef(%q, eph=%v): %v", c.spec, c.ephemeral, err)
			}
			if got != c.want {
				t.Fatalf("ResolveRef(%q) = %s, want %s", c.spec, got, c.want)
			}
		})
	}

	// A NUL byte cannot reach git (exec refuses the argument): it names
	// no ref, as any other invalid name.
	for _, spec := range []string{"", "nope", "refs/heads/nope", "deadbeef", "HEAD~1", "main~1", "--verify", strings.ToUpper(f.c1[:8]), "main\x00", "refs/heads/ma\x00in", "\x00" + f.c1} {
		if sha, err := ResolveRef(ctx, f.dir, spec, false); !errors.Is(err, ErrNotFound) {
			t.Errorf("ResolveRef(%q) = (%q, %v), want ErrNotFound", spec, sha, err)
		}
	}
	// A persistent branch name must not resolve in the ephemeral namespace.
	if sha, err := ResolveRef(ctx, f.dir, "feature", true); !errors.Is(err, ErrNotFound) {
		t.Errorf("ephemeral ResolveRef(feature) = (%q, %v), want ErrNotFound", sha, err)
	}
}

// TestResolveRefGitFailureIsNotNotFound: when a git child fails without
// answering (here exec.ErrWaitDelay: git exited, but its output pipe
// stayed open past childWaitDelay, as on a long-stalled host), ResolveRef
// returns that failure. It must never report ErrNotFound for a ref that
// exists: a client would then believe the ref is gone (404).
func TestResolveRefGitFailureIsNotNotFound(t *testing.T) {
	f := newFixture(t)
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	for _, sub := range []string{"check-ref-format", "rev-parse"} {
		t.Run(sub, func(t *testing.T) {
			// A background child keeps git's stdout open after git exits.
			fakeGit(t, `if [ "$1" = `+sub+` ]; then (sleep 5) & fi
exec `+realGit+` "$@"`)
			sha, err := ResolveRef(context.Background(), f.dir, "main", false)
			if err == nil || errors.Is(err, ErrNotFound) || !errors.Is(err, exec.ErrWaitDelay) {
				t.Fatalf("ResolveRef(main) with a stalled %s = (%q, %v), want the exec.ErrWaitDelay failure, not ErrNotFound", sub, sha, err)
			}
		})
	}
}

func TestPeeledTags(t *testing.T) {
	f := newFixture(t)
	// Tags plain git can push but invariant 10 cannot list: a lightweight
	// tag on a blob, an annotated tag on a tree — both must be skipped —
	// and a nested annotated tag (tag -> tag -> commit), which must peel
	// all the way to the commit.
	mustGit(t, f.dir, "tag", "a-blob-tag", f.c1+":README.md")
	mustGit(t, f.dir, "tag", "-a", "-m", "tree tag", "b-tree-tag", f.c1+"^{tree}")
	mustGit(t, f.dir, "tag", "-a", "-m", "nested", "c-nested", "v1")
	// A tag whose name collides with a branch: %(refname:short) would
	// disambiguate it as "tags/feature", but Tag.Name must be the plain
	// short name for exact-name matching (GET /repos/tag).
	mustGit(t, f.dir, "tag", "feature", f.c1)

	tags, err := PeeledTags(context.Background(), f.dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(tags) != 4 {
		t.Fatalf("got %d tags, want 4 (non-commit tags skipped): %+v", len(tags), tags)
	}
	// Lexical order; nested + annotated peel to c1, lightweight points at c2.
	if tags[0].Name != "c-nested" || tags[0].SHA != f.c1 {
		t.Errorf("tags[0] = %+v, want c-nested -> %s (peeled through the tag chain)", tags[0], f.c1)
	}
	if tags[1].Name != "feature" || tags[1].SHA != f.c1 {
		t.Errorf("tags[1] = %+v, want feature -> %s (branch-name collision stays plain)", tags[1], f.c1)
	}
	if tags[2].Name != "v1" || tags[2].SHA != f.c1 {
		t.Errorf("tags[2] = %+v, want v1 -> %s (peeled)", tags[2], f.c1)
	}
	if tags[3].Name != "v2-light" || tags[3].SHA != f.c2 {
		t.Errorf("tags[3] = %+v, want v2-light -> %s", tags[3], f.c2)
	}
}

func TestMatchGlob(t *testing.T) {
	cases := []struct {
		pattern, path string
		want          bool
	}{
		{"*.md", "README.md", true},
		{"*.md", "docs/deep/file.md", true}, // basename match for slash-free patterns
		{"*.md", "file.go", false},
		{"docs/*.md", "docs/a.md", true},
		{"docs/*.md", "docs/deep/a.md", false}, // * does not cross /
		{"**/*.md", "docs/deep/a.md", true},
		{"**/*.md", "a.md", true}, // ** matches zero segments
		{"docs/**", "docs/deep/a.md", true},
		{"docs/**", "src/a.md", false},
		{"docs/**/*.txt", "docs/a/b/c.txt", true},
		{"", "anything", false},
		{"[", "x", false}, // malformed pattern matches nothing
	}
	for _, c := range cases {
		if got := MatchGlob(c.pattern, c.path); got != c.want {
			t.Errorf("MatchGlob(%q, %q) = %v, want %v", c.pattern, c.path, got, c.want)
		}
	}
}
