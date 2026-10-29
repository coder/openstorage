package testimages

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// pinShape is the only form a pin may take: repository, tag and a full
// sha256 index digest.
var pinShape = regexp.MustCompile(`^[a-z0-9./-]+:[\w.-]+@sha256:[0-9a-f]{64}$`)

// pinCopies names a pin and the files that must hold a copy of it.
type pinCopies struct {
	pin      string
	required []string
}

var pins = []pinCopies{
	{Postgres, []string{"docker-compose.yml"}},
	{SeaweedFS, []string{"docker-compose.yml", "README.md", "CONTRIBUTING.md"}},
}

// TestDigestCopiesMatch keeps the hand-written copies of the pins in sync
// with the constants, which are the source of truth. checkTree and
// badCopies say which copies it checks.
func TestDigestCopiesMatch(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("repository root not found at %s: %v", root, err)
	}
	problems, err := checkTree(root, pins)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range problems {
		t.Error(p)
	}
}

// checkTree returns one problem for each bad copy of a pin under root and
// for each required file without a digest-pinned copy. It scans every
// tracked text file outside vendor/, docs/plans/ and docs/archive/
// (history may keep old digests). Each required file must hold at least
// one copy, so replacing a pinned reference with a tag-only one also fails.
func checkTree(root string, pins []pinCopies) ([]string, error) {
	// A source tree without Git metadata (an archive) has no file list, so
	// the test then reads only the files that must hold a copy.
	var names []string
	for _, pc := range pins {
		if !pinShape.MatchString(pc.pin) {
			return nil, fmt.Errorf("pin %q is not repository:tag@sha256:<64 hex>", pc.pin)
		}
		for _, name := range pc.required {
			if !slices.Contains(names, name) {
				names = append(names, name)
			}
		}
	}
	if _, err := os.Stat(filepath.Join(root, ".git")); err == nil {
		out, err := exec.Command("git", "-C", root, "ls-files", "-z").Output()
		if err != nil {
			return nil, fmt.Errorf("git ls-files: %w", err)
		}
		names = strings.Split(strings.TrimSuffix(string(out), "\x00"), "\x00")
		if !slices.Contains(names, "go.mod") {
			return nil, fmt.Errorf("git ls-files in %s did not list go.mod", root)
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("stat .git: %w", err)
	}
	// Only files that hold a digest reference in any case reach the
	// regexps, which are slow on the whole tree.
	type file struct{ name, text string }
	var files []file
	opened := map[string]bool{}
	for _, name := range names {
		if strings.HasPrefix(name, "vendor/") || strings.HasPrefix(name, "docs/plans/") || strings.HasPrefix(name, "docs/archive/") {
			continue
		}
		// Go reuses a cached pass until a file or directory that the test
		// opened changes. Opening each directory on the way to a scanned
		// file makes a newly added file rerun the test.
		for dir := path.Dir(name); !opened[dir]; dir = path.Dir(dir) {
			opened[dir] = true
			if d, err := os.Open(filepath.Join(root, filepath.FromSlash(dir))); err == nil {
				_ = d.Close()
			}
			if dir == "." {
				break
			}
		}
		b, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(name)))
		if errors.Is(err, fs.ErrNotExist) {
			continue // deleted in the work tree but not yet staged
		}
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", name, err)
		}
		if bytes.IndexByte(b, 0) < 0 && bytes.Contains(bytes.ToLower(b), []byte("@sha256:")) {
			files = append(files, file{name, string(b)})
		}
	}
	var problems []string
	for _, pc := range pins {
		repo, _, _ := strings.Cut(pc.pin, ":")
		pinned := map[string]bool{}
		for _, f := range files {
			bad, ok := badCopies(pc.pin, f.text)
			pinned[f.name] = ok
			for _, c := range bad {
				problems = append(problems, fmt.Sprintf("%s:%d: %s\n\twant %s (internal/testutil/testimages)", f.name, c.line, c.text, pc.pin))
			}
		}
		for _, name := range pc.required {
			if !pinned[name] {
				problems = append(problems, fmt.Sprintf("%s: no digest-pinned %s reference; want %s", name, repo, pc.pin))
			}
		}
	}
	return problems, nil
}

// badCopy is a copy of a pin that differs from it, at a 1-based line.
type badCopy struct {
	line int
	text string
}

// badCopies returns every copy of pin in text that differs from pin, and
// whether text holds a copy with a digest. pin must match pinShape.
//
// A copy is a digest reference to the pinned repository, with or without a
// registry prefix and in any letter case. It must equal pin (repository,
// tag and digest). A copy with a prose placeholder instead of the digest
// (`...`, `…` or `<digest>`) must have the pin's repository and tag, but
// it does not count as a digest-pinned copy. A tag-less placeholder has
// nothing to compare and is skipped. Other text after "@sha256:", such as
// a closing quote in code that appends the digest, is not a copy.
func badCopies(pin, text string) (bad []badCopy, pinned bool) {
	if !pinShape.MatchString(pin) {
		panic(fmt.Sprintf("badCopies: pin %q is not repository:tag@sha256:<64 hex>", pin))
	}
	repo, _, _ := strings.Cut(pin, ":")
	// Group 1 is an optional registry host plus "library/" for Docker
	// Hub official images. As in the reference grammar, a first path
	// component is a host when it has a dot, a port or an uppercase
	// letter, is localhost, or is a bracketed IPv6 address.
	// The leading class stops "postgres" from matching inside another
	// name or in another image's tag. The tag is optional, the digest
	// takes any hex length and (?i) takes any letter case, so a
	// tag-less, cut-off or wrong-case copy is a mismatch, not a miss.
	// Tag-less copies stay in scope (#565): Docker pulls them by digest,
	// so a stale one is real drift.
	// Group 2 is the repository and tag, group 3 the digest. A
	// placeholder leaves group 3 unset.
	prefix := `(?:(?:localhost|\[[0-9a-f:]+\](?::\d+)?|[\w-]+(?:\.[\w-]+)+(?::\d+)?|[\w-]+:\d+|(?-i:[\w-]*[A-Z][\w-]*))/)?`
	if !strings.Contains(repo, "/") {
		prefix += `(?:library/)?`
	}
	ref := regexp.MustCompile(`(?i)(?:^|[^\w./:-])(` + prefix + `)(` + regexp.QuoteMeta(repo) + `(?::[\w.-]+)?)@sha256:(?:([0-9a-f]+)|\.\.\.|…|<[^<>\n]*>)`)
	nameTag, _, _ := strings.Cut(pin, "@")
	for _, m := range ref.FindAllStringSubmatchIndex(text, -1) {
		got, want := text[m[4]:m[1]], pin
		if m[6] < 0 {
			got, want = text[m[4]:m[5]], nameTag
			if !strings.Contains(got, ":") {
				continue
			}
		} else {
			pinned = true
		}
		if got != want {
			bad = append(bad, badCopy{strings.Count(text[:m[2]], "\n") + 1, text[m[2]:m[1]]})
		}
	}
	return bad, pinned
}

// TestBadCopies runs planted copies through the matcher. The bad rows
// include the blind spots of the #520 matcher (#521) and of the #564
// matcher (#630). The good rows include the false positives that review
// found.
// The pins and repositories are made up, so a pin bump does not touch
// this table and TestDigestCopiesMatch does not read it as a copy.
func TestBadCopies(t *testing.T) {
	db := "dbimage:17-alpine@sha256:" + strings.Repeat("a1", 32)
	dbDigest := strings.Repeat("a1", 32)
	other := "@sha256:" + strings.Repeat("b2", 32)
	wrong := "dbimage:17-alpine" + other
	st := "acme/store:4.51@sha256:" + strings.Repeat("c3", 32)
	for _, tc := range []struct {
		name   string
		pin    string
		text   string
		bad    []badCopy
		pinned bool
	}{
		// Good copies and other images.
		{"exact copy", db, "image: " + db, nil, true},
		{"right copy behind docker.io/library/", db, "image: docker.io/library/" + db, nil, true},
		{"right copy behind an IPv6 host", db, "image: [2001:db8::1]:5000/" + db, nil, true},
		{"other image bitnami/dbimage", db, "image: bitnami/dbimage:17" + other, nil, false},
		{"repository name in another image's tag", db, "image: example:dbimage" + other, nil, false},
		{"other image under a mirror path", st, "image: mirror/acme/store:4.48@sha256:...", nil, false},
		{"tag-only reference", db, "image: dbimage:17-alpine", nil, false},
		{"placeholder with the pinned tag", db, "`dbimage:17-alpine@sha256:...`", nil, false},
		{"tag-less placeholder", db, "`dbimage@sha256:<digest>`", nil, false},
		{"code that appends the digest", st, `ref := "acme/store:4.48@sha256:" + digest`, nil, false},
		{"shell variable digest", st, "acme/store:4.48@sha256:${DIGEST}", nil, false},

		// Bad copies that the #520 matcher caught.
		{"wrong digest", db, "image: " + wrong, []badCopy{{1, wrong}}, true},
		{"wrong tag", db, "image: dbimage:16-alpine@sha256:" + dbDigest, []badCopy{{1, "dbimage:16-alpine@sha256:" + dbDigest}}, true},
		{"cut-off digest", db, "image: " + db[:len(db)-8], []badCopy{{1, db[:len(db)-8]}}, true},
		{"bad copy on a later line", db, "a: " + db + "\n\nb: " + wrong, []badCopy{{3, wrong}}, true},

		// Bad copies that the #520 matcher missed (#521, #564 review).
		{"wrong digest behind docker.io/", db, "image: docker.io/" + wrong, []badCopy{{1, "docker.io/" + wrong}}, true},
		{"wrong digest behind docker.io/library/", db, "image: docker.io/library/" + wrong, []badCopy{{1, "docker.io/library/" + wrong}}, true},
		{"wrong digest behind a registry port", db, "image: registry:5000/" + wrong, []badCopy{{1, "registry:5000/" + wrong}}, true},
		{"wrong digest behind an IPv6 host", db, "image: [2001:db8::1]:5000/" + wrong, []badCopy{{1, "[2001:db8::1]:5000/" + wrong}}, true},
		{"wrong digest behind an uppercase host", db, "image: Registry/" + wrong, []badCopy{{1, "Registry/" + wrong}}, true},
		{"wrong digest behind localhost", db, "image: localhost/" + wrong, []badCopy{{1, "localhost/" + wrong}}, true},
		{"uppercase digest", db, "image: dbimage:17-alpine@sha256:" + strings.ToUpper(dbDigest), []badCopy{{1, "dbimage:17-alpine@sha256:" + strings.ToUpper(dbDigest)}}, true},
		{"tag-less copy with a wrong digest", db, "image: dbimage" + other, []badCopy{{1, "dbimage" + other}}, true},
		{"tag-less copy with the right digest", db, "image: dbimage@sha256:" + dbDigest, []badCopy{{1, "dbimage@sha256:" + dbDigest}}, true},

		// Stale tags on placeholder digests, which the #564 matcher missed
		// (#630).
		{"stale tag on a ... placeholder", st, "pinned `acme/store:4.48@sha256:...` reference", []badCopy{{1, "acme/store:4.48@sha256:..."}}, false},
		{"stale tag on a … placeholder", st, "acme/store:4.48@sha256:…", []badCopy{{1, "acme/store:4.48@sha256:…"}}, false},
		{"stale tag on a <digest> placeholder behind docker.io/", st, "docker.io/acme/store:4.48@sha256:<digest>", []badCopy{{1, "docker.io/acme/store:4.48@sha256:<digest>"}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad, pinned := badCopies(tc.pin, tc.text)
			if !slices.Equal(bad, tc.bad) || pinned != tc.pinned {
				t.Errorf("badCopies(%q) = %+v, %v; want %+v, %v", tc.text, bad, pinned, tc.bad, tc.pinned)
			}
		})
	}
}

// TestCheckTreeFiles plants copies in a scratch tree to check which files
// checkTree reads: every tracked file outside vendor/, docs/plans/ and
// docs/archive/ in a Git tree, and only the required files without Git.
func TestCheckTreeFiles(t *testing.T) {
	// Isolate git from the caller's repository and config.
	for _, kv := range os.Environ() {
		if k, _, _ := strings.Cut(kv, "="); strings.HasPrefix(k, "GIT_") {
			t.Setenv(k, "")
			if err := os.Unsetenv(k); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")

	db := "dbimage:17-alpine@sha256:" + strings.Repeat("a1", 32)
	wrong := "dbimage:17-alpine@sha256:" + strings.Repeat("b2", 32)
	st := "acme/store:4.51@sha256:" + strings.Repeat("c3", 32)
	treePins := []pinCopies{
		{db, []string{"docker-compose.yml"}},
		{st, []string{"docker-compose.yml", "README.md"}},
	}
	write := func(t *testing.T, root string, files map[string]string) {
		t.Helper()
		for name, text := range files {
			p := filepath.Join(root, filepath.FromSlash(name))
			if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(p, []byte(text), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}

	t.Run("git tree", func(t *testing.T) {
		root := t.TempDir()
		write(t, root, map[string]string{
			"go.mod":                   "module example\n",
			"docker-compose.yml":       "a: " + db + "\nb: " + st + "\n",
			"README.md":                st,
			"docs/how-to/x.md":         "\nimage: " + wrong,
			"vendor/example/README.md": wrong,
			"docs/plans/old.md":        wrong,
			"docs/archive/old.md":      wrong,
		})
		for _, args := range [][]string{{"init", "-q"}, {"add", "--", "."}} {
			if out, err := exec.Command("git", append([]string{"-C", root}, args...)...).CombinedOutput(); err != nil {
				t.Fatalf("git %v: %v\n%s", args, err, out)
			}
		}
		write(t, root, map[string]string{"untracked.md": wrong})
		problems, err := checkTree(root, treePins)
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"docs/how-to/x.md:2: " + wrong + "\n\twant " + db + " (internal/testutil/testimages)"}
		if !slices.Equal(problems, want) {
			t.Errorf("checkTree = %q; want %q", problems, want)
		}
	})

	t.Run("tree without Git", func(t *testing.T) {
		root := t.TempDir()
		write(t, root, map[string]string{
			"docker-compose.yml": "a: " + db + "\nb: " + st + "\n",
			"README.md":          "acme/store:4.51@sha256:...",
			"docs/how-to/x.md":   wrong,
		})
		problems, err := checkTree(root, treePins)
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"README.md: no digest-pinned acme/store reference; want " + st}
		if !slices.Equal(problems, want) {
			t.Errorf("checkTree = %q; want %q", problems, want)
		}
	})
}
