package testimages

import (
	"bytes"
	"errors"
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

// TestDigestCopiesMatch keeps the hand-written copies of the pins in sync
// with the constants, which are the source of truth. It scans every tracked
// text file outside vendor/, docs/plans/ and docs/archive/ (history may keep
// old digests). Every digest reference to a pinned repository, with or
// without a registry prefix and in any letter case, must equal the constant
// (repository, tag and digest). Each required file must hold at least one,
// so replacing a pinned reference with a tag-only one also fails.
func TestDigestCopiesMatch(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("repository root not found at %s: %v", root, err)
	}
	pins := []struct {
		pin      string
		required []string
	}{
		{Postgres, []string{"docker-compose.yml"}},
		{SeaweedFS, []string{"docker-compose.yml", "README.md", "CONTRIBUTING.md"}},
	}
	// A source tree without Git metadata (an archive) has no file list, so
	// the test then reads only the files that must hold a copy.
	var names []string
	for _, tc := range pins {
		for _, name := range tc.required {
			if !slices.Contains(names, name) {
				names = append(names, name)
			}
		}
	}
	if _, err := os.Stat(filepath.Join(root, ".git")); err == nil {
		out, err := exec.Command("git", "-C", root, "ls-files", "-z").Output()
		if err != nil {
			t.Fatalf("git ls-files: %v", err)
		}
		names = strings.Split(strings.TrimSuffix(string(out), "\x00"), "\x00")
		if !slices.Contains(names, "go.mod") {
			t.Fatalf("git ls-files in %s did not list go.mod", root)
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("stat .git: %v", err)
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
			t.Fatalf("read %s: %v", name, err)
		}
		if bytes.IndexByte(b, 0) < 0 && bytes.Contains(bytes.ToLower(b), []byte("@sha256:")) {
			files = append(files, file{name, string(b)})
		}
	}
	for _, tc := range pins {
		if !pinShape.MatchString(tc.pin) {
			t.Fatalf("pin %q is not repository:tag@sha256:<64 hex>", tc.pin)
		}
		repo, _, _ := strings.Cut(tc.pin, ":")
		// Group 1 is an optional registry host (it has a dot or a port, or
		// is localhost or a bracketed IPv6 address, as in the reference
		// grammar), plus "library/" for Docker Hub official images.
		// The leading class stops "postgres" from matching inside another
		// name. The tag is optional, the digest takes any hex length and
		// (?i) takes any letter case, so a tag-less, cut-off or wrong-case
		// copy is a mismatch instead of a miss.
		prefix := `(?:(?:(?:localhost|\[[0-9a-f:]+\]|[\w-]+(?:\.[\w-]+)+)(?::\d+)?|[\w-]+:\d+)/)?`
		if !strings.Contains(repo, "/") {
			prefix += `(?:library/)?`
		}
		ref := regexp.MustCompile(`(?i)(?:^|[^\w./-])(` + prefix + `)(` + regexp.QuoteMeta(repo) + `(?::[\w.-]+)?@sha256:[0-9a-f]+)`)
		found := map[string]bool{}
		for _, f := range files {
			name, text := f.name, f.text
			for _, m := range ref.FindAllStringSubmatchIndex(text, -1) {
				found[name] = true
				if got := text[m[4]:m[5]]; got != tc.pin {
					line := strings.Count(text[:m[2]], "\n") + 1
					t.Errorf("%s:%d: %s\n\twant %s (internal/testutil/testimages)", name, line, text[m[2]:m[5]], tc.pin)
				}
			}
		}
		for _, name := range tc.required {
			if !found[name] {
				t.Errorf("%s: no digest-pinned %s reference; want %s", name, repo, tc.pin)
			}
		}
	}
}
