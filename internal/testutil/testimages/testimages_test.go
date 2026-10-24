package testimages

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
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
	out, err := exec.Command("git", "-C", root, "ls-files", "-z").Output()
	if err != nil {
		t.Fatalf("git ls-files: %v", err)
	}
	// Only files that hold a digest reference in any case reach the
	// regexps, which are slow on the whole tree.
	type file struct{ name, text string }
	var files []file
	listed := false
	for _, name := range strings.Split(strings.TrimSuffix(string(out), "\x00"), "\x00") {
		listed = listed || name == "go.mod"
		if strings.HasPrefix(name, "vendor/") || strings.HasPrefix(name, "docs/plans/") || strings.HasPrefix(name, "docs/archive/") {
			continue
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
	if !listed {
		t.Fatalf("git ls-files in %s did not list go.mod", root)
	}
	for _, tc := range []struct {
		pin      string
		required []string
	}{
		{Postgres, []string{"docker-compose.yml"}},
		{SeaweedFS, []string{"docker-compose.yml", "README.md", "CONTRIBUTING.md"}},
	} {
		if !pinShape.MatchString(tc.pin) {
			t.Fatalf("pin %q is not repository:tag@sha256:<64 hex>", tc.pin)
		}
		repo, _, _ := strings.Cut(tc.pin, ":")
		// Group 1 is an optional registry host (it has a dot or a port, or
		// is localhost), plus "library/" for Docker Hub official images.
		// The leading class stops "postgres" from matching inside another
		// name. The digest takes any hex length and (?i) takes any letter
		// case, so a cut-off or wrong-case copy is a mismatch instead of a
		// miss.
		prefix := `(?:(?:localhost|[\w-]+(?:\.[\w-]+)+)(?::\d+)?/)?`
		if !strings.Contains(repo, "/") {
			prefix += `(?:library/)?`
		}
		ref := regexp.MustCompile(`(?i)(?:^|[^\w./-])(` + prefix + `)(` + regexp.QuoteMeta(repo) + `:[\w.-]+@sha256:[0-9a-f]+)`)
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
