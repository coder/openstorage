package testimages

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// pinShape is the only form a pin may take: repository, tag and a full
// sha256 index digest.
var pinShape = regexp.MustCompile(`^[a-z0-9./-]+:[\w.-]+@sha256:[0-9a-f]{64}$`)

// TestDigestCopiesMatch keeps the hand-written copies of the pins in sync
// with the constants, which are the source of truth. Every digest reference
// to a pinned repository in the listed files must equal the constant
// (repository, tag and digest), and each listed file must hold at least one,
// so replacing a pinned reference with a tag-only one also fails.
func TestDigestCopiesMatch(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("repository root not found at %s: %v", root, err)
	}
	for _, tc := range []struct {
		pin   string
		files []string
	}{
		{Postgres, []string{"docker-compose.yml"}},
		{SeaweedFS, []string{"docker-compose.yml", "README.md", "CONTRIBUTING.md"}},
	} {
		if !pinShape.MatchString(tc.pin) {
			t.Fatalf("pin %q is not repository:tag@sha256:<64 hex>", tc.pin)
		}
		repo, _, _ := strings.Cut(tc.pin, ":")
		// The leading class stops "postgres" from matching inside another
		// name. The digest takes any hex length, so a cut-off copy is a
		// mismatch instead of a miss.
		ref := regexp.MustCompile(`(?:^|[^\w./-])(` + regexp.QuoteMeta(repo) + `:[\w.-]+@sha256:[0-9a-f]+)`)
		for _, name := range tc.files {
			b, err := os.ReadFile(filepath.Join(root, name))
			if err != nil {
				t.Fatalf("read %s: %v", name, err)
			}
			text := string(b)
			matches := ref.FindAllStringSubmatchIndex(text, -1)
			if len(matches) == 0 {
				t.Errorf("%s: no digest-pinned %s reference; want %s", name, repo, tc.pin)
			}
			for _, m := range matches {
				if got := text[m[2]:m[3]]; got != tc.pin {
					line := strings.Count(text[:m[2]], "\n") + 1
					t.Errorf("%s:%d: %s\n\twant %s (internal/testutil/testimages)", name, line, got, tc.pin)
				}
			}
		}
	}
}
