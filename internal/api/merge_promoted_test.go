package api

import (
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/auth"
)

// TestPromotedCommitsPastTheArgumentLimit counts the commits that a merge
// promotes in a repository with 140,000 branches. As arguments, their tips
// would pass every system's total limit (6 MiB at most on Linux, 1 MiB on
// macOS), so git gets them on stdin.
func TestPromotedCommitsPastTheArgumentLimit(t *testing.T) {
	dir := t.TempDir()
	coreGit(t, dir, "init", "-q", "-b", "main", ".")
	const n = 140_000
	var stream strings.Builder
	for i := 1; i <= n; i++ {
		stream.WriteString("commit refs/heads/main\nmark :" + strconv.Itoa(i) + "\ncommitter p <p@test.invalid> " + strconv.Itoa(1_700_000_000+i) + " +0000\ndata 2\nc\n")
		if i > 1 {
			stream.WriteString("from :" + strconv.Itoa(i-1) + "\n")
		}
		stream.WriteString("\n")
	}
	stream.WriteString("done\n")
	fi := exec.Command("git", "fast-import", "--quiet", "--done")
	fi.Dir, fi.Stdin = dir, strings.NewReader(stream.String())
	if out, err := fi.CombinedOutput(); err != nil {
		t.Fatalf("fast-import: %v\n%s", err, out)
	}
	shas := strings.Fields(coreGit(t, dir, "rev-list", "--reverse", "main"))
	if len(shas) != n {
		t.Fatalf("%d commits, want %d", len(shas), n)
	}
	tips := map[string]string{}
	for i, sha := range shas[:n-1] {
		tips["refs/heads/b"+strconv.Itoa(i)] = sha
	}
	tips[auth.EphemeralRefName("scratch")] = shas[n-1] // an ephemeral tip excludes nothing
	sc, err := wcNewScratch(filepath.Join(dir, ".git"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sc.close)
	if got, err := wmPromotedCommits(t.Context(), sc, tips, shas[n-1], false, true); err != nil || got != 1 {
		t.Fatalf("promoted = %d, %v; want 1", got, err)
	}
}
