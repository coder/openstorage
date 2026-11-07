package api

import (
	"context"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/auth"
)

// seedSymlinkRepo seeds a repo whose main branch holds a file and a symlink
// with the given target, and returns a git:read token for it.
func seedSymlinkRepo(t *testing.T, e *testEnv, org, target string) string {
	t.Helper()
	url := "https://example.com/" + org + "/symlink-" + randomHex(t, 4)
	repo, err := e.store.CreateRepo(context.Background(), org, url, "main")
	if err != nil {
		t.Fatalf("create repo: %v", err)
	}
	dir := t.TempDir()
	coreGit(t, dir, "init", "-q", "-b", "main", ".")
	stream := fmt.Sprintf("commit refs/heads/main\ncommitter S <s@example.com> 1700000000 +0000\ndata 2\nc\n"+
		"M 100644 inline a.txt\ndata 2\na\nM 120000 inline link\ndata %d\n%s\n", len(target), target)
	cmd := exec.Command("git", "fast-import", "--quiet")
	cmd.Dir = dir
	cmd.Stdin = strings.NewReader(stream)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("fast-import: %v\n%s", err, out)
	}
	coreSeedStores(t, e, repo.RepoID, dir, map[string]string{"refs/heads/main": coreRevParse(t, dir, "main")})
	return e.mint(t, tokenSpec{iss: org, repo: url, scopes: []string{auth.ScopeGitRead}})
}

// The archive carries a symlink target of 4095 bytes, the longest that Linux
// accepts, and answers 422 for a longer one before any response byte (the
// sizes are numbers, not gitread.MaxSymlinkTarget).
func TestArchiveCapsASymlinkTarget(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)

		t.Run("4095 bytes", func(t *testing.T) {
			target := strings.Repeat("t", 4095)
			tok := seedSymlinkRepo(t, e, org, target)
			resp, body := e.doReq(t, http.MethodPost, "/api/v1/repos/archive", tok, []byte(`{}`), "")
			coreRequireStatus(t, resp, body, http.StatusOK)
			if link := rqReadArchive(t, body)["link"]; link.linkname != target {
				t.Fatalf("link entry has a target of %d bytes, want %d", len(link.linkname), len(target))
			}
		})

		t.Run("4096 bytes", func(t *testing.T) {
			tok := seedSymlinkRepo(t, e, org, strings.Repeat("t", 4096))
			resp, body := e.doReq(t, http.MethodPost, "/api/v1/repos/archive", tok, []byte(`{}`), "")
			requireLimitStatus(t, resp, body, http.StatusUnprocessableEntity, false, "symlink target exceeds the API read size limit of 4095 bytes")
			if ct := resp.Header.Get("Content-Type"); strings.Contains(ct, "gzip") {
				t.Fatalf("Content-Type = %q: the error came after the archive's headers", ct)
			}
		})

		t.Run("excluded by a glob", func(t *testing.T) {
			tok := seedSymlinkRepo(t, e, org, strings.Repeat("t", 4096))
			resp, body := e.doReq(t, http.MethodPost, "/api/v1/repos/archive", tok, []byte(`{"exclude_globs": ["link"]}`), "")
			coreRequireStatus(t, resp, body, http.StatusOK)
			if _, ok := rqReadArchive(t, body)["link"]; ok {
				t.Fatal("the excluded symlink is in the archive")
			}
		})
	})
}
