package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/gitread"
)

// seedEntry is one file of a seeded repo: its git mode, path and content.
type seedEntry struct{ mode, path, content string }

// seedSymlinkRepo seeds a repo whose main branch holds a file and a symlink
// with the given target, and returns a git:read token for it.
func seedSymlinkRepo(t *testing.T, e *testEnv, org, target string) string {
	t.Helper()
	return seedEntriesRepo(t, e, org, seedEntry{"100644", "a.txt", "a"}, seedEntry{"120000", "link", target})
}

// seedEntriesRepo seeds a repo whose main branch holds the entries, and
// returns a git:read token for it.
func seedEntriesRepo(t *testing.T, e *testEnv, org string, entries ...seedEntry) string {
	t.Helper()
	url := "https://example.com/" + org + "/symlink-" + randomHex(t, 4)
	repo, err := e.store.CreateRepo(context.Background(), org, url, "main")
	if err != nil {
		t.Fatalf("create repo: %v", err)
	}
	dir := t.TempDir()
	coreGit(t, dir, "init", "-q", "-b", "main", ".")
	var stream strings.Builder
	stream.WriteString("commit refs/heads/main\ncommitter S <s@example.com> 1700000000 +0000\ndata 2\nc\n")
	for _, f := range entries {
		fmt.Fprintf(&stream, "M %s inline %s\ndata %d\n%s\n", f.mode, f.path, len(f.content), f.content)
	}
	cmd := exec.Command("git", "fast-import", "--quiet")
	cmd.Dir = dir
	cmd.Stdin = strings.NewReader(stream.String())
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
			requireLimitStatus(t, resp, body, http.StatusUnprocessableEntity, false, "")
			if msg := problemError(t, body); !strings.Contains(msg, `the target of symlink "link" exceeds the API read size limit of 4095 bytes`) || !strings.Contains(msg, "exclude_globs") {
				t.Fatalf("error %q does not name the symlink, the limit and exclude_globs", msg)
			}
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

// The error names the entry, so that a caller can skip it with exclude_globs.
// A name is quoted, so bytes that are not UTF-8 stay in the JSON text, and a
// long one is cut.
func TestArchiveLimitErrorNamesAnOddPath(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		name := "d\xffir/" + strings.Repeat("n", 300) + "TAIL"
		tok := seedEntriesRepo(t, e, org, seedEntry{"100644", "a.txt", "a"}, seedEntry{"120000", name, strings.Repeat("t", 4096)})
		resp, body := e.doReq(t, http.MethodPost, "/api/v1/repos/archive", tok, []byte(`{}`), "")
		requireLimitStatus(t, resp, body, http.StatusUnprocessableEntity, false, "")
		msg := problemError(t, body)
		if !strings.Contains(msg, `symlink "d\xffir/nnn`) || !strings.Contains(msg, fmt.Sprintf("a path of %d bytes", len(name))) || strings.Contains(msg, "TAIL") {
			t.Fatalf("error %q does not hold the start of the path with \\xff, its length and not its end", msg)
		}
	})
}

// A file over the size limit is named in the same way, for file and archive,
// and exclude_globs leaves it out of the archive.
func TestArchiveNamesAFileOverTheSizeLimit(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		tok := seedEntriesRepo(t, e, org, seedEntry{"100644", "a.txt", "a"}, seedEntry{"100644", "dir/big.bin", "12345678"})
		withGitReadPolicy(t, testGitReadPolicy(2, time.Minute, gitread.DefaultOutputBytes,
			gitread.Limits{HeavyHeap: gitread.HeavyHeapBytes, LightHeap: gitread.LightHeapBytes, MaxBlob: 4}))
		want := `file "dir/big.bin" exceeds the API read size limit of 4 bytes`
		resp, body := e.doReq(t, http.MethodPost, "/api/v1/repos/archive", tok, []byte(`{}`), "")
		requireLimitStatus(t, resp, body, http.StatusUnprocessableEntity, false, "")
		if msg := problemError(t, body); !strings.Contains(msg, want) {
			t.Fatalf("archive error %q does not contain %q", msg, want)
		}
		resp, body = coreGet(t, e, "GET", "/api/v1/repos/file?path=dir/big.bin", tok, nil)
		requireLimitStatus(t, resp, body, http.StatusUnprocessableEntity, false, "")
		if msg := problemError(t, body); !strings.Contains(msg, want) {
			t.Fatalf("file error %q does not contain %q", msg, want)
		}
		resp, body = e.doReq(t, http.MethodPost, "/api/v1/repos/archive", tok, []byte(`{"exclude_globs": ["dir/big.bin"]}`), "")
		coreRequireStatus(t, resp, body, http.StatusOK)
		if got := rqReadArchive(t, body); len(got) != 1 || got["a.txt"].content != "a" {
			t.Fatalf("archive without the big file has %d entries, want only a.txt", len(got))
		}
	})
}

// problemError returns the error text of a problem body.
func problemError(t *testing.T, body []byte) string {
	t.Helper()
	var p struct {
		Error, Detail string
	}
	if err := json.Unmarshal(body, &p); err != nil || p.Error == "" || p.Detail != p.Error {
		t.Fatalf("body %s is not an error envelope: %v", body, err)
	}
	return p.Error
}

// A path of up to 256 bytes is named whole, and a longer one is cut at 256
// bytes, before a rune that the limit would split. The step back is at most
// 3 bytes, so a name that is not UTF-8 is cut at 253 bytes, not at none.
func TestNamedEntryCutsAtARuneBoundary(t *testing.T) {
	cut := func(kept string, total int) string {
		return fmt.Sprintf("x %q... (a path of %d bytes)", kept, total)
	}
	for _, c := range []struct{ name, path, want string }{
		{"a short path", "a/b", `x "a/b"`},
		{"256 bytes are named whole", strings.Repeat("a", 256), fmt.Sprintf("x %q", strings.Repeat("a", 256))},
		{"257 bytes are cut at 256", strings.Repeat("a", 257), cut(strings.Repeat("a", 256), 257)},
		{"a cut inside a 3-byte rune", strings.Repeat("\u20ac", 100), cut(strings.Repeat("\u20ac", 85), 300)},
		{"a cut inside a 4-byte rune", "a" + strings.Repeat("\U0001d11e", 100), cut("a"+strings.Repeat("\U0001d11e", 63), 401)},
		{"bytes that are not UTF-8", strings.Repeat("\x80", 300), cut(strings.Repeat("\x80", 253), 300)},
	} {
		if got := namedEntry("x", gitread.CheckSymlinkTarget(c.path, 5000)); got != c.want {
			t.Errorf("%s: namedEntry = %s, want %s", c.name, got, c.want)
		}
	}
	if got := namedEntry("x", errors.New("other")); got != "x" {
		t.Fatalf("namedEntry of an error with no path = %s", got)
	}
}
