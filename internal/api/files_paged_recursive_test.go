package api

import (
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/gitread"
)

// TestFilesPagedRecursiveListingPastTheOutputLimit pins #612: a recursive
// files call with a limit or a cursor lists only up to its page, so a tree
// whose whole listing passes the git output allowance still pages, with
// the same pages and cursors as before. A call without a limit keeps the
// allowance and its 422.
func TestFilesPagedRecursiveListingPastTheOutputLimit(t *testing.T) {
	const n = 1002
	var in strings.Builder
	in.WriteString("commit refs/heads/main\nmark :1\ncommitter D <d@example.com> 1700000000 +0000\ndata 4\nroot\n")
	for i := range n {
		fmt.Fprintf(&in, "M 100644 inline d%02d/f%04d.txt\ndata 2\nx\n", i%10, i)
	}
	in.WriteString("\n")

	runBackends(t, func(t *testing.T, e *testEnv) {
		token, _ := filesFastImportRepo(t, e, in.String())
		whole := getFilesListing(t, e, "/api/v1/repos/files", token).paths
		if len(whole) != n {
			t.Fatalf("whole listing: %d entries, want %d", len(whole), n)
		}

		// 16 KiB holds a 100-entry page (about 80 bytes per ls-tree
		// record), not the 1002-entry tree.
		withGitReadPolicy(t, testGitReadPolicy(2, time.Minute, 16<<10, gitread.DefaultLimits()))
		resp, body := coreGet(t, e, "GET", "/api/v1/repos/files", token, nil)
		coreRequireStatus(t, resp, body, http.StatusUnprocessableEntity)

		var walked []string
		cursor := ""
		for page := 0; ; page++ {
			if page > n/100+1 {
				t.Fatal("pagination never exhausted")
			}
			q := "/api/v1/repos/files?limit=100"
			if cursor != "" {
				q += "&cursor=" + url.QueryEscape(cursor)
			}
			l := getFilesListing(t, e, q, token)
			want := whole[min(len(walked), n):min(len(walked)+100, n)]
			wantMore := len(walked)+100 < n
			if !slices.Equal(l.paths, want) || l.hasMore != wantMore {
				t.Fatalf("page %d: %d entries from %.40v, has_more %v; want %d from %.40v, has_more %v",
					page, len(l.paths), l.paths, l.hasMore, len(want), want, wantMore)
			}
			walked = append(walked, l.paths...)
			if !l.hasMore {
				if c, ok := l.raw["next_cursor"]; ok {
					t.Fatalf("last page carries next_cursor %s", c)
				}
				break
			}
			if wantCursor := coreCursorEncode(cursorKindFiles, want[len(want)-1]); l.cursor != wantCursor {
				t.Fatalf("page %d next_cursor = %q, want %q (format unchanged)", page, l.cursor, wantCursor)
			}
			cursor = l.cursor
		}

		// A deep cursor near the end still answers.
		deep := coreCursorEncode(cursorKindFiles, whole[n-3])
		l := getFilesListing(t, e, "/api/v1/repos/files?limit=50&cursor="+url.QueryEscape(deep), token)
		if !slices.Equal(l.paths, whole[n-2:]) || l.hasMore {
			t.Fatalf("deep cursor: %v, has_more %v; want the last 2 entries", l.paths, l.hasMore)
		}

		// path keeps its meanings: a folder, a file, nothing.
		l = getFilesListing(t, e, "/api/v1/repos/files?path=d03&limit=5", token)
		var d03 []string
		for _, p := range whole {
			if strings.HasPrefix(p, "d03/") {
				d03 = append(d03, p)
			}
		}
		if !slices.Equal(l.paths, d03[:5]) || !l.hasMore {
			t.Fatalf("path=d03: %v, has_more %v; want %v", l.paths, l.hasMore, d03[:5])
		}
		l = getFilesListing(t, e, "/api/v1/repos/files?path="+url.QueryEscape(d03[0])+"&limit=5", token)
		if !slices.Equal(l.paths, d03[:1]) || l.hasMore {
			t.Fatalf("path=<file>: %v, has_more %v; want the file", l.paths, l.hasMore)
		}
		resp, body = coreGet(t, e, "GET", "/api/v1/repos/files?path=nope&limit=5", token, nil)
		coreRequireStatus(t, resp, body, http.StatusNotFound)
	})
}

// A tree stored out of order (fsck's treeNotSorted; serve accepts such
// pushes) still pages in byte order, as before #612.
func TestFilesPagedRecursiveListingOfAnUnsortedTree(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		repoURL := "https://example.com/" + org + "/unsorted-" + randomHex(t, 4)
		repo, err := e.store.CreateRepo(t.Context(), org, repoURL, "main")
		if err != nil {
			t.Fatal(err)
		}
		dir := t.TempDir()
		coreGit(t, dir, "init", "-q", "-b", "main", ".")
		blob := gitStdin(t, dir, "x", "hash-object", "-w", "--stdin")
		raw, err := hex.DecodeString(blob)
		if err != nil {
			t.Fatal(err)
		}
		tree := gitStdin(t, dir, "100644 c\x00"+string(raw)+"100644 b\x00"+string(raw)+"100644 a\x00"+string(raw),
			"hash-object", "-t", "tree", "--literally", "-w", "--stdin")
		commit := strings.TrimSpace(coreGit(t, dir, "commit-tree", tree, "-m", "unsorted"))
		coreGit(t, dir, "update-ref", "refs/heads/main", commit)
		coreSeedStores(t, e, repo.RepoID, dir, map[string]string{"refs/heads/main": commit})
		token := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeGitRead}})

		l := getFilesListing(t, e, "/api/v1/repos/files?limit=2", token)
		if !slices.Equal(l.paths, []string{"a", "b"}) || !l.hasMore {
			t.Fatalf("page 1 = %v, has_more %v; want [a b], true", l.paths, l.hasMore)
		}
		l = getFilesListing(t, e, "/api/v1/repos/files?limit=2&cursor="+url.QueryEscape(l.cursor), token)
		if !slices.Equal(l.paths, []string{"c"}) || l.hasMore {
			t.Fatalf("page 2 = %v, has_more %v; want [c], false", l.paths, l.hasMore)
		}
	})
}

// gitStdin runs git in dir with stdin, without the inherited GIT_*
// environment, and returns its trimmed stdout.
func gitStdin(t *testing.T, dir, stdin string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "GIT_") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
	cmd.Stdin = strings.NewReader(stdin)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git %s: %v", strings.Join(args, " "), err)
	}
	return strings.TrimSpace(string(out))
}
