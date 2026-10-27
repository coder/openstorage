package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/gitread"
)

// filesFastImportRepo creates a repo in e from a git fast-import stream
// on branch main and returns its token and the work repo.
func filesFastImportRepo(t *testing.T, e *testEnv, stream string) (token, dir string) {
	t.Helper()
	org := e.newOrg(t)
	url := "https://example.com/" + org + "/files-" + randomHex(t, 4)
	repo, err := e.store.CreateRepo(context.Background(), org, url, "main")
	if err != nil {
		t.Fatalf("create repo: %v", err)
	}
	dir = t.TempDir()
	coreGit(t, dir, "init", "-q", "-b", "main", ".")
	cmd := exec.Command("git", "fast-import", "--quiet")
	cmd.Dir = dir
	cmd.Stdin = strings.NewReader(stream)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("fast-import: %v\n%s", err, out)
	}
	coreSeedStores(t, e, repo.RepoID, dir, map[string]string{"refs/heads/main": coreRevParse(t, dir, "main")})
	return e.mint(t, tokenSpec{iss: org, repo: url, scopes: []string{auth.ScopeGitRead}}), dir
}

// filesListing is the part of both listings' bodies these tests read.
// Raw keeps the body's top-level keys, because next_cursor must be absent
// (not empty) when has_more is false.
type filesListing struct {
	paths   []string
	last    map[string]string // files/metadata: path -> last_commit_sha
	hasMore bool
	cursor  string
	raw     map[string]json.RawMessage
}

func getFilesListing(t *testing.T, e *testEnv, path, token string) filesListing {
	t.Helper()
	resp, body := coreGet(t, e, "GET", path, token, nil)
	coreRequireStatus(t, resp, body, http.StatusOK)
	var got struct {
		Entries    []fileEntry     `json:"entries"`
		Files      []fileMetaEntry `json:"files"`
		NextCursor string          `json:"next_cursor"`
		HasMore    bool            `json:"has_more"`
	}
	coreDecode(t, body, &got)
	l := filesListing{last: map[string]string{}, hasMore: got.HasMore, cursor: got.NextCursor}
	coreDecode(t, body, &l.raw)
	for _, en := range got.Entries {
		l.paths = append(l.paths, en.Path)
	}
	for _, f := range got.Files {
		l.paths = append(l.paths, f.Path)
		l.last[f.Path] = f.LastCommitSHA
	}
	return l
}

// TestFilesListingsWithoutLimitReturnEveryEntry pins spec c494c01b (#569):
// files and files/metadata with no limit and no cursor (or limit=0) return
// every entry with has_more false; with a cursor and no limit they keep
// the 1000 and 200 pages.
func TestFilesListingsWithoutLimitReturnEveryEntry(t *testing.T) {
	const n = 1002 // more than one default page of files (1000)
	var in strings.Builder
	in.WriteString("commit refs/heads/main\nmark :1\ncommitter D <d@example.com> 1700000000 +0000\ndata 4\nroot\n")
	for i := range n {
		fmt.Fprintf(&in, "M 100644 inline d%02d/f%04d.txt\ndata 2\nx\n", i%10, i)
	}
	// A second commit gives one file a different last commit.
	in.WriteString("\ncommit refs/heads/main\nmark :2\ncommitter D <d@example.com> 1700000001 +0000\ndata 4\nedit\nfrom :1\nM 100644 inline d00/f0000.txt\ndata 2\ny\n\n")

	runBackends(t, func(t *testing.T, e *testEnv) {
		token, dir := filesFastImportRepo(t, e, in.String())
		tip := coreRevParse(t, dir, "main")

		for _, tc := range []struct {
			name, path string
			pageSize   int
		}{
			{"files", "/api/v1/repos/files?recursive=true", 1000},
			{"files/metadata", "/api/v1/repos/files/metadata?recursive=true", 200},
		} {
			t.Run(tc.name, func(t *testing.T) {
				for _, q := range []string{"", "&limit=0"} {
					l := getFilesListing(t, e, tc.path+q, token)
					if len(l.paths) != n || l.hasMore {
						t.Fatalf("%s%s: %d entries, has_more %v; want all %d, has_more false", tc.path, q, len(l.paths), l.hasMore, n)
					}
					if c, ok := l.raw["next_cursor"]; ok {
						t.Fatalf("%s%s: next_cursor %s present on the complete listing", tc.path, q, c)
					}
				}
				first := getFilesListing(t, e, tc.path+"&limit=1", token)
				if len(first.paths) != 1 || !first.hasMore || first.cursor == "" {
					t.Fatalf("limit=1 page = %+v", first)
				}
				for _, q := range []string{"", "&limit=0"} {
					l := getFilesListing(t, e, tc.path+"&cursor="+first.cursor+q, token)
					if len(l.paths) != tc.pageSize || !l.hasMore || l.paths[0] == first.paths[0] {
						t.Fatalf("cursor%s without limit: %d entries %.80v, has_more %v; want the %d-entry page after %q",
							q, len(l.paths), l.paths, l.hasMore, tc.pageSize, first.paths[0])
					}
				}
			})
		}

		t.Run("metadata last commits", func(t *testing.T) {
			l := getFilesListing(t, e, "/api/v1/repos/files/metadata", token)
			edited, others := 0, map[string]bool{}
			for p, sha := range l.last {
				if p == "d00/f0000.txt" {
					if sha != tip {
						t.Errorf("%s last_commit_sha = %s, want the tip %s", p, sha, tip)
					}
					edited++
					continue
				}
				others[sha] = true
			}
			if edited != 1 || len(others) != 1 || others[tip] {
				t.Fatalf("last commits: edited file seen %d times, other files' commits %v; want one root commit", edited, others)
			}
		})
	})
}

// TestFilesMetadataOverDistinctLastCommitsAnswers429 pins spec c494c01b
// (#569): files/metadata answers 429 when the files under `path` have more
// than 50,000 distinct last commits, also for a paged call, and without
// Retry-After (a retry gets the same answer). A narrower path, or a ref
// with exactly 50,000, is served.
func TestFilesMetadataOverDistinctLastCommitsAnswers429(t *testing.T) {
	// Commit i adds file i, so the tip has 50,001 files with 50,001
	// distinct last commits. Three directory levels keep trees small.
	const n = 50001
	var in strings.Builder
	for i := range n {
		fmt.Fprintf(&in, "commit refs/heads/main\nmark :%d\ncommitter D <d@example.com> %d +0000\ndata 0\n", i+1, 1700000000+i)
		if i > 0 {
			fmt.Fprintf(&in, "from :%d\n", i)
		}
		fmt.Fprintf(&in, "M 100644 inline %02d/%02d/%02d\ndata 0\n\n", i%37, (i/37)%37, i/(37*37))
	}

	runBackends(t, func(t *testing.T, e *testEnv) {
		// The 50,000-file listing below is slow under -race; this test is
		// about the limit, not the default 30 s git time budget.
		withGitReadPolicy(t, testGitReadPolicy(2, 2*time.Minute, gitread.DefaultOutputBytes, gitread.DefaultLimits()))
		token, dir := filesFastImportRepo(t, e, in.String())
		parent := coreRevParse(t, dir, "main^")

		for _, q := range []string{"", "?limit=1", "?limit=0"} {
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/files/metadata"+q, token, nil)
			coreRequireStatus(t, resp, body, http.StatusTooManyRequests)
			var problem errorEnvelope
			coreDecode(t, body, &problem)
			if problem.Code != "rate_limited" || !strings.Contains(problem.Detail, "50,000 distinct last commits") {
				t.Fatalf("files/metadata%s problem = %+v", q, problem)
			}
			for _, h := range []string{"Retry-After", "RateLimit"} {
				if v := resp.Header.Get(h); v != "" {
					t.Fatalf("files/metadata%s %s = %q, want none: a retry gets the same 429", q, h, v)
				}
			}
		}

		l := getFilesListing(t, e, "/api/v1/repos/files/metadata?path=00", token)
		if len(l.paths) != 1352 || l.hasMore {
			t.Fatalf("path=00: %d files, has_more %v; want all 1352", len(l.paths), l.hasMore)
		}
		l = getFilesListing(t, e, "/api/v1/repos/files/metadata?ref="+parent, token)
		distinct := map[string]bool{}
		for _, sha := range l.last {
			distinct[sha] = true
		}
		if len(l.paths) != n-1 || len(distinct) != n-1 || l.hasMore {
			t.Fatalf("ref=main^: %d files, %d distinct last commits, has_more %v; want %d of each", len(l.paths), len(distinct), l.hasMore, n-1)
		}
	})
}

// TestWriteJSONListsMatchesWriteJSON: the streamed listings are the bytes
// writeJSON writes for the whole response.
func TestWriteJSONListsMatchesWriteJSON(t *testing.T) {
	entries := []fileEntry{{Path: "a<&>.txt", Type: "blob", Mode: "100644"}, {Path: "d", Type: "tree", Mode: "040000"}, {Path: "é\"x", Type: "blob", Mode: "100755"}}
	for _, next := range []string{"", "Y3Vyc29y"} {
		whole := httptest.NewRecorder()
		writeJSON(whole, http.StatusOK, listFilesResponse{Paths: []string{"a<&>.txt", "é\"x"}, Ref: "main", Entries: entries, NextCursor: next, HasMore: next != ""})
		streamed := httptest.NewRecorder()
		writeJSONLists(streamed, http.StatusOK, listFilesResponse{Paths: []string{}, Ref: "main", Entries: []fileEntry{}, NextCursor: next, HasMore: next != ""},
			jsonList{"paths", func(yield func(any) bool) { _ = yield("a<&>.txt") && yield("é\"x") }},
			jsonList{"entries", func(yield func(any) bool) {
				for _, e := range entries {
					if !yield(e) {
						return
					}
				}
			}})
		if whole.Body.String() != streamed.Body.String() || streamed.Header().Get("Content-Type") != "application/json" {
			t.Fatalf("streamed %q (%v), want %q", streamed.Body, streamed.Header(), whole.Body)
		}
	}
	whole := httptest.NewRecorder()
	files := []fileMetaEntry{{Path: "x", Mode: "100644", Size: 3, LastCommitSHA: "c1", Type: "blob"}}
	commits := map[string]fileMetaCommit{"c1": {Author: "a", Date: "d", Message: "m", Order: 0}, "c0": {Order: 1}}
	writeJSON(whole, http.StatusOK, listFilesMetadataResponse{Files: files, Commits: commits, Ref: "r"})
	streamed := httptest.NewRecorder()
	writeJSONLists(streamed, http.StatusOK, listFilesMetadataResponse{Files: []fileMetaEntry{}, Commits: commits, Ref: "r"},
		jsonList{"files", func(yield func(any) bool) { yield(files[0]) }})
	if whole.Body.String() != streamed.Body.String() {
		t.Fatalf("streamed %q, want %q", streamed.Body, whole.Body)
	}
}
