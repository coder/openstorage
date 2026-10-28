package api

// Issue #581: a pushed commit whose author or committer line git cannot
// parse reads with git's fallbacks (empty name and email, the Unix epoch
// for a date git cannot read or RFC 3339 cannot write) instead of 500.

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/auth"
)

func mustRFC3339(t *testing.T, s string) time.Time {
	t.Helper()
	when, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return when
}

var identCases = []struct {
	name, ident        string // ident: the line after "author "/"committer "
	wantName, wantMail string
	wantDate           string
}{
	{"well formed", "N <n@x> 1700000300 +0000", "N", "n@x", "2023-11-14T22:18:20Z"},
	{"no date", "N <n@x>", "N", "n@x", "1970-01-01T00:00:00Z"},
	{"time zone +9999", "N <n@x> 1700000300 +9999", "N", "n@x", "2023-11-14T22:18:20Z"},
	{"year 10000", "N <n@x> 253402300800 +0000", "N", "n@x", "1970-01-01T00:00:00Z"},
	{"overflows uint64", "N <n@x> 99999999999999999999999 +0000", "N", "n@x", "1970-01-01T00:00:00Z"},
	{"NUL in name", "N\x00ame <n@x> 1700000300 +0000", "", "", "1970-01-01T00:00:00Z"},
}

func TestCommitReadsMalformedIdents(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		repoURL := "https://example.com/" + org + "/ident-" + randomHex(t, 4)
		repo, err := e.store.CreateRepo(context.Background(), org, repoURL, "main")
		if err != nil {
			t.Fatalf("create repo: %v", err)
		}
		dir := t.TempDir()
		coreGit(t, dir, "init", "-q", "-b", "main", ".")
		shas := make([]string, len(identCases)) // oldest first
		parent := ""
		for i, c := range identCases {
			coreWriteFile(t, dir, fmt.Sprintf("f%02d.txt", i), c.name+"\n")
			coreGit(t, dir, "add", ".")
			tree := strings.TrimSpace(coreGit(t, dir, "write-tree"))
			var obj strings.Builder
			obj.WriteString("tree " + tree + "\n")
			if parent != "" {
				obj.WriteString("parent " + parent + "\n")
			}
			fmt.Fprintf(&obj, "author %s\ncommitter %s\n\n%s\n", c.ident, c.ident, c.name)
			p := filepath.Join(t.TempDir(), "commit")
			if err := os.WriteFile(p, []byte(obj.String()), 0o644); err != nil {
				t.Fatal(err)
			}
			// --literally: git's fsck rejects a NUL in a header, but a
			// server push runs no fsck.
			shas[i] = strings.TrimSpace(coreGit(t, dir, "hash-object", "-t", "commit", "-w", "--literally", p))
			parent = shas[i]
		}
		coreGit(t, dir, "update-ref", "refs/heads/main", parent) // repack packs only reachable objects
		coreSeedStores(t, e, repo.RepoID, dir, map[string]string{"refs/heads/main": parent})
		token := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeGitRead}})

		checkInfo := func(t *testing.T, what string, i int, got map[string]any) {
			t.Helper()
			c := identCases[i]
			for key, want := range map[string]string{"sha": shas[i], "author_name": c.wantName, "author_email": c.wantMail,
				"committer_name": c.wantName, "committer_email": c.wantMail, "date": c.wantDate, "committer_date": c.wantDate} {
				if got[key] != want {
					t.Errorf("%s %s: %s = %#v, want %q", c.name, what, key, got[key], want)
				}
			}
		}

		resp, body := coreGet(t, e, "GET", "/api/v1/repos/commits?limit=100", token, nil)
		coreRequireStatus(t, resp, body, 200)
		var list struct {
			Commits []map[string]any `json:"commits"`
		}
		coreDecode(t, body, &list)
		if len(list.Commits) != len(identCases) {
			t.Fatalf("listed %d commits, want %d: %s", len(list.Commits), len(identCases), body)
		}
		for i := range identCases {
			checkInfo(t, "commits", i, list.Commits[len(identCases)-1-i]) // newest first
		}

		resp, body = coreGet(t, e, "GET", "/api/v1/repos/files/metadata", token, nil)
		coreRequireStatus(t, resp, body, 200)
		var meta struct {
			Commits map[string]map[string]any `json:"commits"`
		}
		coreDecode(t, body, &meta)
		for i, c := range identCases {
			if got := meta.Commits[shas[i]]; got["author"] != c.wantName || got["date"] != c.wantDate {
				t.Errorf("%s files/metadata: commit %#v, want author %q date %q", c.name, got, c.wantName, c.wantDate)
			}
		}

		for i, c := range identCases {
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/commit?ref="+shas[i], token, nil)
			coreRequireStatus(t, resp, body, 200)
			var one struct {
				Commit map[string]any `json:"commit"`
			}
			coreDecode(t, body, &one)
			checkInfo(t, "commit", i, one.Commit)

			file := url.QueryEscape(fmt.Sprintf("f%02d.txt", i))
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/commits?path="+file, token, nil)
			coreRequireStatus(t, resp, body, 200)
			var byPath struct {
				Commits []map[string]any `json:"commits"`
			}
			coreDecode(t, body, &byPath)
			if len(byPath.Commits) != 1 {
				t.Fatalf("%s commits?path: %d commits, want 1: %s", c.name, len(byPath.Commits), body)
			}
			checkInfo(t, "commits?path", i, byPath.Commits[0])

			resp, body = coreGet(t, e, "HEAD", "/api/v1/repos/file?path="+file, token, nil)
			coreRequireStatus(t, resp, body, 200)
			if got := resp.Header.Get("X-Last-Commit-Sha"); got != shas[i] {
				t.Errorf("%s HEAD file: X-Last-Commit-Sha = %q, want %s", c.name, got, shas[i])
			}
			// http.ServeContent sends no Last-Modified for the epoch.
			wantLM := ""
			if when := mustRFC3339(t, c.wantDate); when.Unix() != 0 {
				wantLM = when.Format(http.TimeFormat)
			}
			if got := resp.Header.Get("Last-Modified"); got != wantLM {
				t.Errorf("%s HEAD file: Last-Modified = %q, want %q", c.name, got, wantLM)
			}

			resp, body = coreGet(t, e, "GET", "/api/v1/repos/blame?path="+file, token, nil)
			coreRequireStatus(t, resp, body, 200)
			var blame struct {
				Lines []map[string]any `json:"lines"`
			}
			coreDecode(t, body, &blame)
			if len(blame.Lines) != 1 || blame.Lines[0]["commit_sha"] != shas[i] {
				t.Fatalf("%s blame: %s", c.name, body)
			}
			for _, key := range []string{"author_time", "committer_time"} {
				if got := blame.Lines[0][key]; got != c.wantDate {
					t.Errorf("%s blame: %s = %#v, want %q", c.name, key, got, c.wantDate)
				}
			}

			resp, body = e.doReq(t, "POST", "/api/v1/repos/archive", token, []byte(`{"ref":"`+shas[i]+`"}`), "")
			coreRequireStatus(t, resp, body, 200)
			gz, err := gzip.NewReader(bytes.NewReader(body))
			if err != nil {
				t.Fatalf("%s archive: %v", c.name, err)
			}
			hdr, err := tar.NewReader(gz).Next()
			if err != nil {
				t.Fatalf("%s archive: %v", c.name, err)
			}
			if want := mustRFC3339(t, c.wantDate); !hdr.ModTime.Equal(want) {
				t.Errorf("%s archive: %s mtime %v, want %v", c.name, hdr.Name, hdr.ModTime, want)
			}
		}
	})
}
