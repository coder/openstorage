package api

// Issue #570 (upstream docs spec b598ce96): GET /repos/commits list entries
// carry only the subject line, "the first paragraph of the message as one
// line, without the body and the trailers"; GET /repos/commit keeps the full
// message.

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/auth"
)

// upstreamSubjectWording is the vendored spec's Commit.message rule that
// subjectCases encode. If a spec refresh rewords it, recheck the cases.
const upstreamSubjectWording = "In repos.commits.list responses, only the subject line: " +
	"the first paragraph of the message as one line, without the body and the trailers."

func TestCommitSubjectWordingMatchesUpstream(t *testing.T) {
	raw, err := os.ReadFile(upstreamSpecPath)
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(raw), upstreamSubjectWording); n != 1 {
		t.Fatalf("%s holds the Commit.message subject rule %d times, want 1", upstreamSpecPath, n)
	}
}

// subjectCase is one commit message shape and the subject upstream's
// wording gives for it.
type subjectCase struct {
	name, raw, subject string
}

var subjectCases = []subjectCase{
	{"single line", "just a subject\n", "just a subject"},
	{"no trailing newline", "no newline at end", "no newline at end"},
	{"empty message", "", ""},
	{"body and trailers", "subject line\n\nbody paragraph\nmore body\n\nSigned-off-by: A <a@test.invalid>\n", "subject line"},
	{"trailers only", "subject only\n\nSigned-off-by: A <a@test.invalid>\nChange-Id: I0123\n", "subject only"},
	// Git never reads trailers from the first paragraph, so a trailer-like
	// line there is part of the subject.
	{"trailer line in first paragraph", "subject\nSigned-off-by: A <a@test.invalid>\n", "subject Signed-off-by: A <a@test.invalid>"},
	// A multi-line first paragraph becomes one line: lines joined by one
	// space, trailing whitespace of each line dropped.
	{"multi-line first paragraph", "first line  \nsecond line\t\nthird line\n\nbody\n", "first line second line third line"},
	{"crlf", "subject one\r\nsubject two\r\n\r\nbody\r\n", "subject one subject two"},
	// A whitespace-only line ends the paragraph.
	{"whitespace-only separator", "para one\n \t\npara two\n", "para one"},
	// Blank lines before the first paragraph are not a paragraph.
	{"leading blank lines", "\n \n\nreal subject\n\nbody\n", "real subject"},
}

// subjectCommit writes one commit object with exactly raw as its message
// (hash-object keeps the bytes; `git commit` would clean them up).
func subjectCommit(t *testing.T, dir, tree, parent, raw string) string {
	t.Helper()
	var obj strings.Builder
	obj.WriteString("tree " + tree + "\n")
	if parent != "" {
		obj.WriteString("parent " + parent + "\n")
	}
	obj.WriteString("author core-test <core@test.invalid> 1704164645 +0000\n")
	obj.WriteString("committer core-test <core@test.invalid> 1706933106 +0000\n")
	obj.WriteString("\n" + raw)
	p := filepath.Join(t.TempDir(), "commit")
	if err := os.WriteFile(p, []byte(obj.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(coreGit(t, dir, "hash-object", "-t", "commit", "-w", p))
}

func TestCommitListMessageIsSubjectLine(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		url := "https://example.com/" + org + "/subject-" + randomHex(t, 4)
		repo, err := e.store.CreateRepo(context.Background(), org, url, "main")
		if err != nil {
			t.Fatalf("create repo: %v", err)
		}
		dir := t.TempDir()
		coreGit(t, dir, "init", "-q", "-b", "main", ".")
		coreWriteFile(t, dir, "a.txt", "a\n")
		coreGit(t, dir, "add", ".")
		tree := strings.TrimSpace(coreGit(t, dir, "write-tree"))

		shas := make([]string, len(subjectCases)) // oldest first
		parent := ""
		for i, c := range subjectCases {
			shas[i] = subjectCommit(t, dir, tree, parent, c.raw)
			parent = shas[i]
			// The oracle literal must be what git's %s prints: the
			// implementation relies on that equivalence.
			if got := strings.TrimSuffix(coreGit(t, dir, "log", "-1", "--format=%s", shas[i]), "\n"); got != c.subject {
				t.Fatalf("%s: git %%s = %q, want %q", c.name, got, c.subject)
			}
		}
		coreGit(t, dir, "update-ref", "refs/heads/main", parent) // repack packs only reachable objects
		coreSeedStores(t, e, repo.RepoID, dir, map[string]string{"refs/heads/main": parent})
		token := e.mint(t, tokenSpec{iss: org, repo: url, scopes: []string{auth.ScopeGitRead}})

		resp, body := coreGet(t, e, "GET", "/api/v1/repos/commits?limit=100", token, nil)
		coreRequireStatus(t, resp, body, 200)
		// Raw decode: the key must be present even for an empty subject.
		var list struct {
			Commits []map[string]any `json:"commits"`
		}
		coreDecode(t, body, &list)
		if len(list.Commits) != len(subjectCases) {
			t.Fatalf("listed %d commits, want %d: %s", len(list.Commits), len(subjectCases), body)
		}
		for i, c := range subjectCases {
			entry := list.Commits[len(subjectCases)-1-i] // newest first
			if entry["sha"] != shas[i] {
				t.Fatalf("%s: entry sha = %v, want %s", c.name, entry["sha"], shas[i])
			}
			msg, ok := entry["message"].(string)
			if !ok || msg != c.subject {
				t.Errorf("%s: list message = %#v, want %q", c.name, entry["message"], c.subject)
			}
		}

		// GET /repos/commit keeps the full message (trailing newlines
		// trimmed, as before).
		for i, c := range subjectCases {
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/commit?ref="+shas[i], token, nil)
			coreRequireStatus(t, resp, body, 200)
			var got struct {
				Commit map[string]any `json:"commit"`
			}
			coreDecode(t, body, &got)
			if want := strings.TrimRight(c.raw, "\n"); got.Commit["message"] != want {
				t.Errorf("%s: commit message = %#v, want %q", c.name, got.Commit["message"], want)
			}
		}
	})
}
