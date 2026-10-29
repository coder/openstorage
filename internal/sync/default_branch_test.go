package sync

import (
	"bytes"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
)

// TestParseHeadSymref: only a HEAD symref to a valid branch names a
// default branch (issue #599); everything else says why not.
func TestParseHeadSymref(t *testing.T) {
	sha := strings.Repeat("a", 40)
	for _, tc := range []struct {
		out, branch string
	}{
		{"ref: refs/heads/trunk\tHEAD\n" + sha + "\tHEAD\n", "trunk"},
		{"ref: refs/heads/release/1.0\tHEAD\n", "release/1.0"},
		{"ref: refs/heads/x\trefs/heads/foo/HEAD\nref: refs/heads/trunk\tHEAD\n", "trunk"},
		{sha + "\tHEAD\n", ""},                      // no symref (dumb HTTP, old server)
		{"", ""},                                    // empty upstream
		{"ref: refs/tags/v1\tHEAD\n", ""},           // not a branch
		{"ref: refs/heads/bad..name\tHEAD\n", ""},   // git-invalid
		{"ref: refs/heads/-x\tHEAD\n", ""},          // API-invalid
		{"ref: refs/heads/trunk HEAD\n", ""},        // no tab
		{"ref: refs/heads/trunk\tHEAD extra\n", ""}, // not exactly HEAD
	} {
		branch, reason := parseHeadSymref(tc.out)
		if branch != tc.branch || (branch == "") == (reason == "") {
			t.Errorf("parseHeadSymref(%q) = %q, %q; want branch %q and a reason exactly when none", tc.out, branch, reason, tc.branch)
		}
	}
}

func TestCappedBuffer(t *testing.T) {
	c := &cappedBuffer{n: 4}
	for _, p := range []string{"ab", "cde", "fg"} {
		if n, err := c.Write([]byte(p)); n != len(p) || err != nil {
			t.Fatalf("Write(%q) = %d, %v; want the full length and no error", p, n, err)
		}
	}
	if string(c.buf) != "abcd" {
		t.Fatalf("buffer = %q, want the first 4 bytes", c.buf)
	}
}

// TestUpstreamHeadNeedsFetchedBranch: an upstream HEAD branch that the
// fetch did not return is never adopted, and the worker logs a WARN line.
// Once the mirror holds the branch, the same HEAD resolves.
func TestUpstreamHeadNeedsFetchedBranch(t *testing.T) {
	root := t.TempDir()
	seedUpstream(t, root, "o", "n")
	bare := filepath.Join(root, "o", "n.git")
	testGit(t, bare, "branch", "trunk", "main")
	testGit(t, bare, "symbolic-ref", "HEAD", "refs/heads/trunk")
	var logs bytes.Buffer
	w := &Worker{AllowProtocol: "file", Logger: slog.New(slog.NewTextHandler(&logs, nil))}
	env := hermeticGitEnv(w.allowProtocol())
	mirror := filepath.Join(t.TempDir(), "mirror.git")
	testGit(t, root, "init", "--bare", mirror)
	job := Job{Org: "o", RepoID: "repo_0123456789ab"}

	if got := w.upstreamHead(t.Context(), job, mirror, env, "file://"+bare); got != "" {
		t.Fatalf("HEAD with an unfetched branch = %q, want none", got)
	}
	if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "which the fetch did not return") {
		t.Fatalf("logs = %q, want a WARN line naming the unfetched branch", logs.String())
	}
	testGit(t, mirror, append([]string{"fetch", "file://" + bare}, mirrorRefspecs...)...)
	if got := w.upstreamHead(t.Context(), job, mirror, env, "file://"+bare); got != "trunk" {
		t.Fatalf("HEAD after the fetch = %q, want trunk", got)
	}
}
