package gitread

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// hashCommit writes a raw commit object and returns its SHA. literally
// skips git's fsck checks, which reject a NUL byte, as a push to a server
// that does not fsck would.
func hashCommit(t *testing.T, dir, raw string, literally bool) string {
	t.Helper()
	args := []string{"hash-object", "-t", "commit", "-w", "--stdin"}
	if literally {
		args = append(args, "--literally")
	}
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), gitEnv...)
	cmd.Stdin = strings.NewReader(raw)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("hash-object: %v\n%s", err, out)
	}
	return strings.TrimSpace(string(out))
}

// TestLogReadsControlBytesInFreeTextFields pins #580: names, emails and
// messages may hold 0x1f (git's fsck allows it), and a message that skipped
// fsck may hold a NUL byte. Each such commit still parses into the right
// fields, and the commits after it stay aligned.
func TestLogReadsControlBytesInFreeTextFields(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--initial-branch=main")
	tree := strings.TrimSpace(mustGit(t, dir, "mktree"))
	raw := func(parent, author, committer, msg string) string {
		var b strings.Builder
		fmt.Fprintf(&b, "tree %s\n", tree)
		if parent != "" {
			fmt.Fprintf(&b, "parent %s\n", parent)
		}
		fmt.Fprintf(&b, "author %s 1700000000 +0000\ncommitter %s 1700000001 +0000\n\n%s", author, committer, msg)
		return b.String()
	}
	unit := hashCommit(t, dir, raw("", "A\x1fB <a\x1fb@x>", "C\x1fD <c\x1fd@x>", "sub\x1fject\n\nbody\x1ftext\n"), false)
	// git stops a message at its first NUL byte.
	nul := hashCommit(t, dir, raw(unit, "N <n@x>", "C <c@x>", "nul\x00hidden\n"), true)
	plain := hashCommit(t, dir, raw(nul, "P <p@x>", "Q <q@x>", "plain\n"), false)

	want := []Commit{
		{SHA: plain, ParentSHAs: []string{nul}, AuthorName: "P", AuthorEmail: "p@x", CommitterName: "Q", CommitterEmail: "q@x",
			Message: "plain\n", Subject: "plain"},
		{SHA: nul, ParentSHAs: []string{unit}, AuthorName: "N", AuthorEmail: "n@x", CommitterName: "C", CommitterEmail: "c@x",
			Message: "nul", Subject: "nul"},
		{SHA: unit, ParentSHAs: []string{}, AuthorName: "A\x1fB", AuthorEmail: "a\x1fb@x", CommitterName: "C\x1fD", CommitterEmail: "c\x1fd@x",
			Message: "sub\x1fject\n\nbody\x1ftext\n", Subject: "sub\x1fject"},
	}
	check := func(t *testing.T, got Commit, w Commit, subjectOnly bool) {
		t.Helper()
		if subjectOnly {
			w.Message = ""
		} else {
			w.Subject = ""
		}
		w.TreeSHA = tree
		if !got.AuthorDate.Equal(time.Unix(1700000000, 0)) || !got.CommitterDate.Equal(time.Unix(1700000001, 0)) {
			t.Errorf("commit %s: dates %v, %v", w.SHA[:7], got.AuthorDate, got.CommitterDate)
		}
		got.AuthorDate, got.CommitterDate = time.Time{}, time.Time{}
		if fmt.Sprintf("%#v", got) != fmt.Sprintf("%#v", w) {
			t.Errorf("commit %s:\n got %#v\nwant %#v", w.SHA[:7], got, w)
		}
	}
	ctx := t.Context()

	for _, subjectOnly := range []bool{false, true} {
		got, err := Log(ctx, dir, plain, LogOptions{SubjectOnly: subjectOnly})
		if err != nil {
			t.Fatalf("Log(SubjectOnly %v): %v", subjectOnly, err)
		}
		if len(got) != len(want) {
			t.Fatalf("Log(SubjectOnly %v): %d commits, want %d", subjectOnly, len(got), len(want))
		}
		for i := range want {
			check(t, got[i], want[i], subjectOnly)
		}
	}

	bySHA, err := CommitsBySHA(ctx, dir, []string{unit, nul, plain})
	if err != nil {
		t.Fatalf("CommitsBySHA: %v", err)
	}
	for _, w := range want {
		check(t, bySHA[w.SHA], w, true)
	}

	detail, err := GetCommit(ctx, dir, unit)
	if err != nil {
		t.Fatalf("GetCommit: %v", err)
	}
	check(t, detail.Commit, want[2], false)
}

// TestLogRecordsRejectsMisframedOutput checks that logRecords fails
// instead of shifting fields between commits.
func TestLogRecordsRejectsMisframedOutput(t *testing.T) {
	ten := strings.Repeat("f\x00", logFields)
	for _, tc := range []struct {
		name, out string
		records   int // -1: error
	}{
		{"empty", "", 0},
		{"one record", ten, 1},
		{"two records", ten + ten, 2},
		{"no final NUL", strings.TrimSuffix(ten, "\x00"), -1},
		{"one field short", strings.TrimPrefix(ten, "f\x00"), -1},
		{"one field extra", ten + "f\x00", -1},
	} {
		recs, err := logRecords(tc.out)
		switch {
		case tc.records < 0 && err == nil:
			t.Errorf("%s: %d records, want an error", tc.name, len(recs))
		case tc.records >= 0 && (err != nil || len(recs) != tc.records):
			t.Errorf("%s: %d records, %v; want %d", tc.name, len(recs), err, tc.records)
		}
		for _, r := range recs {
			if len(r) != logFields {
				t.Errorf("%s: record of %d fields", tc.name, len(r))
			}
		}
	}
}
