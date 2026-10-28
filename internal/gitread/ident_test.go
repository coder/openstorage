package gitread

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// identShapes are author or committer lines that git cannot fully parse,
// and what the reads return for them (#581). git prints an empty %an/%ae
// when it cannot split the line into a name and an email, and `git log`
// shows the Unix epoch for a date it cannot read or show.
var identShapes = []struct {
	name, ident        string // ident: the line after "author "/"committer "
	wantName, wantMail string
	wantUnix           int64
}{
	{"well formed", "N <n@x> 1700000300 +0000", "N", "n@x", 1700000300},
	{"NUL in name", "N\x00ame <n@x> 1700000300 +0000", "", "", 0},
	{"no <", "N n@x> 1700000300 +0000", "", "", 0},
	{"no >", "N <n@x 1700000300 +0000", "", "", 0},
	{"empty", "", "", "", 0},
	{"no date", "N <n@x>", "N", "n@x", 0},
	{"no time zone", "N <n@x> 1700000300", "N", "n@x", 0},
	{"date not a number", "N <n@x> abc +0000", "N", "n@x", 0},
	{"negative date", "N <n@x> -5 +0000", "N", "n@x", 0},
	{"time zone not a number", "N <n@x> 1700000300 zz", "N", "n@x", 0},
	// git renders this offset as +99:99, which RFC 3339 rejects. The
	// instant is still exact.
	{"time zone +9999", "N <n@x> 1700000300 +9999", "N", "n@x", 1700000300},
	{"last RFC 3339 second", "N <n@x> 253402300799 +0000", "N", "n@x", 253402300799},
	// git prints year 10000, which RFC 3339 cannot hold.
	{"year 10000", "N <n@x> 253402300800 +0000", "N", "n@x", 0},
	{"int64 max", "N <n@x> 9223372036854775807 +0000", "N", "n@x", 0},
	{"overflows uint64", "N <n@x> 99999999999999999999999 +0000", "N", "n@x", 0},
}

// TestLogReadsMalformedIdents pins #581: a commit whose author and
// committer lines git cannot parse (pushable, because the server runs no
// fsck) still reads, with git's own fallbacks, and does not shift the
// commits around it.
func TestLogReadsMalformedIdents(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--initial-branch=main")
	tree := strings.TrimSpace(mustGit(t, dir, "mktree"))
	want := make([]Commit, 0, len(identShapes)) // newest first
	parent := ""
	for _, s := range identShapes {
		var b strings.Builder
		fmt.Fprintf(&b, "tree %s\n", tree)
		if parent != "" {
			fmt.Fprintf(&b, "parent %s\n", parent)
		}
		fmt.Fprintf(&b, "author %s\ncommitter %s\n\n%s\n", s.ident, s.ident, s.name)
		sha := hashCommit(t, dir, b.String(), true)
		parents := []string{}
		if parent != "" {
			parents = []string{parent}
		}
		when := time.Unix(s.wantUnix, 0)
		want = append([]Commit{{SHA: sha, TreeSHA: tree, ParentSHAs: parents,
			AuthorName: s.wantName, AuthorEmail: s.wantMail, AuthorDate: when,
			CommitterName: s.wantName, CommitterEmail: s.wantMail, CommitterDate: when,
			Message: s.name + "\n", Subject: s.name}}, want...)
		parent = sha
	}
	check := func(t *testing.T, got, w Commit, subjectOnly bool) {
		t.Helper()
		if subjectOnly {
			w.Message = ""
		} else {
			w.Subject = ""
		}
		if !got.AuthorDate.Equal(w.AuthorDate) || !got.CommitterDate.Equal(w.CommitterDate) {
			t.Errorf("%s: dates %v, %v; want %v", w.Subject+w.Message, got.AuthorDate, got.CommitterDate, w.AuthorDate)
		}
		got.AuthorDate, got.CommitterDate = time.Time{}, time.Time{}
		w.AuthorDate, w.CommitterDate = time.Time{}, time.Time{}
		if fmt.Sprintf("%#v", got) != fmt.Sprintf("%#v", w) {
			t.Errorf("\n got %#v\nwant %#v", got, w)
		}
	}
	ctx := t.Context()
	for _, subjectOnly := range []bool{false, true} {
		got, err := Log(ctx, dir, parent, LogOptions{SubjectOnly: subjectOnly})
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
	shas := make([]string, 0, len(want))
	for _, w := range want {
		shas = append(shas, w.SHA)
	}
	bySHA, err := CommitsBySHA(ctx, dir, shas)
	if err != nil {
		t.Fatalf("CommitsBySHA: %v", err)
	}
	for _, w := range want {
		check(t, bySHA[w.SHA], w, true)
		detail, err := GetCommit(ctx, dir, w.SHA)
		if err != nil {
			t.Fatalf("GetCommit(%s): %v", w.Subject, err)
		}
		check(t, detail.Commit, w, false)
	}
}

// TestBlameReadsUnrepresentableTimes: git blame prints any 64-bit
// unsigned author-time, and a time past what RFC 3339 can write reads as
// the epoch, as in Log (#581).
func TestBlameReadsUnrepresentableTimes(t *testing.T) {
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--initial-branch=main")
	if err := os.WriteFile(filepath.Join(dir, "f"), []byte("line\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	mustGit(t, dir, "add", "f")
	tree := strings.TrimSpace(mustGit(t, dir, "write-tree"))
	for _, s := range identShapes {
		raw := fmt.Sprintf("tree %s\nauthor %s\ncommitter %s\n\n%s\n", tree, s.ident, s.ident, s.name)
		sha := hashCommit(t, dir, raw, true)
		lines, err := Blame(t.Context(), dir, sha, "f", nil, false)
		if err != nil {
			t.Errorf("%s: Blame: %v", s.name, err)
			continue
		}
		if len(lines) != 1 {
			t.Fatalf("%s: %d blame lines, want 1", s.name, len(lines))
		}
		if l := lines[0]; !l.AuthorTime.Equal(time.Unix(s.wantUnix, 0)) || !l.CommitterTime.Equal(time.Unix(s.wantUnix, 0)) {
			t.Errorf("%s: blame times %v, %v; want unix %d", s.name, l.AuthorTime, l.CommitterTime, s.wantUnix)
		}
	}
}

func TestIdentTime(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int64
		err  bool
	}{
		{"", 0, false},
		{"0", 0, false},
		{"1700000300", 1700000300, false},
		{"253402300799", 253402300799, false},
		{"253402300800", 0, false},
		{fmt.Sprint(int64(math.MaxInt64)), 0, false},
		{"18446744073709551615", 0, false},
		{"99999999999999999999999", 0, false},
		{"-5", 0, true},
		{"+5", 0, true},
		{"12a", 0, true},
		{" 1", 0, true},
		{"%at", 0, true},
	} {
		got, err := identTime(tc.in)
		switch {
		case tc.err && err == nil:
			t.Errorf("identTime(%q) = %v, want an error", tc.in, got)
		case !tc.err && (err != nil || !got.Equal(time.Unix(tc.want, 0)) || got.Location() != time.UTC):
			t.Errorf("identTime(%q) = %v, %v; want unix %d in UTC", tc.in, got, err, tc.want)
		}
	}
}

// TestParseBlameTimeStaysInRFC3339: blame keeps the author's offset, but
// an offset or local year that RFC 3339 cannot write reads as UTC (#581).
func TestParseBlameTimeStaysInRFC3339(t *testing.T) {
	for _, tc := range []struct{ sec, tz, want string }{
		{"1700000300", "+0100", "2023-11-14T23:18:20+01:00"},
		{"1700000300", "-0130", "2023-11-14T20:48:20-01:30"},
		{"1700000300", "+2359", "2023-11-15T22:17:20+23:59"},
		{"1700000300", "+9999", "2023-11-14T22:18:20Z"},
		{"1700000300", "+2400", "2023-11-14T22:18:20Z"},
		{"1700000300", "+0060", "2023-11-14T22:18:20Z"},
		{"1700000300", "(unknown)", "2023-11-14T22:18:20Z"},
		{"253402300799", "+0100", "9999-12-31T23:59:59Z"},
		{"253402300799", "-0100", "9999-12-31T22:59:59-01:00"},
		{"0", "-0100", "1969-12-31T23:00:00-01:00"},
	} {
		got, err := parseBlameTime(tc.sec, tc.tz)
		if err != nil {
			t.Errorf("parseBlameTime(%q, %q): %v", tc.sec, tc.tz, err)
			continue
		}
		s := got.Format(time.RFC3339)
		if s != tc.want {
			t.Errorf("parseBlameTime(%q, %q) = %s, want %s", tc.sec, tc.tz, s, tc.want)
		}
		if _, err := time.Parse(time.RFC3339, s); err != nil {
			t.Errorf("parseBlameTime(%q, %q) = %s, not RFC 3339: %v", tc.sec, tc.tz, s, err)
		}
	}
}
