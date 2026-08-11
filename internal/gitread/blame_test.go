package gitread

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBlame(t *testing.T) {
	f := newFixture(t)
	lines, err := Blame(context.Background(), f.dir, f.c2, "README.md", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 4 {
		t.Fatalf("got %d blame lines, want 4: %+v", len(lines), lines)
	}
	// Lines 1-3 came from c1, line 4 from c2.
	for i, want := range []string{f.c1, f.c1, f.c1, f.c2} {
		l := lines[i]
		if l.LineNumber != i+1 {
			t.Errorf("line %d numbered %d", i+1, l.LineNumber)
		}
		if l.CommitSHA != want {
			t.Errorf("line %d attributed to %s, want %s", i+1, l.CommitSHA, want)
		}
		if l.OriginalPath != "README.md" {
			t.Errorf("line %d original path %q", i+1, l.OriginalPath)
		}
		if l.AuthorName != "Alice Author" || l.AuthorEmail != "alice@example.com" {
			t.Errorf("line %d author %q <%q>", i+1, l.AuthorName, l.AuthorEmail)
		}
		if l.CommitterName != "Bob Committer" {
			t.Errorf("line %d committer %q", i+1, l.CommitterName)
		}
	}
	// c1 introduced its lines at the same numbers.
	if lines[1].OriginalLineNumber != 2 {
		t.Errorf("line 2 original line = %d", lines[1].OriginalLineNumber)
	}
	// The c2 group carries previous = c1; the root c1 group has none.
	if lines[3].PreviousCommitSHA != f.c1 {
		t.Errorf("line 4 previous = %q, want %s", lines[3].PreviousCommitSHA, f.c1)
	}
	if lines[0].PreviousCommitSHA != "" {
		t.Errorf("root line previous = %q, want empty", lines[0].PreviousCommitSHA)
	}
	if lines[0].Summary != "root commit" || lines[3].Summary != "second commit" {
		t.Errorf("summaries = %q / %q", lines[0].Summary, lines[3].Summary)
	}
	// Timestamps carry the fixture's +0200 author time.
	want := time.Date(2024, 1, 2, 3, 4, 5, 0, time.FixedZone("+0200", 2*3600))
	if !lines[0].AuthorTime.Equal(want) {
		t.Errorf("author time = %v, want %v", lines[0].AuthorTime, want)
	}
	wantCommit := time.Date(2024, 1, 2, 4, 5, 6, 0, time.FixedZone("+0200", 2*3600))
	if !lines[0].CommitterTime.Equal(wantCommit) {
		t.Errorf("committer time = %v, want %v", lines[0].CommitterTime, wantCommit)
	}
}

func TestBlameRanges(t *testing.T) {
	f := newFixture(t)
	lines, err := Blame(context.Background(), f.dir, f.c2, "README.md", []string{"2,3"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 2 || lines[0].LineNumber != 2 || lines[1].LineNumber != 3 {
		t.Fatalf("ranged blame = %+v", lines)
	}
}

func TestBlameRenamedFileTracksOriginalPath(t *testing.T) {
	f := newFixture(t)
	lines, err := Blame(context.Background(), f.dir, f.c2, "dir/renamed.txt", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 2 {
		t.Fatalf("blame lines = %+v", lines)
	}
	// Content is unchanged since c1, where the file was dir/data.txt.
	if lines[0].CommitSHA != f.c1 || lines[0].OriginalPath != "dir/data.txt" {
		t.Errorf("renamed blame = %+v", lines[0])
	}
}

func TestBlameMissingPath(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	if _, err := Blame(ctx, f.dir, f.c1, "does-not-exist.txt", nil, false); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing path err = %v, want ErrNotFound", err)
	}
	if _, err := Blame(ctx, f.dir, f.c1, "", nil, false); !errors.Is(err, ErrNotFound) {
		t.Errorf("empty path err = %v, want ErrNotFound", err)
	}
	// A range STARTING past EOF is a git usage error, not a not-found
	// (modern git clamps an end past EOF, so "1,999" succeeds).
	if _, err := Blame(ctx, f.dir, f.c1, "README.md", []string{"999,1000"}, false); err == nil || errors.Is(err, ErrNotFound) {
		t.Errorf("range past EOF err = %v, want non-NotFound error", err)
	}
}
