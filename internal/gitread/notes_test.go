package gitread

import (
	"context"
	"errors"
	"testing"
)

func TestNote(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	mustGit(t, f.dir, "notes", "add", "-m", "reviewed", f.c1)
	mustGit(t, f.dir, "notes", "--ref", "refs/notes/reviews", "add", "-m", "session note", f.c2)

	t.Run("default notes ref", func(t *testing.T) {
		note, err := Note(ctx, f.dir, "refs/notes/commits", f.c1)
		if err != nil || note != "reviewed\n" {
			t.Fatalf("Note = %q, %v; want %q", note, err, "reviewed\n")
		}
	})
	t.Run("custom notes ref", func(t *testing.T) {
		note, err := Note(ctx, f.dir, "refs/notes/reviews", f.c2)
		if err != nil || note != "session note\n" {
			t.Fatalf("Note = %q, %v; want %q", note, err, "session note\n")
		}
	})
	t.Run("object without note is ErrNotFound", func(t *testing.T) {
		if _, err := Note(ctx, f.dir, "refs/notes/commits", f.c2); !errors.Is(err, ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	})
	t.Run("unborn notes ref is ErrNotFound", func(t *testing.T) {
		if _, err := Note(ctx, f.dir, "refs/notes/missing", f.c1); !errors.Is(err, ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	})
	t.Run("unknown object is ErrNotFound", func(t *testing.T) {
		bogus := "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
		if _, err := Note(ctx, f.dir, "refs/notes/commits", bogus); !errors.Is(err, ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	})
}
