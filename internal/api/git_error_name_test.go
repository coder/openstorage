package api

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

// The write paths run git outside gitread.Run, and their errors go to the
// log too. A value of 100 KB in an argument must not be in the error.
func TestWriteGitErrorsNameLongArgumentsBriefly(t *testing.T) {
	dir := t.TempDir()
	coreGit(t, dir, "init", "-q", "-b", "main", ".")
	sc, err := wcNewScratch(filepath.Join(dir, ".git"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sc.close)
	long := strings.Repeat("a", 100_000)
	ctx := context.Background()

	for name, run := range map[string]func() error{
		"scratch": func() error {
			_, err := sc.git(ctx, nil, nil, "rev-parse", "--verify", long)
			return err
		},
		"refs": func() error {
			_, err := wrefGit(ctx, dir, nil, nil, "rev-parse", "--verify", long)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := run()
			if err == nil {
				t.Fatal("git accepted the value")
			}
			msg := err.Error()
			if len(msg) > 1024 {
				t.Fatalf("the error is %d bytes, want it bounded: %.200s", len(msg), msg)
			}
			if want := "git rev-parse --verify aaaa"; !strings.HasPrefix(msg, want) {
				t.Errorf("error %.200q does not start with %q", msg, want)
			}
			if want := "...(100000 bytes)"; !strings.Contains(msg, want) {
				t.Errorf("error %.300q lacks %q", msg, want)
			}
		})
	}
}
