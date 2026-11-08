package jobs

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// envGit puts a git first on PATH that writes its environment to the
// returned file and exits 0. The test's own environment drops
// GIT_NO_REPLACE_OBJECTS, so that only the runner can set it.
func envGit(t *testing.T) string {
	t.Helper()
	bin := t.TempDir()
	log := filepath.Join(bin, "env")
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte("#!/bin/sh\nenv > '"+log+"'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("GIT_NO_REPLACE_OBJECTS", "")
	if err := os.Unsetenv("GIT_NO_REPLACE_OBJECTS"); err != nil {
		t.Fatal(err)
	}
	return log
}

// The repack runners start git without gitlimit.Start, so each sets
// GIT_NO_REPLACE_OBJECTS itself: a pushed refs/replace/<id> must not
// change the objects that a repack reads.
func TestRepackRunnersIgnoreReplacements(t *testing.T) {
	log := envGit(t)
	for name, run := range map[string]func() error{
		"runGitStdin": func() error {
			_, err := runGitStdin(context.Background(), t.TempDir(), nil, "status")
			return err
		},
		"streamShowIndex": func() error {
			return streamShowIndex(context.Background(), t.TempDir(), strings.NewReader(""), func(string) {})
		},
	} {
		_ = os.Remove(log)
		if err := run(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		env, err := os.ReadFile(log)
		if err != nil {
			t.Fatalf("%s: git did not run: %v", name, err)
		}
		if !slices.Contains(strings.Split(string(env), "\n"), "GIT_NO_REPLACE_OBJECTS=1") {
			t.Errorf("%s: git ran without GIT_NO_REPLACE_OBJECTS=1", name)
		}
	}
}
