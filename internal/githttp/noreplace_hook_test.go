package githttp

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// The hook's git calls take GIT_NO_REPLACE_OBJECTS=1 from the hook's
// environment: gitlimit.Start sets it for receive-pack, which passes it
// to its hooks. The object checks, which build their own environment,
// keep it. The object type read of a branch tip is one of them.
func TestHookObjectChecksKeepNoReplaceObjects(t *testing.T) {
	bin := t.TempDir()
	log := filepath.Join(bin, "env")
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte("#!/bin/sh\nenv > '"+log+"'\necho commit\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("GIT_NO_REPLACE_OBJECTS", "1")
	objectIsCommit := func(id string) bool {
		kinds, err := objectKinds([]string{id})
		return err == nil && kinds[id] == "commit"
	}
	for name, check := range map[string]func(string) bool{"objectExists": objectExists, "objectInMainStore": objectInMainStore, "objectKinds": objectIsCommit} {
		_ = os.Remove(log)
		if !check(strings.Repeat("a", 40)) {
			t.Fatalf("%s = false for a git that exits 0", name)
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
