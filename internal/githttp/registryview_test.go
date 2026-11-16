package githttp

import (
	"slices"
	"strings"
	"testing"
)

func TestParseObjectKinds(t *testing.T) {
	a, b, c := strings.Repeat("a", 40), strings.Repeat("b", 40), strings.Repeat("c", 40)
	got, err := ParseObjectKinds([]string{a, b, c}, "commit\n"+b+" missing\ntag\n")
	if err != nil || got[a] != "commit" || got[b] != "missing" || got[c] != "tag" {
		t.Fatalf("ParseObjectKinds = %v, %v", got, err)
	}
	for _, bad := range []string{"commit\n", "commit\nbogus\ntag\n", "commit\n" + c + " missing\ntag\n"} {
		if _, err := ParseObjectKinds([]string{a, b, c}, bad); err == nil {
			t.Fatalf("ParseObjectKinds(%q) = nil error, want a refusal", bad)
		}
	}
}

// The view points git at the cache repository and at the view directory, and at
// no alternate or quarantine that the caller's environment names.
func TestRegisteredPackViewEnvReplacesTheObjectVariables(t *testing.T) {
	v := &RegisteredPackView{dir: "/view", repoPath: "/cache/r.git"}
	env := v.Env([]string{"PATH=/bin", "GIT_DIR=.", "GIT_OBJECT_DIRECTORY=/q", "GIT_ALTERNATE_OBJECT_DIRECTORIES=/main", "GIT_QUARANTINE_PATH=/q", "GIT_COMMON_DIR=/c", "HOME=/h"})
	want := []string{"PATH=/bin", "HOME=/h", "GIT_DIR=/cache/r.git", "GIT_OBJECT_DIRECTORY=/view"}
	if !slices.Equal(env, want) {
		t.Fatalf("Env = %v, want %v", env, want)
	}
}
