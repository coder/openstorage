package gitread

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/ref"
)

// RegisteredClosure decides from the objects that git can read, which
// `rev-list --missing=print` prints as "?<id>" lines, and from the exit status
// 128 with which older git dies on a commit that it cannot read, and from
// nothing else: any other outcome of git is an error, never a refusal. A
// stand-in git shows each outcome.
func TestRegisteredClosureAnswersFromTheMissingObjectsAlone(t *testing.T) {
	sha := strings.Repeat("a", 40)
	st, err := ref.Open(t.Context(), "sqlite://"+filepath.Join(t.TempDir(), "refs.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	if err := st.InitSchema(t.Context()); err != nil {
		t.Fatal(err)
	}
	repoPath := filepath.Join(t.TempDir(), "r.git")
	if err := os.MkdirAll(filepath.Join(repoPath, "objects", "pack"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, script string
		want         bool
		wantErr      string // text of the error, "" for none
	}{
		{"nothing missing", "exit 0", true, ""},
		{"an object is missing", "echo '?" + sha + "'; exit 0", false, ""},
		{"a missing object and a failed walk", "echo '?" + sha + "'; exit 128", false, ""},
		{"output that is no missing object", "echo 'deadbeef'; exit 0", false, "something other than a missing object"},
		{"git cannot walk the history (exit 128)", "echo 'fatal: ...' >&2; exit 128", false, ""},
		{"git fails another way (exit 129)", "echo 'usage: git rev-list' >&2; exit 129", false, "exit status 129"},
		{"git is killed", "kill -9 $$", false, "killed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bin := t.TempDir()
			// rev-list is the walk. The stand-in answers the other git commands with exit 0.
			script := "#!/bin/sh\ncase \"$*\" in *cat-file*) echo commit ;; *rev-list*) " + tc.script + " ;; esac\nexit 0\n"
			if err := os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0o755); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
			got, err := RegisteredClosure(t.Context(), st, nil, repoPath, "repo_x", sha)
			if tc.wantErr == "" && err != nil || tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("RegisteredClosure error = %v, want %q", err, tc.wantErr)
			}
			if err == nil && got != tc.want {
				t.Fatalf("RegisteredClosure = %v, want %v", got, tc.want)
			}
			if tc.name == "a missing object and a failed walk" && err != nil {
				t.Fatalf("a missing object was printed before the walk failed: %v", err)
			}
		})
	}
}

// A walk that outlasts its budget is ErrTimeBudget, which the handlers answer
// with 503, and never a refusal. A walk that the caller gave up is the caller's
// own error.
func TestRegisteredClosureWalkBudget(t *testing.T) {
	sha := strings.Repeat("a", 40)
	st, err := ref.Open(t.Context(), "sqlite://"+filepath.Join(t.TempDir(), "refs.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	if err := st.InitSchema(t.Context()); err != nil {
		t.Fatal(err)
	}
	repoPath := filepath.Join(t.TempDir(), "r.git")
	if err := os.MkdirAll(filepath.Join(repoPath, "objects", "pack"), 0o755); err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte("#!/bin/sh\ncase \"$*\" in *cat-file*) echo commit ;; *rev-list*) sleep 30 ;; esac\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	prev := registeredWalkTimeout
	registeredWalkTimeout = 200 * time.Millisecond
	t.Cleanup(func() { registeredWalkTimeout = prev })

	start := time.Now()
	if _, err := RegisteredClosure(t.Context(), st, nil, repoPath, "repo_x", sha); !errors.Is(err, ErrTimeBudget) {
		t.Fatalf("a walk past its budget: %v, want ErrTimeBudget", err)
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("the walk was not stopped at its budget: %v", d)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := RegisteredClosure(ctx, st, nil, repoPath, "repo_x", sha); errors.Is(err, ErrTimeBudget) || err == nil {
		t.Fatalf("a caller that gave up: %v, want its own error, not ErrTimeBudget", err)
	}
}

// A walk that is left to its own budget must end within the time that the
// documentation promises (docs/reference/http-api.md, "Which commit the base and
// the tag target can name").
func TestRegisteredWalkTimeoutIsThirtySeconds(t *testing.T) {
	if registeredWalkTimeout != 30*time.Second {
		t.Fatalf("registeredWalkTimeout = %v, want 30s as documented", registeredWalkTimeout)
	}
}

// The walk is git's connectivity check, over the registered packs alone and
// with no commit-graph: a commit-graph can describe a commit that no registered
// pack holds, and git then walks past it.
func TestRegisteredClosureRunsGitsConnectivityCheckOverTheView(t *testing.T) {
	sha := strings.Repeat("b", 40)
	st, err := ref.Open(t.Context(), "sqlite://"+filepath.Join(t.TempDir(), "refs.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	if err := st.InitSchema(t.Context()); err != nil {
		t.Fatal(err)
	}
	repoPath := filepath.Join(t.TempDir(), "r.git")
	if err := os.MkdirAll(filepath.Join(repoPath, "objects", "pack"), 0o755); err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	log := filepath.Join(bin, "log")
	script := "#!/bin/sh\ncase \"$*\" in *cat-file*) echo commit ;; *rev-list*) echo \"$*\" >> " + log + "\n" +
		"echo \"GIT_DIR=$GIT_DIR\" >> " + log + "\necho \"OBJECTS=$GIT_OBJECT_DIRECTORY\" >> " + log + "\n" +
		"echo \"ALT=$GIT_ALTERNATE_OBJECT_DIRECTORIES\" >> " + log + " ;; esac\nexit 0\n"
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("GIT_ALTERNATE_OBJECT_DIRECTORIES", "/elsewhere")
	if ok, err := RegisteredClosure(t.Context(), st, nil, repoPath, "repo_x", sha); err != nil || !ok {
		t.Fatalf("RegisteredClosure = %v, %v, want true", ok, err)
	}
	b, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != 4 {
		t.Fatalf("the stand-in logged %q, want the walk once", lines)
	}
	if want := "-c core.commitGraph=false rev-list --objects --missing=print --quiet " + sha + " --not --all"; lines[0] != want {
		t.Fatalf("git ran %q, want %q", lines[0], want)
	}
	if lines[1] != "GIT_DIR="+repoPath {
		t.Fatalf("the walk reads the refs of %q, want %q", lines[1], repoPath)
	}
	if objects := strings.TrimPrefix(lines[2], "OBJECTS="); objects == "" || objects == filepath.Join(repoPath, "objects") {
		t.Fatalf("the walk reads objects from %q, want a view of the registered packs, not the cache's own", objects)
	}
	if lines[3] != "ALT=" {
		t.Fatalf("the walk keeps the alternates %q, want none", lines[3])
	}
}

// git before 2.45 dies on a start object that it cannot read, whatever
// --missing says, so the walk asks for the object first: an object that no
// registered pack holds is missing without a walk.
func TestRegisteredClosureAsksForTheObjectBeforeTheWalk(t *testing.T) {
	sha := strings.Repeat("c", 40)
	st, err := ref.Open(t.Context(), "sqlite://"+filepath.Join(t.TempDir(), "refs.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	if err := st.InitSchema(t.Context()); err != nil {
		t.Fatal(err)
	}
	repoPath := filepath.Join(t.TempDir(), "r.git")
	if err := os.MkdirAll(filepath.Join(repoPath, "objects", "pack"), 0o755); err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	log := filepath.Join(bin, "log")
	script := "#!/bin/sh\necho \"$1\" >> " + log + "\ncase \"$*\" in *cat-file*) cat >/dev/null; echo '" + sha + " missing' ;; *rev-list*) echo 'fatal: bad object' >&2; exit 128 ;; esac\nexit 0\n"
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	if ok, err := RegisteredClosure(t.Context(), st, nil, repoPath, "repo_x", sha); err != nil || ok {
		t.Fatalf("RegisteredClosure = %v, %v, want false and no error", ok, err)
	}
	b, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Fields(string(b)); len(got) != 1 || got[0] != "cat-file" {
		t.Fatalf("git ran %q, want one cat-file and no walk", got)
	}
}
