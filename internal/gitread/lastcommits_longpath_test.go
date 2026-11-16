package gitread

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/gitlimit"
)

// lcGitStdin runs git in dir with stdin and the hermetic fixture identity,
// and returns its trimmed stdout.
func lcGitStdin(t *testing.T, dir, stdin string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir, cmd.Stdin = dir, strings.NewReader(stdin)
	cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null",
		"GIT_AUTHOR_NAME=lc", "GIT_AUTHOR_EMAIL=lc@test.invalid", "GIT_COMMITTER_NAME=lc", "GIT_COMMITTER_EMAIL=lc@test.invalid")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git %s: %v", strings.Join(args, " "), err)
	}
	return strings.TrimSpace(string(out))
}

// TestLastCommitsOfPathsTooLongForAnArgument asks for the last commit of a
// file whose path is as long as one pathspec argument takes, one byte
// longer, and much longer. The pathspec that narrows diff-tree to it is
// left out when it does not fit, and the answer is the same.
func TestLastCommitsOfPathsTooLongForAnArgument(t *testing.T) {
	dir := t.TempDir()
	lcGitStdin(t, dir, "", "init", "-q", "-b", "main", ".")
	blob := lcGitStdin(t, dir, "x\n", "hash-object", "-w", "--stdin")
	longest := gitlimit.MaxArgBytes - len(":(literal)")
	for _, size := range []int{longest, longest + 1, 200_000} {
		name := strings.Repeat("d", size-len("/f.txt")) // the file's whole path is size bytes
		inner := lcGitStdin(t, dir, "100644 blob "+blob+"\tf.txt\x00", "mktree", "-z")
		tree := lcGitStdin(t, dir, "040000 tree "+inner+"\t"+name+"\x00", "mktree", "-z")
		commit := lcGitStdin(t, dir, "c\n", "commit-tree", "-F", "-", tree)
		path := name + "/f.txt"
		last, _, err := LastCommits(t.Context(), dir+"/.git", commit, []string{path}, 10)
		if err != nil {
			t.Fatalf("a %d-byte path: %v", len(path), err)
		}
		if len(last) != 1 || last[0] != commit {
			t.Fatalf("a %d-byte path: last commits %v, want [%s]", len(path), last, commit)
		}
	}
}
