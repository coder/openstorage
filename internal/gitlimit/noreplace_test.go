package gitlimit

import (
	"os/exec"
	"strings"
	"testing"
)

// Any token with git:write can push refs/replace/<id>. A git child that
// the registry starts reads each object as itself.
func TestStartReadsObjectsWithoutReplacements(t *testing.T) {
	dir := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", dir, "-c", "user.name=a", "-c", "user.email=a@b"}, args...)...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q", "-b", "main")
	git("commit", "-q", "--allow-empty", "-m", "c1")
	c1 := git("rev-parse", "HEAD")
	git("commit", "-q", "--allow-empty", "-m", "c2")
	c2 := git("rev-parse", "HEAD")
	git("update-ref", "refs/replace/"+c1, c2)
	if got := git("cat-file", "-p", c1); !strings.Contains(got, "\n\nc2") {
		t.Fatalf("the replacement does not show without the registry: %q", got)
	}
	out, err := Output(exec.Command("git", "-C", dir, "cat-file", "-p", c1))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(out); !strings.Contains(got, "\n\nc1") {
		t.Fatalf("cat-file -p c1 through the registry = %q, want c1's own message", got)
	}
}
