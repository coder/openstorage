package githttp

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/ref"
)

// A signature check that ends without a verdict is a failure, not a
// denial. Here git dies with 128 and prints nothing for verify-commit, as
// it can when the check cannot run. Each ref's update gets the fixed
// failure line, the hook-errors file gets the cause, and no denial is
// cached for the commit: the second ref runs the check again.
func TestVerifySigReportsAFailedCheckAsAFailure(t *testing.T) {
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	priv, pub := genSSHKey(t)
	records := signingKeyEnv(t, pub)
	commit := unverifiedCommit(t, sshSigningArgs(priv)...)
	bin := t.TempDir()
	script := "#!/bin/sh\nfor a; do [ \"$a\" = verify-commit ] && exit 128; done\nexec " + realGit + " \"$@\"\n"
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	for _, name := range []string{"refs/heads/main", "refs/heads/other"} {
		if err := RunHook(t.Context(), []string{"update", name, ref.ZeroSHA, commit}); err == nil || err.Error() != hookFailedLine {
			t.Fatalf("%s = %v; want %q", name, err, hookFailedLine)
		}
	}
	if got := readRecords(t, records); strings.Count(got, "git verify-commit "+commit) != 2 {
		t.Fatalf("the hook-errors file does not have the failed check once per ref: %s", got)
	}
}
