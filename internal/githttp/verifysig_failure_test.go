package githttp

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref"
)

// A signature check that ends without a verdict is not reported as one.
// Here git dies with 128 and prints nothing for verify-commit, as it does
// when the check cannot run or cannot parse the commit's signature. Each
// ref's update gets a denial whose reason names neither a signature fault
// nor the cause, the hook-errors file gets the cause, and the denial is
// cached: the second ref runs no second check.
func TestVerifySigReportsAFailedCheckWithoutAVerdict(t *testing.T) {
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
		want := "verify-sig denied " + name + " at " + commit + ": signature check failed (the server log has the cause)"
		if err := RunHook(t.Context(), []string{"update", name, ref.ZeroSHA, commit}); err == nil || err.Error() != want {
			t.Fatalf("%s = %v; want %q", name, err, want)
		}
	}
	if got := readRecords(t, records); strings.Count(got, "git verify-commit "+commit) != 1 {
		t.Fatalf("the hook-errors file does not have the failed check exactly once: %s", got)
	}
}

// A pusher can make verify-commit die (exit 128, "bad/incompatible
// signature") with a commit whose gpgsig header is garbage. The check's
// failure is cached for the push like a denial, so refs at that commit
// cost one git spawn in all, and the pusher's denial names no cause.
func TestVerifySigCachesAMalformedSignatureHeader(t *testing.T) {
	_, pub := genSSHKey(t)
	records := signingKeyEnv(t, pub)
	unverifiedCommit(t) // the repository, GIT_DIR and the verify state
	tree := strings.TrimSpace(mustGit(t, "", "mktree"))
	body := "tree " + tree + "\nauthor a <a@b.invalid> 1700000000 +0000\ncommitter a <a@b.invalid> 1700000000 +0000\ngpgsig garbage\n\nmalformed signature\n"
	cmd := exec.Command("git", "hash-object", "-t", "commit", "-w", "--stdin")
	cmd.Stdin = strings.NewReader(body)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("hash-object: %v", err)
	}
	commit := strings.TrimSpace(string(out))
	for _, name := range []string{"refs/heads/main", "refs/heads/other", "refs/heads/third"} {
		want := "verify-sig denied " + name + " at " + commit + ": signature check failed (the server log has the cause)"
		if err := RunHook(t.Context(), []string{"update", name, ref.ZeroSHA, commit}); err == nil || err.Error() != want {
			t.Fatalf("%s = %v; want %q", name, err, want)
		}
	}
	if got := readRecords(t, records); strings.Count(got, "git verify-commit "+commit) != 1 || !strings.Contains(got, "bad/incompatible signature") {
		t.Fatalf("the hook-errors file does not have the failed check's cause exactly once: %s", got)
	}
}

// A verify-commit that never started (here its context ended first) is no
// verdict either.
func TestVerifyCommitThatNeverRanIsAnError(t *testing.T) {
	_, pub := genSSHKey(t)
	v, cleanup, err := newSigVerifier([]meta.SigningKey{{ID: 1, KeyText: pub}})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if reason, err := v.verifyCommit(ctx, strings.Repeat("a", 40)); err == nil || reason != "" {
		t.Fatalf("verifyCommit = %q, %v; want no reason and an error", reason, err)
	}
}
