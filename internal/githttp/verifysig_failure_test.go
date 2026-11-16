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

// verifyCommitGit puts a git first on PATH that runs body (sh) for
// verify-commit and the real git for everything else. Each verify-commit
// appends a line to a file first, and the returned func counts them.
func verifyCommitGit(t *testing.T, body string) (spawns func() int) {
	t.Helper()
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	count := filepath.Join(bin, "spawns")
	script := "#!/bin/sh\nfor a; do if [ \"$a\" = verify-commit ]; then echo spawn >> '" + count + "'; " + body + "; fi; done\nexec '" + realGit + "' \"$@\"\n"
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	return func() int {
		b, err := os.ReadFile(count)
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		return strings.Count(string(b), "spawn")
	}
}

// updateHookStderr runs the update hook for one ref creation and returns its
// stderr, which git relays to the pusher, and its error.
func updateHookStderr(t *testing.T, name, commit string) (stderr string, err error) {
	t.Helper()
	f, ferr := os.CreateTemp(t.TempDir(), "stderr")
	if ferr != nil {
		t.Fatal(ferr)
	}
	defer f.Close()
	saved := os.Stderr
	os.Stderr = f
	err = RunHook(t.Context(), []string{"update", name, ref.ZeroSHA, commit})
	os.Stderr = saved
	b, rerr := os.ReadFile(f.Name())
	if rerr != nil {
		t.Fatal(rerr)
	}
	return string(b), err
}

// Exit 1 is verify-commit's only verdict for a bad signature. Every other
// end is a check that did not finish: another exit status, or a kill (a
// timeout kills git too). Each ref gets the fixed denial, the cause and
// git's output reach only the hook-errors file, never the pusher's text or
// the hook's stderr, and the failure is cached: one spawn for two refs.
func TestVerifySigReportsEveryOtherEndAsAFailure(t *testing.T) {
	const canary = "canary-verify-output /srv/private/allowed_signers"
	for _, tc := range []struct{ name, end string }{
		{"exit 2", "exit 2"},
		{"exit 127", "exit 127"},
		{"exit 255", "exit 255"},
		{"killed", "kill -KILL $$"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			priv, pub := genSSHKey(t)
			records := signingKeyEnv(t, pub)
			commit := unverifiedCommit(t, sshSigningArgs(priv)...)
			spawns := verifyCommitGit(t, "echo '"+canary+"' >&2; "+tc.end)
			for _, name := range []string{"refs/heads/main", "refs/heads/other"} {
				stderr, err := updateHookStderr(t, name, commit)
				want := "verify-sig denied " + name + " at " + commit + ": signature check failed (the server log has the cause)"
				if err == nil || err.Error() != want {
					t.Fatalf("%s = %v; want %q", name, err, want)
				}
				if strings.Contains(stderr, canary) || strings.Contains(stderr, "git verify-commit") {
					t.Fatalf("%s: the hook's stderr has the cause: %q", name, stderr)
				}
			}
			got := readRecords(t, records)
			if strings.Count(got, "git verify-commit "+commit) != 1 || !strings.Contains(got, canary) {
				t.Fatalf("the hook-errors file does not have the cause and git's output exactly once: %s", got)
			}
			if n := spawns(); n != 1 {
				t.Fatalf("verify-commit ran %d times for two refs at one commit; want 1", n)
			}
		})
	}
}

// A failed check is cached under the commit that failed, not under the
// first commit the push introduced. The tip T is signed and verifies, and
// its parent G has a garbage gpgsig header, so git dies on G. A later ref
// at G gets the cached denial without a spawn.
func TestVerifySigCachesAFailureUnderTheFailingCommit(t *testing.T) {
	priv, pub := genSSHKey(t)
	records := signingKeyEnv(t, pub)
	signed := unverifiedCommit(t, sshSigningArgs(priv)...)
	tree := strings.TrimSpace(mustGit(t, "", "rev-parse", signed+"^{tree}"))
	cmd := exec.Command("git", "hash-object", "-t", "commit", "-w", "--stdin")
	cmd.Stdin = strings.NewReader("tree " + tree + "\nauthor a <a@b.invalid> 1700000000 +0000\ncommitter a <a@b.invalid> 1700000000 +0000\ngpgsig garbage\n\nmalformed signature\n")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("hash-object: %v", err)
	}
	garbage := strings.TrimSpace(string(out))
	out, err = exec.Command("git", append(append(append([]string{}, gitIdentity...), sshSigningArgs(priv)...), "commit-tree", "-S", tree, "-p", garbage, "-m", "tip")...).Output()
	if err != nil {
		t.Fatalf("commit-tree: %v", err)
	}
	tip := strings.TrimSpace(string(out))
	spawns := verifyCommitGit(t, ":")
	want := func(name string) string {
		return "verify-sig denied " + name + " at " + garbage + ": signature check failed (the server log has the cause)"
	}
	if _, err := updateHookStderr(t, "refs/heads/tip", tip); err == nil || err.Error() != want("refs/heads/tip") {
		t.Fatalf("refs/heads/tip = %v; want %q", err, want("refs/heads/tip"))
	}
	if n := spawns(); n != 2 {
		t.Fatalf("verify-commit ran %d times for the tip and its parent; want 2", n)
	}
	if _, err := updateHookStderr(t, "refs/heads/garbage", garbage); err == nil || err.Error() != want("refs/heads/garbage") {
		t.Fatalf("refs/heads/garbage = %v; want %q", err, want("refs/heads/garbage"))
	}
	if n := spawns(); n != 2 {
		t.Fatalf("a ref at the failed commit ran verify-commit again (%d spawns in all); want the cached denial", n)
	}
	if got := readRecords(t, records); strings.Count(got, "git verify-commit "+garbage) != 1 {
		t.Fatalf("the hook-errors file does not have the failed check exactly once: %s", got)
	}
}

// git verify-commit exits 1 and prints nothing for an unsigned commit: a
// verdict, with its own reason.
func TestVerifySigReportsAnUnsignedCommit(t *testing.T) {
	_, pub := genSSHKey(t)
	signingKeyEnv(t, pub)
	commit := unverifiedCommit(t)
	want := "verify-sig denied refs/heads/main at " + commit + ": commit is not signed"
	if err := RunHook(t.Context(), []string{"update", "refs/heads/main", ref.ZeroSHA, commit}); err == nil || err.Error() != want {
		t.Fatalf("update = %v; want %q", err, want)
	}
}
