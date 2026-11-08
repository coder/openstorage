package githttp

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// noisyGit puts a git first on PATH that prints 1 MiB and then a fatal
// line to stderr, and exits 1.
func noisyGit(t *testing.T) {
	t.Helper()
	bin := t.TempDir()
	script := "#!/bin/sh\nPATH=/usr/bin:/bin\nhead -c 1048576 /dev/zero | tr '\\0' e >&2\necho 'fatal: the end' >&2\nexit 1\n"
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// requireStderrTail checks that err quotes only the tail of git's stderr:
// the errors of git runners reach the log.
func requireStderrTail(t *testing.T, name string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: nil error from a git that exits 1", name)
	}
	if n := len(err.Error()); n > 64<<10+1024 {
		t.Errorf("%s: the error is %d bytes, want git's stderr cut to its last 64 KiB", name, n)
	}
	if !strings.Contains(err.Error(), "fatal: the end") {
		t.Errorf("%s: the error lost the end of git's stderr", name)
	}
}

func TestHydrateGitErrorsKeepStderrTail(t *testing.T) {
	dir := t.TempDir()
	noisyGit(t)
	_, err := runGitCtx(context.Background(), dir, "status")
	requireStderrTail(t, "runGitCtx", err)
	requireStderrTail(t, "updateRefStdin", updateRefStdin(dir, "verify refs/heads/x\n"))
	l, err := startLocalRefSeq(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.close()
	_, _, ok, err := l.next()
	if ok {
		t.Fatal("localRefSeq: a git that exits 1 listed a ref")
	}
	requireStderrTail(t, "localRefSeq", err)
}

func TestCommitGraphErrorKeepsStderrTail(t *testing.T) {
	noisyGit(t)
	err := runCommitGraphAttempt(context.Background(), t.TempDir(), 0, []string{"commit-graph", "write"}, "")
	requireStderrTail(t, "runCommitGraphAttempt", err)
}

// git verify-commit prints the verifier's reason first. A verifier that
// goes on for 64 MiB leaves the head of its output in the error, with the
// count of the bytes past the kept 64 KiB.
func TestVerifyCommitKeepsTheHeadOfItsOutput(t *testing.T) {
	bin := t.TempDir()
	script := "#!/bin/sh\nPATH=/usr/bin:/bin\necho 'gpg: the reason' >&2\nhead -c 67108864 /dev/zero | tr '\\0' e >&2\nexit 128\n"
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	v := &sigVerifier{allowedSigners: filepath.Join(t.TempDir(), "allowed_signers")}
	_, err := v.verifyCommit(context.Background(), strings.Repeat("a", 40))
	if err == nil {
		t.Fatal("verifyCommit: nil error from a git that exits 128")
	}
	msg := err.Error()
	if len(msg) > 4096 {
		t.Errorf("the error is %d bytes, want the verifier's output cut", len(msg))
	}
	// 16 bytes of "gpg: the reason\n" and the first 65520 of the 64 MiB.
	if !strings.Contains(msg, "gpg: the reason") || !strings.Contains(msg, "(67043344 more bytes dropped)") {
		t.Errorf("the error lost the head of the output or the dropped count: %.400s", msg)
	}
}
