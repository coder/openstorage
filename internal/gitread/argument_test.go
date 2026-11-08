package gitread

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// exec refuses an argument with a NUL byte and, on Linux, one past 128 KiB
// (E2BIG). Run reports both as ErrArgument, a client error, and starts no
// git for a NUL byte.
func TestRunRefusedArgumentIsErrArgument(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "ran")
	fakeGit(t, "echo x >> '"+marker+"'\nexit 1")
	run := func(arg string) error {
		t.Helper()
		_, err := Run(context.Background(), Cmd{Class: Light, Dir: t.TempDir(), Args: []string{"grep", "-e", arg}})
		return err
	}
	if err := run("a\x00b"); !errors.Is(err, ErrArgument) {
		t.Fatalf("NUL byte: err = %v, want ErrArgument", err)
	}
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("git ran for an argument with a NUL byte")
	}
	if err := run(strings.Repeat("a", 200<<10)); !errors.Is(err, ErrArgument) {
		t.Fatalf("200 KiB argument: err = %v, want ErrArgument", err)
	}
	// An argument exec takes reaches git, and git's own failure is not
	// ErrArgument.
	err := run(strings.Repeat("a", 64<<10))
	var exitErr *exec.ExitError
	if errors.Is(err, ErrArgument) || !errors.As(err, &exitErr) {
		t.Fatalf("64 KiB argument: err = %v, want git's exit status", err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatal("git did not run for a 64 KiB argument")
	}
}
