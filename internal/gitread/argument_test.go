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

// exec refuses an argument with a NUL byte, and with E2BIG one past
// 128 KiB on Linux or past the 1 MiB total on macOS. Run reports both as
// ErrArgument, a client error, and starts no git for a NUL byte.
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
	// 2 MiB passes Linux's 128 KiB for one argument and macOS's 1 MiB for
	// all of them.
	if err := run(strings.Repeat("a", 2<<20)); !errors.Is(err, ErrArgument) {
		t.Fatalf("2 MiB argument: err = %v, want ErrArgument", err)
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
