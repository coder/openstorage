package gitlimit

import (
	"errors"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
)

// TestStartReportsArgumentsTooLong: an argument or an environment variable
// of 32 pages (128 KiB with 4 KiB pages) or more cannot be exec'd on Linux.
// Start answers ErrArgTooLong and starts nothing, and an argument just under
// the limit still runs. The total limit of all arguments (a quarter of the
// stack, 2 MiB) is then far above one argument, as it is with 4 KiB pages.
func TestStartReportsArgumentsTooLong(t *testing.T) {
	if runtime.GOOS != "linux" || os.Getpagesize() != 4096 {
		t.Skip("one argument is limited to 128 KiB only on Linux with 4 KiB pages")
	}
	const limit = 128 << 10 // MAX_ARG_STRLEN, with its terminating NUL
	for name, build := range map[string]func(n int) *exec.Cmd{
		"argument": func(n int) *exec.Cmd { return exec.Command("git", "version", strings.Repeat("a", n)) },
		"variable": func(n int) *exec.Cmd {
			cmd := exec.Command("git", "version")
			cmd.Env = append(cmd.Environ(), "X="+strings.Repeat("a", n-2))
			return cmd
		},
	} {
		t.Run(name, func(t *testing.T) {
			g := &registry{}
			for _, n := range []int{limit, limit + 1, 4 * limit} {
				cmd := build(n)
				if err := g.start(cmd); !errors.Is(err, ErrArgTooLong) {
					t.Errorf("%d bytes: start = %v, want ErrArgTooLong", n, err)
				}
				if cmd.Process != nil || len(g.groups) != 0 {
					t.Errorf("%d bytes: a child was started or tracked", n)
				}
			}
			cmd := build(limit - 1)
			if err := g.start(cmd); err != nil {
				t.Fatalf("%d bytes: start = %v, want it to run", limit-1, err)
			}
			if err := g.wait(cmd); err != nil {
				t.Errorf("%d bytes: wait = %v", limit-1, err)
			}
		})
	}
}

// TestStartRefusesNUL: an argument or an environment variable with a NUL
// byte cannot be exec'd. Start answers ErrArgNUL, on any platform, and
// starts nothing.
func TestStartRefusesNUL(t *testing.T) {
	g := &registry{}
	for name, cmd := range map[string]*exec.Cmd{
		"argument": exec.Command("git", "version", "a\x00b"),
		"variable": func() *exec.Cmd {
			cmd := exec.Command("git", "version")
			cmd.Env = []string{"X=a\x00b"}
			return cmd
		}(),
	} {
		if err := g.start(cmd); !errors.Is(err, ErrArgNUL) {
			t.Errorf("%s: start = %v, want ErrArgNUL", name, err)
		}
		if cmd.Process != nil || len(g.groups) != 0 {
			t.Errorf("%s: a child was started or tracked", name)
		}
	}
}
