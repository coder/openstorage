package gitlimit

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

const (
	// stderrKeepBytes bounds the stderr that TailBuffer keeps (its tail:
	// git prints its fatal message last).
	stderrKeepBytes = 64 << 10
	// maxErrArgvBytes bounds the git command line that CommandLine quotes.
	// An argv can be request-sized (a grep pattern, a list of paths, a
	// commit message; past 128 KiB, exec fails with E2BIG), and the errors
	// that quote it reach the server's log. 4096 keeps a command with any
	// one path (PATH_MAX on Linux) whole.
	maxErrArgvBytes = 4096
)

// TailBuffer is a stderr sink that keeps only the last 64 KiB written to
// it: git prints its fatal message last, and the rest must not grow an
// error (and the log record that quotes it) without bound.
type TailBuffer struct{ b []byte }

func (t *TailBuffer) Write(p []byte) (int, error) {
	t.b = append(t.b, p...)
	if over := len(t.b) - stderrKeepBytes; over > 0 {
		t.b = append(t.b[:0], t.b[over:]...)
	}
	return len(p), nil
}

// String returns the kept tail without surrounding white space.
func (t *TailBuffer) String() string { return strings.TrimSpace(string(t.b)) }

// CommandLine is the git command line that an error quotes. Past
// maxErrArgvBytes it keeps the head, cut at a rune boundary, and the size.
func CommandLine(args []string) string {
	name := "git " + strings.Join(args, " ")
	if len(name) <= maxErrArgvBytes {
		return name
	}
	cut := maxErrArgvBytes
	for cut > maxErrArgvBytes-utf8.UTFMax && !utf8.RuneStart(name[cut]) {
		cut--
	}
	return fmt.Sprintf("%s... (%d bytes)", name[:cut], len(name))
}
