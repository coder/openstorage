package gitlimit

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

const (
	// maxNameArgBytes bounds each argument that Name prints, and each value that
	// Clip returns.
	maxNameArgBytes = 256
	// maxNameArgs bounds the number of arguments that Name prints.
	maxNameArgs = 32
)

// Name renders a git command line for an error message, and so for a log
// line. A request can put a value of 100 KB or more into an argument (a path,
// a pattern), and a command can carry any number of them. Name clips each
// argument (see Clip). It prints at most maxNameArgs arguments and counts the
// rest. A command line within both limits is printed as it is.
func Name(args []string) string {
	var b strings.Builder
	b.WriteString("git")
	for i, a := range args {
		if i == maxNameArgs {
			fmt.Fprintf(&b, " ...(%d more)", len(args)-maxNameArgs)
			break
		}
		b.WriteByte(' ')
		b.WriteString(Clip(a))
	}
	return b.String()
}

// Clip bounds a value that goes into an error message, and so into a log line.
// A value of at most maxNameArgBytes is returned as it is. A longer one is cut
// at maxNameArgBytes, at a character boundary, and followed by its length.
func Clip(s string) string {
	if len(s) <= maxNameArgBytes {
		return s
	}
	cut := maxNameArgBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return fmt.Sprintf("%s...(%d bytes)", s[:cut], len(s))
}
