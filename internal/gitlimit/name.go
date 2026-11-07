package gitlimit

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

const (
	// maxNameArgBytes bounds each argument that Name prints.
	maxNameArgBytes = 256
	// maxNameArgs bounds the number of arguments that Name prints.
	maxNameArgs = 32
)

// Name renders a git command line for an error message, and so for a log
// line. A request can put a value of 100 KB or more into an argument (a path,
// a pattern), and a command can carry any number of them. Name cuts each
// argument at maxNameArgBytes, at a character boundary, and says how long it
// was. It prints at most maxNameArgs arguments and counts the rest. A command
// line within both limits is printed as it is.
func Name(args []string) string {
	var b strings.Builder
	b.WriteString("git")
	for i, a := range args {
		if i == maxNameArgs {
			fmt.Fprintf(&b, " ...(%d more arguments)", len(args)-maxNameArgs)
			break
		}
		b.WriteByte(' ')
		if len(a) <= maxNameArgBytes {
			b.WriteString(a)
			continue
		}
		cut := maxNameArgBytes
		for cut > 0 && !utf8.RuneStart(a[cut]) {
			cut--
		}
		fmt.Fprintf(&b, "%s...(%d bytes)", a[:cut], len(a))
	}
	return b.String()
}
