package gitlimit

import (
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestName(t *testing.T) {
	exact := strings.Repeat("a", maxNameArgBytes)
	over := strings.Repeat("b", maxNameArgBytes+1)
	huge := strings.Repeat("c", 200_000)
	many := make([]string, 100)
	for i := range many {
		many[i] = "p" + strconv.Itoa(i)
	}
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"short", []string{"log", "-1", "--", "a b"}, "git log -1 -- a b"},
		{"empty argument", []string{"cat-file", ""}, "git cat-file "},
		{"an argument at the limit is printed whole", []string{"x", exact}, "git x " + exact},
		{"an argument over the limit is cut and counted", []string{"x", over}, "git x " + over[:maxNameArgBytes] + "...(" + strconv.Itoa(maxNameArgBytes+1) + " bytes)"},
		{"a huge argument", []string{"grep", huge, "--"}, "git grep " + huge[:maxNameArgBytes] + "...(200000 bytes) --"},
		{"the arguments at the limit are all printed", many[:maxNameArgs], "git " + strings.Join(many[:maxNameArgs], " ")},
		{"more arguments are counted", many, "git " + strings.Join(many[:maxNameArgs], " ") + " ...(68 more arguments)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Name(tc.args); got != tc.want {
				t.Errorf("Name = %.300q, want %.300q", got, tc.want)
			}
		})
	}
}

// A cut inside a multibyte character moves back to its start, so the name
// stays valid UTF-8 and a log line is never garbled.
func TestNameCutsAtACharacterBoundary(t *testing.T) {
	for _, r := range []string{"é", "世", "😀"} {
		for pad := range utf8.UTFMax {
			arg := strings.Repeat("a", maxNameArgBytes-pad) + strings.Repeat(r, 4)
			got := Name([]string{arg})
			if !utf8.ValidString(got) {
				t.Errorf("%q after %d bytes: name is not valid UTF-8", r, maxNameArgBytes-pad)
			}
			if want := "..." + "(" + strconv.Itoa(len(arg)) + " bytes)"; !strings.HasSuffix(got, want) {
				t.Errorf("%q after %d bytes: name %.60q does not end with %q", r, maxNameArgBytes-pad, got, want)
			}
		}
	}
}

// Whatever the arguments, the name has a size that does not depend on them.
func TestNameIsBounded(t *testing.T) {
	args := make([]string, 5_000)
	for i := range args {
		args[i] = strings.Repeat("z", 200_000)
	}
	const bound = (maxNameArgs+1)*(maxNameArgBytes+40) + 4
	if got := Name(args); len(got) > bound {
		t.Fatalf("name of %d arguments of 200,000 bytes is %d bytes, want at most %d", len(args), len(got), bound)
	}
}
