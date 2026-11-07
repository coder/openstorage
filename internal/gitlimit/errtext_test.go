package gitlimit

import (
	"fmt"
	"strings"
	"testing"
)

// CommandLine quotes a command of up to 4096 bytes whole, so a command
// with one PATH_MAX path stays readable, and cuts a longer one at a rune
// boundary, at most 3 bytes before the cap, with the full size after it.
func TestCommandLineCut(t *testing.T) {
	const head = "git grep -e "
	for _, tc := range []struct {
		name string
		arg  string
		kept int // bytes of the command kept; 0 keeps it whole
	}{
		{"4096 bytes", strings.Repeat("a", 4096-len(head)), 0},
		{"4097 bytes", strings.Repeat("a", 4097-len(head)), 4096},
		{"3-byte rune across the cut", strings.Repeat("a", 4094-len(head)) + "€€", 4094},
		{"4-byte rune across the cut", strings.Repeat("a", 4093-len(head)) + "😀😀", 4093},
	} {
		name := head + tc.arg
		want := name
		if tc.kept > 0 {
			want = fmt.Sprintf("%s... (%d bytes)", name[:tc.kept], len(name))
		}
		if got := CommandLine([]string{"grep", "-e", tc.arg}); got != want {
			t.Errorf("%s: CommandLine kept %d bytes, want %d (%.40q)", tc.name, strings.Index(got, "... ("), tc.kept, got[max(0, len(got)-40):])
		}
	}
}
