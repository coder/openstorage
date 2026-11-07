package gitlimit

import (
	"bytes"
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

// TailBuffer keeps exactly the last 64 KiB written to it, however the
// writes are cut: git prints its fatal message last.
func TestTailBufferKeepsTheLast64KiB(t *testing.T) {
	var all bytes.Buffer
	var tail TailBuffer
	for i := range 40 {
		chunk := bytes.Repeat([]byte{byte('a' + i%26)}, 5000+i)
		all.Write(chunk)
		if n, err := tail.Write(chunk); n != len(chunk) || err != nil {
			t.Fatalf("Write = %d, %v, want %d, nil", n, err, len(chunk))
		}
	}
	want := all.Bytes()[all.Len()-64<<10:]
	if got := tail.String(); got != string(want) {
		t.Fatalf("kept %d bytes, want the last %d of %d", len(got), len(want), all.Len())
	}
}
