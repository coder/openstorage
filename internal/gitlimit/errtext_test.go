package gitlimit

import (
	"bytes"
	"strings"
	"testing"
	"unicode/utf8"
)

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

// TailBuffer holds about 64 KiB however much git writes: 64 MiB in one
// Write, or in 4 KiB writes.
func TestTailBufferHoldsAbout64KiB(t *testing.T) {
	big := bytes.Repeat([]byte("e"), 64<<20)
	copy(big[len(big)-len("the end"):], "the end")
	for _, size := range []int{len(big), 4 << 10} {
		var tail TailBuffer
		for p := big; len(p) > 0; {
			n := min(size, len(p))
			_, _ = tail.Write(p[:n])
			p = p[n:]
		}
		if c := cap(tail.b); c > 2*stderrKeepBytes {
			t.Errorf("writes of %d bytes: the buffer holds %d bytes, want at most %d", size, c, 2*stderrKeepBytes)
		}
		if got := tail.String(); len(got) != stderrKeepBytes || !strings.HasSuffix(got, "the end") {
			t.Errorf("writes of %d bytes: kept %d bytes ending %q", size, len(got), got[max(len(got)-8, 0):])
		}
	}
}

// A cut inside a UTF-8 sequence drops its continuation bytes, so the tail
// starts at a character. A tail that was not cut keeps git's bytes as
// they are.
func TestTailBufferStartsAtACharacter(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		// 90000 bytes of 3-byte runes: the cut keeps 65536 bytes, the
		// first of them the last byte of a rune.
		{strings.Repeat("€", 30000), strings.Repeat("€", (64<<10)/3)},
		// 80000 bytes of 4-byte runes, then one ASCII byte: the cut keeps
		// 65536 bytes, the first three of them the end of a rune, which
		// is the most a cut can leave.
		{strings.Repeat("𝄞", 20000) + "a", strings.Repeat("𝄞", (64<<10)/4-1) + "a"},
	} {
		var tail TailBuffer
		_, _ = tail.Write([]byte(c.in))
		got := tail.String()
		if !utf8.ValidString(got) || got != c.want {
			t.Fatalf("tail starts %q and holds %d bytes, want %d bytes of whole runes", got[:min(4, len(got))], len(got), len(c.want))
		}
	}
	var uncut TailBuffer
	_, _ = uncut.Write([]byte("\xa9git"))
	if got := uncut.String(); got != "\xa9git" {
		t.Fatalf("uncut tail = %q, want git's bytes", got)
	}
}
