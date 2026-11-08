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
// starts at a character, whether one Write or many smaller ones (exec
// writes 32 KiB at a time) bring the cut. A tail that was not cut keeps
// git's bytes as they are.
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
		for _, size := range []int{len(c.in), 32 << 10, 1000} {
			var tail TailBuffer
			for p := []byte(c.in); len(p) > 0; {
				n := min(size, len(p))
				_, _ = tail.Write(p[:n])
				p = p[n:]
			}
			got := tail.String()
			if !utf8.ValidString(got) || got != c.want {
				t.Fatalf("writes of %d bytes: tail starts %q and holds %d bytes, want %d bytes of whole runes", size, got[:min(4, len(got))], len(got), len(c.want))
			}
		}
	}
	var uncut TailBuffer
	_, _ = uncut.Write([]byte("\xa9git"))
	if got := uncut.String(); got != "\xa9git" {
		t.Fatalf("uncut tail = %q, want git's bytes", got)
	}
}

// One Write of exactly 64 KiB into an empty buffer drops nothing, so its
// bytes stay as git wrote them. After earlier bytes, the same Write drops
// them: that is a cut, and the tail starts at the next character.
func TestTailBufferExactFit(t *testing.T) {
	fit := "\x82\xac" + strings.Repeat("a", 64<<10-2)
	var only TailBuffer
	_, _ = only.Write([]byte(fit))
	if got := only.String(); got != fit {
		t.Errorf("one Write of 64 KiB: kept %q... (%d bytes), want git's bytes", got[:min(4, len(got))], len(got))
	}
	var after TailBuffer
	_, _ = after.Write([]byte("\xe2"))
	_, _ = after.Write([]byte(fit))
	if got := after.String(); got != fit[2:] {
		t.Errorf("64 KiB after one byte: kept %q... (%d bytes), want the tail from its first character", got[:min(4, len(got))], len(got))
	}
}

// Write keeps no reference to p (the io.Writer contract): exec reuses its
// buffer for the next read, and a caller can reuse its own.
func TestTailBufferCopiesWhatItKeeps(t *testing.T) {
	for _, size := range []int{70000, 1000} {
		p := bytes.Repeat([]byte("a"), size)
		var tail TailBuffer
		_, _ = tail.Write(p)
		for i := range p {
			p[i] = 'z'
		}
		if got := tail.String(); got != strings.Repeat("a", min(size, 64<<10)) {
			t.Errorf("a Write of %d bytes: the tail changed with the caller's array to %.8q...", size, got)
		}
	}
}
