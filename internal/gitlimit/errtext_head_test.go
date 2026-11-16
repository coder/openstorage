package gitlimit

import (
	"bytes"
	"strings"
	"testing"
)

// HeadBuffer keeps the first 64 KiB, also from the Write that crosses the
// limit (here varied bytes, so that its tail cannot stand in for its head),
// and counts the rest.
func TestHeadBufferKeepsTheFirst64KiB(t *testing.T) {
	var h HeadBuffer
	middle := make([]byte, 64<<10)
	for i := range middle {
		middle[i] = byte('a' + i%26)
	}
	var all bytes.Buffer
	for _, s := range []string{"first ", string(middle), "last"} {
		all.WriteString(s)
		if n, err := h.Write([]byte(s)); n != len(s) || err != nil {
			t.Fatalf("Write = %d, %v, want %d, nil", n, err, len(s))
		}
	}
	if got := len(h.Bytes()); got != 64<<10 {
		t.Fatalf("kept %d bytes, want %d", got, 64<<10)
	}
	if !bytes.Equal(h.Bytes(), all.Bytes()[:64<<10]) {
		t.Errorf("kept %.20q, want the first 64 KiB written", h.Bytes())
	}
	if got, want := h.Dropped(), int64(len("first ")+4); got != want {
		t.Errorf("Dropped = %d, want %d", got, want)
	}
}

// HeadBuffer holds about 64 KiB however much is written: 64 MiB in one
// Write, or in 4 KiB writes.
func TestHeadBufferHoldsAbout64KiB(t *testing.T) {
	big := bytes.Repeat([]byte("e"), 64<<20)
	copy(big, "the start")
	for _, size := range []int{len(big), 4 << 10} {
		var h HeadBuffer
		for p := big; len(p) > 0; {
			n := min(size, len(p))
			_, _ = h.Write(p[:n])
			p = p[n:]
		}
		if c := cap(h.b); c > 2*stderrKeepBytes {
			t.Errorf("writes of %d bytes: the buffer holds %d bytes, want at most %d", size, c, 2*stderrKeepBytes)
		}
		if got := h.Bytes(); len(got) != stderrKeepBytes || !strings.HasPrefix(string(got), "the start") {
			t.Errorf("writes of %d bytes: kept %d bytes starting %.12q", size, len(got), got)
		}
		if got, want := h.Dropped(), int64(len(big)-stderrKeepBytes); got != want {
			t.Errorf("writes of %d bytes: Dropped = %d, want %d", size, got, want)
		}
	}
}
