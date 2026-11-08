package gitlimit

import (
	"strings"
	"testing"
)

func TestHeadBufferKeepsTheFirst64KiB(t *testing.T) {
	var h HeadBuffer
	for _, s := range []string{"first ", strings.Repeat("x", 64<<10), "last"} {
		if n, err := h.Write([]byte(s)); n != len(s) || err != nil {
			t.Fatalf("Write = %d, %v, want %d, nil", n, err, len(s))
		}
	}
	if got := len(h.Bytes()); got != 64<<10 {
		t.Fatalf("kept %d bytes, want %d", got, 64<<10)
	}
	if !strings.HasPrefix(string(h.Bytes()), "first x") {
		t.Errorf("kept %.20q, want the head", h.Bytes())
	}
	if got, want := h.Dropped(), int64(len("first ")+4); got != want {
		t.Errorf("Dropped = %d, want %d", got, want)
	}
}
