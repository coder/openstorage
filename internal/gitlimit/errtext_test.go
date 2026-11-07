package gitlimit

import (
	"bytes"
	"testing"
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
