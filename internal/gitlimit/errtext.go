package gitlimit

import (
	"strings"
	"unicode/utf8"
)

// stderrKeepBytes bounds the stderr that TailBuffer keeps (its tail: git
// prints its fatal message last).
const stderrKeepBytes = 64 << 10

// TailBuffer is a stderr sink that keeps only the last 64 KiB written to
// it: git prints its fatal message last, and the rest must not grow an
// error (and the log record that quotes it) without bound. It holds at
// most twice that much memory, however much is written, also in one
// Write, and it copies each written byte a bounded number of times: git
// writes stderr a message at a time, and a slow git gives one pipe read
// per line.
type TailBuffer struct {
	b   []byte // the last bytes written, at most 2 × stderrKeepBytes; the tail is its last stderrKeepBytes
	cut bool   // bytes before b were dropped
}

func (t *TailBuffer) Write(p []byte) (int, error) {
	n := len(p)
	if n >= stderrKeepBytes {
		t.cut = t.cut || len(t.b) > 0 || n > stderrKeepBytes
		t.b = append(t.b[:0], p[n-stderrKeepBytes:]...)
		return n, nil
	}
	switch {
	case len(t.b)+n > 2*stderrKeepBytes:
		// Move the bytes that p leaves in the tail to the front. The
		// buffer takes stderrKeepBytes more before the next move.
		t.cut = true
		t.b = append(t.b[:0], t.b[len(t.b)-(stderrKeepBytes-n):]...)
	case len(t.b)+n > cap(t.b):
		// Grow, at most to 2 × stderrKeepBytes.
		t.b = append(make([]byte, 0, min(2*stderrKeepBytes, max(2*cap(t.b), len(t.b)+n))), t.b...)
	}
	t.b = append(t.b, p...)
	return n, nil
}

// String returns the kept tail without surrounding white space. A tail
// that was cut starts at a character: the cut can land inside a UTF-8
// sequence, and its continuation bytes are dropped.
func (t *TailBuffer) String() string {
	return strings.TrimSpace(string(t.kept()))
}

// kept is the kept tail as written, from its first character if it was
// cut.
func (t *TailBuffer) kept() []byte {
	b, cut := t.b, t.cut
	if len(b) > stderrKeepBytes {
		b, cut = b[len(b)-stderrKeepBytes:], true
	}
	for i := 0; cut && i < utf8.UTFMax-1 && len(b) > 0 && !utf8.RuneStart(b[0]); i++ {
		b = b[1:]
	}
	return b
}

// HeadBuffer is an output sink that keeps only the first 64 KiB written to
// it, for a command whose reason comes first, and counts the bytes it
// drops after that.
type HeadBuffer struct {
	b       []byte
	dropped int64
}

func (h *HeadBuffer) Write(p []byte) (int, error) {
	n := min(max(stderrKeepBytes-len(h.b), 0), len(p))
	h.b = append(h.b, p[:n]...)
	h.dropped += int64(len(p) - n)
	return len(p), nil
}

// Bytes returns the kept head.
func (h *HeadBuffer) Bytes() []byte { return h.b }

// Dropped returns the count of bytes written past the kept head.
func (h *HeadBuffer) Dropped() int64 { return h.dropped }
