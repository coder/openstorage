package gitlimit

import "strings"

// stderrKeepBytes bounds the stderr that TailBuffer keeps (its tail: git
// prints its fatal message last).
const stderrKeepBytes = 64 << 10

// TailBuffer is a stderr sink that keeps only the last 64 KiB written to
// it: git prints its fatal message last, and the rest must not grow an
// error (and the log record that quotes it) without bound.
type TailBuffer struct{ b []byte }

func (t *TailBuffer) Write(p []byte) (int, error) {
	t.b = append(t.b, p...)
	if over := len(t.b) - stderrKeepBytes; over > 0 {
		t.b = append(t.b[:0], t.b[over:]...)
	}
	return len(p), nil
}

// String returns the kept tail without surrounding white space.
func (t *TailBuffer) String() string { return strings.TrimSpace(string(t.b)) }

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
