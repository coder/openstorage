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
