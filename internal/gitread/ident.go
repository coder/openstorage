package gitread

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// maxIdentUnix is 9999-12-31T23:59:59Z, the last second RFC 3339 can write.
const maxIdentUnix = 253402300799

// identTime reads the Unix seconds git prints for an author or committer
// date: %at/%ct in git log, author-time/committer-time in git blame. git
// prints only digits, and %at/%ct print nothing when git cannot read a
// date from the line (no date, no time zone, no <email>). Then, and for a
// date past what RFC 3339 can write (year 10000 on, and every value that
// overflows), it returns the Unix epoch: `git log` shows the epoch for a
// date it cannot read or show, too (#581).
func identTime(s string) (time.Time, error) {
	epoch := time.Unix(0, 0).UTC()
	if s == "" {
		return epoch, nil
	}
	if strings.Trim(s, "0123456789") != "" {
		return time.Time{}, fmt.Errorf("git emitted date %.80q, not Unix seconds", s)
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		// Only digits remain, so only the range can fail.
		assertf(errors.Is(err, strconv.ErrRange), "identTime(%q): %v", s, err)
		return epoch, nil
	}
	if n > maxIdentUnix {
		return epoch, nil
	}
	return time.Unix(n, 0).UTC(), nil
}
