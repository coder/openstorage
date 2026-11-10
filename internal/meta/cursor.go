package meta

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Cursor wire format: base64url (unpadded) of "<created_at_unixnano>|<repo_id>".
// It points AT the last row of the previous page; the next page is every row
// strictly below it in (created_at DESC, repo_id DESC) order.

// encodeCursor renders the keyset position of a row as an opaque cursor.
func encodeCursor(createdAt time.Time, repoID string) string {
	assertf(repoID != "", "encodeCursor: empty repo id")
	assertf(!createdAt.IsZero(), "encodeCursor: zero created_at for repo %q", repoID)
	raw := strconv.FormatInt(createdAt.UnixNano(), 10) + "|" + repoID
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// decodeCursor parses an opaque cursor back into its keyset position.
// Any malformed input maps onto ErrInvalidCursor (docs/reference/http-api.md: 400).
func decodeCursor(cursor string) (time.Time, string, error) {
	assertf(cursor != "", "decodeCursor: empty cursor (caller must skip keyset predicate)")
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("%w: %v", ErrInvalidCursor, err)
	}
	ts, repoID, ok := strings.Cut(string(raw), "|")
	if !ok || repoID == "" {
		return time.Time{}, "", fmt.Errorf("%w: missing repo id", ErrInvalidCursor)
	}
	// The repo id reaches a PostgreSQL text parameter, which refuses a NUL
	// byte and invalid UTF-8 (a server-made id holds neither).
	if !ValidText(repoID) {
		return time.Time{}, "", fmt.Errorf("%w: repo id is not UTF-8 text without NUL", ErrInvalidCursor)
	}
	nanos, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("%w: bad timestamp", ErrInvalidCursor)
	}
	return time.Unix(0, nanos).UTC(), repoID, nil
}
