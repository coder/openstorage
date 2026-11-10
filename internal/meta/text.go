package meta

import (
	"strings"
	"unicode/utf8"
)

// ValidText reports whether PostgreSQL can store s as text: valid UTF-8
// without a NUL (SQLSTATE 22021 otherwise).
func ValidText(s string) bool {
	return utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}
