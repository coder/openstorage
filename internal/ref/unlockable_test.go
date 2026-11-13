package ref

import (
	"strings"
	"testing"
)

func TestUnlockable(t *testing.T) {
	seg := func(n int) string { return strings.Repeat("a", n) }
	for _, tc := range []struct {
		name string
		ref  string
		want bool
	}{
		{"last part 250 bytes", "refs/notes/x/" + seg(250), false},
		{"last part 251 bytes", "refs/notes/x/" + seg(251), true},
		{"last part 255 bytes", "refs/heads/" + seg(255), true},
		{"middle part 255 bytes", "refs/heads/" + seg(255) + "/x", false},
		{"middle part 256 bytes", "refs/heads/" + seg(256) + "/x", true},
		{"first part 256 bytes", seg(256) + "/x", true},
		{"short last part under a long middle part", "refs/heads/" + seg(255) + "/" + seg(250), false},
		{"both parts too long", "refs/heads/" + seg(256) + "/" + seg(251), true},
		{"last part of 126 two-byte characters (252 bytes)", "refs/notes/x/" + strings.Repeat("é", 126), true},
		{"last part of 125 two-byte characters (250 bytes)", "refs/notes/x/" + strings.Repeat("é", 125), false},
		{"ordinary name", "refs/heads/main", false},
		{"empty name", "", false},
		{"trailing slash", "refs/heads/" + seg(255) + "/", false},
		{"trailing slash after a long part", "refs/heads/" + seg(256) + "/", true},
	} {
		if got := Unlockable(tc.ref); got != tc.want {
			t.Errorf("%s: Unlockable = %v, want %v", tc.name, got, tc.want)
		}
	}
}
