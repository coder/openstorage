package meta

import (
	"errors"
	"testing"
)

// TestReservedSecret pins the prefix rule that the stores and the API
// share: a secret value is reserved when it starts with the lower-case
// marker "enc:", the colon included. Nothing else is reserved, so a
// password such as "ENC:x" or "enc" stays valid.
func TestReservedSecret(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		value string
		want  bool
	}{
		{"enc:", true},
		{"enc:x", true},
		{"enc:v1:00000000:AAAA", true},
		{"enc:\x00", true},
		{"", false},
		{"enc", false},
		{"enc-x", false},
		{"encx", false},
		{"ENC:x", false},
		{"Enc:x", false},
		{"eNC:x", false},
		{" enc:x", false},
		{"xenc:", false},
		{"pw-enc:v1:inner", false},
	} {
		if got := ReservedSecret(tc.value); got != tc.want {
			t.Errorf("ReservedSecret(%q) = %v, want %v", tc.value, got, tc.want)
		}
		// encodeSecret refuses exactly what ReservedSecret names. It makes
		// that check before it looks at a codec, and this call passes none.
		_, err := encodeSecret(nil, tc.value)
		if got := errors.Is(err, ErrReservedSecret); got != tc.want {
			t.Errorf("encodeSecret(nil, %q) refused = %v, want %v (err %v)", tc.value, got, tc.want, err)
		}
	}
}
