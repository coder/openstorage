package meta

import (
	"encoding/base64"
	"errors"
	"testing"
	"time"
)

func TestCursorRoundTrip(t *testing.T) {
	t.Parallel()
	// PG timestamptz has microsecond precision; use a microsecond-exact time.
	createdAt := time.Date(2026, 8, 11, 13, 4, 5, 123456000, time.UTC)
	cur := encodeCursor(createdAt, "repo_0123456789ab")

	gotTime, gotID, err := decodeCursor(cur)
	if err != nil {
		t.Fatalf("decodeCursor(%q): %v", cur, err)
	}
	if gotID != "repo_0123456789ab" {
		t.Errorf("repo id = %q, want repo_0123456789ab", gotID)
	}
	if !gotTime.Equal(createdAt) {
		t.Errorf("time = %v, want %v", gotTime, createdAt)
	}
}

func TestCursorSurvivesRepoIDWithPipe(t *testing.T) {
	t.Parallel()
	// strings.Cut splits on the FIRST pipe, so a (hypothetical) id containing
	// one still round-trips.
	createdAt := time.Unix(0, 42).UTC()
	gotTime, gotID, err := decodeCursor(encodeCursor(createdAt, "a|b"))
	if err != nil || gotID != "a|b" || !gotTime.Equal(createdAt) {
		t.Fatalf("got (%v, %q, %v), want (%v, %q, nil)", gotTime, gotID, err, createdAt, "a|b")
	}
}

func TestDecodeCursorInvalid(t *testing.T) {
	t.Parallel()
	enc := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
	cases := map[string]string{
		"not base64url":      "!!!not-base64!!!",
		"padded base64":      enc("123|repo_x") + "==",
		"no pipe":            enc("1234567890"),
		"empty repo id":      enc("123|"),
		"non-numeric time":   enc("12x3|repo_x"),
		"empty time":         enc("|repo_x"),
		"overflow timestamp": enc("99999999999999999999999999|repo_x"),
	}
	for name, cursor := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, _, err := decodeCursor(cursor)
			if !errors.Is(err, ErrInvalidCursor) {
				t.Errorf("decodeCursor(%q) err = %v, want ErrInvalidCursor", cursor, err)
			}
		})
	}
}
