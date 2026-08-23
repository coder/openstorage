// Fuzz targets for package secrets' untrusted-input surfaces (issue #91):
// Decrypt consumes envelope strings read back from database columns (which
// legacy plaintext rows can shape arbitrarily), and ParseKeys consumes the
// operator-supplied CODESTORAGE_SECRETS_KEY value. Both must degrade to the
// documented sentinel errors — never a panic, and never a fabricated
// plaintext.
package secrets

import (
	"bytes"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
)

// fuzzKeyB64 returns the base64 encoding of a KeySize-byte key filled with b.
func fuzzKeyB64(b byte) string {
	return base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{b}, KeySize))
}

func FuzzDecrypt(f *testing.F) {
	k1, k2 := fuzzKeyB64(0x11), fuzzKeyB64(0x22)
	codec, err := ParseKeys(k1 + "," + k2)
	if err != nil {
		f.Fatal(err)
	}
	// Same keys, reversed order: mints genuine envelopes under the
	// non-active key so the stale=true path has seeds.
	rotated, err := ParseKeys(k2 + "," + k1)
	if err != nil {
		f.Fatal(err)
	}
	// Every plaintext this process ever seals. Decrypt succeeding with any
	// value outside this set means the fuzzer forged a GCM tag.
	known := map[string]bool{
		"webhook-hmac-secret":         true,
		"":                            true,
		"enc:legacy-shaped-plaintext": true, // ReservedPrefix, not a v1 envelope
	}
	seeds := []string{
		"",
		":",
		"deadbeef:",
		"deadbeef:AAAA",    // unknown key id
		"DEADBEEF:AAAA",    // uppercase id: malformed
		"deadbee:AAAA",     // short id
		"\x00\x01\x02:AAA", // binary id bytes
		codec.ActiveKeyID() + ":!!!not-base64",
		codec.ActiveKeyID() + ":" + base64.StdEncoding.EncodeToString([]byte("short")),             // < nonce
		codec.ActiveKeyID() + ":" + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0}, 12)), // nonce only
	}
	for pt := range known {
		seeds = append(seeds,
			strings.TrimPrefix(codec.Encrypt(pt), Marker),
			strings.TrimPrefix(rotated.Encrypt(pt), Marker))
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, tail string) {
		stored := tail
		if !IsEncrypted(stored) {
			// Decrypt's contract lets callers gate on IsEncrypted first;
			// grafting the marker keeps every input reachable.
			stored = Marker + stored
		}
		pt, stale, err := codec.Decrypt(stored)
		if err != nil {
			if !errors.Is(err, ErrUnknownKey) && !errors.Is(err, ErrDecrypt) {
				t.Fatalf("Decrypt error %v wraps neither ErrUnknownKey nor ErrDecrypt", err)
			}
			if pt != "" || stale {
				t.Fatalf("Decrypt failed but returned (%q, stale=%v)", pt, stale)
			}
			return
		}
		if !known[pt] {
			t.Fatalf("Decrypt fabricated plaintext %q from an input this process never sealed", pt)
		}
		keyID, _, ok := strings.Cut(stored[len(Marker):], ":")
		if !ok || !validKeyID(keyID) {
			t.Fatalf("Decrypt succeeded on an envelope with malformed key id: %q", stored)
		}
		if stale != (keyID != codec.ActiveKeyID()) {
			t.Fatalf("stale=%v inconsistent with envelope key id %s (active %s)", stale, keyID, codec.ActiveKeyID())
		}
	})
}

func FuzzParseKeys(f *testing.F) {
	valid1, valid2 := fuzzKeyB64(0x33), fuzzKeyB64(0x44)
	for _, seed := range []string{
		"",
		valid1,
		valid1 + "," + valid2,
		" " + valid1 + " , " + valid2 + " ",
		valid1 + "," + valid1, // duplicate
		valid1 + ",",          // trailing empty entry
		"not-base64!",
		base64.StdEncoding.EncodeToString([]byte("short")), // wrong length
		strings.Repeat(",", 64),
		valid1[:len(valid1)-1], // truncated base64
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		codec, err := ParseKeys(raw)
		if err != nil {
			if codec != nil {
				t.Fatalf("ParseKeys returned a codec alongside error %v", err)
			}
			return
		}
		if raw == "" {
			if codec != nil {
				t.Fatal(`ParseKeys("") must disable encryption (nil codec)`)
			}
			return
		}
		if codec == nil {
			t.Fatal("ParseKeys returned (nil, nil) for non-empty input")
		}
		if n, most := codec.KeyCount(), strings.Count(raw, ",")+1; n < 1 || n > most {
			t.Fatalf("KeyCount = %d, want 1..%d", n, most)
		}
		if !validKeyID(codec.ActiveKeyID()) {
			t.Fatalf("ActiveKeyID %q is not a well-formed fingerprint", codec.ActiveKeyID())
		}
		// Any codec ParseKeys accepts must round-trip immediately.
		pt, stale, err := codec.Decrypt(codec.Encrypt("probe"))
		if err != nil || stale || pt != "probe" {
			t.Fatalf("fresh codec round-trip = (%q, %v, %v), want (\"probe\", false, nil)", pt, stale, err)
		}
	})
}
