package secrets

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
)

// testKey returns a fresh random base64 key entry.
func testKey(t *testing.T) string {
	t.Helper()
	var b [KeySize]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return base64.StdEncoding.EncodeToString(b[:])
}

func mustParse(t *testing.T, raw string) *Codec {
	t.Helper()
	c, err := ParseKeys(raw)
	if err != nil {
		t.Fatalf("ParseKeys(%d chars): %v", len(raw), err)
	}
	if c == nil {
		t.Fatal("ParseKeys returned nil codec for non-empty input")
	}
	return c
}

func TestParseKeysDisabled(t *testing.T) {
	c, err := ParseKeys("")
	if c != nil || err != nil {
		t.Fatalf("ParseKeys(\"\") = %v, %v; want nil, nil", c, err)
	}
}

func TestParseKeysValid(t *testing.T) {
	k1, k2 := testKey(t), testKey(t)

	single := mustParse(t, k1)
	if single.KeyCount() != 1 {
		t.Errorf("KeyCount = %d, want 1", single.KeyCount())
	}
	if len(single.ActiveKeyID()) != keyIDLen {
		t.Errorf("ActiveKeyID %q: want %d hex chars", single.ActiveKeyID(), keyIDLen)
	}

	// Multi-key with whitespace tolerance; active = first.
	multi := mustParse(t, " "+k2+" , "+k1+" ")
	if multi.KeyCount() != 2 {
		t.Errorf("KeyCount = %d, want 2", multi.KeyCount())
	}
	if multi.ActiveKeyID() == single.ActiveKeyID() {
		t.Errorf("active key id should be k2's, got k1's %s", single.ActiveKeyID())
	}
}

func TestParseKeysMalformed(t *testing.T) {
	k := testKey(t)
	short := base64.StdEncoding.EncodeToString([]byte("too-short"))
	for name, raw := range map[string]string{
		"bad base64":      "not-base64!!!",
		"wrong length":    short,
		"empty entry":     k + ",," + testKey(t),
		"trailing comma":  k + ",",
		"duplicate key":   k + "," + k,
		"one bad of many": testKey(t) + ",xx",
	} {
		c, err := ParseKeys(raw)
		if err == nil {
			t.Errorf("%s: ParseKeys succeeded (%d keys), want error", name, c.KeyCount())
			continue
		}
		// Errors must never echo key material.
		for entry := range strings.SplitSeq(raw, ",") {
			entry = strings.TrimSpace(entry)
			if len(entry) >= 8 && strings.Contains(err.Error(), entry) {
				t.Errorf("%s: error %q echoes a key entry", name, err)
			}
		}
	}
}

func TestEncryptDecryptRoundtrip(t *testing.T) {
	c := mustParse(t, testKey(t))
	for _, pt := range []string{"", "s3cret", "пароль-ünïcode\x00\xff", strings.Repeat("x", 4096)} {
		stored := c.Encrypt(pt)
		if !IsEncrypted(stored) {
			t.Fatalf("Encrypt output %q lacks marker", stored[:min(20, len(stored))])
		}
		if !strings.HasPrefix(stored, Marker+c.ActiveKeyID()+":") {
			t.Fatalf("Encrypt output not under active key id %s", c.ActiveKeyID())
		}
		got, stale, err := c.Decrypt(stored)
		if err != nil || stale || got != pt {
			t.Fatalf("Decrypt roundtrip: got %d bytes, stale=%v, err=%v", len(got), stale, err)
		}
	}
	// Nondeterministic envelopes (random nonce).
	first := c.Encrypt("same")
	if second := c.Encrypt("same"); first == second {
		t.Error("two Encrypt calls produced identical envelopes")
	}
}

func TestRotationDecryptWithRetiredKey(t *testing.T) {
	k1, k2 := testKey(t), testKey(t)
	old := mustParse(t, k1)
	stored := old.Encrypt("rotate-me")

	rotated := mustParse(t, k2+","+k1) // new active first, old retired
	got, stale, err := rotated.Decrypt(stored)
	if err != nil || got != "rotate-me" {
		t.Fatalf("Decrypt under rotated list: %q, %v", got, err)
	}
	if !stale {
		t.Error("stale=false for a retired-key envelope; want true (rewrite trigger)")
	}
	// Fresh writes use the new active key.
	if !strings.HasPrefix(rotated.Encrypt("x"), Marker+rotated.ActiveKeyID()+":") {
		t.Error("Encrypt not using the new active key")
	}
}

func TestDecryptWrongKeyFailsClosed(t *testing.T) {
	writer := mustParse(t, testKey(t))
	stored := writer.Encrypt("plaintext-material")

	reader := mustParse(t, testKey(t)) // does not list the writer's key
	got, _, err := reader.Decrypt(stored)
	if err == nil {
		t.Fatalf("Decrypt with wrong key list succeeded: %q", got)
	}
	if !errors.Is(err, ErrUnknownKey) {
		t.Errorf("err = %v, want ErrUnknownKey", err)
	}
	if !strings.Contains(err.Error(), writer.ActiveKeyID()) {
		t.Errorf("err %q should name the missing key id %s", err, writer.ActiveKeyID())
	}
	if strings.Contains(err.Error(), "plaintext-material") {
		t.Errorf("err %q leaks plaintext", err)
	}
}

func TestDecryptCorruptCiphertext(t *testing.T) {
	c := mustParse(t, testKey(t))
	stored := c.Encrypt("victim")

	// Flip one ciphertext byte (re-encode the base64 payload).
	head := Marker + c.ActiveKeyID() + ":"
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(stored, head))
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0x01
	if _, _, err := c.Decrypt(head + base64.StdEncoding.EncodeToString(raw)); !errors.Is(err, ErrDecrypt) {
		t.Errorf("corrupt ciphertext: err = %v, want ErrDecrypt", err)
	}

	// Malformed envelopes.
	for _, bad := range []string{Marker, Marker + "xyz", Marker + "0123456789:AAAA", Marker + c.ActiveKeyID() + ":!!!", Marker + c.ActiveKeyID() + ":" + base64.StdEncoding.EncodeToString([]byte("tiny"))} {
		if _, _, err := c.Decrypt(bad); !errors.Is(err, ErrDecrypt) && !errors.Is(err, ErrUnknownKey) {
			t.Errorf("Decrypt(%q): err = %v, want ErrDecrypt/ErrUnknownKey", bad, err)
		}
	}

	// A v1-shaped legacy plaintext value: the 8-char field after the marker
	// is untrusted secret material, so the error must never echo it (only
	// validated lowercase-hex fingerprints may appear in errors).
	_, _, err = c.Decrypt(Marker + "Password:hunter22")
	if !errors.Is(err, ErrDecrypt) {
		t.Fatalf("v1-shaped plaintext: err = %v, want ErrDecrypt", err)
	}
	for _, leak := range []string{"Password", "hunter22"} {
		if strings.Contains(err.Error(), leak) {
			t.Errorf("error %q echoes untrusted envelope text %q", err, leak)
		}
	}
}

func TestMarkerInsideReservedNamespace(t *testing.T) {
	if !strings.HasPrefix(Marker, ReservedPrefix) {
		t.Fatalf("Marker %q must live inside the reserved namespace %q", Marker, ReservedPrefix)
	}
}
