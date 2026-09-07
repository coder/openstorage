// Package secrets implements optional application-level envelope encryption
// for secret columns in the metadata store (issue #54): webhook HMAC signing
// keys and upstream-sync credential passwords.
//
// The envelope is AES-256-GCM with a random 96-bit nonce per value, stored
// as a text column value of the form
//
//	enc:v1:<keyid>:<base64(nonce || ciphertext)>
//
// where <keyid> is the first 8 hex characters of SHA-256(key) — a public
// fingerprint that lets operators verify rotation progress with plain SQL
// (`... WHERE secret NOT LIKE 'enc:v1:<keyid>:%'`) and lets decrypt failures
// name the missing key without exposing material.
//
// Keys come from OPENSTORAGE_SECRETS_KEY: a comma-separated list of base64
// 32-byte keys. The first key encrypts every write; the rest are
// decrypt-only, supporting rotation (put the new key first, keep old keys
// listed until every row has been rewritten). An empty value disables
// encryption entirely (legacy plaintext behavior).
//
// Marker collision: only stored values carrying the exact v1 marker
// ("enc:v1:") are treated as envelopes. Write paths additionally reject NEW
// plaintext secrets starting with ReservedPrefix ("enc:", all versions —
// meta.ErrReservedSecret) so values written from now on can never be
// confused with any envelope version; pre-existing plaintext rows that
// happen to start with "enc:" but are not v1 envelopes keep reading as
// plaintext (they were legal before this package existed) and are migrated
// into envelopes by the lazy rewrite once a key is set, which removes the
// ambiguity permanently. Residual assumption (documented, astronomically
// unlikely): a legacy plaintext secret shaped exactly like a v1 envelope —
// "enc:v1:" + 8 hex + ":" + base64 — would fail closed as an
// unknown-key/corrupt envelope rather than read as plaintext.
//
// Invariant: no function in this package ever includes plaintext, ciphertext,
// or key material in an error message or log — key ids only.
package secrets

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

const (
	// Marker prefixes every v1 envelope.
	Marker = "enc:v1:"
	// ReservedPrefix is the stored-value namespace reserved for envelopes
	// (all versions). Write paths must reject plaintext secrets starting
	// with it — see the package comment.
	ReservedPrefix = "enc:"
	// KeySize is the AES-256 key length in bytes.
	KeySize = 32
	// keyIDLen is the hex length of a key fingerprint (32 bits of SHA-256).
	keyIDLen = 8
)

// Sentinel errors; callers match with errors.Is. Both are operational
// (wrong OPENSTORAGE_SECRETS_KEY for the data at hand), never programming
// errors.
var (
	// ErrUnknownKey: the envelope names a key id absent from the configured
	// list — the operator removed a still-needed key (or points the server
	// at another deployment's database).
	ErrUnknownKey = errors.New("value is encrypted with a key not listed in OPENSTORAGE_SECRETS_KEY")
	// ErrDecrypt: the envelope is malformed or GCM authentication failed —
	// corrupt row, or a different key with a colliding fingerprint.
	ErrDecrypt = errors.New("decrypt failed: ciphertext corrupt or key mismatch")
)

// assertf is a fail-fast invariant check (crash-and-burn style).
func assertf(cond bool, format string, args ...any) {
	if !cond {
		panic(fmt.Sprintf("secrets assertion failed: "+format, args...))
	}
}

// codecKey is one parsed key: its AEAD and public fingerprint.
type codecKey struct {
	aead cipher.AEAD
	id   string
}

// Codec encrypts with keys[0] and decrypts with any listed key.
// A nil *Codec means encryption is disabled.
type Codec struct {
	keys []codecKey

	// DecryptOnly is the OPENSTORAGE_SECRETS_MODE=decrypt-only staging mode
	// for first enablement on multi-replica fleets: every listed key
	// decrypts, but nothing is encrypted or rewritten, so no envelope is
	// written until every replica can already read it. Callers (the meta
	// package) must not call Encrypt while it is set.
	DecryptOnly bool
}

// ParseKeys parses the OPENSTORAGE_SECRETS_KEY format: comma-separated
// base64 (standard, padded) 32-byte keys, first = active. Returns (nil, nil)
// for an empty value (encryption disabled). Error messages name entry
// positions, never material.
func ParseKeys(raw string) (*Codec, error) {
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	c := &Codec{keys: make([]codecKey, 0, len(parts))}
	seen := make(map[string]int, len(parts))
	for i, part := range parts {
		entry := strings.TrimSpace(part)
		if entry == "" {
			return nil, fmt.Errorf("key %d of %d is empty", i+1, len(parts))
		}
		keyBytes, err := base64.StdEncoding.DecodeString(entry)
		if err != nil {
			return nil, fmt.Errorf("key %d of %d: invalid base64: %w", i+1, len(parts), err)
		}
		if len(keyBytes) != KeySize {
			return nil, fmt.Errorf("key %d of %d: decodes to %d bytes, want %d (AES-256)", i+1, len(parts), len(keyBytes), KeySize)
		}
		sum := sha256.Sum256(keyBytes)
		id := hex.EncodeToString(sum[:])[:keyIDLen]
		if prev, dup := seen[string(keyBytes)]; dup {
			return nil, fmt.Errorf("key %d of %d duplicates key %d", i+1, len(parts), prev+1)
		}
		seen[string(keyBytes)] = i
		block, err := aes.NewCipher(keyBytes)
		assertf(err == nil, "aes.NewCipher on a %d-byte key: %v", KeySize, err)
		aead, err := cipher.NewGCM(block)
		assertf(err == nil, "cipher.NewGCM: %v", err)
		c.keys = append(c.keys, codecKey{aead: aead, id: id})
	}
	assertf(len(c.keys) > 0, "ParseKeys: non-empty input produced zero keys")
	return c, nil
}

// KeyCount returns the number of configured keys.
func (c *Codec) KeyCount() int {
	assertf(c != nil && len(c.keys) > 0, "KeyCount on nil/empty codec")
	return len(c.keys)
}

// ActiveKeyID returns the fingerprint of the encrypt (first) key.
func (c *Codec) ActiveKeyID() string {
	assertf(c != nil && len(c.keys) > 0, "ActiveKeyID on nil/empty codec")
	return c.keys[0].id
}

// IsEncrypted reports whether a stored value is a v1 envelope.
func IsEncrypted(stored string) bool {
	return strings.HasPrefix(stored, Marker)
}

// validKeyID reports whether the envelope's key-id field is a well-formed
// fingerprint (exactly keyIDLen lowercase hex chars) — the only shape
// Encrypt ever writes, and the only shape errors may echo.
func validKeyID(id string) bool {
	if len(id) != keyIDLen {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// Encrypt seals plaintext under the active key and returns the stored
// envelope. Sealing a value that is already a v1 envelope is always a bug
// (double encryption); other ReservedPrefix-carrying plaintext is legal
// here only for migrating legacy rows — write paths reject it for new
// secrets (meta.ErrReservedSecret) before reaching Encrypt.
func (c *Codec) Encrypt(plaintext string) string {
	assertf(c != nil && len(c.keys) > 0, "Encrypt on nil/empty codec")
	assertf(!c.DecryptOnly, "Encrypt on a decrypt-only codec (staging mode must never write envelopes)")
	assertf(!IsEncrypted(plaintext), "Encrypt: plaintext is already an %q envelope (double encryption)", Marker)
	k := c.keys[0]
	nonce := make([]byte, k.aead.NonceSize())
	_, err := rand.Read(nonce)
	assertf(err == nil, "crypto/rand failed: %v", err)
	sealed := k.aead.Seal(nonce, nonce, []byte(plaintext), nil)
	stored := Marker + k.id + ":" + base64.StdEncoding.EncodeToString(sealed)
	// Defensive round-trip (writes are rare): a value we cannot read back
	// must never reach the database.
	got, stale, err := c.Decrypt(stored)
	assertf(err == nil && !stale && got == plaintext, "Encrypt round-trip failed under the active key: %v", err)
	return stored
}

// Decrypt opens a stored envelope. stale=true when a non-active key
// decrypted it — the caller should rewrite the row under the active key.
// Errors wrap ErrUnknownKey or ErrDecrypt and carry key ids only.
func (c *Codec) Decrypt(stored string) (plaintext string, stale bool, err error) {
	assertf(c != nil && len(c.keys) > 0, "Decrypt on nil/empty codec")
	assertf(IsEncrypted(stored), "Decrypt on a non-envelope value")
	rest := stored[len(Marker):]
	keyID, b64, ok := strings.Cut(rest, ":")
	if !ok || !validKeyID(keyID) {
		// The field is untrusted (a legacy plaintext secret can be shaped
		// like an envelope), so a malformed id is never echoed into the
		// error — it could be secret bytes.
		return "", false, fmt.Errorf("malformed envelope: %w", ErrDecrypt)
	}
	sealed, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return "", false, fmt.Errorf("malformed envelope (key id %s): %w", keyID, ErrDecrypt)
	}
	matched := false
	for i, k := range c.keys {
		if k.id != keyID {
			continue
		}
		matched = true
		if len(sealed) < k.aead.NonceSize() {
			continue
		}
		pt, err := k.aead.Open(nil, sealed[:k.aead.NonceSize()], sealed[k.aead.NonceSize():], nil)
		if err != nil {
			continue // fingerprint collision with another key, or corrupt row
		}
		return string(pt), i != 0, nil
	}
	if !matched {
		return "", false, fmt.Errorf("key id %s: %w", keyID, ErrUnknownKey)
	}
	return "", false, fmt.Errorf("key id %s: %w", keyID, ErrDecrypt)
}
