// Secret-column envelope encryption (issue #54): both backends thread an
// optional secrets.Codec through these helpers so the two secret column
// classes — webhooks.secret and git_credentials.password — get identical
// treatment on Postgres and SQLite. With a nil codec everything stays
// plaintext except the reserved-prefix write rejection, which applies in
// both modes so a later-enabled codec can never misread legacy plaintext as
// an envelope.

package meta

import (
	"fmt"
	"strings"

	"codestorage/internal/secrets"
)

// ErrReservedSecret: CreateWebhook/CreateGitCredential/UpdateGitCredential
// rejected a secret value starting with the reserved envelope prefix
// ("enc:") → 400. Enforced regardless of whether encryption is enabled —
// see the internal/secrets package comment for the collision argument.
var ErrReservedSecret = fmt.Errorf("secret values must not start with the reserved prefix %q", secrets.ReservedPrefix)

// Option configures Open/OpenPG/OpenSQLite.
type Option func(*storeOptions)

type storeOptions struct {
	codec *secrets.Codec
}

// WithSecretsCodec enables envelope encryption of the secret columns
// (nil codec = plaintext, the default).
func WithSecretsCodec(c *secrets.Codec) Option {
	return func(o *storeOptions) { o.codec = c }
}

func resolveOptions(opts []Option) storeOptions {
	var o storeOptions
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// encodeSecret maps a plaintext secret to its stored column value:
// the reserved-prefix rejection, then encryption when the codec is set.
func encodeSecret(c *secrets.Codec, plaintext string) (string, error) {
	if strings.HasPrefix(plaintext, secrets.ReservedPrefix) {
		return "", ErrReservedSecret
	}
	if c == nil {
		return plaintext, nil
	}
	return c.Encrypt(plaintext), nil
}

// decodeSecret maps a stored column value back to plaintext. rewrite=true
// when the caller should CAS-rewrite the row under the active key: a legacy
// plaintext row read while encryption is enabled, or an envelope under a
// retired (non-first) key. what names the row class for operational errors
// (e.g. `webhook "wh_x" secret`) and must never contain secret material.
//
// Fail-closed cases (never silent ciphertext passthrough):
//   - envelope + nil codec → the operator unset CODESTORAGE_SECRETS_KEY
//     while encrypted rows remain;
//   - reserved prefix without a valid v1 marker → written by a newer
//     version, or corrupt;
//   - unknown key id / GCM failure → secrets.ErrUnknownKey / ErrDecrypt.
func decodeSecret(c *secrets.Codec, stored, what string) (plaintext string, rewrite bool, err error) {
	if secrets.IsEncrypted(stored) {
		if c == nil {
			return "", false, fmt.Errorf("%s is encrypted but CODESTORAGE_SECRETS_KEY is not set", what)
		}
		pt, stale, err := c.Decrypt(stored)
		if err != nil {
			return "", false, fmt.Errorf("%s: %w", what, err)
		}
		return pt, stale, nil
	}
	if strings.HasPrefix(stored, secrets.ReservedPrefix) {
		// Not a v1 envelope, but inside the reserved namespace: written by
		// a newer envelope version, or a pre-#54 plaintext secret that
		// collides with the marker (write paths reject these now). Fail
		// closed rather than guess.
		return "", false, fmt.Errorf("%s carries the reserved %q prefix but is not a valid %q envelope; refusing to return it as plaintext",
			what, secrets.ReservedPrefix, secrets.Marker)
	}
	// Legacy plaintext row: readable in both modes; rewrite when encryption
	// is on.
	return stored, c != nil, nil
}
