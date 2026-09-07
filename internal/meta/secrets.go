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
	"time"

	"github.com/coder/open-storage/internal/secrets"
)

// ErrReservedSecret: CreateWebhook/CreateGitCredential/UpdateGitCredential
// rejected a secret value starting with the reserved envelope prefix
// ("enc:") → 400. Enforced regardless of whether encryption is enabled —
// see the internal/secrets package comment for the collision argument.
var ErrReservedSecret = fmt.Errorf("secret values must not start with the reserved prefix %q", secrets.ReservedPrefix)

// Option configures Open/OpenPG/OpenSQLite.
type Option func(*storeOptions)

type storeOptions struct {
	codec     *secrets.Codec
	now       func() time.Time
	claimSink SyncClaimSink
	// keyRefreshEvery overrides the s3wal org-key snapshot refresh cadence
	// (withKeyRefreshInterval, s3walmeta.go — in-package tests only; the
	// SQL backends ignore it). Zero keeps s3walKeyRefreshInterval.
	keyRefreshEvery time.Duration
}

// WithSecretsCodec enables envelope encryption of the secret columns
// (nil codec = plaintext, the default).
func WithSecretsCodec(c *secrets.Codec) Option {
	return func(o *storeOptions) { o.codec = c }
}

// WithNowFunc pins the store's Go-side clock (nil = time.Now, the default).
// SQLite only: every SQLite timestamp — including the sync-claim lease
// expiry comparison in TryBeginSync — is written and evaluated Go-side, so
// injecting a logical clock makes lease expiry a pure function of that
// clock (the DST harness, internal/simtest, depends on this for
// deterministic traces). PG ignores it deliberately: its lease comparisons
// evaluate on the DATABASE clock so node clock skew never participates
// (issue #86).
func WithNowFunc(now func() time.Time) Option {
	assertf(now != nil, "WithNowFunc: nil clock")
	return func(o *storeOptions) { o.now = now }
}

// WithSyncClaimSink wires the §2.8 sync-claim write-through sink
// (ref.S3WAL in zero-infra mode). Only the s3wal meta backend consumes
// it; the SQL backends ignore it because their CASRef fence reads the
// shared repo_upstreams row directly. Nil restores today's behavior.
func WithSyncClaimSink(sink SyncClaimSink) Option {
	return func(o *storeOptions) { o.claimSink = sink }
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
// A decrypt-only codec (CODESTORAGE_SECRETS_MODE=decrypt-only staging)
// writes plaintext like a nil codec — no envelope may exist before every
// replica can decrypt it; staged rows are re-encrypted lazily once the
// fleet flips to encrypt mode.
func encodeSecret(c *secrets.Codec, plaintext string) (string, error) {
	if strings.HasPrefix(plaintext, secrets.ReservedPrefix) {
		return "", ErrReservedSecret
	}
	if c == nil || c.DecryptOnly {
		return plaintext, nil
	}
	return c.Encrypt(plaintext), nil
}

// decodeSecret maps a stored column value back to plaintext. rewrite=true
// when the caller should CAS-rewrite the row under the active key: a legacy
// plaintext row read while encryption is actively on, or an envelope under
// a retired (non-first) key. A decrypt-only codec never asks for rewrites
// (staging mode writes no envelopes). what names the row class for
// operational errors (e.g. `webhook "wh_x" secret`) and must never contain
// secret material.
//
// Only exact v1 envelopes enter the decrypt path; any other value —
// including pre-#54 plaintext that happens to start with "enc:" — reads as
// legacy plaintext (see the internal/secrets package comment for the
// collision argument). Fail-closed cases (never silent ciphertext
// passthrough): envelope + nil codec (the operator unset
// CODESTORAGE_SECRETS_KEY while encrypted rows remain), and unknown key id
// / GCM failure (secrets.ErrUnknownKey / ErrDecrypt).
func decodeSecret(c *secrets.Codec, stored, what string) (plaintext string, rewrite bool, err error) {
	if secrets.IsEncrypted(stored) {
		if c == nil {
			return "", false, fmt.Errorf("%s is encrypted but CODESTORAGE_SECRETS_KEY is not set", what)
		}
		pt, stale, err := c.Decrypt(stored)
		if err != nil {
			return "", false, fmt.Errorf("%s: %w", what, err)
		}
		return pt, stale && !c.DecryptOnly, nil
	}
	// Legacy plaintext row: readable in both modes; rewrite into an
	// envelope when encryption is actively on (this also migrates legacy
	// values inside the reserved "enc:" namespace, removing the collision).
	return stored, c != nil && !c.DecryptOnly, nil
}
