// Conformance tests for secret-column envelope encryption (issue #54),
// fanned out over both backends like the rest of the suite. Every test
// works on two-plus handles of the SAME database — a plaintext handle and
// codec-equipped handles — simulating serve/hook process pairs and restarts
// that enable, rotate, or lose CODESTORAGE_SECRETS_KEY.
package meta

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	"codestorage/internal/secrets"
)

// newTestKey returns a fresh random base64 CODESTORAGE_SECRETS_KEY entry.
func newTestKey(t *testing.T) string {
	t.Helper()
	var b [secrets.KeySize]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return base64.StdEncoding.EncodeToString(b[:])
}

func newCodec(t *testing.T, keys ...string) *secrets.Codec {
	t.Helper()
	c, err := secrets.ParseKeys(strings.Join(keys, ","))
	if err != nil {
		t.Fatalf("ParseKeys: %v", err)
	}
	return c
}

// runSecretsBackends fans a test body out over both backends. plain is a
// codec-less handle; reopen opens an extra handle on the same database with
// the given codec (nil allowed), closed via t.Cleanup.
func runSecretsBackends(t *testing.T, fn func(t *testing.T, plain Backend, reopen func(*secrets.Codec) Backend)) {
	t.Run("pg", func(t *testing.T) {
		plain := pgBackend(t)
		fn(t, plain, func(c *secrets.Codec) Backend {
			s, err := OpenPG(context.Background(), pgTestURL, WithSecretsCodec(c))
			if err != nil {
				t.Fatalf("OpenPG with codec: %v", err)
			}
			t.Cleanup(s.Close)
			return s
		})
	})
	t.Run("sqlite", func(t *testing.T) {
		plain, dbURL := openTestSQLite(t)
		fn(t, plain, func(c *secrets.Codec) Backend {
			s, err := OpenSQLite(context.Background(), dbURL, WithSecretsCodec(c))
			if err != nil {
				t.Fatalf("OpenSQLite with codec: %v", err)
			}
			t.Cleanup(s.Close)
			return s
		})
	})
}

// rawStored reads a secret column exactly as stored, bypassing the codec.
func rawStored(t *testing.T, s Backend, table, col, id string) string {
	t.Helper()
	ctx := context.Background()
	var v string
	var err error
	switch b := s.(type) {
	case *PG:
		err = b.pool.QueryRow(ctx, `SELECT `+col+` FROM `+table+` WHERE id = $1`, id).Scan(&v)
	case *SQLite:
		err = b.db.QueryRowContext(ctx, `SELECT `+col+` FROM `+table+` WHERE id = ?`, id).Scan(&v)
	default:
		t.Fatalf("rawStored: unknown backend %T", s)
	}
	if err != nil {
		t.Fatalf("rawStored %s.%s id=%s: %v", table, col, id, err)
	}
	return v
}

// seedRepo registers a repo for credential tests and returns its RepoID.
func seedRepo(t *testing.T, s Backend, org string) string {
	t.Helper()
	repo, err := s.CreateRepo(context.Background(), org, "github.com/acme/"+strings.ToLower(strings.ReplaceAll(t.Name(), "/", "-")), "main")
	if err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	return repo.RepoID
}

func TestSecretsEncryptedRoundtrip(t *testing.T) {
	runSecretsBackends(t, func(t *testing.T, plain Backend, reopen func(*secrets.Codec) Backend) {
		ctx := t.Context()
		codec := newCodec(t, newTestKey(t))
		enc := reopen(codec)
		org := seedOrg(t, enc)

		const whSecret = "hmac-signing-secret"
		wh, err := enc.CreateWebhook(ctx, org, "https://example.com/hook", whSecret)
		if err != nil {
			t.Fatalf("CreateWebhook: %v", err)
		}
		if wh.Secret != whSecret {
			t.Errorf("CreateWebhook returned secret %q, want plaintext", wh.Secret)
		}
		raw := rawStored(t, plain, "webhooks", "secret", wh.ID)
		if !strings.HasPrefix(raw, secrets.Marker+codec.ActiveKeyID()+":") {
			t.Errorf("stored secret %q not an active-key envelope", raw)
		}
		if strings.Contains(raw, whSecret) {
			t.Errorf("stored secret leaks plaintext")
		}
		got, err := enc.GetWebhook(ctx, wh.ID)
		if err != nil || got.Secret != whSecret {
			t.Errorf("GetWebhook: secret %q, err %v", got.Secret, err)
		}
		list, err := enc.ListWebhooks(ctx, org)
		if err != nil || len(list) != 1 || list[0].Secret != whSecret {
			t.Errorf("ListWebhooks: %+v, err %v", list, err)
		}

		// Missing key on encrypted rows fails closed with an operational
		// error — never silent ciphertext passthrough.
		if _, err := plain.GetWebhook(ctx, wh.ID); err == nil || !strings.Contains(err.Error(), "CODESTORAGE_SECRETS_KEY") {
			t.Errorf("plaintext-handle read of encrypted row: err = %v, want CODESTORAGE_SECRETS_KEY guidance", err)
		}

		const password = "upstream-token"
		repoID := seedRepo(t, enc, org)
		cred, err := enc.CreateGitCredential(ctx, GitCredential{RepoID: repoID, Username: "bot", Password: password})
		if err != nil || cred.Password != password {
			t.Fatalf("CreateGitCredential: %+v, err %v", cred, err)
		}
		rawPW := rawStored(t, plain, "git_credentials", "password", cred.ID)
		if !secrets.IsEncrypted(rawPW) || strings.Contains(rawPW, password) {
			t.Errorf("stored password %q not encrypted", rawPW)
		}
		if got, err := enc.GitCredentialForRepo(ctx, repoID); err != nil || got.Password != password {
			t.Errorf("GitCredentialForRepo: %q, err %v", got.Password, err)
		}
		if _, err := plain.GetGitCredential(ctx, cred.ID); err == nil {
			t.Error("plaintext-handle read of encrypted password succeeded")
		}

		// Update stores a fresh envelope and echoes plaintext.
		upd, err := enc.UpdateGitCredential(ctx, cred.ID, "bot2", "rotated-token")
		if err != nil || upd.Password != "rotated-token" || upd.Username != "bot2" {
			t.Fatalf("UpdateGitCredential: %+v, err %v", upd, err)
		}
		if raw := rawStored(t, plain, "git_credentials", "password", cred.ID); !secrets.IsEncrypted(raw) {
			t.Errorf("updated password stored plaintext: %q", raw)
		}
	})
}

func TestSecretsLegacyPlaintextLazyRewrite(t *testing.T) {
	runSecretsBackends(t, func(t *testing.T, plain Backend, reopen func(*secrets.Codec) Backend) {
		ctx := t.Context()
		codec := newCodec(t, newTestKey(t))
		org := seedOrg(t, plain)

		// Legacy rows: written before encryption was enabled.
		whGet, err := plain.CreateWebhook(ctx, org, "https://example.com/a", "legacy-a")
		if err != nil {
			t.Fatal(err)
		}
		whList, err := plain.CreateWebhook(ctx, org, "https://example.com/b", "legacy-b")
		if err != nil {
			t.Fatal(err)
		}
		if raw := rawStored(t, plain, "webhooks", "secret", whGet.ID); raw != "legacy-a" {
			t.Fatalf("precondition: stored secret %q, want plaintext", raw)
		}

		enc := reopen(codec)
		// Get path rewrites.
		if got, err := enc.GetWebhook(ctx, whGet.ID); err != nil || got.Secret != "legacy-a" {
			t.Fatalf("GetWebhook legacy: %q, err %v", got.Secret, err)
		}
		if raw := rawStored(t, plain, "webhooks", "secret", whGet.ID); !strings.HasPrefix(raw, secrets.Marker+codec.ActiveKeyID()+":") {
			t.Errorf("Get did not rewrite legacy row: %q", raw)
		}
		// List path rewrites the remaining legacy row too.
		list, err := enc.ListWebhooks(ctx, org)
		if err != nil || len(list) != 2 {
			t.Fatalf("ListWebhooks: %d rows, err %v", len(list), err)
		}
		for _, wh := range list {
			if !strings.HasPrefix(wh.Secret, "legacy-") {
				t.Errorf("List returned non-plaintext secret %q", wh.Secret)
			}
		}
		if raw := rawStored(t, plain, "webhooks", "secret", whList.ID); !secrets.IsEncrypted(raw) {
			t.Errorf("List did not rewrite legacy row: %q", raw)
		}

		// Credentials: same treatment.
		repoID := seedRepo(t, plain, org)
		cred, err := plain.CreateGitCredential(ctx, GitCredential{RepoID: repoID, Password: "legacy-pw"})
		if err != nil {
			t.Fatal(err)
		}
		if got, err := enc.GitCredentialForRepo(ctx, repoID); err != nil || got.Password != "legacy-pw" {
			t.Fatalf("GitCredentialForRepo legacy: %q, err %v", got.Password, err)
		}
		if raw := rawStored(t, plain, "git_credentials", "password", cred.ID); !secrets.IsEncrypted(raw) {
			t.Errorf("credential read did not rewrite legacy row: %q", raw)
		}
		// Rewritten rows stay readable through the codec handle.
		if got, err := enc.GetGitCredential(ctx, cred.ID); err != nil || got.Password != "legacy-pw" {
			t.Errorf("re-read after rewrite: %q, err %v", got.Password, err)
		}
	})
}

func TestSecretsRotation(t *testing.T) {
	runSecretsBackends(t, func(t *testing.T, plain Backend, reopen func(*secrets.Codec) Backend) {
		ctx := t.Context()
		k1, k2 := newTestKey(t), newTestKey(t)
		oldCodec, newCodec2 := newCodec(t, k1), newCodec(t, k2, k1)

		encOld := reopen(oldCodec)
		org := seedOrg(t, encOld)
		wh, err := encOld.CreateWebhook(ctx, org, "https://example.com/hook", "rotate-me")
		if err != nil {
			t.Fatal(err)
		}
		repoID := seedRepo(t, encOld, org)
		cred, err := encOld.CreateGitCredential(ctx, GitCredential{RepoID: repoID, Password: "rotate-pw"})
		if err != nil {
			t.Fatal(err)
		}

		// Rotated list (new active first, old retired): reads succeed and
		// rewrite rows under the new key.
		encNew := reopen(newCodec2)
		if got, err := encNew.GetWebhook(ctx, wh.ID); err != nil || got.Secret != "rotate-me" {
			t.Fatalf("GetWebhook under rotated list: %q, err %v", got.Secret, err)
		}
		if raw := rawStored(t, plain, "webhooks", "secret", wh.ID); !strings.HasPrefix(raw, secrets.Marker+newCodec2.ActiveKeyID()+":") {
			t.Errorf("rotation did not rewrite webhook row: %q", raw)
		}
		if got, err := encNew.GitCredentialForRepo(ctx, repoID); err != nil || got.Password != "rotate-pw" {
			t.Fatalf("credential under rotated list: %q, err %v", got.Password, err)
		}
		if raw := rawStored(t, plain, "git_credentials", "password", cred.ID); !strings.HasPrefix(raw, secrets.Marker+newCodec2.ActiveKeyID()+":") {
			t.Errorf("rotation did not rewrite credential row: %q", raw)
		}

		// After the rewrite the old key can be dropped entirely.
		encOnlyNew := reopen(newCodec(t, k2))
		if got, err := encOnlyNew.GetWebhook(ctx, wh.ID); err != nil || got.Secret != "rotate-me" {
			t.Errorf("GetWebhook after retiring old key: %q, err %v", got.Secret, err)
		}
	})
}

func TestSecretsWrongKeyFailsClosed(t *testing.T) {
	runSecretsBackends(t, func(t *testing.T, plain Backend, reopen func(*secrets.Codec) Backend) {
		ctx := t.Context()
		writer := reopen(newCodec(t, newTestKey(t)))
		org := seedOrg(t, writer)
		wh, err := writer.CreateWebhook(ctx, org, "https://example.com/hook", "sealed-secret")
		if err != nil {
			t.Fatal(err)
		}
		repoID := seedRepo(t, writer, org)
		if _, err := writer.CreateGitCredential(ctx, GitCredential{RepoID: repoID, Password: "sealed-pw"}); err != nil {
			t.Fatal(err)
		}
		before := rawStored(t, plain, "webhooks", "secret", wh.ID)

		wrong := reopen(newCodec(t, newTestKey(t)))
		if _, err := wrong.GetWebhook(ctx, wh.ID); !errors.Is(err, secrets.ErrUnknownKey) {
			t.Errorf("GetWebhook with wrong key: err = %v, want ErrUnknownKey", err)
		} else if strings.Contains(err.Error(), "sealed-secret") {
			t.Errorf("error leaks plaintext: %v", err)
		} else if !strings.Contains(err.Error(), wh.ID) {
			t.Errorf("error should name the webhook row: %v", err)
		}
		if _, err := wrong.ListWebhooks(ctx, org); !errors.Is(err, secrets.ErrUnknownKey) {
			t.Errorf("ListWebhooks with wrong key: err = %v", err)
		}
		if _, err := wrong.GitCredentialForRepo(ctx, repoID); !errors.Is(err, secrets.ErrUnknownKey) {
			t.Errorf("GitCredentialForRepo with wrong key: err = %v", err)
		}
		// Failed reads never mutate the stored row.
		if after := rawStored(t, plain, "webhooks", "secret", wh.ID); after != before {
			t.Errorf("failed decrypt rewrote the row: %q -> %q", before, after)
		}
	})
}

// TestSecretsCredentialRecovery covers the operational escape hatch for
// undecryptable credential rows: the password-free metadata lookup, replace
// (Update), and delete must all work without decrypting the stored value,
// so an operator can recover from a prematurely retired key via the normal
// PUT/DELETE routes.
func TestSecretsCredentialRecovery(t *testing.T) {
	runSecretsBackends(t, func(t *testing.T, plain Backend, reopen func(*secrets.Codec) Backend) {
		ctx := t.Context()
		writer := reopen(newCodec(t, newTestKey(t)))
		org := seedOrg(t, writer)
		repoID := seedRepo(t, writer, org)
		cred, err := writer.CreateGitCredential(ctx, GitCredential{RepoID: repoID, Username: "bot", Password: "lost-key-pw"})
		if err != nil {
			t.Fatal(err)
		}

		// The key is gone: decrypting reads fail, metadata does not.
		wrong := reopen(newCodec(t, newTestKey(t)))
		if _, err := wrong.GetGitCredential(ctx, cred.ID); !errors.Is(err, secrets.ErrUnknownKey) {
			t.Fatalf("GetGitCredential: err = %v, want ErrUnknownKey", err)
		}
		row, err := wrong.GitCredentialMeta(ctx, cred.ID)
		if err != nil {
			t.Fatalf("GitCredentialMeta on undecryptable row: %v", err)
		}
		if row.ID != cred.ID || row.RepoID != repoID || row.Username != "bot" || !row.CreatedAt.Equal(cred.CreatedAt) {
			t.Errorf("GitCredentialMeta = %+v, want id/repo/username/created_at of %+v", row, cred)
		}
		if row.Password != "" {
			t.Errorf("GitCredentialMeta returned a password (%d bytes); must always be empty", len(row.Password))
		}

		// Replace writes a fresh envelope under the wrong-key handle's
		// active key — the row is readable again.
		if _, err := wrong.UpdateGitCredential(ctx, cred.ID, "bot", "replacement-pw"); err != nil {
			t.Fatalf("UpdateGitCredential on undecryptable row: %v", err)
		}
		if got, err := wrong.GetGitCredential(ctx, cred.ID); err != nil || got.Password != "replacement-pw" {
			t.Fatalf("read after replace: %q, err %v", got.Password, err)
		}

		// Delete works regardless of decryptability.
		if err := wrong.DeleteGitCredential(ctx, cred.ID); err != nil {
			t.Fatalf("DeleteGitCredential: %v", err)
		}
		if _, err := wrong.GitCredentialMeta(ctx, cred.ID); !errors.Is(err, ErrNotFound) {
			t.Errorf("GitCredentialMeta after delete: err = %v, want ErrNotFound", err)
		}
	})
}

func TestSecretsReservedPrefixRejected(t *testing.T) {
	runSecretsBackends(t, func(t *testing.T, plain Backend, reopen func(*secrets.Codec) Backend) {
		ctx := t.Context()
		enc := reopen(newCodec(t, newTestKey(t)))
		org := seedOrg(t, plain)
		repoID := seedRepo(t, plain, org)
		cred, err := plain.CreateGitCredential(ctx, GitCredential{RepoID: repoID, Password: "fine"})
		if err != nil {
			t.Fatal(err)
		}

		// Rejected in BOTH modes: plaintext mode must not persist values a
		// later-enabled codec would misread as envelopes.
		for name, s := range map[string]Backend{"plain": plain, "encrypted": enc} {
			if _, err := s.CreateWebhook(ctx, org, "https://example.com/h", "enc:v1:fake"); !errors.Is(err, ErrReservedSecret) {
				t.Errorf("%s CreateWebhook: err = %v, want ErrReservedSecret", name, err)
			}
			if _, err := s.CreateGitCredential(ctx, GitCredential{RepoID: repoID + "x", Password: "enc:pw"}); !errors.Is(err, ErrReservedSecret) {
				t.Errorf("%s CreateGitCredential: err = %v, want ErrReservedSecret", name, err)
			}
			if _, err := s.UpdateGitCredential(ctx, cred.ID, "", "enc:v2:future"); !errors.Is(err, ErrReservedSecret) {
				t.Errorf("%s UpdateGitCredential: err = %v, want ErrReservedSecret", name, err)
			}
		}

		// The namespace is a prefix match only — an inner "enc:" is fine.
		if _, err := plain.CreateWebhook(ctx, org, "https://example.com/ok", "pw-enc:v1:inner"); err != nil {
			t.Errorf("inner marker rejected: %v", err)
		}
	})
}

// TestSecretsReservedPrefixReadFailsClosed covers the read side of the
// collision argument: a stored value inside the reserved namespace that is
// not a valid v1 envelope (e.g. written by a newer version) must error, not
// pass through as plaintext.
func TestSecretsReservedPrefixReadFailsClosed(t *testing.T) {
	runSecretsBackends(t, func(t *testing.T, plain Backend, reopen func(*secrets.Codec) Backend) {
		ctx := t.Context()
		org := seedOrg(t, plain)
		wh, err := plain.CreateWebhook(ctx, org, "https://example.com/h", "placeholder")
		if err != nil {
			t.Fatal(err)
		}
		// Forge a future-version envelope directly in the database.
		forged := "enc:v2:00000000:AAAA"
		switch b := plain.(type) {
		case *PG:
			_, err = b.pool.Exec(ctx, `UPDATE webhooks SET secret = $2 WHERE id = $1`, wh.ID, forged)
		case *SQLite:
			_, err = b.db.ExecContext(ctx, `UPDATE webhooks SET secret = ? WHERE id = ?`, forged, wh.ID)
		}
		if err != nil {
			t.Fatal(err)
		}
		for name, s := range map[string]Backend{"plain": plain, "encrypted": reopen(newCodec(t, newTestKey(t)))} {
			got, err := s.GetWebhook(ctx, wh.ID)
			if err == nil {
				t.Errorf("%s: read of forged envelope returned %q; want fail-closed error", name, got.Secret)
			}
		}
	})
}
