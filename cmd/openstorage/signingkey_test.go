package main

// Container-free tests for the signing-key CLI: key-text validation and the
// add/list/remove flows against an in-memory store. The PG implementation
// behind meta.Store is covered in internal/meta.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/meta"
)

// sshWireBlob builds a minimal SSH public key blob: the length-prefixed
// algorithm name followed by opaque payload bytes (RFC 4253 §6.6).
func sshWireBlob(keyType string, payload []byte) string {
	blob := make([]byte, 4+len(keyType))
	binary.BigEndian.PutUint32(blob, uint32(len(keyType)))
	copy(blob[4:], keyType)
	return base64.StdEncoding.EncodeToString(append(blob, payload...))
}

func TestValidateSigningKeyText(t *testing.T) {
	ed25519Line := "ssh-ed25519 " + sshWireBlob("ssh-ed25519", make([]byte, 36)) + " alice@example.com"
	pgpBlock := "-----BEGIN PGP PUBLIC KEY BLOCK-----\n\nmQENBF...\n-----END PGP PUBLIC KEY BLOCK-----"

	valid := []struct{ name, in, want string }{
		{"ssh with comment", ed25519Line, ed25519Line},
		{"ssh without comment, padded", "  ssh-ed25519 " + sshWireBlob("ssh-ed25519", nil) + "\n", "ssh-ed25519 " + sshWireBlob("ssh-ed25519", nil)},
		{"pgp armor", pgpBlock, pgpBlock},
		{"pgp armor trailing newline", pgpBlock + "\n", pgpBlock},
	}
	for _, tc := range valid {
		t.Run("valid/"+tc.name, func(t *testing.T) {
			got, err := validateSigningKeyText(tc.in)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("normalized = %q, want %q", got, tc.want)
			}
		})
	}

	invalid := []struct{ name, in, wantSub string }{
		{"empty", "   \n", "empty"},
		{"pgp missing footer", pgpArmorHeader + "\nmQENBF...", "must end with"},
		{"multiline ssh", "ssh-ed25519 AAAA\nssh-ed25519 BBBB", "single line"},
		{"one field", "ssh-ed25519", `"<type> <base64> [comment]"`},
		{"unknown type", "ssh-dss AAAA", "unsupported SSH key type"},
		{"bad base64", "ssh-ed25519 !!!not-base64!!!", "base64"},
		{"type mismatch", "ssh-ed25519 " + sshWireBlob("ssh-rsa", nil), "does not match"},
		{"truncated blob", "ssh-ed25519 " + base64.StdEncoding.EncodeToString([]byte{0, 0}), "does not match"},
		{"private key pasted", "-----BEGIN OPENSSH PRIVATE KEY-----", "unsupported SSH key type"},
	}
	for _, tc := range invalid {
		t.Run("invalid/"+tc.name, func(t *testing.T) {
			if _, err := validateSigningKeyText(tc.in); err == nil || !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("want error containing %q, got %v", tc.wantSub, err)
			}
		})
	}
}

// memSigningStore implements the signing-key third of meta.Store in memory;
// every other method is unreachable from the CLI paths under test.
type memSigningStore struct {
	failingStore // panics-free zero value; embedded only for the interface
	org          string
	nextID       int64
	keys         []meta.SigningKey
}

type failingStore struct{}

func (failingStore) UpsertOrgWithKeys(context.Context, string, []string) (meta.Org, error) {
	panic("not used by signing-key CLI")
}

func (failingStore) PublicKeysForOrg(context.Context, string) ([]string, error) {
	panic("not used by signing-key CLI")
}

func (failingStore) OrgNames(context.Context) ([]string, error) {
	panic("not used by signing-key CLI")
}

func (failingStore) CreateRepo(context.Context, string, string, string) (meta.Repo, error) {
	panic("not used by signing-key CLI")
}

func (failingStore) GetRepoByURL(context.Context, string, string) (meta.Repo, error) {
	panic("not used by signing-key CLI")
}

func (failingStore) GetRepoByID(context.Context, string, string) (meta.Repo, error) {
	panic("not used by signing-key CLI")
}

func (failingStore) UpdateRepoDefaultBranch(context.Context, string, string, string, string) (bool, error) {
	panic("not used by signing-key CLI")
}

func (failingStore) SoftDeleteRepo(context.Context, string, string) (meta.Repo, error) {
	panic("not used by signing-key CLI")
}

func (failingStore) ListRepos(context.Context, string, meta.ListParams) (meta.ListResult, error) {
	panic("not used by signing-key CLI")
}

func (failingStore) CreateWebhook(context.Context, string, string, string) (meta.Webhook, error) {
	panic("not used by signing-key CLI")
}

func (failingStore) ListWebhooks(context.Context, string) ([]meta.Webhook, error) {
	panic("not used by signing-key CLI")
}

func (failingStore) GetWebhook(context.Context, string) (meta.Webhook, error) {
	panic("not used by signing-key CLI")
}

func (failingStore) DeleteWebhook(context.Context, string, string) error {
	panic("not used by signing-key CLI")
}

func (failingStore) CreateGitCredential(context.Context, meta.GitCredential) (meta.GitCredential, error) {
	panic("not used by signing-key CLI")
}

func (failingStore) GitCredentialMeta(context.Context, string) (meta.GitCredential, error) {
	panic("not used by signing-key CLI")
}

func (failingStore) GetGitCredential(context.Context, string) (meta.GitCredential, error) {
	panic("not used by signing-key CLI")
}

func (failingStore) UpdateGitCredential(context.Context, string, string, string) (meta.GitCredential, error) {
	panic("not used by signing-key CLI")
}

func (failingStore) DeleteGitCredential(context.Context, string) error {
	panic("not used by signing-key CLI")
}

func (failingStore) GitCredentialForRepo(context.Context, string) (meta.GitCredential, error) {
	panic("not used by signing-key CLI")
}

func (failingStore) SetRepoUpstream(context.Context, meta.RepoUpstream) (meta.RepoUpstream, error) {
	panic("not used by signing-key CLI")
}

func (failingStore) CreateLinkedRepo(context.Context, string, string, string, meta.RepoUpstream, *meta.GitCredential) (meta.Repo, error) {
	panic("not used by signing-key CLI")
}

func (failingStore) GetRepoUpstream(context.Context, string) (meta.RepoUpstream, error) {
	panic("not used by signing-key CLI")
}

func (failingStore) DeleteRepoUpstream(context.Context, string) error {
	panic("not used by signing-key CLI")
}

func (failingStore) RequestSync(context.Context, string) error {
	panic("not used by signing-key CLI")
}

func (failingStore) PendingSyncRepoIDs(context.Context) ([]string, error) {
	panic("not used by signing-key CLI")
}

func (failingStore) TryBeginSync(context.Context, string, string, time.Duration) (meta.RepoUpstream, bool, string, error) {
	panic("not used by signing-key CLI")
}

func (failingStore) FinishSync(context.Context, string, string, bool) (meta.RepoUpstream, error) {
	panic("not used by signing-key CLI")
}

func (failingStore) RequestPush(context.Context, string, bool) error {
	panic("not used by signing-key CLI")
}

func (failingStore) PendingPushRepoIDs(context.Context) ([]string, error) {
	panic("not used by signing-key CLI")
}

func (failingStore) TryBeginPush(context.Context, string, string, time.Duration) (meta.RepoUpstream, bool, bool, string, error) {
	panic("not used by signing-key CLI")
}

func (failingStore) FinishPush(context.Context, string, string, bool) (meta.RepoUpstream, error) {
	panic("not used by signing-key CLI")
}

func (s *memSigningStore) AddSigningKey(_ context.Context, org, keyText string) (meta.SigningKey, error) {
	if org != s.org {
		return meta.SigningKey{}, meta.ErrNotFound
	}
	s.nextID++
	k := meta.SigningKey{ID: s.nextID, OrgID: 1, KeyText: keyText, CreatedAt: time.Unix(1700000000, 0).UTC()}
	s.keys = append(s.keys, k)
	return k, nil
}

func (s *memSigningStore) SigningKeysForOrg(_ context.Context, org string) ([]meta.SigningKey, error) {
	if org != s.org {
		return nil, meta.ErrNotFound
	}
	return append([]meta.SigningKey(nil), s.keys...), nil
}

func (s *memSigningStore) RemoveSigningKey(_ context.Context, org string, id int64) error {
	if org != s.org {
		return meta.ErrNotFound
	}
	for i, k := range s.keys {
		if k.ID == id {
			s.keys = append(s.keys[:i], s.keys[i+1:]...)
			return nil
		}
	}
	return meta.ErrNotFound
}

func TestSigningKeyAddListRemove(t *testing.T) {
	ctx := context.Background()
	ms := &memSigningStore{org: "org1"}

	keyLine := "ssh-ed25519 " + sshWireBlob("ssh-ed25519", make([]byte, 36)) + " ci@example.com"
	keyPath := filepath.Join(t.TempDir(), "id_ed25519.pub")
	if err := os.WriteFile(keyPath, []byte(keyLine+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := runSigningKeyAdd(ctx, ms, &out, "org1", keyPath); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "added signing key 1 for org org1") {
		t.Fatalf("add output: %q", out.String())
	}
	if len(ms.keys) != 1 || ms.keys[0].KeyText != keyLine {
		t.Fatalf("stored keys: %+v", ms.keys)
	}

	// Unknown org maps ErrNotFound to a bootstrap hint.
	if err := runSigningKeyAdd(ctx, ms, io.Discard, "ghost", keyPath); err == nil || !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("want not-registered error, got %v", err)
	}

	// Invalid key text is rejected before touching the store.
	badPath := filepath.Join(t.TempDir(), "bad.pub")
	if err := os.WriteFile(badPath, []byte("ssh-dss AAAA"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := runSigningKeyAdd(ctx, ms, io.Discard, "org1", badPath); err == nil {
		t.Fatal("want validation error for unsupported key type")
	}
	if len(ms.keys) != 1 {
		t.Fatalf("invalid key must not be stored: %+v", ms.keys)
	}

	out.Reset()
	if err := runSigningKeyList(ctx, ms, &out, "org1"); err != nil {
		t.Fatal(err)
	}
	list := out.String()
	if !strings.Contains(list, "1\t2023-11-14T22:13:20Z\t") || !strings.Contains(list, "total: 1 key(s)") {
		t.Fatalf("list output: %q", list)
	}

	out.Reset()
	if err := runSigningKeyRemove(ctx, ms, &out, "org1", 1); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "removed signing key 1 from org org1") {
		t.Fatalf("remove output: %q", out.String())
	}
	if err := runSigningKeyRemove(ctx, ms, io.Discard, "org1", 1); err == nil || !strings.Contains(err.Error(), "no signing key 1") {
		t.Fatalf("want no-such-key error, got %v", err)
	}
	if err := runSigningKeyList(ctx, ms, &out, "org1"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "total: 0 key(s)") {
		t.Fatalf("list after remove: %q", out.String())
	}
}

func TestSigningKeySummary(t *testing.T) {
	if got := signingKeySummary("-----BEGIN PGP PUBLIC KEY BLOCK-----\nxyz\n-----END PGP PUBLIC KEY BLOCK-----"); got != "PGP public key block" {
		t.Fatalf("pgp summary = %q", got)
	}
	long := "ssh-ed25519 " + strings.Repeat("A", 100)
	if got := signingKeySummary(long); got != long[:64]+"..." {
		t.Fatalf("long summary = %q", got)
	}
	if got := signingKeySummary("short line"); got != "short line" {
		t.Fatalf("short summary = %q", got)
	}
}
