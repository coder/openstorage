// signing-key subcommands manage the org's commit-signing key registry
// (docs/reference/authentication.md verify-sig). Upstream exposes this only through its dashboard;
// the OSS equivalent is this CLI writing to the metadata store's
// org_signing_keys table, which the receive-path update hook reads fresh
// from Postgres on every push.
package main

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/coder/code-storage/internal/meta"
)

const (
	pgpArmorHeader = "-----BEGIN PGP PUBLIC KEY BLOCK-----"
	pgpArmorFooter = "-----END PGP PUBLIC KEY BLOCK-----"
)

// sshPublicKeyTypes is the allow-list of SSH public key algorithm names git
// accepts for signature verification (allowed_signers), incl. FIDO variants.
var sshPublicKeyTypes = map[string]bool{
	"ssh-ed25519":                        true,
	"ssh-rsa":                            true,
	"ecdsa-sha2-nistp256":                true,
	"ecdsa-sha2-nistp384":                true,
	"ecdsa-sha2-nistp521":                true,
	"sk-ssh-ed25519@openssh.com":         true,
	"sk-ecdsa-sha2-nistp256@openssh.com": true,
}

func cmdSigningKey(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: codestorage signing-key <add|list|remove> --org <org> ...")
	}
	sub, rest := args[0], args[1:]

	fs := flag.NewFlagSet("signing-key "+sub, flag.ExitOnError)
	org := fs.String("org", "", "organization name (the JWT iss claim)")
	dataDir := fs.String("data-dir", "", "local-mode data directory (falls back to $"+envDataDir+"); derives the database location when no URL is set")
	var keyPath *string
	var id *int64
	switch sub {
	case "add":
		keyPath = fs.String("key", "", "path to an SSH public key line or ASCII-armored PGP public key block")
	case "list":
	case "remove":
		id = fs.Int64("id", 0, "key ID to remove (from signing-key list)")
	default:
		return fmt.Errorf("unknown signing-key subcommand %q (want add, list, or remove)", sub)
	}
	if err := fs.Parse(rest); err != nil {
		return err
	}
	if *org == "" {
		return errors.New("--org is required")
	}

	// Shared derivation with serve/list-packs: env URLs win, local mode
	// (--data-dir / $CODESTORAGE_DATA_DIR) derives the sqlite location.
	urls, err := resolveStorageURLs(os.Getenv, *dataDir)
	if err != nil {
		return err
	}
	if urls.dbURL == "" {
		return errDBURLRequired()
	}
	// Signing keys are not encrypted, but the codec is threaded for uniform
	// store semantics (and boot-fatal key-list validation) across every
	// subcommand that opens the metadata store.
	secretsCodec, err := resolveSecretsCodec(os.Getenv)
	if err != nil {
		return err
	}
	ms, cleanup, err := openMetaStore(ctx, urls.dbURL, secretsCodec)
	if err != nil {
		return err
	}
	defer cleanup()

	switch sub {
	case "add":
		if *keyPath == "" {
			return errors.New("--key is required")
		}
		return runSigningKeyAdd(ctx, ms, os.Stdout, *org, *keyPath)
	case "list":
		return runSigningKeyList(ctx, ms, os.Stdout, *org)
	default: // "remove" — the only subcommand left after validation above.
		if *id <= 0 {
			return errors.New("--id is required (a positive ID from signing-key list)")
		}
		return runSigningKeyRemove(ctx, ms, os.Stdout, *org, *id)
	}
}

func runSigningKeyAdd(ctx context.Context, ms meta.Store, out io.Writer, org, keyPath string) error {
	assertf(ms != nil && out != nil && org != "" && keyPath != "", "runSigningKeyAdd: missing store/out/org/keyPath")
	data, err := os.ReadFile(keyPath)
	if err != nil {
		return err
	}
	text, err := validateSigningKeyText(string(data))
	if err != nil {
		return fmt.Errorf("%s: %w", keyPath, err)
	}
	k, err := ms.AddSigningKey(ctx, org, text)
	if errors.Is(err, meta.ErrNotFound) {
		return fmt.Errorf("org %q is not registered (serve bootstraps it from CODESTORAGE_ORG): %w", org, err)
	}
	if err != nil {
		return err
	}
	assertf(k.ID > 0, "store returned signing key with non-positive ID %d", k.ID)
	fmt.Fprintf(out, "added signing key %d for org %s\n", k.ID, org)
	return nil
}

func runSigningKeyList(ctx context.Context, ms meta.Store, out io.Writer, org string) error {
	assertf(ms != nil && out != nil && org != "", "runSigningKeyList: missing store/out/org")
	keys, err := ms.SigningKeysForOrg(ctx, org)
	if errors.Is(err, meta.ErrNotFound) {
		return fmt.Errorf("org %q is not registered (serve bootstraps it from CODESTORAGE_ORG): %w", org, err)
	}
	if err != nil {
		return err
	}
	for _, k := range keys {
		fmt.Fprintf(out, "%d\t%s\t%s\n", k.ID, k.CreatedAt.UTC().Format(time.RFC3339), signingKeySummary(k.KeyText))
	}
	fmt.Fprintf(out, "total: %d key(s)\n", len(keys))
	return nil
}

func runSigningKeyRemove(ctx context.Context, ms meta.Store, out io.Writer, org string, id int64) error {
	assertf(ms != nil && out != nil && org != "" && id > 0, "runSigningKeyRemove: missing store/out/org/id")
	err := ms.RemoveSigningKey(ctx, org, id)
	if errors.Is(err, meta.ErrNotFound) {
		return fmt.Errorf("no signing key %d for org %q: %w", id, org, err)
	}
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "removed signing key %d from org %s\n", id, org)
	return nil
}

// signingKeySummary renders one registry row's key material as a short
// single-line preview (list output).
func signingKeySummary(keyText string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(keyText), "\n")
	line = strings.TrimSpace(line)
	if line == pgpArmorHeader {
		return "PGP public key block"
	}
	const max = 64
	if len(line) > max {
		return line[:max] + "..."
	}
	return line
}

// validateSigningKeyText checks that text is one of the two key formats the
// verify-sig hook can hand to git (docs/reference/authentication.md): a single SSH public key line
// ("<type> <base64> [comment]") or an ASCII-armored PGP public key block.
// Returns the trimmed text to store. Validation happens here, on add — the
// hook trusts registry contents verbatim.
func validateSigningKeyText(text string) (string, error) {
	t := strings.TrimSpace(text)
	if t == "" {
		return "", errors.New("empty signing key")
	}
	if strings.HasPrefix(t, pgpArmorHeader) {
		if !strings.HasSuffix(t, pgpArmorFooter) {
			return "", fmt.Errorf("PGP public key block must end with %q", pgpArmorFooter)
		}
		return t, nil
	}
	if strings.ContainsAny(t, "\r\n") {
		return "", errors.New("SSH public key must be a single line (or an ASCII-armored PGP public key block)")
	}
	fields := strings.Fields(t)
	if len(fields) < 2 {
		return "", errors.New(`SSH public key must look like "<type> <base64> [comment]"`)
	}
	if !sshPublicKeyTypes[fields[0]] {
		return "", fmt.Errorf("unsupported SSH key type %q", fields[0])
	}
	blob, err := base64.StdEncoding.DecodeString(fields[1])
	if err != nil {
		return "", fmt.Errorf("SSH public key base64: %w", err)
	}
	// The SSH wire format opens with a length-prefixed algorithm name that
	// must echo the declared outer type (RFC 4253 §6.6).
	n := len(fields[0])
	if len(blob) < 4+n || binary.BigEndian.Uint32(blob) != uint32(n) || string(blob[4:4+n]) != fields[0] {
		return "", errors.New("SSH public key blob does not match its declared type")
	}
	return t, nil
}
