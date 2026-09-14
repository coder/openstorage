package main

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/auth"
)

func TestGenerateKeyPEMFilesIfMissing(t *testing.T) {
	for _, alg := range auth.AllowedAlgorithms {
		t.Run(alg, func(t *testing.T) {
			prefix := filepath.Join(t.TempDir(), "org")
			privPath, pubPath, err := generateKeyPEMFiles(alg, prefix, true)
			if err != nil {
				t.Fatal(err)
			}
			if privPath != prefix+".pem" || pubPath != prefix+".pub.pem" {
				t.Fatalf("unexpected paths %q, %q", privPath, pubPath)
			}
			for path, mode := range map[string]os.FileMode{privPath: 0o600, pubPath: 0o644} {
				info, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				if perm := info.Mode().Perm(); perm&^mode != 0 || perm&0o600 != 0o600 {
					t.Fatalf("%s mode = %o, want at most %o with owner read/write", path, perm, mode)
				}
				// A fixed old mtime detects rewrites without sleeping for a filesystem tick.
				old := time.Unix(1_000_000_000, 0)
				if err := os.Chtimes(path, old, old); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := mintToken(privPath, "org", "repo", "git:read", "", alg, time.Hour); err != nil {
				t.Fatalf("fresh key cannot sign with requested algorithm: %v", err)
			}
			before := snapshotKeyFiles(t, prefix)
			for _, requested := range []string{alg, "EdDSA", "HS256"} {
				if _, _, err := generateKeyPEMFiles(requested, prefix, true); err != nil {
					t.Fatalf("reuse with --alg %s: %v", requested, err)
				}
				assertKeyFilesUnchanged(t, prefix, before)
			}
		})
	}
}

func TestGenerateKeyPEMFilesIfMissingRejectsExisting(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(prefix string) error
	}{
		{"missing_private", func(p string) error { return os.Remove(p + ".pem") }},
		{"missing_public", func(p string) error { return os.Remove(p + ".pub.pem") }},
		{"corrupt_private", func(p string) error { return os.WriteFile(p+".pem", []byte("not a key"), 0o600) }},
		{"corrupt_public", func(p string) error { return os.WriteFile(p+".pub.pem", []byte("not a key"), 0o644) }},
		{"mismatched_pair", func(p string) error {
			other, err := auth.GenerateKey("ES256")
			if err != nil {
				return err
			}
			data, err := auth.MarshalPublicKeyPEM(other.Public())
			if err != nil {
				return err
			}
			return os.WriteFile(p+".pub.pem", data, 0o644)
		}},
		{"multiple_public_keys", func(p string) error {
			data, err := os.ReadFile(p + ".pub.pem")
			if err != nil {
				return err
			}
			return os.WriteFile(p+".pub.pem", bytes.Repeat(data, 2), 0o644)
		}},
		{"group_read", func(p string) error { return os.Chmod(p+".pem", 0o640) }},
		{"group_write", func(p string) error { return os.Chmod(p+".pem", 0o620) }},
		{"group_execute", func(p string) error { return os.Chmod(p+".pem", 0o610) }},
		{"other_read", func(p string) error { return os.Chmod(p+".pem", 0o604) }},
		{"other_write", func(p string) error { return os.Chmod(p+".pem", 0o602) }},
		{"other_execute", func(p string) error { return os.Chmod(p+".pem", 0o601) }},
		{"public_group_write", func(p string) error { return os.Chmod(p+".pub.pem", 0o620) }},
		{"public_other_write", func(p string) error { return os.Chmod(p+".pub.pem", 0o602) }},
		{"private_symlink", func(p string) error { return replaceKeyWithSymlink(p + ".pem") }},
		{"public_symlink", func(p string) error { return replaceKeyWithSymlink(p + ".pub.pem") }},
		{"dangling_private_symlink", func(p string) error {
			if err := os.Remove(p + ".pem"); err != nil {
				return err
			}
			return os.Symlink(p+".missing", p+".pem")
		}},
		{"private_directory", func(p string) error { return replaceKeyWithDirectory(p + ".pem") }},
		{"public_directory", func(p string) error { return replaceKeyWithDirectory(p + ".pub.pem") }},
		{"unsupported_curve", func(p string) error {
			key, err := ecdsa.GenerateKey(elliptic.P224(), rand.Reader)
			if err != nil {
				return err
			}
			priv, err := auth.MarshalPrivateKeyPEM(key)
			if err != nil {
				return err
			}
			pub, err := auth.MarshalPublicKeyPEM(key.Public())
			if err != nil {
				return err
			}
			return errors.Join(os.WriteFile(p+".pem", priv, 0o600), os.WriteFile(p+".pub.pem", pub, 0o644))
		}},
		{"unusable_rsa", func(p string) error {
			key := &rsa.PrivateKey{
				N: big.NewInt(3233), E: 17,
				D: big.NewInt(2753), Primes: []*big.Int{big.NewInt(61), big.NewInt(53)},
			}
			priv, err := auth.MarshalPrivateKeyPEM(key)
			if err != nil {
				return err
			}
			pub, err := auth.MarshalPublicKeyPEM(key.Public())
			if err != nil {
				return err
			}
			return errors.Join(os.WriteFile(p+".pem", priv, 0o600), os.WriteFile(p+".pub.pem", pub, 0o644))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prefix := filepath.Join(t.TempDir(), "org")
			if _, _, err := generateKeyPEMFiles("ES256", prefix, false); err != nil {
				t.Fatal(err)
			}
			if err := tc.mutate(prefix); err != nil {
				t.Fatal(err)
			}
			before := snapshotKeyFiles(t, prefix)
			if _, _, err := generateKeyPEMFiles("ES256", prefix, true); err == nil {
				t.Error("want error, not key replacement")
			}
			assertKeyFilesUnchanged(t, prefix, before)
		})
	}
}

func TestGenerateKeyPEMFilesIfMissingRejectsExtraPEMData(t *testing.T) {
	for _, suffix := range []string{".pem", ".pub.pem"} {
		t.Run(suffix, func(t *testing.T) {
			for _, tc := range []struct {
				name   string
				before string
				after  string
			}{
				{"garbage_prefix", "not PEM\n", ""},
				{"garbage_suffix", "", "not PEM\n"},
				{"truncated_before", "-----BEGIN BROKEN-----\nAAAA\n", ""},
				{"truncated_after", "", "-----BEGIN BROKEN-----\nAAAA\n"},
				{"corrupt_before", "-----BEGIN BROKEN-----\n@@@\n-----END BROKEN-----\n", ""},
				{"corrupt_after", "", "-----BEGIN BROKEN-----\n@@@\n-----END BROKEN-----\n"},
				{"multiple_blocks", "", ""},
			} {
				t.Run(tc.name, func(t *testing.T) {
					prefix := filepath.Join(t.TempDir(), "org")
					if _, _, err := generateKeyPEMFiles("ES256", prefix, false); err != nil {
						t.Fatal(err)
					}
					path := prefix + suffix
					data, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					block, _ := pem.Decode(data)
					if block == nil {
						t.Fatal("generated key has no PEM block")
					}
					if tc.name == "multiple_blocks" {
						data = bytes.Repeat(data, 2)
					} else {
						data = append(append([]byte(tc.before), data...), tc.after...)
						data = bytes.ReplaceAll(data, []byte("BROKEN"), []byte(block.Type))
					}
					if err := os.WriteFile(path, data, 0o600); err != nil {
						t.Fatal(err)
					}
					before := snapshotKeyFiles(t, prefix)
					if _, _, err := generateKeyPEMFiles("ES256", prefix, true); err == nil {
						t.Error("want error for extra PEM data")
					}
					assertKeyFilesUnchanged(t, prefix, before)
				})
			}
		})
	}
}

func TestGenerateKeyPEMFilesIfMissingPEMFormatting(t *testing.T) {
	for _, alg := range []string{"ES256", "RS256", "EdDSA"} {
		t.Run(alg, func(t *testing.T) {
			for name, mode := range map[string]os.FileMode{"public_read_only": 0o444, "public_owner_writable": 0o644} {
				t.Run(name, func(t *testing.T) {
					prefix := filepath.Join(t.TempDir(), "org")
					privPath, pubPath, err := generateKeyPEMFiles(alg, prefix, false)
					if err != nil {
						t.Fatal(err)
					}
					for _, path := range []string{privPath, pubPath} {
						data, err := os.ReadFile(path)
						if err != nil {
							t.Fatal(err)
						}
						data = bytes.ReplaceAll(data, []byte("\n"), []byte(" \t\r\n"))
						data = append(append([]byte(" \t\r\n"), data...), " \t\r\n"...)
						if err := os.WriteFile(path, data, 0o600); err != nil {
							t.Fatal(err)
						}
					}
					if err := os.Chmod(pubPath, mode); err != nil {
						t.Fatal(err)
					}
					before := snapshotKeyFiles(t, prefix)
					if _, _, err := generateKeyPEMFiles("HS256", prefix, true); err != nil {
						t.Fatalf("reuse formatted %s pair: %v", alg, err)
					}
					assertKeyFilesUnchanged(t, prefix, before)
				})
			}
		})
	}
}

func TestGenerateKeyPEMFilesIfMissingRSAStrength(t *testing.T) {
	for name, bits := range map[string]int{"512bit": 512, "1024bit": 1024, "2048bit": 2048, "3072bit": 3072} {
		t.Run(name, func(t *testing.T) {
			if bits == 512 {
				// Exercise the bootstrap policy even with Go's weak-key compatibility switch enabled.
				t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",rsa1024min=0")
			}
			key, err := rsa.GenerateKey(rand.Reader, bits)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := auth.MintToken(key, "RS256", auth.Claims{}); err != nil {
				t.Fatalf("fixture must be a signable RSA key: %v", err)
			}
			priv, err := auth.MarshalPrivateKeyPEM(key)
			if err != nil {
				t.Fatal(err)
			}
			pub, err := auth.MarshalPublicKeyPEM(key.Public())
			if err != nil {
				t.Fatal(err)
			}
			prefix := filepath.Join(t.TempDir(), "org")
			if err := errors.Join(os.WriteFile(prefix+".pem", priv, 0o600), os.WriteFile(prefix+".pub.pem", pub, 0o644)); err != nil {
				t.Fatal(err)
			}
			before := snapshotKeyFiles(t, prefix)
			_, _, err = generateKeyPEMFiles("RS256", prefix, true)
			if (err == nil) != (bits >= 2048) {
				t.Errorf("reuse %d-bit RSA key: error = %v, want success only for >=2048 bits", bits, err)
			}
			assertKeyFilesUnchanged(t, prefix, before)
		})
	}
}

func TestGenerateKeyPEMFilesIfMissingSEC1(t *testing.T) {
	prefix := filepath.Join(t.TempDir(), "org")
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	pub, err := auth.MarshalPublicKeyPEM(key.Public())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(prefix+".pem", pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}), 0o400); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(prefix+".pub.pem", pub, 0o444); err != nil {
		t.Fatal(err)
	}
	before := snapshotKeyFiles(t, prefix)
	if _, _, err := generateKeyPEMFiles("EdDSA", prefix, true); err != nil {
		t.Fatal(err)
	}
	assertKeyFilesUnchanged(t, prefix, before)
}

func TestGenerateKeyPEMFilesIfMissingUnknownAlg(t *testing.T) {
	prefix := filepath.Join(t.TempDir(), "org")
	if _, _, err := generateKeyPEMFiles("HS256", prefix, true); err == nil {
		t.Fatal("want error for unsupported fresh algorithm")
	}
	if files := snapshotKeyFiles(t, prefix); len(files) != 0 {
		t.Fatal("failed generation created files")
	}
}

func TestGenerateKeyPEMFilesOverwrite(t *testing.T) {
	prefix := filepath.Join(t.TempDir(), "org")
	if _, _, err := generateKeyPEMFiles("ES256", prefix, false); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(prefix+".pem", 0o640); err != nil {
		t.Fatal(err)
	}
	before := snapshotKeyFiles(t, prefix)
	if _, _, err := generateKeyPEMFiles("EdDSA", prefix, false); err != nil {
		t.Fatal(err)
	}
	for path, after := range snapshotKeyFiles(t, prefix) {
		if bytes.Equal(after.data, before[path].data) || after.mode != before[path].mode {
			t.Fatalf("%s: legacy mode must replace keys and preserve permissions", path)
		}
	}
}

func TestCmdKeygenIfMissing(t *testing.T) {
	prefix := filepath.Join(t.TempDir(), "org")
	output, err := os.CreateTemp(t.TempDir(), "stdout")
	if err != nil {
		t.Fatal(err)
	}
	stdout := os.Stdout
	os.Stdout = output
	t.Cleanup(func() {
		os.Stdout = stdout
		if err := output.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := cmdKeygen([]string{"--out", prefix, "--if-missing", "--alg", "EdDSA"}); err != nil {
		t.Fatal(err)
	}
	before := snapshotKeyFiles(t, prefix)
	if err := cmdKeygen([]string{"--out", prefix, "--if-missing", "--alg", "HS256"}); err != nil {
		t.Fatal(err)
	}
	assertKeyFilesUnchanged(t, prefix, before)
	data, err := os.ReadFile(output.Name())
	if err != nil {
		t.Fatal(err)
	}
	want := "ready " + prefix + ".pem and " + prefix + ".pub.pem\n"
	if string(data) != want+want {
		t.Fatal("CLI should print only the ready key paths, not key material")
	}
}

func replaceKeyWithSymlink(path string) error {
	if err := os.Rename(path, path+".target"); err != nil {
		return err
	}
	return os.Symlink(path+".target", path)
}

func replaceKeyWithDirectory(path string) error {
	if err := os.Remove(path); err != nil {
		return err
	}
	return os.Mkdir(path, 0o700)
}

type keyFileSnapshot struct {
	data    []byte
	mode    os.FileMode
	modTime time.Time
}

func snapshotKeyFiles(t *testing.T, prefix string) map[string]keyFileSnapshot {
	t.Helper()
	dir := filepath.Dir(prefix)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	files := make(map[string]keyFileSnapshot)
	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())
		info, err := os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
		var data []byte
		if info.Mode().IsRegular() {
			data, err = os.ReadFile(path)
		} else if info.Mode()&os.ModeSymlink != 0 {
			var target string
			target, err = os.Readlink(path)
			data = []byte(target)
		}
		if err != nil {
			t.Fatal(err)
		}
		files[path] = keyFileSnapshot{data: data, mode: info.Mode(), modTime: info.ModTime()}
	}
	return files
}

func assertKeyFilesUnchanged(t *testing.T, prefix string, before map[string]keyFileSnapshot) {
	t.Helper()
	after := snapshotKeyFiles(t, prefix)
	if len(after) != len(before) {
		t.Errorf("file count = %d, want %d", len(after), len(before))
	}
	for path, want := range before {
		got, ok := after[path]
		if !ok || !bytes.Equal(got.data, want.data) || got.mode != want.mode || !got.modTime.Equal(want.modTime) {
			t.Errorf("%s: key bytes, permissions, or mtime changed", path)
		}
	}
}
