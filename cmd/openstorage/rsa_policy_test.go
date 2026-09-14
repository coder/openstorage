package main

import (
	"context"
	"crypto/rsa"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/auth"
)

func TestServeRejectsWeakRSA(t *testing.T) {
	if dir := os.Getenv("OPENSTORAGE_RSA_POLICY_TEST"); dir != "" {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		err := cmdServe(ctx, []string{"--data-dir", dir, "--listen", "127.0.0.1:0"})
		if err == nil || !strings.Contains(err.Error(), os.Getenv("OPENSTORAGE_ORG_PUBKEYS")) ||
			!strings.Contains(err.Error(), "minimum 2048") {
			t.Fatalf("serve error = %v, want key filename and RSA minimum 2048", err)
		}
		return
	}
	weak, err := auth.MarshalPublicKeyPEM(&rsa.PublicKey{N: new(big.Int).SetBit(new(big.Int), 1023, 1), E: 65537})
	if err != nil {
		t.Fatal(err)
	}
	for name, keys := range map[string][]byte{
		"weak": weak, "mixed": append(rsaPEM(t), weak...),
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "org.pub.pem")
			if err := os.WriteFile(path, keys, 0o600); err != nil {
				t.Fatal(err)
			}
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, exe, "-test.run=^TestServeRejectsWeakRSA$")
			// Isolate serve's process-global config and listener lifecycle.
			cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"),
				"OPENSTORAGE_RSA_POLICY_TEST=" + dir, "OPENSTORAGE_ORG=acme", "OPENSTORAGE_ORG_PUBKEYS=" + path}
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("serve subprocess: %v\n%s", err, out)
			}
		})
	}
}
