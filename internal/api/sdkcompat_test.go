package api

// Vendor SDK compatibility: the acceptance criterion is that the UNMODIFIED
// vendor Go SDK performs CreateRepo/FindOne/ListRepos/DeleteRepo against our
// /api/v1 surface. The read-only SDK checkout is copied into t.TempDir(), a
// throwaway harness module `replace`s the SDK to that copy, and the harness
// binary (testdata/sdkharness/main.go.tmpl) runs against the live in-process
// server. ES256 keys are generated at runtime and never committed.
//
// p4 follow-up (docs/archive/phases/13-phase4-implementation-notes.md): once the write endpoints (commit-pack/diff-commit/
// restore-commit/merge/branches/tags/notes) land, extend the harness template
// to drive the SDK write methods and assert the docs/explanation/streaming-commit-protocols.md result-envelope
// statuses (ok/precondition_failed/conflict) round-trip through the SDK —
// including envelope-shaped 401/403 on write routes.

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const defaultSDKDir = "/home/coder/codestorage/sdk/packages/code-storage-go"

// copyTree copies a directory tree (regular files + dirs only) so the
// read-only vendor checkout is never touched.
func copyTree(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		if !d.Type().IsRegular() {
			return fmt.Errorf("unexpected non-regular file %s in SDK checkout", path)
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.Create(target)
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			out.Close()
			return err
		}
		return out.Close()
	})
	if err != nil {
		t.Fatalf("copy %s -> %s: %v", src, dst, err)
	}
}

// looksLikeNetworkFailure classifies `go build` output so an offline module
// proxy skips loudly instead of false-failing the acceptance criterion.
func looksLikeNetworkFailure(out string) bool {
	for _, marker := range []string{
		"dial tcp", "i/o timeout", "connection refused", "connection reset",
		"no such host", "TLS handshake", "proxyconnect", "Service Unavailable",
	} {
		if strings.Contains(out, marker) {
			return true
		}
	}
	return false
}

func TestVendorSDKCompat(t *testing.T) {
	sdkDir := os.Getenv("CODESTORAGE_SDK_DIR")
	if sdkDir == "" {
		sdkDir = defaultSDKDir
	}
	if _, err := os.Stat(filepath.Join(sdkDir, "go.mod")); err != nil {
		t.Skipf("vendor Go SDK not found at %s (set CODESTORAGE_SDK_DIR to run the compatibility test): %v", sdkDir, err)
	}

	runBackends(t, func(t *testing.T, e *testEnv) { testVendorSDKCompat(t, e, sdkDir) })
}

func testVendorSDKCompat(t *testing.T, e *testEnv, sdkDir string) {
	org := e.newOrg(t)
	tmp := t.TempDir()

	// Unmodified SDK copy (constraint: never touch the reference checkout).
	sdkCopy := filepath.Join(tmp, "sdk")
	copyTree(t, sdkDir, sdkCopy)

	// Throwaway harness module.
	harnessDir := filepath.Join(tmp, "harness")
	if err := os.MkdirAll(harnessDir, 0o755); err != nil {
		t.Fatal(err)
	}
	harnessMain, err := os.ReadFile(filepath.Join("..", "..", "testdata", "sdkharness", "main.go.tmpl"))
	if err != nil {
		t.Fatalf("read harness template: %v", err)
	}
	if err := os.WriteFile(filepath.Join(harnessDir, "main.go"), harnessMain, 0o644); err != nil {
		t.Fatal(err)
	}
	goMod := fmt.Sprintf(`module sdkharness

go 1.24.3

require github.com/pierrecomputer/sdk/packages/code-storage-go v0.0.0-00010101000000-000000000000

replace github.com/pierrecomputer/sdk/packages/code-storage-go => %s
`, sdkCopy)
	if err := os.WriteFile(filepath.Join(harnessDir, "go.mod"), []byte(goMod), 0o644); err != nil {
		t.Fatal(err)
	}
	// The SDK's own go.sum covers its dependencies (golang-jwt/jwt, uuid);
	// the replaced module itself needs no sum entry.
	sdkSum, err := os.ReadFile(filepath.Join(sdkCopy, "go.sum"))
	if err != nil {
		t.Fatalf("read SDK go.sum: %v", err)
	}
	if err := os.WriteFile(filepath.Join(harnessDir, "go.sum"), sdkSum, 0o644); err != nil {
		t.Fatal(err)
	}

	// Runtime-generated ES256 private key (PKCS#8), scratch-only.
	keyDER, err := x509.MarshalPKCS8PrivateKey(e.keys.es256)
	if err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(tmp, "sdk-key.pem")
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	// Build first so dependency-download problems are distinguishable from
	// wire-compatibility failures.
	binPath := filepath.Join(tmp, "sdkharness-bin")
	build := exec.Command("go", "build", "-o", binPath, ".")
	build.Dir = harnessDir
	build.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=-mod=mod")
	if out, err := build.CombinedOutput(); err != nil {
		if looksLikeNetworkFailure(string(out)) {
			t.Skipf("SKIPPING SDK compatibility test: harness build could not download SDK dependencies (offline?):\n%s", out)
		}
		t.Fatalf("build SDK harness: %v\n%s", err, out)
	}

	run := exec.Command(binPath, "-api", e.srv.URL, "-org", org, "-key", keyPath)
	out, err := run.CombinedOutput()
	t.Logf("harness output:\n%s", out)
	if err != nil {
		t.Fatalf("SDK harness failed: %v", err)
	}
	if !strings.Contains(string(out), "SDKCOMPAT OK") {
		t.Fatalf("harness exited 0 without SDKCOMPAT OK marker")
	}
}
