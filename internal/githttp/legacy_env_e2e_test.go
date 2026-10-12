package githttp

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestServeRefusesLegacyOnlyEnv runs the real binary with only CODESTORAGE_*
// names. serve must exit before it opens a store and name each old variable with
// its replacement, without values. version and hook skip the check: hooks
// inherit the environment of a serve process that already passed it.
func TestServeRefusesLegacyOnlyEnv(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	env := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + t.TempDir(),
		"CODESTORAGE_PG_URL=" + dbURL, "CODESTORAGE_S3_URL=" + blobURL,
		"CODESTORAGE_ORG=" + testOrg, "CODESTORAGE_ORG_PUBKEYS=" + orgKeysPath,
		// A legacy name next to its set replacement is ignored, not refused.
		"OPENSTORAGE_LISTEN=127.0.0.1:0", "CODESTORAGE_LISTEN=127.0.0.1:1"}
	run := func(args ...string) (string, int) {
		cmd := exec.CommandContext(ctx, binPath, args...)
		cmd.Env = env
		out, err := cmd.CombinedOutput()
		var exit *exec.ExitError
		if err != nil && !errors.As(err, &exit) {
			t.Fatalf("%v: %v", args, err)
		}
		return string(out), cmd.ProcessState.ExitCode()
	}
	out, code := run("serve", "--cache-dir", t.TempDir())
	want := "openstorage serve: CODESTORAGE_* variables are no longer read; rename them " +
		"(see docs/how-to/migrate-to-openstorage.md): CODESTORAGE_ORG -> OPENSTORAGE_ORG, " +
		"CODESTORAGE_ORG_PUBKEYS -> OPENSTORAGE_ORG_PUBKEYS, CODESTORAGE_PG_URL -> OPENSTORAGE_PG_URL, " +
		"CODESTORAGE_S3_URL -> OPENSTORAGE_S3_URL\n"
	if code != 1 || out != want {
		t.Fatalf("serve exit %d, output:\n%s\nwant exit 1, output:\n%s", code, out, want)
	}
	if out, code := run("version"); code != 0 || strings.Contains(out, "CODESTORAGE") {
		t.Fatalf("version exit %d: %s", code, out)
	}
	if out, _ := run("hook"); !strings.Contains(out, "missing hook name argument") || strings.Contains(out, "CODESTORAGE") {
		t.Fatalf("hook must skip the legacy check: %s", out)
	}
}
