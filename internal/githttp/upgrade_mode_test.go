package githttp

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Exercise the actual TestMain boundary: rejected require-mode inputs must not
// build/start an application, create a backend, or return a zero-tests success.
func TestUpgradeRequireMode(t *testing.T) {
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		mode string
		args []string
	}{
		{"missing-inputs", "require", nil},
		{"unknown-mode", "yes", nil},
		{"wrong-selection", "require", []string{"-test.run=^$"}},
		{"repetition", "require", []string{"-test.count=2"}},
		{"skip", "require", []string{"-test.skip=ReleaseUpgrade"}},
		{"list-only", "require", []string{"-test.list=ReleaseUpgrade"}},
		{"bench", "require", []string{"-test.bench=."}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			attempt := filepath.Join(t.TempDir(), "attempt")
			args := append([]string{"-test.run=^TestS3WAL_ReleaseUpgrade$", "-test.count=1"}, tc.args...)
			cmd := exec.CommandContext(t.Context(), binary, args...)
			for _, entry := range os.Environ() {
				if !strings.HasPrefix(entry, "OPENSTORAGE_UPGRADE_") && !strings.HasPrefix(entry, "OPENSTORAGE_TEST_UPGRADE=") {
					cmd.Env = append(cmd.Env, entry)
				}
			}
			cmd.Env = append(cmd.Env, "OPENSTORAGE_TEST_UPGRADE="+tc.mode, "OPENSTORAGE_UPGRADE_ATTEMPT="+attempt)
			out, err := cmd.CombinedOutput()
			if err == nil || cmd.ProcessState == nil || cmd.ProcessState.ExitCode() != 1 {
				t.Fatalf("require-mode rejection: %v, output %s", err, out)
			}
			code, err := os.ReadFile(filepath.Join(attempt, "inner-exit.txt"))
			if err != nil || string(code) != "1\n" {
				t.Fatalf("failed attempt exit not preserved: %v, %q", err, code)
			}
			if _, err := os.Stat(filepath.Join(attempt, "phases.log")); !os.IsNotExist(err) {
				t.Fatal("input rejection entered the application fixture")
			}
			// Reusing even a failed attempt must preserve its original receipt.
			cmd = exec.CommandContext(t.Context(), binary, args...)
			cmd.Env = append(os.Environ(), "OPENSTORAGE_TEST_UPGRADE=require", "OPENSTORAGE_UPGRADE_ATTEMPT="+attempt)
			if out, err := cmd.CombinedOutput(); err == nil || !strings.Contains(string(out), "create exclusive upgrade attempt") {
				t.Fatalf("existing attempt accepted: %v, %s", err, out)
			}
		})
	}
}
