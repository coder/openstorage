package githttp

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestPushHookEnvStripsBothPrefixes(t *testing.T) {
	env := []string{"PATH=/bin"}
	for _, name := range []string{
		EnvRepo, EnvOrg, EnvPackQuota, EnvPackReservationTTL,
		EnvAtomic, EnvUsageFloor, EnvVerifyState, EnvVerifySigBudget,
		EnvSyncClaim, EnvScopes, EnvRefsPolicy, EnvOps, EnvPushHold, EnvHookErrors,
	} {
		legacy := strings.Replace(name, "OPENSTORAGE_", "CODESTORAGE_", 1)
		// Duplicates too: neither generation of hook may inherit authority.
		env = append(env, name+"=stale", legacy+"=stale", name+"=duplicate", legacy+"=duplicate")
	}
	got := pushHookEnv(env, "trusted-repo", "trusted-org")
	want := []string{"PATH=/bin", "OPENSTORAGE_GIT_REPO=trusted-repo", "OPENSTORAGE_ORG=trusted-org"}
	if !slices.Equal(got, want) {
		t.Fatalf("pushHookEnv = %q; want %q", got, want)
	}
}

func TestRefTxHookScriptEnvCompatibility(t *testing.T) {
	for _, tt := range []struct {
		name string
		env  []string
		run  bool
	}{
		{name: "unset"},
		{name: "canonical", env: []string{"OPENSTORAGE_GIT_PUSH_HOLD=new"}, run: true},
		{name: "legacy ignored", env: []string{"CODESTORAGE_GIT_PUSH_HOLD=old"}},
		{name: "both", env: []string{"OPENSTORAGE_GIT_PUSH_HOLD=new", "CODESTORAGE_GIT_PUSH_HOLD="}, run: true},
		{name: "canonical empty", env: []string{"OPENSTORAGE_GIT_PUSH_HOLD=", "CODESTORAGE_GIT_PUSH_HOLD=old"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			fake := filepath.Join(dir, "binary")
			if err := os.WriteFile(fake, []byte("#!/bin/sh\nprintf invoked\n"), 0o755); err != nil {
				t.Fatal(err)
			}
			hook := filepath.Join(dir, "hook")
			if err := os.WriteFile(hook, []byte(fmt.Sprintf(refTxHookScript, fake)), 0o755); err != nil {
				t.Fatal(err)
			}
			for _, state := range []string{"prepared", "aborted", "committed"} {
				cmd := exec.CommandContext(t.Context(), hook, state)
				cmd.Env = append([]string{"PATH=/bin"}, tt.env...)
				out, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("hook %s: %v: %s", state, err, out)
				}
				want := ""
				if tt.run && state == "committed" {
					want = "invoked"
				}
				if string(out) != want {
					t.Fatalf("hook %s = %q; want %q", state, out, want)
				}
			}
		})
	}
}
