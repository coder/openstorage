package githttp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/ref"
)

// branchTipVerdicts fails closed. When the object-type child fails or
// answers something other than one known type per id, every branch command
// that sets a value gets an error, and no other command gets a verdict.
func TestBranchTipVerdictsFailClosed(t *testing.T) {
	a, b := strings.Repeat("a", 40), strings.Repeat("b", 40)
	commands := []refCommand{
		{ref.ZeroSHA, a, "refs/heads/main"},
		{ref.ZeroSHA, b, "refs/heads/other"},
		{ref.ZeroSHA, a, "refs/tags/v1"},
		{a, ref.ZeroSHA, "refs/heads/gone"},
	}
	for name, answer := range map[string]string{
		"git fails":                  "exit 1",
		"too few lines":              "echo commit",
		"too many lines":             "echo commit; echo commit; echo commit",
		"unknown type":               "echo commit; echo bogus",
		"missing line of another id": "echo commit; echo " + a + " missing",
		"no answer":                  "true",
	} {
		t.Run(name, func(t *testing.T) {
			fakeGit(t, "cat >/dev/null; "+answer)
			verdicts := branchTipVerdicts(commands)
			for i, c := range commands {
				wantErr := strings.HasPrefix(c.ref, "refs/heads/") && c.new != ref.ZeroSHA
				if (verdicts[i] != nil) != wantErr {
					t.Errorf("%s: verdict = %v, want an error: %v", c.ref, verdicts[i], wantErr)
				}
			}
		})
	}

	// The control: a git that answers well refuses nothing here.
	fakeGit(t, "cat >/dev/null; echo commit; echo commit")
	for i, err := range branchTipVerdicts(commands) {
		if err != nil {
			t.Errorf("%s: verdict = %v for commits", commands[i].ref, err)
		}
	}
}

// fakeGit puts a git in PATH that runs the shell script body.
func fakeGit(t *testing.T, body string) {
	t.Helper()
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte("#!/bin/sh\n"+body+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
}
