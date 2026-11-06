package api

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// TestApplyMessageCases pins the words of git that wcApplyStatus and
// wcPatchModeError read (#613). testdata/applymsg/cases.tsv lists one patch
// per message, with the exit status and the line prefix git must produce.
// scripts/check-apply-messages.sh runs the cases through the git on PATH, and
// CI runs the same script inside the built image, whose git is Alpine's and
// not the one of the test job. A reworded message then fails a check instead of
// turning a client error into a 500 without a sign.
func TestApplyMessageCases(t *testing.T) {
	dir := filepath.Join("testdata", "applymsg")
	raw, err := os.ReadFile(filepath.Join(dir, "cases.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	covered := map[string]bool{}
	for line := range strings.Lines(string(raw)) {
		f := strings.Split(strings.TrimSuffix(line, "\n"), "\t")
		if len(f) != 5 {
			t.Fatalf("cases.tsv line %q: want name, command, exit, status and prefix", line)
		}
		name, command, prefix := f[0], f[1], f[4]
		exit, err := strconv.Atoi(f[2])
		if err != nil {
			t.Fatalf("cases.tsv %s: exit %q: %v", name, f[2], err)
		}
		if _, err := os.Stat(filepath.Join(dir, name+".patch")); err != nil {
			t.Fatal(err)
		}
		covered[prefix] = true
		switch command {
		case "cached":
			want, err := strconv.Atoi(f[3])
			if err != nil {
				t.Fatalf("cases.tsv %s: status %q: %v", name, f[3], err)
			}
			if got := wcApplyStatus(exit, prefix+"x"); got != want {
				t.Errorf("%s: wcApplyStatus(%d, %q) = %d, want %d", name, exit, prefix+"x", got, want)
			}
		case "summary":
			if wcPatchModeError(prefix+"dd\n") == nil {
				t.Errorf("%s: wcPatchModeError accepts a line that starts with %q and holds a directory mode", name, prefix)
			}
		default:
			t.Fatalf("cases.tsv %s: unknown command %q", name, command)
		}
	}
	for _, p := range append(slices.Clone(wcApplyPayloadLines), wcApplyClashLine, " create mode 040000 ", " mode change 100644 => 040000 ") {
		if !covered[p] {
			t.Errorf("no case in cases.tsv for the line prefix %q", p)
		}
	}
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("no sh to run scripts/check-apply-messages.sh")
	}
	out, err := exec.Command("sh", filepath.Join("..", "..", "scripts", "check-apply-messages.sh"), dir).CombinedOutput()
	if err != nil {
		t.Fatalf("scripts/check-apply-messages.sh: %v\n%s", err, out)
	}
}
