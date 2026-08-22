package githttp

// Unit tests for the issue #59 per-push verify-sig state: file round-trips
// across "processes" (fresh loads of the same file), budget accounting, and
// fail-closed parsing of malformed state and budget values.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	shaA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	shaB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	shaC = "cccccccccccccccccccccccccccccccccccccccc"
)

// newStateFile creates an empty state file and points EnvVerifyState at it,
// mirroring what handleServiceRPC does per receive-pack request.
func newStateFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "verify.state")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvVerifyState, path)
	return path
}

func TestVerifyState_RoundTripAcrossLoads(t *testing.T) {
	newStateFile(t)

	st, err := openVerifyState()
	if err != nil {
		t.Fatal(err)
	}
	if err := st.markCommitOK(shaA); err != nil {
		t.Fatal(err)
	}
	if err := st.markCommitDenied(shaB, "commit is not signed"); err != nil {
		t.Fatal(err)
	}
	if err := st.markRangeOK(shaA, shaB); err != nil {
		t.Fatal(err)
	}

	// A later hook process loads the same file fresh.
	st2, err := openVerifyState()
	if err != nil {
		t.Fatal(err)
	}
	if !st2.ok[shaA] {
		t.Fatal("ok verdict lost across loads")
	}
	if reason := st2.deny[shaB]; reason != "commit is not signed" {
		t.Fatalf("deny verdict = %q, want reason preserved", reason)
	}
	if !st2.ranges[rangeKey(shaA, shaB)] {
		t.Fatal("range verdict lost across loads")
	}
	if st2.spent != 2 {
		t.Fatalf("spent = %d, want 2 (ok + deny; okrange lines are free)", st2.spent)
	}
}

func TestVerifyState_UnsetEnvFallsBackToMemory(t *testing.T) {
	t.Setenv(EnvVerifyState, "")
	st, err := openVerifyState()
	if err != nil {
		t.Fatal(err)
	}
	if err := st.markCommitOK(shaA); err != nil {
		t.Fatal(err)
	}
	if !st.ok[shaA] || st.spent != 1 {
		t.Fatalf("in-memory state ok=%v spent=%d, want tracked verdict", st.ok[shaA], st.spent)
	}
}

func TestVerifyState_MissingFileFailsClosed(t *testing.T) {
	t.Setenv(EnvVerifyState, filepath.Join(t.TempDir(), "nope.state"))
	if _, err := openVerifyState(); err == nil {
		t.Fatal("set-but-missing state file must fail closed")
	}
}

func TestVerifyState_MalformedLinesFailClosed(t *testing.T) {
	for _, tc := range []struct{ name, line string }{
		{"unknown-directive", "grant " + shaA},
		{"bad-ok-sha", "ok nothex"},
		{"deny-missing-reason", "deny " + shaA},
		{"bad-range", "okrange " + shaA + " nothex"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := newStateFile(t)
			if err := os.WriteFile(path, []byte(tc.line+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := openVerifyState(); err == nil {
				t.Fatalf("state line %q loaded without error", tc.line)
			}
		})
	}
}

func TestVerifyState_DenyReasonNormalizedToOneLine(t *testing.T) {
	newStateFile(t)
	st, err := openVerifyState()
	if err != nil {
		t.Fatal(err)
	}
	// A hostile or surprising multi-word reason must not corrupt the
	// line-based format; a newline would panic the assertion, so feed the
	// worst legal input (condenseGitReason strips newlines already).
	if err := st.markCommitDenied(shaC, "  gpg:   BAD   signature  "); err != nil {
		t.Fatal(err)
	}
	st2, err := openVerifyState()
	if err != nil {
		t.Fatal(err)
	}
	if got := st2.deny[shaC]; got != "gpg: BAD signature" {
		t.Fatalf("reason round-tripped as %q", got)
	}
}

func TestVerifySigBudget_Parsing(t *testing.T) {
	t.Run("unset-unlimited", func(t *testing.T) {
		t.Setenv(EnvVerifySigBudget, "")
		got, err := verifySigBudget()
		if err != nil || got != 0 {
			t.Fatalf("unset budget = (%d, %v), want (0, nil)", got, err)
		}
	})
	t.Run("value", func(t *testing.T) {
		t.Setenv(EnvVerifySigBudget, "42")
		got, err := verifySigBudget()
		if err != nil || got != 42 {
			t.Fatalf("budget = (%d, %v), want (42, nil)", got, err)
		}
	})
	// The env is set by our own server: nonsense (including a literal "0",
	// which the server never sets — it omits the var instead) fails closed.
	for _, bad := range []string{"0", "-1", "plenty"} {
		t.Run("malformed-"+bad, func(t *testing.T) {
			t.Setenv(EnvVerifySigBudget, bad)
			if _, err := verifySigBudget(); err == nil {
				t.Fatalf("%s=%q parsed without error", EnvVerifySigBudget, bad)
			}
			if _, err := verifySigBudget(); err != nil && !strings.Contains(err.Error(), EnvVerifySigBudget) {
				t.Fatalf("error does not name the env var: %v", err)
			}
		})
	}
}
