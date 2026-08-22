package githttp

// Unit tests for the issue #59 per-push verify-sig state: file round-trips
// across "processes" (fresh loads of the same file), budget accounting, and
// fail-closed parsing of malformed state and budget values.

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/coder/code-storage/internal/ref"
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

	// A later hook process loads the same files fresh; commit verdicts are
	// lazy (range hits never need them).
	st2, err := openVerifyState()
	if err != nil {
		t.Fatal(err)
	}
	if !st2.ranges[rangeKey(shaA, shaB)] {
		t.Fatal("range verdict not available before loadVerdicts (sidecar must load eagerly)")
	}
	if err := st2.loadVerdicts(); err != nil {
		t.Fatal(err)
	}
	if !st2.ok[shaA] {
		t.Fatal("ok verdict lost across loads")
	}
	if reason := st2.deny[shaB]; reason != "commit is not signed" {
		t.Fatalf("deny verdict = %q, want reason preserved", reason)
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
	st, err := openVerifyState()
	if err != nil {
		// Missing sidecars are fine; only the verdict file is mandatory.
		t.Fatalf("open with missing sidecars: %v", err)
	}
	if err := st.loadVerdicts(); err == nil {
		t.Fatal("set-but-missing verdict file must fail closed")
	}
}

func TestVerifyState_MalformedLinesFailClosed(t *testing.T) {
	for _, tc := range []struct{ name, line string }{
		{"unknown-directive", "grant " + shaA},
		{"bad-ok-sha", "ok nothex"},
		{"deny-missing-reason", "deny " + shaA},
		{"range-in-verdict-file", "okrange " + shaA + " " + shaB},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := newStateFile(t)
			if err := os.WriteFile(path, []byte(tc.line+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			st, err := openVerifyState()
			if err != nil {
				t.Fatal(err)
			}
			if err := st.loadVerdicts(); err == nil {
				t.Fatalf("verdict line %q loaded without error", tc.line)
			}
		})
	}
	t.Run("bad-range-sidecar", func(t *testing.T) {
		path := newStateFile(t)
		if err := os.WriteFile(path+verifyRangesSuffix, []byte("okrange "+shaA+" nothex\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := openVerifyState(); err == nil {
			t.Fatal("malformed range sidecar loaded without error")
		}
	})
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
	if err := st2.loadVerdicts(); err != nil {
		t.Fatal(err)
	}
	if got := st2.deny[shaC]; got != "gpg: BAD signature" {
		t.Fatalf("reason round-tripped as %q", got)
	}
}

func TestVerifyState_WalkedAccountingRoundTrip(t *testing.T) {
	path := newStateFile(t)
	st, err := openVerifyState()
	if err != nil {
		t.Fatal(err)
	}
	if err := st.markCommitOK(shaA); err != nil {
		t.Fatal(err)
	}
	if err := st.markWalked(0); err != nil {
		t.Fatal(err)
	}
	if err := st.markWalked(40); err != nil {
		t.Fatal(err)
	}
	if err := st.markWalked(2); err != nil {
		t.Fatal(err)
	}
	st2, err := openVerifyState()
	if err != nil {
		t.Fatal(err)
	}
	if st2.walked != 42 {
		t.Fatalf("walked = %d, want 42 accumulated across loads", st2.walked)
	}
	if err := st2.loadVerdicts(); err != nil {
		t.Fatal(err)
	}
	if !st2.ok[shaA] || st2.spent != 1 {
		t.Fatalf("ok=%v spent=%d, want the verdict to survive counter updates", st2.ok[shaA], st2.spent)
	}
	// Counter-separation invariant (Codex rounds 5–8): the verdict file
	// never carries walk records — the counter lives in a fixed-size
	// sibling — so neither per-walk appends nor per-walk rewrites can
	// amplify state processing with the ref count.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "walked") {
		t.Fatalf("verdict state file carries walk records:\n%s", data)
	}
	counter, err := os.ReadFile(path + verifyWalkedCounterSuffix)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(counter)) != "42" {
		t.Fatalf("counter file = %q, want 42", counter)
	}
}

func TestVerifyState_MalformedWalkedCounterFailsClosed(t *testing.T) {
	for _, bad := range []string{"x", "-1", "1 2"} {
		path := newStateFile(t)
		if err := os.WriteFile(path+verifyWalkedCounterSuffix, []byte(bad+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := openVerifyState(); err == nil {
			t.Fatalf("walked counter %q loaded without error", bad)
		}
	}
}

func TestWalkBudget(t *testing.T) {
	if got := walkBudget(0); got != 0 {
		t.Fatalf("walkBudget(0) = %d, want 0 (unbounded)", got)
	}
	if got := walkBudget(10000); got != 1000000 {
		t.Fatalf("walkBudget(10000) = %d, want 1000000", got)
	}
	if got := walkBudget(int(int64(1) << 60)); got != 1<<62 {
		t.Fatalf("huge budget = %d, want saturation at 1<<62", got)
	}
}

// TestIntroducedPending_WalkAllowance pins the round-3 traversal bound:
// every examined rev-list line — cached verdicts included — charges the
// push's cumulative walk allowance, so re-walking already-verified history
// once per ref is bounded too.
func TestIntroducedPending_WalkAllowance(t *testing.T) {
	dir := t.TempDir()
	run := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-c", "user.email=t@t", "-c", "user.name=t"}, args...)...)
		cmd.Dir = dir
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	run("init", "-b", "main", ".")
	for i := 0; i < 5; i++ {
		run("commit", "--allow-empty", "-m", fmt.Sprintf("c%d", i))
	}
	tip := run("rev-parse", "HEAD")
	t.Chdir(dir)
	t.Setenv(EnvVerifyState, "")

	t.Run("allowance-trips-across-calls", func(t *testing.T) {
		st, err := openVerifyState()
		if err != nil {
			t.Fatal(err)
		}
		// budget 1 ⇒ allowance 100; pre-charge 99 examined commits, and
		// mark the whole history verified so the spawn budget cannot trip
		// first: only the walk allowance can reject.
		for _, sha := range strings.Fields(run("rev-list", tip)) {
			st.ok[sha] = true
		}
		if err := st.markWalked(99); err != nil {
			t.Fatal(err)
		}
		_, _, err = introducedPending(t.Context(), st, "refs/heads/x", ref.ZeroSHA, tip, 1)
		if err == nil || !strings.Contains(err.Error(), "history-walk allowance") {
			t.Fatalf("err = %v, want history-walk allowance rejection", err)
		}
		if st.walked <= 99 {
			t.Fatalf("walked = %d, want the aborted walk charged on top of 99", st.walked)
		}
	})

	t.Run("cached-commits-still-charge", func(t *testing.T) {
		st, err := openVerifyState()
		if err != nil {
			t.Fatal(err)
		}
		for _, sha := range strings.Fields(run("rev-list", tip)) {
			st.ok[sha] = true
		}
		pending, examined, err := introducedPending(t.Context(), st, "refs/heads/x", ref.ZeroSHA, tip, 1000)
		if err != nil {
			t.Fatal(err)
		}
		if len(pending) != 0 {
			t.Fatalf("pending = %v, want none (all cached)", pending)
		}
		if examined != 5 || st.walked != 5 {
			t.Fatalf("examined = %d walked = %d, want 5/5 (cached commits charge the allowance)", examined, st.walked)
		}
	})
}

func TestChargeWalk(t *testing.T) {
	t.Run("accounting-and-persistence", func(t *testing.T) {
		newStateFile(t)
		st, err := openVerifyState()
		if err != nil {
			t.Fatal(err)
		}
		if err := st.chargeWalk(0, 10, "refs/heads/x"); err != nil || st.walked != 0 {
			t.Fatalf("zero charge = (%v, walked %d), want free no-op", err, st.walked)
		}
		if err := st.chargeWalk(9, 10, "refs/heads/x"); err != nil {
			t.Fatal(err)
		}
		// Unbounded (budget 0) never rejects but still accounts.
		if err := st.chargeWalk(1000, 0, "refs/heads/x"); err != nil || st.walked != 1009 {
			t.Fatalf("unbounded charge = (%v, walked %d)", err, st.walked)
		}
		// The charge persists for the next hook process.
		st2, err := openVerifyState()
		if err != nil {
			t.Fatal(err)
		}
		if st2.walked != 1009 {
			t.Fatalf("persisted walked = %d, want 1009", st2.walked)
		}
	})

	t.Run("rejection-is-terminal", func(t *testing.T) {
		newStateFile(t)
		st, err := openVerifyState()
		if err != nil {
			t.Fatal(err)
		}
		if err := st.chargeWalk(9, 10, "refs/heads/x"); err != nil {
			t.Fatal(err)
		}
		if err := st.chargeWalk(2, 10, "refs/heads/x"); err == nil || !strings.Contains(err.Error(), "history-walk allowance") {
			t.Fatalf("over-allowance charge = %v, want allowance rejection", err)
		}
		if st.walked != 9 {
			t.Fatalf("walked = %d, want 9 (rejected charge not applied)", st.walked)
		}
	})
}

func TestVerifyState_ExhaustionMarkerShortCircuits(t *testing.T) {
	newStateFile(t)
	st, err := openVerifyState()
	if err != nil {
		t.Fatal(err)
	}
	if err := st.chargeWalk(5, 3, "refs/heads/x"); err == nil || !strings.Contains(err.Error(), "history-walk allowance") {
		t.Fatalf("over-allowance charge = %v, want allowance rejection", err)
	}
	// Every later evaluation of the push fails closed in constant time —
	// openVerifyState returns the persisted rejection without loading any
	// sidecar (Codex round 16).
	if _, err := openVerifyState(); err == nil || !strings.Contains(err.Error(), "history-walk allowance") {
		t.Fatalf("post-exhaustion open = %v, want the persisted rejection", err)
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
