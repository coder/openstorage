package githttp

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/ref"
)

// TestUnknownPolicyOpRefusalQuotedBounded: the update hook refuses a ref
// policy op that it does not know with a refusal that git shows the
// pusher, and quotes the op with errtext.Quote, so an op of any length
// makes a short line. The budget-free evaluation (fullEval off), which
// the reservation classifier runs, gives the same refusal.
func TestUnknownPolicyOpRefusalQuotedBounded(t *testing.T) {
	const refName = "refs/heads/main"
	newSHA := strings.Repeat("a", 40)
	for _, tc := range []struct{ name, op, quoted string }{
		{"a long op", strings.Repeat("<", 100_000), `"` + strings.Repeat("<", 256) + `…" (100000 bytes)`},
		{"a short op", "no-fly", `"no-fly"`},
		{"a short op with a quote", `no"fly`, `"no\"fly"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			claim, err := json.Marshal([][]any{{"*", []string{tc.op}}})
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv(EnvRefsPolicy, string(claim))
			t.Setenv(EnvOps, "")
			want := "unknown policy op " + tc.quoted + " for " + refName
			for _, fullEval := range []bool{true, false} {
				err := evalRefPolicy(t.Context(), "repo", refName, ref.ZeroSHA, newSHA, fullEval)
				if err == nil || err.Error() != want || !ShownToPusher(err) {
					t.Fatalf("evalRefPolicy(fullEval=%v) = %.300v; want a refusal %.300q", fullEval, err, want)
				}
			}
		})
	}
}
