package auth

import (
	"encoding/json"
	"testing"
)

// The tuple wire encoding of RefPolicy is a frozen contract (docs/02 `refs`
// claim); ParseRefPolicies/MatchPolicy/FoldOps behavior is covered by the
// policy-engine workstream once the stubs are implemented.
func TestRefPolicyTupleCodec(t *testing.T) {
	var policies []RefPolicy
	raw := `[["refs/heads/main",["no-push","verify-sig"]],["*",[]]]`
	if err := json.Unmarshal([]byte(raw), &policies); err != nil {
		t.Fatalf("unmarshal tuples: %v", err)
	}
	if len(policies) != 2 {
		t.Fatalf("got %d policies, want 2", len(policies))
	}
	if policies[0].Pattern != "refs/heads/main" || len(policies[0].Ops) != 2 ||
		policies[0].Ops[0] != OpNoPush || policies[0].Ops[1] != OpVerifySig {
		t.Errorf("policy[0] = %+v", policies[0])
	}
	if policies[1].Pattern != "*" || len(policies[1].Ops) != 0 {
		t.Errorf("policy[1] = %+v", policies[1])
	}

	// Round-trip, including nil ops marshaling as [].
	out, err := json.Marshal([]RefPolicy{{Pattern: "main"}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != `[["main",[]]]` {
		t.Errorf("marshal = %s, want [[\"main\",[]]]", out)
	}

	// Malformed tuples are rejected.
	for _, bad := range []string{
		`[["main"]]`,                 // 1 element
		`[["main",["no-push"],"x"]]`, // 3 elements
		`[{"pattern":"main"}]`,       // object, not tuple
		`[[42,["no-push"]]]`,         // non-string pattern
		`[["main","no-push"]]`,       // ops not an array
	} {
		if err := json.Unmarshal([]byte(bad), &policies); err == nil {
			t.Errorf("unmarshal %q succeeded, want error", bad)
		}
	}
}
