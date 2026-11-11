package auth

import (
	"encoding/json"
	"reflect"
	"testing"
)

// The tuple wire encoding of RefPolicy is a frozen contract (docs/reference/authentication.md `refs`
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

func TestNormalizeRefName(t *testing.T) {
	for in, want := range map[string]string{
		"main":              "refs/heads/main",
		"agents/*":          "refs/heads/agents/*",
		"*":                 "*",
		"HEAD":              "HEAD",
		"refs/heads/main":   "refs/heads/main",
		"refs/tags/*":       "refs/tags/*",
		"refs/notes/commit": "refs/notes/commit",
	} {
		if got := NormalizeRefName(in); got != want {
			t.Errorf("NormalizeRefName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseRefPolicies(t *testing.T) {
	policies, err := ParseRefPolicies(`[["main",["no-push"]],["agents/*",[]],["*",["no-force-push"]]]`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	want := []RefPolicy{
		{Pattern: "refs/heads/main", Ops: []string{OpNoPush}},
		{Pattern: "refs/heads/agents/*", Ops: []string{}},
		{Pattern: "*", Ops: []string{OpNoForcePush}},
	}
	if !reflect.DeepEqual(policies, want) {
		t.Errorf("policies = %+v, want %+v", policies, want)
	}

	for _, bad := range []string{"", "not json", `{"pattern":"main"}`, `[["main"]]`} {
		if _, err := ParseRefPolicies(bad); err == nil {
			t.Errorf("ParseRefPolicies(%q) succeeded, want error", bad)
		}
	}
}

func TestMatchPolicy_GlobSemantics(t *testing.T) {
	cases := []struct {
		pattern, ref string
		match        bool
	}{
		// Exact ref names.
		{"refs/heads/main", "refs/heads/main", true},
		{"refs/heads/main", "refs/heads/main2", false},
		{"refs/heads/main", "refs/heads/ma", false},
		{"refs/tags/v1", "refs/tags/v1", true},
		{"HEAD", "HEAD", true},
		// Shorthand normalization applies to both pattern and ref.
		{"main", "refs/heads/main", true},
		{"refs/heads/main", "main", true},
		{"main", "refs/tags/main", false},
		// Standalone `*` matches every ref, in any namespace.
		{"*", "refs/heads/main", true},
		{"*", "refs/tags/v1", true},
		{"*", "refs/namespaces/ephemeral/refs/heads/main", true},
		// Trailing `/*` is a prefix match, across `/` (no segment matching).
		{"refs/heads/feature/*", "refs/heads/feature/x", true},
		{"refs/heads/feature/*", "refs/heads/feature/a/b", true},
		{"refs/heads/feature/*", "refs/heads/feature", false},
		{"refs/heads/feature/*", "refs/heads/featurette", false},
		{"refs/heads/*", "refs/heads/main", true},
		{"refs/heads/*", "refs/tags/v1", false},
		{"agents/*", "refs/heads/agents/a/b", true},
		// Mid-pattern globs are not globs: never match real refs.
		{"refs/*/main", "refs/heads/main", false},
		{"refs/he*ds/main", "refs/heads/main", false},
		{"refs/heads/*/x", "refs/heads/a/x", false},
		// Trailing `*` not after `/`, and `**`, are not globs either.
		{"refs/heads/feat*", "refs/heads/feature", false},
		{"refs/heads/**", "refs/heads/main", false},
	}
	for _, c := range cases {
		_, ok := MatchPolicy([]RefPolicy{{Pattern: c.pattern}}, c.ref)
		if ok != c.match {
			t.Errorf("MatchPolicy(%q, %q) = %v, want %v", c.pattern, c.ref, ok, c.match)
		}
	}
}

func TestMatchPolicy_FirstMatchWins(t *testing.T) {
	policies := []RefPolicy{
		{Pattern: "refs/heads/main", Ops: []string{}},
		{Pattern: "refs/heads/*", Ops: []string{OpNoForcePush}},
		{Pattern: "*", Ops: []string{OpNoPush}},
	}
	rule, ok := MatchPolicy(policies, "refs/heads/main")
	if !ok || len(rule.Ops) != 0 {
		t.Errorf("main matched %+v (ok=%v), want the empty passthrough rule", rule, ok)
	}
	rule, ok = MatchPolicy(policies, "refs/heads/dev")
	if !ok || !reflect.DeepEqual(rule.Ops, []string{OpNoForcePush}) {
		t.Errorf("dev matched %+v (ok=%v), want the no-force-push rule", rule, ok)
	}
	rule, ok = MatchPolicy(policies, "refs/tags/v1")
	if !ok || !reflect.DeepEqual(rule.Ops, []string{OpNoPush}) {
		t.Errorf("tag matched %+v (ok=%v), want the catch-all no-push rule", rule, ok)
	}

	// No match: unpoliced ref.
	if rule, ok := MatchPolicy(policies[:2], "refs/tags/v1"); ok {
		t.Errorf("unpoliced ref matched %+v", rule)
	}
	if rule, ok := MatchPolicy(nil, "refs/heads/main"); ok {
		t.Errorf("nil policies matched %+v", rule)
	}
}

func TestFoldOps(t *testing.T) {
	refs := []RefPolicy{{Pattern: "refs/heads/main", Ops: []string{}}}

	// No ops: refs returned unchanged.
	if got := FoldOps(refs, nil); !reflect.DeepEqual(got, refs) {
		t.Errorf("FoldOps(refs, nil) = %+v, want %+v", got, refs)
	}

	// No `*` entry: trailing catch-all appended, input not mutated.
	got := FoldOps(refs, []string{OpNoPush})
	want := []RefPolicy{
		{Pattern: "refs/heads/main", Ops: []string{}},
		{Pattern: "*", Ops: []string{OpNoPush}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("FoldOps append = %+v, want %+v", got, want)
	}
	if len(refs) != 1 {
		t.Errorf("FoldOps mutated input refs: %+v", refs)
	}

	// Existing `*` entry: ops merged in, deduplicated, existing first.
	refsStar := []RefPolicy{
		{Pattern: "*", Ops: []string{OpNoForcePush}},
		{Pattern: "refs/heads/dev", Ops: []string{}},
	}
	got = FoldOps(refsStar, []string{OpNoPush, OpNoForcePush})
	want = []RefPolicy{
		{Pattern: "*", Ops: []string{OpNoForcePush, OpNoPush}},
		{Pattern: "refs/heads/dev", Ops: []string{}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("FoldOps merge = %+v, want %+v", got, want)
	}
	if !reflect.DeepEqual(refsStar[0].Ops, []string{OpNoForcePush}) {
		t.Errorf("FoldOps mutated input `*` entry: %+v", refsStar[0])
	}

	// Empty refs + ops: single catch-all (the deprecated repo-wide claim).
	got = FoldOps(nil, []string{OpNoPush})
	want = []RefPolicy{{Pattern: "*", Ops: []string{OpNoPush}}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("FoldOps(nil, ops) = %+v, want %+v", got, want)
	}
}

// docs/reference/authentication.md e2e shape (WS5 #3): refs=[["main",[]]] + ops=["no-push"] must
// allow main and reject everything else (fold appends, first match wins).
func TestFoldOps_SpecificRefStillWins(t *testing.T) {
	refs, err := ParseRefPolicies(`[["main",[]]]`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	folded := FoldOps(refs, []string{OpNoPush})

	rule, ok := MatchPolicy(folded, "refs/heads/main")
	if !ok || len(rule.Ops) != 0 {
		t.Errorf("main matched %+v (ok=%v), want empty passthrough", rule, ok)
	}
	rule, ok = MatchPolicy(folded, "refs/heads/other")
	if !ok || !reflect.DeepEqual(rule.Ops, []string{OpNoPush}) {
		t.Errorf("other matched %+v (ok=%v), want folded no-push", rule, ok)
	}
}

func TestEphemeralRefName(t *testing.T) {
	for in, want := range map[string]string{
		"refs/heads/main": "refs/namespaces/ephemeral/refs/heads/main",
		"main":            "refs/namespaces/ephemeral/refs/heads/main",
		"refs/tags/v1":    "refs/namespaces/ephemeral/refs/tags/v1",
		// A bare name is a branch name, also HEAD and *: NormalizeRefName
		// keeps those two for policy patterns, but a stored ref is a full name.
		"HEAD":  "refs/namespaces/ephemeral/refs/heads/HEAD",
		"*":     "refs/namespaces/ephemeral/refs/heads/*",
		"refsx": "refs/namespaces/ephemeral/refs/heads/refsx",
		"":      "",
	} {
		if got := EphemeralRefName(in); got != want {
			t.Errorf("EphemeralRefName(%q) = %q, want %q", in, got, want)
		}
	}
}

// A rule on the name an ephemeral HEAD branch had before EphemeralRefName
// mapped bare names under refs/heads/ still polices that branch.
func TestParseRefPoliciesLegacyEphemeralHEAD(t *testing.T) {
	policies, err := ParseRefPolicies(`[["refs/namespaces/ephemeral/HEAD", ["no-push"]]]`)
	if err != nil {
		t.Fatal(err)
	}
	if rule, ok := MatchPolicy(policies, EphemeralRefName("HEAD")); !ok || !reflect.DeepEqual(rule.Ops, []string{OpNoPush}) {
		t.Fatalf("ephemeral HEAD matched %+v (ok=%v), want the legacy no-push rule", rule, ok)
	}
	if rule, ok := MatchPolicy(policies, EphemeralRefName("main")); ok {
		t.Fatalf("ephemeral main matched %+v, want no match", rule)
	}
}

// Ephemeral namespace quirk (branch-protection.md): persistent-branch rules
// don't police the ephemeral copy; explicit namespaced patterns and the
// catch-all `*` do.
func TestMatchPolicy_EphemeralQuirk(t *testing.T) {
	eph := EphemeralRefName("main")

	// Persistent rules (exact and prefix glob) don't fire on the rewritten name.
	for _, pattern := range []string{"main", "refs/heads/main", "refs/heads/*"} {
		if rule, ok := MatchPolicy([]RefPolicy{{Pattern: pattern, Ops: []string{OpNoPush}}}, eph); ok {
			t.Errorf("persistent pattern %q matched ephemeral %q: %+v", pattern, eph, rule)
		}
	}

	// Explicit rewritten path, namespace prefix glob, and `*` all match.
	for _, pattern := range []string{
		"refs/namespaces/ephemeral/refs/heads/main",
		"refs/namespaces/ephemeral/*",
		"*",
	} {
		if _, ok := MatchPolicy([]RefPolicy{{Pattern: pattern, Ops: []string{OpNoPush}}}, eph); !ok {
			t.Errorf("pattern %q did not match ephemeral %q", pattern, eph)
		}
	}
}
