package githttp

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

// Round-trip proof for the RefPolicy tuple encoding: Marshal and Unmarshal
// must be inverses, and nil ops must marshal as [] (not null).
func TestRefPolicy_JSONRoundTrip(t *testing.T) {
	in := `[["main",["no-force-push"]],["refs/tags/*",["no-push"]],["*",[]]]`
	policies, err := ParseRefPolicies(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := policies[0].Pattern; got != "refs/heads/main" {
		t.Fatalf("normalization: pattern[0] = %q, want refs/heads/main", got)
	}

	encoded, err := json.Marshal(policies)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	again, err := ParseRefPolicies(string(encoded))
	if err != nil {
		t.Fatalf("re-parse: %v", err)
	}
	if !reflect.DeepEqual(policies, again) {
		t.Fatalf("round trip mismatch:\n first: %+v\nsecond: %+v", policies, again)
	}

	nilOps, err := json.Marshal(RefPolicy{Pattern: "refs/heads/x"})
	if err != nil {
		t.Fatal(err)
	}
	if string(nilOps) != `["refs/heads/x",[]]` {
		t.Fatalf("nil ops marshaled as %s, want [\"refs/heads/x\",[]]", nilOps)
	}
}

// MintToken must emit the refs claim through RefPolicy's tuple encoding
// (normalized), and VerifyToken must read it back.
func TestMintToken_RefsClaimRoundTrip(t *testing.T) {
	priv, err := GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	token, err := MintToken(priv, "spike", "r1", []string{ScopeGitWrite},
		`[["main",["no-push"]],["*",[]]]`, time.Hour)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	claims, err := VerifyToken(&priv.PublicKey, token)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	want := []RefPolicy{
		{Pattern: "refs/heads/main", Ops: []string{"no-push"}},
		{Pattern: "*", Ops: []string{}},
	}
	if !reflect.DeepEqual(claims.Refs, want) {
		t.Fatalf("refs claim = %+v, want %+v", claims.Refs, want)
	}
}
