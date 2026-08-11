// Ref-policy contract for the git receive path (docs/02 `refs`/`ops`
// claims, docs/upstream/guides/branch-protection.md pattern syntax).
//
// FROZEN CONTRACT: the RefPolicy type, its tuple JSON encoding, and the
// function signatures below are the shared surface between the git hook,
// token minting, and tests. Workstream 3 (policy engine) fills in the stub
// bodies; parallel slices build against these exact signatures.
// (Package doc lives in auth.go.)

package auth

import (
	"encoding/json"
	"fmt"
)

// Policy ops (docs/02 + branch-protection.md). Unknown ops fail closed in
// the hook.
const (
	OpNoPush      = "no-push"
	OpNoForcePush = "no-force-push"
	OpVerifySig   = "verify-sig"
)

// RefPolicy is one `[pattern, ops]` tuple from the `refs` claim. Policies
// are ordered; matching is first-match-wins.
type RefPolicy struct {
	Pattern string
	Ops     []string
}

// UnmarshalJSON decodes the wire tuple encoding `["<pattern>", ["<op>", ...]]`.
func (p *RefPolicy) UnmarshalJSON(data []byte) error {
	var tuple []json.RawMessage
	if err := json.Unmarshal(data, &tuple); err != nil {
		return fmt.Errorf("ref policy must be a [pattern, ops] tuple: %w", err)
	}
	if len(tuple) != 2 {
		return fmt.Errorf("ref policy tuple must have exactly 2 elements, got %d", len(tuple))
	}
	if err := json.Unmarshal(tuple[0], &p.Pattern); err != nil {
		return fmt.Errorf("ref policy pattern: %w", err)
	}
	if err := json.Unmarshal(tuple[1], &p.Ops); err != nil {
		return fmt.Errorf("ref policy ops: %w", err)
	}
	return nil
}

// MarshalJSON encodes the wire tuple form; nil ops marshal as [].
func (p RefPolicy) MarshalJSON() ([]byte, error) {
	ops := p.Ops
	if ops == nil {
		ops = []string{}
	}
	return json.Marshal([]any{p.Pattern, ops})
}

// ParseRefPolicies parses the `refs` claim JSON (ordered [pattern, ops]
// tuples) and normalizes each pattern per the docs/02 shorthand rule:
// anything not starting with `refs/` (and not exactly `*` or `HEAD`)
// becomes `refs/heads/<value>`. Returns an error for malformed JSON or
// tuples.
//
// STUB — implemented by workstream 3 (policy engine).
func ParseRefPolicies(raw string) ([]RefPolicy, error) {
	panic("auth.ParseRefPolicies not implemented (workstream 3: policy engine)")
}

// MatchPolicy scans normalized policies in order and returns the first
// whose pattern matches ref (normalized with the same shorthand rule);
// ok=false when nothing matches (unpoliced ref). Pattern syntax per
// branch-protection.md: an exact ref name, the standalone `*` (matches
// everything), or a single trailing `*` after a `/` (prefix match, e.g.
// `refs/heads/feature/*` matches nested `feature/a/b`). No mid-pattern
// globs, no segment matching.
//
// STUB — implemented by workstream 3 (policy engine).
func MatchPolicy(policies []RefPolicy, ref string) (RefPolicy, bool) {
	panic("auth.MatchPolicy not implemented (workstream 3: policy engine)")
}

// FoldOps merges the deprecated repo-wide `ops` claim into refs as a
// fallback (docs/02): with no ops it returns refs unchanged; when refs
// contains a `*` pattern entry, ops are merged into that entry's op list
// (deduplicated, existing ops first); otherwise a trailing `["*", ops]`
// rule is appended. `refs` and `ops` are not additive — callers apply this
// exactly once at parse time.
//
// STUB — implemented by workstream 3 (policy engine).
func FoldOps(refs []RefPolicy, ops []string) []RefPolicy {
	panic("auth.FoldOps not implemented (workstream 3: policy engine)")
}
