// Ref-policy contract for the git receive path (docs/reference/authentication.md `refs`/`ops`
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
	"strings"
)

// Policy ops (docs/reference/authentication.md + branch-protection.md). Unknown ops fail closed in
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

// EphemeralNamespacePrefix is the storage-side ref prefix for writes that
// go through the `+ephemeral` namespace (branch-protection.md "Ephemeral
// namespace quirk"): `refs/heads/main` is stored as
// `refs/namespaces/ephemeral/refs/heads/main`.
const EphemeralNamespacePrefix = "refs/namespaces/ephemeral/"

// NormalizeRefName applies the docs/reference/authentication.md shorthand rule: anything not
// starting with `refs/` (and not exactly `*` or `HEAD`) becomes
// `refs/heads/<value>`.
func NormalizeRefName(s string) string {
	if s == "*" || s == "HEAD" || strings.HasPrefix(s, "refs/") {
		return s
	}
	return "refs/heads/" + s
}

// EphemeralRefName rewrites ref to its ephemeral-namespace storage name,
// normalizing shorthand first (`main` → `refs/heads/main` →
// `refs/namespaces/ephemeral/refs/heads/main`). Policies for a push
// targeting a `+ephemeral` repo are matched against this rewritten name,
// so persistent-branch rules don't police ephemeral pushes unless the
// token uses `*` or an explicit namespaced pattern.
//
// An empty ref (malformed external input) yields "", which never names a
// real ref and never matches a policy pattern (MatchPolicy treats "" as
// no-match) — callers validate emptiness where it matters.
func EphemeralRefName(ref string) string {
	if ref == "" {
		return ""
	}
	return EphemeralNamespacePrefix + NormalizeRefName(ref)
}

// ParseRefPolicies parses the `refs` claim JSON (ordered [pattern, ops]
// tuples) and normalizes each pattern per the docs/reference/authentication.md shorthand rule:
// anything not starting with `refs/` (and not exactly `*` or `HEAD`)
// becomes `refs/heads/<value>`. Returns an error for malformed JSON or
// tuples.
func ParseRefPolicies(raw string) ([]RefPolicy, error) {
	var policies []RefPolicy
	if err := json.Unmarshal([]byte(raw), &policies); err != nil {
		return nil, fmt.Errorf("parse refs policy: %w", err)
	}
	for i := range policies {
		policies[i].Pattern = NormalizeRefName(policies[i].Pattern)
	}
	return policies, nil
}

// MatchPolicy scans normalized policies in order and returns the first
// whose pattern matches ref (normalized with the same shorthand rule);
// ok=false when nothing matches (unpoliced ref). Pattern syntax per
// branch-protection.md: an exact ref name, the standalone `*` (matches
// everything), or a single trailing `*` after a `/` (prefix match, e.g.
// `refs/heads/feature/*` matches nested `feature/a/b`). No mid-pattern
// globs, no segment matching.
func MatchPolicy(policies []RefPolicy, ref string) (RefPolicy, bool) {
	if ref == "" {
		// Malformed external input: explicit no-match instead of a crash.
		// Callers (the update hook) reject empty ref names up front.
		return RefPolicy{}, false
	}
	ref = NormalizeRefName(ref)
	for _, p := range policies {
		if patternMatch(NormalizeRefName(p.Pattern), ref) {
			return p, true
		}
	}
	return RefPolicy{}, false
}

// patternMatch implements the branch-protection.md pattern syntax against
// a normalized ref. Any other use of `*` (mid-pattern, `**`, trailing `*`
// not after `/`) is not a glob: it falls through to the exact comparison,
// which never matches because git forbids `*` in ref names — the rule is
// dead rather than over-matching.
func patternMatch(pattern, ref string) bool {
	if ref == "" {
		return false // empty ref never matches any pattern
	}
	if pattern == "*" {
		return true
	}
	if strings.HasSuffix(pattern, "/*") && strings.Count(pattern, "*") == 1 {
		// Prefix glob: `refs/heads/feature/*` matches everything under
		// `refs/heads/feature/`, including nested `feature/a/b` — but not
		// `refs/heads/feature` itself.
		return strings.HasPrefix(ref, pattern[:len(pattern)-1])
	}
	return pattern == ref
}

// FoldOps merges the deprecated repo-wide `ops` claim into refs as a
// fallback (docs/reference/authentication.md): with no ops it returns refs unchanged; when refs
// contains a `*` pattern entry, ops are merged into that entry's op list
// (deduplicated, existing ops first); otherwise a trailing `["*", ops]`
// rule is appended. `refs` and `ops` are not additive — callers apply this
// exactly once at parse time.
//
// The input slices are never mutated; a fresh slice is returned whenever
// ops is non-empty. Only the first `*` entry is merged into: with
// first-match-wins evaluation, later `*` entries are dead rules.
func FoldOps(refs []RefPolicy, ops []string) []RefPolicy {
	if len(ops) == 0 {
		return refs
	}
	out := make([]RefPolicy, len(refs))
	copy(out, refs)
	for i, p := range out {
		if p.Pattern == "*" {
			out[i].Ops = mergeOps(p.Ops, ops)
			return out
		}
	}
	return append(out, RefPolicy{Pattern: "*", Ops: mergeOps(nil, ops)})
}

// mergeOps returns existing followed by extra, deduplicated in first-seen
// order, always as a freshly allocated slice.
func mergeOps(existing, extra []string) []string {
	merged := make([]string, 0, len(existing)+len(extra))
	seen := make(map[string]bool, len(existing)+len(extra))
	for _, op := range existing {
		if !seen[op] {
			seen[op] = true
			merged = append(merged, op)
		}
	}
	for _, op := range extra {
		if !seen[op] {
			seen[op] = true
			merged = append(merged, op)
		}
	}
	return merged
}
