package main

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/coder/code-storage/internal/auth"
	"github.com/coder/code-storage/internal/githttp"
	"github.com/coder/code-storage/internal/meta"
	upsync "github.com/coder/code-storage/internal/sync"
)

// forwardPokeStore backs enqueueSyncOrForward tests: one canned upstream
// row plus a recording RequestPush; everything else inherits failingStore's
// panics so unexpected store traffic fails loudly.
type forwardPokeStore struct {
	failingStore
	up    meta.RepoUpstream
	marks []bool
}

func (s *forwardPokeStore) GetRepoUpstream(context.Context, string) (meta.RepoUpstream, error) {
	return s.up, nil
}

func (s *forwardPokeStore) RequestPush(_ context.Context, _ string, force bool) error {
	s.marks = append(s.marks, force)
	return nil
}

// TestEnqueueSyncOrForwardPolicedCallerGate: the forward reroute fails
// closed for callers whose token carries ANY ref-policy op (Codex round 3
// on #90) or lacks git:read (security round: a write-only token must not
// export content it cannot clone) — matching POST /repos/push-upstream —
// while server-initiated writes (no claims) and read+write unpoliced
// tokens forward, and the PULL poke stays ungated (documented invariant-11
// behavior this slice does not change).
func TestEnqueueSyncOrForwardPolicedCallerGate(t *testing.T) {
	bg := context.Background()
	rw := []string{auth.ScopeGitRead, auth.ScopeGitWrite}
	cases := []struct {
		name     string
		ctx      context.Context
		forward  bool // upstream row's ForwardOnPush
		wantPush bool
		wantPull bool
	}{
		{"forward_server_initiated_no_claims", bg, true, true, false},
		{"forward_unpoliced_token", auth.WithClaims(bg, &auth.Claims{Scopes: rw}), true, true, false},
		{"forward_pattern_without_ops", auth.WithClaims(bg, &auth.Claims{Scopes: rw,
			Refs: json.RawMessage(`[["refs/heads/*", []]]`)}), true, true, false},
		{"forward_policed_refs_denied", auth.WithClaims(bg, &auth.Claims{Scopes: rw,
			Refs: json.RawMessage(`[["refs/heads/main", ["no-push"]]]`)}), true, false, false},
		{"forward_deprecated_ops_denied", auth.WithClaims(bg, &auth.Claims{Scopes: rw,
			Ops: []string{"no-force-push"}}), true, false, false},
		{"forward_malformed_refs_fails_closed", auth.WithClaims(bg, &auth.Claims{Scopes: rw,
			Refs: json.RawMessage(`{not json`)}), true, false, false},
		{"forward_write_only_token_denied", auth.WithClaims(bg, &auth.Claims{
			Scopes: []string{auth.ScopeGitWrite}}), true, false, false},
		{"pull_policed_still_pokes", auth.WithClaims(bg, &auth.Claims{Scopes: []string{auth.ScopeGitWrite},
			Refs: json.RawMessage(`[["refs/heads/main", ["no-push"]]]`)}), false, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := &forwardPokeStore{up: meta.RepoUpstream{RepoID: "r1", ForwardOnPush: tc.forward}}
			var pulled, pushed int
			poke := enqueueSyncOrForward(st,
				func(context.Context, upsync.Job) error { pulled++; return nil },
				func(context.Context, upsync.Job) error { pushed++; return nil },
				"acme", forwardDeniedFromContext)
			if err := poke(tc.ctx, "r1"); err != nil {
				t.Fatalf("poke: %v", err)
			}
			if got := pushed > 0; got != tc.wantPush {
				t.Fatalf("push enqueued = %v, want %v", got, tc.wantPush)
			}
			if marked := len(st.marks) > 0; marked != tc.wantPush {
				t.Fatalf("RequestPush marked = %v, want %v (mark-before-enqueue must match the gate)", marked, tc.wantPush)
			}
			if got := pulled > 0; got != tc.wantPull {
				t.Fatalf("pull enqueued = %v, want %v", got, tc.wantPull)
			}
		})
	}
}

// TestHookCallerDeniedForward: the hook subprocess derives the same
// verdict from the enforceRefPolicy environment plus the scope list,
// failing closed on missing or malformed values.
func TestHookCallerDeniedForward(t *testing.T) {
	rw := `["git:read","git:write"]`
	cases := []struct {
		name, scopes, refs, ops string
		want                    bool
	}{
		{"read_write_unpoliced", rw, "", "", false},
		{"refs_policy_denied", rw, `[["refs/heads/main", ["no-push"]]]`, "", true},
		{"pattern_without_ops", rw, `[["refs/heads/*", []]]`, "", false},
		{"deprecated_ops_denied", rw, "", `["verify-sig"]`, true},
		{"malformed_refs_fails_closed", rw, `{not json`, "", true},
		{"malformed_ops_fails_closed", rw, "", `{not json`, true},
		{"missing_scopes_fails_closed", "", "", "", true},
		{"write_only_denied", `["git:write"]`, "", "", true},
		{"malformed_scopes_fails_closed", `{not json`, "", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(githttp.EnvScopes, tc.scopes)
			t.Setenv(githttp.EnvRefsPolicy, tc.refs)
			t.Setenv(githttp.EnvOps, tc.ops)
			if got := hookCallerDeniedForward(); got != tc.want {
				t.Fatalf("hookCallerDeniedForward() = %v, want %v", got, tc.want)
			}
		})
	}
}
