package api

import (
	"encoding/json"
	"net/url"
	"testing"

	"github.com/coder/openstorage/internal/auth"
)

// TestListNextCursorAbsentWhenExhausted pins upstream's pagination contract
// (spec 2026-10-02, issue #375) on the raw JSON of every list response with a
// shared next_cursor: a has_more page carries a non-empty `next_cursor`, and an
// exhausted or empty page omits the key entirely. A decoded Go string cannot
// tell `"next_cursor":""` from an absent key, so this test inspects key
// presence. grep is excluded on purpose: upstream still documents its
// next_cursor as nullable and that schema did not change.
func TestListNextCursorAbsentWhenExhausted(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		fx := coreSeedFixture(t, e, org)
		nfx := rnotesSeedFixture(t, e)
		emptyRepo, emptyTok := coreFreshRepo(t, e, org)
		orgTok := e.mint(t, tokenSpec{iss: org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeOrgRead}})
		emptyOrg := e.newOrg(t)
		emptyOrgTok := e.mint(t, tokenSpec{iss: emptyOrg, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeOrgRead}})

		// check fetches path and asserts has_more plus next_cursor presence.
		check := func(t *testing.T, path, token string, wantMore bool) {
			t.Helper()
			resp, body := coreGet(t, e, "GET", path, token, nil)
			coreRequireStatus(t, resp, body, 200)
			var raw map[string]json.RawMessage
			coreDecode(t, body, &raw)
			var hasMore bool
			coreDecode(t, raw["has_more"], &hasMore)
			if hasMore != wantMore {
				t.Fatalf("%s: has_more = %v, want %v (body %s)", path, hasMore, wantMore, body)
			}
			cur, present := raw["next_cursor"]
			if !wantMore {
				if present {
					t.Fatalf("%s: exhausted page carries next_cursor %s; upstream says absent", path, cur)
				}
				return
			}
			var s string
			if !present || json.Unmarshal(cur, &s) != nil || s == "" {
				t.Fatalf("%s: has_more page needs a non-empty string next_cursor, got %s (present=%v)", path, cur, present)
			}
		}

		// Each repo-scoped list, in both path styles where the fixture
		// exposes the repo name (alias byte-identity is pinned by
		// TestAliasV1Equivalence; notes/refs runs on the v1 style only).
		lists := []struct {
			name, suffix, token string
			repoURL             string
		}{
			{"branches", "/branches", fx.token, fx.repo.URL},
			{"tags", "/tags", fx.token, fx.repo.URL},
			{"commits", "/commits", fx.token, fx.repo.URL},
			{"files", "/files?ref=main&recursive=true", fx.token, fx.repo.URL},
			{"files/metadata", "/files/metadata?ref=main", fx.token, fx.repo.URL},
			{"notes/refs", "/notes/refs", nfx.token, ""},
		}
		for _, l := range lists {
			prefixes := []string{"/api/v1/repos"}
			if l.repoURL != "" {
				prefixes = append(prefixes, "/api/repos/"+url.PathEscape(l.repoURL))
			}
			for _, prefix := range prefixes {
				base := prefix + l.suffix
				sep := "?"
				if u, err := url.Parse(base); err != nil {
					t.Fatal(err)
				} else if u.RawQuery != "" {
					sep = "&"
				}
				t.Run(l.name+" "+prefix, func(t *testing.T) {
					check(t, base+sep+"limit=1", l.token, true)
					check(t, base+sep+"limit=100", l.token, false)
				})
			}
		}

		t.Run("empty pages", func(t *testing.T) {
			if emptyRepo.URL == "" {
				t.Fatal("fresh repo has no URL")
			}
			check(t, "/api/v1/repos/branches", emptyTok, false)
			check(t, "/api/v1/repos/tags", emptyTok, false)
			check(t, "/api/v1/repos", emptyOrgTok, false)
		})

		t.Run("repos", func(t *testing.T) {
			// The org holds the fixture repo and the fresh repo, so
			// limit=1 leaves a second page.
			check(t, "/api/v1/repos?limit=1", orgTok, true)
			check(t, "/api/v1/repos?limit=100", orgTok, false)
		})
	})
}
