package api

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"
)

// TestBranchCreateNamesEveryInvalidField: POST branches/create names each
// invalid body field (upstream: `invalid_params` "names each invalid
// field"), in the order of the docs table. The two rules that name no
// field (a base is required, force needs a target_branch) add their
// message only.
func TestBranchCreateNamesEveryInvalidField(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := ipNewFixture(t, e)
		const path = "/api/v1/repos/branches/create"
		sha := strings.Repeat("a", 40)
		ipRun(t, e, []ipCase{
			{
				// A named target is not an allocation, so force and a
				// non-zero guard cannot break here: every other field does.
				name: "every field of a named target", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"target_branch":"bad..name","expected_target_sha":"x"}`,
				wantError: "invalid target_branch; base_ref, base_branch, or preferred_base is required; invalid expected_target_sha",
				want: []ipParam{
					{"target_branch", "must be a valid branch name"},
					{"expected_target_sha", "must be a full commit SHA"},
				},
			},
			{
				// An omitted target_branch asks for an allocated name, which
				// force and a guard cannot hold. The docs order puts the base
				// before force and the guard.
				name: "every field of an allocated target", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"force":true,"expected_target_sha":"x"}`,
				wantError: "base_ref, base_branch, or preferred_base is required; force requires target_branch; invalid expected_target_sha",
				want:      []ipParam{{"expected_target_sha", "must be a full commit SHA"}},
			},
			{
				name: "an allocated target takes only the zero guard", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"force":true,"expected_target_sha":"` + sha + `"}`,
				wantError: "base_ref, base_branch, or preferred_base is required; force requires target_branch; expected_target_sha requires target_branch",
				want:      []ipParam{{"expected_target_sha", "requires target_branch unless it is 40 zeros"}},
			},
			{
				name: "ephemeral HEAD target and a bad guard", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"target_branch":"HEAD","target_is_ephemeral":true,"base_ref":"main","expected_target_sha":"x"}`,
				wantError: "invalid target_branch; invalid expected_target_sha",
				want: []ipParam{
					{"target_branch", "must not be HEAD for an ephemeral branch"},
					{"expected_target_sha", "must be a full commit SHA"},
				},
			},
			{
				// The deprecated base_branch and preferred_base satisfy the
				// base rule too, so only the guard breaks here.
				name: "a deprecated base counts", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"target_branch":"ok","base_branch":"main","expected_target_sha":"x"}`,
				wantError: "invalid expected_target_sha",
				want:      []ipParam{{"expected_target_sha", "must be a full commit SHA"}},
			},
			{
				name: "a preferred base counts", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"target_branch":"bad..name","preferred_base":"main"}`,
				wantError: "invalid target_branch",
				want:      []ipParam{{"target_branch", "must be a valid branch name"}},
			},

			// A named target, even an invalid one, is not an allocation:
			// force and a non-zero guard are valid, and nothing names them.
			{
				name: "an invalid target does not make force or the guard invalid", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"target_branch":"bad..name","base_ref":"main","force":true,"expected_target_sha":"` + sha + `"}`,
				wantError: "invalid target_branch",
				want:      []ipParam{{"target_branch", "must be a valid branch name"}},
			},
			{
				name: "an uppercase guard is a full sha", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"target_branch":"bad..name","base_ref":"main","expected_target_sha":"` + strings.ToUpper(sha) + `"}`,
				wantError: "invalid target_branch",
				want:      []ipParam{{"target_branch", "must be a valid branch name"}},
			},
			{
				name: "a persistent HEAD target is a valid name", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"target_branch":"HEAD","base_ref":"main","expected_target_sha":"x"}`,
				wantError: "invalid expected_target_sha",
				want:      []ipParam{{"expected_target_sha", "must be a full commit SHA"}},
			},
			{
				name: "a null target is named, not allocated", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"target_branch":null,"base_ref":"main","force":true}`,
				wantError: "invalid target_branch",
				want:      []ipParam{{"target_branch", "must be a valid branch name"}},
			},
			{
				name: "a target of the wrong type is named once", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"target_branch":5,"target_is_ephemeral":true}`,
				wantError: "invalid target_branch; base_ref, base_branch, or preferred_base is required",
				want:      []ipParam{{"target_branch", "must be a valid branch name"}},
			},

			// A body with one problem answers as before.
			{
				name: "one problem: target_branch", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"target_branch":"refs/heads/x","base_ref":"main"}`,
				wantError: "invalid target_branch",
				want:      []ipParam{{"target_branch", "must be a valid branch name"}},
			},
			{
				name: "one problem: ephemeral HEAD", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"target_branch":"HEAD","target_is_ephemeral":true,"base_ref":"main"}`,
				wantError: "invalid target_branch",
				want:      []ipParam{{"target_branch", "must not be HEAD for an ephemeral branch"}},
			},
			{
				name: "one problem: no base", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"target_branch":"ok"}`,
				wantError: "base_ref, base_branch, or preferred_base is required",
			},
			{
				name: "one problem: force without a target", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"base_ref":"main","force":true}`,
				wantError: "force requires target_branch",
			},
			{
				name: "one problem: guard", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"target_branch":"ok","base_ref":"main","expected_target_sha":"` + sha[:39] + `"}`,
				wantError: "invalid expected_target_sha",
				want:      []ipParam{{"expected_target_sha", "must be a full commit SHA"}},
			},
			{
				name: "one problem: guard of 40 characters that are not hex", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"target_branch":"ok","base_ref":"main","expected_target_sha":"` + strings.Repeat("g", 40) + `"}`,
				wantError: "invalid expected_target_sha",
				want:      []ipParam{{"expected_target_sha", "must be a full commit SHA"}},
			},
			{
				name: "one problem: guard without a target", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"base_ref":"main","expected_target_sha":"` + sha + `"}`,
				wantError: "expected_target_sha requires target_branch",
				want:      []ipParam{{"expected_target_sha", "requires target_branch unless it is 40 zeros"}},
			},
			{
				name: "the path style answers the same", method: http.MethodPost, path: "/api/repos/" + fx.url + "/branches/create", token: fx.gitTok,
				body:      `{"target_branch":"bad..name","expected_target_sha":"x"}`,
				wantError: "invalid target_branch; base_ref, base_branch, or preferred_base is required; invalid expected_target_sha",
				want: []ipParam{
					{"target_branch", "must be a valid branch name"},
					{"expected_target_sha", "must be a full commit SHA"},
				},
			},
		})
	})
}

// TestTagCreateNamesEveryInvalidField: POST tags names both an invalid
// name and a missing ref, in the order of the docs table.
func TestTagCreateNamesEveryInvalidField(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := ipNewFixture(t, e)
		const path = "/api/v1/repos/tags"
		ipRun(t, e, []ipCase{
			{
				name: "every field", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"name":"refs/tags/x"}`,
				wantError: "invalid tag name; ref is required",
				want: []ipParam{
					{"name", "must be a valid tag name"},
					{"ref", "is required"},
				},
			},
			{
				name: "the deprecated alias target counts as ref", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"name":"bad..name","target":"main"}`,
				wantError: "invalid tag name",
				want:      []ipParam{{"name", "must be a valid tag name"}},
			},
			{
				name: "an empty target does not count", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"name":"","target":""}`,
				wantError: "invalid tag name; ref is required",
				want: []ipParam{
					{"name", "must be a valid tag name"},
					{"ref", "is required"},
				},
			},
			{
				name: "one problem: name", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"name":"bad..name","ref":"main"}`,
				wantError: "invalid tag name",
				want:      []ipParam{{"name", "must be a valid tag name"}},
			},
			{
				name: "one problem: ref", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"name":"v1"}`,
				wantError: "ref is required",
				want:      []ipParam{{"ref", "is required"}},
			},
			{
				name: "the path style answers the same", method: http.MethodPost, path: "/api/repos/" + fx.url + "/tags", token: fx.gitTok,
				body:      `{"name":"refs/tags/x"}`,
				wantError: "invalid tag name; ref is required",
				want: []ipParam{
					{"name", "must be a valid tag name"},
					{"ref", "is required"},
				},
			},
		})
	})
}

// TestBranchAndTagDecoderErrorHidesTheOtherProblems: a field of the wrong
// JSON type is a decoder error, which names every such field in the body,
// in body order, and none of the other problems. The checks of the fields
// run only when every field has its right type. The docs promise this.
func TestBranchAndTagDecoderErrorHidesTheOtherProblems(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := ipNewFixture(t, e)
		str, boolean := "must be a string", "must be a boolean"
		for _, tc := range []struct {
			name, path, body string
			want             []invalidParam
		}{
			{"branch: every wrong type in the body", "/api/v1/repos/branches/create",
				`{"target_branch":"bad..name","expected_target_sha":7,"force":"yes"}`, []invalidParam{{"expected_target_sha", str}, {"force", boolean}}},
			{"branch: the same fields in another order", "/api/v1/repos/branches/create",
				`{"force":"yes","target_branch":"bad..name","expected_target_sha":7}`, []invalidParam{{"force", boolean}, {"expected_target_sha", str}}},
			{"branch: a wrong type hides a missing base", "/api/repos/" + fx.url + "/branches/create",
				`{"target_branch":"ok","base_is_ephemeral":"no"}`, []invalidParam{{"base_is_ephemeral", boolean}}},
			{"tag: every wrong type in the body", "/api/v1/repos/tags",
				`{"name":5,"ref":6}`, []invalidParam{{"name", str}, {"ref", str}}},
			{"tag: the same fields in another order", "/api/v1/repos/tags",
				`{"ref":6,"name":5}`, []invalidParam{{"ref", str}, {"name", str}}},
			{"tag: a wrong type hides an invalid name", "/api/repos/" + fx.url + "/tags",
				`{"name":"bad..name","target":false}`, []invalidParam{{"target", str}}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				resp, body := e.doReq(t, http.MethodPost, tc.path, fx.gitTok, []byte(tc.body), "")
				if resp.StatusCode != http.StatusBadRequest {
					t.Fatalf("status = %d, want 400 (body %s)", resp.StatusCode, body)
				}
				msg := requireEnvelope(t, resp, body)
				if !strings.HasPrefix(msg, "invalid JSON body: ") || strings.Count(msg, "cannot unmarshal") != len(tc.want) {
					t.Fatalf("error = %q, want the decoder's message about each of %v", msg, tc.want)
				}
				var p problemBody
				if err := json.Unmarshal(body, &p); err != nil {
					t.Fatal(err)
				}
				var got []invalidParam
				for _, ip := range p.InvalidParams {
					got = append(got, invalidParam{ip.Name, ip.Reason})
				}
				if !slices.Equal(got, tc.want) {
					t.Fatalf("invalid_params = %+v, want %+v", got, tc.want)
				}
			})
		}
	})
}

// TestBranchAndTagDeleteKeepTheirAnswers: the deletes have one body field
// at most, so they have nothing to collect. DELETE branches names no
// field today, and its query parameters are not part of this batch.
func TestBranchAndTagDeleteKeepTheirAnswers(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := ipNewFixture(t, e)
		ipRun(t, e, []ipCase{
			{
				name: "delete branch: invalid name", method: http.MethodDelete, path: "/api/v1/repos/branches?target_branch=bad..name", token: fx.gitTok,
				wantError: "invalid branch name",
			},
			{
				name: "delete branch: invalid name in the deprecated body", method: http.MethodDelete, path: "/api/v1/repos/branches", token: fx.gitTok,
				body:      `{"name":"refs/heads/x"}`,
				wantError: "invalid branch name",
			},
			{
				name: "delete tag: invalid name", method: http.MethodDelete, path: "/api/v1/repos/tags", token: fx.gitTok,
				body:      `{"name":"bad..name"}`,
				wantError: "invalid tag name",
				want:      []ipParam{{"name", "must be a valid tag name"}},
			},
		})
	})
}
