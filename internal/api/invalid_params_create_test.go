package api

import (
	"net/http"
	"testing"

	"github.com/coder/openstorage/internal/auth"
)

// Items and messages that several rows of this file share.
var (
	ipcBranchItem = ipParam{"default_branch", "is not a valid branch name"}
	ipcNulReason  = "must be UTF-8 text without NUL"
)

// TestCreateNamesEveryInvalidField: POST /repos names each invalid body
// field (upstream: `invalid_params` "names each invalid field"), in the
// order of the docs row `{repo_name?, default_branch?, upstream?,
// base_repo?}`, and a body with one problem answers as before.
func TestCreateNamesEveryInvalidField(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		// Every row is a 400, so no row creates the repository, and one
		// token serves them all.
		tok := e.mint(t, tokenSpec{iss: org, repo: "ipc-" + randomHex(t, 4), scopes: []string{auth.ScopeRepoWrite}})
		post := func(name, body, wantError string, want ...ipParam) ipCase {
			return ipCase{name: name, method: http.MethodPost, path: "/api/v1/repos", token: tok, body: body, wantError: wantError, want: want}
		}
		ipRun(t, e, []ipCase{
			// The upstream form: provider, owner, name, access, host.
			post("upstream every field",
				`{"default_branch":"bad..name","upstream":{"provider":"svn","owner":"o\u0000","name":"","access":"x","host":"bad host"}}`,
				`invalid default_branch; unsupported upstream provider "svn"; invalid upstream.owner (want UTF-8 text without NUL)`+
					`; upstream provider, owner, and name are required; invalid upstream access "x"; invalid upstream host "bad host"`,
				ipcBranchItem, ipParam{"upstream.provider", "is not supported"}, ipParam{"upstream.owner", ipcNulReason},
				ipParam{"upstream.name", "is required"}, ipParam{"upstream.access", "must be app or public"},
				ipParam{"upstream.host", "is not a valid host"}),
			post("upstream one required error names each empty field", `{"upstream":{}}`,
				"upstream provider, owner, and name are required",
				ipParam{"upstream.provider", "is required"}, ipParam{"upstream.owner", "is required"}, ipParam{"upstream.name", "is required"}),
			post("upstream github defaults to app access and fails its host", `{"upstream":{"provider":"github","owner":"","name":"n","host":"bad host"}}`,
				"upstream provider, owner, and name are required; "+upstreamAppAccessMsg+`; invalid upstream host "bad host"`,
				ipParam{"upstream.owner", "is required"}, ipParam{"upstream.access", "must be public"}, ipParam{"upstream.host", "is not a valid host"}),
			post("upstream access and host depend on the provider", `{"upstream":{"provider":"gitea","owner":"o","name":"n","access":"app"}}`,
				`upstream access applies to provider github only; upstream host is required for provider "gitea"`,
				ipParam{"upstream.access", "applies to provider github only"}, ipParam{"upstream.host", "is required for this provider"}),
			// S2: a provider that is not supported leaves what depends on
			// it unchecked (the access rule, the default host).
			post("upstream unsupported provider names itself only", `{"upstream":{"provider":"svn","owner":"o","name":"n","access":"public"}}`,
				`unsupported upstream provider "svn"`, ipParam{"upstream.provider", "is not supported"}),
			post("upstream missing provider leaves the host unchecked", `{"upstream":{"owner":"o","name":"n","access":"x"}}`,
				`upstream provider, owner, and name are required; invalid upstream access "x"`,
				ipParam{"upstream.provider", "is required"}, ipParam{"upstream.access", "must be app or public"}),
			// One required message names all the empty fields of its group.
			// The items keep the order of the fields: a field between two
			// empty ones has its own problem named between them.
			post("upstream required group keeps the order of the fields", `{"upstream":{"owner":"o\u0000"}}`,
				"upstream provider, owner, and name are required; invalid upstream.owner (want UTF-8 text without NUL)",
				ipParam{"upstream.provider", "is required"}, ipParam{"upstream.owner", ipcNulReason}, ipParam{"upstream.name", "is required"}),
			post("upstream required group with a problem before and after", `{"upstream":{"provider":"svn","name":"n\u0000"}}`,
				`unsupported upstream provider "svn"; upstream provider, owner, and name are required; invalid upstream.name (want UTF-8 text without NUL)`,
				ipParam{"upstream.provider", "is not supported"}, ipParam{"upstream.owner", "is required"}, ipParam{"upstream.name", ipcNulReason}),
			post("upstream one problem answers as before", `{"upstream":{"provider":"github","access":"public","owner":"o"}}`,
				"upstream provider, owner, and name are required", ipParam{"upstream.name", "is required"}),

			// The deprecated sync form: provider, owner, name, operation,
			// auth.token, default_branch, upstream_host.
			post("base_repo every field with the default branch of the body",
				`{"default_branch":"bad..name","base_repo":{"provider":"svn","owner":"o\u0000","name":"","auth":{"token":"t\u0000"},"upstream_host":"bad host"}}`,
				`invalid default_branch; unsupported base_repo provider "svn"; invalid base_repo.owner (want UTF-8 text without NUL)`+
					`; base_repo owner and name are required; invalid base_repo.auth.token (want UTF-8 text without NUL)`+
					`; invalid base_repo upstream_host "bad host"`,
				ipcBranchItem, ipParam{"base_repo.provider", "is not supported"}, ipParam{"base_repo.owner", ipcNulReason},
				ipParam{"base_repo.name", "is required"}, ipParam{"base_repo.auth.token", ipcNulReason},
				ipParam{"base_repo.upstream_host", "is not a valid host"}),
			post("base_repo every field with its own default branch",
				`{"base_repo":{"provider":"gitea","owner":"","name":"n\u0000","auth":{"token":"t\u0000"},"default_branch":"bad..name"}}`,
				`base_repo owner and name are required; invalid base_repo.name (want UTF-8 text without NUL)`+
					`; invalid base_repo.auth.token (want UTF-8 text without NUL); invalid default_branch`+
					`; base_repo upstream_host is required for provider "gitea"`,
				ipParam{"base_repo.owner", "is required"}, ipParam{"base_repo.name", ipcNulReason}, ipParam{"base_repo.auth.token", ipcNulReason},
				ipParam{"base_repo.default_branch", "is not a valid branch name"}, ipParam{"base_repo.upstream_host", "is required for this provider"}),
			// S2: base_repo.default_branch only counts when the body has no
			// default_branch of its own.
			post("base_repo own default branch is unused next to the body's",
				`{"default_branch":"main","base_repo":{"provider":"gitea","owner":"","name":"n","default_branch":"bad..name","upstream_host":"h.example"}}`,
				"base_repo owner and name are required", ipParam{"base_repo.owner", "is required"}),
			// S2: the operation decides which fields apply, so a bad one
			// is the only base_repo problem.
			post("base_repo bad operation names itself only",
				`{"default_branch":"bad..name","base_repo":{"provider":"svn","owner":"","name":"","operation":"bogus","auth":{"token":"t\u0000"},"upstream_host":"bad host"}}`,
				`invalid default_branch; invalid base_repo operation "bogus"`,
				ipcBranchItem, ipParam{"base_repo.operation", "must be sync or fork"}),
			post("base_repo fork operation names itself only", `{"base_repo":{"provider":"github","operation":"fork","owner":"","upstream_host":"bad host"}}`,
				`base_repo operation "fork" not supported`, ipParam{"base_repo.operation", "is not supported"}),
			post("base_repo code provider needs the fork operation",
				`{"default_branch":"bad..name","base_repo":{"provider":"code","operation":"sync","owner":"","upstream_host":"bad host"}}`,
				`invalid default_branch; base_repo provider "code" requires operation "fork"`,
				ipcBranchItem, ipParam{"base_repo.operation", "must be fork for this provider"}),
			// S2: a provider that is not supported leaves the default host
			// unchecked.
			post("base_repo unsupported provider names itself only", `{"base_repo":{"provider":"svn","owner":"o","name":"n"}}`,
				`unsupported base_repo provider "svn"`, ipParam{"base_repo.provider", "is not supported"}),
			post("base_repo one problem answers as before", `{"base_repo":{"provider":"github","owner":"o"}}`,
				"base_repo owner and name are required", ipParam{"base_repo.name", "is required"}),

			// A member that fails to decode answers alone, as any decode
			// failure does: no other check of the body runs.
			post("upstream wrong type hides the other problems", `{"default_branch":"bad..name","upstream":{"provider":"svn","owner":1}}`,
				"invalid upstream: json: cannot unmarshal number into Go struct field upstreamConfig.owner of type string",
				ipParam{"upstream.owner", "must be a string"}),
			post("upstream unknown field hides the other problems", `{"default_branch":"bad..name","upstream":{"provider":"svn","x":1}}`,
				`invalid upstream: json: unknown field "x"`),
			post("base_repo wrong type hides the other problems", `{"default_branch":"bad..name","base_repo":{"provider":"svn","auth":3}}`,
				"invalid base_repo: json: cannot unmarshal number into Go struct field usyncBaseRepo.auth of type api.usyncBaseRepoAuth",
				ipParam{"base_repo.auth", "must be an object"}),
			post("body wrong type hides the other problems", `{"default_branch":5,"upstream":{"provider":"svn"}}`,
				"invalid JSON body: json: cannot unmarshal number into Go struct field createRepoRequest.default_branch of type string",
				ipParam{"default_branch", "must be a string"}),

			// Rules across fields add their message and no item.
			post("both forms and a bad default branch", `{"default_branch":"bad..name","base_repo":{"provider":"github"},"upstream":{"provider":"github"}}`,
				"invalid default_branch; base_repo and upstream cannot both be set", ipcBranchItem),
			post("both forms leave each form unchecked", `{"base_repo":{"provider":"svn","operation":"bogus"},"upstream":{"provider":"svn"}}`,
				"base_repo and upstream cannot both be set"),
			// Neither form is decoded then, so a member that would fail to
			// decode does not answer.
			post("both forms leave upstream undecoded", `{"base_repo":{"provider":"github"},"upstream":{"provider":"github","owner":1}}`,
				"base_repo and upstream cannot both be set"),
			post("both forms leave upstream's unknown field undecoded", `{"base_repo":{"provider":"github"},"upstream":{"x":1}}`,
				"base_repo and upstream cannot both be set"),
			post("both forms leave base_repo undecoded", `{"base_repo":{"provider":"github","auth":3},"upstream":{"provider":"github"}}`,
				"base_repo and upstream cannot both be set"),
			// A mismatching repo_name has its own code and answers first.
			post("repo_name mismatch answers alone", `{"repo_name":"other","default_branch":"bad..name","upstream":{}}`,
				repoNameMismatchMsg),

			// S5: the egress check may look up a name, so it runs after
			// every check of the body.
			post("a bad default branch answers before the egress check",
				`{"default_branch":"bad..name","upstream":{"provider":"gitea","owner":"o","name":"n","host":"169.254.169.254"}}`,
				"invalid default_branch", ipcBranchItem),
			post("a bad field answers before the egress check", `{"upstream":{"provider":"gitea","name":"n","host":"169.254.169.254"}}`,
				"upstream provider, owner, and name are required", ipParam{"upstream.owner", "is required"}),
			post("a bad default branch of base_repo answers before the egress check",
				`{"base_repo":{"provider":"gitea","owner":"o","name":"n","upstream_host":"169.254.169.254","default_branch":"bad..name"}}`,
				"invalid default_branch", ipParam{"base_repo.default_branch", "is not a valid branch name"}),
			post("egress denial alone answers as before", `{"upstream":{"provider":"gitea","owner":"o","name":"n","host":"169.254.169.254"}}`,
				"upstream host denied by sync egress policy: egress target 169.254.169.254 denied: cloud metadata address",
				ipParam{"upstream.host", "is denied by the sync egress policy"}),
			post("default branch alone answers as before", `{"default_branch":"bad..name"}`, "invalid default_branch", ipcBranchItem),
			// The query parameter is no body field: it names none.
			{
				name: "query default branch alone names no field", method: http.MethodPost, path: "/api/v1/repos?default_branch=bad..name",
				token: tok, body: `{}`, wantError: "invalid default_branch",
			},
			// The query parameter is no body field: the egress check still
			// answers before it, as before.
			{
				name: "query default branch answers after the egress check", method: http.MethodPost, path: "/api/v1/repos?default_branch=bad..name",
				token: tok, body: `{"upstream":{"provider":"gitea","owner":"o","name":"n","host":"169.254.169.254"}}`,
				wantError: "upstream host denied by sync egress policy: egress target 169.254.169.254 denied: cloud metadata address",
				want:      []ipParam{{"upstream.host", "is denied by the sync egress policy"}},
			},
			{
				name: "query default branch answers after the base_repo egress check", method: http.MethodPost, path: "/api/v1/repos?default_branch=bad..name",
				token: tok, body: `{"base_repo":{"provider":"gitea","owner":"o","name":"n","upstream_host":"169.254.169.254"}}`,
				wantError: "base_repo upstream_host denied by sync egress policy: egress target 169.254.169.254 denied: cloud metadata address",
				want:      []ipParam{{"base_repo.upstream_host", "is denied by the sync egress policy"}},
			},
			{
				name: "query default branch is not part of the body's problems", method: http.MethodPost, path: "/api/v1/repos?default_branch=bad..name",
				token: tok, body: `{"upstream":{}}`, wantError: "upstream provider, owner, and name are required",
				want: []ipParam{{"upstream.provider", "is required"}, {"upstream.owner", "is required"}, {"upstream.name", "is required"}},
			},
		})

		// S4: the default branch of a body that has no other problem
		// answers after the ref policy, as before. Any other body
		// problem answers before it.
		t.Run("ref policy answers before a default branch alone", func(t *testing.T) {
			policed := e.mint(t, tokenSpec{iss: org, repo: "ipc-pol-" + randomHex(t, 4),
				scopes: []string{auth.ScopeRepoWrite}, refs: `[["main", ["no-push"]]]`})
			const link = `"upstream":{"provider":"gitea","owner":"o","name":"n","host":"upstream.invalid"}`
			resp, body := e.doReq(t, http.MethodPost, "/api/v1/repos", policed, []byte(`{"default_branch":"bad..name",`+link+`}`), "")
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("status = %d, want 403 (body %s)", resp.StatusCode, body)
			}
			if msg := requireEnvelope(t, resp, body); msg != `ref policy op "no-push" denies sync repository create` {
				t.Fatalf("error = %q", msg)
			}
			// The same for the default branch of base_repo.
			resp, body = e.doReq(t, http.MethodPost, "/api/v1/repos", policed,
				[]byte(`{"base_repo":{"provider":"gitea","owner":"o","name":"n","upstream_host":"upstream.invalid","default_branch":"bad..name"}}`), "")
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("base_repo status = %d, want 403 (body %s)", resp.StatusCode, body)
			}
			ipRun(t, e, []ipCase{{
				name: "a field next to the default branch answers before it", method: http.MethodPost, path: "/api/v1/repos", token: policed,
				body:      `{"default_branch":"bad..name","upstream":{"provider":"gitea","owner":"","name":"n","host":"upstream.invalid"}}`,
				wantError: "invalid default_branch; upstream provider, owner, and name are required",
				want:      []ipParam{ipcBranchItem, {"upstream.owner", "is required"}},
			}})
		})
	})
}

// TestForkNamesEveryInvalidField: the copy and fork forms of POST /repos
// name each invalid body field, in the order of the docs.
func TestForkNamesEveryInvalidField(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		tok := e.mint(t, tokenSpec{iss: org, repo: "ipc-fork-" + randomHex(t, 4), scopes: []string{auth.ScopeRepoWrite}})
		post := func(name, body, wantError string, want ...ipParam) ipCase {
			return ipCase{name: name, method: http.MethodPost, path: "/api/v1/repos", token: tok, body: body, wantError: wantError, want: want}
		}
		ipRun(t, e, []ipCase{
			// The copy form: default_branch, then base_repo.name, then
			// base_repo.auth.token.
			post("copy every field", `{"default_branch":"bad..name","base_repo":{"owner":"o","auth":{"auth_type":"x"}}}`,
				"invalid default_branch; base_repo.name is required for a copy; "+copySyncFieldsMsg+"; base_repo.auth.token is required for a copy",
				ipcBranchItem, ipParam{"base_repo.name", "is required for a copy"}, ipParam{"base_repo.auth.token", "is required for a copy"}),
			post("copy with its own default branch", `{"base_repo":{"default_branch":"bad..name"}}`,
				"base_repo.name is required for a copy; base_repo.auth.token is required for a copy; invalid default_branch",
				ipParam{"base_repo.name", "is required for a copy"}, ipParam{"base_repo.auth.token", "is required for a copy"},
				ipParam{"base_repo.default_branch", "is not a valid branch name"}),
			// A rule across fields adds its message and no item.
			post("copy sync fields beside a missing name", `{"base_repo":{"owner":"o","auth":{"token":"t"}}}`,
				"base_repo.name is required for a copy; "+copySyncFieldsMsg, ipParam{"base_repo.name", "is required for a copy"}),
			post("copy sync fields alone answer as before", `{"base_repo":{"name":"x","owner":"o","auth":{"token":"t"}}}`, copySyncFieldsMsg),
			post("copy name alone answers as before", `{"base_repo":{"auth":{"token":"t"}}}`,
				"base_repo.name is required for a copy", ipParam{"base_repo.name", "is required for a copy"}),
			// The explicit fork form.
			post("fork every field", `{"default_branch":"bad..name","base_repo":{"provider":"code","operation":"fork"}}`,
				`invalid default_branch; base_repo.auth.token is required for provider "code"`,
				ipcBranchItem, ipParam{"base_repo.auth.token", "is required"}),
			post("fork with its own default branch", `{"base_repo":{"provider":"code.storage","operation":"fork","default_branch":"bad..name"}}`,
				`base_repo.auth.token is required for provider "code.storage"; invalid default_branch`,
				ipParam{"base_repo.auth.token", "is required"}, ipParam{"base_repo.default_branch", "is not a valid branch name"}),
			post("fork token alone answers as before", `{"base_repo":{"provider":"code","operation":"fork"}}`,
				`base_repo.auth.token is required for provider "code"`, ipParam{"base_repo.auth.token", "is required"}),
		})

		// S5: a default branch with no other problem answers after the
		// source token is verified, as before.
		t.Run("source token answers before a default branch alone", func(t *testing.T) {
			resp, body := e.doReq(t, http.MethodPost, "/api/v1/repos", tok,
				[]byte(`{"default_branch":"bad..name","base_repo":{"name":"src","auth":{"token":"not-a-token"}}}`), "")
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("status = %d, want 403 (body %s)", resp.StatusCode, body)
			}
			if msg := requireEnvelope(t, resp, body); msg != "base_repo.auth.token: invalid token" {
				t.Fatalf("error = %q", msg)
			}
			// The same for the default branch of base_repo.
			resp, body = e.doReq(t, http.MethodPost, "/api/v1/repos", tok,
				[]byte(`{"base_repo":{"provider":"code","operation":"fork","default_branch":"bad..name","auth":{"token":"not-a-token"}}}`), "")
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("base_repo status = %d, want 403 (body %s)", resp.StatusCode, body)
			}
		})
	})
}

// TestForkDefaultBranchAnswersAfterTheSource: with a source that exists, a
// default branch that is the only invalid field still answers after the
// source's fork point, as before (the fork point is state).
func TestForkDefaultBranchAnswersAfterTheSource(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		src := wrefSeedFixture(t, e)
		srcTok := e.mint(t, tokenSpec{iss: src.org, repo: src.url, scopes: []string{auth.ScopeGitRead}})
		tok := e.mint(t, tokenSpec{iss: e.newOrg(t), repo: "ipc-src-" + randomHex(t, 4), scopes: []string{auth.ScopeRepoWrite}})
		copyOf := func(extra string) string {
			return `{"name":"` + src.url + `","auth":{"token":"` + srcTok + `"}` + extra + `}`
		}
		post := func(name, path, body, wantError string, want ...ipParam) ipCase {
			return ipCase{name: name, method: http.MethodPost, path: path, token: tok, body: body, wantError: wantError, want: want}
		}
		ipRun(t, e, []ipCase{
			post("body default branch", "/api/v1/repos", `{"default_branch":"bad..name","base_repo":`+copyOf("")+`}`,
				"invalid default_branch", ipcBranchItem),
			post("base_repo default branch", "/api/v1/repos", `{"base_repo":`+copyOf(`,"default_branch":"bad..name"`)+`}`,
				"invalid default_branch", ipParam{"base_repo.default_branch", "is not a valid branch name"}),
			post("query default branch", "/api/v1/repos?default_branch=bad..name", `{"base_repo":`+copyOf("")+`}`, "invalid default_branch"),
			post("fork point answers first", "/api/v1/repos", `{"default_branch":"bad..name","base_repo":`+copyOf(`,"ref":"no-such-ref"`)+`}`,
				`base_repo fork point "no-such-ref" not found in source repository`),
			post("a field next to the default branch answers before the source", "/api/v1/repos",
				`{"default_branch":"bad..name","base_repo":`+copyOf(`,"owner":"o"`)+`}`,
				"invalid default_branch; "+copySyncFieldsMsg, ipcBranchItem),
		})
	})
}

// TestRepoUpdateAndPullNameTheirOneField: PATCH /repo and the pull-upstream
// routes check one field of their body each, so each names that field, and
// no other field of the body.
func TestRepoUpdateAndPullNameTheirOneField(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := ipNewFixture(t, e)
		const nulRef = `invalid ref (want UTF-8 text without NUL)`
		refItem := []ipParam{{"ref", ipcNulReason}}
		ipRun(t, e, []ipCase{
			{
				name: "patch required", method: http.MethodPatch, path: "/api/v1/repo", token: fx.repoTok, body: `{}`,
				wantError: "default_branch is required", want: []ipParam{{"default_branch", "is required"}},
			},
			{
				name: "patch invalid", method: http.MethodPatch, path: "/api/v1/repo", token: fx.repoTok, body: `{"default_branch":"bad..name"}`,
				wantError: "invalid default_branch", want: []ipParam{ipcBranchItem},
			},
			{
				name: "patch invalid with a field that it ignores", method: http.MethodPatch, path: "/api/repos/" + fx.url, token: fx.repoTok,
				body: `{"default_branch":"refs/tags/v1","other":1}`, wantError: "invalid default_branch", want: []ipParam{ipcBranchItem},
			},
			{
				name: "pull ref", method: http.MethodPost, path: "/api/v1/repos/pull-upstream", token: fx.gitTok,
				body: `{"ref":"a\u0000b","after":"c\u0000d"}`, wantError: nulRef, want: refItem,
			},
			{
				name: "alias pull ref", method: http.MethodPost, path: "/api/repos/" + fx.url + "/pull-upstream", token: fx.gitTok,
				body: `{"ref":"a\u0000b"}`, wantError: nulRef, want: refItem,
			},
			{
				name: "upstream pull ref", method: http.MethodPost, path: "/api/repos/" + fx.url + "/upstream/pull", token: fx.gitTok,
				body: `{"ref":"a\u0000b","after":"c\u0000d"}`, wantError: nulRef, want: refItem,
			},
		})
	})
}
