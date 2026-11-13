package api

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"
)

// goDecoderText is what encoding/json says about a body that does not decode.
// It names Go structs and types, which a client must never see (issue #841).
var goDecoderText = []string{"Go struct", "Go value", "json:", "cannot unmarshal", "of type", "unexpected end of JSON", "invalid character"}

// ddRoute is one route that decodes a JSON body, with a body that has a
// field of the wrong type and the answer it must get.
type ddRoute struct {
	name, method, path, token string
	body                      string
	// want is the whole message of the answer, as upstream's example
	// `default_branch must be a string`, and params its invalid_params.
	want   string
	params []ipParam
	// generic is the message for a body that is not JSON. A commit route
	// reads NDJSON frames and answers in its result envelope.
	generic  string
	envelope bool
	// lead is the NDJSON frames that precede the frame that fails.
	lead string
}

// ddAnswer returns the message of a 400 and its invalid_params, and fails
// the test when the answer is not a 400 or holds text of encoding/json.
func ddAnswer(t *testing.T, e *testEnv, r ddRoute, body string) (string, []ipParam) {
	t.Helper()
	resp, raw := e.doReq(t, r.method, r.path, r.token, []byte(body), "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("%s %s: status %d, want 400 (body %.300s)", r.method, r.path, resp.StatusCode, raw)
	}
	for _, bad := range goDecoderText {
		if strings.Contains(string(raw), bad) {
			t.Fatalf("%s %s: the answer holds %q: %.400s", r.method, r.path, bad, raw)
		}
	}
	if r.envelope {
		var env struct {
			Result struct {
				Message string `json:"message"`
			} `json:"result"`
		}
		if err := json.Unmarshal(raw, &env); err != nil || env.Result.Message == "" {
			t.Fatalf("%s %s: not a result envelope with a message: %.300s", r.method, r.path, raw)
		}
		return env.Result.Message, nil
	}
	msg := requireEnvelope(t, resp, raw)
	var p problemBody
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatal(err)
	}
	var params []ipParam
	for _, ip := range p.InvalidParams {
		params = append(params, ipParam{ip.Name, ip.Reason})
	}
	if p.Detail != msg || p.Code != "bad_request" {
		t.Fatalf("%s %s: detail %q, error %q, code %q", r.method, r.path, p.Detail, msg, p.Code)
	}
	return msg, params
}

// ddRoutes lists every route that decodes a JSON request body.
func ddRoutes(fx ipdFixture) []ddRoute {
	const generic = "invalid JSON body"
	head := `{"metadata":{"target_branch":"main","commit_message":"m","author":{"name":"a","email":"e@x"}`
	route := func(name, method, path, token, body, want string, params ...ipParam) ddRoute {
		return ddRoute{name: name, method: method, path: path, token: token, body: body, want: want, params: params, generic: generic}
	}
	frame := func(name, path, lead, bad, want string) ddRoute {
		return ddRoute{name: name, method: http.MethodPost, path: path, token: fx.gitTok, lead: lead, body: lead + bad + "\n", want: want,
			generic: "malformed NDJSON frame", envelope: true}
	}
	// A strictly decoded body names its own problem, which holds no text of
	// encoding/json.
	strict := func(r ddRoute) ddRoute {
		r.generic = "invalid JSON body: body must be a JSON object"
		return r
	}
	str := func(names ...string) (out []ipParam) {
		for _, n := range names {
			out = append(out, ipParam{n, "must be a string"})
		}
		return out
	}
	return []ddRoute{
		route("create", http.MethodPost, "/api/v1/repos", fx.repoTok, `{"default_branch":7}`, "default_branch must be a string", str("default_branch")...),
		route("create, canonical path", http.MethodPost, "/api/repos", fx.repoTok, `{"default_branch":7}`, "default_branch must be a string", str("default_branch")...),
		route("create several fields", http.MethodPost, "/api/v1/repos", fx.repoTok, `{"default_branch":7,"repo_name":true}`,
			"default_branch must be a string; repo_name must be a string", str("default_branch", "repo_name")...),
		route("create base_repo", http.MethodPost, "/api/v1/repos", fx.repoTok, `{"base_repo":{"owner":1,"auth":{"token":2}}}`,
			"base_repo.owner must be a string; base_repo.auth.token must be a string", str("base_repo.owner", "base_repo.auth.token")...),
		route("create base_repo that is not an object", http.MethodPost, "/api/v1/repos", fx.repoTok, `{"base_repo":"x"}`,
			"base_repo must be an object", ipParam{"base_repo", "must be an object"}),
		route("create upstream", http.MethodPost, "/api/v1/repos", fx.repoTok, `{"upstream":{"provider":"github","owner":1}}`,
			"upstream.owner must be a string", str("upstream.owner")...),
		route("create upstream that is not an object", http.MethodPost, "/api/v1/repos", fx.repoTok, `{"upstream":5}`,
			"upstream must be an object", ipParam{"upstream", "must be an object"}),
		route("update default branch", http.MethodPatch, "/api/v1/repo", fx.repoTok, `{"default_branch":7}`, "default_branch must be a string", str("default_branch")...),
		route("update default branch, path form", http.MethodPatch, fx.alias, fx.repoTok, `{"default_branch":7}`, "default_branch must be a string", str("default_branch")...),
		route("credential create", http.MethodPost, "/api/v1/repos/git-credentials", fx.repoTok, `{"repo_id":1}`, "repo_id must be a string", str("repo_id")...),
		route("credential update", http.MethodPut, "/api/v1/repos/git-credentials", fx.orgTok, `{"id":1}`, "id must be a string", str("id")...),
		route("credential update, path form", http.MethodPut, fx.credPath, fx.repoTok, `{"username":1}`, "username must be a string", str("username")...),
		route("grep", http.MethodPost, "/api/v1/repos/grep", fx.gitTok, `{"paths":["a",1],"limits":{"max_lines":"x"}}`,
			"paths.1 must be a string; limits.max_lines must be an integer", ipParam{"paths.1", "must be a string"}, ipParam{"limits.max_lines", "must be an integer"}),
		route("archive", http.MethodPost, "/api/v1/repos/archive", fx.gitTok, `{"max_blob_size":"1"}`, "max_blob_size must be an integer", ipParam{"max_blob_size", "must be an integer"}),
		route("pull-upstream", http.MethodPost, "/api/v1/repos/pull-upstream", fx.gitTok, `{"ref":1,"after":2}`, "ref must be a string; after must be a string", str("ref", "after")...),
		route("pull-upstream, path form", http.MethodPost, fx.alias+"/pull-upstream", fx.gitTok, `{"ref":1}`, "ref must be a string", str("ref")...),
		strict(route("upstream/pull, strict", http.MethodPost, fx.alias+"/upstream/pull", fx.gitTok, `{"after":1,"ref":2}`, "after must be a string", str("after")...)),
		route("push-upstream", http.MethodPost, "/api/v1/repos/push-upstream", fx.gitTok, `{"force":"yes"}`, "force must be a boolean", ipParam{"force", "must be a boolean"}),
		route("webhook", http.MethodPost, "/api/v1/webhooks", fx.orgTok, `{"url":{},"secret":5}`, "url must be a string; secret must be a string", str("url", "secret")...),
		route("merge", http.MethodPost, "/api/v1/repos/merge", fx.gitTok, `{"strategy":5,"squash":"yes","author":{"name":1}}`,
			"strategy must be a string; squash must be a boolean; author.name must be a string",
			ipParam{"strategy", "must be a string"}, ipParam{"squash", "must be a boolean"}, ipParam{"author.name", "must be a string"}),
		route("branch create", http.MethodPost, "/api/v1/repos/branches/create", fx.gitTok, `{"force":"y","base_ref":5}`,
			"force must be a boolean; base_ref must be a string", ipParam{"force", "must be a boolean"}, ipParam{"base_ref", "must be a string"}),
		route("branch delete", http.MethodDelete, "/api/v1/repos/branches", fx.gitTok, `{"name":1}`, "name must be a string", str("name")...),
		route("tag create", http.MethodPost, "/api/v1/repos/tags", fx.gitTok, `{"name":1,"ref":2}`, "name must be a string; ref must be a string", str("name", "ref")...),
		route("tag delete", http.MethodDelete, "/api/v1/repos/tags", fx.gitTok, `{"name":1}`, "name must be a string", str("name")...),
		route("credential delete", http.MethodDelete, "/api/v1/repos/git-credentials", fx.orgTok, `{"id":1}`, "id must be a string", str("id")...),
		route("note add", http.MethodPost, "/api/v1/repos/notes", fx.gitTok, `{"note":1,"author":{"email":2}}`,
			"note must be a string; author.email must be a string", str("note", "author.email")...),
		route("note delete", http.MethodDelete, "/api/v1/repos/notes", fx.gitTok, `{"note":1}`, "note must be a string", str("note")...),
		strict(route("note append, strict", http.MethodPost, fx.alias+"/notes/append", fx.gitTok, `{"note":1,"object_ref":2}`, "note must be a string", str("note")...)),
		frame("commit-pack metadata", "/api/v1/repos/commit-pack", "", `{"metadata":{"target_branch":5,"files":[{"path":1}]}}`,
			"malformed NDJSON frame: metadata.target_branch must be a string"),
		frame("commit-pack chunk", "/api/v1/repos/commit-pack", head+`,"files":[{"path":"a","content_id":"c","operation":"upsert"}]}}`+"\n", `{"blob_chunk":{"eof":"x"}}`,
			"malformed NDJSON frame: blob_chunk.eof must be a boolean"),
		frame("diff-commit metadata", "/api/v1/repos/diff-commit", "", `{"metadata":{"ephemeral":"x"}}`,
			"malformed NDJSON frame: metadata.ephemeral must be a boolean"),
		frame("diff-commit chunk", "/api/v1/repos/diff-commit", head+"}}\n", `{"diff_chunk":{"data":5}}`,
			"malformed NDJSON frame: diff_chunk.data must be a string"),
	}
}

// TestDecodeAnswersHoldNoGoTypeNames covers issue #841: a 400 for a body that
// does not decode names the fields of the wrong type as invalid_params does,
// and holds no text of encoding/json, on every route that decodes a JSON body.
func TestDecodeAnswersHoldNoGoTypeNames(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := ipdNewFixture(t, e)
		for _, r := range ddRoutes(fx) {
			t.Run(r.name, func(t *testing.T) {
				msg, params := ddAnswer(t, e, r, r.body)
				if msg != r.want || !slices.Equal(params, r.params) {
					t.Fatalf("answer %q %+v, want %q %+v", msg, params, r.want, r.params)
				}
				// A body that is not JSON, or has the wrong top-level type,
				// names no field: its message is the generic one.
				for _, bad := range []string{"{", `{"x":`, "[1]", "nope"} {
					body := bad
					if r.envelope {
						body = r.lead + bad + "\n"
					}
					msg, params := ddAnswer(t, e, r, body)
					if msg != r.generic || len(params) != 0 {
						t.Fatalf("body %q: answer %q %+v, want %q and no invalid_params", bad, msg, params, r.generic)
					}
				}
			})
		}
	})
}
