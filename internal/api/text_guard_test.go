package api

// PostgreSQL refuses a NUL byte or invalid UTF-8 in a text parameter
// (SQLSTATE 22021), and answered 500 for most request text that held one
// (a value stored encrypted under a secrets key was the exception).
// SQLite and S3-WAL stored or ignored it. These tests pin one answer on
// every backend: a key with such text (a server-made ID, a name) names no
// row and answers the 404 of an unknown key, and a value the request asks
// to store answers 400 (meta.ValidText). A JSON body decodes to valid
// UTF-8, so a body can carry only the NUL; a path or a query can carry
// both.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/meta"
)

// textProblem decodes an error body into the members the tests compare.
func textProblem(t *testing.T, body []byte) problemBody {
	t.Helper()
	var p problemBody
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatalf("decode error body %q: %v", body, err)
	}
	return p
}

// TestRequestTextKeysAreUnknown covers a webhook ID and a git credential
// ID from the path or the body.
func TestRequestTextKeysAreUnknown(t *testing.T) { runBackends(t, testRequestTextKeysAreUnknown) }

func testRequestTextKeysAreUnknown(t *testing.T, e *testEnv) {
	ctx := context.Background()
	org := e.newOrg(t)
	repo, err := e.store.CreateRepo(ctx, org, "https://example.com/"+org+"/tk-"+randomHex(t, 4), "main")
	if err != nil {
		t.Fatalf("create repo: %v", err)
	}
	cred, err := e.store.CreateGitCredential(ctx, meta.GitCredential{RepoID: repo.RepoID, Username: "u0", Password: "p0"})
	if err != nil {
		t.Fatalf("create credential: %v", err)
	}
	toks := map[string]string{
		"org":  e.mint(t, tokenSpec{iss: org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeRepoWrite}}),
		"repo": e.mint(t, tokenSpec{iss: org, repo: repo.URL, scopes: []string{auth.ScopeRepoWrite}}),
	}
	alias := "/api/repos/" + url.PathEscape(repo.URL) + "/git-credentials/"

	type route struct {
		name, method, tok string
		path              func(key string) string // the escaped request path
		body              func(key string) string // "" sends no body; JSON text is NUL only
	}
	pathKey := func(prefix string) func(string) string {
		return func(key string) string { return prefix + url.PathEscape(key) }
	}
	fixed := func(path string) func(string) string { return func(string) string { return path } }
	jsonID := func(rest string) func(string) string {
		return func(key string) string {
			id, err := json.Marshal(key)
			if err != nil {
				t.Fatal(err)
			}
			return `{"id":` + string(id) + rest + `}`
		}
	}
	routes := []route{
		{"DELETE /webhooks/{id}", http.MethodDelete, "org", pathKey("/api/v1/webhooks/"), nil},
		{"PUT /repos/git-credentials", http.MethodPut, "org", fixed("/api/v1/repos/git-credentials"), jsonID(`,"password":"x"`)},
		{"DELETE /repos/git-credentials", http.MethodDelete, "org", fixed("/api/v1/repos/git-credentials"), jsonID("")},
		{"PUT /repos/{repo_name}/git-credentials/{id}", http.MethodPut, "repo", pathKey(alias),
			func(string) string { return `{"password":"x"}` }},
		{"DELETE /repos/{repo_name}/git-credentials/{id}", http.MethodDelete, "repo", pathKey(alias), nil},
	}
	for _, rt := range routes {
		send := func(key string) (*http.Response, problemBody) {
			var body []byte
			if rt.body != nil {
				body = []byte(rt.body(key))
			}
			resp, raw := e.doReq(t, rt.method, rt.path(key), toks[rt.tok], body, "")
			return resp, textProblem(t, raw)
		}
		// What an ordinary unknown key answers.
		wantResp, want := send("no-such-id")
		if wantResp.StatusCode != http.StatusNotFound {
			t.Fatalf("%s: an unknown key answers %d, want 404", rt.name, wantResp.StatusCode)
		}
		for _, key := range []string{"a\x00b", "\x00", cred.ID + "\x00", "a\xffb", "\xff"} {
			if rt.body != nil && !utf8ValidJSONKey(key) {
				continue // a body cannot hold invalid UTF-8
			}
			t.Run(rt.name+" "+strings.ToValidUTF8(key, "\ufffd"), func(t *testing.T) {
				resp, got := send(key)
				if resp.StatusCode != http.StatusNotFound || got.Code != want.Code || got.Detail != want.Detail || got.Error != want.Error {
					t.Errorf("key %q answers %d %+v, want 404 like an unknown key: %+v", key, resp.StatusCode, got, want)
				}
			})
		}
	}

	// The credential survived the attempts.
	if got, err := e.store.GetGitCredential(ctx, cred.ID); err != nil || got.Username != "u0" || got.Password != "p0" {
		t.Errorf("credential after the requests = %+v, %v", got, err)
	}
}

// utf8ValidJSONKey reports whether a JSON body can carry key unchanged.
func utf8ValidJSONKey(key string) bool {
	b, err := json.Marshal(key)
	if err != nil {
		return false
	}
	var back string
	return json.Unmarshal(b, &back) == nil && back == key
}

// TestRequestTextValuesAreRefused covers text the request asks to store: a
// webhook secret, a git credential, the owner, name and token of a sync
// link, and the ref of a pull; and a list cursor, which carries a repo ID.
func TestRequestTextValuesAreRefused(t *testing.T) { runBackends(t, testRequestTextValuesAreRefused) }

func testRequestTextValuesAreRefused(t *testing.T, e *testEnv) {
	ctx := context.Background()
	org := e.newOrg(t)
	newRepo := func(name string) meta.Repo {
		repo, err := e.store.CreateRepo(ctx, org, "https://example.com/"+org+"/"+name+"-"+randomHex(t, 4), "main")
		if err != nil {
			t.Fatalf("create repo %s: %v", name, err)
		}
		return repo
	}
	plain, withCred, linked := newRepo("plain"), newRepo("cred"), newRepo("linked")
	cred, err := e.store.CreateGitCredential(ctx, meta.GitCredential{RepoID: withCred.RepoID, Username: "u0", Password: "p0"})
	if err != nil {
		t.Fatalf("create credential: %v", err)
	}
	if _, err := e.store.SetRepoUpstream(ctx, meta.RepoUpstream{RepoID: linked.RepoID, Provider: "github", Owner: "o", Name: "n", UpstreamHost: "github.com"}); err != nil {
		t.Fatalf("set upstream: %v", err)
	}
	orgTok := e.mint(t, tokenSpec{iss: org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeOrgRead, auth.ScopeRepoWrite}})
	repoTok := func(repo meta.Repo, scopes ...string) string {
		return e.mint(t, tokenSpec{iss: org, repo: repo.URL, scopes: scopes})
	}
	credPath := "/api/repos/" + url.PathEscape(withCred.URL) + "/git-credentials/" + cred.ID
	pullPath := "/api/repos/" + url.PathEscape(linked.URL) + "/upstream/pull"
	nul := func(rest string) string { return "1700000000000000000|repo_" + rest }
	cursor := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }

	type row struct {
		name, method, path, tok, body string
		param                         string // the invalid_params name, "" for none
		msg                           string // the error text
	}
	const want = " (want UTF-8 text without NUL)"
	rows := []row{
		{"webhook secret", http.MethodPost, "/api/v1/webhooks", orgTok,
			`{"url":"https://hooks.example.invalid/x","secret":"a\u0000b"}`, "", "invalid secret" + want},

		{"credential username", http.MethodPost, "/api/v1/repos/git-credentials", repoTok(plain, auth.ScopeRepoWrite),
			`{"username":"a\u0000b","password":"p"}`, "username", "invalid username" + want},
		{"credential password", http.MethodPost, "/api/v1/repos/git-credentials", repoTok(plain, auth.ScopeRepoWrite),
			`{"username":"u","password":"a\u0000b"}`, "password", "invalid password" + want},
		{"credential password after a valid username", http.MethodPost, "/api/v1/repos/git-credentials", repoTok(plain, auth.ScopeRepoWrite),
			`{"username":"u","password":"\u0000"}`, "password", "invalid password" + want},
		{"credential username on the alias", http.MethodPost, "/api/repos/" + url.PathEscape(plain.URL) + "/git-credentials", repoTok(plain, auth.ScopeRepoWrite),
			`{"username":"a\u0000b","password":"p"}`, "username", "invalid username" + want},
		{"PUT credential username", http.MethodPut, "/api/v1/repos/git-credentials", orgTok,
			`{"id":"` + cred.ID + `","username":"a\u0000b","password":"p"}`, "username", "invalid username" + want},
		{"PUT credential password", http.MethodPut, "/api/v1/repos/git-credentials", orgTok,
			`{"id":"` + cred.ID + `","password":"a\u0000b"}`, "password", "invalid password" + want},
		{"PUT credential username by path", http.MethodPut, credPath, repoTok(withCred, auth.ScopeRepoWrite),
			`{"username":"a\u0000b","password":"p"}`, "username", "invalid username" + want},
		{"PUT credential password by path", http.MethodPut, credPath, repoTok(withCred, auth.ScopeRepoWrite),
			`{"password":"a\u0000b"}`, "password", "invalid password" + want},

		{"pull ref", http.MethodPost, "/api/v1/repos/pull-upstream", repoTok(linked, auth.ScopeGitWrite),
			`{"ref":"a\u0000b"}`, "ref", "invalid ref" + want},
		{"pull ref by path", http.MethodPost, pullPath, repoTok(linked, auth.ScopeGitWrite),
			`{"ref":"a\u0000b"}`, "ref", "invalid ref" + want},

		{"list cursor with a NUL repo ID", http.MethodGet, "/api/v1/repos?cursor=" + cursor(nul("a\x00b")), orgTok, "", "", "invalid cursor"},
		{"list cursor with a repo ID that is not UTF-8", http.MethodGet, "/api/v1/repos?cursor=" + cursor(nul("a\xffb")), orgTok, "", "", "invalid cursor"},
	}

	// A sync create takes its repo name from the token, so each row gets a
	// new name; the name stays free when the request is refused.
	var createdNames []string
	for _, c := range []struct{ name, body, param string }{
		{"base_repo owner", `{"base_repo":{"provider":"github","owner":"a\u0000b","name":"n"}}`, "base_repo.owner"},
		{"base_repo name", `{"base_repo":{"provider":"github","owner":"o","name":"a\u0000b"}}`, "base_repo.name"},
		{"base_repo token", `{"base_repo":{"provider":"github","owner":"o","name":"n","auth":{"token":"a\u0000b"}}}`, "base_repo.auth.token"},
		{"upstream owner", `{"upstream":{"provider":"github","owner":"a\u0000b","name":"n","access":"public"}}`, "upstream.owner"},
		{"upstream name", `{"upstream":{"provider":"github","owner":"o","name":"a\u0000b","access":"public"}}`, "upstream.name"},
	} {
		name := "https://example.com/" + org + "/new-" + randomHex(t, 4)
		createdNames = append(createdNames, name)
		rows = append(rows, row{"create with " + c.name, http.MethodPost, "/api/v1/repos",
			e.mint(t, tokenSpec{iss: org, repo: name, scopes: []string{auth.ScopeRepoWrite}}),
			c.body, c.param, "invalid " + c.param + want})
	}

	for _, rw := range rows {
		t.Run(rw.name, func(t *testing.T) {
			var body []byte
			if rw.body != "" {
				body = []byte(rw.body)
			}
			resp, raw := e.doReq(t, rw.method, rw.path, rw.tok, body, "")
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d (body %.300s), want 400", resp.StatusCode, raw)
			}
			requireEnvelope(t, resp, raw)
			got := textProblem(t, raw)
			if got.Code != "bad_request" || got.Error != rw.msg {
				t.Errorf("code %q, error %q, want bad_request, %q", got.Code, got.Error, rw.msg)
			}
			switch {
			case rw.param == "" && len(got.InvalidParams) != 0:
				t.Errorf("invalid_params = %+v, want none", got.InvalidParams)
			case rw.param != "" && (len(got.InvalidParams) != 1 || got.InvalidParams[0].Name != rw.param ||
				got.InvalidParams[0].Reason != "must be UTF-8 text without NUL"):
				t.Errorf("invalid_params = %+v, want one item naming %q", got.InvalidParams, rw.param)
			}
		})
	}

	// Each request was refused before anything was stored.
	if hooks, err := e.store.ListWebhooks(ctx, org); err != nil || len(hooks) != 0 {
		t.Errorf("webhooks = %+v, %v, want none", hooks, err)
	}
	if got, err := e.store.GitCredentialForRepo(ctx, plain.RepoID); !errors.Is(err, meta.ErrNotFound) {
		t.Errorf("credential of the plain repo = %+v, %v, want ErrNotFound", got, err)
	}
	if got, err := e.store.GitCredentialForRepo(ctx, withCred.RepoID); err != nil || got.Username != "u0" || got.Password != "p0" {
		t.Errorf("credential after the updates = %+v, %v, want u0 and p0", got, err)
	}
	if up, err := e.store.GetRepoUpstream(ctx, linked.RepoID); err != nil || up.SyncPending {
		t.Errorf("upstream after the pulls = %+v, %v, want no pending sync", up, err)
	}
	for _, name := range createdNames {
		if got, err := e.store.GetRepoByURL(ctx, org, name); !errors.Is(err, meta.ErrNotFound) {
			t.Errorf("repo %s after the refused create = %+v, %v, want ErrNotFound", name, got, err)
		}
	}
}

// TestRequestTextValidValuesAreStored pins that multi-byte UTF-8 and other
// control bytes are still stored as before.
func TestRequestTextValidValuesAreStored(t *testing.T) {
	runBackends(t, testRequestTextValidValuesAreStored)
}

func testRequestTextValidValuesAreStored(t *testing.T, e *testEnv) {
	ctx := context.Background()
	org := e.newOrg(t)
	orgTok := e.mint(t, tokenSpec{iss: org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeOrgRead, auth.ScopeRepoWrite}})

	resp, raw := e.doReq(t, http.MethodPost, "/api/v1/webhooks", orgTok,
		[]byte(`{"url":"https://hooks.example.invalid/x","secret":"s\u00e9cr\u00e9t \u65e5\u672c\u001f"}`), "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("webhook with a UTF-8 secret = %d (%.300s), want 200", resp.StatusCode, raw)
	}
	var wh webhookResponse
	if err := json.Unmarshal(raw, &wh); err != nil {
		t.Fatalf("decode webhook %q: %v", raw, err)
	}
	if got, err := e.store.GetWebhook(ctx, wh.ID); err != nil || got.Secret != "s\u00e9cr\u00e9t \u65e5\u672c\u001f" {
		t.Errorf("stored webhook = %+v, %v", got, err)
	}

	repoURL := "https://example.com/" + org + "/valid-" + randomHex(t, 4)
	repo, err := e.store.CreateRepo(ctx, org, repoURL, "main")
	if err != nil {
		t.Fatalf("create repo: %v", err)
	}
	repoTok := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeRepoWrite}})
	resp, raw = e.doReq(t, http.MethodPost, "/api/v1/repos/git-credentials", repoTok,
		[]byte(`{"username":"\u00fcser","password":"p\u00e4ssw\u00f6rd"}`), "")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("credential with UTF-8 text = %d (%.300s), want 201", resp.StatusCode, raw)
	}
	if got, err := e.store.GitCredentialForRepo(ctx, repo.RepoID); err != nil || got.Username != "\u00fcser" || got.Password != "p\u00e4ssw\u00f6rd" {
		t.Errorf("stored credential = %+v, %v", got, err)
	}

	name := "https://example.com/" + org + "/valid-sync-" + randomHex(t, 4)
	syncTok := e.mint(t, tokenSpec{iss: org, repo: name, scopes: []string{auth.ScopeRepoWrite}})
	resp, raw = e.doReq(t, http.MethodPost, "/api/v1/repos", syncTok,
		[]byte(`{"base_repo":{"provider":"github","owner":"\u00dcn\u00ef","name":"n-\u65e5\u672c"}}`), "")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("sync create with a UTF-8 owner = %d (%.300s), want 201", resp.StatusCode, raw)
	}
}
