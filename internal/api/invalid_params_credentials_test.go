package api

import (
	"context"
	"net/http"
	"net/url"
	"testing"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/meta"
)

// TestCredentialsNameEveryInvalidField: the git-credentials routes name
// each invalid body field, in the order of the docs table (id, password,
// username), and never put a username or a password into a message. A
// password with the reserved prefix is a check of the store, so alone it
// answers after the lookups of its route (404 first); it is named together
// with another invalid field.
func TestCredentialsNameEveryInvalidField(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		ctx := context.Background()
		org := e.newOrg(t)
		repo, err := e.store.CreateRepo(ctx, org, "https://example.com/"+org+"/ipc-"+randomHex(t, 4), "main")
		if err != nil {
			t.Fatalf("create repo: %v", err)
		}
		cred, err := e.store.CreateGitCredential(ctx, meta.GitCredential{RepoID: repo.RepoID, Username: "u0", Password: "p0"})
		if err != nil {
			t.Fatalf("create credential: %v", err)
		}
		orgTok := e.mint(t, tokenSpec{iss: org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeRepoWrite}})
		repoTok := e.mint(t, tokenSpec{iss: org, repo: repo.URL, scopes: []string{auth.ScopeRepoWrite}})
		const (
			v1   = "/api/v1/repos/git-credentials"
			text = " (want UTF-8 text without NUL)"
			// The values of the bodies below are credentials: none of them
			// may appear in a message, so each wantError spells none.
			reserved = `invalid password: secret values must not start with the reserved prefix "enc:"`
		)
		pwText := ipParam{"password", "must be UTF-8 text without NUL"}
		userText := ipParam{"username", "must be UTF-8 text without NUL"}
		pwReserved := ipParam{"password", "is a reserved value"}
		byName := "/api/repos/" + url.PathEscape(repo.URL) + "/git-credentials"
		byPath := byName + "/" + cred.ID

		ipRun(t, e, []ipCase{
			// POST: password, username.
			{
				name: "POST every field", method: http.MethodPost, path: v1, token: repoTok,
				body:      `{"username":"ipuser\u0000","password":"ippass\u0000"}`,
				wantError: "invalid password" + text + "; invalid username" + text,
				want:      []ipParam{pwText, userText},
			},
			{
				name: "POST on the path of the repository", method: http.MethodPost, path: byName, token: repoTok,
				body:      `{"username":"ipuser\u0000"}`,
				wantError: "password is required; invalid username" + text,
				want:      []ipParam{{"password", "is required"}, userText},
			},
			{
				name: "POST a reserved password is named with a bad username", method: http.MethodPost, path: v1, token: repoTok,
				body:      `{"username":"ipuser\u0000","password":"enc:ippass"}`,
				wantError: reserved + "; invalid username" + text,
				want:      []ipParam{pwReserved, userText},
			},
			{
				name: "POST a password has one problem: the NUL goes before the prefix", method: http.MethodPost, path: v1, token: repoTok,
				body:      `{"username":"ipuser\u0000","password":"enc:\u0000"}`,
				wantError: "invalid password" + text + "; invalid username" + text,
				want:      []ipParam{pwText, userText},
			},
			{
				name: "POST the body is checked before the repository", method: http.MethodPost, path: v1, token: repoTok,
				body:      `{"repo_id":"repo_missing","username":"ipuser\u0000","password":"enc:ippass"}`,
				wantError: reserved + "; invalid username" + text,
				want:      []ipParam{pwReserved, userText},
			},
			{
				name: "POST one problem: password", method: http.MethodPost, path: v1, token: repoTok,
				body: `{}`, wantError: "password is required", want: []ipParam{{"password", "is required"}},
			},
			{
				name: "POST one problem: username", method: http.MethodPost, path: v1, token: repoTok,
				body: `{"username":"ipuser\u0000","password":"ippass"}`, wantError: "invalid username" + text, want: []ipParam{userText},
			},
			{
				name: "POST one problem: a reserved password at the store", method: http.MethodPost, path: v1, token: repoTok,
				body: `{"password":"enc:ippass"}`, wantError: reserved, want: []ipParam{pwReserved},
			},

			// PUT: id, password, username.
			{
				name: "PUT every field", method: http.MethodPut, path: v1, token: orgTok,
				body:      `{"username":"ipuser\u0000","password":"ippass\u0000"}`,
				wantError: "id is required; invalid password" + text + "; invalid username" + text,
				want:      []ipParam{{"id", "is required"}, pwText, userText},
			},
			{
				name: "PUT a missing password is named with the id and the username", method: http.MethodPut, path: v1, token: orgTok,
				body:      `{"username":"ipuser\u0000"}`,
				wantError: "id is required; password is required; invalid username" + text,
				want:      []ipParam{{"id", "is required"}, {"password", "is required"}, userText},
			},
			{
				name: "PUT a reserved password is named with a missing id", method: http.MethodPut, path: v1, token: orgTok,
				body:      `{"password":"enc:ippass"}`,
				wantError: "id is required; " + reserved,
				want:      []ipParam{{"id", "is required"}, pwReserved},
			},
			{
				name: "PUT a reserved password is named with a bad username", method: http.MethodPut, path: v1, token: orgTok,
				body:      `{"id":"` + cred.ID + `","username":"ipuser\u0000","password":"enc:ippass"}`,
				wantError: reserved + "; invalid username" + text,
				want:      []ipParam{pwReserved, userText},
			},
			{
				name: "PUT the body is checked before the credential lookup", method: http.MethodPut, path: v1, token: orgTok,
				body:      `{"id":"ggc_missing","username":"ipuser\u0000","password":"ippass\u0000"}`,
				wantError: "invalid password" + text + "; invalid username" + text,
				want:      []ipParam{pwText, userText},
			},
			{
				name: "PUT one problem: id", method: http.MethodPut, path: v1, token: orgTok,
				body: `{"password":"p"}`, wantError: "id is required", want: []ipParam{{"id", "is required"}},
			},
			{
				name: "PUT one problem: password", method: http.MethodPut, path: v1, token: orgTok,
				body: `{"id":"` + cred.ID + `"}`, wantError: "password is required", want: []ipParam{{"password", "is required"}},
			},
			{
				name: "PUT one problem: a reserved password at the store", method: http.MethodPut, path: v1, token: orgTok,
				body: `{"id":"` + cred.ID + `","password":"enc:ippass"}`, wantError: reserved, want: []ipParam{pwReserved},
			},

			// PUT by the path of the credential: password, username.
			{
				name: "PUT by path every field", method: http.MethodPut, path: byPath, token: repoTok,
				body:      `{"username":"ipuser\u0000","password":"ippass\u0000"}`,
				wantError: "invalid password" + text + "; invalid username" + text,
				want:      []ipParam{pwText, userText},
			},
			{
				name: "PUT by path a missing password is named with a bad username", method: http.MethodPut, path: byPath, token: repoTok,
				body:      `{"username":"ipuser\u0000"}`,
				wantError: "password is required; invalid username" + text,
				want:      []ipParam{{"password", "is required"}, userText},
			},
			{
				name: "PUT by path a reserved password is named with a bad username", method: http.MethodPut, path: byPath, token: repoTok,
				body:      `{"username":"ipuser\u0000","password":"enc:ippass"}`,
				wantError: reserved + "; invalid username" + text,
				want:      []ipParam{pwReserved, userText},
			},
			{
				name: "PUT by path one problem: a reserved password at the store", method: http.MethodPut, path: byPath, token: repoTok,
				body: `{"password":"enc:ippass"}`, wantError: reserved, want: []ipParam{pwReserved},
			},

			// The org-level PUT of the path form checks the body before the
			// credential that the body names.
			{
				name: "PUT on the path of the repository checks the body before the credential lookup", method: http.MethodPut, path: byName, token: orgTok,
				body:      `{"id":"ggc_missing","username":"ipuser\u0000","password":"ippass\u0000"}`,
				wantError: "invalid password" + text + "; invalid username" + text,
				want:      []ipParam{pwText, userText},
			},

			// DELETE names the one field it has.
			{
				name: "DELETE one problem: id", method: http.MethodDelete, path: v1, token: orgTok,
				body: `{}`, wantError: "id is required", want: []ipParam{{"id", "is required"}},
			},
		})

		// A route that names the repository or credential in its path looks
		// it up before it reads the body: a missing one is 404 even when the
		// body is invalid.
		const badBody = `{"username":"ipuser\u0000","password":"enc:ippass"}`
		gone := "/api/repos/" + url.PathEscape("https://example.com/"+org+"/ipc-missing") + "/git-credentials"
		for _, tc := range []struct {
			name, method, path, token, code string
		}{
			{"POST on the path of a missing repository", http.MethodPost, gone, repoTok, codeRepositoryNotFound},
			{"PUT on the path of a missing repository", http.MethodPut, gone, orgTok, codeRepositoryNotFound},
			{"PUT by path on a missing repository", http.MethodPut, gone + "/" + cred.ID, repoTok, codeRepositoryNotFound},
			{"PUT by path on a missing credential", http.MethodPut, byName + "/ggc_missing", repoTok, codeCredentialNotFound},
		} {
			t.Run(tc.name+" is 404 before the body", func(t *testing.T) {
				resp, body := e.doReq(t, tc.method, tc.path, tc.token, []byte(badBody), "")
				if resp.StatusCode != http.StatusNotFound {
					t.Fatalf("status = %d, want 404 (body %s)", resp.StatusCode, body)
				}
				requireProblemCode(t, resp, body, tc.code)
			})
		}

		// A reserved password that is the only problem of the body answers
		// at the store, after the lookups: 404 for a missing repository or
		// credential.
		for _, tc := range []struct {
			name, method, path, token, body, code string
		}{
			{"POST a reserved password alone on a missing repository", http.MethodPost, v1, repoTok,
				`{"repo_id":"repo_missing","password":"enc:ippass"}`, codeRepositoryNotFound},
			{"PUT a reserved password alone on a missing credential", http.MethodPut, v1, orgTok,
				`{"id":"ggc_missing","password":"enc:ippass"}`, codeCredentialNotFound},
		} {
			t.Run(tc.name, func(t *testing.T) {
				resp, body := e.doReq(t, tc.method, tc.path, tc.token, []byte(tc.body), "")
				if resp.StatusCode != http.StatusNotFound {
					t.Fatalf("status = %d, want 404 (body %s)", resp.StatusCode, body)
				}
				requireProblemCode(t, resp, body, tc.code)
			})
		}
	})
}
