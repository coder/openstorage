package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/coder/openstorage/internal/auth"
)

// TestCredentialUsernameOptional pins upstream's git-credentials contract
// (spec 2026-10-08, issue #396): only `password` is required on create and
// update, on the v1 and the path-style routes. The stored username is
// exactly what the request sent, and an update that omits it clears the
// stored one.
func TestCredentialUsernameOptional(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		repoURL := "team/creduser-" + randomHex(t, 4)
		repo := createRepo(t, e, org, repoURL, nil, http.StatusCreated)
		repoTok := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeRepoWrite}})
		orgTok := e.mint(t, tokenSpec{iss: org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeRepoWrite}})
		credsPath := "/api/repos/" + url.PathEscape(repoURL) + "/git-credentials"

		requireStored := func(t *testing.T, wantUser, wantPass string) string {
			t.Helper()
			cred, err := e.store.GitCredentialForRepo(context.Background(), repo.RepoID)
			if err != nil {
				t.Fatalf("load stored credential: %v", err)
			}
			if cred.Username != wantUser || cred.Password != wantPass {
				t.Fatalf("stored username/password = %q/%q, want %q/%q", cred.Username, cred.Password, wantUser, wantPass)
			}
			return cred.ID
		}
		do := func(t *testing.T, method, path, tok, body string, want int) {
			t.Helper()
			var b []byte
			if body != "" {
				b = []byte(body)
			}
			resp, respBody := e.doReq(t, method, path, tok, b, "")
			if resp.StatusCode != want {
				t.Fatalf("%s %s %s: status %d, want %d: %s", method, path, body, resp.StatusCode, want, respBody)
			}
			if want == http.StatusCreated {
				var out map[string]string
				if err := json.Unmarshal(respBody, &out); err != nil || out["id"] == "" {
					t.Fatalf("create body = %s (err %v), want {id}", respBody, err)
				}
			}
		}
		deleteCred := func(t *testing.T, id string) {
			t.Helper()
			do(t, http.MethodDelete, credsPath+"/"+id, repoTok, "", http.StatusNoContent)
		}

		// v1 create without and with a username.
		do(t, http.MethodPost, "/api/v1/repos/git-credentials", repoTok, `{"repo_id":"`+repo.RepoID+`","password":"p1"}`, http.StatusCreated)
		deleteCred(t, requireStored(t, "", "p1"))
		do(t, http.MethodPost, "/api/v1/repos/git-credentials", repoTok, `{"repo_id":"`+repo.RepoID+`","username":"oauth2","password":"p2"}`, http.StatusCreated)
		deleteCred(t, requireStored(t, "oauth2", "p2"))

		// Path-style create (no repo_id) with and without a username.
		do(t, http.MethodPost, credsPath, repoTok, `{"username":"x-token-auth","password":"p3"}`, http.StatusCreated)
		deleteCred(t, requireStored(t, "x-token-auth", "p3"))
		do(t, http.MethodPost, credsPath, repoTok, `{"password":"p4"}`, http.StatusCreated)
		id := requireStored(t, "", "p4")

		// v1 update: a username is stored, and omitting it clears it.
		do(t, http.MethodPut, "/api/v1/repos/git-credentials", orgTok, `{"id":"`+id+`","username":"oauth2","password":"p5"}`, http.StatusOK)
		requireStored(t, "oauth2", "p5")
		do(t, http.MethodPut, "/api/v1/repos/git-credentials", orgTok, `{"id":"`+id+`","password":"p6"}`, http.StatusOK)
		requireStored(t, "", "p6")

		// Path-style update: same rules.
		do(t, http.MethodPut, credsPath+"/"+id, repoTok, `{"username":"oauth2","password":"p7"}`, http.StatusOK)
		requireStored(t, "oauth2", "p7")
		do(t, http.MethodPut, credsPath+"/"+id, repoTok, `{"password":"p8"}`, http.StatusOK)
		requireStored(t, "", "p8")

		// password stays required everywhere.
		do(t, http.MethodPut, "/api/v1/repos/git-credentials", orgTok, `{"id":"`+id+`","username":"oauth2"}`, http.StatusBadRequest)
		do(t, http.MethodPut, credsPath+"/"+id, repoTok, `{"username":"oauth2"}`, http.StatusBadRequest)
		requireStored(t, "", "p8")
	})
}
