package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/meta"
	upsync "github.com/coder/openstorage/internal/sync"
)

// TestSyncValuesGitCannotTakeAtRegistration: POST /repos refuses a sync
// link whose upstream URL is over 131,070 bytes or whose token is over
// 131,042 bytes, and the git credential routes refuse a username or a
// password over 131,042 bytes: the limits of the sync run, which would
// fail such a link on every run. A value at the limit is stored. The
// numbers are literals, so a limit that moves fails here.
func TestSyncValuesGitCannotTakeAtRegistration(t *testing.T) {
	const (
		urlMax  = 131070
		credMax = 131042
		host    = "forge.example.invalid"
		urlMsg  = "invalid upstream URL (want at most 131070 bytes)"
		urlWhy  = "must keep the upstream URL at most 131070 bytes"
		credWhy = "must be at most 131042 bytes"
	)
	if upsync.MaxUpstreamURLBytes != urlMax || upsync.MaxCredentialBytes != credMax {
		t.Fatalf("limits %d and %d, want %d and %d", upsync.MaxUpstreamURLBytes, upsync.MaxCredentialBytes, urlMax, credMax)
	}
	// owner returns an owner that makes the URL of name at base n bytes.
	owner := func(base, name string, n int) string {
		return strings.Repeat("o", n-len(base)-len("/"+name+".git"))
	}
	hostBase := "https://" + host + "/"
	githubBase := "https://github.com/"
	body := func(v any) string {
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	upstream := func(o string, extra map[string]any) string {
		cfg := map[string]any{"provider": "gitea", "owner": o, "name": "n", "host": host}
		for k, v := range extra {
			cfg[k] = v
		}
		return body(map[string]any{"upstream": cfg})
	}
	baseRepo := func(o, token string) string {
		return body(map[string]any{"base_repo": map[string]any{
			"provider": "gitea", "owner": o, "name": "n", "upstream_host": host, "auth": map[string]any{"token": token}}})
	}
	if got := len(upsync.Upstream{Owner: owner(hostBase, "n", urlMax), Name: "n", UpstreamHost: host}.CloneURL()); got != urlMax {
		t.Fatalf("the URL at the limit is %d bytes, want %d", got, urlMax)
	}

	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		tok := e.mint(t, tokenSpec{iss: org, repo: "lim-" + randomHex(t, 4), scopes: []string{auth.ScopeRepoWrite}})
		post := func(name, body, wantError string, want ...ipParam) ipCase {
			return ipCase{name: name, method: http.MethodPost, path: "/api/v1/repos", token: tok, body: body, wantError: wantError, want: want}
		}
		over := owner(hostBase, "n", urlMax+1)
		ipRun(t, e, []ipCase{
			post("upstream URL one byte over", upstream(over, nil), urlMsg,
				ipParam{"upstream.owner", urlWhy}, ipParam{"upstream.name", urlWhy}, ipParam{"upstream.host", urlWhy}),
			// The host of the provider is no field of the body.
			post("upstream URL over the limit with the default host",
				body(map[string]any{"upstream": map[string]any{"provider": "github", "owner": owner(githubBase, "n", urlMax+1), "name": "n", "access": "public"}}),
				urlMsg, ipParam{"upstream.owner", urlWhy}, ipParam{"upstream.name", urlWhy}),
			// access lies between name and host in the docs: its item
			// stays there.
			post("upstream URL over the limit and a bad access", upstream(over, map[string]any{"access": "x"}),
				urlMsg+`; invalid upstream access "x"`,
				ipParam{"upstream.owner", urlWhy}, ipParam{"upstream.name", urlWhy},
				ipParam{"upstream.access", "must be app or public"}, ipParam{"upstream.host", urlWhy}),
			// A host with a problem of its own makes no URL.
			post("upstream bad host is not a URL problem", upstream(over, map[string]any{"host": "bad host"}),
				`invalid upstream host "bad host"`, ipParam{"upstream.host", "is not a valid host"}),
			post("base_repo URL one byte over", baseRepo(over, "t"), urlMsg,
				ipParam{"base_repo.owner", urlWhy}, ipParam{"base_repo.name", urlWhy}, ipParam{"base_repo.upstream_host", urlWhy}),
			post("base_repo token one byte over", baseRepo("o", strings.Repeat("t", credMax+1)),
				"invalid base_repo.auth.token (want at most 131042 bytes)", ipParam{"base_repo.auth.token", credWhy}),
			post("base_repo URL and token over", baseRepo(over, strings.Repeat("t", credMax+1)),
				urlMsg+"; invalid base_repo.auth.token (want at most 131042 bytes)",
				ipParam{"base_repo.owner", urlWhy}, ipParam{"base_repo.name", urlWhy},
				ipParam{"base_repo.auth.token", credWhy}, ipParam{"base_repo.upstream_host", urlWhy}),
		})

		// At the limits the link and its credential are stored.
		atURL := createRepo(t, e, org, "lim-url-"+randomHex(t, 4), []byte(upstream(owner(hostBase, "n", urlMax), nil)), http.StatusCreated)
		atBase := createRepo(t, e, org, "lim-base-"+randomHex(t, 4),
			[]byte(baseRepo(owner(hostBase, "n", urlMax), strings.Repeat("t", credMax))), http.StatusCreated)
		for _, created := range []createRepoResponse{atURL, atBase} {
			link, err := e.store.(meta.EventingStore).GetRepoUpstream(context.Background(), created.RepoID)
			if err != nil {
				t.Fatalf("upstream of %s: %v", created.RepoID, err)
			}
			if got := len(upsync.Upstream{Owner: link.Owner, Name: link.Name, UpstreamHost: link.UpstreamHost}.CloneURL()); got != urlMax {
				t.Fatalf("stored URL is %d bytes, want %d", got, urlMax)
			}
		}
		cred, err := e.store.GitCredentialForRepo(context.Background(), atBase.RepoID)
		if err != nil || len(cred.Password) != credMax {
			t.Fatalf("stored token: %d bytes, err %v; want %d bytes", len(cred.Password), err, credMax)
		}

		// The git credential routes.
		repo, err := e.store.CreateRepo(context.Background(), org, "https://example.com/"+org+"/lim-cred-"+randomHex(t, 4), "main")
		if err != nil {
			t.Fatalf("create repo: %v", err)
		}
		stored, err := e.store.CreateGitCredential(context.Background(), meta.GitCredential{RepoID: repo.RepoID, Username: "u0", Password: "p0"})
		if err != nil {
			t.Fatalf("create credential: %v", err)
		}
		orgTok := e.mint(t, tokenSpec{iss: org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeRepoWrite}})
		repoTok := e.mint(t, tokenSpec{iss: org, repo: repo.URL, scopes: []string{auth.ScopeRepoWrite}})
		const v1 = "/api/v1/repos/git-credentials"
		byPath := "/api/repos/" + url.PathEscape(repo.URL) + "/git-credentials/" + stored.ID
		long := strings.Repeat("c", credMax+1)
		both := `{"username":"` + long + `","password":"` + long + `"}`
		const bothMsg = "invalid password (want at most 131042 bytes); invalid username (want at most 131042 bytes)"
		bothItems := []ipParam{{"password", credWhy}, {"username", credWhy}}
		ipRun(t, e, []ipCase{
			{name: "POST username and password over", method: http.MethodPost, path: v1, token: repoTok, body: both, wantError: bothMsg, want: bothItems},
			{name: "PUT username and password over", method: http.MethodPut, path: v1, token: orgTok,
				body: `{"id":"` + stored.ID + `","username":"` + long + `","password":"` + long + `"}`, wantError: bothMsg, want: bothItems},
			{name: "PUT by path username and password over", method: http.MethodPut, path: byPath, token: repoTok, body: both, wantError: bothMsg, want: bothItems},
			{name: "POST password over", method: http.MethodPost, path: v1, token: repoTok, body: `{"password":"` + long + `"}`,
				wantError: "invalid password (want at most 131042 bytes)", want: []ipParam{{"password", credWhy}}},
			// A value over the limit that also holds a NUL is one problem.
			{name: "POST username over with a NUL", method: http.MethodPost, path: v1, token: repoTok,
				body: `{"username":"` + long + `\u0000","password":"p"}`, wantError: "invalid username (want UTF-8 text without NUL)",
				want: []ipParam{{"username", ipcNulReason}}},
		})
		fresh, err := e.store.CreateRepo(context.Background(), org, "https://example.com/"+org+"/lim-fresh-"+randomHex(t, 4), "main")
		if err != nil {
			t.Fatalf("create repo: %v", err)
		}
		freshTok := e.mint(t, tokenSpec{iss: org, repo: fresh.URL, scopes: []string{auth.ScopeRepoWrite}})
		atLimit := strings.Repeat("c", credMax)
		pair := `{"username":"` + atLimit + `","password":"` + atLimit + `"}`
		for _, tc := range []struct {
			name, method, path, token, body string
		}{
			{"POST", http.MethodPost, v1, freshTok, pair},
			{"PUT", http.MethodPut, v1, orgTok, `{"id":"` + stored.ID + `","username":"` + atLimit + `","password":"` + atLimit + `"}`},
			{"PUT by path", http.MethodPut, byPath, repoTok, pair},
		} {
			resp, out := e.doReq(t, tc.method, tc.path, tc.token, []byte(tc.body), "")
			if resp.StatusCode >= 300 {
				t.Fatalf("%s at the limit: status = %d, want 2xx (body %.300s)", tc.name, resp.StatusCode, out)
			}
		}
	})
}
