package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/meta"
)

// TestSyncValuesTheRunRefusesAtRegistration: POST /repos and the git
// credential routes refuse, with 400 before anything is stored, the values
// that every sync run would refuse or that the store refuses: a credential
// with a carriage return or line feed (git's credential helper protocol
// reads one value per line), and a base_repo.auth.token with the reserved
// "enc:" prefix, which answered 500. No message repeats a credential.
func TestSyncValuesTheRunRefusesAtRegistration(t *testing.T) {
	const (
		host     = "forge.example.invalid"
		lineWhy  = "must not contain a carriage return or line feed"
		reserved = `invalid base_repo.auth.token: secret values must not start with the reserved prefix "enc:"`
	)
	body := func(v any) string {
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	baseRepo := func(token string) string {
		return body(map[string]any{"base_repo": map[string]any{
			"provider": "gitea", "owner": "o", "name": "n", "upstream_host": host, "auth": map[string]any{"token": token}}})
	}
	long := strings.Repeat("t", 131043)
	runBackends(t, func(t *testing.T, e *testEnv) {
		ctx := context.Background()
		org := e.newOrg(t)
		tok := e.mint(t, tokenSpec{iss: org, repo: "val-" + randomHex(t, 4), scopes: []string{auth.ScopeRepoWrite}})
		post := func(name, body, wantError string, want ...ipParam) ipCase {
			return ipCase{name: name, method: http.MethodPost, path: "/api/v1/repos", token: tok, body: body, wantError: wantError, want: want}
		}
		ipRun(t, e, []ipCase{
			post("base_repo token with the reserved prefix", baseRepo("enc:token"), reserved,
				ipParam{"base_repo.auth.token", "is a reserved value"}),
			post("base_repo token with a line feed", baseRepo("to\nken"),
				"invalid base_repo.auth.token (want no carriage return or line feed)", ipParam{"base_repo.auth.token", lineWhy}),
			post("base_repo token with a carriage return", baseRepo("token\r"),
				"invalid base_repo.auth.token (want no carriage return or line feed)", ipParam{"base_repo.auth.token", lineWhy}),
			// One problem for each token: a NUL first, then a line break,
			// then the length, then the reserved prefix.
			post("base_repo token with a NUL and a line feed", baseRepo("enc:\n\x00"),
				"invalid base_repo.auth.token (want UTF-8 text without NUL)", ipParam{"base_repo.auth.token", ipcNulReason}),
			post("base_repo token with a line feed and over the limit", baseRepo("enc:\n"+long),
				"invalid base_repo.auth.token (want no carriage return or line feed)", ipParam{"base_repo.auth.token", lineWhy}),
			post("base_repo token with the reserved prefix and over the limit", baseRepo("enc:"+long),
				"invalid base_repo.auth.token (want at most 131042 bytes)", ipParam{"base_repo.auth.token", "must be at most 131042 bytes"}),
			// The reserved token keeps its place among the other fields.
			post("base_repo reserved token and a bad host",
				body(map[string]any{"base_repo": map[string]any{
					"provider": "gitea", "owner": "o", "name": "n", "upstream_host": "bad host", "auth": map[string]any{"token": "enc:x"}}}),
				reserved+`; invalid base_repo upstream_host "bad host"`,
				ipParam{"base_repo.auth.token", "is a reserved value"}, ipParam{"base_repo.upstream_host", "is not a valid host"}),
		})

		// The tracked create (CreateLinkedRepo) refuses the same tokens
		// before it stores a row, and a repeat create of a linked repo
		// checks its body first too.
		s := newPendingSyncStack(t, e, true)
		linked := "val-linked-" + randomHex(t, 4)
		if code, _, msg := s.create(t, linked, baseRepo("token")); code != http.StatusCreated {
			t.Fatalf("tracked create = %d %q, want 201", code, msg)
		}
		for _, tc := range []struct{ token, want string }{
			{"to\nken", "invalid base_repo.auth.token (want no carriage return or line feed)"},
			{"enc:token", reserved},
		} {
			name := "val-tracked-" + randomHex(t, 4)
			if code, _, msg := s.create(t, name, baseRepo(tc.token)); code != http.StatusBadRequest || msg != tc.want {
				t.Fatalf("tracked create with token %q = %d %q, want 400 %q", tc.token, code, msg, tc.want)
			}
			if r, err := e.store.GetRepoByURL(ctx, s.org, name); !errors.Is(err, meta.ErrNotFound) {
				t.Fatalf("repo after a refused tracked create = %+v, %v; want not found", r, err)
			}
			if code, _, msg := s.create(t, linked, baseRepo(tc.token)); code != http.StatusBadRequest || msg != tc.want {
				t.Fatalf("repeat create with token %q = %d %q, want 400 %q", tc.token, code, msg, tc.want)
			}
		}

		repo, err := e.store.CreateRepo(ctx, org, "https://example.com/"+org+"/val-cred-"+randomHex(t, 4), "main")
		if err != nil {
			t.Fatalf("create repo: %v", err)
		}
		stored, err := e.store.CreateGitCredential(ctx, meta.GitCredential{RepoID: repo.RepoID, Username: "u0", Password: "p0"})
		if err != nil {
			t.Fatalf("create credential: %v", err)
		}
		orgTok := e.mint(t, tokenSpec{iss: org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeRepoWrite}})
		repoTok := e.mint(t, tokenSpec{iss: org, repo: repo.URL, scopes: []string{auth.ScopeRepoWrite}})
		const (
			v1       = "/api/v1/repos/git-credentials"
			bothMsg  = "invalid password (want no carriage return or line feed); invalid username (want no carriage return or line feed)"
			passOnly = "invalid password (want no carriage return or line feed)"
		)
		byName := "/api/repos/" + url.PathEscape(repo.URL) + "/git-credentials"
		byPath := byName + "/" + stored.ID
		both := `{"username":"a\nb","password":"p\r"}`
		bothItems := []ipParam{{"password", lineWhy}, {"username", lineWhy}}
		ipRun(t, e, []ipCase{
			{name: "POST username and password with line breaks", method: http.MethodPost, path: v1, token: repoTok, body: both, wantError: bothMsg, want: bothItems},
			{name: "POST on the path of the repository", method: http.MethodPost, path: byName, token: repoTok, body: both, wantError: bothMsg, want: bothItems},
			{name: "PUT username and password with line breaks", method: http.MethodPut, path: v1, token: orgTok,
				body: `{"id":"` + stored.ID + `","username":"a\nb","password":"p\r"}`, wantError: bothMsg, want: bothItems},
			{name: "PUT on the path of the repository", method: http.MethodPut, path: byName, token: orgTok,
				body: `{"id":"` + stored.ID + `","username":"a\nb","password":"p\r"}`, wantError: bothMsg, want: bothItems},
			{name: "PUT by path username and password with line breaks", method: http.MethodPut, path: byPath, token: repoTok, body: both, wantError: bothMsg, want: bothItems},
			{name: "POST password with a CRLF", method: http.MethodPost, path: v1, token: repoTok, body: `{"password":"p\r\nq"}`,
				wantError: passOnly, want: []ipParam{{"password", lineWhy}}},
		})

		// Nothing was stored: the stored credential is unchanged.
		got, err := e.store.GitCredentialForRepo(ctx, repo.RepoID)
		if err != nil || got.Username != "u0" || got.Password != "p0" {
			t.Fatalf("stored credential %q/%q, err %v; want u0/p0", got.Username, got.Password, err)
		}
	})
}

// TestSyncCreateQuotesValuesBounded: a sync create quotes a provider, an
// operation, an access or a host it refuses. A value of up to 256 bytes is
// quoted whole; a longer one is cut at 256 bytes, ends with an ellipsis and
// names its length, so the answer stays small whatever the body holds. A
// zone-scoped IPv6 host is not a valid host, so the egress answer, which
// repeats a hostname whole, never gets its zone.
func TestSyncCreateQuotesValuesBounded(t *testing.T) {
	huge := strings.Repeat("<", 900_000) // JSON escapes each to six bytes
	cut := strings.Repeat("<", 256) + "…"
	// A space makes the host invalid.
	hugeHost := " " + huge[1:]
	cutHost := " " + cut[1:]
	// A zone-scoped IPv6 address is not a valid host: the egress guard
	// refuses it under every policy, and its zone can be any text.
	zoneHost := "https://[fe80::1%25" + huge[:900_000-20] + "]"
	cutZone := zoneHost[:256] + "…"
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		tok := e.mint(t, tokenSpec{iss: org, repo: "quote-" + randomHex(t, 4), scopes: []string{auth.ScopeRepoWrite}})
		for _, tc := range []struct {
			name, body, want string
		}{
			{"upstream host", `{"upstream":{"provider":"gitea","owner":"o","name":"n","host":"` + hugeHost + `"}}`,
				`invalid upstream host "` + cutHost + `" (900000 bytes)`},
			{"upstream provider", `{"upstream":{"provider":"` + huge + `","owner":"o","name":"n"}}`,
				`unsupported upstream provider "` + cut + `" (900000 bytes)`},
			{"upstream access", `{"upstream":{"provider":"github","owner":"o","name":"n","access":"` + huge + `"}}`,
				`invalid upstream access "` + cut + `" (900000 bytes)`},
			{"base_repo upstream_host", `{"base_repo":{"provider":"gitea","owner":"o","name":"n","upstream_host":"` + hugeHost + `"}}`,
				`invalid base_repo upstream_host "` + cutHost + `" (900000 bytes)`},
			{"base_repo provider", `{"base_repo":{"provider":"` + huge + `","owner":"o","name":"n"}}`,
				`unsupported base_repo provider "` + cut + `" (900000 bytes)`},
			{"base_repo operation", `{"base_repo":{"provider":"gitea","owner":"o","name":"n","operation":"` + huge + `"}}`,
				`invalid base_repo operation "` + cut + `" (900000 bytes)`},
			// 256 bytes are quoted whole.
			{"upstream host at the limit", `{"upstream":{"provider":"gitea","owner":"o","name":"n","host":"` + hugeHost[:256] + `"}}`,
				`invalid upstream host "` + hugeHost[:256] + `"`},
			{"upstream host one byte over", `{"upstream":{"provider":"gitea","owner":"o","name":"n","host":"` + hugeHost[:257] + `"}}`,
				`invalid upstream host "` + cutHost + `" (257 bytes)`},
			{"upstream host with a long zone", `{"upstream":{"provider":"gitea","owner":"o","name":"n","host":"` + zoneHost + `"}}`,
				`invalid upstream host "` + cutZone + `" (900000 bytes)`},
			{"base_repo upstream_host with a long zone", `{"base_repo":{"provider":"gitea","owner":"o","name":"n","upstream_host":"` + zoneHost + `"}}`,
				`invalid base_repo upstream_host "` + cutZone + `" (900000 bytes)`},
			{"upstream host with a zone", `{"upstream":{"provider":"gitea","owner":"o","name":"n","host":"https://[fe80::1%25eth0]"}}`,
				`invalid upstream host "https://[fe80::1%25eth0]"`},
			{"bare upstream host with a zone", `{"upstream":{"provider":"gitea","owner":"o","name":"n","host":"[fe80::1%25eth0]:443"}}`,
				`invalid upstream host "[fe80::1%25eth0]:443"`},
		} {
			t.Run(tc.name, func(t *testing.T) {
				resp, out := e.doReq(t, http.MethodPost, "/api/v1/repos", tok, []byte(tc.body), "")
				var p problemBody
				if err := json.Unmarshal(out, &p); err != nil {
					t.Fatal(err)
				}
				if resp.StatusCode != http.StatusBadRequest || p.Detail != tc.want {
					t.Fatalf("status %d, detail %.400q; want 400 %.400q", resp.StatusCode, p.Detail, tc.want)
				}
				if len(out) > 8<<10 {
					t.Fatalf("answer is %d bytes, want at most 8 KiB", len(out))
				}
			})
		}
	})
}
