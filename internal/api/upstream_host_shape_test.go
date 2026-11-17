package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"
)

// TestSyncCreateRefusesUndialableHosts: POST /repos refuses, with 400
// before anything is stored, a host that no sync run could use, in both
// host fields. Before, the create answered 201 and every run failed.
func TestSyncCreateRefusesUndialableHosts(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		for _, host := range []string{"<<<.com", "https://1.2.3.4%25eth0", "https://forge.example?", "2130706433", "0x7f000001", "forge.example:65536", "HTTPS://forge.example"} {
			for _, field := range []struct{ name, body, msg string }{
				{"upstream.host", `{"upstream":{"provider":"gitea","owner":"o","name":"n","host":%s}}`, "invalid upstream host "},
				{"base_repo.upstream_host", `{"base_repo":{"provider":"gitea","owner":"o","name":"n","upstream_host":%s}}`, "invalid base_repo upstream_host "},
			} {
				t.Run(field.name+" "+host, func(t *testing.T) {
					name := "undialable-" + randomHex(t, 4)
					tok := e.mint(t, tokenSpec{iss: org, repo: name, scopes: []string{"repo:write"}})
					quoted, err := json.Marshal(host)
					if err != nil {
						t.Fatal(err)
					}
					resp, out := e.doReq(t, http.MethodPost, "/api/v1/repos", tok, []byte(strings.Replace(field.body, "%s", string(quoted), 1)), "")
					var p problemBody
					if err := json.Unmarshal(out, &p); err != nil {
						t.Fatal(err)
					}
					wantDetail := field.msg + strconv.Quote(host)
					if resp.StatusCode != http.StatusBadRequest || p.Detail != wantDetail ||
						len(p.InvalidParams) != 1 || p.InvalidParams[0].Name != field.name || p.InvalidParams[0].Reason != "is not a valid host" {
						t.Fatalf("status %d, body %s; want 400 %q naming %s", resp.StatusCode, out, wantDetail, field.name)
					}
					if _, err := e.store.GetRepoByURL(t.Context(), org, name); err == nil {
						t.Fatal("the refused create stored a repository")
					}
				})
			}
		}
	})
}
