package githttp

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/meta"
)

// TestFork_PendingNameAnswers503 (issue #388) runs on every lane, S3-WAL
// included (ADR-51 v2 stores create state): a fork through the real server
// ends complete, and repeat plain, copy and fork creates of it answer 200.
// A fork row left pending, as a crash before its last step leaves it,
// answers plain, copy and fork creates with 503 + Retry-After and a sync
// create with 409, until DELETE clears the name. A legacy row is 409, and a
// repeat sync create is 200 (S3-WAL tracks sync creates since #451).
func TestFork_PendingNameAnswers503(t *testing.T) {
	src := registerRepo(t, "pending-fork-src")
	// The names this test creates through the API, unique per run (see runUnique).
	done, legacyName, syncName, stuck := runUnique("pending-fork-done"), runUnique("pending-fork-legacy"), runUnique("pending-fork-sync"), runUnique("pending-fork-stuck")
	work := initWorkRepo(t)
	mustGit(t, work, "push", remoteURL(nodeA, src, mintTestToken(t, src, "git:read,git:write", "")), "HEAD:refs/heads/main")
	srcTok := mintTestToken(t, src, "git:read", "")

	fork := fmt.Sprintf(`{"base_repo":{"provider":"code","operation":"fork","auth":{"token":%q}}}`, srcTok)
	copyBody := fmt.Sprintf(`{"base_repo":{"name":%q,"auth":{"token":%q}}}`, src, srcTok)
	syncBody := `{"upstream":{"provider":"gitea","owner":"o","name":"n","host":"upstream.invalid"}}`
	post := func(name, body string) (int, string) {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, "http://"+nodeA.addr+"/api/v1/repos", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+mintTestToken(t, name, "repo:write", ""))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if _, err := io.Copy(io.Discard, resp.Body); err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, resp.Header.Get("Retry-After")
	}

	if code, _ := post(done, fork); code != http.StatusCreated {
		t.Fatalf("fork = %d, want 201", code)
	}
	if r, err := testMeta.GetRepoByURL(context.Background(), testOrg, done); err != nil || r.CreateState != meta.CreateStateComplete {
		t.Fatalf("forked repo = %+v, %v; want complete", r, err)
	}
	legacy := registerRepoURL(t, legacyName) // CreateRepo: a legacy row
	if code, _ := post(syncName, syncBody); code != http.StatusCreated {
		t.Fatalf("sync create = %d, want 201", code)
	}
	for _, c := range []struct {
		name, form, body string
		want             int
	}{
		{done, "plain", "", http.StatusOK}, {done, "copy", copyBody, http.StatusOK},
		{done, "fork", fork, http.StatusOK}, {done, "sync", syncBody, http.StatusConflict},
		{legacyName, "plain", "", http.StatusConflict}, {legacyName, "copy", copyBody, http.StatusConflict},
		{syncName, "sync", syncBody, http.StatusOK}, {syncName, "plain", "", http.StatusConflict},
	} {
		if code, _ := post(c.name, c.body); code != c.want {
			t.Fatalf("repeat %s create of %s = %d, want %d", c.form, c.name, code, c.want)
		}
	}
	if r, err := testMeta.GetRepoByID(context.Background(), testOrg, legacy); err != nil || r.CreateState != meta.CreateStateLegacy {
		t.Fatalf("legacy row = %+v, %v; want legacy", r, err)
	}

	// The row a crash before the fork's last step leaves behind.
	pending, err := testMeta.CreateRepoWithState(context.Background(), testOrg, stuck, "main", meta.CreateStatePending)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		form, body string
		want       int
	}{{"plain", "", http.StatusServiceUnavailable}, {"copy", copyBody, http.StatusServiceUnavailable},
		{"fork", fork, http.StatusServiceUnavailable}, {"sync", syncBody, http.StatusConflict}} {
		code, ra := post(stuck, c.body)
		if code != c.want || (code == http.StatusServiceUnavailable && ra == "") {
			t.Fatalf("%s create of a pending fork name = %d (Retry-After %q), want %d", c.form, code, ra, c.want)
		}
	}
	if got, err := testMeta.GetRepoByID(context.Background(), testOrg, pending.RepoID); err != nil || got.CreateState != meta.CreateStatePending {
		t.Fatalf("pending row = %+v, %v; want still pending", got, err)
	}
}
