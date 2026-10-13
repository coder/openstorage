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
// ends complete. A fork row left pending, as a crash before its last step
// leaves it, answers plain, copy and fork creates with 503 + Retry-After
// and a sync create with 409, until DELETE clears the name.
func TestFork_PendingNameAnswers503(t *testing.T) {
	src := registerRepo(t, "pending-fork-src")
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

	if code, _ := post("pending-fork-done", fork); code != http.StatusCreated {
		t.Fatalf("fork = %d, want 201", code)
	}
	if r, err := testMeta.GetRepoByURL(context.Background(), testOrg, "pending-fork-done"); err != nil || r.CreateState != meta.CreateStateComplete {
		t.Fatalf("forked repo = %+v, %v; want complete", r, err)
	}

	// The row a crash before the fork's last step leaves behind.
	pending, err := testMeta.CreateRepoWithState(context.Background(), testOrg, "pending-fork-stuck", "main", meta.CreateStatePending)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		form, body string
		want       int
	}{{"plain", "", http.StatusServiceUnavailable}, {"copy", copyBody, http.StatusServiceUnavailable},
		{"fork", fork, http.StatusServiceUnavailable}, {"sync", syncBody, http.StatusConflict}} {
		code, ra := post("pending-fork-stuck", c.body)
		if code != c.want || (code == http.StatusServiceUnavailable && ra == "") {
			t.Fatalf("%s create of a pending fork name = %d (Retry-After %q), want %d", c.form, code, ra, c.want)
		}
	}
	if got, err := testMeta.GetRepoByID(context.Background(), testOrg, pending.RepoID); err != nil || got.CreateState != meta.CreateStatePending {
		t.Fatalf("pending row = %+v, %v; want still pending", got, err)
	}
}
