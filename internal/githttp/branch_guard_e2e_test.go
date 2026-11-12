package githttp

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/ref"
)

// TestBranchCreateTargetGuard drives expected_target_sha on
// POST /api/v1/repos/branches/create (#803) through a real serve node, so
// each lane (pg, local SQLite, S3-WAL) checks the guard against its own ref
// store: 40 zeros requires a new branch, the current tip guards a move,
// and a failed guard answers 409 ref_guard_failed and leaves the
// stored ref alone, even with force.
func TestBranchCreateTargetGuard(t *testing.T) {
	url := runUnique("branch-guard")
	id := registerRepoURL(t, url)
	token := mintTestToken(t, url, "git:read,git:write", "")
	work := initWorkRepo(t)
	c1 := revParse(t, work, "HEAD")
	c2 := commitFile(t, work, "b.txt", "two\n", "two", nil)
	mustGit(t, work, "push", remoteURL(nodeA, id, token), "HEAD:refs/heads/main")

	post := func(t *testing.T, body map[string]any) (int, map[string]any) {
		t.Helper()
		payload, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		req, err := http.NewRequest(http.MethodPost, "http://"+nodeA.addr+"/api/v1/repos/branches/create", strings.NewReader(string(payload)))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]any{}
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("decode %s: %v", raw, err)
		}
		return resp.StatusCode, out
	}
	requireGuard := func(t *testing.T, status int, body map[string]any, current string) {
		t.Helper()
		if status != http.StatusConflict || body["code"] != "ref_guard_failed" ||
			body["guard"] != "expected_target_sha" || body["current_sha"] != current {
			t.Fatalf("status %d body %v, want 409 ref_guard_failed with current_sha %s", status, body, current)
		}
	}

	for _, ephemeral := range []bool{false, true} {
		t.Run(fmt.Sprintf("ephemeral=%t", ephemeral), func(t *testing.T) {
			stored := func(branch string) string {
				if ephemeral {
					return auth.EphemeralRefName(branch)
				}
				return "refs/heads/" + branch
			}
			requireRef := func(t *testing.T, branch, want string) {
				t.Helper()
				got, ok := pgRefs(t, id)[stored(branch)]
				if want == "" && ok {
					t.Fatalf("%s = %s, want no ref", stored(branch), got)
				}
				if want != "" && got != want {
					t.Fatalf("%s = %q, want %s", stored(branch), got, want)
				}
			}
			req := func(branch string, extra map[string]any) map[string]any {
				b := map[string]any{"target_branch": branch, "base_ref": "main", "target_is_ephemeral": ephemeral}
				for k, v := range extra {
					b[k] = v
				}
				return b
			}

			if status, body := post(t, req("new", map[string]any{"expected_target_sha": ref.ZeroSHA})); status != http.StatusOK || body["commit_sha"] != c2 {
				t.Fatalf("zeros on a new branch: status %d body %v", status, body)
			}
			requireRef(t, "new", c2)

			if status, body := post(t, req("old", map[string]any{"base_ref": c1})); status != http.StatusOK {
				t.Fatalf("seed old: status %d body %v", status, body)
			}
			status, body := post(t, req("old", map[string]any{"expected_target_sha": ref.ZeroSHA, "force": true}))
			requireGuard(t, status, body, c1)
			requireRef(t, "old", c1)
			status, body = post(t, req("old", map[string]any{"expected_target_sha": c2, "force": true}))
			requireGuard(t, status, body, c1)
			requireRef(t, "old", c1)
			if status, body := post(t, req("old", map[string]any{"expected_target_sha": c1, "force": true})); status != http.StatusOK || body["message"] != "branch updated" {
				t.Fatalf("current tip with force: status %d body %v", status, body)
			}
			requireRef(t, "old", c2)

			if status, body := post(t, req("ff", map[string]any{"base_ref": c1})); status != http.StatusOK {
				t.Fatalf("seed ff: status %d body %v", status, body)
			}
			if status, body := post(t, req("ff", map[string]any{"expected_target_sha": c1})); status != http.StatusOK || body["message"] != "branch updated" {
				t.Fatalf("current tip without force: status %d body %v", status, body)
			}
			requireRef(t, "ff", c2)

			status, body = post(t, req("missing", map[string]any{"expected_target_sha": c1}))
			requireGuard(t, status, body, ref.ZeroSHA)
			requireRef(t, "missing", "")
		})
	}
}
