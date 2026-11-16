package githttp

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestMerge_ExpectedSourceSHA drives the merge source pin (#531) through a
// real serve node on every lane (pg, local, s3wal): the merge uses the
// pinned commit although the source branch has moved past it, and a pin
// that the source branch does not contain is a 409 with code
// ref_guard_failed (#568) that leaves the target unchanged.
func TestMerge_ExpectedSourceSHA(t *testing.T) {
	repoURL := fmt.Sprintf("t531-merge-pin-%d", time.Now().UnixNano())
	repoID := registerRepoURL(t, repoURL)
	token := mintTestToken(t, repoURL, "git:read,git:write", "")
	remote := remoteURL(nodeA, repoID, token)

	work := initWorkRepo(t)
	base := revParse(t, work, "HEAD")
	mustGit(t, work, "push", remote, "HEAD:refs/heads/main", "HEAD:refs/heads/target")
	pin := commitFile(t, work, "p1.txt", "p1\n", "p1", nil)
	commitFile(t, work, "p2.txt", "p2\n", "p2", nil) // source tip, past the pin
	mustGit(t, work, "push", remote, "HEAD:refs/heads/feature")
	mustGit(t, work, "checkout", "-q", "-b", "side", base)
	side := commitFile(t, work, "s.txt", "s\n", "side", nil)
	mustGit(t, work, "push", remote, "HEAD:refs/heads/side")

	tipOf := func(branch string) string {
		t.Helper()
		out := mustGit(t, "", "ls-remote", remote, "refs/heads/"+branch)
		for line := range strings.Lines(out) { // stderr warnings are mixed in
			if f := strings.Fields(line); len(f) == 2 && f[1] == "refs/heads/"+branch {
				return f[0]
			}
		}
		t.Fatalf("ls-remote %s = %q", branch, out)
		return ""
	}
	merge := func(expected string) (int, map[string]any) {
		t.Helper()
		body := fmt.Sprintf(`{"source_ref":"feature","target_branch":"target","strategy":"ff_only","expected_source_sha":%q}`, expected)
		req, err := http.NewRequest(http.MethodPost, "http://"+nodeA.addr+"/api/v1/repos/merge", strings.NewReader(body))
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
		decoded := map[string]any{}
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("decode merge response %q: %v", raw, err)
		}
		return resp.StatusCode, decoded
	}

	status, body := merge(side)
	if status != http.StatusConflict || body["code"] != "ref_guard_failed" ||
		body["guard"] != "expected_source_sha" || body["current_sha"] != tipOf("feature") {
		t.Fatalf("pin on another branch: %d %v, want 409 ref_guard_failed with the feature tip", status, body)
	}
	if got := tipOf("target"); got != base {
		t.Fatalf("target moved to %s after a refused merge, want %s", got, base)
	}

	status, body = merge(pin)
	if status != http.StatusOK || body["result"] != "fast_forward" || body["commit_sha"] != pin {
		t.Fatalf("pinned merge: %d %v, want fast_forward to %s", status, body, pin)
	}
	if got := tipOf("target"); got != pin {
		t.Fatalf("target = %s, want the pinned commit %s, not the source tip", got, pin)
	}
}
