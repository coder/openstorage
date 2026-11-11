package githttp

import (
	"encoding/base64"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
)

// docs/explanation/git-protocol-and-namespaces.md: persistent remotes must
// not see ephemeral refs (issue #799). The API keeps its ephemeral branches
// under the base repository as refs/namespaces/ephemeral/refs/heads/<name>,
// so the persistent remote hides every refs/namespaces/ ref from fetch,
// ls-remote and push, while the `+ephemeral` remote keeps its own ref space.
func TestPersistentRemoteHidesNamespacedRefs(t *testing.T) {
	repo := registerRepo(t, "t-hidden-ns")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	persistent := remoteURL(nodeA, repo, token)
	mustGit(t, work, "push", persistent, "HEAD:refs/heads/main")

	// A commit that only the API's ephemeral branch reaches: push it to a
	// temporary branch, branch the ephemeral copy from it over REST, then
	// delete the temporary branch.
	solo := commitFile(t, work, "solo.txt", "solo\n", "ephemeral only", nil)
	mustGit(t, work, "push", persistent, "HEAD:refs/heads/tmp")
	body := `{"base_ref":"tmp","target_branch":"agent","target_is_ephemeral":true}`
	if status := apiJSON(t, nodeA, http.MethodPost, token, "/api/v1/repos/branches/create", body, nil); status != http.StatusOK {
		t.Fatalf("ephemeral branch create: status %d", status)
	}
	mustGit(t, work, "push", persistent, ":refs/heads/tmp")
	const agent = "refs/namespaces/ephemeral/refs/heads/agent"
	if got := pgRefs(t, repo)[agent]; got != solo {
		t.Fatalf("authority %s = %q, want %q", agent, got, solo)
	}

	// Both advertisements leave the namespaced refs out.
	if out := mustGit(t, "", "ls-remote", persistent); strings.Contains(out, "refs/namespaces/") || !strings.Contains(out, "refs/heads/main") {
		t.Fatalf("persistent ls-remote:\n%s", out)
	}
	for _, service := range []string{"git-upload-pack", "git-receive-pack"} {
		adv := infoRefs(t, repo, token, service)
		if strings.Contains(adv, "refs/namespaces/") || !strings.Contains(adv, "refs/heads/main") {
			t.Fatalf("%s advertisement:\n%q", service, adv)
		}
	}

	// A fetch never brings a namespaced ref or the commit only it reaches.
	mirror := filepath.Join(t.TempDir(), "mirror.git")
	mustGit(t, "", "clone", "--mirror", persistent, mirror)
	if out := mustGit(t, mirror, "for-each-ref", "refs/namespaces/"); out != "" {
		t.Fatalf("mirror clone holds namespaced refs:\n%s", out)
	}
	if _, err := tryGit(mirror, "cat-file", "-e", solo); err == nil {
		t.Fatalf("mirror clone holds %s, which only %s reaches", solo, agent)
	}
	if out, err := tryGit(mirror, "fetch", persistent, agent); err == nil || !strings.Contains(out, "couldn't find remote ref") {
		t.Fatalf("fetch of %s by name: err=%v\n%s", agent, err, out)
	}

	// Every push to refs/namespaces/ gets its own ng, and the authority
	// keeps its value: an update, a delete, a creation in another
	// namespace, and #797's branch named HEAD.
	other := commitFile(t, work, "other.txt", "other\n", "persistent", nil)
	for _, tc := range []struct{ spec, msg, ref string }{
		{"HEAD:" + agent, "deny updating a hidden ref", agent},
		{":" + agent, "deny deleting a hidden ref", agent},
		{"HEAD:refs/namespaces/other/refs/heads/x", "deny updating a hidden ref", "refs/namespaces/other/refs/heads/x"},
		{"HEAD:refs/namespaces/ephemeral/refs/heads/HEAD", "deny updating a hidden ref", "refs/namespaces/ephemeral/refs/heads/HEAD"},
	} {
		before := pgRefs(t, repo)[tc.ref]
		out, err := tryGit(work, "push", persistent, tc.spec)
		if err == nil || !strings.Contains(out, "[remote rejected]") || !strings.Contains(out, tc.msg) {
			t.Fatalf("push %s: err=%v, want a %q rejection:\n%s", tc.spec, err, tc.msg, out)
		}
		if got := pgRefs(t, repo)[tc.ref]; got != before {
			t.Fatalf("push %s moved %s from %q to %q", tc.spec, tc.ref, before, got)
		}
	}

	// A plain push with a hidden ref still lands its other refs; an atomic
	// one lands nothing.
	out, err := tryGit(work, "push", persistent, "HEAD:refs/heads/mixed", "HEAD:refs/namespaces/ephemeral/refs/heads/mixed")
	if err == nil || !strings.Contains(out, "deny updating a hidden ref") {
		t.Fatalf("mixed push: err=%v\n%s", err, out)
	}
	if refs := pgRefs(t, repo); refs["refs/heads/mixed"] != other || refs["refs/namespaces/ephemeral/refs/heads/mixed"] != "" {
		t.Fatalf("mixed push: authority mixed=%q, hidden=%q", refs["refs/heads/mixed"], refs["refs/namespaces/ephemeral/refs/heads/mixed"])
	}
	out, err = tryGit(work, "push", "--atomic", persistent, "HEAD:refs/heads/side", "HEAD:refs/namespaces/ephemeral/refs/heads/side")
	if err == nil || !strings.Contains(out, "deny updating a hidden ref") {
		t.Fatalf("atomic push: err=%v\n%s", err, out)
	}
	if refs := pgRefs(t, repo); refs["refs/heads/side"] != "" || refs["refs/namespaces/ephemeral/refs/heads/side"] != "" {
		t.Fatalf("atomic push landed in part: side=%q, hidden=%q", refs["refs/heads/side"], refs["refs/namespaces/ephemeral/refs/heads/side"])
	}

	// The API still lists and resolves its ephemeral branch.
	var branches struct {
		Branches []struct {
			Name    string `json:"name"`
			HeadSHA string `json:"head_sha"`
		} `json:"branches"`
	}
	if status := apiGet(t, nodeA, token, "/api/v1/repos/branches?ephemeral=true", &branches); status != http.StatusOK {
		t.Fatalf("GET branches?ephemeral=true = %d", status)
	}
	if len(branches.Branches) != 1 || branches.Branches[0].Name != "agent" || branches.Branches[0].HeadSHA != solo {
		t.Fatalf("ephemeral branches = %+v, want agent@%s", branches.Branches, solo)
	}

	// The `+ephemeral` remote keeps listing and updating its own refs.
	eph := remoteURL(nodeA, repo+"+ephemeral", token)
	mustGit(t, work, "push", eph, "HEAD:refs/heads/scratch")
	if got := lsRemoteSHA(t, "", eph, "refs/heads/scratch"); got != other {
		t.Fatalf("+ephemeral scratch = %q, want %q", got, other)
	}
	next := commitFile(t, work, "next.txt", "next\n", "ephemeral update", nil)
	mustGit(t, work, "push", eph, "HEAD:refs/heads/scratch")
	if got := lsRemoteSHA(t, "", eph, "refs/heads/scratch"); got != next {
		t.Fatalf("+ephemeral scratch after update = %q, want %q", got, next)
	}
	mustGit(t, work, "push", eph, ":refs/heads/scratch")
	if got := lsRemoteSHA(t, "", eph, "refs/heads/scratch"); got != "" {
		t.Fatalf("+ephemeral scratch after delete = %q, want absent", got)
	}
	// Only the persistent remote hides refs/namespaces/: an atomic push to
	// `+ephemeral` still lands one, and its advertisement lists it.
	const kept = "refs/namespaces/kept/refs/heads/n"
	mustGit(t, work, "push", "--atomic", eph, "HEAD:refs/heads/both", "HEAD:"+kept)
	if got := lsRemoteSHA(t, "", eph, kept); got != next {
		t.Fatalf("+ephemeral %s = %q, want %q", kept, got, next)
	}
}

// infoRefs returns the smart-HTTP advertisement body of service for repo.
func infoRefs(t *testing.T, repo, token, service string) string {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://"+nodeA.addr+"/"+repo+".git/info/refs?service="+service, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("t:"+token)))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	adv, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET info/refs?service=%s = %d: %s", service, resp.StatusCode, adv)
	}
	return string(adv)
}
