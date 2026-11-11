package githttp

import (
	"context"
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
		adv := smartHTTP(t, repo, token, http.MethodGet, "info/refs?service="+service, "")
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
	// git refuses a want of an unadvertised commit on the client side, so
	// the upload-pack RPC gets that want raw: it must refuse it too.
	want := pktLine("want "+solo+"\n") + "0000" + pktLine("done\n")
	if rpc := smartHTTP(t, repo, token, http.MethodPost, "git-upload-pack", want); !strings.Contains(rpc, "not our ref") || strings.Contains(rpc, "PACK") {
		t.Fatalf("upload-pack RPC answered a want of %s:\n%q", solo, rpc)
	}

	// Every push to refs/namespaces/ gets its own ng, and the authority
	// keeps its value: an update, a delete, a creation in another
	// namespace, and #797's branch named HEAD. The pushed commit is new
	// to the server, and no pack is stored for it: no ref can reach it.
	other := commitFile(t, work, "other.txt", "other\n", "persistent", nil)
	packsBefore, blobsBefore := len(pgPacks(t, repo)), len(s3Packs(t, repo))
	for _, tc := range []struct{ spec, msg, ref string }{
		{"HEAD:" + agent, "deny updating a hidden ref", agent},
		{":" + agent, "deny deleting a hidden ref", agent},
		{"HEAD:refs/namespaces/other/refs/heads/x", "deny updating a hidden ref", "refs/namespaces/other/refs/heads/x"},
		{"HEAD:refs/namespaces/ephemeral/refs/heads/HEAD", "deny updating a hidden ref", "refs/namespaces/ephemeral/refs/heads/HEAD"},
		{"HEAD:refs/namespaces", "deny updating a hidden ref", "refs/namespaces"},
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
	if packs, blobs := len(pgPacks(t, repo)), len(s3Packs(t, repo)); packs != packsBefore || blobs != blobsBefore {
		t.Fatalf("pushes to hidden refs stored packs: registry %d -> %d, blob store %d -> %d", packsBefore, packs, blobsBefore, blobs)
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
	if err == nil || !strings.Contains(out, "deny updating a hidden ref") || !strings.Contains(out, "is hidden on this remote") {
		t.Fatalf("atomic push: err=%v\n%s", err, out)
	}
	if refs := pgRefs(t, repo); refs["refs/heads/side"] != "" || refs["refs/namespaces/ephemeral/refs/heads/side"] != "" {
		t.Fatalf("atomic push landed in part: side=%q, hidden=%q", refs["refs/heads/side"], refs["refs/namespaces/ephemeral/refs/heads/side"])
	}
	// A delete-only atomic push carries no pack, so only pre-receive's
	// atomic gate stops its visible delete. The hidden ref is absent, so
	// the atomic base check (old values against the authority) passes.
	out, err = tryGit(work, "push", "--atomic", persistent, ":refs/heads/mixed", ":refs/namespaces/ephemeral/refs/heads/nope")
	if err == nil || !strings.Contains(out, "deny deleting a hidden ref") {
		t.Fatalf("atomic delete-only push: err=%v\n%s", err, out)
	}
	if got := pgRefs(t, repo)["refs/heads/mixed"]; got != other {
		t.Fatalf("atomic delete-only push landed in part: mixed=%q, want %q", got, other)
	}

	// Only refs/namespaces itself and the refs under it are hidden.
	// The pushed commit is new to the server, so its pack is published.
	const near = "refs/namespacesx/refs/heads/n"
	nearSHA := strings.TrimSpace(mustGit(t, work, "commit-tree", "HEAD^{tree}", "-p", "HEAD", "-m", "near"))
	mustGit(t, work, "push", persistent, nearSHA+":"+near)
	if got := lsRemoteSHA(t, "", persistent, near); got != nearSHA {
		t.Fatalf("persistent %s = %q, want %q", near, got, nearSHA)
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
	for _, remote := range []string{eph, remoteURL(nodeA, repo+"+import", token)} {
		mustGit(t, work, "push", "--atomic", remote, "HEAD:refs/heads/both", "HEAD:"+kept)
		if got := lsRemoteSHA(t, "", remote, kept); got != next {
			t.Fatalf("%s on a namespaced remote = %q, want %q", kept, got, next)
		}
	}
}

// A hidden ref never commits (git refuses it before any update hook), so a
// plain push that names one reserves no push hold for it, with or without
// a pack: a REST write or a hydration of that ephemeral branch never waits
// behind the push.
func TestPersistentRemoteHoldsNoHiddenRef(t *testing.T) {
	repo := registerRepo(t, "t-hidden-hold")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main", "HEAD:refs/heads/dd")
	body := `{"base_ref":"main","target_branch":"gone","target_is_ephemeral":true}`
	if status := apiJSON(t, nodeA, http.MethodPost, token, "/api/v1/repos/branches/create", body, nil); status != http.StatusOK {
		t.Fatalf("ephemeral branch create: status %d", status)
	}
	head := commitFile(t, work, "hh.txt", "hh\n", "held", nil)

	nodeCache := t.TempDir()
	node, err := startNode(nodeCache, EnvHookDelay+"=3000")
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(node.stop)
	type pushResult struct {
		out string
		err error
	}
	for _, tc := range []struct {
		name, visible, hidden string
		specs                 []string
	}{
		{"with a pack", "refs/heads/hh", "refs/namespaces/ephemeral/refs/heads/hh", []string{"HEAD:refs/heads/hh", "HEAD:refs/namespaces/ephemeral/refs/heads/hh"}},
		{"delete only", "refs/heads/dd", "refs/namespaces/ephemeral/refs/heads/gone", []string{":refs/heads/dd", ":refs/namespaces/ephemeral/refs/heads/gone"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := pgRefs(t, repo)[tc.hidden]
			done := make(chan pushResult, 1)
			go func() {
				out, err := tryGit(work, append([]string{"push", remoteURL(node, repo, token)}, tc.specs...)...)
				done <- pushResult{out, err}
			}()
			waitForPushHold(t, nodeCache, repo, tc.visible, done)
			holds, err := readPushHolds(filepath.Join(RepoPath(nodeCache, repo), pushHoldDir), "")
			if err != nil {
				t.Fatal(err)
			}
			if holds.contains(tc.hidden) {
				t.Fatalf("the push holds %s, which it can never commit", tc.hidden)
			}
			res := <-done
			if res.err == nil || !strings.Contains(res.out, "a hidden ref") {
				t.Fatalf("push: err=%v\n%s", res.err, res.out)
			}
			if got := pgRefs(t, repo)[tc.hidden]; got != before {
				t.Fatalf("authority %s moved from %q to %q", tc.hidden, before, got)
			}
		})
	}
	if refs := pgRefs(t, repo); refs["refs/heads/hh"] != head || refs["refs/heads/dd"] != "" {
		t.Fatalf("authority hh=%q dd=%q, want hh=%s and dd deleted", refs["refs/heads/hh"], refs["refs/heads/dd"], head)
	}
	// The +ephemeral remote hides nothing, so its refs/namespaces/ refs
	// still take holds.
	done := make(chan pushResult, 1)
	const kept = "refs/namespaces/kept/refs/heads/h"
	go func() {
		out, err := tryGit(work, "push", remoteURL(node, repo+"+ephemeral", token), "HEAD:"+kept)
		done <- pushResult{out, err}
	}()
	waitForPushHold(t, nodeCache, repo+"+ephemeral", kept, done)
	if res := <-done; res.err != nil {
		t.Fatalf("+ephemeral push: %v\n%s", res.err, res.out)
	}
}

// The persistent remote serves no commit that only a hidden ref reaches,
// also when the repository's default branch is that hidden ref (an
// earlier version, an older replica or a sync could store one, and git
// advertises HEAD whatever it points at) and when git config would allow
// a want of an unadvertised commit.
func TestPersistentRemoteServesNoHiddenCommit(t *testing.T) {
	const agent = "refs/namespaces/ephemeral/refs/heads/agent"
	rec, err := testMeta.CreateRepo(context.Background(), testOrg, runUnique("t-hidden-head"), agent)
	if err != nil {
		t.Fatal(err)
	}
	repo := rec.RepoID
	cache := t.TempDir()
	node, err := startNode(cache)
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(node.stop)
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	persistent := remoteURL(node, repo, token)
	mustGit(t, work, "push", persistent, "HEAD:refs/heads/main")
	solo := commitFile(t, work, "solo.txt", "solo\n", "ephemeral only", nil)
	mustGit(t, work, "push", persistent, "HEAD:refs/heads/tmp")
	body := `{"base_ref":"tmp","target_branch":"agent","target_is_ephemeral":true}`
	if status := apiJSON(t, node, http.MethodPost, token, "/api/v1/repos/branches/create", body, nil); status != http.StatusOK {
		t.Fatalf("ephemeral branch create: status %d", status)
	}
	mustGit(t, work, "push", persistent, ":refs/heads/tmp")

	for _, service := range []string{"git-upload-pack", "git-receive-pack"} {
		adv := smartHTTPAt(t, node, repo, token, http.MethodGet, "info/refs?service="+service, "")
		if strings.Contains(adv, solo) || strings.Contains(adv, "refs/namespaces/") || !strings.Contains(adv, "refs/heads/main") {
			t.Fatalf("the %s advertisement shows the hidden default branch:\n%q", service, adv)
		}
	}
	mirror := filepath.Join(t.TempDir(), "mirror.git")
	mustGit(t, "", "clone", "--mirror", persistent, mirror)
	if _, err := tryGit(mirror, "cat-file", "-e", solo); err == nil {
		t.Fatalf("mirror clone holds %s, which only %s reaches", solo, agent)
	}
	want := pktLine("want "+solo+"\n") + "0000" + pktLine("done\n")
	for _, key := range []string{"uploadpack.allowTipSHA1InWant", "uploadpack.allowReachableSHA1InWant", "uploadpack.allowAnySHA1InWant"} {
		mustGit(t, "", "-C", RepoPath(cache, repo), "config", key, "true")
		if rpc := smartHTTPAt(t, node, repo, token, http.MethodPost, "git-upload-pack", want); !strings.Contains(rpc, "not our ref") || strings.Contains(rpc, "PACK") {
			t.Fatalf("with %s=true, upload-pack answered a want of %s:\n%q", key, solo, rpc)
		}
		mustGit(t, "", "-C", RepoPath(cache, repo), "config", "--unset", key)
	}
}

// smartHTTP sends one smart-HTTP request for repo to nodeA and returns the
// 200 response body.
func smartHTTP(t *testing.T, repo, token, method, rest, body string) string {
	t.Helper()
	return smartHTTPAt(t, nodeA, repo, token, method, rest, body)
}

// smartHTTPAt is smartHTTP against node n.
func smartHTTPAt(t *testing.T, n *testNode, repo, token, method, rest, body string) string {
	t.Helper()
	req, err := http.NewRequest(method, "http://"+n.addr+"/"+repo+".git/"+rest, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("t:"+token)))
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/x-"+rest+"-request")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s %s = %d: %s", method, rest, resp.StatusCode, out)
	}
	return string(out)
}
