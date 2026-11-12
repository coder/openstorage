package githttp

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
	// `+ephemeral` hides nothing: an atomic push still lands a
	// refs/namespaces/ ref, and its advertisement lists it.
	const kept = "refs/namespaces/kept/refs/heads/n"
	mustGit(t, work, "push", "--atomic", eph, "HEAD:refs/heads/both", "HEAD:"+kept)
	if got := lsRemoteSHA(t, "", eph, kept); got != next {
		t.Fatalf("+ephemeral %s = %q, want %q", kept, got, next)
	}

	// An import push writes the base repository's refs, so it follows the
	// persistent remote's rules: each hidden ref gets its ng, a plain push
	// still lands its other refs, and an atomic one lands nothing.
	imp := remoteURL(nodeA, repo+"+import", token)
	if adv := smartHTTP(t, repo+"+import", token, http.MethodGet, "info/refs?service=git-receive-pack", ""); strings.Contains(adv, "refs/namespaces/") || !strings.Contains(adv, "refs/heads/") {
		t.Fatalf("+import receive-pack advertisement:\n%q", adv)
	}
	importPacks, importBlobs := len(pgPacks(t, repo)), len(s3Packs(t, repo))
	for _, tc := range []struct{ spec, msg, ref string }{
		{"HEAD:" + agent, "deny updating a hidden ref", agent},
		{":" + agent, "deny deleting a hidden ref", agent},
		{"HEAD:" + kept, "deny updating a hidden ref", kept},
	} {
		before := pgRefs(t, repo)[tc.ref]
		out, err := tryGit(work, "push", imp, tc.spec)
		if err == nil || !strings.Contains(out, "[remote rejected]") || !strings.Contains(out, tc.msg) {
			t.Fatalf("import push %s: err=%v, want a %q rejection:\n%s", tc.spec, err, tc.msg, out)
		}
		if got := pgRefs(t, repo)[tc.ref]; got != before {
			t.Fatalf("import push %s moved %s from %q to %q", tc.spec, tc.ref, before, got)
		}
	}
	if packs, blobs := len(pgPacks(t, repo)), len(s3Packs(t, repo)); packs != importPacks || blobs != importBlobs {
		t.Fatalf("import pushes to hidden refs stored packs: registry %d -> %d, blob store %d -> %d", importPacks, packs, importBlobs, blobs)
	}
	out, err = tryGit(work, "push", imp, "HEAD:refs/heads/imported", "HEAD:"+kept)
	if err == nil || !strings.Contains(out, "deny updating a hidden ref") {
		t.Fatalf("mixed import push: err=%v\n%s", err, out)
	}
	if refs := pgRefs(t, repo); refs["refs/heads/imported"] != next || refs[kept] != "" {
		t.Fatalf("mixed import push: authority imported=%q, hidden=%q", refs["refs/heads/imported"], refs[kept])
	}
	out, err = tryGit(work, "push", "--atomic", imp, "HEAD:refs/heads/imported-side", "HEAD:"+kept)
	if err == nil || !strings.Contains(out, "is hidden on this remote") {
		t.Fatalf("atomic import push: err=%v\n%s", err, out)
	}
	if refs := pgRefs(t, repo); refs["refs/heads/imported-side"] != "" || refs[kept] != "" {
		t.Fatalf("atomic import push landed in part: side=%q, hidden=%q", refs["refs/heads/imported-side"], refs[kept])
	}
	if got := lsRemoteSHA(t, "", persistent, "refs/heads/imported"); got != next {
		t.Fatalf("persistent refs/heads/imported = %q, want %q", got, next)
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
	// A commit the server does not have yet, so the import case needs a pack.
	imported := strings.TrimSpace(mustGit(t, work, "commit-tree", "HEAD^{tree}", "-p", "HEAD", "-m", "imported"))

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
		name, suffix, visible, hidden string
		specs                         []string
	}{
		{"with a pack", "", "refs/heads/hh", "refs/namespaces/ephemeral/refs/heads/hh", []string{"HEAD:refs/heads/hh", "HEAD:refs/namespaces/ephemeral/refs/heads/hh"}},
		{"delete only", "", "refs/heads/dd", "refs/namespaces/ephemeral/refs/heads/gone", []string{":refs/heads/dd", ":refs/namespaces/ephemeral/refs/heads/gone"}},
		// An import push writes the base key, so it takes no hold either.
		{"import with a pack", "+import", "refs/heads/ih", "refs/namespaces/ephemeral/refs/heads/ih", []string{imported + ":refs/heads/ih", imported + ":refs/namespaces/ephemeral/refs/heads/ih"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := pgRefs(t, repo)[tc.hidden]
			done := make(chan pushResult, 1)
			go func() {
				out, err := tryGit(work, append([]string{"push", remoteURL(node, repo+tc.suffix, token)}, tc.specs...)...)
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
			// Pre-receive writes the sidecar markers right after the hold,
			// and the server removes them with the hold when the push ends:
			// check for one until then (the hook delay keeps the push open).
			dir := filepath.Join(RepoPath(nodeCache, repo), pushHoldDir)
			var res pushResult
			for running := true; running; {
				select {
				case res = <-done:
					running = false
				case <-time.After(10 * time.Millisecond):
				}
				entries, err := os.ReadDir(dir)
				if err != nil && !os.IsNotExist(err) {
					t.Fatal(err)
				}
				for _, e := range entries {
					if isPushHoldSidecar(e.Name()) {
						continue
					}
					if _, err := os.Stat(pushHoldUnreservedMarker(filepath.Join(dir, e.Name()), tc.hidden)); err == nil {
						t.Fatalf("the push wrote a sidecar marker for %s, which it can never commit", tc.hidden)
					}
				}
			}
			if res.err == nil || !strings.Contains(res.out, "a hidden ref") {
				t.Fatalf("push: err=%v\n%s", res.err, res.out)
			}
			if got := pgRefs(t, repo)[tc.hidden]; got != before {
				t.Fatalf("authority %s moved from %q to %q", tc.hidden, before, got)
			}
		})
	}
	if refs := pgRefs(t, repo); refs["refs/heads/hh"] != head || refs["refs/heads/dd"] != "" || refs["refs/heads/ih"] != imported {
		t.Fatalf("authority hh=%q dd=%q ih=%q, want hh=%s, dd deleted and ih=%s", refs["refs/heads/hh"], refs["refs/heads/dd"], refs["refs/heads/ih"], head, imported)
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
	// A cache that an earlier version hydrated keeps HEAD on the hidden ref
	// and a marker that records that target: the next request repoints HEAD.
	rp := RepoPath(cache, repo)
	gen, _, _, ok := readGenerationMarker(rp)
	if !ok {
		t.Fatal("no generation marker after hydration")
	}
	mustGit(t, "", "-C", rp, "symbolic-ref", "HEAD", agent)
	writeGenerationMarker(rp, gen, agent)
	if adv := smartHTTPAt(t, node, repo, token, http.MethodGet, "info/refs?service=git-upload-pack", ""); strings.Contains(adv, solo) || strings.Contains(adv, "refs/namespaces/") {
		t.Fatalf("after an earlier version's hydration, the advertisement shows the hidden default branch:\n%q", adv)
	}

	// The +ephemeral remote hides nothing: a ref there under that name is
	// its HEAD.
	eph := remoteURL(node, repo+"+ephemeral", token)
	mustGit(t, work, "push", eph, "HEAD:"+agent)
	if out := mustGit(t, "", "ls-remote", "--symref", eph, "HEAD"); !strings.Contains(out, "ref: "+agent+"\tHEAD") {
		t.Fatalf("+ephemeral HEAD:\n%s", out)
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

// The server speaks git protocol v0 only, also when serve's own
// environment carries GIT_PROTOCOL: v2's fetch serves any want, a hidden
// ref's commits included.
func TestPersistentRemoteIgnoresServeGitProtocol(t *testing.T) {
	node, err := startNode(t.TempDir(), "GIT_PROTOCOL=version=2")
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(node.stop)
	repo := registerRepo(t, "t-v0-only")
	token := mintTestToken(t, repo, "git:read,git:write", "")
	work := initWorkRepo(t)
	persistent := remoteURL(node, repo, token)
	mustGit(t, work, "push", persistent, "HEAD:refs/heads/main")
	for _, service := range []string{"git-upload-pack", "git-receive-pack"} {
		if adv := smartHTTPAt(t, node, repo, token, http.MethodGet, "info/refs?service="+service, ""); strings.Contains(adv, "version 2") || !strings.Contains(adv, "refs/heads/main") {
			t.Fatalf("%s advertisement is not protocol v0:\n%q", service, adv)
		}
	}
	// The RPC child speaks v0 too: a v2 fetch request of a commit that only
	// an ephemeral branch reaches gets no pack, and a v0 want of it gets
	// "not our ref".
	solo := commitFile(t, work, "solo.txt", "solo\n", "ephemeral only", nil)
	mustGit(t, work, "push", persistent, "HEAD:refs/heads/tmp")
	body := `{"base_ref":"tmp","target_branch":"agent","target_is_ephemeral":true}`
	if status := apiJSON(t, node, http.MethodPost, token, "/api/v1/repos/branches/create", body, nil); status != http.StatusOK {
		t.Fatalf("ephemeral branch create: status %d", status)
	}
	mustGit(t, work, "push", persistent, ":refs/heads/tmp")
	v2 := pktLine("command=fetch\n") + pktLine("object-format=sha1\n") + "0001" + pktLine("want "+solo+"\n") + pktLine("done\n") + "0000"
	if rpc := smartHTTPAt(t, node, repo, token, http.MethodPost, "git-upload-pack", v2); strings.Contains(rpc, "PACK") {
		t.Fatalf("a v2 fetch got the hidden-only commit %s:\n%q", solo, rpc)
	}
	v0 := pktLine("want "+solo+"\n") + "0000" + pktLine("done\n")
	if rpc := smartHTTPAt(t, node, repo, token, http.MethodPost, "git-upload-pack", v0); !strings.Contains(rpc, "not our ref") || strings.Contains(rpc, "PACK") {
		t.Fatalf("upload-pack RPC answered a want of %s:\n%q", solo, rpc)
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
