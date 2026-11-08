package githttp

import (
	"strings"
	"testing"
)

// Any token with git:write can push refs/replace/<id>. The pre-receive
// policy checks read each commit as itself.

// A replacement that makes a root commit read as a child of main's tip
// leaves a push of that root to main a non-fast-forward.
func TestRefPolicy_NoForcePushIgnoresReplacements(t *testing.T) {
	repo := registerRepo(t, "replace-nff")
	work := initWorkRepo(t)
	nf := mintTestToken(t, repo, "git:read,git:write", `[["refs/heads/main",["no-force-push"]],["*",[]]]`)
	mustGit(t, work, "push", remoteURL(nodeA, repo, nf), "HEAD:refs/heads/main")
	tip := revParse(t, work, "HEAD")
	tree := revParse(t, work, "HEAD^{tree}")
	root := strings.TrimSpace(mustGit(t, work, "commit-tree", tree, "-m", "root"))
	child := strings.TrimSpace(mustGit(t, work, "commit-tree", tree, "-p", tip, "-m", "child"))
	mustGit(t, work, "push", remoteURL(nodeA, repo, nf), root+":refs/heads/carrier", child+":refs/replace/"+root)
	out, err := tryGit(work, "push", remoteURL(nodeA, repo, nf), "+"+root+":refs/heads/main")
	if err == nil || !policyRejected.MatchString(out) {
		t.Fatalf("forced push of root to main was not rejected (err %v):\n%s", err, out)
	}
	if got := pgRefs(t, repo)["refs/heads/main"]; got != tip {
		t.Fatalf("main = %s, want %s", got, tip)
	}
}

// A replacement that makes an unsigned commit read as a signed one leaves
// the unsigned commit unsigned.
func TestVerifySig_IgnoresReplacements(t *testing.T) {
	repo := registerRepo(t, "replace-vs")
	work := initWorkRepo(t)
	vs := mintTestToken(t, repo, "git:read,git:write", verifySigMainRefs)
	privPath, pubLine := genSSHKey(t)
	key := registerSigningKey(t, pubLine)
	t.Cleanup(func() { removeSigningKey(t, key.ID) })
	plain := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, plain), "HEAD:refs/heads/main")
	base := revParse(t, work, "HEAD")
	signed := commitFile(t, work, "s.txt", "signed\n", "signed", nil, sshSigningArgs(privPath)...)
	mustGit(t, work, "reset", "-q", "--hard", base)
	unsigned := commitFile(t, work, "u.txt", "unsigned\n", "unsigned", nil)
	mustGit(t, work, "push", remoteURL(nodeA, repo, vs), signed+":refs/heads/carrier", signed+":refs/replace/"+unsigned)
	out, err := tryGit(work, "push", remoteURL(nodeA, repo, vs), unsigned+":refs/heads/main")
	if err == nil || !strings.Contains(out, "verify-sig denied refs/heads/main at "+unsigned) {
		t.Fatalf("push of the unsigned commit was not denied (err %v):\n%s", err, out)
	}
	if got := pgRefs(t, repo)["refs/heads/main"]; got != base {
		t.Fatalf("main = %s, want %s", got, base)
	}
}
