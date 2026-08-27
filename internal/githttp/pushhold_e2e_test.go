package githttp

// End-to-end coverage for issue #170: a concurrent request's Hydrate must
// not full-sync a just-CAS-committed ref into the shared cache repo before
// the committing push's own receive-pack performs its local ref update —
// git would then reject the push ("cannot lock ref ... reference already
// exists") even though the authority accepted it.
//
// The race window (post-CAS, pre-local-update) is opened deterministically
// with the EnvUpdateBarrier hook seam.

import (
	"crypto/sha1"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func waitForFile(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}

// TestPush_SurvivesConcurrentHydrateOfCommittedRef: a branch-creation push
// pauses after its update hook committed the authority CAS; a concurrent
// request on the same node then hydrates (full sync — the CAS bumped the
// repo generation); the released push must still succeed.
func TestPush_SurvivesConcurrentHydrateOfCommittedRef(t *testing.T) {
	repo := registerRepo(t, "t-hydrate-race")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")
	head := revParse(t, work, "HEAD")

	barrier := filepath.Join(t.TempDir(), "update-barrier")
	node := startNodeT(t, EnvUpdateBarrier+"="+barrier)

	type pushResult struct {
		out string
		err error
	}
	done := make(chan pushResult, 1)
	go func() {
		out, err := tryGit(work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/race")
		done <- pushResult{out, err}
	}()

	// The hook signals: authority CAS committed, local ref update pending.
	waitForFile(t, barrier+".ready", 30*time.Second)

	// Concurrent request on the same node. The CAS bumped the generation,
	// so this takes the full-sync path over the same cache repo the paused
	// receive-pack is about to update.
	mustGit(t, "", "ls-remote", remoteURL(node, repo, token))

	// Release the hook; receive-pack now performs its local ref update.
	if err := os.WriteFile(barrier+".release", nil, 0o644); err != nil {
		t.Fatalf("release barrier: %v", err)
	}

	res := <-done
	t.Logf("push racing a concurrent hydrate:\n%s", res.out)
	if res.err != nil {
		t.Fatalf("push spuriously rejected while racing a concurrent hydrate: %v\n%s", res.err, res.out)
	}
	if got := pgRefs(t, repo)["refs/heads/race"]; got != head {
		t.Fatalf("authority refs/heads/race = %q, want %q", got, head)
	}
	// The cache repo converges: once the push is done, an advertisement on
	// the same node serves the new branch.
	out := mustGit(t, "", "ls-remote", remoteURL(node, repo, token), "refs/heads/race")
	if !strings.Contains(out, head) {
		t.Fatalf("post-push advertisement lacks refs/heads/race@%s:\n%s", head, out)
	}
}

// TestPush_DFCreateWaitsForHeldNeighborDeletion (Codex P1 on #173): push A
// deletes refs/heads/a and pauses post-CAS (hold live, local deletion
// pending — hydration deliberately skips held refs, so the stale local `a`
// survives every sync). Push B then creates refs/heads/a/b: its update hook
// must wait for A's hold to clear before committing, or B's authority CAS
// would succeed while its local update fails against the surviving loose
// ref — the same authority-accepted-but-client-rejected wedge as the plain
// race.
func TestPush_DFCreateWaitsForHeldNeighborDeletion(t *testing.T) {
	repo := registerRepo(t, "t-hydrate-race-df")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main", "HEAD:refs/heads/a")
	head := revParse(t, work, "HEAD")

	barrier := filepath.Join(t.TempDir(), "update-barrier")
	node := startNodeT(t, EnvUpdateBarrier+"="+barrier)

	type pushResult struct {
		out string
		err error
	}
	// Push A: delete refs/heads/a; its update hook (the node's first)
	// consumes the one-shot barrier and pauses post-CAS.
	doneA := make(chan pushResult, 1)
	go func() {
		out, err := tryGit(work, "push", remoteURL(node, repo, token), ":refs/heads/a")
		doneA <- pushResult{out, err}
	}()
	waitForFile(t, barrier+".ready", 30*time.Second)

	// Push B: create the D/F child refs/heads/a/b while A's hold is live.
	doneB := make(chan pushResult, 1)
	go func() {
		out, err := tryGit(work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/a/b")
		doneB <- pushResult{out, err}
	}()
	// Give B time to reach its update hook's conflicting-hold wait, then
	// release A. (Timing slop only makes B trivially conflict-free; the
	// assertions below hold either way.)
	time.Sleep(1 * time.Second)
	if err := os.WriteFile(barrier+".release", nil, 0o644); err != nil {
		t.Fatalf("release barrier: %v", err)
	}

	resA := <-doneA
	t.Logf("push A (delete a):\n%s", resA.out)
	if resA.err != nil {
		t.Fatalf("deletion push failed: %v\n%s", resA.err, resA.out)
	}
	resB := <-doneB
	t.Logf("push B (create a/b):\n%s", resB.out)
	if resB.err != nil {
		t.Fatalf("D/F creation push spuriously rejected: %v\n%s", resB.err, resB.out)
	}
	refs := pgRefs(t, repo)
	if _, ok := refs["refs/heads/a"]; ok {
		t.Fatalf("refs/heads/a survived its deletion: %v", refs)
	}
	if refs["refs/heads/a/b"] != head {
		t.Fatalf("authority refs/heads/a/b = %q, want %q", refs["refs/heads/a/b"], head)
	}
}

// TestPush_DFChildOfExistingRefCleanlyDeclined (Codex round-4 P1 family):
// the authority CAS is name-keyed, so without the update hook's
// authority-side D/F probe a push creating refs/heads/a/b while
// refs/heads/a exists commits a/b into the authority even though no git
// ref store can hold both — permanently wedging every future hydration of
// the repo. The hook must decline the update per-ref BEFORE the CAS,
// keeping the authority representable and the repo serving.
func TestPush_DFChildOfExistingRefCleanlyDeclined(t *testing.T) {
	repo := registerRepo(t, "t-df-existing")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main", "HEAD:refs/heads/a")

	out, err := tryGit(work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/a/b")
	if err == nil {
		t.Fatalf("D/F-conflicting creation must be rejected:\n%s", out)
	}
	t.Logf("D/F-conflicting push (expected clean decline):\n%s", out)
	if !strings.Contains(out, "directory/file") {
		t.Fatalf("rejection is not the hook's clean D/F decline:\n%s", out)
	}
	refs := pgRefs(t, repo)
	if _, ok := refs["refs/heads/a/b"]; ok {
		t.Fatalf("authority accepted an unrepresentable D/F pair: %v", refs)
	}
	// The repo still serves: no committed-but-unmaterializable ref is
	// wedging hydration.
	mustGit(t, "", "ls-remote", remoteURL(nodeA, repo, token))
}

// TestPush_DeniedParentAllowedChildLands (Codex round-7 P2): a non-atomic
// push carrying a policy-DENIED creation of refs/heads/den and an allowed
// creation of refs/heads/den/ok must keep git's per-ref semantics — the
// denied parent never survives, so the allowed child lands; whole-push D/F
// denial is only correct for atomic pushes, where an already-committed
// authority CAS cannot be taken back when git aborts the local
// transaction.
func TestPush_DeniedParentAllowedChildLands(t *testing.T) {
	repo := registerRepo(t, "t-df-denied-parent")
	work := initWorkRepo(t)
	full := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, full), "HEAD:refs/heads/main")
	head := revParse(t, work, "HEAD")

	// A fresh commit so the allowed child genuinely needs its pack — a
	// stored-objects-only mixed push is refused whole by the deliberate
	// publication gate (issue #21), which is not the semantics under test.
	writeFile(t, work, "b.txt", "two\n")
	mustGit(t, work, "add", ".")
	mustGit(t, work, "commit", "-m", "two")
	head2 := revParse(t, work, "HEAD")

	restricted := mintTestToken(t, repo, "git:read,git:write", `[["refs/heads/den",["no-push"]],["*",[]]]`)
	out, err := tryGit(work, "push", remoteURL(nodeA, repo, restricted), head+":refs/heads/den", "HEAD:refs/heads/den/ok")
	if err == nil {
		t.Fatalf("push must report the denied parent:\n%s", out)
	}
	t.Logf("mixed denied-parent push (child must land):\n%s", out)
	refs := pgRefs(t, repo)
	if _, ok := refs["refs/heads/den"]; ok {
		t.Fatalf("denied parent committed: %v", refs)
	}
	if refs["refs/heads/den/ok"] != head2 {
		t.Fatalf("allowed child rejected alongside its denied parent: %v", refs)
	}
}

// TestAtomicPush_DeniedWhenCreationDoomedByAuthorityDF (Codex round-8 P1):
// an atomic push mixing a healthy update with a creation whose D/F
// relative exists in the authority must be denied WHOLE in pre-receive —
// otherwise the healthy command's authority CAS commits before the doomed
// creation's hook declines, git aborts the local atomic transaction, and
// the client is told the whole push failed while the authority moved.
func TestAtomicPush_DeniedWhenCreationDoomedByAuthorityDF(t *testing.T) {
	repo := registerRepo(t, "t-df-atomic-doomed")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main", "HEAD:refs/heads/good", "HEAD:refs/heads/z")
	head := revParse(t, work, "HEAD")

	writeFile(t, work, "b.txt", "two\n")
	mustGit(t, work, "add", ".")
	mustGit(t, work, "commit", "-m", "two")

	out, err := tryGit(work, "push", "--atomic", remoteURL(nodeA, repo, token),
		"HEAD:refs/heads/good", "HEAD:refs/heads/z/child")
	if err == nil {
		t.Fatalf("atomic push with an authority-doomed creation must be denied:\n%s", out)
	}
	t.Logf("atomic doomed push (expected whole-push denial):\n%s", out)
	refs := pgRefs(t, repo)
	if refs["refs/heads/good"] != head {
		t.Fatalf("authority moved refs/heads/good under a whole-push failure report: %v", refs)
	}
	if _, ok := refs["refs/heads/z/child"]; ok {
		t.Fatalf("authority accepted the doomed creation: %v", refs)
	}
}

// TestPush_DeniedDeletionSurvivorBlocksDFChild (Codex round-4 P1, exact
// scenario): one non-atomic push carries a policy-DENIED deletion of
// refs/heads/a plus an allowed creation of refs/heads/a/b. The denied
// deletion means `a` survives, so the update hook's authority probe must
// decline a/b at commit time — pre-receive verdicts are advisory and must
// not be trusted to predict survivors.
func TestPush_DeniedDeletionSurvivorBlocksDFChild(t *testing.T) {
	repo := registerRepo(t, "t-df-denied-survivor")
	work := initWorkRepo(t)
	full := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, full), "HEAD:refs/heads/main", "HEAD:refs/heads/a")

	// A fresh commit so the allowed a/b creation genuinely needs its pack
	// (otherwise the publication gate already refuses the mixed push
	// before any hook runs — a valid but different denial).
	writeFile(t, work, "b.txt", "two\n")
	mustGit(t, work, "add", ".")
	mustGit(t, work, "commit", "-m", "two")

	// no-push on exactly refs/heads/a denies its deletion; everything
	// else is allowed by the catch-all.
	restricted := mintTestToken(t, repo, "git:read,git:write", `[["refs/heads/a",["no-push"]],["*",[]]]`)
	out, err := tryGit(work, "push", remoteURL(nodeA, repo, restricted), ":refs/heads/a", "HEAD:refs/heads/a/b")
	if err == nil {
		t.Fatalf("push with a denied-survivor D/F conflict must be rejected:\n%s", out)
	}
	t.Logf("denied-survivor push (expected per-ref declines):\n%s", out)
	refs := pgRefs(t, repo)
	if _, ok := refs["refs/heads/a"]; !ok {
		t.Fatalf("policy-denied deletion still removed refs/heads/a: %v", refs)
	}
	if _, ok := refs["refs/heads/a/b"]; ok {
		t.Fatalf("authority accepted a/b although its denied-survivor parent remains: %v", refs)
	}
	mustGit(t, "", "ls-remote", remoteURL(nodeA, repo, full))
}

// TestPush_SiblingDFCreationsExactlyOneLands (Codex round-9 P1): a
// non-atomic push creating BOTH refs/heads/n/a and refs/heads/n/a/b passes
// the pre-receive authority probe (neither name exists yet); if both were
// bulk-reserved, both hooks would skip the commit-time probe and both
// authority CASes would commit before git's local per-ref update rejects
// one — an unrepresentable authority. Exactly one may land: whichever
// command git processes first commits, and the sibling's own hook declines
// it against the authority at commit time.
func TestPush_SiblingDFCreationsExactlyOneLands(t *testing.T) {
	repo := registerRepo(t, "t-df-sibling-creations")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")
	head := revParse(t, work, "HEAD")

	out, err := tryGit(work, "push", remoteURL(nodeA, repo, token),
		"HEAD:refs/heads/n/a", "HEAD:refs/heads/n/a/b")
	if err == nil {
		t.Fatalf("a push creating a D/F sibling pair must reject one side:\n%s", out)
	}
	t.Logf("sibling D/F creation push (exactly one must land):\n%s", out)
	refs := pgRefs(t, repo)
	_, hasA := refs["refs/heads/n/a"]
	_, hasB := refs["refs/heads/n/a/b"]
	if hasA && hasB {
		t.Fatalf("authority accepted an unrepresentable D/F pair: %v", refs)
	}
	if !hasA && !hasB {
		t.Fatalf("no sibling landed at all: %v\n%s", refs, out)
	}
	winner := "refs/heads/n/a"
	if hasB {
		winner = "refs/heads/n/a/b"
	}
	if refs[winner] != head {
		t.Fatalf("winner %s = %q, want %q", winner, refs[winner], head)
	}
	// The repo still serves — the authority stayed representable.
	mustGit(t, "", "ls-remote", remoteURL(nodeA, repo, token))
}

// emptyPack is a syntactically complete packfile with zero objects — what
// a client effectively sends when every pushed ref points at objects the
// server already has.
func emptyPack() []byte {
	hdr := []byte{'P', 'A', 'C', 'K', 0, 0, 0, 2, 0, 0, 0, 0}
	sum := sha1.Sum(hdr)
	return append(hdr, sum[:]...)
}

// TestAtomicPush_GitInvalidNameDeniedWhole (Codex round-9 P1): git's own
// funny-refname check runs per-command INSIDE the atomic transaction —
// AFTER earlier commands' update hooks, and therefore their authority
// CASes, already ran. The pre-receive gate must be at least as strict as
// git: a git-invalid name like "refs/heads/bad." must deny the WHOLE push
// before any hook runs, or the client is told the push failed while the
// authority moved. Crafted over raw pkt-lines — a git client refuses to
// send such a destination itself.
func TestAtomicPush_GitInvalidNameDeniedWhole(t *testing.T) {
	repo := registerRepo(t, "t-atomic-funny-format")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")
	head := revParse(t, work, "HEAD")

	zero := strings.Repeat("0", 40)
	body := pktLine(zero+" "+head+" refs/heads/ok\x00report-status side-band-64k atomic") +
		pktLine(zero+" "+head+" refs/heads/bad.") +
		"0000" + string(emptyPack())
	status, resp := postReceivePack(t, nodeA, repo, token, strings.NewReader(body), false)
	if status != http.StatusOK {
		t.Fatalf("status = %d\n%s", status, resp)
	}
	t.Logf("atomic push with git-invalid name (expected whole-push denial):\n%s", resp)
	refs := pgRefs(t, repo)
	if _, ok := refs["refs/heads/ok"]; ok {
		t.Fatalf("authority moved refs/heads/ok under a whole-push failure report: %v", refs)
	}
	if _, ok := refs["refs/heads/bad."]; ok {
		t.Fatalf("authority accepted a git-invalid ref name: %v", refs)
	}
	if !strings.Contains(resp, "pre-receive hook declined") {
		t.Fatalf("denial did not come from the pre-receive whole-push gate:\n%s", resp)
	}
}

// TestPush_UnreservedSidecarBounded (Codex round-9 P2): every command
// pre-receive cannot bulk-reserve becomes one marker inode in the sidecar,
// and unreserved membership is attacker-chosen — a tiny crafted push can
// carry hundreds of thousands of undeliverable commands under the byte
// cap. Above maxPushUnreserved the push must be denied whole, before any
// marker is created. Delete-only (no pack needed), raw pkt-lines: two-level
// names like refs/x000001 are git-valid but never updatable here.
func TestPush_UnreservedSidecarBounded(t *testing.T) {
	repo := registerRepo(t, "t-sidecar-bounded")
	token := mintTestToken(t, repo, "git:read,git:write", "")

	fake := strings.Repeat("1", 40)
	zero := strings.Repeat("0", 40)
	var body strings.Builder
	for i := 0; i <= maxPushUnreserved; i++ {
		cmd := fmt.Sprintf("%s %s refs/x%06d", fake, zero, i)
		if i == 0 {
			cmd += "\x00report-status side-band-64k"
		}
		body.WriteString(pktLine(cmd))
	}
	body.WriteString("0000")
	status, resp := postReceivePack(t, nodeA, repo, token, strings.NewReader(body.String()), false)
	if status != http.StatusOK {
		t.Fatalf("status = %d\n%s", status, resp)
	}
	if !strings.Contains(resp, "cannot be bulk-reserved") {
		t.Fatalf("oversized unreserved sidecar was not denied whole:\n%s", resp)
	}
}
