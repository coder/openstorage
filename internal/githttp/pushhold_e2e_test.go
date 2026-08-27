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

// TestPush_WaitedCreationReprobesAuthority (Codex round-10 P1): pushes X
// (create refs/heads/q) and Y (create refs/heads/q/c) race on one node.
// Both pre-receive authority probes run before either CAS and see neither
// name; Y's reservation then waits out X's hold, by which time X has
// COMMITTED q. Without the post-wait re-probe Y becomes bulk-reserved, its
// hook skips the commit-time check, and its CAS wedges the authority with
// q + q/c. Fixed, Y is re-probed after the wait, diverted, and cleanly
// declined per-ref. (If scheduling ever runs Y after X's CAS instead, the
// FIRST probe dooms it — same assertions, so the test cannot flake red.)
func TestPush_WaitedCreationReprobesAuthority(t *testing.T) {
	repo := registerRepo(t, "t-waited-reprobe")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")
	head := revParse(t, work, "HEAD")

	// Every update hook on this node sleeps 3s BEFORE its CAS: X's hold
	// stays live and uncommitted long enough for Y's pre-receive probe
	// and reservation wait to begin.
	node := startNodeT(t, EnvHookDelay+"=3000")

	type pushResult struct {
		out string
		err error
	}
	doneX := make(chan pushResult, 1)
	go func() {
		out, err := tryGit(work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/q")
		doneX <- pushResult{out, err}
	}()
	time.Sleep(500 * time.Millisecond)
	doneY := make(chan pushResult, 1)
	go func() {
		out, err := tryGit(work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/q/c")
		doneY <- pushResult{out, err}
	}()

	resX := <-doneX
	t.Logf("push X (create q):\n%s", resX.out)
	if resX.err != nil {
		t.Fatalf("push X failed: %v\n%s", resX.err, resX.out)
	}
	resY := <-doneY
	t.Logf("push Y (create q/c, expected clean decline):\n%s", resY.out)
	if resY.err == nil {
		t.Fatalf("push Y must be declined against committed q:\n%s", resY.out)
	}
	if !strings.Contains(resY.out, "directory/file") {
		t.Fatalf("Y's rejection is not the clean D/F decline:\n%s", resY.out)
	}
	refs := pgRefs(t, repo)
	if refs["refs/heads/q"] != head {
		t.Fatalf("authority refs/heads/q = %q, want %q", refs["refs/heads/q"], head)
	}
	if _, ok := refs["refs/heads/q/c"]; ok {
		t.Fatalf("authority accepted an unrepresentable D/F pair: %v", refs)
	}
	mustGit(t, "", "ls-remote", remoteURL(nodeA, repo, token))
}

// TestAtomicPush_WaitedCreationDeniedWhole (Codex round-10 P1, atomic
// arm): when the waiter is ATOMIC, a per-ref hook decline would fire only
// AFTER sibling hooks' CASes committed — the post-wait re-probe must deny
// the WHOLE push in pre-receive instead, before any hook runs.
func TestAtomicPush_WaitedCreationDeniedWhole(t *testing.T) {
	repo := registerRepo(t, "t-waited-atomic")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")
	head := revParse(t, work, "HEAD")

	node := startNodeT(t, EnvHookDelay+"=3000")

	type pushResult struct {
		out string
		err error
	}
	doneX := make(chan pushResult, 1)
	go func() {
		out, err := tryGit(work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/q")
		doneX <- pushResult{out, err}
	}()
	time.Sleep(500 * time.Millisecond)
	doneY := make(chan pushResult, 1)
	go func() {
		out, err := tryGit(work, "push", "--atomic", remoteURL(node, repo, token),
			"HEAD:refs/heads/good", "HEAD:refs/heads/q/c")
		doneY <- pushResult{out, err}
	}()

	resX := <-doneX
	t.Logf("push X (create q):\n%s", resX.out)
	if resX.err != nil {
		t.Fatalf("push X failed: %v\n%s", resX.err, resX.out)
	}
	resY := <-doneY
	t.Logf("atomic push Y (expected whole-push denial):\n%s", resY.out)
	if resY.err == nil {
		t.Fatalf("atomic push with a doomed waited creation must be denied whole:\n%s", resY.out)
	}
	refs := pgRefs(t, repo)
	if refs["refs/heads/q"] != head {
		t.Fatalf("authority refs/heads/q = %q, want %q", refs["refs/heads/q"], head)
	}
	if _, ok := refs["refs/heads/good"]; ok {
		t.Fatalf("authority moved refs/heads/good under a whole-push failure report: %v", refs)
	}
	if _, ok := refs["refs/heads/q/c"]; ok {
		t.Fatalf("authority accepted the doomed waited creation: %v", refs)
	}
}

// TestPush_ParentOfExistingChildCleanlyDeclined (Codex round-11 P1
// end-state): creating refs/heads/d while refs/heads/d/child exists must
// be declined per-ref with the authority untouched. This exercises the
// full post-RESERVATION probe path: the creation is bulk-reserved first,
// the probe then dooms it against the existing child, pre-receive releases
// its reservation (no namespace squat beyond the probe round-trip) and
// diverts it to the sidecar, and the hook's commit-time probe declines.
func TestPush_ParentOfExistingChildCleanlyDeclined(t *testing.T) {
	repo := registerRepo(t, "t-df-existing-child")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main", "HEAD:refs/heads/d/child")
	head := revParse(t, work, "HEAD")

	out, err := tryGit(work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/d")
	if err == nil {
		t.Fatalf("creating the parent of an existing child must be declined:\n%s", out)
	}
	t.Logf("parent-of-existing-child push (expected clean decline):\n%s", out)
	if !strings.Contains(out, "directory/file") {
		t.Fatalf("rejection is not the clean D/F decline:\n%s", out)
	}
	refs := pgRefs(t, repo)
	if _, ok := refs["refs/heads/d"]; ok {
		t.Fatalf("authority accepted an unrepresentable D/F pair: %v", refs)
	}
	if refs["refs/heads/d/child"] != head {
		t.Fatalf("existing child disturbed: %v", refs)
	}
	mustGit(t, "", "ls-remote", remoteURL(nodeA, repo, token))
}

// TestPush_LargeSignedFanoutNotCapped (Codex round-13 P2): a non-atomic
// push of maxPushUnreserved+1 creations under a broad verify-sig policy,
// all validly signed, must NOT die at the unreserved-sidecar cap:
// pre-receive evaluates the push's REAL verdicts once (verifystate keeps
// the walk single-charged) and bulk-reserves the verified refs, so nothing
// signature-gated lands in the sidecar. Before the fix every gated command
// was flatly diverted and 1,025 legitimate signed creations
// deterministically died at the cap before any hook could verify them.
func TestPush_LargeSignedFanoutNotCapped(t *testing.T) {
	repo := registerRepo(t, "t-signed-fanout")
	work := initWorkRepo(t)
	priv, pub := genSSHKey(t)
	key := registerSigningKey(t, pub)
	defer removeSigningKey(t, key.ID)

	// The whole history must be signed: creations introduce every commit
	// reachable from the tip. Amend the unsigned seed commit signed, then
	// add one more signed commit on top.
	mustGit(t, work, append(sshSigningArgs(priv), "commit", "--amend", "--no-edit")...)
	plain := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, plain), "HEAD:refs/heads/main")
	writeFile(t, work, "s.txt", "signed\n")
	mustGit(t, work, "add", "s.txt")
	mustGit(t, work, append(sshSigningArgs(priv), "commit", "-m", "signed")...)
	head := revParse(t, work, "HEAD")

	vs := mintTestToken(t, repo, "git:read,git:write", `[["*",["verify-sig"]]]`)
	args := []string{"push", remoteURL(nodeA, repo, vs)}
	for i := 0; i <= maxPushUnreserved; i++ {
		args = append(args, fmt.Sprintf("HEAD:refs/heads/f/%04d", i))
	}
	out, err := tryGit(work, args...)
	if err != nil {
		t.Fatalf("signed %d-ref fanout push must not be capped: %v\n%.2000s", maxPushUnreserved+1, err, out)
	}
	refs := pgRefs(t, repo)
	first, last := "refs/heads/f/0000", fmt.Sprintf("refs/heads/f/%04d", maxPushUnreserved)
	if refs[first] != head || refs[last] != head {
		t.Fatalf("fanout refs missing: %s=%q %s=%q, want %q", first, refs[first], last, refs[last], head)
	}
}

// TestPush_SignedFanoutSurvivesSmallBudget (Codex round-16 P2): a no-pack
// push of 60 signed refs at one already-stored tip under
// CODESTORAGE_VERIFY_SIG_BUDGET=1 must succeed. Pre-receive's reservation
// evaluation charges the per-evaluation verify fee once per command
// (~61 of the 100 allowed units); before the fix each update hook charged
// another unit — reading the SAME persisted range verdict — so the
// accumulated fees pushed a fully compliant push past the walk allowance.
// The hooks now consume pre-receive's verified-verdict markers without
// re-charging.
func TestPush_SignedFanoutSurvivesSmallBudget(t *testing.T) {
	repo := registerRepo(t, "t-signed-fanout-budget")
	work := initWorkRepo(t)
	priv, pub := genSSHKey(t)
	key := registerSigningKey(t, pub)
	defer removeSigningKey(t, key.ID)

	mustGit(t, work, append(sshSigningArgs(priv), "commit", "--amend", "--no-edit")...)
	plain := mintTestToken(t, repo, "git:read,git:write", "")
	node := startNodeT(t, "CODESTORAGE_VERIFY_SIG_BUDGET=1")
	// Store the whole (1-commit) history first, so the fanout push below
	// carries no pack and its only budget pressure is the per-evaluation
	// fees.
	mustGit(t, work, "push", remoteURL(node, repo, plain), "HEAD:refs/heads/main")
	head := revParse(t, work, "HEAD")

	vs := mintTestToken(t, repo, "git:read,git:write", `[["*",["verify-sig"]]]`)
	args := []string{"push", remoteURL(node, repo, vs)}
	for i := 0; i < 60; i++ {
		args = append(args, fmt.Sprintf("HEAD:refs/heads/b/%04d", i))
	}
	out, err := tryGit(work, args...)
	if err != nil {
		t.Fatalf("signed 60-ref same-tip fanout must survive budget 1: %v\n%.2000s", err, out)
	}
	refs := pgRefs(t, repo)
	if refs["refs/heads/b/0000"] != head || refs["refs/heads/b/0059"] != head {
		t.Fatalf("fanout refs missing: %v", refs["refs/heads/b/0000"])
	}
}

// TestPush_DoomedCreationReleasesHoldOnDenial (Codex round-16 P2): a
// sidecar-diverted creation that the update hook's commit-time D/F probe
// denies must release its self-reservation immediately — before the fix
// the doomed name stayed held until the whole receive-pack exited, so a
// push bundling a doomed parent with slow work blocked every concurrent
// legitimate update under that namespace for the push's full duration
// (10s reservation timeout, then denial).
func TestPush_DoomedCreationReleasesHoldOnDenial(t *testing.T) {
	repo := registerRepo(t, "t-doomed-hold-release")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main", "HEAD:refs/heads/a/x")

	// Concurrent pusher C's clone, advanced one commit for its a/x update.
	workC := filepath.Join(t.TempDir(), "clone-c")
	mustGit(t, "", "clone", work, workC)
	mustGit(t, workC, "commit", "--allow-empty", "-m", "C: advance a/x")
	headC := revParse(t, workC, "HEAD")

	barrier := filepath.Join(t.TempDir(), "update-barrier")
	node := startNodeT(t, EnvUpdateBarrier+"="+barrier)

	type pushResult struct {
		out string
		err error
	}
	// Push B: doomed creation a (its D/F child a/x exists, so pre-receive
	// diverts it to the sidecar and its hook is denied pre-CAS) plus
	// creation zzz, whose hook — the push's first COMMITTED one, running
	// after a's denial — consumes the one-shot barrier and pauses
	// post-CAS, keeping B's receive-pack alive with its holds.
	doneB := make(chan pushResult, 1)
	go func() {
		out, err := tryGit(work, "push", remoteURL(node, repo, token),
			"HEAD:refs/heads/a", "HEAD:refs/heads/zzz")
		doneB <- pushResult{out, err}
	}()
	waitForFile(t, barrier+".ready", 30*time.Second)

	// While B is paused: a legitimate a/x update must reserve immediately
	// — B's denied creation a released its hold, so nothing covers a/x.
	// Before the fix this push waited out the full 10s reservation
	// deadline on the leaked hold and failed closed.
	outC, errC := tryGit(workC, "push", remoteURL(node, repo, token), "HEAD:refs/heads/a/x")
	if errC != nil {
		t.Fatalf("a/x update blocked by a denied creation's leaked hold: %v\n%s", errC, outC)
	}

	if err := os.WriteFile(barrier+".release", nil, 0o644); err != nil {
		t.Fatalf("release barrier: %v", err)
	}
	resB := <-doneB
	t.Logf("push B (doomed a + zzz):\n%s", resB.out)
	if resB.err == nil {
		t.Fatal("push B fully succeeded; the doomed creation was supposed to be denied per-ref")
	}
	if !strings.Contains(resB.out, "directory/file") {
		t.Fatalf("creation a must be denied by the D/F probe:\n%s", resB.out)
	}
	refs := pgRefs(t, repo)
	if _, ok := refs["refs/heads/a"]; ok {
		t.Fatalf("doomed creation committed: %v", refs)
	}
	if refs["refs/heads/a/x"] != headC {
		t.Fatalf("authority a/x = %q, want C's %q", refs["refs/heads/a/x"], headC)
	}
	if _, ok := refs["refs/heads/zzz"]; !ok {
		t.Fatalf("B's valid creation zzz missing: %v", refs)
	}
}

// TestAtomicPush_StaleBaseDeniedBeforeAnyCAS (Codex round-14 P1 on #173):
// while push A pauses between its committed authority CAS and its local
// ref update, hydration deliberately leaves the held ref at its old cache
// value — so a concurrent atomic push B is ADVERTISED the stale base and
// declares it. Under --atomic, letting B reach its update hooks
// half-commits: an unrelated command's CAS lands first, then the stale
// ref's CAS loses, git aborts the whole local transaction, and the client
// is told the atomic push failed while the unrelated update stands in the
// authority. B must instead (1) wait out A's same-name hold at reservation
// (covers semantics — the hold freezes the name's authority value on this
// node) and then (2) be denied WHOLE by the pre-receive atomic base check,
// before any update hook can run.
func TestAtomicPush_StaleBaseDeniedBeforeAnyCAS(t *testing.T) {
	repo := registerRepo(t, "t-atomic-stale-base")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token),
		"HEAD:refs/heads/main", "HEAD:refs/heads/race", "HEAD:refs/heads/early")
	v1 := revParse(t, work, "HEAD")

	// Clone at V1 for the atomic pusher, then advance work to V2 for A.
	workB := filepath.Join(t.TempDir(), "clone-b")
	mustGit(t, "", "clone", work, workB)
	mustGit(t, work, "commit", "--allow-empty", "-m", "A: race to V2")
	v2 := revParse(t, work, "HEAD")
	mustGit(t, workB, "commit", "--allow-empty", "-m", "B: race to V3, never lands")

	barrier := filepath.Join(t.TempDir(), "update-barrier")
	cacheDir := filepath.Join(t.TempDir(), "cache")
	node, err := startNode(cacheDir, EnvUpdateBarrier+"="+barrier)
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(node.stop)

	type pushResult struct {
		out string
		err error
	}
	// Push A: race V1 -> V2; its update hook (the node's first) consumes
	// the one-shot barrier and pauses post-CAS with its hold live.
	doneA := make(chan pushResult, 1)
	go func() {
		out, err := tryGit(work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/race")
		doneA <- pushResult{out, err}
	}()
	waitForFile(t, barrier+".ready", 30*time.Second)

	// Push B (atomic): its advertisement reads the cache, where hydration
	// skipped the held race ref — B declares the STALE base for race —
	// plus an update of the existing sibling refs/heads/early, whose
	// update hook runs FIRST ("early" precedes "race" in receive-pack's
	// command order) and would commit its authority CAS before the stale
	// ref's hook can lose: the finding's half-commit shape.
	doneB := make(chan pushResult, 1)
	go func() {
		out, err := tryGit(workB, "push", "--atomic", remoteURL(node, repo, token),
			"HEAD:refs/heads/early", "HEAD:refs/heads/race")
		doneB <- pushResult{out, err}
	}()

	// Release A only after B's receive-pack created its object quarantine
	// (objects/tmp_objdir-*): git pushes advertise via GET strictly before
	// the POST whose receive-pack quarantines the pack, so by then B's
	// stale V1 advertisement is fixed — taken strictly before A's local
	// update could heal the cache. Any quarantine seen here is B's: git
	// migrates a push's quarantine when its PRE-RECEIVE approves, so A —
	// paused inside an update hook since before B started — no longer has
	// one. Whether B then waits out A's same-name hold or reserves just
	// after A released, the base check reads race=V2 against B's declared
	// V1 either way — no sleep-based timing.
	objDir := filepath.Join(RepoPath(cacheDir, repo), "objects")
	deadline := time.Now().Add(30 * time.Second)
	for {
		quarantines := 0
		entries, _ := os.ReadDir(objDir)
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), "tmp_objdir") {
				quarantines++
			}
		}
		if quarantines >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for push B's receive-pack quarantine")
		}
		time.Sleep(25 * time.Millisecond)
	}
	if err := os.WriteFile(barrier+".release", nil, 0o644); err != nil {
		t.Fatalf("release barrier: %v", err)
	}

	resA := <-doneA
	t.Logf("push A (race to V2):\n%s", resA.out)
	if resA.err != nil {
		t.Fatalf("push A failed: %v\n%s", resA.err, resA.out)
	}
	resB := <-doneB
	t.Logf("push B (atomic, stale base):\n%s", resB.out)
	if resB.err == nil {
		t.Fatalf("atomic push with a stale base accepted:\n%s", resB.out)
	}
	refs := pgRefs(t, repo)
	// The load-bearing assertion: nothing of B may stand in the authority
	// after git told B's client the whole atomic push failed.
	if refs["refs/heads/early"] != v1 {
		t.Fatalf("PARTIAL atomic commit: refs/heads/early=%s stands in the authority while the client saw a whole-push failure (want untouched %s)", refs["refs/heads/early"], v1)
	}
	if refs["refs/heads/race"] != v2 {
		t.Fatalf("authority refs/heads/race = %q, want A's %q", refs["refs/heads/race"], v2)
	}
	// "fetch first" is the pre-receive base check's marker: the denial
	// must come before any update hook, not from a hook's own
	// CAS-conflict message — by then sibling hooks may already have
	// committed.
	if !strings.Contains(resB.out, "fetch first") {
		t.Fatalf("denial must come from the pre-receive atomic base check:\n%s", resB.out)
	}
}
