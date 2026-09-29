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
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref"
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

// waitForPushHold polls the node's push-hold directory until a live hold
// names ref: the push that owns it has reserved the name and has not yet
// finished (issue #364 — a fixed sleep let a slow first push lose the race
// to the push meant to wait on it). done is that push's result channel; if
// the push finishes first, the hold can no longer be observed, so fail
// with its result instead of waiting out the deadline.
func waitForPushHold[R any](t *testing.T, cacheDir, repo, ref string, done <-chan R) {
	t.Helper()
	dir := filepath.Join(RepoPath(cacheDir, repo), pushHoldDir)
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		if holds, err := readPushHolds(dir, ""); err == nil && holds.contains(ref) {
			return
		}
		select {
		case res := <-done:
			t.Fatalf("push finished before %s was observed held: %+v", ref, res)
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatalf("timed out waiting for a push hold on %s in %s", ref, dir)
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
	doneX := make(chan pushResult, 1)
	go func() {
		out, err := tryGit(work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/q")
		doneX <- pushResult{out, err}
	}()
	// Start Y only once X holds q (issue #364). X reserves q before its
	// hook's 3s pre-CAS delay, so Y meets the live, uncommitted hold; were
	// Y slower than that, X's committed q would doom Y's first probe with
	// the same outcome.
	waitForPushHold(t, nodeCache, repo, "refs/heads/q", doneX)
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
	doneX := make(chan pushResult, 1)
	go func() {
		out, err := tryGit(work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/q")
		doneX <- pushResult{out, err}
	}()
	// Start Y only once X holds q (issue #364). X reserves q before its
	// hook's 3s pre-CAS delay, so Y meets the live, uncommitted hold; were
	// Y slower than that, X's committed q would doom Y's first probe with
	// the same outcome.
	waitForPushHold(t, nodeCache, repo, "refs/heads/q", doneX)
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
// OPENSTORAGE_VERIFY_SIG_BUDGET=1 must succeed. Pre-receive's reservation
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
	node := startNodeT(t, "OPENSTORAGE_VERIFY_SIG_BUDGET=1")
	// Store the whole (1-commit) history first, so the fanout push below
	// carries no pack and its only budget pressure is the per-evaluation
	// fees.
	mustGit(t, work, "push", remoteURL(node, repo, plain), "HEAD:refs/heads/main")
	head := revParse(t, work, "HEAD")

	vs := mintTestToken(t, repo, "git:read,git:write", `[["*",["verify-sig"]]]`)
	args := []string{"push", remoteURL(node, repo, vs)}
	for i := range 60 {
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

// TestPush_PackPathJITFanoutSurvivesSmallBudget (Codex round-32 P2 on
// #173): under a combined ["verify-sig","no-force-push"] rule the
// budget-free classifier defers every update to a JIT reservation, and on
// the PACK path pre-receive's publication gate performs the real
// evaluations — but their successful verdicts were not persisted, so each
// update hook re-evaluated the same command and re-charged the shared
// walk allowance, failing a large compliant SIGNED fast-forward fanout
// after its pack was already published. The gate's passed JIT verdicts
// now persist like needs-verify ones; hooks consume the digest without a
// second fee.
func TestPush_PackPathJITFanoutSurvivesSmallBudget(t *testing.T) {
	repo := registerRepo(t, "t-jit-pack-fanout-budget")
	work := initWorkRepo(t)
	priv, pub := genSSHKey(t)
	key := registerSigningKey(t, pub)
	defer removeSigningKey(t, key.ID)

	mustGit(t, work, append(sshSigningArgs(priv), "commit", "--amend", "--no-edit")...)
	plain := mintTestToken(t, repo, "git:read,git:write", "")
	node := startNodeT(t, "OPENSTORAGE_VERIFY_SIG_BUDGET=1")
	// Seed main plus the 60-ref fanout at v1.
	args := []string{"push", remoteURL(node, repo, plain), "HEAD:refs/heads/main"}
	for i := range 60 {
		args = append(args, fmt.Sprintf("HEAD:refs/heads/b/%04d", i))
	}
	mustGit(t, work, args...)
	// One new SIGNED commit so the fanout update push carries a pack.
	mustGit(t, work, append(sshSigningArgs(priv), "commit", "--allow-empty", "-m", "v2")...)
	head := revParse(t, work, "HEAD")

	combined := mintTestToken(t, repo, "git:read,git:write", `[["*",["verify-sig","no-force-push"]]]`)
	args = []string{"push", remoteURL(node, repo, combined)}
	for i := range 60 {
		args = append(args, fmt.Sprintf("HEAD:refs/heads/b/%04d", i))
	}
	out, err := tryGit(work, args...)
	if err != nil {
		t.Fatalf("signed 60-ref JIT fast-forward pack fanout must survive budget 1: %v\n%.2000s", err, out)
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

// TestPush_LegacyOverDepthRefDeletable (Codex round-18 P2): the authority
// can hold a git-valid ref deeper than today's materialization caps —
// created before the caps existed or imported by sync from a permissive
// upstream. Its deletion adds no name and removes the on-disk path, so it
// must pass the name gate; re-creating it stays denied.
func TestPush_LegacyOverDepthRefDeletable(t *testing.T) {
	repo := registerRepo(t, "t-legacy-deep-delete")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")
	head := revParse(t, work, "HEAD")

	// Seed the over-depth ref directly in the authority, as a pre-cap
	// server (or permissive upstream sync) would have.
	deep := "refs/heads" + strings.Repeat("/d", maxPushRefDepth)
	if won, err := testStore.CASRef(context.Background(), repo, deep, ref.ZeroSHA, head); err != nil || !won {
		t.Fatalf("seed legacy deep ref: won=%v err=%v", won, err)
	}

	out, err := tryGit(work, "push", remoteURL(nodeA, repo, token), ":"+deep)
	if err != nil {
		t.Fatalf("legacy over-depth ref must stay deletable: %v\n%s", err, out)
	}
	if _, ok := pgRefs(t, repo)[deep]; ok {
		t.Fatalf("deletion accepted but the authority still holds %s", deep)
	}
	// The bounds still deny re-creating it.
	out, err = tryGit(work, "push", remoteURL(nodeA, repo, token), "HEAD:"+deep)
	if err == nil {
		t.Fatalf("over-depth creation accepted:\n%s", out)
	}
	if !strings.Contains(out, "not updatable") {
		t.Fatalf("over-depth creation denial reason:\n%s", out)
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

// TestAtomicPush_SignedFanoutSurvivesSmallBudget (Codex round-19 P1 on
// #173): the atomic gate fully evaluates every command pre-reservation —
// those verdicts must be persisted exactly like the non-atomic
// reservation evaluations, or every update hook re-charges the shared
// walk allowance and a fully compliant atomic signed fanout dies
// mid-transaction: earlier hooks' authority CASes have already committed
// when the allowance runs out, while git aborts the whole local atomic
// transaction — the half-commit shape under --atomic.
func TestAtomicPush_SignedFanoutSurvivesSmallBudget(t *testing.T) {
	repo := registerRepo(t, "t-atomic-signed-fanout-budget")
	work := initWorkRepo(t)
	priv, pub := genSSHKey(t)
	key := registerSigningKey(t, pub)
	defer removeSigningKey(t, key.ID)

	mustGit(t, work, append(sshSigningArgs(priv), "commit", "--amend", "--no-edit")...)
	plain := mintTestToken(t, repo, "git:read,git:write", "")
	node := startNodeT(t, "OPENSTORAGE_VERIFY_SIG_BUDGET=1")
	// Store the whole (1-commit) history first, so the fanout push below
	// carries no pack and its only budget pressure is the per-evaluation
	// fees.
	mustGit(t, work, "push", remoteURL(node, repo, plain), "HEAD:refs/heads/main")
	head := revParse(t, work, "HEAD")

	vs := mintTestToken(t, repo, "git:read,git:write", `[["*",["verify-sig"]]]`)
	args := []string{"push", "--atomic", remoteURL(node, repo, vs)}
	for i := range 60 {
		args = append(args, fmt.Sprintf("HEAD:refs/heads/b/%04d", i))
	}
	out, err := tryGit(work, args...)
	if err != nil {
		t.Fatalf("signed 60-ref atomic same-tip fanout must survive budget 1: %v\n%.2000s", err, out)
	}
	refs := pgRefs(t, repo)
	if refs["refs/heads/b/0000"] != head || refs["refs/heads/b/0059"] != head {
		t.Fatalf("fanout refs missing: %v", refs["refs/heads/b/0000"])
	}
}

// TestPush_NoForcePushDenialReleasesHold (Codex round-19 P2 on #173):
// pre-receive's reservation classifier deliberately reserves a
// no-force-push UPDATE without the ancestry subprocess (classification
// cost is attacker-chosen), so a non-fast-forward update bulk-reserves
// and is only denied by its own update hook's real evaluation — pre-CAS.
// That denial must release the name's reservation immediately: bundled
// with slow allowed work, the doomed rewind otherwise keeps the protected
// name held for the push's whole duration, timing out the concurrent
// legitimate fast-forwards no-force-push exists to protect.
func TestPush_NoForcePushDenialReleasesHold(t *testing.T) {
	repo := registerRepo(t, "t-noff-denial-release")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	v1 := revParse(t, work, "HEAD")
	mustGit(t, work, "commit", "--allow-empty", "-m", "advance protected to v2")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token),
		"HEAD:refs/heads/main", "HEAD:refs/heads/protected")

	// Concurrent pusher C: clone at v2, one commit ahead for its
	// fast-forward of protected.
	workC := filepath.Join(t.TempDir(), "clone-c")
	mustGit(t, "", "clone", work, workC)
	mustGit(t, workC, "commit", "--allow-empty", "-m", "C: fast-forward to v3")
	headC := revParse(t, workC, "HEAD")

	// B's zzz tip is a NEW commit: the push must carry a pack an ALLOWED
	// command needs, or pre-receive's publication gate refuses the whole
	// push once the rewind's shared verdict comes back denied (the
	// round-7 decoy rule) and no update hook — the leak under test —
	// ever runs.
	mustGit(t, work, "commit", "--allow-empty", "-m", "B: new tip for zzz")

	barrier := filepath.Join(t.TempDir(), "update-barrier")
	node := startNodeT(t, EnvUpdateBarrier+"="+barrier)
	noff := mintTestToken(t, repo, "git:read,git:write", `[["refs/heads/protected",["no-force-push"]]]`)

	type pushResult struct {
		out string
		err error
	}
	// Push B: the protected rewind (v2 -> v1) bulk-reserves and is denied
	// pre-CAS by its update hook's ancestry check; creation zzz, whose
	// hook runs after protected's denial, consumes the one-shot barrier
	// and pauses post-CAS, keeping B's receive-pack (and its hold file)
	// alive.
	doneB := make(chan pushResult, 1)
	go func() {
		out, err := tryGit(work, "push", "--force", remoteURL(node, repo, noff),
			v1+":refs/heads/protected", "HEAD:refs/heads/zzz")
		doneB <- pushResult{out, err}
	}()
	waitForFile(t, barrier+".ready", 30*time.Second)

	// While B is paused: a legitimate fast-forward of protected must
	// reserve immediately — B's denied rewind released its hold. Before
	// the fix this push waited out the full 10s reservation deadline on
	// the leaked hold and failed closed.
	outC, errC := tryGit(workC, "push", remoteURL(node, repo, token), "HEAD:refs/heads/protected")
	if errC != nil {
		t.Fatalf("fast-forward blocked by the denied rewind's leaked hold: %v\n%s", errC, outC)
	}

	if err := os.WriteFile(barrier+".release", nil, 0o644); err != nil {
		t.Fatalf("release barrier: %v", err)
	}
	resB := <-doneB
	t.Logf("push B (doomed rewind + zzz):\n%s", resB.out)
	if resB.err == nil {
		t.Fatal("push B fully succeeded; the rewind was supposed to be denied per-ref")
	}
	refs := pgRefs(t, repo)
	if refs["refs/heads/protected"] != headC {
		t.Fatalf("authority protected = %q, want C's fast-forward %q", refs["refs/heads/protected"], headC)
	}
	if _, ok := refs["refs/heads/zzz"]; !ok {
		t.Fatalf("B's valid creation zzz missing: %v", refs)
	}
}

// TestServiceRPC_ReleasesPushHoldBeforeRepackTrigger (Codex round-19 P2
// on #173): the push's ref reservations protect receive-pack's local ref
// transaction — nothing after cmd.Wait needs them. The synchronous
// MaybeRepack trigger that follows may take seconds (a ListPacks against
// shared storage plus enqueue), the same order as pushHoldWaitMax, so a
// hold surviving into the trigger times out concurrent same-name or
// D/F-relative pushes that no longer race anything. The trigger must
// observe the hold ALREADY released. Runs the push in-process (httptest +
// TestMain's hook re-exec dispatch) because MaybeRepack is injectable
// only on the Server struct.
func TestServiceRPC_ReleasesPushHoldBeforeRepackTrigger(t *testing.T) {
	// The in-process server plays the serve node, so the hook
	// subprocesses inherit THIS process's environment.
	t.Setenv("OPENSTORAGE_PG_URL", dbURL)
	t.Setenv("OPENSTORAGE_S3_URL", blobURL)

	repo := "t-hold-before-repack"
	cacheDir := t.TempDir()
	key, pemBlock := genKeyPEM(t, "ES256")
	laneStore, laneBlob := openLaneStores(t)
	// -1 = the trigger never ran.
	var liveHolds atomic.Int64
	liveHolds.Store(-1)
	srv := &Server{
		Store:    laneStore,
		Blob:     laneBlob,
		CacheDir: cacheDir,
		Verifier: auth.NewVerifier(memKeys{testOrg: {pemBlock}}),
		Org:      testOrg,
		ResolveRepo: func(_ context.Context, base string) (meta.Repo, bool) {
			if base != repo {
				return meta.Repo{}, false
			}
			return meta.Repo{RepoID: repo, URL: repo, DefaultBranch: "main"}, true
		},
		MaybeRepack: func(_ context.Context, _ string) {
			entries, err := os.ReadDir(filepath.Join(RepoPath(cacheDir, repo), pushHoldDir))
			if err != nil {
				t.Errorf("read hold dir from the repack trigger: %v", err)
				return
			}
			n := int64(0)
			for _, e := range entries {
				if strings.HasSuffix(e.Name(), pushHoldUnreservedSuffix) ||
					strings.HasSuffix(e.Name(), pushHoldVerifiedSuffix) || e.IsDir() {
					continue
				}
				n++
			}
			liveHolds.Store(n)
		},
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	work := initWorkRepo(t)
	token := mintUnitToken(t, key, "ES256", unitClaims(testOrg, repo, auth.ScopeGitRead, auth.ScopeGitWrite))
	url := strings.Replace(ts.URL, "http://", "http://t:"+token+"@", 1) + "/" + repo + ".git"
	out, err := tryGit(work, "push", url, "HEAD:refs/heads/main")
	if err != nil {
		t.Fatalf("push through the in-process server: %v\n%s", err, out)
	}
	// The trigger runs synchronously in the handler AFTER the response
	// streamed, so the client-side push can return first; wait out that
	// small tail.
	for deadline := time.Now().Add(10 * time.Second); liveHolds.Load() == -1 && time.Now().Before(deadline); {
		time.Sleep(10 * time.Millisecond)
	}
	if got := liveHolds.Load(); got != 0 {
		t.Fatalf("repack trigger observed %d live push holds, want 0 (released before the trigger)", got)
	}
}

// TestPush_TrailingDoomedRewindDoesNotHoldProtectedName (Codex round-25
// P2 on #173): the budget-free classifier cannot know a no-force-push
// UPDATE's ancestry verdict, and bulk-reserving it on faith let a crafted
// push sort one doomed rewind of a protected name AFTER other slow
// commands — the denial release only fires when the rewind's own hook
// runs, so the name stayed held through every earlier hook, timing out
// exactly the legitimate fast-forwards the policy protects. Such updates
// now reserve JUST-IN-TIME in their own hook, after the real ancestry
// check passes. Pinned by pausing an EARLIER command's hook (one-shot
// barrier) while the trailing rewind has not run: the protected name must
// not be held (a legitimate fast-forward push lands immediately), and a
// fast-forward under the same policy must still commit through the JIT
// reserve→CAS path afterwards.
func TestPush_TrailingDoomedRewindDoesNotHoldProtectedName(t *testing.T) {
	repo := registerRepo(t, "t-jit-trailing-rewind")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	v1 := revParse(t, work, "HEAD")
	mustGit(t, work, "commit", "--allow-empty", "-m", "advance protected to v2")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token),
		"HEAD:refs/heads/main", "HEAD:refs/heads/protected")

	// Concurrent pusher C: clone at v2, one commit ahead for its
	// fast-forward of protected.
	workC := filepath.Join(t.TempDir(), "clone-c")
	mustGit(t, "", "clone", work, workC)
	mustGit(t, workC, "commit", "--allow-empty", "-m", "C: fast-forward to v3")
	headC := revParse(t, workC, "HEAD")

	// B's main tip is a NEW commit so the push carries a pack an allowed
	// command needs (publication-gate requirement).
	mustGit(t, work, "commit", "--allow-empty", "-m", "B: new tip for main")
	headB := revParse(t, work, "HEAD")

	barrier := filepath.Join(t.TempDir(), "update-barrier")
	nodeCache := t.TempDir()
	node, err := startNode(nodeCache, EnvUpdateBarrier+"="+barrier)
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(node.stop)
	noff := mintTestToken(t, repo, "git:read,git:write", `[["refs/heads/protected",["no-force-push"]]]`)

	type pushResult struct {
		out string
		err error
	}
	// Push B: the fast-forward of main runs FIRST — git orders updates
	// of advertised refs before other commands, in advertisement order,
	// and "main" precedes "protected" (verified against this server: a
	// bundled CREATION's hook ran after the rewind's regardless of the
	// client's argument order). Its hook consumes the one-shot barrier
	// and pauses post-CAS; the doomed protected rewind (v2 -> v1) runs
	// LAST, so its hook has not executed while the barrier holds — which
	// was exactly the pre-fix squat window.
	doneB := make(chan pushResult, 1)
	go func() {
		out, err := tryGit(work, "push", "--force", remoteURL(node, repo, noff),
			"HEAD:refs/heads/main", v1+":refs/heads/protected")
		doneB <- pushResult{out, err}
	}()
	t.Cleanup(func() { _ = os.WriteFile(barrier+".release", nil, 0o644) })
	waitForFile(t, barrier+".ready", 30*time.Second)

	// The trailing rewind must not be holding the protected name.
	holds, err := readPushHolds(filepath.Join(RepoPath(nodeCache, repo), pushHoldDir), "")
	if err != nil {
		t.Fatalf("readPushHolds: %v", err)
	}
	if holds.covers("refs/heads/protected") {
		t.Fatal("the trailing unevaluated rewind bulk-reserved the protected name")
	}
	// And a legitimate fast-forward lands immediately while B is paused.
	outC, errC := tryGit(workC, "push", remoteURL(node, repo, token), "HEAD:refs/heads/protected")
	if errC != nil {
		t.Fatalf("fast-forward blocked by the trailing rewind's reservation: %v\n%.1500s", errC, outC)
	}

	if err := os.WriteFile(barrier+".release", nil, 0o644); err != nil {
		t.Fatalf("release barrier: %v", err)
	}
	resB := <-doneB
	if resB.err == nil {
		t.Fatal("push B fully succeeded; the trailing rewind was supposed to be denied per-ref")
	}
	refs := pgRefs(t, repo)
	if refs["refs/heads/protected"] != headC {
		t.Fatalf("authority protected = %.8s, want C's fast-forward %.8s", refs["refs/heads/protected"], headC)
	}
	if got := refs["refs/heads/main"]; got != headB {
		t.Fatalf("B's valid fast-forward of main = %.8s, want %.8s", got, headB)
	}

	// The JIT reserve→CAS path itself: a fast-forward of the protected
	// name under the SAME policy reserves in its own hook and commits
	// (workC's HEAD is headC — exactly the authority's current value).
	mustGit(t, workC, "commit", "--allow-empty", "-m", "C: v4 fast-forward under policy")
	headD := revParse(t, workC, "HEAD")
	if out, err := tryGit(workC, "push", remoteURL(node, repo, noff), "HEAD:refs/heads/protected"); err != nil {
		t.Fatalf("fast-forward under no-force-push failed through the JIT reservation: %v\n%.1500s", err, out)
	}
	if got := pgRefs(t, repo)["refs/heads/protected"]; got != headD {
		t.Fatalf("authority protected = %.8s, want %.8s", got, headD)
	}
}

// TestPush_ConcurrentPushProceedsDuringPublication (Codex round-23
// security P2 on #173): the hold reservation must not span pack
// publication — upload/registration/verification is unbounded backend
// time under a bounded byte cap, and a reservation taken before it
// turned every concurrent same-name or D/F-relative push into a
// 10-second wait plus denial while no CAS could occur yet. Push A parks
// between its publication and its DEFERRED reservation; push B to the
// same ref must land normally in that window (red with the pre-fix
// order: A already holds the name through publication, so B times out).
// Released, A reserves and re-probes: its now-stale creation dies as the
// ordinary CAS conflict, never a spurious local rejection, and the
// authority keeps B's value.
func TestPush_ConcurrentPushProceedsDuringPublication(t *testing.T) {
	repo := registerRepo(t, "t-deferred-reserve")
	barrier := filepath.Join(t.TempDir(), "prereceive-barrier")
	node := startNodeT(t, EnvPreReceiveBarrier+"="+barrier)
	token := mintTestToken(t, repo, "git:read,git:write", "")

	workA := initWorkRepo(t)
	mustGit(t, workA, "commit", "--allow-empty", "-m", "A tip")
	workB := initWorkRepo(t)
	mustGit(t, workB, "commit", "--allow-empty", "-m", "B tip")
	headB := revParse(t, workB, "HEAD")

	type pushResult struct {
		out string
		err error
	}
	done := make(chan pushResult, 1)
	go func() {
		out, err := tryGit(workA, "push", remoteURL(node, repo, token), "HEAD:refs/heads/race")
		done <- pushResult{out, err}
	}()
	t.Cleanup(func() { _ = os.WriteFile(barrier+".release", nil, 0o644) }) // never leave A parked
	for deadline := time.Now().Add(30 * time.Second); ; {
		if _, err := os.Stat(barrier + ".ready"); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("push A never reached the post-publication barrier")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// A is published but UNRESERVED: B's push of the same ref must land
	// while A is parked, not wait out a reservation A does not need yet.
	if out, err := tryGit(workB, "push", remoteURL(node, repo, token), "HEAD:refs/heads/race"); err != nil {
		t.Fatalf("concurrent push blocked during A's publication window: %v\n%.1500s", err, out)
	}
	if got := pgRefs(t, repo)["refs/heads/race"]; got != headB {
		t.Fatalf("authority refs/heads/race = %.8s, want B's %.8s", got, headB)
	}

	if err := os.WriteFile(barrier+".release", nil, 0o644); err != nil {
		t.Fatalf("release barrier: %v", err)
	}
	res := <-done
	if res.err == nil {
		t.Fatalf("A's stale creation succeeded against B's committed ref:\n%.1500s", res.out)
	}
	if got := pgRefs(t, repo)["refs/heads/race"]; got != headB {
		t.Fatalf("authority moved to %.8s after A's denial, want B's %.8s", got, headB)
	}
}

// TestPush_HoldStaysFreshThroughLocalRefTransaction (Codex round-23 P1
// on #173): per githooks(5), update runs BEFORE git updates the ref and
// post-receive AFTER all of them — so git's local ref transaction runs
// with every hook-owned refresher already stopped. A high-fanout atomic
// transaction outlasting pushHoldTTL would lose its hold mid-transaction:
// readers expire it, a concurrent hydrate materializes the already-CASed
// authority value under receive-pack, and the local transaction aborts
// even though every authority CAS committed. The HANDLER therefore owns a
// refresher across receive-pack's whole lifetime. Pinned by parking a
// push INSIDE the transaction (a blocking reference-transaction hook —
// no server hook runs there) for several shrunk TTLs and asserting the
// hold is still live for readers.
func TestPush_HoldStaysFreshThroughLocalRefTransaction(t *testing.T) {
	// The in-process server plays the serve node, so the hook
	// subprocesses inherit THIS process's environment.
	t.Setenv("OPENSTORAGE_PG_URL", dbURL)
	t.Setenv("OPENSTORAGE_S3_URL", blobURL)
	// Shrunk so the reader-expiry gap is observable on test timescales.
	// The hook subprocesses keep the production TTL (fresh re-exec), but
	// hook-owned refreshers are exactly what is ABSENT in the gap under
	// test; the serve-side reader, sweeper, and handler refresher all
	// honor the shrunk value.
	prevTTL := pushHoldTTL
	pushHoldTTL = 2 * time.Second // refresher cadence TTL/4 = 500ms
	t.Cleanup(func() { pushHoldTTL = prevTTL })

	repo := "t-hold-txn-gap"
	cacheDir := t.TempDir()
	key, pemBlock := genKeyPEM(t, "ES256")
	laneStore, laneBlob := openLaneStores(t)
	srv := &Server{
		Store:    laneStore,
		Blob:     laneBlob,
		CacheDir: cacheDir,
		Verifier: auth.NewVerifier(memKeys{testOrg: {pemBlock}}),
		Org:      testOrg,
		ResolveRepo: func(_ context.Context, base string) (meta.Repo, bool) {
			if base != repo {
				return meta.Repo{}, false
			}
			return meta.Repo{RepoID: repo, URL: repo, DefaultBranch: "main"}, true
		},
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	work := initWorkRepo(t)
	token := mintUnitToken(t, key, "ES256", unitClaims(testOrg, repo, auth.ScopeGitRead, auth.ScopeGitWrite))
	url := strings.Replace(ts.URL, "http://", "http://t:"+token+"@", 1) + "/" + repo + ".git"
	if out, err := tryGit(work, "push", url, "HEAD:refs/heads/main"); err != nil {
		t.Fatalf("seed push: %v\n%s", err, out)
	}

	// Park the NEXT push inside its local ref transaction: git runs the
	// reference-transaction hook at "prepared" — after the update hook
	// exited, before the commit — the exact gap under test.
	ctl := t.TempDir()
	sentinel := filepath.Join(ctl, "in-transaction")
	barrier := filepath.Join(ctl, "release")
	script := fmt.Sprintf(`#!/bin/sh
in=$(cat)
[ "$1" = "prepared" ] || exit 0
case "$in" in
*refs/heads/gap*)
	: > %q
	while [ ! -e %q ]; do sleep 0.05; done
	;;
esac
exit 0
`, sentinel, barrier)
	hookPath := filepath.Join(RepoPath(cacheDir, repo), "hooks", "reference-transaction")
	if err := os.WriteFile(hookPath, []byte(script), 0o755); err != nil {
		t.Fatalf("install reference-transaction hook: %v", err)
	}

	mustGit(t, work, "commit", "--allow-empty", "-m", "tip for gap")
	type pushResult struct {
		out string
		err error
	}
	done := make(chan pushResult, 1)
	go func() {
		out, err := tryGit(work, "push", url, "HEAD:refs/heads/gap")
		done <- pushResult{out, err}
	}()
	t.Cleanup(func() { _ = os.WriteFile(barrier, nil, 0o644) }) // never leave the push parked

	for deadline := time.Now().Add(30 * time.Second); ; {
		if _, err := os.Stat(sentinel); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("push never reached its local ref transaction")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// All hooks' refreshers are gone (update exited; post-receive hasn't
	// run). Sit in the transaction well past the TTL: only the handler's
	// refresher can keep the hold live for readers.
	time.Sleep(2*pushHoldTTL + pushHoldTTL/2)
	set, err := readPushHolds(filepath.Join(RepoPath(cacheDir, repo), pushHoldDir), "")
	if err != nil {
		t.Fatalf("readPushHolds: %v", err)
	}
	if !set.contains("refs/heads/gap") {
		t.Fatal("hold expired mid-transaction: a concurrent hydrate would materialize the already-CASed authority value under receive-pack and abort its local transaction")
	}

	if err := os.WriteFile(barrier, nil, 0o644); err != nil {
		t.Fatalf("release barrier: %v", err)
	}
	res := <-done
	if res.err != nil {
		t.Fatalf("parked push failed: %v\n%s", res.err, res.out)
	}
	// And the hold still dies promptly with the push (post-receive, with
	// the handler release as fallback) — the handler refresher must not
	// have turned it into a leak.
	for deadline := time.Now().Add(10 * time.Second); ; {
		entries, err := os.ReadDir(filepath.Join(RepoPath(cacheDir, repo), pushHoldDir))
		if err != nil {
			t.Fatalf("read hold dir: %v", err)
		}
		live := 0
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), pushHoldUnreservedSuffix) ||
				strings.HasSuffix(e.Name(), pushHoldVerifiedSuffix) || e.IsDir() {
				continue
			}
			live++
		}
		if live == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d hold file(s) survived the push", live)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestPush_RefPathBudgetDeniesUnmaterializableName (Codex round-21 P2 on
// #173): the static 900-byte name cap cannot see the operator-chosen
// --cache-dir depth. git resolves the git dir ABSOLUTE at startup, so
// under a deep cache root a name inside the static caps still exceeds
// PATH_MAX on the loose lock path — the authority CAS commits, then
// receive-pack's local update fails ("cannot lock ref", verified against
// real git with the update hook already exited 0): an authority-only ref
// every later hydration also fails to materialize. The name gate must
// deny such refs against the ACTUAL repository path before any CAS.
func TestPush_RefPathBudgetDeniesUnmaterializableName(t *testing.T) {
	repo := registerRepo(t, "t-ref-path-budget")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")

	// A cache root deep enough that root+repo+longRef crosses PATH_MAX
	// (4096) while longRef stays inside the static 900/200/32 caps.
	deep := t.TempDir()
	seg := strings.Repeat("d", 180)
	for len(deep) < 3400 {
		deep = filepath.Join(deep, seg)
	}
	if err := os.MkdirAll(deep, 0o755); err != nil {
		t.Fatalf("mkdir deep cache root: %v", err)
	}
	node, err := startNode(deep)
	if err != nil {
		t.Fatalf("start node on deep cache root: %v", err)
	}
	t.Cleanup(node.stop)

	longRef := "refs/heads/" + strings.Repeat(strings.Repeat("r", 174)+"/", 3) + strings.Repeat("r", 174)
	out, err := tryGit(work, "push", remoteURL(node, repo, token), "HEAD:"+longRef, "HEAD:refs/heads/ok")
	if err == nil {
		t.Fatalf("push fully succeeded; the unmaterializable name was supposed to be denied:\n%.1500s", out)
	}
	refs := pgRefs(t, repo)
	if _, wedged := refs[longRef]; wedged {
		t.Fatal("authority holds the locally unmaterializable ref — the authority-only wedge under test")
	}
	if _, ok := refs["refs/heads/ok"]; !ok {
		t.Fatalf("materializable sibling did not land: %v", refs)
	}
	if !strings.Contains(out, "not updatable here") {
		t.Fatalf("want the per-ref not-updatable denial, got:\n%.1500s", out)
	}
}

// openLaneStores opens the SAME backend lane stores the TestMain-booted
// environment (and therefore every hook subprocess, via
// OPENSTORAGE_PG_URL / OPENSTORAGE_S3_URL) uses. In-process servers must
// use these, NOT openTestRefStore/openTestBlob: those are per-test
// isolated stores, and a server whose hydrate reads a different store
// than its hooks write silently invalidates every cross-checking pin
// (generation bumps, registered packs, and ref values would all diverge).
func openLaneStores(t *testing.T) (ref.Store, blob.BlobStore) {
	t.Helper()
	st, err := ref.Open(context.Background(), dbURL)
	if err != nil {
		t.Fatalf("open lane ref store: %v", err)
	}
	t.Cleanup(st.Close)
	bl, err := blob.Open(context.Background(), blobURL)
	if err != nil {
		t.Fatalf("open lane blob store: %v", err)
	}
	t.Cleanup(func() { _ = bl.Close() })
	return st, bl
}

// countingRefStore counts paged ref scans; everything else forwards.
// Wrapping deliberately hides optional capabilities (HydrateSnapshotter),
// pinning hydration to the paged ListRefsAfter path on every lane.
type countingRefStore struct {
	ref.Store
	listPages atomic.Int64
}

func (c *countingRefStore) ListRefsAfter(ctx context.Context, repo, after string, limit int) ([]ref.RefEntry, error) {
	c.listPages.Add(1)
	return c.Store.ListRefsAfter(ctx, repo, after, limit)
}

// TestPreReceive_PostReservationDenialReleasesHold (Codex round-29 P2 on
// #173): a pre-receive error AFTER the hold reservation — here the atomic
// base check catching a stale advertised base — rejects the whole push:
// git runs no update hook and no post-receive, so the only remaining
// release was the handler fallback behind receive-pack's client-stallable
// report drain, with the handler refresher keeping every reserved name
// held into concurrent pushes' reservation timeouts. Pre-receive must
// release the whole hold on any error return.
func TestPreReceive_PostReservationDenialReleasesHold(t *testing.T) {
	t.Setenv("OPENSTORAGE_PG_URL", dbURL)
	laneStore, _ := openLaneStores(t)
	repo := "t-prereceive-denial-release"
	shaA := strings.Repeat("a", 40)
	shaB := strings.Repeat("b", 40)
	if _, err := laneStore.CASRef(context.Background(), repo, "refs/heads/main", ref.ZeroSHA, shaA); err != nil {
		t.Fatalf("seed authority ref: %v", err)
	}
	// The no-pack path verifies the pushed object exists in the cwd repo
	// before reserving; run the hook from a work repo that has a commit.
	work := initWorkRepo(t)
	newSHA := revParse(t, work, "HEAD")
	t.Chdir(work)

	holdPath := filepath.Join(t.TempDir(), pushHoldDir, "push-prereceive")
	if err := os.MkdirAll(filepath.Dir(holdPath), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	t.Setenv(EnvRepo, repo)
	t.Setenv(EnvPushHold, holdPath)
	t.Setenv(EnvAtomic, "1")

	// stdin claims the ref sits at shaB while the authority holds shaA: the
	// atomic base check inside reserveHoldsAndProbe denies AFTER the
	// reservation landed.
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	oldStdin := os.Stdin
	os.Stdin = r
	t.Cleanup(func() { os.Stdin = oldStdin; _ = r.Close() })
	if _, err := fmt.Fprintf(w, "%s %s refs/heads/main\n", shaB, newSHA); err != nil {
		t.Fatalf("write stdin: %v", err)
	}
	_ = w.Close()

	hookErr := RunHook(context.Background(), []string{"pre-receive"})
	if hookErr == nil || !strings.Contains(hookErr.Error(), "moved concurrently") {
		t.Fatalf("stale atomic base must deny the push, got %v", hookErr)
	}
	if _, statErr := os.Stat(holdPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("pre-receive denial after reservation must release the whole hold: %v", statErr)
	}
	if _, statErr := os.Stat(holdPath + pushHoldVerifiedSuffix); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("denial must release the verified-verdict sidecar too: %v", statErr)
	}
}

// TestHydrate_HeldSkipDoesNotRescanPerRequest (Codex round-22 security
// P2 on #173): a sync that skipped a push-held ref publishes no
// generation marker, so with a long hook-phase push holding a DIFFERING
// ref (its CAS committed, its local update pending) every authenticated
// advertisement re-ran the full paged authority scan — one no-pack push
// plus advertisement spam sustained repository-scale store reads for the
// push's whole duration. The held-generation marker coalesces them: the
// scan's verdict is reusable until the generation, HEAD target, or the
// live hold fingerprint changes, so each waiter no longer repeats the
// scan while the held state is provably unchanged.
func TestHydrate_HeldSkipDoesNotRescanPerRequest(t *testing.T) {
	t.Setenv("OPENSTORAGE_PG_URL", dbURL)
	t.Setenv("OPENSTORAGE_S3_URL", blobURL)
	t.Setenv("OPENSTORAGE_TEST_MODE", "1")
	barrier := filepath.Join(t.TempDir(), "adv-barrier")
	t.Setenv(EnvUpdateBarrier, barrier)

	repo := "t-held-scan-coalesce"
	cacheDir := t.TempDir()
	key, pemBlock := genKeyPEM(t, "ES256")
	laneStore, laneBlob := openLaneStores(t)
	st := &countingRefStore{Store: laneStore}
	srv := &Server{
		Store:    st,
		Blob:     laneBlob,
		CacheDir: cacheDir,
		Verifier: auth.NewVerifier(memKeys{testOrg: {pemBlock}}),
		Org:      testOrg,
		ResolveRepo: func(_ context.Context, base string) (meta.Repo, bool) {
			if base != repo {
				return meta.Repo{}, false
			}
			return meta.Repo{RepoID: repo, URL: repo, DefaultBranch: "main"}, true
		},
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	work := initWorkRepo(t)
	token := mintUnitToken(t, key, "ES256", unitClaims(testOrg, repo, auth.ScopeGitRead, auth.ScopeGitWrite))
	url := strings.Replace(ts.URL, "http://", "http://t:"+token+"@", 1) + "/" + repo + ".git"
	adv := func() {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, ts.URL+"/"+repo+".git/info/refs?service=git-upload-pack", nil)
		if err != nil {
			t.Fatalf("build advertisement request: %v", err)
		}
		req.SetBasicAuth("t", token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("advertisement: %v", err)
		}
		defer resp.Body.Close()
		if _, err := io.Copy(io.Discard, resp.Body); err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("advertisement: status %d, read err %v", resp.StatusCode, err)
		}
	}

	// The push's held ref DIFFERS while paused: its hook committed the
	// authority CAS (bumping the generation) and pauses at the barrier
	// before git's local ref update.
	done := make(chan error, 1)
	go func() {
		_, err := tryGit(work, "push", url, "HEAD:refs/heads/held")
		done <- err
	}()
	waitForFile(t, barrier+".ready", 30*time.Second)

	// First advertisement: full paged scan, skips the held ref.
	adv()
	first := st.listPages.Load()
	if first == 0 {
		t.Fatal("first advertisement ran no paged scan")
	}
	// Second advertisement: generation, HEAD, and holds unchanged — the
	// held-skip verdict must be reused, not rescanned.
	adv()
	if got := st.listPages.Load(); got != first {
		t.Fatalf("second advertisement rescanned (%d -> %d pages) with generation and holds unchanged", first, got)
	}

	if err := os.WriteFile(barrier+".release", nil, 0o644); err != nil {
		t.Fatalf("release barrier: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("push: %v", err)
	}
	// Holds released: the coalesced verdict is stale, the next
	// advertisement rescans and heals into a normal marker.
	adv()
	if got := st.listPages.Load(); got == first {
		t.Fatal("post-push advertisement must rescan once the hold is released")
	}
}

// TestPush_CommittedNonAtomicRefReleasedBeforeTail (issue #186 findings
// 1+6, design comment on #186): committed refs must stop blocking
// same-name pushes the moment git reports their LOCAL updates durable —
// the reference-transaction hook (committed state) appends exactly the
// refs each committed callback lists to the push's `.released` tombstone
// sidecar, and reservation readers subtract tombstones. The premise pin
// in this test surfaced that modern git (2.55 probed empirically)
// BATCHES a non-atomic push's local ref updates into ONE transaction
// after every update hook has run — finding 1's original per-command
// trace assumed older per-command transactions — so releasing any name
// before its committed callback would reopen the hydrate/receive-pack
// race this machinery closes. Release-at-committed is sound on both
// shapes: per-command gits release each name as it lands, batched gits
// release the whole set the instant the batch commits, strictly before
// post-receive and the client-stallable report phase this test parks in.
func TestPush_CommittedNonAtomicRefReleasedBeforeTail(t *testing.T) {
	t.Setenv("OPENSTORAGE_PG_URL", dbURL)
	t.Setenv("OPENSTORAGE_S3_URL", blobURL)
	repo := "t-tombstone-committed"
	cacheDir := t.TempDir()
	key, pemBlock := genKeyPEM(t, "ES256")
	laneStore, laneBlob := openLaneStores(t)
	srv := &Server{
		Store:    laneStore,
		Blob:     laneBlob,
		CacheDir: cacheDir,
		Verifier: auth.NewVerifier(memKeys{testOrg: {pemBlock}}),
		Org:      testOrg,
		ResolveRepo: func(_ context.Context, base string) (meta.Repo, bool) {
			if base != repo {
				return meta.Repo{}, false
			}
			return meta.Repo{RepoID: repo, URL: repo, DefaultBranch: "main"}, true
		},
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	work := initWorkRepo(t)
	token := mintUnitToken(t, key, "ES256", unitClaims(testOrg, repo, auth.ScopeGitRead, auth.ScopeGitWrite))
	url := strings.Replace(ts.URL, "http://", "http://t:"+token+"@", 1) + "/" + repo + ".git"
	if out, err := tryGit(work, "push", url, "HEAD:refs/heads/main"); err != nil {
		t.Fatalf("seed push: %v\n%s", err, out)
	}

	// Park the push at the COMMITTED callback that lists the tail ref —
	// after chaining to the server binary so the production tombstone
	// path under test has run, before git proceeds toward post-receive.
	// This simulates any stall between the local commits and the
	// post-receive release (the report phase a non-reading client can
	// stall indefinitely).
	self, err := os.Executable()
	if err != nil {
		t.Fatalf("executable: %v", err)
	}
	ctl := t.TempDir()
	sentinel := filepath.Join(ctl, "committed-parked")
	barrier := filepath.Join(ctl, "release")
	script := fmt.Sprintf(`#!/bin/sh
in=$(cat)
[ "$1" = committed ] || exit 0
printf '%%s\n' "$in" | %q hook reference-transaction "$1"
case "$in" in
*refs/heads/tail*)
	: > %q
	while [ ! -e %q ]; do sleep 0.05; done
	;;
esac
exit 0
`, self, sentinel, barrier)
	hookPath := filepath.Join(RepoPath(cacheDir, repo), "hooks", "reference-transaction")
	if err := os.WriteFile(hookPath, []byte(script), 0o755); err != nil {
		t.Fatalf("install reference-transaction hook: %v", err)
	}

	mustGit(t, work, "commit", "--allow-empty", "-m", "tip two")
	tipTwo := strings.TrimSpace(mustGit(t, work, "rev-parse", "HEAD"))
	done := make(chan error, 1)
	go func() {
		_, err := tryGit(work, "push", url, "HEAD:refs/heads/main", "HEAD:refs/heads/tail")
		done <- err
	}()
	t.Cleanup(func() { _ = os.WriteFile(barrier, nil, 0o644) })
	for deadline := time.Now().Add(30 * time.Second); ; {
		if _, err := os.Stat(sentinel); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("push never parked at its committed callback")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Premise pin: a ref listed by a COMMITTED callback has its local
	// update durably applied — the fact release-at-committed stands on,
	// version-agnostic across per-command and batched receive-packs.
	if got := strings.TrimSpace(mustGit(t, RepoPath(cacheDir, repo), "rev-parse", "refs/heads/main")); got != tipTwo {
		t.Fatalf("premise broken: local refs/heads/main = %s at its committed callback, want %s", got, tipTwo)
	}

	// Finding 1, writer half: the committed names are tombstoned in the
	// push's `.released` sidecar while post-receive is still parked away.
	holdDir := filepath.Join(RepoPath(cacheDir, repo), pushHoldDir)
	entries, err := os.ReadDir(holdDir)
	if err != nil {
		t.Fatalf("read hold dir: %v", err)
	}
	released := ""
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), pushHoldReleasedSuffix) {
			b, err := os.ReadFile(filepath.Join(holdDir, e.Name()))
			if err != nil {
				t.Fatalf("read released sidecar: %v", err)
			}
			released = string(b)
		}
	}
	if !strings.Contains(released, "refs/heads/main\n") {
		t.Fatalf("committed ref not tombstoned while post-receive is parked away: released = %q", released)
	}

	// Finding 1, reader half: a concurrent push to the committed name
	// must succeed BEFORE post-receive — its reservation subtracts the
	// tombstone instead of timing out against the parked push's hold.
	mustGit(t, work, "commit", "--allow-empty", "-m", "tip three")
	if out, err := tryGit(work, "push", url, "HEAD:refs/heads/main"); err != nil {
		t.Fatalf("concurrent same-name push must succeed against a tombstoned hold: %v\n%s", err, out)
	}

	if err := os.WriteFile(barrier, nil, 0o644); err != nil {
		t.Fatalf("release barrier: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("parked push failed: %v", err)
	}
}

// TestPush_AtomicFanoutHookPhaseAdvertisementsCoalesce (issue #186
// finding 5): an atomic fanout bumps the store generation once per
// committed hook CAS while the local ref transaction waits for the last
// hook — a held marker recording one exact generation therefore missed
// EVERY advertisement inside that window and re-paid a full pack listing
// + paged ref scan per request. With each won CAS's exact attribution
// claimed under the push's live hold, the marker must coalesce: one paged
// scan for the whole hook phase, however many CASes land between
// advertisements.
func TestPush_AtomicFanoutHookPhaseAdvertisementsCoalesce(t *testing.T) {
	t.Setenv("OPENSTORAGE_PG_URL", dbURL)
	t.Setenv("OPENSTORAGE_S3_URL", blobURL)
	t.Setenv("OPENSTORAGE_TEST_MODE", "1")
	barrier := filepath.Join(t.TempDir(), "fanout-barrier")
	t.Setenv(EnvUpdateBarrierEach, barrier)

	repo := "t-fanout-adv-coalesce"
	cacheDir := t.TempDir()
	key, pemBlock := genKeyPEM(t, "ES256")
	laneStore, laneBlob := openLaneStores(t)
	st := &countingRefStore{Store: laneStore}
	srv := &Server{
		Store:    st,
		Blob:     laneBlob,
		CacheDir: cacheDir,
		Verifier: auth.NewVerifier(memKeys{testOrg: {pemBlock}}),
		Org:      testOrg,
		ResolveRepo: func(_ context.Context, base string) (meta.Repo, bool) {
			if base != repo {
				return meta.Repo{}, false
			}
			return meta.Repo{RepoID: repo, URL: repo, DefaultBranch: "main"}, true
		},
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	work := initWorkRepo(t)
	token := mintUnitToken(t, key, "ES256", unitClaims(testOrg, repo, auth.ScopeGitRead, auth.ScopeGitWrite))
	url := strings.Replace(ts.URL, "http://", "http://t:"+token+"@", 1) + "/" + repo + ".git"
	adv := func() {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, ts.URL+"/"+repo+".git/info/refs?service=git-upload-pack", nil)
		if err != nil {
			t.Fatalf("build advertisement request: %v", err)
		}
		req.SetBasicAuth("t", token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("advertisement: %v", err)
		}
		defer resp.Body.Close()
		if _, err := io.Copy(io.Discard, resp.Body); err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("advertisement: status %d, read err %v", resp.StatusCode, err)
		}
	}

	refs := []string{"refs/heads/f0", "refs/heads/f1", "refs/heads/f2", "refs/heads/f3"}
	t.Cleanup(func() {
		// Unwedge any still-parked hook if an assertion failed mid-loop.
		for _, name := range refs {
			_ = os.WriteFile(updateBarrierEachPath(barrier, name)+".release", nil, 0o644)
		}
	})
	args := []string{"push", "--atomic", url}
	for _, name := range refs {
		args = append(args, "HEAD:"+name)
	}
	done := make(chan error, 1)
	go func() {
		_, err := tryGit(work, args...)
		done <- err
	}()

	// git runs the update hooks strictly sequentially; each parks after
	// its committed CAS (generation moved, claim appended). Advertise in
	// every window: the first pays the one full scan and publishes the
	// held marker, every later one must coalesce across the claimed
	// range instead of rescanning.
	var first int64
	released := 0
	for remaining := append([]string(nil), refs...); len(remaining) > 0; {
		idx := -1
		deadline := time.Now().Add(30 * time.Second)
		for idx < 0 && time.Now().Before(deadline) {
			for i, name := range remaining {
				if _, err := os.Stat(updateBarrierEachPath(barrier, name) + ".ready"); err == nil {
					idx = i
					break
				}
			}
			if idx < 0 {
				time.Sleep(10 * time.Millisecond)
			}
		}
		if idx < 0 {
			t.Fatalf("no update hook parked (released %d of %d)", released, len(refs))
		}
		adv()
		if released == 0 {
			first = st.listPages.Load()
			if first == 0 {
				t.Fatal("first advertisement ran no paged scan")
			}
		} else if got := st.listPages.Load(); got != first {
			t.Fatalf("advertisement between fanout CASes rescanned (%d -> %d pages); the claimed range must coalesce", first, got)
		}
		if err := os.WriteFile(updateBarrierEachPath(barrier, remaining[idx])+".release", nil, 0o644); err != nil {
			t.Fatalf("release barrier: %v", err)
		}
		remaining = append(remaining[:idx], remaining[idx+1:]...)
		released++
	}
	if err := <-done; err != nil {
		t.Fatalf("push: %v", err)
	}
	// Holds released by post-receive: the coalesced verdict is stale and
	// the next advertisement heals with a fresh scan.
	adv()
	if got := st.listPages.Load(); got == first {
		t.Fatal("post-push advertisement must rescan once the hold is released")
	}
}

// TestHydrate_ExternalWriteDuringHookPhaseForcesRescan (issue #186
// finding 5, the gap rule's e2e half): a generation bump NOBODY's live
// hold claims — here a REST-style direct store write landing mid-push —
// must force the healing rescan even though the fingerprint still
// matches; the marker may never vouch for work it cannot attribute.
func TestHydrate_ExternalWriteDuringHookPhaseForcesRescan(t *testing.T) {
	t.Setenv("OPENSTORAGE_PG_URL", dbURL)
	t.Setenv("OPENSTORAGE_S3_URL", blobURL)
	t.Setenv("OPENSTORAGE_TEST_MODE", "1")
	barrier := filepath.Join(t.TempDir(), "gap-barrier")
	t.Setenv(EnvUpdateBarrier, barrier)

	repo := "t-external-gap-rescan"
	cacheDir := t.TempDir()
	key, pemBlock := genKeyPEM(t, "ES256")
	laneStore, laneBlob := openLaneStores(t)
	st := &countingRefStore{Store: laneStore}
	srv := &Server{
		Store:    st,
		Blob:     laneBlob,
		CacheDir: cacheDir,
		Verifier: auth.NewVerifier(memKeys{testOrg: {pemBlock}}),
		Org:      testOrg,
		ResolveRepo: func(_ context.Context, base string) (meta.Repo, bool) {
			if base != repo {
				return meta.Repo{}, false
			}
			return meta.Repo{RepoID: repo, URL: repo, DefaultBranch: "main"}, true
		},
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	work := initWorkRepo(t)
	token := mintUnitToken(t, key, "ES256", unitClaims(testOrg, repo, auth.ScopeGitRead, auth.ScopeGitWrite))
	url := strings.Replace(ts.URL, "http://", "http://t:"+token+"@", 1) + "/" + repo + ".git"
	adv := func() {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, ts.URL+"/"+repo+".git/info/refs?service=git-upload-pack", nil)
		if err != nil {
			t.Fatalf("build advertisement request: %v", err)
		}
		req.SetBasicAuth("t", token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("advertisement: %v", err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("advertisement: status %d, read err %v, body %.500q", resp.StatusCode, err, string(body))
		}
	}

	done := make(chan error, 1)
	go func() {
		_, err := tryGit(work, "push", url, "HEAD:refs/heads/held")
		done <- err
	}()
	waitForFile(t, barrier+".ready", 30*time.Second)

	adv()
	first := st.listPages.Load()
	if first == 0 {
		t.Fatal("first advertisement ran no paged scan")
	}
	// The unclaimed bump: a direct store write no live hold vouches for.
	// The target object is the pushed commit — published and registered
	// by pre-receive before any hook ran — so the healing sync can
	// materialize the ref.
	sha := revParse(t, work, "HEAD")
	if _, err := laneStore.CASRef(context.Background(), repo, "refs/heads/external", ref.ZeroSHA, sha); err != nil {
		t.Fatalf("external CASRef: %v", err)
	}
	adv()
	if got := st.listPages.Load(); got == first {
		t.Fatal("an unclaimed external write must force the healing rescan")
	}

	if err := os.WriteFile(barrier+".release", nil, 0o644); err != nil {
		t.Fatalf("release barrier: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("push: %v", err)
	}
}
