package githttp

// Unit coverage for the push-hold machinery (issue #170): full syncs must
// skip create/update/delete commands naming a held ref (in both the single
// pass and the overflow second pass), ignore expired holds, and suppress
// the generation marker whenever anything was skipped.

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/coder/code-storage/internal/ref"
)

// mintHold creates a hold file in repoPath naming the given refs, through
// the same atomic reservation pushes use.
func mintHold(t *testing.T, repoPath string, names ...string) string {
	t.Helper()
	holdPath, err := mintPushHold(repoPath)
	if err != nil {
		t.Fatalf("mintPushHold: %v", err)
	}
	if err := reservePushHolds(context.Background(), holdPath, names); err != nil {
		t.Fatalf("reservePushHolds(%v): %v", names, err)
	}
	return holdPath
}

// TestSyncRefsStreamed_SkipsPushHeldRefs: held creations, updates, and
// deletions are all left untouched while unheld refs in the same sync
// apply; the sync reports skipped=true. Batch 1 forces the overflow second
// pass and per-command delete flushes, so both passes are covered.
func TestSyncRefsStreamed_SkipsPushHeldRefs(t *testing.T) {
	for _, batch := range []int{1, 3, syncRefsBatch} {
		dir := initWorkRepo(t)
		head := revParse(t, dir, "HEAD")
		writeFile(t, dir, "b.txt", "two\n")
		mustGit(t, dir, "add", ".")
		mustGit(t, dir, "commit", "-m", "two")
		head2 := revParse(t, dir, "HEAD")
		mustGit(t, dir, "branch", "stale", head)
		mustGit(t, dir, "branch", "upd", head)

		mintHold(t, dir, "refs/heads/stale", "refs/heads/upd", "refs/heads/new")

		want := map[string]string{
			"refs/heads/main": head2,
			"refs/heads/upd":  head2, // held: must stay at head
			"refs/heads/new":  head2, // held: must not be created
			"refs/heads/free": head2, // unheld: must be created
			// stale absent from want: held, must survive the deletion.
		}
		syncFromMapSkipped(t, dir, want, "refs/heads/main", batch, true)

		got := localRefs(t, dir)
		if got["refs/heads/stale"] != head {
			t.Fatalf("batch %d: held stale ref = %q, want %q (deletion must be skipped)", batch, got["refs/heads/stale"], head)
		}
		if got["refs/heads/upd"] != head {
			t.Fatalf("batch %d: held upd ref = %q, want %q (update must be skipped)", batch, got["refs/heads/upd"], head)
		}
		if _, ok := got["refs/heads/new"]; ok {
			t.Fatalf("batch %d: held new ref was created: %v", batch, got)
		}
		if got["refs/heads/free"] != head2 {
			t.Fatalf("batch %d: unheld free ref = %q, want %q", batch, got["refs/heads/free"], head2)
		}
	}
}

// syncFromMapSkipped is syncFromMap asserting the skipped verdict.
func syncFromMapSkipped(t *testing.T, repoPath string, want map[string]string, headTarget string, batch int, wantSkipped bool) {
	t.Helper()
	names := make([]string, 0, len(want))
	for name := range want {
		names = append(names, name)
	}
	sort.Strings(names)
	newStore := func() refSeq { return &sliceRefSeq{names: names, shas: want} }
	skipped, err := syncRefsStreamed(context.Background(), repoPath, headTarget, newStore, batch)
	if err != nil {
		t.Fatalf("syncRefsStreamed(batch=%d): %v", batch, err)
	}
	if skipped != wantSkipped {
		t.Fatalf("syncRefsStreamed(batch=%d) skipped = %v, want %v", batch, skipped, wantSkipped)
	}
}

// TestSyncRefsStreamed_SkipsHeldDFRelatives: a hold covers strict D/F
// relatives of the held name in BOTH directions — git's files backend
// cannot hold refs/heads/a and refs/heads/a/b simultaneously, so while a
// push owns one side a sync must not touch the other side either.
func TestSyncRefsStreamed_SkipsHeldDFRelatives(t *testing.T) {
	dir := initWorkRepo(t)
	head := revParse(t, dir, "HEAD")
	mustGit(t, dir, "branch", "a", head)

	// Hold ONLY refs/heads/a: both its deletion and the creation of its
	// D/F child refs/heads/a/b must be skipped.
	mintHold(t, dir, "refs/heads/a")

	want := map[string]string{
		"refs/heads/main": head,
		"refs/heads/a/b":  head,
	}
	syncFromMapSkipped(t, dir, want, "refs/heads/main", 1, true)

	got := localRefs(t, dir)
	if got["refs/heads/a"] != head {
		t.Fatalf("held D/F source deleted: %v", got)
	}
	if _, ok := got["refs/heads/a/b"]; ok {
		t.Fatalf("D/F child of a held ref created: %v", got)
	}
}

// TestPushHoldSet_CoversAndConflicts pins the D/F matching semantics:
// covers is equality plus strict prefix relations in both directions;
// conflicts excludes equality (same-name races are resolved cleanly by the
// authority CAS itself).
func TestPushHoldSet_CoversAndConflicts(t *testing.T) {
	s := newPushHoldSet([]string{"refs/heads/a"})
	for name, want := range map[string][2]bool{ // name -> {covers, conflicts}
		"refs/heads/a":     {true, false},
		"refs/heads/a/b":   {true, true},
		"refs/heads/a/b/c": {true, true},
		"refs/heads/ab":    {false, false},
		"refs/heads":       {true, true}, // held name is its strict child
		"refs/tags/a":      {false, false},
	} {
		if got := s.covers(name); got != want[0] {
			t.Errorf("covers(%q) = %v, want %v", name, got, want[0])
		}
		if got := s.conflicts(name); got != want[1] {
			t.Errorf("conflicts(%q) = %v, want %v", name, got, want[1])
		}
	}
}

// TestTouchPushHold_RestoresFreshness: the hook-side refresh keeps a hold
// live past the TTL for as long as its owner runs.
func TestTouchPushHold_RestoresFreshness(t *testing.T) {
	dir := t.TempDir()
	holdPath := mintHold(t, dir, "refs/heads/x")
	expired := time.Now().Add(-pushHoldTTL - time.Minute)
	if err := os.Chtimes(holdPath, expired, expired); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}
	if loadPushHolds(dir).covers("refs/heads/x") {
		t.Fatal("expired hold still live")
	}
	touchPushHold(holdPath)
	if !loadPushHolds(dir).covers("refs/heads/x") {
		t.Fatal("touched hold not live again")
	}
}

// TestSyncRefsStreamed_IgnoresExpiredHold: readers enforce the TTL
// themselves — a crash-leaked hold must not suppress syncs forever.
func TestSyncRefsStreamed_IgnoresExpiredHold(t *testing.T) {
	dir := initWorkRepo(t)
	head := revParse(t, dir, "HEAD")

	holdPath := mintHold(t, dir, "refs/heads/new")
	expired := time.Now().Add(-pushHoldTTL - time.Minute)
	if err := os.Chtimes(holdPath, expired, expired); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}

	want := map[string]string{"refs/heads/main": head, "refs/heads/new": head}
	syncFromMapSkipped(t, dir, want, "refs/heads/main", syncRefsBatch, false)
	if got := localRefs(t, dir)["refs/heads/new"]; got != head {
		t.Fatalf("expired hold still suppressed the sync: refs/heads/new = %q, want %q", got, head)
	}
}

// TestMintPushHold_SweepsExpiredLeaks: minting reaps crash-leaked expired
// files and leaves live ones alone.
func TestMintPushHold_SweepsExpiredLeaks(t *testing.T) {
	dir := t.TempDir()
	leaked := mintHold(t, dir, "refs/heads/leaked")
	expired := time.Now().Add(-pushHoldTTL - time.Minute)
	if err := os.Chtimes(leaked, expired, expired); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}
	live := mintHold(t, dir, "refs/heads/live")

	if _, err := mintPushHold(dir); err != nil {
		t.Fatalf("mintPushHold: %v", err)
	}
	if _, err := os.Stat(leaked); !os.IsNotExist(err) {
		t.Fatalf("expired hold survived the mint sweep: %v", err)
	}
	if _, err := os.Stat(live); err != nil {
		t.Fatalf("live hold reaped by the mint sweep: %v", err)
	}
}

// TestReservePushHolds_RejectsMalformedInput: relative paths (the hook
// runs with a different working directory than the server) and
// line-splitting ref names must fail the push rather than mint a broken
// hold.
func TestReservePushHolds_RejectsMalformedInput(t *testing.T) {
	if err := reservePushHolds(context.Background(), "relative/hold", []string{"refs/heads/x"}); err == nil {
		t.Fatal("relative hold path accepted")
	}
	abs := filepath.Join(t.TempDir(), "holds", "hold")
	if err := reservePushHolds(context.Background(), abs, []string{"refs/heads/a\nrefs/heads/b"}); err == nil {
		t.Fatal("line-splitting ref name accepted")
	}
	if err := reservePushHolds(context.Background(), abs, []string{""}); err == nil {
		t.Fatal("empty ref name accepted")
	}
}

// TestReservePushHolds_DeniesLiveConflictFailsClosed: a reservation racing
// a live D/F-conflicting hold from another push waits briefly and then
// DENIES rather than committing past the conflict; same-name overlap and
// unrelated names reserve immediately, and an expired conflicting hold is
// ignored.
func TestReservePushHolds_DeniesLiveConflictFailsClosed(t *testing.T) {
	dir := t.TempDir()
	other := mintHold(t, dir, "refs/heads/a")

	prev := pushHoldWaitMax
	pushHoldWaitMax = 100 * time.Millisecond
	defer func() { pushHoldWaitMax = prev }()

	own, err := mintPushHold(dir)
	if err != nil {
		t.Fatalf("mintPushHold: %v", err)
	}
	if err := reservePushHolds(context.Background(), own, []string{"refs/heads/a/b"}); err == nil {
		t.Fatal("reservation past a live D/F-conflicting hold accepted")
	} else if !strings.Contains(err.Error(), "conflicting") {
		t.Fatalf("denial reason = %v, want a conflict message", err)
	}
	// Same-name and unrelated refs are NOT conflicts.
	if err := reservePushHolds(context.Background(), own, []string{"refs/heads/a", "refs/heads/z"}); err != nil {
		t.Fatalf("non-conflicting reservation denied: %v", err)
	}
	// An expired conflicting hold is a crash leak, not a conflict. (own
	// also holds refs/heads/a by now — expire both.)
	expired := time.Now().Add(-pushHoldTTL - time.Minute)
	for _, p := range []string{other, own} {
		if err := os.Chtimes(p, expired, expired); err != nil {
			t.Fatalf("Chtimes: %v", err)
		}
	}
	own2, err := mintPushHold(dir)
	if err != nil {
		t.Fatalf("mintPushHold: %v", err)
	}
	if err := reservePushHolds(context.Background(), own2, []string{"refs/heads/a/c"}); err != nil {
		t.Fatalf("reservation blocked by an expired hold: %v", err)
	}
}

// TestReservePushHolds_FailsClosedOnUnreadableHolds: an unreadable peer
// hold is not proof of no conflict — the reservation must deny, never
// proceed on a partial peer set.
func TestReservePushHolds_FailsClosedOnUnreadableHolds(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("directory permissions do not bind root")
	}
	dir := t.TempDir()
	mintHold(t, dir, "refs/heads/other")
	holdsDir := filepath.Join(dir, pushHoldDir)
	if err := os.Chmod(holdsDir, 0o000); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(holdsDir, 0o755) })

	own := filepath.Join(holdsDir, "push-own")
	if err := reservePushHolds(context.Background(), own, []string{"refs/heads/x"}); err == nil {
		t.Fatal("reservation succeeded over an unreadable hold directory")
	}
}

// TestIntraPushDFCheck pins the intra-push directory/file rules: a command
// set whose end state cannot exist in one git ref store must be denied
// before any authority CAS, while ordered non-atomic D/F renames stay
// allowed.
func TestIntraPushDFCheck(t *testing.T) {
	const sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	del := func(name string) refCommand { return refCommand{old: sha, new: ref.ZeroSHA, ref: name} }
	upd := func(name string) refCommand { return refCommand{old: ref.ZeroSHA, new: sha, ref: name} }
	for _, tc := range []struct {
		name     string
		commands []refCommand
		atomic   bool
		wantErr  bool
	}{
		{"unrelated names", []refCommand{upd("refs/heads/a"), upd("refs/heads/ab"), upd("refs/tags/a")}, false, false},
		{"both survive", []refCommand{upd("refs/heads/a"), upd("refs/heads/a/b")}, false, true},
		{"both survive, deep", []refCommand{upd("refs/heads/a"), upd("refs/heads/a/b/c")}, false, true},
		{"delete before create", []refCommand{del("refs/heads/a"), upd("refs/heads/a/b")}, false, false},
		{"create before delete", []refCommand{upd("refs/heads/a/b"), del("refs/heads/a")}, false, true},
		{"descendant delete before ancestor create", []refCommand{del("refs/heads/a/b"), upd("refs/heads/a")}, false, false},
		{"ancestor create before descendant delete", []refCommand{upd("refs/heads/a"), del("refs/heads/a/b")}, false, true},
		{"both deletions", []refCommand{del("refs/heads/a"), del("refs/heads/a/b")}, false, false},
		{"atomic delete+create", []refCommand{del("refs/heads/a"), upd("refs/heads/a/b")}, true, true},
		{"atomic both deletions", []refCommand{del("refs/heads/a"), del("refs/heads/a/b")}, true, false},
	} {
		err := intraPushDFCheck(tc.commands, tc.atomic)
		if (err != nil) != tc.wantErr {
			t.Errorf("%s: err = %v, wantErr %v", tc.name, err, tc.wantErr)
		}
	}
}

// TestReservePushHolds_LockAcquisitionBounded: a reserver stalled while
// holding the reserve lock must not hang pushes queued behind it — the
// wait deadline covers lock acquisition too, yielding the retryable
// denial.
func TestReservePushHolds_LockAcquisitionBounded(t *testing.T) {
	dir := t.TempDir()
	holdsDir := filepath.Join(dir, pushHoldDir)
	if err := os.MkdirAll(holdsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	unlock, err := flockExclusive(holdsDir + ".reserve.lock")
	if err != nil {
		t.Fatalf("flockExclusive: %v", err)
	}
	defer unlock()

	prev := pushHoldWaitMax
	pushHoldWaitMax = 150 * time.Millisecond
	defer func() { pushHoldWaitMax = prev }()

	start := time.Now()
	err = reservePushHolds(context.Background(), filepath.Join(holdsDir, "push-own"), []string{"refs/heads/x"})
	if err == nil {
		t.Fatal("reservation succeeded while the reserve lock was held elsewhere")
	}
	if !strings.Contains(err.Error(), "retry shortly") {
		t.Fatalf("denial = %v, want a retryable timeout", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("reservation blocked %v, want the ~150ms deadline", elapsed)
	}
}

// TestPushRefUpdatable pins the committable-name rules: namespaced refs of
// bounded depth pass; namespace roots, non-refs names, malformed segments,
// and attacker-chosen depth amplification are refused.
func TestPushRefUpdatable(t *testing.T) {
	deep := "refs/heads" + strings.Repeat("/x", maxPushRefDepth)
	ok := "refs/heads" + strings.Repeat("/x", maxPushRefDepth-2)
	for name, want := range map[string]bool{
		"refs/heads/main":        true,
		"refs/tags/v1.0.0":       true,
		"refs/heads/feat/deep/x": true,
		ok:                       true,
		"refs/heads":             false, // namespace root
		"refs":                   false,
		"HEAD":                   false,
		"refs/heads/":            false, // empty segment
		"refs//x":                false,
		"refs/heads/..":          false,
		"refs/heads/x.lock":      false,
		deep:                     false, // depth cap
	} {
		if got := pushRefUpdatable(name); got != want {
			t.Errorf("pushRefUpdatable(%q) = %v, want %v", name, got, want)
		}
	}
}

// TestHydrate_SkippedHeldRefSuppressesMarker: a full sync that skipped a
// held ref must not advance the generation marker — otherwise a fresh
// marker could vouch for a generation whose held ref the cache never
// applied (fatal if the holding push dies before its local ref update).
func TestHydrate_SkippedHeldRefSuppressesMarker(t *testing.T) {
	st, bl, cacheDir := hydrateEnv(t)
	repoPath := mustHydrate(t, st, bl, cacheDir, "r")
	if gen, _, _, ok := readGenerationMarker(repoPath); !ok || gen != 7 {
		t.Fatalf("baseline marker = (%d, %v), want (7, true)", gen, ok)
	}

	// A push is mid-flight: its ref is committed in the store and held.
	// The SHA never needs to exist locally — the sync must skip it.
	mintHold(t, repoPath, "refs/heads/held")
	st.mu.Lock()
	st.refs["refs/heads/held"] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	st.mu.Unlock()
	st.setGen(8)

	if _, refsErr, err := Hydrate(context.Background(), st, bl, cacheDir, "r", "main"); refsErr != nil || err != nil {
		t.Fatalf("Hydrate with held ref = (refsErr %v, err %v), want clean", refsErr, err)
	}
	if gen, _, _, ok := readGenerationMarker(repoPath); !ok || gen != 7 {
		t.Fatalf("marker after skipped sync = (%d, %v), want unchanged (7, true)", gen, ok)
	}

	// The push finished (server removed the hold; here it also vanished
	// from the store as if deleted): the next full sync converges and the
	// marker advances again.
	holds, err := os.ReadDir(filepath.Join(repoPath, pushHoldDir))
	if err != nil {
		t.Fatalf("read holds: %v", err)
	}
	for _, e := range holds {
		if err := os.Remove(filepath.Join(repoPath, pushHoldDir, e.Name())); err != nil {
			t.Fatalf("remove hold: %v", err)
		}
	}
	st.mu.Lock()
	delete(st.refs, "refs/heads/held")
	st.mu.Unlock()
	st.setGen(9)
	mustHydrate(t, st, bl, cacheDir, "r")
	if gen, _, _, ok := readGenerationMarker(repoPath); !ok || gen != 9 {
		t.Fatalf("marker after hold cleared = (%d, %v), want (9, true)", gen, ok)
	}
}
