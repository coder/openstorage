package githttp

// Unit coverage for the push-hold machinery (issue #170): full syncs must
// skip create/update/delete commands naming a held ref (in both the single
// pass and the overflow second pass), ignore expired holds, and suppress
// the generation marker whenever anything was skipped.

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
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

// expireHold ages holdPath past BOTH observations of the round-25
// two-phase expiry: the hold itself past the wall TTL and its reap marker
// past the grace, exactly the state a genuine crash leak reaches after
// its first expired-looking observation plus the grace.
func expireHold(t *testing.T, holdPath string) {
	t.Helper()
	past := time.Now().Add(-2 * pushHoldTTL)
	if err := os.Chtimes(holdPath, past, past); err != nil {
		t.Fatalf("chtimes hold: %v", err)
	}
	reap := holdPath + pushHoldReapSuffix
	if err := os.WriteFile(reap, nil, 0o600); err != nil {
		t.Fatalf("write reap marker: %v", err)
	}
	markerPast := time.Now().Add(-pushHoldTTL/2 - time.Minute)
	if err := os.Chtimes(reap, markerPast, markerPast); err != nil {
		t.Fatalf("chtimes marker: %v", err)
	}
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
// conflicts excludes equality — it serves pre-receive's INTRA-push
// creation check, where the probed set always contains the probed name
// itself (peer-hold reservation uses covers: same-name holds are
// exclusive, see the round-14 atomic base check).
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
	expireHold(t, holdPath)
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
	expireHold(t, holdPath)

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
	expireHold(t, leaked)
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
// a live conflicting hold from another push — a strict D/F relative or the
// SAME name (Codex round-14 P1: same-name exclusion is what freezes a held
// name's authority value on this node and makes the atomic base check
// sound) — waits briefly and then DENIES rather than committing past the
// conflict; unrelated names reserve immediately, and an expired
// conflicting hold is ignored.
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
	if err := reservePushHolds(context.Background(), own, []string{"refs/heads/a"}); err == nil {
		t.Fatal("reservation past a live same-name hold accepted")
	}
	// Unrelated refs reserve immediately.
	if err := reservePushHolds(context.Background(), own, []string{"refs/heads/z"}); err != nil {
		t.Fatalf("non-conflicting reservation denied: %v", err)
	}
	// An expired conflicting hold is a crash leak, not a conflict.
	expireHold(t, other)
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

// TestAtomicPushDFCheck pins the atomic-only intra-push directory/file
// rule: any strict-prefix pair is fatal unless both sides are deletions —
// an atomic transaction can neither hold both nor reorder a delete+create
// swap. Non-atomic pushes are deliberately unchecked (per-ref semantics;
// the update hook's authority probe decides at commit time).
func TestAtomicPushDFCheck(t *testing.T) {
	const sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	del := func(name string) refCommand { return refCommand{old: sha, new: ref.ZeroSHA, ref: name} }
	upd := func(name string) refCommand { return refCommand{old: ref.ZeroSHA, new: sha, ref: name} }
	for _, tc := range []struct {
		name     string
		commands []refCommand
		wantErr  bool
	}{
		{"unrelated names", []refCommand{upd("refs/heads/a"), upd("refs/heads/ab"), upd("refs/tags/a")}, false},
		{"both survive", []refCommand{upd("refs/heads/a"), upd("refs/heads/a/b")}, true},
		{"both survive, deep", []refCommand{upd("refs/heads/a"), upd("refs/heads/a/b/c")}, true},
		{"delete plus create", []refCommand{del("refs/heads/a"), upd("refs/heads/a/b")}, true},
		{"create plus delete", []refCommand{upd("refs/heads/a/b"), del("refs/heads/a")}, true},
		{"both deletions", []refCommand{del("refs/heads/a"), del("refs/heads/a/b")}, false},
	} {
		err := atomicPushDFCheck(tc.commands)
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
	// Just under the total-length bound (4 segments of 179 plus the root
	// and leaf stay <= maxPushRefNameBytes) — every segment also within
	// the segment bound.
	longOK := "refs/" + strings.Repeat(strings.Repeat("b", 179)+"/", 4) + "x"
	cases := map[string]bool{
		"refs/heads/main":        true,
		"refs/tags/v1.0.0":       true,
		"refs/heads/feat/deep/x": true,
		"refs/heads/@":           true, // git bans "@" only as the WHOLE name
		"refs/heads/foo@bar":     true,
		"refs/heads/foo{bar":     true, // "{" is only banned after "@"
		"refs/heads/foo./bar":    true, // only the FINAL trailing dot is banned
		ok:                       true,
		"refs/heads":             false, // namespace root
		"refs":                   false,
		"HEAD":                   false,
		"refs/heads/":            false, // empty segment
		"refs//x":                false,
		"refs/heads/..":          false,
		"refs/heads/x.lock":      false,
		"refs/heads/x.lock/y":    false, // .lock bans every segment
		deep:                     false, // depth cap
		// git check_refname_format parity (Codex round-9 P1): every name
		// git's own funny-refname check rejects must be rejected here too,
		// or an atomic push carrying it aborts only AFTER earlier hooks'
		// authority CASes committed.
		"refs/heads/foo.":    false, // trailing dot
		"refs/heads/a..b":    false, // double dot
		"refs/heads/.foo":    false, // segment starts with a dot
		"refs/heads/f@{oo":   false, // "@{"
		"refs/heads/fo o":    false, // space
		"refs/heads/fo~o":    false,
		"refs/heads/fo^o":    false,
		"refs/heads/fo:o":    false,
		"refs/heads/fo?o":    false,
		"refs/heads/fo*o":    false,
		"refs/heads/fo[o":    false,
		"refs/heads/fo\\o":   false,
		"refs/heads/fo\x01o": false, // control byte
		"refs/heads/fo\x7fo": false, // DEL
		// Filesystem materializability (Codex round-16 P1): git's format
		// check accepts a 256-byte component, but the files backend then
		// fails ENAMETOOLONG on the .lock file AFTER the authority CAS —
		// bound segment and total name length before any CAS.
		"refs/heads/" + strings.Repeat("a", maxPushRefSegmentBytes):     true,
		"refs/heads/" + strings.Repeat("a", maxPushRefSegmentBytes+1):   false,
		"refs/heads/" + strings.Repeat("a", 256):                        false, // the observed git-accepted, fs-rejected shape
		longOK:                                                          true,
		"refs/" + strings.Repeat(strings.Repeat("b", 179)+"/", 5) + "x": false, // total length over maxPushRefNameBytes
	}
	for name, want := range cases {
		if got := pushRefUpdatable(name); got != want {
			t.Errorf("pushRefUpdatable(%q) = %v, want %v", name, got, want)
		}
	}
	// Oracle for the load-bearing direction: every name this predicate
	// ACCEPTS must be acceptable to git itself, or the atomic whole-push
	// gate lets a doomed command reach the hooks. (The reverse is server
	// policy: namespace roots and the depth cap are deliberately stricter
	// than git.)
	git, err := exec.LookPath("git")
	if err != nil {
		t.Skip("git not installed")
	}
	for name, want := range cases {
		if !want {
			continue
		}
		if out, err := exec.Command(git, "check-ref-format", name).CombinedOutput(); err != nil {
			t.Errorf("pushRefUpdatable(%q) = true but git check-ref-format rejects it: %v %s", name, err, out)
		}
	}
}

// TestPushHoldReaders_SkipUnreservedSidecar: the sidecar lists a push's
// NOT-reserved names — treating them as held would hand policy-denied refs
// the very namespace hold the reservation filter exists to refuse.
func TestPushHoldReaders_SkipUnreservedSidecar(t *testing.T) {
	dir := t.TempDir()
	holdPath := mintHold(t, dir, "refs/heads/held")
	if err := os.MkdirAll(holdPath+pushHoldUnreservedSuffix, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(pushHoldUnreservedMarker(holdPath, "refs/heads/denied"), nil, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	holds := loadPushHolds(dir)
	if !holds.covers("refs/heads/held") {
		t.Fatal("real hold not loaded")
	}
	if holds.covers("refs/heads/denied") {
		t.Fatal("sidecar name treated as held")
	}
	// Reservation conflict checks must skip it too.
	own, err := mintPushHold(dir)
	if err != nil {
		t.Fatalf("mintPushHold: %v", err)
	}
	if err := reservePushHolds(context.Background(), own, []string{"refs/heads/denied/x"}); err != nil {
		t.Fatalf("sidecar name blocked a reservation: %v", err)
	}
}

// probeCountStore lets authorityDoomedCreations tests count exactly the
// reads the probe performs: GetRef point-reads (countingStore's panics —
// it guards Hydrate) and the ROWS ListRefsAfter returns, the number that
// exposes a repo-sized scan hiding behind few calls.
type probeCountStore struct {
	*countingStore
	getRefCalls int
	listRows    int
}

func (s *probeCountStore) GetRef(_ context.Context, _ string, name string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getRefCalls++
	if sha, ok := s.refs[name]; ok {
		return sha, nil
	}
	return "", ref.ErrNotFound
}

func (s *probeCountStore) ListRefsAfter(ctx context.Context, repo, after string, limit int) ([]ref.RefEntry, error) {
	page, err := s.countingStore.ListRefsAfter(ctx, repo, after, limit)
	s.mu.Lock()
	s.listRows += len(page)
	s.mu.Unlock()
	return page, err
}

// TestAuthorityDoomedCreations_PushProportionalProbes: creations whose
// strict D/F relative exists in the authority are doomed (both
// directions), unrelated and exact-name matches are not — and the probe
// work must scale with the PUSH, never the repository (Codex round-9
// security P2: the previous full-repo paged scan let five tiny creations
// read every authority ref).
func TestAuthorityDoomedCreations_PushProportionalProbes(t *testing.T) {
	refs := map[string]string{
		"refs/heads/a":     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"refs/heads/x/y":   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"refs/heads/plain": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	// Repo-sized ballast the probe must never read.
	for i := 0; i < 2000; i++ {
		refs[fmt.Sprintf("refs/zbulk/b%04d", i)] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	}
	st := &probeCountStore{countingStore: &countingStore{gen: 1, refs: refs}}
	creations := []string{
		"refs/heads/a/b",   // doomed: strict ancestor a exists
		"refs/heads/x",     // doomed: strict descendant x/y exists
		"refs/heads/plain", // exact name exists: NOT a D/F conflict
		"refs/heads/new1",
		"refs/heads/new2",
		"refs/heads/ab", // sibling of a: not a conflict
	}
	doomed, err := authorityDoomedCreations(context.Background(), st, "r", creations)
	if err != nil {
		t.Fatalf("authorityDoomedCreations: %v", err)
	}
	for _, want := range []string{"refs/heads/a/b", "refs/heads/x"} {
		if _, ok := doomed[want]; !ok {
			t.Errorf("%s not reported doomed: %v", want, doomed)
		}
	}
	if len(doomed) != 2 {
		t.Fatalf("doomed = %v, want exactly the two D/F conflicts", doomed)
	}
	// Proportionality: unique strict prefixes across the set are refs,
	// refs/heads, refs/heads/a — three point reads; the seek-merge may
	// list at most one single-row page per creation.
	if st.getRefCalls != 3 {
		t.Errorf("getRefCalls = %d, want 3 (deduped unique prefixes)", st.getRefCalls)
	}
	_, listCalls, _ := st.counts()
	if listCalls > len(creations) {
		t.Errorf("listRefsCalls = %d, want <= %d (one seek per creation)", listCalls, len(creations))
	}
	if st.listRows > len(creations) {
		t.Errorf("listRows = %d, want <= %d — the probe read repo-sized pages", st.listRows, len(creations))
	}
}

// TestAuthorityDoomedCreations_DecoratedOrderSeek: '.' (0x2E) sorts below
// '/' (0x2F), so RAW-sorted creations ("refs/heads/a" before
// "refs/heads/a.b") have NON-monotonic descendant-range starts
// ("refs/heads/a.b/" < "refs/heads/a/"). A seek-merge walking raw order
// would carry its skip window backwards over refs/heads/a.b and silently
// miss its existing descendant. Both creations must be doomed.
func TestAuthorityDoomedCreations_DecoratedOrderSeek(t *testing.T) {
	st := &probeCountStore{countingStore: &countingStore{gen: 1, refs: map[string]string{
		"refs/heads/a.b/c": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"refs/heads/a/z":   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}}}
	doomed, err := authorityDoomedCreations(context.Background(), st, "r",
		[]string{"refs/heads/a", "refs/heads/a.b"})
	if err != nil {
		t.Fatalf("authorityDoomedCreations: %v", err)
	}
	for _, want := range []string{"refs/heads/a", "refs/heads/a.b"} {
		if _, ok := doomed[want]; !ok {
			t.Errorf("%s not reported doomed: %v", want, doomed)
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

// TestPushHoldDirCache_ReparsesOnlyOnChange (Codex round-10 P2): the
// reservation conflict poll re-loads peer holds every 25ms for up to 10s;
// unchanged hold files must cost a stat, not a re-read + re-split +
// re-sort of potentially repo-scale name lists.
func TestPushHoldDirCache_ReparsesOnlyOnChange(t *testing.T) {
	dir := t.TempDir()
	holdA := mintHold(t, dir, "refs/heads/a")
	mintHold(t, dir, "refs/heads/b")

	cache := &pushHoldDirCache{dir: filepath.Join(dir, pushHoldDir)}
	set, err := cache.load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !set.contains("refs/heads/a") || !set.contains("refs/heads/b") {
		t.Fatalf("first load missed holds: %v", set.names)
	}
	if cache.parses != 2 {
		t.Fatalf("parses after first load = %d, want 2", cache.parses)
	}
	for i := 0; i < 5; i++ {
		if _, err := cache.load(); err != nil {
			t.Fatalf("steady-state load: %v", err)
		}
	}
	if cache.parses != 2 {
		t.Fatalf("parses after 5 unchanged loads = %d, want still 2", cache.parses)
	}

	// An append (size change) re-parses exactly the changed file.
	if err := appendPushHolds(holdA, []string{"refs/heads/a2"}); err != nil {
		t.Fatalf("appendPushHolds: %v", err)
	}
	set, err = cache.load()
	if err != nil {
		t.Fatalf("load after append: %v", err)
	}
	if cache.parses != 3 {
		t.Fatalf("parses after one append = %d, want 3", cache.parses)
	}
	if !set.contains("refs/heads/a2") {
		t.Fatalf("appended name missing: %v", set.names)
	}

	// A refresher touch (mtime change) re-parses that file once.
	future := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(holdA, future, future); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}
	if _, err := cache.load(); err != nil {
		t.Fatalf("load after touch: %v", err)
	}
	if cache.parses != 4 {
		t.Fatalf("parses after touch = %d, want 4", cache.parses)
	}

	// A removed hold drops out without re-parsing survivors.
	if err := os.Remove(holdA); err != nil {
		t.Fatalf("remove: %v", err)
	}
	set, err = cache.load()
	if err != nil {
		t.Fatalf("load after remove: %v", err)
	}
	if set.contains("refs/heads/a") || !set.contains("refs/heads/b") {
		t.Fatalf("set after remove = %v", set.names)
	}
	if cache.parses != 4 {
		t.Fatalf("parses after remove = %d, want still 4", cache.parses)
	}
}

// proberStore satisfies dfConflictProber; its embedded countingStore's
// GetRef PANICS and its ListRefsAfter count must stay zero, proving the
// batch path never falls back to per-name reads.
type proberStore struct {
	*countingStore
	probes int
}

func (s *proberStore) DFConflicts(_ context.Context, _ string, names []string) (map[string]string, error) {
	s.mu.Lock()
	existing := make([]string, 0, len(s.refs))
	for name := range s.refs {
		existing = append(existing, name)
	}
	s.probes++
	s.mu.Unlock()
	sort.Strings(existing)
	out := map[string]string{}
	for _, name := range names {
		for i := 0; i < len(name); i++ {
			if name[i] == '/' {
				if j := sort.SearchStrings(existing, name[:i]); j < len(existing) && existing[j] == name[:i] {
					out[name] = name[:i]
					break
				}
			}
		}
		if _, ok := out[name]; ok {
			continue
		}
		lo := name + "/"
		if j := sort.SearchStrings(existing, lo); j < len(existing) && strings.HasPrefix(existing[j], lo) {
			out[name] = existing[j]
		}
	}
	return out, nil
}

// TestAuthorityDoomedCreations_BatchProber (Codex rounds 10-13 on #173):
// stores implementing the batched D/F seam must be probed through ONE
// DFConflicts call — zero point reads, zero list pages — whatever the
// creation count or authority shape (the store's GetRef panics and its
// list counter must stay zero, so any fallback is loudly red). The shape
// is the reviewers' crafted worst case: one existing child beneath each
// proposed parent.
func TestAuthorityDoomedCreations_BatchProber(t *testing.T) {
	refs := map[string]string{
		"refs/heads/a":     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"refs/heads/x/y":   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"refs/heads/plain": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	var creations []string
	for i := 0; i < 100; i++ {
		parent := fmt.Sprintf("refs/heads/p%03d", i)
		refs[parent+"/held"] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		creations = append(creations, parent)
	}
	creations = append(creations,
		"refs/heads/a/b",   // doomed: strict ancestor a exists
		"refs/heads/x",     // doomed: strict descendant x/y exists
		"refs/heads/plain", // exact name exists: NOT a D/F conflict
		"refs/heads/new1",
	)
	st := &proberStore{countingStore: &countingStore{gen: 1, refs: refs}}
	doomed, err := authorityDoomedCreations(context.Background(), st, "r", creations)
	if err != nil {
		t.Fatalf("authorityDoomedCreations: %v", err)
	}
	if len(doomed) != 102 {
		t.Fatalf("doomed = %d names, want 102 (100 squatted parents + a/b + x)", len(doomed))
	}
	for _, want := range []string{"refs/heads/a/b", "refs/heads/x", "refs/heads/p000", "refs/heads/p099"} {
		if _, ok := doomed[want]; !ok {
			t.Errorf("%s not reported doomed", want)
		}
	}
	if st.probes != 1 {
		t.Errorf("DFConflicts calls = %d, want exactly 1", st.probes)
	}
	_, listCalls, _ := st.counts()
	if listCalls != 0 {
		t.Errorf("listRefsCalls = %d, want 0 — the batch path issued per-name store reads", listCalls)
	}
}

// valuesProberStore satisfies refValuesProber; its embedded countingStore's
// GetRef PANICS, proving the batch path never falls back to per-name reads.
type valuesProberStore struct {
	*countingStore
	probes int
}

func (s *valuesProberStore) RefValues(_ context.Context, _ string, names []string) (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.probes++
	out := map[string]string{}
	for _, name := range names {
		if sha, ok := s.refs[name]; ok {
			out[name] = sha
		}
	}
	return out, nil
}

// TestAuthorityRefValues_BatchProber (Codex round-14 P1 on #173): stores
// implementing the batched exact-value seam answer the atomic base check
// through ONE RefValues call — zero point reads whatever the command count
// (the store's GetRef panics, so any fallback is loudly red); stores
// without the seam fall back to per-name point-reads with absent names
// omitted.
func TestAuthorityRefValues_BatchProber(t *testing.T) {
	const sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	refs := map[string]string{"refs/heads/a": sha, "refs/heads/b": sha}
	names := []string{"refs/heads/a", "refs/heads/b", "refs/heads/absent"}
	want := map[string]string{"refs/heads/a": sha, "refs/heads/b": sha}

	st := &valuesProberStore{countingStore: &countingStore{gen: 1, refs: refs}}
	got, err := authorityRefValues(context.Background(), st, "r", names)
	if err != nil {
		t.Fatalf("authorityRefValues: %v", err)
	}
	if !maps.Equal(got, want) {
		t.Fatalf("prober path = %v, want %v", got, want)
	}
	if st.probes != 1 {
		t.Errorf("RefValues calls = %d, want exactly 1", st.probes)
	}

	fb := &probeCountStore{countingStore: &countingStore{gen: 1, refs: refs}}
	got, err = authorityRefValues(context.Background(), fb, "r", names)
	if err != nil {
		t.Fatalf("authorityRefValues fallback: %v", err)
	}
	if !maps.Equal(got, want) {
		t.Fatalf("fallback path = %v, want %v", got, want)
	}
	if fb.getRefCalls != len(names) {
		t.Errorf("fallback getRefCalls = %d, want %d (one per name)", fb.getRefCalls, len(names))
	}
}

// TestRemovePushHolds: releasing a doomed reservation must drop exactly
// the named refs, keep the rest, and leave no temp file behind (the
// rewrite is temp+rename so readers never see a truncated hold).
func TestRemovePushHolds(t *testing.T) {
	dir := t.TempDir()
	holdPath := mintHold(t, dir, "refs/heads/a", "refs/heads/b", "refs/heads/c")
	if err := removePushHolds(holdPath, []string{"refs/heads/b"}); err != nil {
		t.Fatalf("removePushHolds: %v", err)
	}
	holds := loadPushHolds(dir)
	for name, want := range map[string]bool{
		"refs/heads/a": true,
		"refs/heads/b": false,
		"refs/heads/c": true,
	} {
		if got := holds.contains(name); got != want {
			t.Errorf("contains(%s) = %v, want %v", name, got, want)
		}
	}
	if _, err := os.Stat(holdPath + ".release"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rewrite temp left behind: %v", err)
	}
}

// TestIsAncestor_Memoized (Codex round-11 security P2): the ancestry
// verdict for a (old, new) pair must be served from the per-process memo
// on repeat — the SHAs below name no real objects, so any fallthrough to
// git merge-base panics, making a cache miss loudly red.
func TestIsAncestor_Memoized(t *testing.T) {
	key := [2]string{
		strings.Repeat("d", 40),
		strings.Repeat("c", 40),
	}
	ancestryMemoMu.Lock()
	ancestryMemo[key] = true
	ancestryMemoMu.Unlock()
	defer func() {
		ancestryMemoMu.Lock()
		delete(ancestryMemo, key)
		ancestryMemoMu.Unlock()
	}()
	if !isAncestor(key[0], key[1]) {
		t.Fatal("memoized verdict not served")
	}
}

// TestClassifyReservation (Codex rounds 12+13 on #173): the budget-free
// reservation filter must return needs-verify for signature-gated
// creations and updates — flatly allowing them lets unsigned pushers
// D/F-squat namespaces, flatly denying them breaks legitimate signed
// fanout pushes at the sidecar cap (pre-receive resolves needs-verify by
// evaluating the push's REAL verdicts once). Deletions and rules without
// verify-sig stay allowed; no-push stays denied.
func TestClassifyReservation(t *testing.T) {
	t.Setenv(EnvRefsPolicy, `[["refs/heads/sigx",["verify-sig"]],["refs/heads/plainx",[]],["refs/heads/nopush",["no-push"]],["refs/heads/noff",["no-force-push"]],["refs/heads/signoff",["verify-sig","no-force-push"]],["refs/heads/noffsig",["no-force-push","verify-sig"]]]`)
	sha := strings.Repeat("a", 40)
	sha2 := strings.Repeat("b", 40)
	zero := strings.Repeat("0", 40)
	for _, tc := range []struct {
		ref, old, new string
		want          reservationClass
	}{
		{"refs/heads/sigx", zero, sha, reservationNeedsVerify}, // gated creation
		{"refs/heads/sigx", sha, sha2, reservationNeedsVerify}, // gated update
		{"refs/heads/sigx", sha, zero, reservationAllowed},     // deletion: vouchable
		{"refs/heads/plainx", zero, sha, reservationAllowed},   // matched, no verify-sig
		{"refs/heads/free", zero, sha, reservationAllowed},     // unpoliced
		{"refs/heads/nopush", zero, sha, reservationDenied},    // hard denial
		// no-force-push updates classify WITHOUT the ancestry subprocess
		// (Codex round-14 security P2: classification cost is
		// attacker-chosen, so it must never launch git merge-base) to a
		// JUST-IN-TIME reservation: the ref's own hook runs the real
		// check and reserves only on a pass (round-25 P2 — bulk-reserving
		// on faith let a doomed trailing rewind squat the protected name
		// through every earlier hook). These shas exist in no repository,
		// so reservationJIT is only reachable subprocess-free: a real
		// ancestry evaluation would deny.
		{"refs/heads/noff", sha, sha2, reservationJIT},     // update: hook checks + reserves
		{"refs/heads/noff", sha, zero, reservationDenied},  // deletion: budget-free denial
		{"refs/heads/noff", zero, sha, reservationAllowed}, // creation: no rewind possible
		// COMBINED rules classify independent of op ORDER, JIT outranking
		// needs-verify (Codex round-27 P2): the documented
		// ["verify-sig","no-force-push"] ordering used to hit the
		// signature sentinel first, so pre-receive paid a REAL evaluation
		// — signature walk plus ancestry subprocess — for a whole crafted
		// list of doomed rewinds before the sidecar cap could reject the
		// push. A JIT command instead charges nothing in pre-receive; its
		// own hook runs the full check (signature included) and reserves
		// only on a pass.
		{"refs/heads/signoff", sha, sha2, reservationJIT},         // update: doc op order
		{"refs/heads/noffsig", sha, sha2, reservationJIT},         // update: flipped op order
		{"refs/heads/signoff", zero, sha, reservationNeedsVerify}, // creation: no rewind, sig still gates
		{"refs/heads/signoff", sha, zero, reservationDenied},      // deletion: no-force-push denies
	} {
		if got := classifyReservation("r", tc.ref, tc.old, tc.new); got != tc.want {
			t.Errorf("classifyReservation(%s %.4s->%.4s) = %v, want %v",
				tc.ref, tc.old, tc.new, got, tc.want)
		}
	}
}

// TestPushCommandUpdatable_DeletionsSkipMaterializationBounds (Codex
// round-18 P2 on #173): a repository can already hold git-valid refs past
// the depth/length caps (created before the caps, or imported by sync
// from a permissive upstream). Deletions add no name and remove the disk
// path instead of creating one, so they are gated on format validity
// only — bounds still deny creations and updates of such names.
func TestPushCommandUpdatable_DeletionsSkipMaterializationBounds(t *testing.T) {
	sha := strings.Repeat("a", 40)
	sha2 := strings.Repeat("b", 40)
	zero := ref.ZeroSHA
	deep := "refs/heads" + strings.Repeat("/x", maxPushRefDepth+5)
	// Over the POLICY cap (200) but within the HARD lock-path limit
	// (NAME_MAX minus ".lock" = 250): still deletable. Segments past the
	// hard limit are not — git itself cannot lock them for deletion
	// (round-23 P2; see TestPushRefDeletable_LockPathBounds).
	longSeg := "refs/heads/" + strings.Repeat("a", maxPushRefSegmentBytes+40)
	for _, tc := range []struct {
		name, old, new string
		want           bool
	}{
		{deep, sha, zero, true},               // legacy over-depth deletion
		{longSeg, sha, zero, true},            // legacy over-length deletion
		{"refs/heads/fo~o", sha, zero, false}, // git-invalid name: git itself rejects
		{"refs/heads", sha, zero, false},      // namespace root stays undeliverable
		{deep, zero, sha, false},              // creation keeps the bounds
		{longSeg, sha, sha2, false},           // update keeps the bounds
	} {
		if got := pushCommandUpdatable(tc.name, tc.old, tc.new); got != tc.want {
			t.Errorf("pushCommandUpdatable(%.40s…, %.4s->%.4s) = %v, want %v",
				tc.name, tc.old, tc.new, got, tc.want)
		}
	}
}

// TestReservationVerdicts_MixedPushStaysSelective (Codex round-15 P2 on
// #173): one signature-gated command must not upgrade the WHOLE push to a
// full policy evaluation — the ungated no-force-push update keeps its
// budget-free reservationJIT verdict. The shas exist in no repository,
// so a full evaluation of the noff command would run git merge-base, fail
// the ancestry check, and flip it to denied — reservationJIT is only
// reachable when the mixed push leaves ungated commands unevaluated. The
// gated command itself pays a REAL evaluation: with no verify state
// available here it fails closed to denied (in production, valid signed
// refs bulk-reserve, invalid ones divert to the sidecar).
func TestReservationVerdicts_MixedPushStaysSelective(t *testing.T) {
	t.Setenv(EnvRefsPolicy, `[["refs/heads/sigx",["verify-sig"]],["refs/heads/noff",["no-force-push"]]]`)
	sha := strings.Repeat("a", 40)
	sha2 := strings.Repeat("b", 40)
	commands := []refCommand{
		{ref: "refs/heads/noff", old: sha, new: sha2},
		{ref: "refs/heads/sigx", old: sha, new: sha2},
		{ref: "refs/heads/free", old: sha, new: sha2},
		// Gated but undeliverable (round-18 security P2): must be denied
		// WITHOUT an evaluation — it diverts to the sidecar regardless.
		{ref: "refs/heads/sigx", old: sha2, new: sha},
	}
	updatable := []bool{true, true, true, false}
	evaluated := make([]bool, len(commands))
	evalOne := func(i int) error {
		evaluated[i] = true
		c := commands[i]
		return enforceRefPolicy(context.Background(), "r", c.ref, c.old, c.new)
	}
	classes, verified := reservationVerdicts("r", commands, updatable, evalOne)
	want := []reservationClass{reservationJIT, reservationDenied, reservationAllowed, reservationDenied}
	for i, cls := range classes {
		if cls != want[i] {
			t.Errorf("classes[%d] = %v, want %v", i, cls, want[i])
		}
	}
	// Only the gated UPDATABLE command may pay a real evaluation, and a
	// FAILED evaluation must not mark the command verified.
	if evaluated[0] || !evaluated[1] || evaluated[2] || evaluated[3] {
		t.Errorf("evaluated = %v, want only the deliverable signature-gated command", evaluated)
	}
	for i, v := range verified {
		if v {
			t.Errorf("verified[%d] = true, want none (the gated evaluation fails closed here)", i)
		}
	}
}

// TestPushVerifiedVerdicts_EveryPassPreserved (Codex round-19 P2 on
// #173): the consolidated verdict sidecar must preserve EVERY successful
// evaluation — the former one-inode-per-verdict directory capped markers
// at 4096 and silently dropped the rest, so exactly the fanouts big
// enough to need the budget protection re-charged it. 5000 verdicts (past
// the old cap) all stay consumable through the binary-search lookup, the
// sidecar is ONE fixed-width file (32 bytes per verdict, no inode
// amplification), and commands that never passed stay unverified.
func TestPushVerifiedVerdicts_EveryPassPreserved(t *testing.T) {
	hold, err := mintPushHold(t.TempDir())
	if err != nil {
		t.Fatalf("mintPushHold: %v", err)
	}
	sha := strings.Repeat("a", 40)
	sha2 := strings.Repeat("b", 40)
	const n = 5000
	digests := make([][sha256.Size]byte, 0, n)
	for i := 0; i < n; i++ {
		digests = append(digests, pushVerifiedDigest(fmt.Sprintf("refs/heads/b/%04d", i), sha, sha2))
	}
	if err := writePushVerifiedVerdicts(hold, digests); err != nil {
		t.Fatalf("writePushVerifiedVerdicts: %v", err)
	}
	fi, err := os.Stat(hold + pushHoldVerifiedSuffix)
	if err != nil {
		t.Fatalf("stat verdict sidecar: %v", err)
	}
	if fi.IsDir() || fi.Size() != int64(n*sha256.Size) {
		t.Fatalf("sidecar = dir:%v size:%d, want one %d-byte file", fi.IsDir(), fi.Size(), n*sha256.Size)
	}
	for i := 0; i < n; i++ {
		if !pushVerifiedVerdict(hold, fmt.Sprintf("refs/heads/b/%04d", i), sha, sha2) {
			t.Fatalf("verdict %d dropped (the legacy inode cap was 4096)", i)
		}
	}
	// Same name with different shas, and unseen names: never consumable.
	if pushVerifiedVerdict(hold, "refs/heads/b/0000", sha2, sha) {
		t.Fatal("verdict consumed for a command that never passed")
	}
	if pushVerifiedVerdict(hold, "refs/heads/unseen", sha, sha2) {
		t.Fatal("verdict consumed for an unseen ref")
	}
	// No sidecar at all (unlimited budgets write none): plain fallback.
	hold2, err := mintPushHold(t.TempDir())
	if err != nil {
		t.Fatalf("mintPushHold: %v", err)
	}
	if pushVerifiedVerdict(hold2, "refs/heads/b/0000", sha, sha2) {
		t.Fatal("verdict consumed without a sidecar")
	}
}

// TestPushRefPathTooLong (Codex round-21 P2 on #173) pins the whole-path
// budget arithmetic: `<repoDir>/<name>.lock` plus NUL against the target
// OS's PATH_MAX (round-24 security P2: derived per-OS — Linux 4096,
// Darwin 1024 — so the boundary is computed from the constant, not
// hard-coded). The e2e deep-cache-root test proves the end-to-end denial;
// this pins the boundary itself.
func TestPushRefPathTooLong(t *testing.T) {
	// PATH_MAX = repoDirLen + 1 (slash) + nameLen + 5 (".lock") + 1
	// (NUL): compute the largest fitting name under a repo dir that
	// leaves 700 bytes of headroom on any supported OS.
	repoDirLen := pushRefPathMax - 700
	fit := pushRefPathMax - repoDirLen - 1 - len(".lock") - 1 // 693
	if pushRefPathTooLong(repoDirLen, fit) {
		t.Fatalf("%d-byte name under a %d-byte repo dir fits PATH_MAX (%d); must be allowed", fit, repoDirLen, pushRefPathMax)
	}
	if !pushRefPathTooLong(repoDirLen, fit+1) {
		t.Fatalf("%d-byte name under a %d-byte repo dir exceeds PATH_MAX (%d); must be denied", fit+1, repoDirLen, pushRefPathMax)
	}
	// Typical roots leave the static 900-byte cap authoritative (Linux;
	// tighter-PATH_MAX targets lean on the dynamic budget instead).
	if pushRefPathMax >= 4096 && pushRefPathTooLong(100, maxPushRefNameBytes) {
		t.Fatal("the full static name cap must fit under a typical cache root")
	}
}

// TestPushHolds_FutureDatedHoldClampedLive (Codex round-21 P2 and
// round-22 P1 on #173): a far-future hold mtime means the wall clock
// stepped backward beneath it — under a LIVE push (whose reservation
// must never lapse: dropping it reopens the D/F pair race until the next
// refresher tick) or as a crash leak restored from a VM snapshot (whose
// lease must not extend until wall time catches up). Readers and the
// sweeper therefore CLAMP a far-future mtime to now and keep the hold
// live: a live push loses nothing, and a leak ages out within one TTL of
// first observation — the crash-leak bound, via the clamp instead of
// instant expiry. Near-future mtimes (ordinary skew) stay live untouched.
func TestPushHolds_FutureDatedHoldClampedLive(t *testing.T) {
	repoDir := t.TempDir()
	hold, err := mintPushHold(repoDir)
	if err != nil {
		t.Fatalf("mintPushHold: %v", err)
	}
	if err := os.WriteFile(hold, []byte("refs/heads/x\n"), 0o644); err != nil {
		t.Fatalf("write hold: %v", err)
	}
	holdDir := filepath.Join(repoDir, pushHoldDir)
	future := time.Now().Add(pushHoldTTL + time.Minute)
	if err := os.Chtimes(hold, future, future); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	set, err := readPushHolds(holdDir, "")
	if err != nil {
		t.Fatalf("readPushHolds: %v", err)
	}
	if !set.contains("refs/heads/x") {
		t.Fatal("future-dated hold dropped by the reader: a live push's reservation lapsed on a backward clock step")
	}
	fi, err := os.Stat(hold)
	if err != nil {
		t.Fatalf("stat hold: %v", err)
	}
	if age := time.Since(fi.ModTime()); age < 0 || age > time.Minute {
		t.Fatalf("future mtime not clamped to now (age %v): the leak's lease would outlive the TTL bound", age)
	}
	// From the clamp the hold ages NORMALLY: past the TTL it expires for
	// readers and the sweeper alike (the round-21 crash-leak bound),
	// through the round-25 two-observation rule — the first
	// expired-looking pass arms the reap marker and stays live; once the
	// marker outlives the grace with no owner touch, the hold is gone.
	past := time.Now().Add(-pushHoldTTL - time.Minute)
	if err := os.Chtimes(hold, past, past); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	set, err = readPushHolds(holdDir, "")
	if err != nil || !set.contains("refs/heads/x") {
		t.Fatalf("first expired-looking observation must stay live (forward-jump safety): set=%v err=%v", set, err)
	}
	markerPast := time.Now().Add(-pushHoldTTL/2 - time.Minute)
	if err := os.Chtimes(hold+pushHoldReapSuffix, markerPast, markerPast); err != nil {
		t.Fatalf("chtimes marker: %v", err)
	}
	set, err = readPushHolds(holdDir, "")
	if err != nil || set.contains("refs/heads/x") {
		t.Fatalf("expired hold still live: set=%v err=%v", set, err)
	}
	sweepExpiredPushHolds(holdDir)
	if _, err := os.Stat(hold); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired hold survived the sweep: %v", err)
	}
	// The sweeper clamps future-dated files rather than deleting them: a
	// crashed push's leak dies one TTL later; a live push's file is
	// never yanked out from under its refresher.
	hold2, err := mintPushHold(repoDir)
	if err != nil {
		t.Fatalf("mintPushHold: %v", err)
	}
	if err := os.WriteFile(hold2, []byte("refs/heads/y\n"), 0o644); err != nil {
		t.Fatalf("write hold: %v", err)
	}
	if err := os.Chtimes(hold2, future, future); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	sweepExpiredPushHolds(holdDir)
	fi2, err := os.Stat(hold2)
	if err != nil {
		t.Fatalf("future-dated hold deleted by the sweep (must be clamped): %v", err)
	}
	if age := time.Since(fi2.ModTime()); age < 0 || age > time.Minute {
		t.Fatalf("sweep did not clamp the future mtime (age %v)", age)
	}
	// Near-future skew (inside the TTL) is left untouched and live.
	near := time.Now().Add(pushHoldTTL / 2)
	if err := os.Chtimes(hold2, near, near); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	set, err = readPushHolds(holdDir, "")
	if err != nil || !set.contains("refs/heads/y") {
		t.Fatalf("near-future hold must stay live: set=%v err=%v", set, err)
	}
	fi2b, err := os.Stat(hold2)
	if err != nil || !fi2b.ModTime().Equal(near) {
		t.Fatalf("near-future mtime must stay untouched: %v (%v)", fi2b.ModTime(), err)
	}
}

// TestRunHook_PostReceiveReleasesHold (Codex round-22 P2 on #173): the
// push hold protects receive-pack's LOCAL ref transaction, and git's
// post-receive hook runs exactly when that transaction has completed —
// strictly before the per-ref report-status writes whose delivery a
// non-reading client can stall indefinitely (no server write timeout).
// Releasing in post-receive decouples the hold lifetime from
// client-controlled response drainage; the handler's own release stays
// as the fallback for pushes where git skips post-receive (no ref
// updated).
func TestRunHook_PostReceiveReleasesHold(t *testing.T) {
	repoDir := t.TempDir()
	hold, err := mintPushHold(repoDir)
	if err != nil {
		t.Fatalf("mintPushHold: %v", err)
	}
	if err := os.WriteFile(hold, []byte("refs/heads/x\n"), 0o644); err != nil {
		t.Fatalf("write hold: %v", err)
	}
	if err := os.MkdirAll(hold+pushHoldUnreservedSuffix, 0o755); err != nil {
		t.Fatalf("mkdir sidecar: %v", err)
	}
	if err := os.WriteFile(hold+pushHoldVerifiedSuffix, []byte("digest"), 0o600); err != nil {
		t.Fatalf("write sidecar: %v", err)
	}
	t.Setenv(EnvPushHold, hold)
	if err := RunHook(context.Background(), []string{"post-receive"}); err != nil {
		t.Fatalf("post-receive hook: %v", err)
	}
	for _, p := range []string{hold, hold + pushHoldUnreservedSuffix, hold + pushHoldVerifiedSuffix} {
		if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("post-receive left %s in place: %v", p, err)
		}
	}
	// Hookless invocation (no hold env): a clean no-op.
	t.Setenv(EnvPushHold, "")
	if err := RunHook(context.Background(), []string{"post-receive"}); err != nil {
		t.Fatalf("post-receive without a hold env: %v", err)
	}
}

// TestRunHook_PostReceiveReleasesHoldBeforeDrainingStdin (Codex round-23
// P2 on #173): post-receive's stdin carries one record per updated ref —
// attacker-scaled — and per githooks(5) the local ref transaction is
// already complete when the hook starts, so nothing the hold protects is
// still pending. Releasing only AFTER the drain would keep same-name and
// D/F-relative pushes blocked into their reservation timeout for the
// stream's whole duration. The hold must be gone while stdin is still
// open.
func TestRunHook_PostReceiveReleasesHoldBeforeDrainingStdin(t *testing.T) {
	repoDir := t.TempDir()
	hold, err := mintPushHold(repoDir)
	if err != nil {
		t.Fatalf("mintPushHold: %v", err)
	}
	if err := os.WriteFile(hold, []byte("refs/heads/x\n"), 0o644); err != nil {
		t.Fatalf("write hold: %v", err)
	}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	oldStdin := os.Stdin
	os.Stdin = r
	t.Setenv(EnvPushHold, hold)
	finished := make(chan struct{})
	var hookErr error
	go func() {
		hookErr = RunHook(context.Background(), []string{"post-receive"})
		close(finished)
	}()
	t.Cleanup(func() {
		// Unblock the drain (idempotent), wait the hook out, THEN swap
		// os.Stdin back — restoring while the hook still reads it would
		// be a race.
		_ = w.Close()
		<-finished
		os.Stdin = oldStdin
		_ = r.Close()
	})
	// The hold must vanish while the updated-ref stream is still OPEN.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, statErr := os.Stat(hold); errors.Is(statErr, os.ErrNotExist) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("hold still present while post-receive's stdin is open: the release is blocked behind the client-scaled drain")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close stdin pipe: %v", err)
	}
	<-finished
	if hookErr != nil {
		t.Fatalf("post-receive: %v", hookErr)
	}
}

// TestPushRefDeletable_LockPathBounds (Codex round-23 P2 on #173): git
// DELETES through <name>.lock too — update-ref -d creates the lock before
// unlinking (verified against real git: a manually materialized ref with
// a 251-byte final component is visible to for-each-ref, but update-ref
// -d fails "File name too long", while a 250-byte one deletes fine) — so
// the deletion gate must enforce the HARD filesystem lock-path limits
// even while waiving the conservative policy caps for legacy refs.
// Otherwise the update hook commits the authority deletion, git fails
// the local delete, the client is told the deletion failed while the
// authority durably dropped it, and later hydration cannot remove the
// stale cache ref either (same lock).
func TestPushRefDeletable_LockPathBounds(t *testing.T) {
	seg := func(n int) string { return strings.Repeat("a", n) }
	// Final component: NAME_MAX minus the ".lock" suffix = 250.
	if !pushRefDeletable("refs/heads/" + seg(250)) {
		t.Fatal("250-byte final component locks within NAME_MAX; must stay deletable")
	}
	if pushRefDeletable("refs/heads/" + seg(251)) {
		t.Fatal("251-byte final component's .lock exceeds NAME_MAX; git cannot delete it locally")
	}
	// Intermediate components are directory names: the full NAME_MAX.
	if !pushRefDeletable("refs/heads/" + seg(255) + "/x") {
		t.Fatal("255-byte intermediate directory fits NAME_MAX; must stay deletable")
	}
	if pushRefDeletable("refs/heads/" + seg(256) + "/x") {
		t.Fatal("256-byte intermediate directory exceeds NAME_MAX; git cannot create the lock's parent")
	}
	// The whole lock path budgets against PATH_MAX (round-21 P2's budget,
	// applied to deletions too). Long enough to overflow under ANY cwd.
	deep := "refs/heads/" + strings.Repeat(seg(200)+"/", 25) + "x"
	if pushRefDeletable(deep) {
		t.Fatal("a lock path past PATH_MAX must not be deletable")
	}
	// Legacy refs past the POLICY caps (depth 32, 200-byte segments,
	// 900-byte names) stay deletable — the round-18 P2 exemption the
	// hard limits must not reintroduce.
	legacy := "refs/heads/" + strings.Repeat("d/", 40) + seg(240)
	if !pushRefDeletable(legacy) {
		t.Fatal("legacy over-policy-cap ref must stay deletable")
	}
	if pushRefUpdatable(legacy) {
		t.Fatal("the policy caps must still gate creations and updates")
	}
}

// TestLivePushHoldsKey_StatOnly (Codex round-23 security P2 on #173): the
// held-marker fingerprint runs once per authenticated advertisement while
// a push is paused, so it must be answerable from directory STATS alone —
// the pre-fix implementation re-read and re-split every held name per
// request. Pins both halves: computing the key does not scale with hold
// content (a 100k-name hold costs a handful of allocations), and the key
// still tracks exactly the lifecycle events that must invalidate a held
// marker — touch, append, release, expiry.
func TestLivePushHoldsKey_StatOnly(t *testing.T) {
	repoDir := t.TempDir()
	hold, err := mintPushHold(repoDir)
	if err != nil {
		t.Fatalf("mintPushHold: %v", err)
	}
	var sb strings.Builder
	for i := 0; i < 100_000; i++ {
		fmt.Fprintf(&sb, "refs/heads/b/%06d\n", i)
	}
	if err := os.WriteFile(hold, []byte(sb.String()), 0o644); err != nil {
		t.Fatalf("write hold: %v", err)
	}
	key1, ok := livePushHoldsKey(repoDir)
	if !ok || key1 == "" {
		t.Fatalf("livePushHoldsKey: ok=%v", ok)
	}
	if key2, ok := livePushHoldsKey(repoDir); !ok || key2 != key1 {
		t.Fatalf("fingerprint unstable across unchanged holds: %q vs %q (ok=%v)", key2, key1, ok)
	}
	// Content-size independence: reading the ~1.7MB hold body costs at
	// least its length in allocated bytes per call; statting costs a few
	// KB. TotalAlloc is monotonic, so the delta is exact regardless of
	// GC.
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < 10; i++ {
		if _, ok := livePushHoldsKey(repoDir); !ok {
			t.Fatal("livePushHoldsKey failed")
		}
	}
	runtime.ReadMemStats(&after)
	if delta := after.TotalAlloc - before.TotalAlloc; delta > 1<<20 {
		t.Fatalf("livePushHoldsKey allocated %d bytes across 10 calls over a 100k-name hold: it is reading content, not statting", delta)
	}
	// A refresher touch invalidates (mtime moves; near-future is live).
	touched := time.Now().Add(time.Second)
	if err := os.Chtimes(hold, touched, touched); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	key3, ok := livePushHoldsKey(repoDir)
	if !ok || key3 == key1 {
		t.Fatal("refresher touch must change the fingerprint")
	}
	// An append invalidates (size moves).
	f, err := os.OpenFile(hold, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open for append: %v", err)
	}
	if _, err := f.WriteString("refs/heads/late\n"); err != nil {
		t.Fatalf("append: %v", err)
	}
	_ = f.Close()
	key4, ok := livePushHoldsKey(repoDir)
	if !ok || key4 == key3 {
		t.Fatal("hold append must change the fingerprint")
	}
	// Expiry (past the round-25 two-observation grace) drops the hold
	// out — same key as no holds at all.
	past := time.Now().Add(-pushHoldTTL - time.Minute)
	if err := os.Chtimes(hold, past, past); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	keySuspect, ok := livePushHoldsKey(repoDir)
	if !ok || keySuspect == key4 {
		t.Fatal("the expired-looking observation must still change the fingerprint (mtime moved)")
	}
	markerPast := time.Now().Add(-pushHoldTTL/2 - time.Minute)
	if err := os.Chtimes(hold+pushHoldReapSuffix, markerPast, markerPast); err != nil {
		t.Fatalf("chtimes marker: %v", err)
	}
	keyExpired, ok := livePushHoldsKey(repoDir)
	if !ok || keyExpired == keySuspect {
		t.Fatal("expiry must change the fingerprint")
	}
	// A release lands on the same fingerprint as expiry (no live holds).
	if err := os.Remove(hold); err != nil {
		t.Fatalf("remove hold: %v", err)
	}
	if keyGone, ok := livePushHoldsKey(repoDir); !ok || keyGone != keyExpired {
		t.Fatalf("released fingerprint %q != no-live-holds fingerprint %q (ok=%v)", keyGone, keyExpired, ok)
	}
}

// TestPushHolds_ForwardJumpKeepsHoldLive (Codex round-25 P2 on #173): a
// FORWARD wall-clock step or host suspend/resume makes a LIVE push's hold
// look ancient the instant the clock lands — but no real time passed for
// the push, its refresher can only Chtimes an existing file, and dropping
// or deleting the hold reopens the authority-CAS-to-local-transaction
// window this machinery exists to close. Expiry therefore needs a second
// observation: the first one past the wall TTL arms a reap marker and
// keeps the hold fully live for readers, reservers, and the mint sweep.
func TestPushHolds_ForwardJumpKeepsHoldLive(t *testing.T) {
	repoDir := t.TempDir()
	hold, err := mintPushHold(repoDir)
	if err != nil {
		t.Fatalf("mintPushHold: %v", err)
	}
	if err := os.WriteFile(hold, []byte("refs/heads/x\n"), 0o644); err != nil {
		t.Fatalf("write hold: %v", err)
	}
	// As the resumed clock sees it, the hold was last touched >TTL ago.
	old := time.Now().Add(-pushHoldTTL - time.Minute)
	if err := os.Chtimes(hold, old, old); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	dir := filepath.Join(repoDir, pushHoldDir)
	// Readers must keep it live on this first post-jump observation.
	set, err := readPushHolds(dir, "")
	if err != nil {
		t.Fatalf("readPushHolds: %v", err)
	}
	if !set.contains("refs/heads/x") {
		t.Fatal("forward clock jump dropped a live hold from readers: hydrate would materialize its in-flight ref mid-push")
	}
	// The mint sweep must not delete it either.
	if _, err := mintPushHold(repoDir); err != nil {
		t.Fatalf("second mint: %v", err)
	}
	if _, err := os.Stat(hold); err != nil {
		t.Fatal("mint sweep deleted a live hold on its first expired-looking observation")
	}
	// Reservers must still honor the conflict.
	prevWait := pushHoldWaitMax
	pushHoldWaitMax = 200 * time.Millisecond
	t.Cleanup(func() { pushHoldWaitMax = prevWait })
	peer := filepath.Join(dir, "push-peer")
	if err := reservePushHolds(context.Background(), peer, []string{"refs/heads/x"}); err == nil {
		t.Fatal("a conflicting reservation went through against a live (suspended) hold")
	}
	// The owner's next refresher tick supersedes the suspicion.
	touchPushHold(hold)
	set, err = readPushHolds(dir, "")
	if err != nil {
		t.Fatalf("readPushHolds after revive: %v", err)
	}
	if !set.contains("refs/heads/x") {
		t.Fatal("revived hold missing")
	}
}

// TestPushHolds_LeakExpiresAfterReapGrace: the two-observation expiry
// must still reap genuine crash leaks — once the reap marker (armed at
// first observation) is older than the grace with no owner touch after
// it, readers skip the hold and the sweep deletes it with its sidecars.
func TestPushHolds_LeakExpiresAfterReapGrace(t *testing.T) {
	repoDir := t.TempDir()
	hold, err := mintPushHold(repoDir)
	if err != nil {
		t.Fatalf("mintPushHold: %v", err)
	}
	if err := os.WriteFile(hold, []byte("refs/heads/x\n"), 0o644); err != nil {
		t.Fatalf("write hold: %v", err)
	}
	old := time.Now().Add(-2 * pushHoldTTL)
	if err := os.Chtimes(hold, old, old); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	dir := filepath.Join(repoDir, pushHoldDir)
	// First observation: live, and the reap marker is armed.
	set, err := readPushHolds(dir, "")
	if err != nil {
		t.Fatalf("readPushHolds: %v", err)
	}
	if !set.contains("refs/heads/x") {
		t.Fatal("first observation must keep the hold live")
	}
	reap := hold + pushHoldReapSuffix
	if _, err := os.Stat(reap); err != nil {
		t.Fatalf("first expired-looking observation did not arm the reap marker: %v", err)
	}
	// Age the MARKER past the grace: real observed time has now passed
	// between two observations with no owner touch.
	past := time.Now().Add(-pushHoldTTL/2 - time.Minute)
	if err := os.Chtimes(reap, past, past); err != nil {
		t.Fatalf("chtimes marker: %v", err)
	}
	set, err = readPushHolds(dir, "")
	if err != nil {
		t.Fatalf("readPushHolds: %v", err)
	}
	if set.contains("refs/heads/x") {
		t.Fatal("leaked hold still live after the reap grace")
	}
	sweepExpiredPushHolds(dir)
	if _, err := os.Stat(hold); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("sweep left the leaked hold: %v", err)
	}
	if _, err := os.Stat(reap); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("sweep left the reap marker: %v", err)
	}
}

// TestPushHolds_FutureReapMarkerClampedNotPinned (Codex round-27 P2 on
// #173): the backward-step clamp covered the hold FILE but not its reap
// marker. A crash leak whose marker went far-future (the wall clock
// stepped backward after the marker was armed — e.g. a VM snapshot
// restore) made the grace subtraction negative, keeping the leak live —
// skipping hydration and timing out same-name pushes — until wall time
// caught up with the marker. The observation must clamp the marker to
// now, restarting the grace at the anomaly's discovery, so the leak
// still ages out on observed time.
func TestPushHolds_FutureReapMarkerClampedNotPinned(t *testing.T) {
	dir := t.TempDir()
	hold := filepath.Join(dir, "push-future-marker")
	if err := os.WriteFile(hold, []byte("refs/heads/x\n"), 0o600); err != nil {
		t.Fatalf("write hold: %v", err)
	}
	past := time.Now().Add(-2 * pushHoldTTL)
	if err := os.Chtimes(hold, past, past); err != nil {
		t.Fatalf("chtimes hold: %v", err)
	}
	reap := hold + pushHoldReapSuffix
	if err := os.WriteFile(reap, nil, 0o600); err != nil {
		t.Fatalf("write reap marker: %v", err)
	}
	future := time.Now().Add(time.Hour)
	if err := os.Chtimes(reap, future, future); err != nil {
		t.Fatalf("chtimes marker: %v", err)
	}
	info, err := os.Stat(hold)
	if err != nil {
		t.Fatalf("stat hold: %v", err)
	}
	// The anomaly observation stays live (a single wall read is never
	// trusted) but must clamp the marker back to now.
	if !pushHoldLive(hold, info.ModTime()) {
		t.Fatal("anomaly observation must keep the hold live")
	}
	ri, err := os.Stat(reap)
	if err != nil {
		t.Fatalf("stat marker: %v", err)
	}
	if ri.ModTime().After(time.Now().Add(time.Minute)) {
		t.Fatalf("future reap marker was not clamped: %v", ri.ModTime())
	}
	// With the marker back on observed time, the untouched leak ages out
	// after the grace like any other.
	markerPast := time.Now().Add(-pushHoldTTL/2 - time.Minute)
	if err := os.Chtimes(reap, markerPast, markerPast); err != nil {
		t.Fatalf("chtimes marker: %v", err)
	}
	if pushHoldLive(hold, info.ModTime()) {
		t.Fatal("aged leak must expire after the reap grace")
	}
}

// TestPushHolds_RefresherTouchDuringVerdictKeepsHoldLive (Codex round-30
// P2 on #173): pushHoldLive's mtime argument comes from the caller's
// earlier stat. A delayed refresher (a second suspend, a long scheduler
// pause) can touch the hold between that observation and the expiry
// verdict; expiring the now-fresh hold would let hydration touch its
// in-flight refs and the mint sweep delete a file the refresher can only
// Chtimes. The verdict must re-stat and stay live on any owner touch.
func TestPushHolds_RefresherTouchDuringVerdictKeepsHoldLive(t *testing.T) {
	dir := t.TempDir()
	hold := filepath.Join(dir, "push-stale-observation")
	if err := os.WriteFile(hold, []byte("refs/heads/x\n"), 0o600); err != nil {
		t.Fatalf("write hold: %v", err)
	}
	stale := time.Now().Add(-2 * pushHoldTTL)
	if err := os.Chtimes(hold, stale, stale); err != nil {
		t.Fatalf("chtimes hold: %v", err)
	}
	reap := hold + pushHoldReapSuffix
	if err := os.WriteFile(reap, nil, 0o600); err != nil {
		t.Fatalf("write reap marker: %v", err)
	}
	markerPast := time.Now().Add(-pushHoldTTL/2 - time.Minute)
	if err := os.Chtimes(reap, markerPast, markerPast); err != nil {
		t.Fatalf("chtimes marker: %v", err)
	}
	// The owner's refresher touches the hold AFTER the caller's stat but
	// BEFORE the verdict.
	now := time.Now()
	if err := os.Chtimes(hold, now, now); err != nil {
		t.Fatalf("refresher touch: %v", err)
	}
	if !pushHoldLive(hold, stale) {
		t.Fatal("a hold touched after the caller's observation must stay live")
	}
	// Same stale observation with NO owner touch still expires.
	if err := os.Chtimes(hold, stale, stale); err != nil {
		t.Fatalf("re-age hold: %v", err)
	}
	if pushHoldLive(hold, stale) {
		t.Fatal("an untouched leak must still expire")
	}
}

// TestClassifyReservation_ParsesPolicyOnce (Codex round-30 P2 on #173):
// budget-free classification runs once per command, and reparsing the
// policy claim per command multiplies an attacker-sized policy by the
// push's command count in CPU and allocations before any update hook
// runs. One (policyJSON, opsJSON) pair must cost one parse per process,
// not one per command.
func TestClassifyReservation_ParsesPolicyOnce(t *testing.T) {
	t.Setenv(EnvRefsPolicy, `[["refs/heads/noff",["no-force-push"]]]`)
	t.Setenv(EnvOps, "")
	sha := strings.Repeat("a", 40)
	sha2 := strings.Repeat("b", 40)
	refPolicyParses.Store(0)
	for i := 0; i < 100; i++ {
		if got := classifyReservation("r", "refs/heads/noff", sha, sha2); got != reservationJIT {
			t.Fatalf("classifyReservation = %v, want %v", got, reservationJIT)
		}
	}
	// At most one real parse: zero when an earlier test already cached
	// this exact env pair, one otherwise — never per-command.
	if got := refPolicyParses.Load(); got > 1 {
		t.Fatalf("policy parsed %d times across 100 classifications; want at most 1", got)
	}
}

// TestRunHook_AtomicUpdateDenialReleasesWholeHold (Codex round-27 P2 on
// #173): under --atomic one hook denial dooms the whole push — git aborts
// the local ref transaction and never runs post-receive, the primary
// release — so the denying hook must release the WHOLE hold. The handler
// fallback fires only after receive-pack drains its report to the client,
// a write a non-reading client can stall indefinitely while the handler
// refresher keeps the hold fresh: earlier commands' committed names would
// block every same-name and D/F-relative push until the client went away.
// A non-atomic denial keeps releasing only its own ref.
func TestRunHook_AtomicUpdateDenialReleasesWholeHold(t *testing.T) {
	sha := strings.Repeat("a", 40)
	sha2 := strings.Repeat("b", 40)
	t.Setenv(EnvRepo, "t-atomic-release")
	t.Setenv(EnvRefsPolicy, `[["refs/heads/b",["no-push"]]]`)
	seed := func(t *testing.T) string {
		holdPath := filepath.Join(t.TempDir(), pushHoldDir, "push-atomic")
		if err := os.MkdirAll(filepath.Dir(holdPath), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(holdPath, []byte("refs/heads/a\nrefs/heads/b\n"), 0o600); err != nil {
			t.Fatalf("write hold: %v", err)
		}
		t.Setenv(EnvPushHold, holdPath)
		return holdPath
	}

	t.Setenv(EnvAtomic, "1")
	hold := seed(t)
	if err := RunHook(context.Background(), []string{"update", "refs/heads/b", sha, sha2}); err == nil {
		t.Fatal("no-push policy must deny the update")
	}
	if _, err := os.Stat(hold); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("atomic denial must release the whole hold: %v", err)
	}

	t.Setenv(EnvAtomic, "")
	hold = seed(t)
	if err := RunHook(context.Background(), []string{"update", "refs/heads/b", sha, sha2}); err == nil {
		t.Fatal("no-push policy must deny the update")
	}
	b, err := os.ReadFile(hold)
	if err != nil {
		t.Fatalf("non-atomic denial must keep the peers' hold: %v", err)
	}
	if got := string(b); got != "refs/heads/a\n" {
		t.Fatalf("non-atomic denial must release only its own ref: %q", got)
	}
}

// TestReservePushHolds_PeerScanWorkBounded (Codex round-27 security P2 on
// #173): sidecar/JIT self-reservations are fresh processes, each
// streaming every live peer hold — without a push-wide allowance, two
// overlapping crafted pushes reread repository-scale bytes once per hook
// for the push's whole duration. The counter sidecar accrues each scan's
// opened bytes; once over the allowance, later self-reservations are
// denied retryably BEFORE scanning again, and every release site removes
// the counter with the hold.
func TestReservePushHolds_PeerScanWorkBounded(t *testing.T) {
	repoDir := t.TempDir()
	dir := filepath.Join(repoDir, pushHoldDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	var sb strings.Builder
	for i := 0; i < 100; i++ {
		fmt.Fprintf(&sb, "refs/heads/peer/%03d\n", i)
	}
	peer := filepath.Join(dir, "push-peer")
	if err := os.WriteFile(peer, []byte(sb.String()), 0o644); err != nil {
		t.Fatalf("write peer hold: %v", err)
	}
	oldMax := maxPushPeerScanBytes
	t.Cleanup(func() { maxPushPeerScanBytes = oldMax })

	// ZERO overshoot (Codex round-31 P2): a peer set larger than the
	// remaining allowance is denied from stats alone, BEFORE any read —
	// the previous check ran only against previously charged work, so
	// one scan could exceed the limit by the full peer set (parsed and
	// sorted under the reservation flock) before the budget had any
	// effect. Both the single-name and multi-name paths deny up front,
	// and neither charges the counter.
	maxPushPeerScanBytes = int64(sb.Len()) - 1
	hold := filepath.Join(dir, "push-self")
	err := reservePushHolds(context.Background(), hold, []string{"refs/heads/x"})
	if err == nil || !strings.Contains(err.Error(), "scan allowance") {
		t.Fatalf("over-allowance single-name scan must be denied up front, got %v", err)
	}
	err = reservePushHolds(context.Background(), hold, []string{"refs/heads/m1", "refs/heads/m2"})
	if err == nil || !strings.Contains(err.Error(), "scan allowance") {
		t.Fatalf("over-allowance multi-name scan must be denied up front, got %v", err)
	}
	if _, statErr := os.Stat(hold + pushHoldScanSuffix); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("denied-up-front reservations must charge nothing: %v", statErr)
	}

	// Within the allowance, scans run and ACCRUE: once the counter plus
	// the (unchanged) peer set exceed the budget, later self-reservations
	// are denied without reading the peers again.
	maxPushPeerScanBytes = int64(sb.Len()) + 8
	if err := reservePushHolds(context.Background(), hold, []string{"refs/heads/x"}); err != nil {
		t.Fatalf("first self-reservation: %v", err)
	}
	err = reservePushHolds(context.Background(), hold, []string{"refs/heads/y"})
	if err == nil || !strings.Contains(err.Error(), "scan allowance") {
		t.Fatalf("accrued-over-allowance reservation must be denied, got %v", err)
	}
	// The counter is a sidecar: hold readers must not treat it as a hold
	// file, and releases must remove it with the hold.
	if !isPushHoldSidecar("push-self" + pushHoldScanSuffix) {
		t.Fatal("scan-work counter must be classified as a hold sidecar")
	}
	if base, ok := pushHoldSidecarBase("push-self" + pushHoldScanSuffix); !ok || base != "push-self" {
		t.Fatalf("scan-work sidecar base = %q, %v", base, ok)
	}
	removePushHoldFiles(hold)
	if _, statErr := os.Stat(hold + pushHoldScanSuffix); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("release must remove the scan-work counter: %v", statErr)
	}
	// The MULTI-name path (every compact ≥2-command push is a fresh
	// pre-receive process whose first load parses the whole peer set)
	// charges the same allowance (Codex round-29 security P2) — it was
	// initially budgeted only for len(names)==1.
	hold2 := filepath.Join(dir, "push-self2")
	maxPushPeerScanBytes = 4 * int64(sb.Len())
	if err := reservePushHolds(context.Background(), hold2, []string{"refs/heads/m1", "refs/heads/m2"}); err != nil {
		t.Fatalf("first multi-name reservation: %v", err)
	}
	maxPushPeerScanBytes = int64(sb.Len()) + 8
	err = reservePushHolds(context.Background(), hold2, []string{"refs/heads/m3", "refs/heads/m4"})
	if err == nil || !strings.Contains(err.Error(), "scan allowance") {
		t.Fatalf("over-allowance multi-name reservation must be denied, got %v", err)
	}
}

// TestPushHoldRefresh_FollowsInodeAcrossRename (Codex round-31 P2 on
// #173): the expiry reaper quarantines an expired-verdict hold by RENAME
// and revalidates the quarantined INODE before unlinking, so a live
// push's refresher must touch the inode, not the path — a path-based
// Chtimes hits ENOENT during quarantine, the touch is lost, and the
// reaper deletes a live hold its refresher can never recreate.
func TestPushHoldRefresh_FollowsInodeAcrossRename(t *testing.T) {
	oldTTL := pushHoldTTL
	pushHoldTTL = 200 * time.Millisecond
	t.Cleanup(func() { pushHoldTTL = oldTTL })
	dir := t.TempDir()
	hold := filepath.Join(dir, "push-fd")
	if err := os.WriteFile(hold, []byte("refs/heads/x\n"), 0o600); err != nil {
		t.Fatalf("write hold: %v", err)
	}
	stop := startPushHoldRefresh(hold)
	defer stop()
	quarantine := hold + ".reaping"
	if err := os.Rename(hold, quarantine); err != nil {
		t.Fatalf("rename: %v", err)
	}
	past := time.Now().Add(-time.Hour)
	if err := os.Chtimes(quarantine, past, past); err != nil {
		t.Fatalf("age quarantine: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		qi, err := os.Stat(quarantine)
		if err != nil {
			t.Fatalf("stat quarantine: %v", err)
		}
		if qi.ModTime().After(past.Add(time.Minute)) {
			return // a refresher touch landed on the renamed inode
		}
		if time.Now().After(deadline) {
			t.Fatal("refresher touches do not follow the inode across a rename")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestReapExpiredPushHold_RestoresLockedHold (Codex round-32 P2 on
// #173): the reaper must not unlink a quarantined hold whose owner is
// still alive even when the mtime revalidation matches — a resumed
// refresher's fd touch could land between the stat and the unlink. Every
// live refresher holds a SHARED flock on the hold's inode, and the
// reaper deletes only after winning an EXCLUSIVE non-blocking flock, so
// the kernel arbitrates the race with no timing window at all.
func TestReapExpiredPushHold_RestoresLockedHold(t *testing.T) {
	dir := t.TempDir()
	hold := filepath.Join(dir, "push-locked")
	if err := os.WriteFile(hold, []byte("refs/heads/x\n"), 0o600); err != nil {
		t.Fatalf("write hold: %v", err)
	}
	old := time.Now().Add(-2 * pushHoldTTL)
	if err := os.Chtimes(hold, old, old); err != nil {
		t.Fatalf("age hold: %v", err)
	}
	info, err := os.Stat(hold)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	f, err := os.OpenFile(hold, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_SH|syscall.LOCK_NB); err != nil {
		t.Fatalf("flock: %v", err)
	}
	// mtime matches the observation exactly, yet the live owner's shared
	// lock must veto the unlink.
	reapExpiredPushHold(hold, info.ModTime())
	if _, err := os.Stat(hold); err != nil {
		t.Fatalf("locked hold must survive the reap at its original path: %v", err)
	}
	// Released lock: the same reap now completes.
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatalf("unlock: %v", err)
	}
	reapExpiredPushHold(hold, info.ModTime())
	if _, err := os.Stat(hold); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("unlocked untouched leak must be unlinked")
	}
}

// TestReapExpiredPushHold_ReservationDuringQuarantineSurvives (Codex
// round-33 P1 on #173/#186): while a live hold sits in quarantine, a JIT
// or sidecar hook's reservePushHolds can recreate the hold path and
// append a NEW reservation — and the reaper's restore rename would
// silently replace that file, discarding a reservation whose hook then
// commits its CAS believing the ref is held, reopening the hydration
// race. The reap must run under the same reservation flock the append
// path serializes on, so a mid-quarantine reservation lands strictly
// before the quarantine or strictly after the restore — never underneath
// it.
func TestReapExpiredPushHold_ReservationDuringQuarantineSurvives(t *testing.T) {
	repoDir := t.TempDir()
	dir := filepath.Join(repoDir, pushHoldDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	hold := filepath.Join(dir, "push-quarantine-race")
	if err := os.WriteFile(hold, []byte("refs/heads/x\n"), 0o600); err != nil {
		t.Fatalf("write hold: %v", err)
	}
	old := time.Now().Add(-2 * pushHoldTTL)
	if err := os.Chtimes(hold, old, old); err != nil {
		t.Fatalf("age hold: %v", err)
	}
	info, err := os.Stat(hold)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	// A live owner: the refresher's shared liveness flock vetoes the
	// unlink, forcing the reaper down the RESTORE path.
	f, err := os.OpenFile(hold, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_SH|syscall.LOCK_NB); err != nil {
		t.Fatalf("flock: %v", err)
	}
	// Mid-quarantine, the push's own hook reserves an additional name.
	// Unfixed, the reservation recreates the hold path and completes
	// immediately; fixed, it blocks on the reservation flock until the
	// reap finishes (the 200ms timeout lets the reaper proceed).
	done := make(chan error, 1)
	testHookReapAfterQuarantine = func() {
		go func() { done <- reservePushHolds(context.Background(), hold, []string{"refs/heads/new"}) }()
		select {
		case err := <-done:
			done <- err
		case <-time.After(200 * time.Millisecond):
		}
	}
	defer func() { testHookReapAfterQuarantine = nil }()
	reapExpiredPushHold(hold, info.ModTime())
	if err := <-done; err != nil {
		t.Fatalf("mid-quarantine reservation: %v", err)
	}
	b, err := os.ReadFile(hold)
	if err != nil {
		t.Fatalf("read hold: %v", err)
	}
	for _, name := range []string{"refs/heads/x\n", "refs/heads/new\n"} {
		if !strings.Contains(string(b), name) {
			t.Fatalf("restore discarded a reservation: hold = %q, want %q", b, name)
		}
	}
	if _, err := os.Stat(hold + ".reaping"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("quarantine must not linger after a restore")
	}
}

// TestPushHoldRefresh_HoldsLivenessLock (Codex round-32 P2 on #173): the
// refresher must hold the shared liveness flock the reaper's exclusive
// lock checks against — without it the kernel gate above protects
// nothing.
func TestPushHoldRefresh_HoldsLivenessLock(t *testing.T) {
	dir := t.TempDir()
	hold := filepath.Join(dir, "push-lock-beacon")
	if err := os.WriteFile(hold, []byte("refs/heads/x\n"), 0o600); err != nil {
		t.Fatalf("write hold: %v", err)
	}
	stop := startPushHoldRefresh(hold)
	f, err := os.OpenFile(hold, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
		t.Fatal("exclusive flock must fail while the refresher lives")
	}
	stop()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatalf("exclusive flock must succeed after the refresher stops: %v", err)
	}
}

// TestLivePushHoldsKey_TransientHoldChangesKey (Codex round-32 P2 on
// #173): equal per-entry endpoint snapshots are ABA-blind to a hold
// reserved and released entirely between them — a sync that skipped that
// transient hold's ref would publish a held marker vouching for a hold
// set that was never stable. Every hold mutator rewrites the lifecycle
// epoch file with fresh randomness (a directory mtime cannot serve: a
// full lifecycle inside one coarse clock tick leaves it unchanged), so
// the transient lifecycle changes the key.
func TestLivePushHoldsKey_TransientHoldChangesKey(t *testing.T) {
	repoDir := t.TempDir()
	dir := filepath.Join(repoDir, pushHoldDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "push-steady"), []byte("refs/heads/x\n"), 0o600); err != nil {
		t.Fatalf("write hold: %v", err)
	}
	key1, ok := livePushHoldsKey(repoDir)
	if !ok {
		t.Fatal("livePushHoldsKey failed")
	}
	transient := filepath.Join(dir, "push-transient")
	if err := reservePushHolds(context.Background(), transient, []string{"refs/heads/y"}); err != nil {
		t.Fatalf("reserve transient hold: %v", err)
	}
	removePushHoldFiles(transient)
	key2, ok := livePushHoldsKey(repoDir)
	if !ok {
		t.Fatal("livePushHoldsKey failed")
	}
	if key1 == key2 {
		t.Fatal("a transient hold lifecycle between snapshots must change the key")
	}
	// With no further lifecycle transitions the key is stable — the
	// marker machinery depends on unchanged sets fingerprinting equal.
	key3, ok := livePushHoldsKey(repoDir)
	if !ok || key3 != key2 {
		t.Fatalf("key unstable without lifecycle transitions: %q vs %q", key3, key2)
	}
}

// TestLivePushHoldsKey_MidSnapshotTransientChangesKey (Codex round-33 P2
// on #173/#186): one snapshot's directory listing and epoch read are two
// reads with a gap. Listing FIRST let a transient hold slip between them
// — the pre-scan lists the old set yet reads the post-create epoch, and
// when the release's explicitly best-effort bump is lost, the post-scan
// reproduces exactly that mixed-generation key: a held marker publishes
// for a hold set that changed mid-scan. Reading the epoch BEFORE the
// listing closes it — the fail-closed create bump then lands strictly
// after the pre-scan's epoch read, so the bracketing keys differ even
// with the release bump lost.
func TestLivePushHoldsKey_MidSnapshotTransientChangesKey(t *testing.T) {
	repoDir := t.TempDir()
	dir := filepath.Join(repoDir, pushHoldDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := bumpPushHoldEpoch(dir); err != nil {
		t.Fatalf("seed epoch: %v", err)
	}
	testHookHoldsKeyMidSnapshot = func() {
		transient := filepath.Join(dir, "push-transient")
		if err := reservePushHolds(context.Background(), transient, []string{"refs/heads/y"}); err != nil {
			t.Errorf("reserve transient hold: %v", err)
		}
		// Release with the best-effort epoch bump LOST (delayed past the
		// post-scan or failed): a raw unlink, exactly what the reviewer's
		// trace requires.
		if err := os.Remove(transient); err != nil {
			t.Errorf("remove transient hold: %v", err)
		}
	}
	key1, ok1 := livePushHoldsKey(repoDir)
	testHookHoldsKeyMidSnapshot = nil
	key2, ok2 := livePushHoldsKey(repoDir)
	if !ok2 {
		t.Fatal("livePushHoldsKey must succeed on a quiet hold set")
	}
	// Safe outcomes for the straddled snapshot: refused outright (the
	// round-35 seqlock detects the mid-snapshot bump) or a key that can
	// never equal the stable one. Equal keys are the bug.
	if ok1 && key1 == key2 {
		t.Fatal("a transient lifecycle inside one snapshot must change the key")
	}
}

// TestRemovePushHoldFiles_NoopReleaseKeepsKey (Codex round-33 security
// P2 on #173/#186): every receive-pack mints a hold PATH, but rejected or
// malformed RPCs may never create the FILE — and every such request still
// runs the release. An unconditional epoch bump there let each tiny
// denied push invalidate the held-generation fast path, re-buying the
// full pack listing and ref paging under the hydrate lock for a live
// hold set that never changed: repository-scale work amplification from
// requests that did no work. A never-created hold was never in any
// fingerprint listing (and sidecars are excluded from it), so a no-op
// release must leave the key untouched; a REAL removal must keep
// changing it.
func TestRemovePushHoldFiles_NoopReleaseKeepsKey(t *testing.T) {
	repoDir := t.TempDir()
	dir := filepath.Join(repoDir, pushHoldDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := bumpPushHoldEpoch(dir); err != nil {
		t.Fatalf("seed epoch: %v", err)
	}
	key1, ok := livePushHoldsKey(repoDir)
	if !ok {
		t.Fatal("livePushHoldsKey failed")
	}
	removePushHoldFiles(filepath.Join(dir, "push-never-created"))
	key2, ok := livePushHoldsKey(repoDir)
	if !ok {
		t.Fatal("livePushHoldsKey failed")
	}
	if key1 != key2 {
		t.Fatal("releasing a never-created hold must not change the holds key")
	}
	// Non-vacuity: an actual removal still bumps — the ABA property the
	// epoch exists for (see TestLivePushHoldsKey_TransientHoldChangesKey).
	hold := filepath.Join(dir, "push-real")
	if err := os.WriteFile(hold, []byte("refs/heads/x\n"), 0o600); err != nil {
		t.Fatalf("write hold: %v", err)
	}
	epochBefore, err := os.ReadFile(dir + pushHoldEpochSuffix)
	if err != nil {
		t.Fatalf("read epoch: %v", err)
	}
	removePushHoldFiles(hold)
	epochAfter, err := os.ReadFile(dir + pushHoldEpochSuffix)
	if err != nil {
		t.Fatalf("read epoch: %v", err)
	}
	if string(epochBefore) == string(epochAfter) {
		t.Fatal("removing an existing hold must bump the epoch")
	}
}

// TestRemovePushHoldFiles_NoRemovalWithoutEpochBump (Codex round-34 P2 on
// #173/#186): appendPushHolds fails a push closed when its epoch bump
// fails, but the hold FILE is already visible by then — and the error
// cleanup used to remove it with its own bump failing for the same
// cause. A hydration bracketing that transient lifecycle would see
// identical pre/post keys (same epoch, same empty listing) despite
// having skipped the held ref mid-scan, publishing a stale held marker.
// The invariant: a hold file never transitions visible→absent without a
// SUCCESSFUL epoch bump — on failure the file stays, and while it lives
// it is part of every fingerprint listing.
func TestRemovePushHoldFiles_NoRemovalWithoutEpochBump(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("directory write permissions do not bind root")
	}
	repoDir := t.TempDir()
	dir := filepath.Join(repoDir, pushHoldDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := bumpPushHoldEpoch(dir); err != nil {
		t.Fatalf("seed epoch: %v", err)
	}
	key1, ok := livePushHoldsKey(repoDir)
	if !ok {
		t.Fatal("livePushHoldsKey failed")
	}
	// The epoch is a SIBLING of the hold directory: an unwritable repo
	// root with a writable hold directory is exactly the split the
	// reviewer's trace needs (bump fails, hold write succeeds).
	if err := os.Chmod(repoDir, 0o555); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(repoDir, 0o755) })
	hold := filepath.Join(dir, "push-bumpless")
	if err := appendPushHolds(hold, []string{"refs/heads/x"}); err == nil {
		t.Fatal("appendPushHolds must fail closed when the epoch bump fails")
	}
	if _, err := os.Stat(hold); err != nil {
		t.Fatalf("hold must be visible after the failed reservation: %v", err)
	}
	removePushHoldFiles(hold) // the pre-receive error cleanup
	if _, err := os.Stat(hold); err != nil {
		t.Fatalf("a hold must not vanish without a successful epoch bump: %v", err)
	}
	key2, ok := livePushHoldsKey(repoDir)
	if !ok {
		t.Fatal("livePushHoldsKey failed")
	}
	if key1 == key2 {
		t.Fatal("the transient lifecycle must be visible in the fingerprint")
	}
	// Once the epoch can record the transition again, the release goes
	// through and changes the key once more.
	if err := os.Chmod(repoDir, 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	removePushHoldFiles(hold)
	if _, err := os.Stat(hold); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("release must remove the hold once the bump succeeds: %v", err)
	}
	key3, ok := livePushHoldsKey(repoDir)
	if !ok {
		t.Fatal("livePushHoldsKey failed")
	}
	if key3 == key2 {
		t.Fatal("the successful release must change the key")
	}
}

// TestReapExpiredPushHold_NoUnlinkWithoutEpochBump (Codex round-34 P2 on
// #173/#186): the expiry reap enforces the same rule as the release —
// bump BEFORE unlink, restore the quarantined hold when the epoch cannot
// record the transition. A later sweep retries once the epoch is
// writable again.
func TestReapExpiredPushHold_NoUnlinkWithoutEpochBump(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("directory write permissions do not bind root")
	}
	repoDir := t.TempDir()
	dir := filepath.Join(repoDir, pushHoldDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := bumpPushHoldEpoch(dir); err != nil {
		t.Fatalf("seed epoch: %v", err)
	}
	// Pre-create the reserve lock: the reaper serializes with reservers
	// (round-33 P1), and O_CREATE under a read-only repo root would
	// otherwise short-circuit the reap before the behavior under test.
	if f, err := os.Create(dir + ".reserve.lock"); err != nil {
		t.Fatalf("create reserve lock: %v", err)
	} else {
		_ = f.Close()
	}
	hold := filepath.Join(dir, "push-bumpless-reap")
	if err := os.WriteFile(hold, []byte("refs/heads/x\n"), 0o600); err != nil {
		t.Fatalf("write hold: %v", err)
	}
	expireHold(t, hold)
	info, err := os.Stat(hold)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if err := os.Chmod(repoDir, 0o555); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(repoDir, 0o755) })
	reapExpiredPushHold(hold, info.ModTime())
	if _, err := os.Stat(hold); err != nil {
		t.Fatalf("an expired hold must survive a reap whose epoch bump fails: %v", err)
	}
}

// TestPushHoldRefresh_RetriesLostLivenessLock (Codex round-34 P2 on
// #173/#186): the refresher's shared flock is best-effort per ATTEMPT but
// must not be best-effort per LIFETIME. If the reaper's exclusive lock
// wins the race against a freshly opened refresher fd, ignoring the
// failed LOCK_SH would leave the refresher touching an unprotected fd
// forever — the reaper's stat can then precede the Futimes and unlink a
// live hold. A failed shared lock must close the descriptor and retry on
// a later touch, re-establishing the liveness beacon.
func TestPushHoldRefresh_RetriesLostLivenessLock(t *testing.T) {
	oldTTL := pushHoldTTL
	pushHoldTTL = 200 * time.Millisecond
	t.Cleanup(func() { pushHoldTTL = oldTTL })
	dir := t.TempDir()
	hold := filepath.Join(dir, "push-lock-retry")
	if err := os.WriteFile(hold, []byte("refs/heads/x\n"), 0o600); err != nil {
		t.Fatalf("write hold: %v", err)
	}
	// A reaper mid-verdict: exclusive lock held while the refresher's
	// first touch opens its fd, so the refresher's LOCK_SH loses.
	blocker, err := os.OpenFile(hold, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := syscall.Flock(int(blocker.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatalf("flock: %v", err)
	}
	stop := startPushHoldRefresh(hold)
	defer stop()
	_ = blocker.Close() // reaper done (hold restored); lock released
	// The refresher must re-acquire the shared lock on a later touch:
	// an exclusive probe then fails while the refresher lives.
	deadline := time.Now().Add(3 * time.Second)
	for {
		probe, err := os.OpenFile(hold, os.O_WRONLY, 0)
		if err != nil {
			t.Fatalf("open probe: %v", err)
		}
		lockErr := syscall.Flock(int(probe.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		_ = probe.Close()
		if lockErr != nil {
			return // shared lock re-established — refresher is protected
		}
		if time.Now().After(deadline) {
			t.Fatal("refresher never re-acquired its liveness lock after losing the race")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestLivePushHoldsKey_MidSnapshotLifecycleDetectedAfterListing (Codex
// round-35 P2 on #173/#186): reading the epoch only BEFORE the listing
// still lets the post-scan mix generations — a reservation can write its
// hold after this key's epoch read (its create bump still pending), the
// ref sync skips the held ref, and by the time this key lists the
// directory the push has failed its CAS and cleanup has bumped and
// removed the hold: the listing is empty and the already-read epoch is
// the OLD one, reproducing the pre-scan key. The key must re-read the
// epoch after the listing and refuse the snapshot (ok=false) when the
// two reads differ — callers treat that as "neither publish nor honor",
// the conservative direction.
func TestLivePushHoldsKey_MidSnapshotLifecycleDetectedAfterListing(t *testing.T) {
	repoDir := t.TempDir()
	dir := filepath.Join(repoDir, pushHoldDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := bumpPushHoldEpoch(dir); err != nil {
		t.Fatalf("seed epoch: %v", err)
	}
	key1, ok := livePushHoldsKey(repoDir)
	if !ok {
		t.Fatal("livePushHoldsKey failed")
	}
	// Between the post-scan's epoch read and its listing: the transient
	// hold's full lifecycle completes, late create bump and cleanup bump
	// included.
	testHookHoldsKeyMidSnapshot = func() {
		hold := filepath.Join(dir, "push-transient-late-bump")
		if err := os.WriteFile(hold, []byte("refs/heads/y\n"), 0o600); err != nil {
			t.Errorf("write hold: %v", err)
		}
		if err := bumpPushHoldEpoch(dir); err != nil {
			t.Errorf("late create bump: %v", err)
		}
		if err := os.Remove(hold); err != nil {
			t.Errorf("remove hold: %v", err)
		}
		if err := bumpPushHoldEpoch(dir); err != nil {
			t.Errorf("cleanup bump: %v", err)
		}
	}
	key2, ok := livePushHoldsKey(repoDir)
	testHookHoldsKeyMidSnapshot = nil
	if ok && key2 == key1 {
		t.Fatal("a lifecycle between the epoch read and the listing must not reproduce the old key")
	}
	// And once the set is quiet again, the key must be obtainable.
	if _, ok := livePushHoldsKey(repoDir); !ok {
		t.Fatal("livePushHoldsKey must succeed on a quiet hold set")
	}
}

// TestBumpPushHoldEpoch_RemovesTempOnWriteFailure (Codex round-35 P2 on
// #173/#186): the epoch temporaries are SIBLINGS of the hold directory,
// invisible to every sweep, and each attempt picks a fresh random name —
// a temp left behind by a failed write (ENOSPC/EIO mid-write or close)
// would accumulate forever, worsening the very storage condition that
// stranded it. Every error path must remove the temp it created.
func TestBumpPushHoldEpoch_RemovesTempOnWriteFailure(t *testing.T) {
	repoDir := t.TempDir()
	dir := filepath.Join(repoDir, pushHoldDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	oldWrite := writeEpochFile
	writeEpochFile = func(name string, data []byte, perm os.FileMode) error {
		// A partial write followed by the error — what a full volume
		// leaves behind.
		_ = os.WriteFile(name, data[:4], perm)
		return errors.New("injected: no space left on device")
	}
	t.Cleanup(func() { writeEpochFile = oldWrite })
	if err := bumpPushHoldEpoch(dir); err == nil {
		t.Fatal("bump must fail when the temp write fails")
	}
	entries, err := os.ReadDir(repoDir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	for _, e := range entries {
		if strings.Contains(e.Name(), pushHoldEpochSuffix+".tmp-") {
			t.Fatalf("failed bump left temp file behind: %s", e.Name())
		}
	}
}

// TestWritePushVerifiedVerdicts_SurvivesSweepWithoutReservations (Codex
// round-35 P2 on #173/#186): an all-JIT push reserves no names, so no
// hold file exists — persisting its pack-path verdicts used to write a
// PARENTLESS `.verified` sidecar. Orphan sidecars age out on a single
// wall observation, so a host suspend or forward clock step followed by
// any other push's mint sweep deleted the verdicts before the first
// update hook consumed them — re-charging the walk budget for every
// evaluation, the exact exhaustion the persistence exists to prevent.
// The sidecar must be anchored to a base hold file that lives on the
// refresher's clock with two-observation expiry.
func TestWritePushVerifiedVerdicts_SurvivesSweepWithoutReservations(t *testing.T) {
	repoDir := t.TempDir()
	dir := filepath.Join(repoDir, pushHoldDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	hold := filepath.Join(dir, "push-jit-only")
	oldSHA, newSHA := strings.Repeat("0", 40), strings.Repeat("1", 40)
	digests := [][sha256.Size]byte{pushVerifiedDigest("refs/heads/a", oldSHA, newSHA)}
	if err := writePushVerifiedVerdicts(hold, digests); err != nil {
		t.Fatalf("write verdicts: %v", err)
	}
	// A suspend/forward clock step: every file of the push looks ancient
	// at once (ENOENT Chtimes are no-ops for whichever files each
	// implementation maintains).
	old := time.Now().Add(-2 * pushHoldTTL)
	_ = os.Chtimes(hold, old, old)
	_ = os.Chtimes(hold+pushHoldVerifiedSuffix, old, old)
	sweepExpiredPushHolds(dir) // another push's mint sweep
	if !pushVerifiedVerdict(hold, "refs/heads/a", oldSHA, newSHA) {
		t.Fatal("verified verdicts must survive one sweep observation after a clock anomaly")
	}
}

// TestRemovePushHolds_SurvivesConcurrentReapRestore (Codex round-36 P2
// on #173/#186): the partial rewrite (read → reduced temp → rename into
// holdPath) can straddle a reaper's quarantine window — the read lands
// before the quarantine rename, the replacement rename lands during it,
// and the reaper's restore (a live refresher's shared flock vetoed the
// unlink) then renames the OLD full file over the replacement, silently
// resurrecting every released name until receive-pack exits. The rewrite
// must run under the same reservation flock as the reaper's window.
func TestRemovePushHolds_SurvivesConcurrentReapRestore(t *testing.T) {
	repoDir := t.TempDir()
	dir := filepath.Join(repoDir, pushHoldDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	hold := filepath.Join(dir, "push-rewrite-race")
	if err := os.WriteFile(hold, []byte("refs/heads/a\nrefs/heads/b\n"), 0o600); err != nil {
		t.Fatalf("write hold: %v", err)
	}
	expireHold(t, hold)
	info, err := os.Stat(hold)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	// The live refresher whose shared flock forces the reaper's restore.
	f, err := os.OpenFile(hold, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_SH|syscall.LOCK_NB); err != nil {
		t.Fatalf("flock: %v", err)
	}
	// Orchestration: mid-rewrite (after the read, before the replacement
	// rename), a sweep quarantines the hold and pauses; the rewrite's
	// rename lands during quarantine; the reaper then restores. Fixed,
	// the rewrite holds the reservation flock, so the reap cannot even
	// quarantine until the rewrite completes.
	gate := make(chan struct{})
	reapDone := make(chan struct{})
	var closeGate sync.Once
	testHookReapAfterQuarantine = func() { <-gate }
	testHookRemoveHoldsPreRename = func() {
		go func() {
			defer close(reapDone)
			reapExpiredPushHold(hold, info.ModTime())
		}()
		deadline := time.Now().Add(500 * time.Millisecond)
		for {
			if _, err := os.Stat(hold + ".reaping"); err == nil || time.Now().After(deadline) {
				return // quarantined (unfixed) or reap was excluded (fixed)
			}
			time.Sleep(time.Millisecond)
		}
	}
	defer func() {
		testHookRemoveHoldsPreRename = nil
		testHookReapAfterQuarantine = nil
		closeGate.Do(func() { close(gate) })
	}()
	if err := removePushHolds(hold, []string{"refs/heads/a"}); err != nil {
		t.Fatalf("removePushHolds: %v", err)
	}
	closeGate.Do(func() { close(gate) })
	<-reapDone
	b, err := os.ReadFile(hold)
	if err != nil {
		t.Fatalf("read hold: %v", err)
	}
	if strings.Contains(string(b), "refs/heads/a") {
		t.Fatalf("reap restore resurrected a released name: %q", b)
	}
	if !strings.Contains(string(b), "refs/heads/b") {
		t.Fatalf("kept name lost: %q", b)
	}
}

// TestRemovePushHolds_RetainsContentsWhenBumpFails (Codex round-36 P2 on
// #173/#186): the partial rewrite is a hold-set mutation like any other —
// replacing the contents with the epoch bump failing (and ignored) lets
// a JIT append + error-path rewrite-back fingerprint identically under
// coarse timestamp granularity. Mirror the removal invariant: bump
// BEFORE the replacement rename, retain the old contents when the epoch
// cannot record the transition.
func TestRemovePushHolds_RetainsContentsWhenBumpFails(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("directory write permissions do not bind root")
	}
	repoDir := t.TempDir()
	dir := filepath.Join(repoDir, pushHoldDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := bumpPushHoldEpoch(dir); err != nil {
		t.Fatalf("seed epoch: %v", err)
	}
	if f, err := os.Create(dir + ".reserve.lock"); err != nil {
		t.Fatalf("create reserve lock: %v", err)
	} else {
		_ = f.Close()
	}
	hold := filepath.Join(dir, "push-bumpless-rewrite")
	want := "refs/heads/a\nrefs/heads/b\n"
	if err := os.WriteFile(hold, []byte(want), 0o600); err != nil {
		t.Fatalf("write hold: %v", err)
	}
	if err := os.Chmod(repoDir, 0o555); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(repoDir, 0o755) })
	if err := removePushHolds(hold, []string{"refs/heads/a"}); err == nil {
		t.Fatal("removePushHolds must fail when the epoch bump fails")
	}
	b, err := os.ReadFile(hold)
	if err != nil {
		t.Fatalf("read hold: %v", err)
	}
	if string(b) != want {
		t.Fatalf("contents must be retained when the bump fails: %q", b)
	}
}

// TestPushHoldRefresh_OpposesConcurrentReap (Codex round-36 P2 on
// #173/#186): closing the descriptor on a lost shared-lock attempt
// (round 34) still leaves the CURRENT reap unopposed — a reaper can
// quarantine and win LOCK_EX between the refresher's open and its
// LOCK_SH, and the refresher's fallback path touch then hits ENOENT
// while the reaper unlinks a live reservation. Establishing the fd and
// the shared lock must be atomic with respect to reapers: it runs under
// the same reservation flock as the reap window, so a reap either
// finished before the refresher's open or cannot start until the shared
// lock is held.
func TestPushHoldRefresh_OpposesConcurrentReap(t *testing.T) {
	oldTTL := pushHoldTTL
	pushHoldTTL = 200 * time.Millisecond
	t.Cleanup(func() { pushHoldTTL = oldTTL })
	repoDir := t.TempDir()
	dir := filepath.Join(repoDir, pushHoldDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	hold := filepath.Join(dir, "push-establish-race")
	if err := os.WriteFile(hold, []byte("refs/heads/x\n"), 0o600); err != nil {
		t.Fatalf("write hold: %v", err)
	}
	expireHold(t, hold)
	info, err := os.Stat(hold)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	fired := false
	testHookRefreshBeforeLock = func() {
		if fired {
			return
		}
		fired = true
		// The expiry sweep runs exactly between the refresher's open and
		// its LOCK_SH.
		reapExpiredPushHold(hold, info.ModTime())
	}
	defer func() { testHookRefreshBeforeLock = nil }()
	stop := startPushHoldRefresh(hold) // first touch is synchronous
	defer stop()
	if _, err := os.Stat(hold); err != nil {
		t.Fatalf("an actively refreshed hold was reaped mid-establishment: %v", err)
	}
}

// TestRemovePushHoldFiles_SerializesWithQuarantine (Codex round-36
// security P2 on #173/#186): a release racing a reaper's quarantine
// window sees the hold path ABSENT, removes only the sidecars, and
// returns having released nothing — the reaper then restores the hold
// (the handler refresher's shared flock vetoed the unlink), resurrecting
// it AFTER post-receive completed. The handler refresher keeps touching
// it while a non-reading client stalls the response copy, blocking
// same-name and D/F-relative pushes indefinitely. Releases must
// serialize with the quarantine window on the reservation flock: they
// then run strictly before it (normal removal) or strictly after it
// (removal of the restored hold, or a no-op if the reaper unlinked).
func TestRemovePushHoldFiles_SerializesWithQuarantine(t *testing.T) {
	repoDir := t.TempDir()
	dir := filepath.Join(repoDir, pushHoldDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	hold := filepath.Join(dir, "push-release-race")
	if err := os.WriteFile(hold, []byte("refs/heads/x\n"), 0o600); err != nil {
		t.Fatalf("write hold: %v", err)
	}
	expireHold(t, hold)
	info, err := os.Stat(hold)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	// The handler refresher whose shared flock forces the restore path.
	f, err := os.OpenFile(hold, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_SH|syscall.LOCK_NB); err != nil {
		t.Fatalf("flock: %v", err)
	}
	// Post-receive's release fires mid-quarantine. Unfixed it completes
	// immediately (sidecars only); fixed it blocks on the reservation
	// flock until the reap window ends, then removes the restored hold.
	released := make(chan struct{})
	testHookReapAfterQuarantine = func() {
		go func() {
			defer close(released)
			removePushHoldFiles(hold)
		}()
		select {
		case <-released:
		case <-time.After(300 * time.Millisecond):
		}
	}
	defer func() { testHookReapAfterQuarantine = nil }()
	reapExpiredPushHold(hold, info.ModTime())
	<-released
	if _, err := os.Stat(hold); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a hold released during a quarantine window must stay released: %v", err)
	}
	if _, err := os.Stat(hold + ".reaping"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("quarantine must not linger")
	}
}

// TestReleaseDeniedPushHold_LinearWork (issue #186 finding 6): a
// non-atomic high-fanout push whose advertised bases all went stale has
// every update hook deny post-CAS-loss, and each denial used to rewrite
// the ENTIRE remaining hold file — O(N²) bytes of read+write I/O and
// allocation from an O(N) request, unbounded by the rate limiter or the
// push cap. Tombstone appends make each release O(name). Pinned by
// TotalAlloc delta (NOT AllocsPerRun — split-based parsing shares backing
// buffers and undercounts): 2000 releases over a 2000-name hold cost
// ~80MB quadratic, low single-digit MB linear. Coverage semantics are
// pinned alongside: released names must vanish from the readers' view.
func TestReleaseDeniedPushHold_LinearWork(t *testing.T) {
	repoDir := t.TempDir()
	dir := filepath.Join(repoDir, pushHoldDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	hold := filepath.Join(dir, "push-fanout")
	const n = 2000
	names := make([]string, n)
	var sb strings.Builder
	for i := range names {
		names[i] = fmt.Sprintf("refs/heads/fanout-%04d", i)
		sb.WriteString(names[i])
		sb.WriteByte('\n')
	}
	if err := os.WriteFile(hold, []byte(sb.String()), 0o600); err != nil {
		t.Fatalf("write hold: %v", err)
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for _, name := range names {
		if err := releaseDeniedPushHold(hold, name); err != nil {
			t.Fatalf("release %s: %v", name, err)
		}
	}
	runtime.ReadMemStats(&after)
	if delta := after.TotalAlloc - before.TotalAlloc; delta > 32<<20 {
		t.Fatalf("all-denied fanout release allocated %d bytes — quadratic rewrite behavior (want O(N) appends, < 32MiB)", delta)
	}
	// Non-vacuity + reader semantics: every released name is out of the
	// combined coverage view.
	set, err := readPushHolds(dir, "")
	if err != nil {
		t.Fatalf("readPushHolds: %v", err)
	}
	for _, name := range []string{names[0], names[n/2], names[n-1]} {
		if set.contains(name) {
			t.Fatalf("released name %s still covered", name)
		}
	}
}

// TestReapExpiredPushHold_RestoresTouchedHold (Codex round-31 P2 on
// #173): the expiry verdict and the removal linearize at the quarantine
// rename — an owner touch that preceded it is observed on the quarantined
// inode and restores the hold untouched; only a verifiably untouched leak
// is unlinked, together with its sidecars.
func TestReapExpiredPushHold_RestoresTouchedHold(t *testing.T) {
	dir := t.TempDir()
	hold := filepath.Join(dir, "push-reap-lin")
	if err := os.WriteFile(hold, []byte("refs/heads/x\n"), 0o600); err != nil {
		t.Fatalf("write hold: %v", err)
	}
	old := time.Now().Add(-2 * pushHoldTTL)
	// A refresher touch lands AFTER the sweep's expiry observation (old)
	// but before the reap: the hold must survive at its original path.
	now := time.Now()
	if err := os.Chtimes(hold, now, now); err != nil {
		t.Fatalf("touch hold: %v", err)
	}
	reapExpiredPushHold(hold, old)
	if _, err := os.Stat(hold); err != nil {
		t.Fatalf("touched hold must be restored at its original path: %v", err)
	}
	if _, err := os.Stat(hold + ".reaping"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("quarantine must not linger after a restore")
	}
	// An untouched leak is unlinked with its sidecars.
	if err := os.Chtimes(hold, old, old); err != nil {
		t.Fatalf("age hold: %v", err)
	}
	info, err := os.Stat(hold)
	if err != nil {
		t.Fatalf("stat hold: %v", err)
	}
	if err := os.WriteFile(hold+pushHoldScanSuffix, []byte("7"), 0o600); err != nil {
		t.Fatalf("write sidecar: %v", err)
	}
	reapExpiredPushHold(hold, info.ModTime())
	if _, err := os.Stat(hold); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("untouched leak must be unlinked")
	}
	if _, err := os.Stat(hold + pushHoldScanSuffix); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("reap must remove the leak's sidecars")
	}
}

// TestReservePushHolds_EmptySetSkipsPeerScan (Codex round-28 security P2
// on #173): a push whose every command was denied, sidecar-diverted, or
// JIT-deferred reserves NOTHING, yet the multi-name path parsed every
// live peer hold under the reservation flock before consulting the empty
// name list — repeated compact doomed pushes against a 100k-name hold
// bought repository-scale parses per push, work the single-name scan
// allowance never sees. A zero-name reservation conflicts with nothing:
// it must return before touching the lock or any peer file.
func TestReservePushHolds_EmptySetSkipsPeerScan(t *testing.T) {
	repoDir := t.TempDir()
	dir := filepath.Join(repoDir, pushHoldDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	peer := filepath.Join(dir, "push-peer")
	if err := os.WriteFile(peer, []byte("refs/heads/x\n"), 0o644); err != nil {
		t.Fatalf("write peer hold: %v", err)
	}
	hold := filepath.Join(dir, "push-self")
	if err := reservePushHolds(context.Background(), hold, nil); err != nil {
		t.Fatalf("empty reservation must be a no-op success: %v", err)
	}
	// The reserve lock is created lazily by the first real reservation;
	// an empty set must never get that far (nor scan a peer to charge the
	// allowance counter).
	if _, err := os.Stat(dir + ".reserve.lock"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("empty reservation took the reserve lock: %v", err)
	}
	if _, err := os.Stat(hold + pushHoldScanSuffix); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("empty reservation charged scan work: %v", err)
	}
	// A real reservation through the same path still works afterwards.
	if err := reservePushHolds(context.Background(), hold, []string{"refs/heads/y"}); err != nil {
		t.Fatalf("real reservation after the no-op: %v", err)
	}
}

// TestPeerHoldCovers_StreamsWithoutParsing (Codex round-25 security P2 on
// #173): every sidecar/JIT hook self-reservation runs in a fresh process,
// so its peer-conflict check must stream the peer holds — building the
// combined split+sorted set per hook turned a 1,024-command sidecar
// against a 100k-name peer hold into ~1,000 repository-scale parses under
// the reservation lock. Pins coverage-semantics parity with the set-based
// path and the allocation bound that proves no per-name parsing.
func TestPeerHoldCovers_StreamsWithoutParsing(t *testing.T) {
	repoDir := t.TempDir()
	dir := filepath.Join(repoDir, pushHoldDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	var sb strings.Builder
	for i := 0; i < 100_000; i++ {
		fmt.Fprintf(&sb, "refs/heads/b/%06d\n", i)
	}
	peer := filepath.Join(dir, "push-peer")
	if err := os.WriteFile(peer, []byte(sb.String()), 0o644); err != nil {
		t.Fatalf("write peer hold: %v", err)
	}
	for _, tc := range []struct {
		name string
		want bool
	}{
		{"refs/heads/b/000123", true},   // exact
		{"refs/heads/b", true},          // ancestor of held names
		{"refs/heads/b/000123/x", true}, // descendant of a held name
		{"refs/heads/b/0001", false},    // sibling prefix, no '/' boundary
		{"refs/heads/zzz", false},       // disjoint
	} {
		got, _, err := peerHoldCovers(dir, "", tc.name)
		if err != nil {
			t.Fatalf("peerHoldCovers(%s): %v", tc.name, err)
		}
		if got != tc.want {
			t.Errorf("peerHoldCovers(%s) = %v, want %v", tc.name, got, tc.want)
		}
	}
	// The exceptPath (our own hold) never conflicts with itself — and an
	// excluded self costs no scan work.
	if got, n, err := peerHoldCovers(dir, peer, "refs/heads/b/000123"); err != nil || got || n != 0 {
		t.Fatalf("except-path hold treated as a peer: got=%v scanned=%d err=%v", got, n, err)
	}
	// Allocation bound: scanning may not scale with the peer's NAME
	// count. TotalAlloc is monotonic, so the delta is exact.
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < 10; i++ {
		if _, _, err := peerHoldCovers(dir, "", "refs/heads/zzz"); err != nil {
			t.Fatalf("peerHoldCovers: %v", err)
		}
	}
	runtime.ReadMemStats(&after)
	if delta := after.TotalAlloc - before.TotalAlloc; delta > 4<<20 {
		t.Fatalf("peerHoldCovers allocated %d bytes across 10 scans of a 100k-name hold: it is parsing, not streaming", delta)
	}
}

// TestValidateCacheDir (Codex round-25 P2 on #173): a cache root deep
// enough that push-hold bookkeeping paths exceed the OS PATH_MAX must be
// rejected when the operator's choice is accepted, not discovered as an
// opaque mid-push failure.
func TestValidateCacheDir(t *testing.T) {
	if err := ValidateCacheDir(t.TempDir()); err != nil {
		t.Fatalf("typical cache dir rejected: %v", err)
	}
	deep := "/" + strings.Repeat("d/", pushRefPathMax/2) + "x"
	if err := ValidateCacheDir(deep); err == nil {
		t.Fatal("a cache dir past the PATH_MAX budget must be rejected at acceptance time")
	}
}
