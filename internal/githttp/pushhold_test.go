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
	expired := time.Now().Add(-pushHoldTTL - time.Minute)
	if err := os.Chtimes(other, expired, expired); err != nil {
		t.Fatalf("Chtimes: %v", err)
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
	t.Setenv(EnvRefsPolicy, `[["refs/heads/sigx",["verify-sig"]],["refs/heads/plainx",[]],["refs/heads/nopush",["no-push"]],["refs/heads/noff",["no-force-push"]]]`)
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
		// attacker-chosen, so it must never launch git merge-base — the
		// update hook performs the real check). These shas exist in no
		// repository, so reservationAllowed is only reachable
		// subprocess-free: a real ancestry evaluation would deny.
		{"refs/heads/noff", sha, sha2, reservationAllowed}, // update: hook enforces
		{"refs/heads/noff", sha, zero, reservationDenied},  // deletion: budget-free denial
		{"refs/heads/noff", zero, sha, reservationAllowed}, // creation: no rewind possible
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
	longSeg := "refs/heads/" + strings.Repeat("a", maxPushRefSegmentBytes+56)
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
// budget-free reservationAllowed verdict. The shas exist in no repository,
// so a full evaluation of the noff command would run git merge-base, fail
// the ancestry check, and flip it to denied — reservationAllowed is only
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
	want := []reservationClass{reservationAllowed, reservationDenied, reservationAllowed, reservationDenied}
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
// budget arithmetic: `<repoDir>/<name>.lock` plus NUL against PATH_MAX
// (4096). The e2e deep-cache-root test proves the end-to-end denial; this
// pins the boundary itself.
func TestPushRefPathTooLong(t *testing.T) {
	// 4096 = repoDirLen + 1 (slash) + nameLen + 5 (".lock") + 1 (NUL):
	// the largest fitting name under a 3400-byte repo dir is 689 bytes.
	if pushRefPathTooLong(3400, 689) {
		t.Fatal("689-byte name under a 3400-byte repo dir fits PATH_MAX; must be allowed")
	}
	if !pushRefPathTooLong(3400, 690) {
		t.Fatal("690-byte name under a 3400-byte repo dir exceeds PATH_MAX; must be denied")
	}
	// Typical roots leave the static 900-byte cap authoritative.
	if pushRefPathTooLong(100, maxPushRefNameBytes) {
		t.Fatal("the full static name cap must fit under a typical cache root")
	}
}

// TestPushHolds_FutureDatedHoldExpires (Codex round-21 P2 on #173): a
// hold file resurrected across a backward clock jump — a VM snapshot
// restore of a crashed in-flight push — carries an mtime in the FUTURE.
// A negative age must not extend the lease: readers would treat the leak
// as live and the mint-time sweeper would never remove it, blocking every
// same-name or D/F-relative push until wall time catches up and then
// advances another full TTL — far past the stated crash-leak bound. Both
// the reader and the sweeper must treat far-future mtimes as expired.
// Live pushes tolerate the symmetric bound: their refreshers re-touch
// with CURRENT time every pushHoldTTL/4, so after a backward jump a live
// hold looks future-dated for at most one refresh tick.
func TestPushHolds_FutureDatedHoldExpires(t *testing.T) {
	repoDir := t.TempDir()
	hold, err := mintPushHold(repoDir)
	if err != nil {
		t.Fatalf("mintPushHold: %v", err)
	}
	if err := os.WriteFile(hold, []byte("refs/heads/x\n"), 0o644); err != nil {
		t.Fatalf("write hold: %v", err)
	}
	holdDir := filepath.Join(repoDir, pushHoldDir)
	set, err := readPushHolds(holdDir, "")
	if err != nil || !set.contains("refs/heads/x") {
		t.Fatalf("fresh hold must be live: set=%v err=%v", set, err)
	}
	future := time.Now().Add(pushHoldTTL + time.Minute)
	if err := os.Chtimes(hold, future, future); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	set, err = readPushHolds(holdDir, "")
	if err != nil {
		t.Fatalf("readPushHolds: %v", err)
	}
	if set.contains("refs/heads/x") {
		t.Fatal("future-dated hold treated as live by the reader (negative age extends the lease)")
	}
	sweepExpiredPushHolds(holdDir)
	if _, err := os.Stat(hold); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("future-dated hold survived the sweep: %v", err)
	}
	// Just inside the future bound stays live: refresher touches from a
	// marginally fast clock must not expire a running push's hold.
	hold2, err := mintPushHold(repoDir)
	if err != nil {
		t.Fatalf("mintPushHold: %v", err)
	}
	if err := os.WriteFile(hold2, []byte("refs/heads/y\n"), 0o644); err != nil {
		t.Fatalf("write hold: %v", err)
	}
	near := time.Now().Add(pushHoldTTL / 2)
	if err := os.Chtimes(hold2, near, near); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	set, err = readPushHolds(holdDir, "")
	if err != nil || !set.contains("refs/heads/y") {
		t.Fatalf("near-future hold must stay live: set=%v err=%v", set, err)
	}
}
