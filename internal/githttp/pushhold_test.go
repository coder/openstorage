package githttp

// Unit coverage for the push-hold machinery (issue #170): full syncs must
// skip create/update/delete commands naming a held ref (in both the single
// pass and the overflow second pass), ignore expired holds, and suppress
// the generation marker whenever anything was skipped.

import (
	"context"
	"fmt"
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
	if _, err := reservePushHolds(context.Background(), holdPath, names); err != nil {
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
	if _, err := reservePushHolds(context.Background(), "relative/hold", []string{"refs/heads/x"}); err == nil {
		t.Fatal("relative hold path accepted")
	}
	abs := filepath.Join(t.TempDir(), "holds", "hold")
	if _, err := reservePushHolds(context.Background(), abs, []string{"refs/heads/a\nrefs/heads/b"}); err == nil {
		t.Fatal("line-splitting ref name accepted")
	}
	if _, err := reservePushHolds(context.Background(), abs, []string{""}); err == nil {
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
	if _, err := reservePushHolds(context.Background(), own, []string{"refs/heads/a/b"}); err == nil {
		t.Fatal("reservation past a live D/F-conflicting hold accepted")
	} else if !strings.Contains(err.Error(), "conflicting") {
		t.Fatalf("denial reason = %v, want a conflict message", err)
	}
	// Same-name and unrelated refs are NOT conflicts.
	if _, err := reservePushHolds(context.Background(), own, []string{"refs/heads/a", "refs/heads/z"}); err != nil {
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
	if _, err := reservePushHolds(context.Background(), own2, []string{"refs/heads/a/c"}); err != nil {
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
	if _, err := reservePushHolds(context.Background(), own, []string{"refs/heads/x"}); err == nil {
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
	_, err = reservePushHolds(context.Background(), filepath.Join(holdsDir, "push-own"), []string{"refs/heads/x"})
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
	if _, err := reservePushHolds(context.Background(), own, []string{"refs/heads/denied/x"}); err != nil {
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

// TestReservePushHolds_ReportsWaitedNames (Codex round-10 P1 substrate):
// the reservation must tell its caller exactly which names sat out a
// conflicting peer hold — pre-receive re-probes the authority for those
// creations, because the peer may have committed the conflicting ref
// before releasing.
func TestReservePushHolds_ReportsWaitedNames(t *testing.T) {
	dir := t.TempDir()
	peer := mintHold(t, dir, "refs/heads/a")

	own, err := mintPushHold(dir)
	if err != nil {
		t.Fatalf("mintPushHold: %v", err)
	}
	type result struct {
		waited []string
		err    error
	}
	done := make(chan result, 1)
	go func() {
		waited, err := reservePushHolds(context.Background(), own,
			[]string{"refs/heads/a/b", "refs/heads/z"})
		done <- result{waited, err}
	}()
	// Let the reservation hit the conflict at least once, then clear it.
	time.Sleep(300 * time.Millisecond)
	if err := os.Remove(peer); err != nil {
		t.Fatalf("remove peer hold: %v", err)
	}
	res := <-done
	if res.err != nil {
		t.Fatalf("reservePushHolds after peer cleared: %v", res.err)
	}
	if len(res.waited) != 1 || res.waited[0] != "refs/heads/a/b" {
		t.Fatalf("waited = %v, want exactly [refs/heads/a/b] (refs/heads/z never conflicted)", res.waited)
	}

	// The unconflicted fast path reports nothing.
	own2, err := mintPushHold(dir)
	if err != nil {
		t.Fatalf("mintPushHold: %v", err)
	}
	waited, err := reservePushHolds(context.Background(), own2, []string{"refs/heads/free"})
	if err != nil || waited != nil {
		t.Fatalf("unconflicted reserve = (%v, %v), want (nil, nil)", waited, err)
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

// snapshotStore satisfies refNameSnapshotter; its embedded countingStore's
// GetRef PANICS, proving the snapshot path never falls back to point reads.
type snapshotStore struct {
	*countingStore
	snapshots int
}

func (s *snapshotStore) SnapshotRefNames(_ context.Context, _ string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshots++
	names := make([]string, 0, len(s.refs))
	for name := range s.refs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// TestAuthorityDoomedCreations_SnapshotStore (Codex round-10 security P2):
// on stores where every read materializes the full ref table (s3wal
// manifests), the probe must batch through ONE SnapshotRefNames call —
// zero point reads, zero list pages — and reach the same verdicts.
func TestAuthorityDoomedCreations_SnapshotStore(t *testing.T) {
	refs := map[string]string{
		"refs/heads/a":     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"refs/heads/x/y":   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"refs/heads/plain": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	// One existing child beneath each proposed parent — the round-10
	// crafted shape that defeated the seek-merge's skip windows.
	var creations []string
	for i := 0; i < 50; i++ {
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
	st := &snapshotStore{countingStore: &countingStore{gen: 1, refs: refs}}
	doomed, err := authorityDoomedCreations(context.Background(), st, "r", creations)
	if err != nil {
		t.Fatalf("authorityDoomedCreations: %v", err)
	}
	if len(doomed) != 52 {
		t.Fatalf("doomed = %d names, want 52 (50 squatted parents + a/b + x): %v", len(doomed), doomed)
	}
	for _, want := range []string{"refs/heads/a/b", "refs/heads/x", "refs/heads/p000", "refs/heads/p049"} {
		if _, ok := doomed[want]; !ok {
			t.Errorf("%s not reported doomed", want)
		}
	}
	if st.snapshots != 1 {
		t.Errorf("snapshots = %d, want exactly 1", st.snapshots)
	}
	_, listCalls, _ := st.counts()
	if listCalls != 0 {
		t.Errorf("listRefsCalls = %d, want 0 — the snapshot path issued store reads", listCalls)
	}
}
