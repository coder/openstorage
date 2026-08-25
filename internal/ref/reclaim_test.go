// Unreferenced-LFS-object reclamation row tests (issue #73), fanned out
// over both backends via runRefBackends like ledger_test.go.
package ref

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// lfsReclaimStore is the concrete reclamation surface both backends carry
// (jobs.LFSReclaimRegistry mirrors it; the methods are deliberately not on
// RefStore — only the background job drives them).
type lfsReclaimStore interface {
	Store
	LFSReposWithObjects(ctx context.Context, after string, limit int) ([]string, error)
	ListRepoLFSObjects(ctx context.Context, repo, after string, limit int) ([]LFSObjectRow, error)
	MarkLFSObjectsUnreferenced(ctx context.Context, repo string, keys []string) (int64, error)
	ClearLFSObjectsUnreferenced(ctx context.Context, repo string, keys []string) (int64, error)
	RetireUnreferencedLFSObject(ctx context.Context, repo, key string, pinnedGens map[string]int64, grace, reapMaxAge time.Duration) (bool, error)
	CountRefs(ctx context.Context, repo string, limit int64) (int64, error)
	FenceRepoPurge(ctx context.Context, base string) error
	ScanStateStore
}

func reclaimStore(t *testing.T, s Store) lfsReclaimStore {
	t.Helper()
	rs, ok := s.(lfsReclaimStore)
	if !ok {
		t.Fatalf("store %T lacks the reclamation surface", s)
	}
	// Retirement gates on a fresh destructive-reap watermark (issue
	// #120); seed one so verdict tests exercise their own guards.
	if err := rs.PutJobScanState(context.Background(), ScanJobSweep, ScanScopeLFSDestructiveReap, "", 0); err != nil {
		t.Fatalf("seed reap watermark: %v", err)
	}
	return rs
}

// canonicalKey mints a canonical-spelling LFS row key for the kit's base.
func canonicalLFSKey(repo, oidByte string) string {
	return "lfs/" + repo + "/o2/" + strings.Repeat(oidByte, 32)
}

func pinCurrentGens(t *testing.T, s Store, group []string) map[string]int64 {
	t.Helper()
	gens := make(map[string]int64, len(group))
	for _, r := range group {
		g, err := s.GetGeneration(context.Background(), r)
		if err != nil {
			t.Fatalf("GetGeneration(%q): %v", r, err)
		}
		gens[r] = g
	}
	return gens
}

// TestLFSReclaim_MarkClearRetire is the core verdict lifecycle: rows start
// unmarked, marking stamps only unmarked rows, clearing removes marks, and
// retirement deletes exactly the grace-expired marked row while refunding
// the object count, the usage bytes, and bumping the adoption generation.
func TestLFSReclaim_MarkClearRetire(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		rs := reclaimStore(t, s)
		ctx := context.Background()
		k := newLedgerKit(t, s)
		keep := canonicalLFSKey(k.base(), "aa")
		junk := canonicalLFSKey(k.base(), "bb")
		for _, key := range []string{keep, junk} {
			if err := s.RegisterLFSObject(ctx, k.base(), key, 100); err != nil {
				t.Fatal(err)
			}
		}
		genBefore, err := s.LFSAdoptionGen(ctx, k.base())
		if err != nil {
			t.Fatal(err)
		}

		rows, err := rs.ListRepoLFSObjects(ctx, k.base(), "", 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 2 || !rows[0].UnreferencedSince.IsZero() || !rows[1].UnreferencedSince.IsZero() {
			t.Fatalf("fresh rows = %+v, want 2 unmarked", rows)
		}

		if n, err := rs.MarkLFSObjectsUnreferenced(ctx, k.base(), []string{keep, junk}); err != nil || n != 2 {
			t.Fatalf("mark = (%d, %v), want 2", n, err)
		}
		// Re-marking is a no-op: the original clock survives.
		if n, err := rs.MarkLFSObjectsUnreferenced(ctx, k.base(), []string{keep, junk}); err != nil || n != 0 {
			t.Fatalf("re-mark = (%d, %v), want 0", n, err)
		}
		if n, err := rs.ClearLFSObjectsUnreferenced(ctx, k.base(), []string{keep}); err != nil || n != 1 {
			t.Fatalf("clear = (%d, %v), want 1", n, err)
		}

		gens := pinCurrentGens(t, s, k.group)
		// The cleared row is spared even at zero grace.
		if retired, err := rs.RetireUnreferencedLFSObject(ctx, k.base(), keep, gens, 0, time.Hour); err != nil || retired {
			t.Fatalf("retire cleared row = (%v, %v), want spared", retired, err)
		}
		// The marked row retires: row gone, count and usage refunded,
		// adoption generation bumped.
		if retired, err := rs.RetireUnreferencedLFSObject(ctx, k.base(), junk, gens, 0, time.Hour); err != nil || !retired {
			t.Fatalf("retire marked row = (%v, %v), want retired", retired, err)
		}
		if has, err := s.HasLFSObject(ctx, k.base(), junk); err != nil || has {
			t.Fatalf("retired row still registered (%v, %v)", has, err)
		}
		if n, err := s.CountLFSObjects(ctx, k.base()); err != nil || n != 1 {
			t.Fatalf("count after retire = (%d, %v), want 1", n, err)
		}
		if st := k.state(); st.UsageBytes != 100 {
			t.Fatalf("usage after retire = %d, want 100", st.UsageBytes)
		}
		if gen, err := s.LFSAdoptionGen(ctx, k.base()); err != nil || gen != genBefore+1 {
			t.Fatalf("adoption gen after retire = (%d, %v), want %d", gen, err, genBefore+1)
		}
		// Retiring again is a no-op verdict.
		if retired, err := rs.RetireUnreferencedLFSObject(ctx, k.base(), junk, gens, 0, time.Hour); err != nil || retired {
			t.Fatalf("re-retire = (%v, %v), want no-op", retired, err)
		}
	})
}

// TestLFSReclaim_GraceSparesFreshMark: the guarded delete re-checks the
// mark's age inside the transaction — a mark younger than grace never
// retires, no matter what the scan concluded.
func TestLFSReclaim_GraceSparesFreshMark(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		rs := reclaimStore(t, s)
		ctx := context.Background()
		k := newLedgerKit(t, s)
		key := canonicalLFSKey(k.base(), "cc")
		if err := s.RegisterLFSObject(ctx, k.base(), key, 10); err != nil {
			t.Fatal(err)
		}
		if _, err := rs.MarkLFSObjectsUnreferenced(ctx, k.base(), []string{key}); err != nil {
			t.Fatal(err)
		}
		gens := pinCurrentGens(t, s, k.group)
		if retired, err := rs.RetireUnreferencedLFSObject(ctx, k.base(), key, gens, time.Hour, time.Hour); err != nil || retired {
			t.Fatalf("retire fresh mark = (%v, %v), want spared", retired, err)
		}
		if has, err := s.HasLFSObject(ctx, k.base(), key); err != nil || !has {
			t.Fatalf("spared row missing (%v, %v)", has, err)
		}
	})
}

// TestLFSReclaim_LiveAdoptionClaimSpares (Codex round 11): every
// server-side canonical materialization — the PUT publish/register region,
// adoption and compat copies — holds the per-key fleet adoption claim with
// renewal, and its zero-growth verdict assumes the row's slot and byte
// credit survive the region. Retirement must honor a live claim for both
// row spellings (claims are minted on the canonical key), and retire again
// once it releases.
func TestLFSReclaim_LiveAdoptionClaimSpares(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		rs := reclaimStore(t, s)
		ctx := context.Background()
		k := newLedgerKit(t, s)
		oid := strings.Repeat("ff", 32)
		canonical := "lfs/" + k.base() + "/o2/" + oid
		legacy := "lfs/" + k.base() + "/" + oid
		for _, key := range []string{canonical, legacy} {
			if err := s.RegisterLFSObject(ctx, k.base(), key, 10); err != nil {
				t.Fatal(err)
			}
			if _, err := rs.MarkLFSObjectsUnreferenced(ctx, k.base(), []string{key}); err != nil {
				t.Fatal(err)
			}
		}
		claimed, owner, err := s.TryAdoptionClaim(ctx, k.base(), canonical)
		if err != nil || !claimed {
			t.Fatalf("TryAdoptionClaim = (%v, %v)", claimed, err)
		}
		gens := pinCurrentGens(t, s, k.group)
		// Both spellings are spared while the canonical claim lives.
		for _, key := range []string{canonical, legacy} {
			if retired, err := rs.RetireUnreferencedLFSObject(ctx, k.base(), key, gens, 0, time.Hour); err != nil || retired {
				t.Fatalf("retire %s under live claim = (%v, %v), want spared", key, retired, err)
			}
		}
		if err := s.ReleaseAdoptionClaim(ctx, canonical, owner); err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{canonical, legacy} {
			if retired, err := rs.RetireUnreferencedLFSObject(ctx, k.base(), key, gens, 0, time.Hour); err != nil || !retired {
				t.Fatalf("retire %s after release = (%v, %v), want retired", key, retired, err)
			}
		}
	})
}

// TestLFSReclaim_CountRefs: the reclaimer prices ref fanout with a counting
// query instead of materializing refs; both backends agree with the rows.
func TestLFSReclaim_CountRefs(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		rs := reclaimStore(t, s)
		ctx := context.Background()
		k := newLedgerKit(t, s)
		if n, err := rs.CountRefs(ctx, k.base(), 10); err != nil || n != 0 {
			t.Fatalf("CountRefs empty = (%d, %v), want 0", n, err)
		}
		for i := 0; i < 3; i++ {
			if won, err := s.CASRef(ctx, k.base(), fmt.Sprintf("refs/heads/b%d", i), ZeroSHA, strings.Repeat("22", 20)); err != nil || !won {
				t.Fatalf("CASRef = (%v, %v)", won, err)
			}
		}
		if n, err := rs.CountRefs(ctx, k.base(), 10); err != nil || n != 3 {
			t.Fatalf("CountRefs = (%d, %v), want 3", n, err)
		}
		// The cap bounds the count's own index walk (Codex round 12): a
		// saturated count reports exactly the limit, never past it.
		if n, err := rs.CountRefs(ctx, k.base(), 2); err != nil || n != 2 {
			t.Fatalf("capped CountRefs = (%d, %v), want 2", n, err)
		}
	})
}

// TestLFSReclaim_SubSecondGraceIsNotZero (Codex round 4): SQLite stores
// reclamation marks at whole-second resolution — a sub-second grace must
// still hold a freshly marked row through a real window instead of
// truncating to zero and retiring it immediately.
func TestLFSReclaim_SubSecondGraceIsNotZero(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		rs := reclaimStore(t, s)
		ctx := context.Background()
		k := newLedgerKit(t, s)
		key := canonicalLFSKey(k.base(), "ee")
		if err := s.RegisterLFSObject(ctx, k.base(), key, 10); err != nil {
			t.Fatal(err)
		}
		if _, err := rs.MarkLFSObjectsUnreferenced(ctx, k.base(), []string{key}); err != nil {
			t.Fatal(err)
		}
		gens := pinCurrentGens(t, s, k.group)
		if retired, err := rs.RetireUnreferencedLFSObject(ctx, k.base(), key, gens, 300*time.Millisecond, time.Hour); err != nil || retired {
			t.Fatalf("retire fresh mark at sub-second grace = (%v, %v), want spared", retired, err)
		}
		// An explicit zero grace waives the window: the row stays retirable.
		if retired, err := rs.RetireUnreferencedLFSObject(ctx, k.base(), key, gens, 0, time.Hour); err != nil || !retired {
			t.Fatalf("retire with waived grace = (%v, %v), want retired", retired, err)
		}
	})
}

// TestLFSReclaim_GenerationFence: a ref mutation anywhere in the namespace
// group after the scan pinned its generations invalidates the verdict — a
// pointer file pushed mid-scan lives in a bumped generation by definition.
func TestLFSReclaim_GenerationFence(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		rs := reclaimStore(t, s)
		ctx := context.Background()
		k := newLedgerKit(t, s)
		key := canonicalLFSKey(k.base(), "dd")
		if err := s.RegisterLFSObject(ctx, k.base(), key, 10); err != nil {
			t.Fatal(err)
		}
		if _, err := rs.MarkLFSObjectsUnreferenced(ctx, k.base(), []string{key}); err != nil {
			t.Fatal(err)
		}
		gens := pinCurrentGens(t, s, k.group)
		// A push lands after the pin (any group member counts).
		if won, err := s.CASRef(ctx, k.base(), "refs/heads/main", ZeroSHA, strings.Repeat("11", 20)); err != nil || !won {
			t.Fatalf("CASRef = (%v, %v)", won, err)
		}
		if retired, err := rs.RetireUnreferencedLFSObject(ctx, k.base(), key, gens, 0, time.Hour); err != nil || retired {
			t.Fatalf("retire with stale generations = (%v, %v), want spared", retired, err)
		}
		// Re-pinned generations pass.
		gens = pinCurrentGens(t, s, k.group)
		if retired, err := rs.RetireUnreferencedLFSObject(ctx, k.base(), key, gens, 0, time.Hour); err != nil || !retired {
			t.Fatalf("retire with fresh generations = (%v, %v), want retired", retired, err)
		}
	})
}

// TestLFSReclaim_TouchSpares: the zero-growth liveness handshake — a touch
// clears the mark (sparing the row from a stale verdict) and reports
// whether the row still exists, so callers of a retired row fall through
// to their upload path.
func TestLFSReclaim_TouchSpares(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		rs := reclaimStore(t, s)
		ctx := context.Background()
		k := newLedgerKit(t, s)
		key := canonicalLFSKey(k.base(), "ee")
		if err := s.RegisterLFSObject(ctx, k.base(), key, 10); err != nil {
			t.Fatal(err)
		}
		if _, err := rs.MarkLFSObjectsUnreferenced(ctx, k.base(), []string{key}); err != nil {
			t.Fatal(err)
		}
		if touched, err := s.TouchLFSObject(ctx, k.base(), key); err != nil || !touched {
			t.Fatalf("touch = (%v, %v), want touched", touched, err)
		}
		gens := pinCurrentGens(t, s, k.group)
		if retired, err := rs.RetireUnreferencedLFSObject(ctx, k.base(), key, gens, 0, time.Hour); err != nil || retired {
			t.Fatalf("retire touched row = (%v, %v), want spared", retired, err)
		}
		// A touch of a vanished row reports the miss.
		if touched, err := s.TouchLFSObject(ctx, k.base(), canonicalLFSKey(k.base(), "ff")); err != nil || touched {
			t.Fatalf("touch missing row = (%v, %v), want untouched", touched, err)
		}
	})
}

// TestLFSReclaim_ReRegistrationClearsMark: the idempotent registration path
// clears the mark atomically with its settlement — a re-uploaded object
// must survive a scan verdict minted before the upload.
func TestLFSReclaim_ReRegistrationClearsMark(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		rs := reclaimStore(t, s)
		ctx := context.Background()
		k := newLedgerKit(t, s)
		key := canonicalLFSKey(k.base(), "ab")
		if err := s.RegisterLFSObject(ctx, k.base(), key, 10); err != nil {
			t.Fatal(err)
		}
		if _, err := rs.MarkLFSObjectsUnreferenced(ctx, k.base(), []string{key}); err != nil {
			t.Fatal(err)
		}
		if err := s.RegisterLFSObject(ctx, k.base(), key, 10); err != nil {
			t.Fatal(err)
		}
		rows, err := rs.ListRepoLFSObjects(ctx, k.base(), "", 10)
		if err != nil || len(rows) != 1 {
			t.Fatalf("rows = (%+v, %v), want 1", rows, err)
		}
		if !rows[0].UnreferencedSince.IsZero() {
			t.Fatalf("mark survived re-registration: %+v", rows[0])
		}
	})
}

// TestLFSReclaim_PurgeFenceSkips: fenced repos' rows belong to the purge
// pipeline; the verdict declines them.
func TestLFSReclaim_PurgeFenceSkips(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		rs := reclaimStore(t, s)
		ctx := context.Background()
		k := newLedgerKit(t, s)
		key := canonicalLFSKey(k.base(), "de")
		if err := s.RegisterLFSObject(ctx, k.base(), key, 10); err != nil {
			t.Fatal(err)
		}
		if _, err := rs.MarkLFSObjectsUnreferenced(ctx, k.base(), []string{key}); err != nil {
			t.Fatal(err)
		}
		gens := pinCurrentGens(t, s, k.group)
		if err := rs.FenceRepoPurge(ctx, k.base()); err != nil {
			t.Fatal(err)
		}
		if retired, err := rs.RetireUnreferencedLFSObject(ctx, k.base(), key, gens, 0, time.Hour); err != nil || retired {
			t.Fatalf("retire fenced repo = (%v, %v), want declined", retired, err)
		}
		if has, err := s.HasLFSObject(ctx, k.base(), key); err != nil || !has {
			t.Fatalf("fenced row missing (%v, %v)", has, err)
		}
	})
}

// TestLFSReclaim_RepoPaging: LFSReposWithObjects pages repos with live
// counts (keyset, ascending) and drops repos whose count returns to zero.
func TestLFSReclaim_RepoPaging(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		rs := reclaimStore(t, s)
		ctx := context.Background()
		k := newLedgerKit(t, s)
		key := canonicalLFSKey(k.base(), "fa")
		if err := s.RegisterLFSObject(ctx, k.base(), key, 10); err != nil {
			t.Fatal(err)
		}
		page := func(after string) []string {
			t.Helper()
			repos, err := rs.LFSReposWithObjects(ctx, after, 1000)
			if err != nil {
				t.Fatal(err)
			}
			return repos
		}
		found := false
		for _, r := range page("") {
			if r == k.base() {
				found = true
			}
		}
		if !found {
			t.Fatalf("repo with objects not paged: %v", page(""))
		}
		// Keyset: pages after the repo exclude it.
		for _, r := range page(k.base()) {
			if r == k.base() {
				t.Fatalf("keyset page re-listed %q", r)
			}
		}
		// Retire the only row: the repo leaves the work queue.
		if _, err := rs.MarkLFSObjectsUnreferenced(ctx, k.base(), []string{key}); err != nil {
			t.Fatal(err)
		}
		gens := pinCurrentGens(t, s, k.group)
		if retired, err := rs.RetireUnreferencedLFSObject(ctx, k.base(), key, gens, 0, time.Hour); err != nil || !retired {
			t.Fatalf("retire = (%v, %v)", retired, err)
		}
		for _, r := range page("") {
			if r == k.base() {
				t.Fatalf("zero-count repo still paged")
			}
		}
	})
}
