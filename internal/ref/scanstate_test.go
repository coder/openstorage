// Fleet-persistent scan-state row tests (issues #120/#121/#125), fanned
// out over both backends via runRefBackends.
package ref

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func scanStateStore(t *testing.T, s Store) ScanStateStore {
	t.Helper()
	ss, ok := s.(ScanStateStore)
	if !ok {
		t.Fatalf("store %T lacks the scan-state seams", s)
	}
	return ss
}

// absenceCoverStore is the cover surface both concrete backends carry
// (jobs.AbsenceRegistry mirrors it; deliberately not on RefStore — only
// the absence reconciler drives it).
type absenceCoverStore interface {
	Store
	RenewAbsenceCovers(ctx context.Context, ttl time.Duration) (int64, error)
	CASAbsenceCoverOwner(ctx context.Context, rawKey, expectedOwner string, expectedBytes int64, newOwner string) (bool, error)
	LapsedAbsenceCoverExists(ctx context.Context, ttl time.Duration) (bool, error)
	ListAbsenceCovers(ctx context.Context, afterRaw string, limit int, ttl time.Duration) ([]AbsenceCoverRow, error)
	AbsenceCoverHead(ctx context.Context, prefix string, ttl time.Duration) (AbsenceCoverRow, bool, error)
	DeleteAbsenceCover(ctx context.Context, key string) error
}

func coverStore(t *testing.T, s Store) absenceCoverStore {
	t.Helper()
	cs, ok := s.(absenceCoverStore)
	if !ok {
		t.Fatalf("store %T lacks the absence cover seams", s)
	}
	return cs
}

// TestJobScanState_RoundTrip: rows read back exactly, upserts replace
// payload and generation atomically, deletes are idempotent, and
// updated_at advances on every write (the watermark's clock).
func TestJobScanState_RoundTrip(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		ss := scanStateStore(t, s)
		ctx := context.Background()
		job, scope := "test-job", "scope/"+t.Name()

		if _, ok, err := ss.GetJobScanState(ctx, job, scope); err != nil || ok {
			t.Fatalf("missing row = (ok=%v, %v), want absent", ok, err)
		}
		if err := ss.PutJobScanState(ctx, job, scope, "cursor-1", 7); err != nil {
			t.Fatalf("put: %v", err)
		}
		st, ok, err := ss.GetJobScanState(ctx, job, scope)
		if err != nil || !ok || st.Payload != "cursor-1" || st.Generation != 7 {
			t.Fatalf("get = (%+v, %v, %v), want cursor-1/7", st, ok, err)
		}
		if st.UpdatedAt.IsZero() {
			t.Fatal("updated_at not stamped")
		}
		if d := time.Since(st.UpdatedAt); d < -time.Minute || d > time.Minute {
			t.Fatalf("updated_at %v is not near now", st.UpdatedAt)
		}

		// SQLite stamps whole seconds; cross the boundary so the advance
		// is observable on both backends.
		time.Sleep(1100 * time.Millisecond)
		if err := ss.PutJobScanState(ctx, job, scope, "cursor-2", 9); err != nil {
			t.Fatalf("re-put: %v", err)
		}
		st2, ok, err := ss.GetJobScanState(ctx, job, scope)
		if err != nil || !ok || st2.Payload != "cursor-2" || st2.Generation != 9 {
			t.Fatalf("get 2 = (%+v, %v, %v), want cursor-2/9", st2, ok, err)
		}
		if !st2.UpdatedAt.After(st.UpdatedAt) {
			t.Fatalf("updated_at did not advance: %v -> %v", st.UpdatedAt, st2.UpdatedAt)
		}

		if err := ss.DeleteJobScanState(ctx, job, scope); err != nil {
			t.Fatalf("delete: %v", err)
		}
		if _, ok, err := ss.GetJobScanState(ctx, job, scope); err != nil || ok {
			t.Fatalf("deleted row = (ok=%v, %v), want absent", ok, err)
		}
		if err := ss.DeleteJobScanState(ctx, job, scope); err != nil {
			t.Fatalf("re-delete: %v", err)
		}
	})
}

// TestJobScanState_PutIf: a conditional write lands only when the row's
// generation is the expected one, an absent row counts as generation 0, and
// among writers that hold the same generation exactly one wins.
func TestJobScanState_PutIf(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		ss := scanStateStore(t, s)
		ctx := context.Background()
		job, scope := "test-job", "put-if/"+t.Name()
		want := func(payload string, generation int64) {
			t.Helper()
			st, ok, err := ss.GetJobScanState(ctx, job, scope)
			if err != nil || !ok || st.Payload != payload || st.Generation != generation {
				t.Fatalf("row = (%+v, %v, %v), want %s at generation %d", st, ok, err, payload, generation)
			}
		}

		if err := ss.PutJobScanStateIf(ctx, job, scope, "late", 8, 7); !errors.Is(err, ErrScanStateConflict) {
			t.Fatalf("write that expects a generation on an absent row = %v, want ErrScanStateConflict", err)
		}
		if _, ok, _ := ss.GetJobScanState(ctx, job, scope); ok {
			t.Fatal("a refused write created the row")
		}
		if err := ss.PutJobScanStateIf(ctx, job, scope, "first", 1, 0); err != nil {
			t.Fatalf("create with expected generation 0: %v", err)
		}
		want("first", 1)
		if err := ss.PutJobScanStateIf(ctx, job, scope, "second", 1, 0); !errors.Is(err, ErrScanStateConflict) {
			t.Fatalf("create over an existing row = %v, want ErrScanStateConflict", err)
		}
		if err := ss.PutJobScanStateIf(ctx, job, scope, "stale", 2, 5); !errors.Is(err, ErrScanStateConflict) {
			t.Fatalf("write with a stale generation = %v, want ErrScanStateConflict", err)
		}
		want("first", 1)
		if err := ss.PutJobScanStateIf(ctx, job, scope, "next", 1, 1); err != nil {
			t.Fatalf("write with the current generation: %v", err)
		}
		want("next", 1)
		if err := ss.PutJobScanStateIf(ctx, job, scope, "new pass", 2, 1); err != nil {
			t.Fatalf("advance the generation: %v", err)
		}
		want("new pass", 2)
		// The run that still holds generation 1 loses the row.
		if err := ss.PutJobScanStateIf(ctx, job, scope, "old run", 1, 1); !errors.Is(err, ErrScanStateConflict) {
			t.Fatalf("write by the run that lost the row = %v, want ErrScanStateConflict", err)
		}
		want("new pass", 2)

		// A row at generation 0 matches an expected generation 0.
		zero := scope + "/zero"
		if err := ss.PutJobScanState(ctx, job, zero, "zero", 0); err != nil {
			t.Fatal(err)
		}
		if err := ss.PutJobScanStateIf(ctx, job, zero, "rewritten", 0, 0); err != nil {
			t.Fatalf("write over a generation-0 row: %v", err)
		}

		// Eight writers hold generation 2 and each writes generation 3.
		const writers = 8
		var wins atomic.Int32
		var wg sync.WaitGroup
		for i := range writers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := ss.PutJobScanStateIf(ctx, job, scope, fmt.Sprintf("writer %d", i), 3, 2)
				switch {
				case err == nil:
					wins.Add(1)
				case !errors.Is(err, ErrScanStateConflict):
					t.Errorf("writer %d: %v", i, err)
				}
			}()
		}
		wg.Wait()
		if wins.Load() != 1 {
			t.Fatalf("%d writers won, want exactly 1", wins.Load())
		}
		if st, _, _ := ss.GetJobScanState(ctx, job, scope); st.Generation != 3 {
			t.Fatalf("generation = %d, want 3", st.Generation)
		}
	})
}

// TestJobScanState_PutIfCreateRace: writers that expect generation 0 on a row
// that does not exist yet, and write generation 0 again, all succeed. The row
// never leaves generation 0, so none of them may be told that another run took
// it over.
func TestJobScanState_PutIfCreateRace(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		ss := scanStateStore(t, s)
		ctx := context.Background()
		const writers = 6
		for round := range 8 {
			scope := fmt.Sprintf("put-if-create/%s/%d", t.Name(), round)
			errs := make(chan error, writers)
			var wg sync.WaitGroup
			for i := range writers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					errs <- ss.PutJobScanStateIf(ctx, "test-job", scope, fmt.Sprintf("writer %d", i), 0, 0)
				}()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				if err != nil {
					t.Fatalf("round %d: a writer that expected generation 0 on a row at generation 0 got %v", round, err)
				}
			}
		}
	})
}

// TestJobScanState_ScopesIsolated: rows are keyed by (job, scope) — one
// job's cursors never collide with another's.
func TestJobScanState_ScopesIsolated(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		ss := scanStateStore(t, s)
		ctx := context.Background()
		scope := "shared-scope/" + t.Name()
		if err := ss.PutJobScanState(ctx, "job-a", scope, "a", 1); err != nil {
			t.Fatal(err)
		}
		if err := ss.PutJobScanState(ctx, "job-b", scope, "b", 2); err != nil {
			t.Fatal(err)
		}
		if st, ok, _ := ss.GetJobScanState(ctx, "job-a", scope); !ok || st.Payload != "a" {
			t.Fatalf("job-a row = (%+v, %v)", st, ok)
		}
		if err := ss.DeleteJobScanState(ctx, "job-a", scope); err != nil {
			t.Fatal(err)
		}
		if st, ok, _ := ss.GetJobScanState(ctx, "job-b", scope); !ok || st.Payload != "b" || st.Generation != 2 {
			t.Fatalf("job-b row = (%+v, %v), want untouched by job-a's delete", st, ok)
		}
		// DB-clock age (Codex round 10 on PR #132): a fresh row reads a
		// small non-negative age regardless of this process's clock; an
		// absent row reads ok=false.
		if age, ok, err := ss.JobScanStateAge(ctx, "job-b", scope); err != nil || !ok || age < 0 || age > time.Minute {
			t.Fatalf("age = (%v, %v, %v), want a fresh non-negative DB-clock age", age, ok, err)
		}
		if _, ok, err := ss.JobScanStateAge(ctx, "job-a", scope); err != nil || ok {
			t.Fatalf("age of deleted row = (ok=%v, %v), want absent", ok, err)
		}
	})
}

// TestAbsenceCovers_PrefixFamilyPageBoundary (Codex round 3 on PR #156):
// cover RAW keys decorate the base with a '/'-led tail, and '/' (0x2F)
// sorts ABOVE '-' (0x2D) — so a prefix-family sibling base ("abc-1")
// holds cover rows that sort BEFORE its head's ("abc/…") even though the
// sibling sorts AFTER the head as a base. The listing must emit in raw
// order with a page boundary exactly between the family members: a
// base-ordered traversal would emit the head first, park the cursor on
// its row, and then skip the sibling's rows forever (raw <= cursor) —
// covers that are never again re-pinned, folded, or released.
func TestAbsenceCovers_PrefixFamilyPageBoundary(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		cs := coverStore(t, s)
		ctx := context.Background()
		head := t.Name()       // base-order FIRST
		sibling := head + "-1" // raw-order FIRST ('-' < '/')
		mint := func(base string, size int64) string {
			t.Helper()
			key := AbsenceCoverPrefix + base + "/lfs-e0000000000000000001"
			if err := ReserveRestoreCover(ctx, s, []string{base}, key, size, time.Hour); err != nil {
				t.Fatalf("cover %s: %v", key, err)
			}
			return key
		}
		keyHead := mint(head, 10)
		keySibling := mint(sibling, 20)
		// The cover listing is fleet-global: release both rows so later
		// tests sharing this backend see only their own covers.
		t.Cleanup(func() {
			for _, key := range []string{keyHead, keySibling} {
				if err := cs.DeleteAbsenceCover(ctx, key); err != nil {
					t.Errorf("cleanup cover %s: %v", key, err)
				}
			}
		})

		// Page 1 (limit 1): raw order puts the SIBLING's row first.
		p1, err := cs.ListAbsenceCovers(ctx, "", 1, time.Hour)
		if err != nil || len(p1) != 1 || p1[0].Key != keySibling {
			t.Fatalf("page 1 = (%+v, %v), want exactly the sibling row %q", p1, err, keySibling)
		}
		// Page 2 resumes across the family boundary: the head's row —
		// which the manifest listing places BEFORE the cursor's base —
		// must still be emitted, strictly above the cursor.
		p2, err := cs.ListAbsenceCovers(ctx, p1[0].RawKey, 10, time.Hour)
		if err != nil || len(p2) != 1 || p2[0].Key != keyHead {
			t.Fatalf("page 2 = (%+v, %v), want exactly the head row %q (page-boundary cover lost?)", p2, err, keyHead)
		}
		if p2[0].RawKey <= p1[0].RawKey {
			t.Fatalf("cursor order inverted: %q after %q", p2[0].RawKey, p1[0].RawKey)
		}
		// The cursor on the LAST row ends the scan.
		p3, err := cs.ListAbsenceCovers(ctx, p2[0].RawKey, 10, time.Hour)
		if err != nil || len(p3) != 0 {
			t.Fatalf("page 3 = (%+v, %v), want empty", p3, err)
		}
	})
}

// TestAbsenceCovers_ListHeadDelete: the absence reconciler's durable cover
// surface — prefix-ranged listing with an opaque keyset cursor, the
// highest-incarnation head lookup (zero-padded byte order), and targeted
// release — over real reservation rows on both backends.
func TestAbsenceCovers_ListHeadDelete(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		cs := coverStore(t, s)
		ctx := context.Background()
		k := newLedgerKit(t, s)
		base := k.base()
		cover := func(key string, size int64) {
			t.Helper()
			if err := ReserveRestoreCover(ctx, s, k.group, key, size, time.Hour); err != nil {
				t.Fatalf("cover %s: %v", key, err)
			}
		}
		keyLow := AbsenceCoverPrefix + base + "/lfs-e0000000000000000001"
		keyHigh := AbsenceCoverPrefix + base + "/lfs-e0000000000000000002"
		keyPacks := AbsenceCoverPrefix + base + "/packs-e0000000000000000001"
		cover(keyLow, 100)
		cover(keyHigh, 60)
		cover(keyPacks, 40)
		// Foreign reservation spellings under the prefix (no repair-cover
		// suffix) must be invisible to the listing, the head lookup, and
		// — critically — pagination: a page of foreign rows must never
		// read as the end of the keyspace (Codex round 3 on PR #132).
		// One sorts between the covers, one above every incarnation key.
		for _, foreign := range []string{
			AbsenceCoverPrefix + base + "/lfs-e0000000000000000001x-foreign",
			AbsenceCoverPrefix + base + "/zz-foreign",
		} {
			if dec, err := s.ReservePackBytes(ctx, k.group, []Pack{{Key: foreign, Size: 5}}, NewReservationOwner(), 0, ReserveNoFloorGen, 1<<40, time.Hour); err != nil || !dec.OK {
				t.Fatalf("plant foreign row %s: (%+v, %v)", foreign, dec, err)
			}
		}

		// Paged listing: one row per page, cursor round-trips opaquely.
		var got []AbsenceCoverRow
		after := ""
		for {
			rows, err := cs.ListAbsenceCovers(ctx, after, 1, time.Hour)
			if err != nil {
				t.Fatalf("list: %v", err)
			}
			if len(rows) == 0 {
				break
			}
			got = append(got, rows...)
			after = rows[len(rows)-1].RawKey
		}
		if len(got) != 3 {
			t.Fatalf("listed %d covers, want 3: %+v", len(got), got)
		}
		if got[0].Key != keyLow || got[1].Key != keyHigh || got[2].Key != keyPacks {
			t.Fatalf("listing order = %q %q %q, want byte order", got[0].Key, got[1].Key, got[2].Key)
		}
		for _, row := range got {
			if row.Base != base {
				t.Fatalf("cover %q base = %q, want %q", row.Key, row.Base, base)
			}
		}

		// Head lookup: the highest incarnation under the class prefix —
		// the planted foreign row sorting above it must not shadow it.
		head, ok, err := cs.AbsenceCoverHead(ctx, AbsenceCoverPrefix+base+"/lfs-e", time.Hour)
		if err != nil || !ok || head.Key != keyHigh || head.Bytes != 60 {
			t.Fatalf("head = (%+v, %v, %v), want the e2 row", head, ok, err)
		}
		if _, ok, err := cs.AbsenceCoverHead(ctx, AbsenceCoverPrefix+base+"/nope-e", time.Hour); err != nil || ok {
			t.Fatalf("head of empty prefix = (ok=%v, %v), want absent", ok, err)
		}

		// Expired rows are invisible to both reads (Codex round 6 on
		// PR #132): under a liveness window that already elapsed, the
		// same rows must not be listed (renewal would resurrect a stale
		// reservation admission no longer counts) nor returned as the
		// head (charging would resurrect its GREATEST-pinned total).
		// The rows were stamped above, so a nanosecond window has
		// always elapsed — no sleeping, no backdating.
		if rows, err := cs.ListAbsenceCovers(ctx, "", 10, time.Nanosecond); err != nil || len(rows) != 0 {
			t.Fatalf("expired listing = (%+v, %v), want empty", rows, err)
		}
		if _, ok, err := cs.AbsenceCoverHead(ctx, AbsenceCoverPrefix+base+"/lfs-e", time.Nanosecond); err != nil || ok {
			t.Fatalf("expired head = (ok=%v, %v), want absent", ok, err)
		}

		// Bulk touch (Codex rounds 9+15 on PR #132): LIVE deterministic
		// incarnation rows AND un-folded legacy nonce rows are
		// re-stamped — planted foreign spellings never, an elapsed
		// liveness window touches nothing (expired rows stay dead), a
		// folded mark removes the nonce from the touch, and a re-charge
		// (owner overwritten) un-marks it.
		legacyKey := AbsenceCoverPrefix + base + "/0123456789abcdef0123"
		legacyRaw := reservRowKey(legacyKey + RepairCoverSuffix)
		cover(legacyKey, 30)
		if n, err := cs.RenewAbsenceCovers(ctx, time.Hour); err != nil || n != 4 {
			t.Fatalf("bulk touch = (%d, %v), want 3 deterministic + 1 un-folded legacy row", n, err)
		}
		if n, err := cs.RenewAbsenceCovers(ctx, time.Nanosecond); err != nil || n != 0 {
			t.Fatalf("expired bulk touch = (%d, %v), want zero rows", n, err)
		}
		// The folded mark is a CAS on the LISTED (owner, bytes) — Codex
		// round 18: a drifted owner or bytes (concurrent re-charge)
		// must lose, writing nothing.
		listed, err := cs.ListAbsenceCovers(ctx, "", 10, time.Hour)
		if err != nil {
			t.Fatalf("list for CAS: %v", err)
		}
		listedOwner := ""
		for _, row := range listed {
			if row.RawKey == legacyRaw {
				listedOwner = row.Owner
			}
		}
		if listedOwner == "" {
			t.Fatal("legacy row missing from listing")
		}
		if ok, err := cs.CASAbsenceCoverOwner(ctx, legacyRaw, "some-stale-owner", 30, AbsenceFoldedOwner); err != nil || ok {
			t.Fatalf("stale-owner CAS = (%v, %v), want a miss", ok, err)
		}
		if ok, err := cs.CASAbsenceCoverOwner(ctx, legacyRaw, listedOwner, 31, AbsenceFoldedOwner); err != nil || ok {
			t.Fatalf("stale-bytes CAS = (%v, %v), want a miss", ok, err)
		}
		if n, err := cs.RenewAbsenceCovers(ctx, time.Hour); err != nil || n != 4 {
			t.Fatalf("bulk touch after missed CAS = (%d, %v), want the row still un-folded", n, err)
		}
		if ok, err := cs.CASAbsenceCoverOwner(ctx, legacyRaw, listedOwner, 30, AbsenceFoldedOwner); err != nil || !ok {
			t.Fatalf("matched CAS = (%v, %v), want the fold mark written", ok, err)
		}
		if n, err := cs.RenewAbsenceCovers(ctx, time.Hour); err != nil || n != 3 {
			t.Fatalf("bulk touch after fold mark = (%d, %v), want the marked nonce left to decay", n, err)
		}
		cover(legacyKey, 30) // an old replica re-charges: owner overwritten
		if n, err := cs.RenewAbsenceCovers(ctx, time.Hour); err != nil || n != 4 {
			t.Fatalf("bulk touch after re-charge = (%d, %v), want the un-marked nonce touched again", n, err)
		}
		if err := cs.DeleteAbsenceCover(ctx, legacyKey); err != nil {
			t.Fatalf("clean up legacy row: %v", err)
		}

		// Targeted release: only the named row disappears; re-deleting is
		// a no-op.
		if err := cs.DeleteAbsenceCover(ctx, keyHigh); err != nil {
			t.Fatalf("delete: %v", err)
		}
		if err := cs.DeleteAbsenceCover(ctx, keyHigh); err != nil {
			t.Fatalf("re-delete: %v", err)
		}
		head, ok, err = cs.AbsenceCoverHead(ctx, AbsenceCoverPrefix+base+"/lfs-e", time.Hour)
		if err != nil || !ok || head.Key != keyLow || head.Bytes != 100 {
			t.Fatalf("head after delete = (%+v, %v, %v), want the e1 row", head, ok, err)
		}
	})
}

// TestLapsedAbsenceCoverExists (Codex round 32 on PR #132, security): the
// probe backing evidence-based fence arming — a physically present cover
// row aged past the ttl is lapse evidence; live rows, folded nonces
// (deliberate decay), and foreign reservation spellings are not. The
// probe is deliberately fleet-global (one boolean over the whole cover
// keyspace), so the strict empty/foreign/folded assertions need a
// PRIVATE keyspace: the shared pg lane hosts sibling tests' cover rows,
// all "expired" under a nanosecond window. The sqlite lane is fresh per
// test; the pg lane gets a dedicated database on the package container.
func TestLapsedAbsenceCoverExists(t *testing.T) {
	t.Run("pg", func(t *testing.T) {
		pgTestStore(t) // boot the package container
		ctx := context.Background()
		conn, err := pgx.Connect(ctx, pgTestURL)
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		if _, err := conn.Exec(ctx, "CREATE DATABASE lapsed_probe"); err != nil {
			t.Fatalf("create database: %v", err)
		}
		_ = conn.Close(ctx)
		privURL := strings.Replace(pgTestURL, "/postgres?", "/lapsed_probe?", 1)
		if privURL == pgTestURL {
			t.Fatalf("could not derive a private database URL from %q", pgTestURL)
		}
		s, err := OpenPG(ctx, privURL)
		if err != nil {
			t.Fatalf("open private pg: %v", err)
		}
		t.Cleanup(s.Close)
		if err := s.InitSchema(ctx); err != nil {
			t.Fatalf("init schema: %v", err)
		}
		testLapsedAbsenceCoverExists(t, s)
	})
	t.Run("sqlite", func(t *testing.T) {
		s, _ := openTestSQLite(t)
		testLapsedAbsenceCoverExists(t, s)
	})
}

func testLapsedAbsenceCoverExists(t *testing.T, s Store) {
	{
		cs := coverStore(t, s)
		ctx := context.Background()
		k := newLedgerKit(t, s)
		base := k.base()

		// Empty keyspace: no evidence.
		if ok, err := cs.LapsedAbsenceCoverExists(ctx, time.Nanosecond); err != nil || ok {
			t.Fatalf("empty probe = (%v, %v), want false", ok, err)
		}
		// A foreign spelling under the prefix is never evidence, however
		// old.
		foreign := AbsenceCoverPrefix + base + "/zz-foreign"
		if dec, err := s.ReservePackBytes(ctx, k.group, []Pack{{Key: foreign, Size: 5}}, NewReservationOwner(), 0, ReserveNoFloorGen, 1<<40, time.Hour); err != nil || !dec.OK {
			t.Fatalf("plant foreign row: (%+v, %v)", dec, err)
		}
		if ok, err := cs.LapsedAbsenceCoverExists(ctx, time.Nanosecond); err != nil || ok {
			t.Fatalf("foreign-only probe = (%v, %v), want false", ok, err)
		}
		// A cover row is not evidence within its liveness window, and IS
		// once the window elapses (the row was stamped in the past, so a
		// nanosecond window has always elapsed — no sleeping).
		legacyKey := AbsenceCoverPrefix + base + "/0123456789abcdef0123"
		if err := ReserveRestoreCover(ctx, s, k.group, legacyKey, 30, time.Hour); err != nil {
			t.Fatalf("cover: %v", err)
		}
		if ok, err := cs.LapsedAbsenceCoverExists(ctx, time.Hour); err != nil || ok {
			t.Fatalf("live probe = (%v, %v), want false", ok, err)
		}
		if ok, err := cs.LapsedAbsenceCoverExists(ctx, time.Nanosecond); err != nil || !ok {
			t.Fatalf("elapsed probe = (%v, %v), want true", ok, err)
		}
		// Folding the nonce removes it from evidence: its decay is
		// deliberate retirement, not a lapse.
		listed, err := cs.ListAbsenceCovers(ctx, "", 10, time.Hour)
		if err != nil || len(listed) != 1 {
			t.Fatalf("list = (%+v, %v), want the nonce", listed, err)
		}
		if ok, err := cs.CASAbsenceCoverOwner(ctx, listed[0].RawKey, listed[0].Owner, listed[0].Bytes, AbsenceFoldedOwner); err != nil || !ok {
			t.Fatalf("fold = (%v, %v)", ok, err)
		}
		if ok, err := cs.LapsedAbsenceCoverExists(ctx, time.Nanosecond); err != nil || ok {
			t.Fatalf("folded probe = (%v, %v), want false", ok, err)
		}
	}
}

// TestRetireUnreferencedLFSObject_ReapWatermarkGate (issue #120): the
// retirement transaction refuses slot refunds while the destructive-reap
// watermark is absent or stale, with the distinct sentinel; a fresh
// watermark lets the same verdict commit.
func TestRetireUnreferencedLFSObject_ReapWatermarkGate(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		rs := reclaimStore(t, s)
		ctx := context.Background()
		k := newLedgerKit(t, s)
		key := canonicalLFSKey(k.base(), "ee")
		if err := s.RegisterLFSObject(ctx, k.base(), key, 100); err != nil {
			t.Fatal(err)
		}
		if n, err := rs.MarkLFSObjectsUnreferenced(ctx, k.base(), []string{key}); err != nil || n != 1 {
			t.Fatalf("mark = (%d, %v)", n, err)
		}
		gens := pinCurrentGens(t, s, k.group)

		// No watermark at all: refuse with the sentinel, write nothing.
		if err := rs.DeleteJobScanState(ctx, ScanJobSweep, ScanScopeLFSDestructiveReap); err != nil {
			t.Fatal(err)
		}
		if retired, err := rs.RetireUnreferencedLFSObject(ctx, k.base(), key, gens, 0, time.Hour); !errors.Is(err, ErrReapWatermarkStale) || retired {
			t.Fatalf("retire without watermark = (%v, %v), want ErrReapWatermarkStale", retired, err)
		}
		if has, err := s.HasLFSObject(ctx, k.base(), key); err != nil || !has {
			t.Fatalf("row after refusal = (%v, %v), want intact", has, err)
		}

		// A stale watermark (older than the bound) refuses identically.
		if err := rs.RecordLFSReapComplete(ctx, 1, false); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1100 * time.Millisecond) // cross SQLite's second stamp
		if retired, err := rs.RetireUnreferencedLFSObject(ctx, k.base(), key, gens, 0, time.Millisecond); !errors.Is(err, ErrReapWatermarkStale) || retired {
			t.Fatalf("retire with stale watermark = (%v, %v), want ErrReapWatermarkStale", retired, err)
		}

		// Fresh watermark within the bound: the verdict commits.
		if err := rs.RecordLFSReapComplete(ctx, 1, false); err != nil {
			t.Fatal(err)
		}
		if retired, err := rs.RetireUnreferencedLFSObject(ctx, k.base(), key, gens, 0, time.Hour); err != nil || !retired {
			t.Fatalf("retire with fresh watermark = (%v, %v), want retired", retired, err)
		}
	})
}

// TestRetireUnreferencedLFSObject_LegacyReapGate (Codex round 17 on PR
// #132): a LEGACY-SPELLED row may only retire under a fresh watermark
// whose payload certifies a legacy-armed sweep — an unarmed sweep
// deliberately spares legacy orphan blobs, so its watermark must not back
// their slot refunds (the blob would never be physically reaped and
// upload/retire cycles could grow storage past the object cap). Canonical
// rows retire under either payload.
func TestRetireUnreferencedLFSObject_LegacyReapGate(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		rs := reclaimStore(t, s)
		ctx := context.Background()
		// A fresh watermark whose payload records a legacy-UNARMED sweep.
		if err := rs.RecordLFSReapComplete(ctx, 1, false); err != nil {
			t.Fatal(err)
		}
		k := newLedgerKit(t, s)
		legacy := "lfs/" + k.base() + "/" + strings.Repeat("ab", 32)
		canonical := canonicalLFSKey(k.base(), "cd")
		for _, key := range []string{legacy, canonical} {
			if err := s.RegisterLFSObject(ctx, k.base(), key, 100); err != nil {
				t.Fatal(err)
			}
		}
		if n, err := rs.MarkLFSObjectsUnreferenced(ctx, k.base(), []string{legacy, canonical}); err != nil || n != 2 {
			t.Fatalf("mark = (%d, %v)", n, err)
		}
		gens := pinCurrentGens(t, s, k.group)

		// Unarmed payload: the legacy row defers with the distinct
		// sentinel and stays intact; the canonical row retires.
		if retired, err := rs.RetireUnreferencedLFSObject(ctx, k.base(), legacy, gens, 0, time.Hour); !errors.Is(err, ErrLegacyReapUnarmed) || retired {
			t.Fatalf("retire legacy row unarmed = (%v, %v), want ErrLegacyReapUnarmed", retired, err)
		}
		if has, err := s.HasLFSObject(ctx, k.base(), legacy); err != nil || !has {
			t.Fatalf("legacy row after refusal = (%v, %v), want intact", has, err)
		}
		if retired, err := rs.RetireUnreferencedLFSObject(ctx, k.base(), canonical, gens, 0, time.Hour); err != nil || !retired {
			t.Fatalf("retire canonical row unarmed = (%v, %v), want retired", retired, err)
		}

		// Legacy-armed payload: the same legacy verdict commits.
		if err := rs.RecordLFSReapComplete(ctx, 1, true); err != nil {
			t.Fatal(err)
		}
		if retired, err := rs.RetireUnreferencedLFSObject(ctx, k.base(), legacy, gens, 0, time.Hour); err != nil || !retired {
			t.Fatalf("retire legacy row armed = (%v, %v), want retired", retired, err)
		}
	})
}

// TestPurgeRepoLedger_DropsSettlementCursor (Codex round 1 on PR #132, P2):
// a purge that runs between reclamation passes must drop the repo's parked
// settlement cursor with the rest of its ledger rows — the reclaimer never
// revisits a repo whose registry rows are gone, so nothing else would.
func TestPurgeRepoLedger_DropsSettlementCursor(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		ss := scanStateStore(t, s)
		ctx := context.Background()
		k := newLedgerKit(t, s)
		purger, ok := s.(interface {
			PurgeRepoLedger(ctx context.Context, group []string) (int64, error)
		})
		if !ok {
			t.Fatalf("store %T lacks PurgeRepoLedger", s)
		}
		if err := ss.PutJobScanState(ctx, ScanJobLFSReclaim, ScanScopeReclaimRepo(k.base()), "parked-row-key", 42); err != nil {
			t.Fatal(err)
		}
		if _, err := purger.PurgeRepoLedger(ctx, k.group); err != nil {
			t.Fatalf("PurgeRepoLedger: %v", err)
		}
		if _, ok, err := ss.GetJobScanState(ctx, ScanJobLFSReclaim, ScanScopeReclaimRepo(k.base())); err != nil || ok {
			t.Fatalf("settlement cursor after purge = (ok=%v, %v), want dropped", ok, err)
		}
	})
}

// TestParkRepoJobScanState_PurgeFence (Codex round 26 on PR #132): the
// purge's ledger cleanup deletes the repo's settlement cursor, but a
// park racing the purge — probe observes rows, purge commits, park
// writes — would recreate the row, and a purged repo is never listed
// again, so nothing would ever remove it. The fenced park must skip the
// write once the base is purge-fenced; unfenced bases park normally.
func TestParkRepoJobScanState_PurgeFence(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		ss := scanStateStore(t, s)
		ctx := context.Background()
		k := newLedgerKit(t, s)
		purger, ok := s.(interface {
			FenceRepoPurge(ctx context.Context, base string) error
			PurgeRepoLedger(ctx context.Context, group []string) (int64, error)
		})
		if !ok {
			t.Fatalf("store %T lacks the purge seams", s)
		}
		scope := ScanScopeReclaimRepo(k.base())

		// Unfenced: the park behaves exactly like PutJobScanState,
		// insert and update alike.
		if err := ss.ParkRepoJobScanState(ctx, ScanJobLFSReclaim, scope, "cursor-1", 7, k.base()); err != nil {
			t.Fatalf("park: %v", err)
		}
		if err := ss.ParkRepoJobScanState(ctx, ScanJobLFSReclaim, scope, "cursor-2", 9, k.base()); err != nil {
			t.Fatalf("re-park: %v", err)
		}
		st, ok2, err := ss.GetJobScanState(ctx, ScanJobLFSReclaim, scope)
		if err != nil || !ok2 || st.Payload != "cursor-2" || st.Generation != 9 {
			t.Fatalf("get = (%+v, %v, %v), want cursor-2/9", st, ok2, err)
		}

		// The straggler interleaving: purge fences and cleans up, THEN
		// the in-flight park lands — it must not recreate the row.
		if err := purger.FenceRepoPurge(ctx, k.base()); err != nil {
			t.Fatalf("FenceRepoPurge: %v", err)
		}
		if _, err := purger.PurgeRepoLedger(ctx, k.group); err != nil {
			t.Fatalf("PurgeRepoLedger: %v", err)
		}
		if err := ss.ParkRepoJobScanState(ctx, ScanJobLFSReclaim, scope, "orphan", 11, k.base()); err != nil {
			t.Fatalf("fenced park: %v", err)
		}
		if _, ok, err := ss.GetJobScanState(ctx, ScanJobLFSReclaim, scope); err != nil || ok {
			t.Fatalf("cursor after fenced park = (ok=%v, %v), want absent", ok, err)
		}
	})
}

// TestReservePackBytes_RenewalDegradedFence (Codex round 26 on PR #132,
// security): while the absence reconciler's admission-fence marker is
// present, finite quota-gated admissions fail closed with the distinct
// RenewalDegraded verdict — TTL'd covers may have lapsed un-renewed and
// the strict-TTL reservation SUM under-counts them — while unbounded
// internal reservations (restores, covers) proceed so recovery can
// re-cover. Clearing the marker restores admission.
func TestReservePackBytes_RenewalDegradedFence(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		ss := scanStateStore(t, s)
		ctx := context.Background()
		k := newLedgerKit(t, s)

		if dec := k.reserve([]Pack{k.pack(k.base(), "p1", 10)}, NewReservationOwner(), 0, 1_000); !dec.OK {
			t.Fatalf("unfenced reserve = %+v, want admitted", dec)
		}
		if err := ss.PutJobScanState(ctx, ScanJobAbsence, ScanScopeAbsenceAdmissionFence, "{}", 0); err != nil {
			t.Fatal(err)
		}
		dec := k.reserve([]Pack{k.pack(k.base(), "p2", 10)}, NewReservationOwner(), 0, 1_000)
		if dec.OK || !dec.RenewalDegraded {
			t.Fatalf("fenced finite reserve = %+v, want RenewalDegraded denial", dec)
		}
		if dec := k.reserve([]Pack{k.pack(k.base(), "p3", 10)}, NewReservationOwner(), 0, math.MaxInt64); !dec.OK || dec.RenewalDegraded {
			t.Fatalf("fenced unbounded reserve = %+v, want exempt", dec)
		}
		if err := ss.DeleteJobScanState(ctx, ScanJobAbsence, ScanScopeAbsenceAdmissionFence); err != nil {
			t.Fatal(err)
		}
		if dec := k.reserve([]Pack{k.pack(k.base(), "p4", 10)}, NewReservationOwner(), 0, 1_000); !dec.OK {
			t.Fatalf("post-clear reserve = %+v, want admitted", dec)
		}
	})
}

// TestReservePackBytes_GCRecordsLapseEvidence (Codex round 33 on PR #132,
// P1): ReservePackBytes' self-healing GC prunes a base's expired
// reservation rows at admission time — the very writers the admission
// fence should block would destroy the physical lapse evidence the
// reconciler's first-health-stamp probe arms on. Consuming an expired
// UN-FOLDED absence-cover row must record the durable lapse-evidence
// marker in the same transaction; folded nonces (deliberate decay) and
// foreign spellings under the prefix are never evidence.
func TestReservePackBytes_GCRecordsLapseEvidence(t *testing.T) {
	runRefBackends(t, func(t *testing.T, s Store) {
		ss := scanStateStore(t, s)
		cs := coverStore(t, s)
		ctx := context.Background()
		k := newLedgerKit(t, s)
		base := k.base()
		// The marker scope is fleet-global: clear leftovers from sibling
		// tests sharing the pg database.
		if err := ss.DeleteJobScanState(ctx, ScanJobAbsence, ScanScopeAbsenceLapse); err != nil {
			t.Fatal(err)
		}

		// Expired FOREIGN spelling and FOLDED cover: pruned silently.
		foreign := AbsenceCoverPrefix + base + "/zz-foreign"
		if dec, err := s.ReservePackBytes(ctx, k.group, []Pack{{Key: foreign, Size: 5}}, NewReservationOwner(), 0, ReserveNoFloorGen, 1<<40, time.Hour); err != nil || !dec.OK {
			t.Fatalf("plant foreign row: (%+v, %v)", dec, err)
		}
		foldKey := AbsenceCoverPrefix + base + "/0123456789abcdef0123"
		if err := ReserveRestoreCover(ctx, s, k.group, foldKey, 30, time.Hour); err != nil {
			t.Fatalf("plant folded cover: %v", err)
		}
		// List (the row surface carrying Owner) and fold OUR nonce; the
		// listing is fleet-global on the shared pg lane, so filter by key.
		folded := false
		for after := ""; !folded; {
			listed, err := cs.ListAbsenceCovers(ctx, after, 100, time.Hour)
			if err != nil || len(listed) == 0 {
				t.Fatalf("listing for fold = (%+v, %v), want our nonce", listed, err)
			}
			for _, row := range listed {
				if row.Key != foldKey {
					continue
				}
				if ok, err := cs.CASAbsenceCoverOwner(ctx, row.RawKey, row.Owner, row.Bytes, AbsenceFoldedOwner); err != nil || !ok {
					t.Fatalf("fold = (%v, %v)", ok, err)
				}
				folded = true
			}
			after = listed[len(listed)-1].RawKey
		}
		// A nanosecond TTL expires every existing row: the GC prunes
		// both, and neither is lapse evidence.
		if dec, err := s.ReservePackBytes(ctx, k.group, []Pack{k.pack(base, "g1", 1)}, NewReservationOwner(), 0, ReserveNoFloorGen, 1<<40, time.Nanosecond); err != nil || !dec.OK {
			t.Fatalf("gc reserve 1: (%+v, %v)", dec, err)
		}
		if _, ok, err := ss.GetJobScanState(ctx, ScanJobAbsence, ScanScopeAbsenceLapse); err != nil || ok {
			t.Fatalf("marker after folded/foreign GC = (ok=%v, %v), want absent", ok, err)
		}

		// An expired UN-FOLDED cover: the GC that consumes it must
		// record the marker atomically, payload = the doomed raw key.
		coverKey := AbsenceCoverPrefix + base + "/lfs-e0000000000000000001"
		if err := ReserveRestoreCover(ctx, s, k.group, coverKey, 40, time.Hour); err != nil {
			t.Fatalf("plant un-folded cover: %v", err)
		}
		if dec, err := s.ReservePackBytes(ctx, k.group, []Pack{k.pack(base, "g2", 1)}, NewReservationOwner(), 0, ReserveNoFloorGen, 1<<40, time.Nanosecond); err != nil || !dec.OK {
			t.Fatalf("gc reserve 2: (%+v, %v)", dec, err)
		}
		if _, ok, err := cs.AbsenceCoverHead(ctx, coverKey, time.Hour); err != nil || ok {
			t.Fatalf("cover after GC = (ok=%v, %v), want pruned", ok, err)
		}
		st, ok, err := ss.GetJobScanState(ctx, ScanJobAbsence, ScanScopeAbsenceLapse)
		if err != nil || !ok {
			t.Fatalf("marker after un-folded GC = (ok=%v, %v), want present", ok, err)
		}
		if !strings.HasPrefix(st.Payload, coverKey) {
			t.Fatalf("marker payload = %q, want the consumed cover's raw key", st.Payload)
		}
		// Hygiene for the shared lane.
		if err := ss.DeleteJobScanState(ctx, ScanJobAbsence, ScanScopeAbsenceLapse); err != nil {
			t.Fatal(err)
		}
	})
}

// TestSweepExpiredPackReservations_RecordsLapseEvidence (Codex round 34
// on PR #132, P1): the SCHEDULED reservation sweep is a second consumer
// of the physical lapse evidence beside ReservePackBytes' admission-time
// GC — running after covers expire but before the reconciler's first
// health stamp, it would delete every expired un-folded cover and leave
// the probe nothing to arm on. The sweep must record the durable marker
// atomically; folded nonces and foreign spellings are never evidence.
// The sweep is fleet-global (no repo scoping), so the strict no-marker
// assertions need a private keyspace on the shared pg lane — the
// TestLapsedAbsenceCoverExists pattern.
func TestSweepExpiredPackReservations_RecordsLapseEvidence(t *testing.T) {
	t.Run("pg", func(t *testing.T) {
		pgTestStore(t) // boot the package container
		ctx := context.Background()
		conn, err := pgx.Connect(ctx, pgTestURL)
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		if _, err := conn.Exec(ctx, "CREATE DATABASE sweep_lapse_probe"); err != nil {
			t.Fatalf("create database: %v", err)
		}
		_ = conn.Close(ctx)
		privURL := strings.Replace(pgTestURL, "/postgres?", "/sweep_lapse_probe?", 1)
		if privURL == pgTestURL {
			t.Fatalf("could not derive a private database URL from %q", pgTestURL)
		}
		s, err := OpenPG(ctx, privURL)
		if err != nil {
			t.Fatalf("open private pg: %v", err)
		}
		t.Cleanup(s.Close)
		if err := s.InitSchema(ctx); err != nil {
			t.Fatalf("init schema: %v", err)
		}
		testSweepRecordsLapseEvidence(t, s)
	})
	t.Run("sqlite", func(t *testing.T) {
		s, _ := openTestSQLite(t)
		testSweepRecordsLapseEvidence(t, s)
	})
}

func testSweepRecordsLapseEvidence(t *testing.T, s Store) {
	ss := scanStateStore(t, s)
	cs := coverStore(t, s)
	ctx := context.Background()
	k := newLedgerKit(t, s)
	base := k.base()

	// Expired FOREIGN spelling and FOLDED nonce: swept without evidence.
	foreign := AbsenceCoverPrefix + base + "/zz-foreign"
	if dec, err := s.ReservePackBytes(ctx, k.group, []Pack{{Key: foreign, Size: 5}}, NewReservationOwner(), 0, ReserveNoFloorGen, 1<<40, time.Hour); err != nil || !dec.OK {
		t.Fatalf("plant foreign row: (%+v, %v)", dec, err)
	}
	foldKey := AbsenceCoverPrefix + base + "/0123456789abcdef0123"
	if err := ReserveRestoreCover(ctx, s, k.group, foldKey, 30, time.Hour); err != nil {
		t.Fatalf("plant folded cover: %v", err)
	}
	listed, err := cs.ListAbsenceCovers(ctx, "", 10, time.Hour)
	if err != nil || len(listed) != 1 || listed[0].Key != foldKey {
		t.Fatalf("listing for fold = (%+v, %v), want our nonce", listed, err)
	}
	if ok, err := cs.CASAbsenceCoverOwner(ctx, listed[0].RawKey, listed[0].Owner, listed[0].Bytes, AbsenceFoldedOwner); err != nil || !ok {
		t.Fatalf("fold = (%v, %v)", ok, err)
	}
	if n, err := s.SweepExpiredPackReservations(ctx, 0); err != nil || n != 2 {
		t.Fatalf("sweep folded/foreign = (%d, %v), want 2 rows", n, err)
	}
	if _, ok, err := ss.GetJobScanState(ctx, ScanJobAbsence, ScanScopeAbsenceLapse); err != nil || ok {
		t.Fatalf("marker after folded/foreign sweep = (ok=%v, %v), want absent", ok, err)
	}

	// An expired UN-FOLDED cover: the sweep must record the marker in
	// the same statement/transaction it consumes the evidence.
	coverKey := AbsenceCoverPrefix + base + "/lfs-e0000000000000000001"
	if err := ReserveRestoreCover(ctx, s, k.group, coverKey, 40, time.Hour); err != nil {
		t.Fatalf("plant un-folded cover: %v", err)
	}
	if n, err := s.SweepExpiredPackReservations(ctx, 0); err != nil || n != 1 {
		t.Fatalf("sweep un-folded = (%d, %v), want 1 row", n, err)
	}
	if _, ok, err := cs.AbsenceCoverHead(ctx, coverKey, time.Hour); err != nil || ok {
		t.Fatalf("cover after sweep = (ok=%v, %v), want pruned", ok, err)
	}
	st, ok, err := ss.GetJobScanState(ctx, ScanJobAbsence, ScanScopeAbsenceLapse)
	if err != nil || !ok {
		t.Fatalf("marker after un-folded sweep = (ok=%v, %v), want present", ok, err)
	}
	if !strings.HasPrefix(st.Payload, coverKey) {
		t.Fatalf("marker payload = %q, want the consumed cover's raw key", st.Payload)
	}
}

// TestArmAbsenceAdmissionFence_SerializesWithFiniteAdmissions (Codex
// round 35 on PR #132, P2 security; PostgreSQL-only — SQLite's single
// IMMEDIATE writer is structurally serial): a finite admission holds the
// SHARED fence lock across its fence check and reservation sums, and
// arming takes the EXCLUSIVE side, so neither can slip between the
// other's check and commit. Unbounded internal reservations stay exempt
// so recovery can proceed under an armed fence.
func TestArmAbsenceAdmissionFence_SerializesWithFiniteAdmissions(t *testing.T) {
	s := pgTestStore(t)
	ctx := context.Background()
	k := newLedgerKit(t, s)
	ss := scanStateStore(t, s)
	clearFence := func() {
		if err := ss.DeleteJobScanState(context.Background(), ScanJobAbsence, ScanScopeAbsenceAdmissionFence); err != nil {
			t.Fatalf("clear fence: %v", err)
		}
	}
	clearFence()
	t.Cleanup(clearFence) // the marker is fleet-global on the shared lane

	conn, err := pgx.Connect(ctx, pgTestURL) // the racing peer's session
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	// Direction 1: an in-flight finite admission (shared side held)
	// blocks arming until it commits.
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(hashtext($1), $2)`,
		absenceFenceLockName, absenceFenceLockClass); err != nil {
		t.Fatal(err)
	}
	armCtx, cancelArm := context.WithTimeout(ctx, 500*time.Millisecond)
	err = s.ArmAbsenceAdmissionFence(armCtx, `{"lfs":0,"packs":0}`)
	cancelArm()
	if err == nil {
		t.Fatal("arm under an in-flight finite admission = nil, want blocked until the admission ends")
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.ArmAbsenceAdmissionFence(ctx, `{"lfs":0,"packs":0}`); err != nil {
		t.Fatalf("arm after the admission ended: %v", err)
	}
	clearFence()

	// Direction 2: an arming transaction (exclusive side held) blocks
	// finite admissions — while unbounded internal reservations stay
	// exempt.
	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1), $2)`,
		absenceFenceLockName, absenceFenceLockClass); err != nil {
		t.Fatal(err)
	}
	rctx, cancelReserve := context.WithTimeout(ctx, 500*time.Millisecond)
	_, err = s.ReservePackBytes(rctx, k.group, []Pack{k.pack(k.base(), "f1", 10)}, NewReservationOwner(), 0, ReserveNoFloorGen, 1_000, time.Hour)
	cancelReserve()
	if err == nil {
		t.Fatal("finite reserve under an in-flight arm = nil, want blocked until the arm commits")
	}
	uctx, cancelUnbounded := context.WithTimeout(ctx, 2*time.Second)
	dec, err := s.ReservePackBytes(uctx, k.group, []Pack{k.pack(k.base(), "f2", 10)}, NewReservationOwner(), 0, ReserveNoFloorGen, math.MaxInt64, time.Hour)
	cancelUnbounded()
	if err != nil || !dec.OK {
		t.Fatalf("unbounded reserve under an in-flight arm = (%+v, %v), want exempt", dec, err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if dec, err := s.ReservePackBytes(ctx, k.group, []Pack{k.pack(k.base(), "f3", 10)}, NewReservationOwner(), 0, ReserveNoFloorGen, 1_000, time.Hour); err != nil || !dec.OK {
		t.Fatalf("finite reserve after the arm ended = (%+v, %v), want admitted", dec, err)
	}
}
