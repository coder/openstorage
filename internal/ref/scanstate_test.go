// Fleet-persistent scan-state row tests (issues #120/#121/#125), fanned
// out over both backends via runRefBackends.
package ref

import (
	"context"
	"errors"
	"testing"
	"time"
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
	ListAbsenceCovers(ctx context.Context, afterRaw string, limit int) ([]AbsenceCoverRow, error)
	AbsenceCoverHead(ctx context.Context, prefix string) (AbsenceCoverRow, bool, error)
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

		// Paged listing: one row per page, cursor round-trips opaquely.
		var got []AbsenceCoverRow
		after := ""
		for {
			rows, err := cs.ListAbsenceCovers(ctx, after, 1)
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

		// Head lookup: the highest incarnation under the class prefix.
		head, ok, err := cs.AbsenceCoverHead(ctx, AbsenceCoverPrefix+base+"/lfs-e")
		if err != nil || !ok || head.Key != keyHigh || head.Bytes != 60 {
			t.Fatalf("head = (%+v, %v, %v), want the e2 row", head, ok, err)
		}
		if _, ok, err := cs.AbsenceCoverHead(ctx, AbsenceCoverPrefix+base+"/nope-e"); err != nil || ok {
			t.Fatalf("head of empty prefix = (ok=%v, %v), want absent", ok, err)
		}

		// Targeted release: only the named row disappears; re-deleting is
		// a no-op.
		if err := cs.DeleteAbsenceCover(ctx, keyHigh); err != nil {
			t.Fatalf("delete: %v", err)
		}
		if err := cs.DeleteAbsenceCover(ctx, keyHigh); err != nil {
			t.Fatalf("re-delete: %v", err)
		}
		head, ok, err = cs.AbsenceCoverHead(ctx, AbsenceCoverPrefix+base+"/lfs-e")
		if err != nil || !ok || head.Key != keyLow || head.Bytes != 100 {
			t.Fatalf("head after delete = (%+v, %v, %v), want the e1 row", head, ok, err)
		}
	})
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
		if err := rs.PutJobScanState(ctx, ScanJobSweep, ScanScopeLFSDestructiveReap, "", 0); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1100 * time.Millisecond) // cross SQLite's second stamp
		if retired, err := rs.RetireUnreferencedLFSObject(ctx, k.base(), key, gens, 0, time.Millisecond); !errors.Is(err, ErrReapWatermarkStale) || retired {
			t.Fatalf("retire with stale watermark = (%v, %v), want ErrReapWatermarkStale", retired, err)
		}

		// Fresh watermark within the bound: the verdict commits.
		if err := rs.PutJobScanState(ctx, ScanJobSweep, ScanScopeLFSDestructiveReap, "", 0); err != nil {
			t.Fatal(err)
		}
		if retired, err := rs.RetireUnreferencedLFSObject(ctx, k.base(), key, gens, 0, time.Hour); err != nil || !retired {
			t.Fatalf("retire with fresh watermark = (%v, %v), want retired", retired, err)
		}
	})
}
