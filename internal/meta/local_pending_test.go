package meta

import (
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/ref/s3wal"
)

func TestLocalPendingSyncWork(t *testing.T) {
	for name, open := range map[string]func(*testing.T) Backend{"sqlite": sqliteBackend, "s3wal": s3walBackend} {
		t.Run(name, func(t *testing.T) {
			s := open(t)
			live, ok := s.(LocalPendingStore)
			if !ok {
				t.Fatal("local backend has no bounded live query")
			}
			ctx := t.Context()
			var ids []string
			for i := range 6 {
				id := testRepoID(t, s, fmt.Sprint(i))
				if _, err := s.SetRepoUpstream(ctx, RepoUpstream{RepoID: id}); err != nil {
					t.Fatal(err)
				}
				ids = append(ids, id)
			}
			slices.Sort(ids)
			// Idle and active-only rows must never be reported as new work.
			if _, ok, _, _, err := s.TryBeginPush(ctx, ids[1], "held", time.Minute); err != nil || !ok {
				t.Fatalf("begin push: %v, %v", ok, err)
			}
			if _, ok, _, err := s.TryBeginSync(ctx, ids[1], "held", time.Minute); err != nil || !ok {
				t.Fatalf("begin sync: %v, %v", ok, err)
			}
			if err := s.RequestSync(ctx, ids[2]); err != nil {
				t.Fatal(err)
			}
			if err := s.RequestPush(ctx, ids[3], true); err != nil {
				t.Fatal(err)
			}
			if err := s.RequestSync(ctx, ids[4]); err != nil {
				t.Fatal(err)
			}
			if err := s.RequestPush(ctx, ids[4], false); err != nil {
				t.Fatal(err)
			}
			if err := s.RequestPush(ctx, ids[5], false); err != nil {
				t.Fatal(err)
			}
			if _, ok, _, _, err := s.TryBeginPush(ctx, ids[5], "held", time.Minute); err != nil || !ok {
				t.Fatal(err)
			}
			if err := s.RequestPush(ctx, ids[5], true); err != nil {
				t.Fatal(err)
			}
			want := []PendingSyncWork{{ids[2], true, false}, {ids[3], false, true}, {ids[4], true, true}, {ids[5], false, true}}
			var got []PendingSyncWork
			after := ""
			for range 4 {
				page, err := live.ListPendingSyncWork(ctx, after, 2)
				if err != nil {
					t.Fatal(err)
				}
				if len(page) > 2 {
					t.Fatalf("unbounded page: %+v", page)
				}
				if len(page) == 0 {
					break
				}
				for _, row := range page {
					if row.RepoID <= after {
						t.Fatalf("cursor did not advance: %+v after %q", row, after)
					}
					after = row.RepoID
				}
				got = append(got, page...)
			}
			if !slices.Equal(got, want) {
				t.Fatalf("pages=%+v, want %+v", got, want)
			}
			up, err := s.GetRepoUpstream(ctx, ids[5])
			if err != nil || up.PushPending != 2 || !up.PushSyncing {
				t.Fatalf("query mutated active force intent: %+v, %v", up, err)
			}
			if err := s.DeleteRepoUpstream(ctx, ids[4]); err != nil {
				t.Fatal(err)
			}
			page, err := live.ListPendingSyncWork(ctx, ids[3], 2)
			if err != nil || !slices.Equal(page, want[3:]) {
				t.Fatalf("detached page=%+v, err=%v", page, err)
			}
		})
	}
}

// Report the cost of the existing-schema scan, including S3WAL's whole-document
// read. A small result page is not a claim of constant-cost metadata access.
func BenchmarkLocalPendingSyncWork(b *testing.B) {
	for _, backend := range []string{"sqlite", "s3wal"} {
		for _, count := range []int{64, 1024} {
			b.Run(fmt.Sprintf("%s/rows=%d", backend, count), func(b *testing.B) {
				var st Backend
				if backend == "sqlite" {
					s, err := OpenSQLite(b.Context(), "sqlite://"+filepath.Join(b.TempDir(), "meta.db"))
					if err != nil {
						b.Fatal(err)
					}
					st = s
				} else {
					st = NewS3WAL(s3wal.NewMemCAS())
				}
				defer st.Close()
				if err := st.InitSchema(b.Context()); err != nil {
					b.Fatal(err)
				}
				if _, err := st.UpsertOrgWithKeys(b.Context(), "bench", []string{"-----BEGIN PUBLIC KEY-----\ndummy\n-----END PUBLIC KEY-----\n"}); err != nil {
					b.Fatal(err)
				}
				for i := range count {
					r, err := st.CreateRepo(b.Context(), "bench", fmt.Sprintf("https://example.com/%d.git", i), "main")
					if err != nil {
						b.Fatal(err)
					}
					if _, err := st.SetRepoUpstream(b.Context(), RepoUpstream{RepoID: r.RepoID}); err != nil {
						b.Fatal(err)
					}
					if err := st.RequestPush(b.Context(), r.RepoID, false); err != nil {
						b.Fatal(err)
					}
				}
				live := st.(LocalPendingStore)
				b.ReportAllocs()
				for b.Loop() {
					page, err := live.ListPendingSyncWork(b.Context(), "", 16)
					if err != nil || len(page) != 16 {
						b.Fatalf("page size=%d err=%v", len(page), err)
					}
				}
			})
		}
	}
}
