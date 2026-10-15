package meta

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/ref/s3wal"
)

// Every test in this package runs with the shared-view check on: any
// viewDoc caller that writes into the shared document trips an assertion
// at the next 304 read (issue #470).
func init() { checkSharedView = true }

// A 304 read answers the shared decoded view without decoding again; a
// change on the store, or our own write, replaces it.
func TestS3WALSharedView(t *testing.T) {
	ctx := context.Background()
	cas := s3wal.NewMemCAS()
	a, b := NewS3WAL(cas), NewS3WAL(cas)
	t.Cleanup(func() { a.Close(); b.Close() })
	if _, err := a.UpsertOrgWithKeys(ctx, "org", []string{"pem"}); err != nil {
		t.Fatal(err)
	}
	first, err := a.viewDoc(ctx)
	if err != nil {
		t.Fatal(err)
	}
	second, err := a.viewDoc(ctx)
	if err != nil || second != first {
		t.Fatalf("second quiet read = %p, %v; want the shared view %p", second, err, first)
	}
	// A mutation gets its own copy, never the shared view.
	if _, err := a.CreateRepo(ctx, "org", "own-write", "main"); err != nil {
		t.Fatal(err)
	}
	afterOwn, err := a.viewDoc(ctx)
	if err != nil || afterOwn == first || len(afterOwn.Repos) != 1 || len(first.Repos) != 0 {
		t.Fatalf("after our write: view %p with %d repos (old view %d repos), %v", afterOwn, len(afterOwn.Repos), len(first.Repos), err)
	}
	// Another process's write replaces the view too.
	if _, err := b.CreateRepo(ctx, "org", "other-write", "main"); err != nil {
		t.Fatal(err)
	}
	afterOther, err := a.viewDoc(ctx)
	if err != nil || afterOther == afterOwn || len(afterOther.Repos) != 2 {
		t.Fatalf("after another writer: view with %d repos, %v; want 2", len(afterOther.Repos), err)
	}
}

// The check catches a viewDoc caller that writes into the shared view,
// whatever comes next: a quiet 304 read, a fresh read after another
// writer, or our own write (issue #503). Each failure is reported before
// the panic, for the e2e harness.
func TestS3WALSharedViewMutationAsserts(t *testing.T) {
	for _, tc := range []struct {
		name string
		next func(ctx context.Context, st, other *S3WAL) error
	}{
		{"quiet read", func(ctx context.Context, st, _ *S3WAL) error {
			_, err := st.viewDoc(ctx)
			return err
		}},
		{"fresh read after another writer", func(ctx context.Context, st, other *S3WAL) error {
			if _, err := other.CreateRepo(ctx, "org", "other-write", "main"); err != nil {
				return err
			}
			_, err := st.viewDoc(ctx)
			return err
		}},
		{"own write", func(ctx context.Context, st, _ *S3WAL) error {
			_, err := st.CreateRepo(ctx, "org", "own-write", "main")
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cas := s3wal.NewMemCAS()
			st, other := NewS3WAL(cas), NewS3WAL(cas)
			t.Cleanup(func() { st.Close(); other.Close() })
			if _, err := st.UpsertOrgWithKeys(ctx, "org", []string{"pem"}); err != nil {
				t.Fatal(err)
			}
			view, err := st.viewDoc(ctx) // a 304 read: installs the shared view
			if err != nil {
				t.Fatal(err)
			}
			var reported []string
			saved := reportViewCheck
			reportViewCheck = func(msg string) { reported = append(reported, msg) }
			t.Cleanup(func() { reportViewCheck = saved })
			view.NextOrgID += 100 // a buggy caller
			defer func() {
				if recover() == nil {
					t.Fatal("mutated shared view went undetected")
				}
				if len(reported) != 1 || !strings.Contains(reported[0], "mutated the shared view") {
					t.Fatalf("reported %q, want one view-check failure", reported)
				}
			}()
			_ = tc.next(ctx, st, other)
		})
	}
}

// BenchmarkS3WALQuietRead measures one quiet (304) read of the metadata
// document by a read-only caller, as the sync sweep does once a second.
// "decode" is the cost before #470 (decode on every 304), "shared" after.
// The sweep modes time one whole quiet sweep pass (ListLocalWork, nothing
// due): "sweep-decode" as before #470 (each pass decodes, then scans every
// lane), "sweep" after it (shared view, then the scan), and "sweep-checked"
// with the view check that tests and openstorage_viewcheck builds turn on
// (issue #497). Release builds run "sweep".
func BenchmarkS3WALQuietRead(b *testing.B) {
	saved := checkSharedView
	b.Cleanup(func() { checkSharedView = saved })
	for _, n := range []int{1000, 10000} {
		body := benchMetaDoc(b, n)
		for _, mode := range []string{"decode", "shared", "sweep-decode", "sweep", "sweep-checked"} {
			b.Run(fmt.Sprintf("repos=%d/%s", n, mode), func(b *testing.B) {
				cas := s3wal.NewMemCAS()
				if _, err := cas.Create(context.Background(), s3walMetaKey, body); err != nil {
					b.Fatal(err)
				}
				// Before NewS3WAL: its key refresher reads the flag.
				checkSharedView = mode == "sweep-checked"
				st := NewS3WAL(cas)
				defer st.Close()
				read := func() {
					if strings.HasPrefix(mode, "sweep") {
						if mode == "sweep-decode" {
							st.mu.Lock()
							st.cacheView = nil // the 304 decodes again, as before #470
							st.mu.Unlock()
						}
						if _, err := (s3walLocalSchedule{st}).ListLocalWork(context.Background(), "org", "", 16); err != nil {
							b.Fatal(err)
						}
						return
					}
					st.mu.Lock()
					_, _, err := st.readDocShared(context.Background(), mode == "shared")
					st.mu.Unlock()
					if err != nil {
						b.Fatal(err)
					}
				}
				read()
				read()
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					read()
				}
				b.ReportMetric(float64(len(body)), "doc_bytes")
			})
		}
	}
}

// benchMetaDoc encodes a v2 document with n linked repositories.
func benchMetaDoc(b *testing.B, n int) []byte {
	b.Helper()
	doc := newS3WALMetaDoc()
	doc.Seq, doc.Writer, doc.NextOrgID = 1, "0123456789abcdef0123456789abcdef", 2
	doc.Orgs = map[string]*s3walMetaOrg{"org": {ID: 1, PublicKeys: []string{"pem"}}}
	doc.Repos = make(map[string]*s3walMetaRepo, n)
	doc.Upstreams = make(map[string]*s3walMetaUpstream, n)
	for i := range n {
		id := fmt.Sprintf("repo_%012x", i)
		doc.Repos[id] = &s3walMetaRepo{OrgID: 1, URL: "https://example.com/acme/" + id + ".git", DefaultBranch: "main", CreatedAt: 1, CreateState: int(CreateStateComplete)}
		doc.Upstreams[id] = &s3walMetaUpstream{Provider: "github", Owner: "acme", Name: id, UpstreamHost: "github.com", CreatedAt: 1, RunCount: 5, SuccessCount: 5}
	}
	body, err := doc.encodeNext()
	if err != nil {
		b.Fatal(err)
	}
	return body
}
