// Ambiguous-PUT recovery and cache-validation tests for the s3wal meta
// backend (plan §2.3, simplified to the single meta object): a faulted
// conditional PUT may or may not have landed, and the mutate loop must
// resolve the ambiguity via the writer nonce and the per-method recovery
// predicate — never double-applying counters and never fabricating wins.
// A competing writer is interleaved between the faulted PUT and the
// recovery read (MemCASHooks.AfterPut) so the writer-nonce fast path
// cannot answer and the PRED/ID predicates are actually exercised.

package meta

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/coder/code-storage/internal/ref/s3wal"
)

// newS3WALMetaPair returns one shared MemCAS and two independent meta
// stores over it (the ambiguity victim and the competing writer).
func newS3WALMetaPair(t *testing.T) (*s3wal.MemCAS, *S3WAL, *S3WAL) {
	t.Helper()
	cas := s3wal.NewMemCAS()
	a := NewS3WAL(cas)
	b := NewS3WAL(cas)
	t.Cleanup(func() { a.Close(); b.Close() })
	return cas, a, b
}

// armAmbiguousPut faults exactly ONE conditional PUT on the meta object
// with f, then interleaves the competing write before the victim's
// recovery read. Hooks are cleared via t.Cleanup.
func armAmbiguousPut(t *testing.T, cas *s3wal.MemCAS, f s3wal.PutFault, interleave func()) {
	t.Helper()
	faulted := false
	cas.Hooks.PutFault = func(op, key string) s3wal.PutFault {
		if faulted || key != s3walMetaKey {
			return s3wal.FaultNone
		}
		faulted = true
		return f
	}
	interleaved := false
	cas.Hooks.AfterPut = func(op, key string) {
		if !faulted || interleaved {
			return // only after the faulted PUT; guard reentrancy
		}
		interleaved = true
		interleave()
	}
	t.Cleanup(func() { cas.Hooks = s3wal.MemCASHooks{} })
}

// seedS3WALUpstream seeds org + repo + upstream through s and returns the
// repo ID.
func seedS3WALUpstream(t *testing.T, s *S3WAL) string {
	t.Helper()
	ctx := context.Background()
	if _, err := s.UpsertOrgWithKeys(ctx, "org", []string{"pem"}); err != nil {
		t.Fatalf("seed org: %v", err)
	}
	repo, err := s.CreateRepo(ctx, "org", "https://example.com/"+t.Name()+".git", "main")
	if err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	if _, err := s.SetRepoUpstream(ctx, RepoUpstream{RepoID: repo.RepoID, Provider: "github", Owner: "acme", Name: "demo", UpstreamHost: "github.com"}); err != nil {
		t.Fatalf("seed upstream: %v", err)
	}
	return repo.RepoID
}

// TestS3WALMetaRecoveryCreateRepo: an ambiguous CreateRepo PUT — landed or
// dropped, superseded by a competitor either way — must yield exactly one
// stored row whose ID matches the returned one.
func TestS3WALMetaRecoveryCreateRepo(t *testing.T) {
	for _, tc := range []struct {
		name  string
		fault s3wal.PutFault
	}{
		{"applied", s3wal.FaultAmbiguousApplied},
		{"dropped", s3wal.FaultAmbiguousDropped},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cas, a, b := newS3WALMetaPair(t)
			if _, err := a.UpsertOrgWithKeys(ctx, "org", []string{"pem"}); err != nil {
				t.Fatalf("seed org: %v", err)
			}
			armAmbiguousPut(t, cas, tc.fault, func() {
				if _, err := b.UpsertOrgWithKeys(ctx, "interloper", []string{"pem"}); err != nil {
					t.Errorf("interleaved write: %v", err)
				}
			})
			created, err := a.CreateRepo(ctx, "org", "https://example.com/amb.git", "main")
			if err != nil {
				t.Fatalf("CreateRepo through ambiguity: %v", err)
			}
			got, err := a.GetRepoByURL(ctx, "org", "https://example.com/amb.git")
			if err != nil || got.RepoID != created.RepoID {
				t.Fatalf("stored row = (%+v, %v), want the returned ID %s", got, err, created.RepoID)
			}
			// Exactly one row: a re-create is a clean 409, never a dup.
			if _, err := a.CreateRepo(ctx, "org", "https://example.com/amb.git", "main"); !errors.Is(err, ErrAlreadyExists) {
				t.Fatalf("re-create err = %v, want ErrAlreadyExists", err)
			}
		})
	}
}

// TestS3WALMetaRecoveryTryBeginSync: an ambiguous claim PUT must resolve
// to exactly one live claim under the caller's holder with the attempt's
// expiry stamp — the (holder, expiry) PRED — and never consume counters.
func TestS3WALMetaRecoveryTryBeginSync(t *testing.T) {
	for _, tc := range []struct {
		name  string
		fault s3wal.PutFault
	}{
		{"applied", s3wal.FaultAmbiguousApplied},
		{"dropped", s3wal.FaultAmbiguousDropped},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cas, a, b := newS3WALMetaPair(t)
			repoID := seedS3WALUpstream(t, a)
			if err := a.RequestSync(ctx, repoID); err != nil {
				t.Fatal(err)
			}
			armAmbiguousPut(t, cas, tc.fault, func() {
				if _, err := b.UpsertOrgWithKeys(ctx, "interloper", []string{"pem"}); err != nil {
					t.Errorf("interleaved write: %v", err)
				}
			})
			up, claimed, stolenFrom, err := a.TryBeginSync(ctx, repoID, "holder-amb", time.Minute)
			if err != nil || !claimed || stolenFrom != "" {
				t.Fatalf("claim through ambiguity = (%v, %q, %v), want clean claim", claimed, stolenFrom, err)
			}
			if !up.Syncing || up.SyncClaimHolder != "holder-amb" || up.SyncPending {
				t.Fatalf("claimed row = %+v, want holder-amb with consumed pending", up)
			}
			// The claim is real: a competing holder is refused.
			if _, claimed, _, err := b.TryBeginSync(ctx, repoID, "holder-other", time.Minute); err != nil || claimed {
				t.Fatalf("contending claim = (%v, %v), want refused", claimed, err)
			}
			// And it finishes exactly once.
			fin, err := a.FinishSync(ctx, repoID, "holder-amb", true)
			if err != nil || fin.RunCount != 1 || fin.SuccessCount != 1 {
				t.Fatalf("finish = (%+v, %v), want one counted run", fin, err)
			}
		})
	}
}

// TestS3WALMetaRecoveryFinishSync: an ambiguous finish PUT must advance
// the counters exactly once (the counter-advancement PRED), for both the
// landed and dropped windows.
func TestS3WALMetaRecoveryFinishSync(t *testing.T) {
	for _, tc := range []struct {
		name  string
		fault s3wal.PutFault
	}{
		{"applied", s3wal.FaultAmbiguousApplied},
		{"dropped", s3wal.FaultAmbiguousDropped},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cas, a, b := newS3WALMetaPair(t)
			repoID := seedS3WALUpstream(t, a)
			if _, claimed, _, err := a.TryBeginSync(ctx, repoID, "holder-fin", time.Minute); err != nil || !claimed {
				t.Fatalf("claim = (%v, %v), want claimed", claimed, err)
			}
			armAmbiguousPut(t, cas, tc.fault, func() {
				if _, err := b.UpsertOrgWithKeys(ctx, "interloper", []string{"pem"}); err != nil {
					t.Errorf("interleaved write: %v", err)
				}
			})
			fin, err := a.FinishSync(ctx, repoID, "holder-fin", true)
			if err != nil {
				t.Fatalf("FinishSync through ambiguity: %v", err)
			}
			if fin.Syncing || fin.RunCount != 1 || fin.SuccessCount != 1 || fin.SyncClaimHolder != "" {
				t.Fatalf("finished row = %+v, want exactly one counted run with a cleared lease", fin)
			}
			got, err := a.GetRepoUpstream(ctx, repoID)
			if err != nil || got.RunCount != 1 || got.SuccessCount != 1 {
				t.Fatalf("stored row = (%+v, %v), want counters advanced exactly once", got, err)
			}
		})
	}
}

// TestS3WALMetaCacheValidation covers the 304-validated read cache: quiet
// reads revalidate the cached body with conditional GETs (ErrNotModified,
// exactly one full-body fetch), and a foreign write is picked up by the
// next validation — the cache never serves without revalidation.
func TestS3WALMetaCacheValidation(t *testing.T) {
	ctx := context.Background()
	inner := s3wal.NewMemCAS()
	counting := &countingCAS{ConditionalStore: inner}
	a := NewS3WAL(counting)
	b := NewS3WAL(inner) // foreign writer bypassing a's cache
	t.Cleanup(func() { a.Close(); b.Close() })

	if _, err := a.UpsertOrgWithKeys(ctx, "org", []string{"pem"}); err != nil {
		t.Fatal(err)
	}
	fullBodies := counting.fullBodies
	for range 3 {
		names, err := a.OrgNames(ctx)
		if err != nil || len(names) != 1 {
			t.Fatalf("OrgNames = (%v, %v), want [org]", names, err)
		}
	}
	if counting.notModified < 3 {
		t.Errorf("notModified = %d, want >= 3 (quiet reads must revalidate the cache, not refetch)", counting.notModified)
	}
	if counting.fullBodies != fullBodies {
		t.Errorf("full-body fetches advanced %d -> %d across quiet reads, want none", fullBodies, counting.fullBodies)
	}

	// A foreign write invalidates on the NEXT validation — no stale serve.
	if _, err := b.UpsertOrgWithKeys(ctx, "org-b", []string{"pem"}); err != nil {
		t.Fatal(err)
	}
	names, err := a.OrgNames(ctx)
	if err != nil || len(names) != 2 {
		t.Fatalf("OrgNames after foreign write = (%v, %v), want both orgs", names, err)
	}
	if counting.fullBodies != fullBodies+1 {
		t.Errorf("full-body fetches = %d, want exactly one refetch after the foreign write", counting.fullBodies-fullBodies)
	}
}

// countingCAS decorates a ConditionalStore with read-path counters.
type countingCAS struct {
	s3wal.ConditionalStore
	fullBodies  int // Get calls + GetIfChanged calls that returned a body
	notModified int // GetIfChanged calls answered by ErrNotModified
}

func (c *countingCAS) Get(ctx context.Context, key string) (s3wal.Object, error) {
	obj, err := c.ConditionalStore.Get(ctx, key)
	if err == nil {
		c.fullBodies++
	}
	return obj, err
}

func (c *countingCAS) GetIfChanged(ctx context.Context, key, etag string) (s3wal.Object, error) {
	obj, err := c.ConditionalStore.GetIfChanged(ctx, key, etag)
	switch {
	case errors.Is(err, s3wal.ErrNotModified):
		c.notModified++
	case err == nil:
		c.fullBodies++
	}
	return obj, err
}
