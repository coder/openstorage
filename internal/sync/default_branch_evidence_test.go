package sync

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	stdsync "sync"
	"testing"
	"time"
)

// evidenceStore is a MemStore whose upstream row reads flagged until the
// flag is adopted away. It adopts and keeps the reconcile evidence like the
// meta adapter does on PostgreSQL.
type evidenceStore struct {
	*MemStore
	mu       stdsync.Mutex
	flagged  bool
	adoptErr error // the adoption write fails once flagged
	adopted  []string
	checked  []int
}

func (s *evidenceStore) mark(up Upstream) Upstream {
	s.mu.Lock()
	defer s.mu.Unlock()
	up.DefaultBranchOmitted = s.flagged
	return up
}

func (s *evidenceStore) BeginSync(ctx context.Context, repoID, holder string, lease time.Duration) (Upstream, string, error) {
	up, stolen, err := s.MemStore.BeginSync(ctx, repoID, holder, lease)
	return s.mark(up), stolen, err
}

func (s *evidenceStore) FinishSync(ctx context.Context, repoID, holder string, success bool) (Upstream, error) {
	up, err := s.MemStore.FinishSync(ctx, repoID, holder, success)
	return s.mark(up), err
}

func (s *evidenceStore) AdoptDefaultBranch(_ context.Context, _, branch string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.flagged {
		return false, nil
	}
	if s.adoptErr != nil {
		return false, s.adoptErr
	}
	s.flagged = false
	s.adopted = append(s.adopted, branch)
	return true, nil
}

func (s *evidenceStore) RecordDefaultBranchChecked(_ context.Context, _ string, successCount int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checked = append(s.checked, successCount)
	return nil
}

// TestWorkerRecordsDefaultBranchEvidence (issue #609): a worker with the
// adoption rule leaves evidence at its run's success count when the repo
// stays flagged, whether the run resolved no HEAD or failed, so the
// reconcile job asks for another sync only after a worker without the rule
// runs one. An adopted or unflagged repo needs none. A failed adoption
// write, or a failure after the HEAD resolved and before the adoption,
// leaves none: the reconcile job must ask for the retry.
func TestWorkerRecordsDefaultBranchEvidence(t *testing.T) {
	for _, tc := range []struct {
		name        string
		flagged     bool
		adoptErr    error
		tokenErr    bool   // the loopback token fails after the HEAD resolved
		head        string // upstream HEAD branch; "" leaves main
		host        string // "" uses the seeded upstream; else a missing one
		wantAdopted []string
		wantChecked []int
	}{
		{name: "the HEAD resolves", flagged: true, head: "trunk", wantAdopted: []string{"trunk"}},
		{name: "an unresolvable HEAD", flagged: true, head: "missing", wantChecked: []int{1}},
		{name: "a failed run", flagged: true, host: "file:///nonexistent-upstream", wantChecked: []int{0}},
		{name: "no flag", flagged: false, head: "trunk"},
		{name: "a failure after the HEAD resolved", flagged: true, head: "trunk", tokenErr: true},
		{name: "a failed adoption write", flagged: true, head: "trunk", adoptErr: errors.New("transient write failure")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newWorkerFixture(t)
			store := &evidenceStore{MemStore: f.store, flagged: tc.flagged, adoptErr: tc.adoptErr}
			f.worker.Upstreams = store
			seedUpstream(t, f.upstreamDir, "own", "proj")
			bare := filepath.Join(f.upstreamDir, "own", "proj.git")
			testGit(t, bare, "branch", "trunk", "main")
			if tc.head != "" {
				testGit(t, bare, "symbolic-ref", "HEAD", "refs/heads/"+tc.head)
			}
			if tc.tokenErr {
				f.worker.Loopback.Token = func(context.Context, string, RepoInfo, time.Duration, string) (string, error) {
					return "", errors.New("token service down")
				}
			}
			f.addLoopbackRepo(t, "repo_aa")
			host := "file://" + f.upstreamDir
			if tc.host != "" {
				host = tc.host
			}
			f.putUpstream(t, "repo_aa", "own", "proj", host)

			if err := f.worker.Run(t.Context(), Job{Org: "acme", RepoID: "repo_aa"}); err != nil && tc.host == "" && !tc.tokenErr {
				t.Fatalf("Run: %v", err)
			}
			store.mu.Lock()
			defer store.mu.Unlock()
			if !slices.Equal(store.adopted, tc.wantAdopted) || !slices.Equal(store.checked, tc.wantChecked) {
				t.Fatalf("adopted %v, evidence %v; want %v and %v", store.adopted, store.checked, tc.wantAdopted, tc.wantChecked)
			}
		})
	}
}
