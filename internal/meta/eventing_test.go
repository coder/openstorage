// EventingStore conformance tests (eventing.go), fanned out over both
// backends via runBackends like the Store suite in pg_test.go. The pg lane
// shares one container-wide store, so per-repo rows isolate with
// test-unique repo IDs (t.Name() is subtest-unique).

package meta

import (
	"errors"
	"regexp"
	"sync"
	"testing"
	"time"
)

// testRepoID seeds a LIVE repo row and returns its server-generated,
// test-unique ID — credential/upstream inserts are guarded on a live repos
// row (issue #52), so eventing rows can no longer attach to made-up IDs.
func testRepoID(t *testing.T, s Backend, suffix string) string {
	t.Helper()
	org := seedOrg(t, s)
	r, err := s.CreateRepo(t.Context(), org, "https://example.com/"+t.Name()+suffix+".git", "main")
	if err != nil {
		t.Fatalf("testRepoID: CreateRepo: %v", err)
	}
	return r.RepoID
}

// testClaimHolder is the claim token tests use where holder identity is
// not itself under test; a 1-minute lease keeps claims unexpired for the
// duration of any test that is not exercising expiry.
const testClaimHolder = "test-holder"

func TestWebhooks(t *testing.T) { runBackends(t, testWebhooks) }

func testWebhooks(t *testing.T, s Backend) {
	ctx := t.Context()
	org := seedOrg(t, s)

	// Unknown org -> 404 on every org-scoped method.
	if _, err := s.CreateWebhook(ctx, "no-such-org", "https://x.test/h", "sec"); !errors.Is(err, ErrNotFound) {
		t.Errorf("create unknown-org err = %v, want ErrNotFound", err)
	}
	if _, err := s.ListWebhooks(ctx, "no-such-org"); !errors.Is(err, ErrNotFound) {
		t.Errorf("list unknown-org err = %v, want ErrNotFound", err)
	}
	if err := s.DeleteWebhook(ctx, "no-such-org", "wh_0000000000000000"); !errors.Is(err, ErrNotFound) {
		t.Errorf("delete unknown-org err = %v, want ErrNotFound", err)
	}

	empty, err := s.ListWebhooks(ctx, org)
	if err != nil {
		t.Fatalf("ListWebhooks empty: %v", err)
	}
	if empty == nil || len(empty) != 0 {
		t.Errorf("empty list = %#v, want empty non-nil slice", empty)
	}

	w1, err := s.CreateWebhook(ctx, org, "https://x.test/hook1", "s3cret-1")
	if err != nil {
		t.Fatalf("CreateWebhook: %v", err)
	}
	if !regexp.MustCompile(`\Awh_[0-9a-f]{16}\z`).MatchString(w1.ID) {
		t.Errorf("ID = %q, want wh_<16 hex>", w1.ID)
	}
	if w1.OrgID <= 0 || w1.URL != "https://x.test/hook1" || w1.Secret != "s3cret-1" {
		t.Errorf("bad webhook row: %+v", w1)
	}
	if w1.CreatedAt.IsZero() || w1.CreatedAt.Location() != time.UTC {
		t.Errorf("CreatedAt = %v, want non-zero UTC", w1.CreatedAt)
	}
	w2, err := s.CreateWebhook(ctx, org, "https://x.test/hook2", "s3cret-2")
	if err != nil {
		t.Fatalf("CreateWebhook: %v", err)
	}

	got, err := s.ListWebhooks(ctx, org)
	if err != nil {
		t.Fatalf("ListWebhooks: %v", err)
	}
	// Registration order (created_at, id).
	want := []Webhook{w1, w2}
	if w2.CreatedAt.Before(w1.CreatedAt) || (w2.CreatedAt.Equal(w1.CreatedAt) && w2.ID < w1.ID) {
		want = []Webhook{w2, w1}
	}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("list = %+v, want %+v", got, want)
	}

	// GetWebhook resolves by ID regardless of org (delivery jobs carry only
	// the ID).
	byID, err := s.GetWebhook(ctx, w1.ID)
	if err != nil {
		t.Fatalf("GetWebhook: %v", err)
	}
	if byID != w1 {
		t.Errorf("get = %+v, want %+v", byID, w1)
	}
	if _, err := s.GetWebhook(ctx, "wh_0000000000000000"); !errors.Is(err, ErrNotFound) {
		t.Errorf("get absent err = %v, want ErrNotFound", err)
	}

	// Org scoping: another org's list never shows these hooks, and deleting
	// across orgs is a 404.
	other, err := s.UpsertOrgWithKeys(ctx, t.Name()+"-other", []string{"pem"})
	if err != nil {
		t.Fatalf("seed other org: %v", err)
	}
	otherHooks, err := s.ListWebhooks(ctx, other.Name)
	if err != nil {
		t.Fatalf("ListWebhooks other org: %v", err)
	}
	if len(otherHooks) != 0 {
		t.Errorf("other org sees %+v, want none", otherHooks)
	}
	if err := s.DeleteWebhook(ctx, other.Name, w1.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("cross-org delete err = %v, want ErrNotFound", err)
	}

	if err := s.DeleteWebhook(ctx, org, w1.ID); err != nil {
		t.Fatalf("DeleteWebhook: %v", err)
	}
	if _, err := s.GetWebhook(ctx, w1.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("get deleted err = %v, want ErrNotFound", err)
	}
	if err := s.DeleteWebhook(ctx, org, w1.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("double delete err = %v, want ErrNotFound", err)
	}
	if got, err := s.ListWebhooks(ctx, org); err != nil || len(got) != 1 || got[0] != w2 {
		t.Errorf("list after delete = %+v (err %v), want [%+v]", got, err, w2)
	}
}

func TestGitCredentials(t *testing.T) { runBackends(t, testGitCredentials) }

func testGitCredentials(t *testing.T, s Backend) {
	ctx := t.Context()
	repoID := testRepoID(t, s, "")

	// Caller-set ID/CreatedAt are ignored: the store assigns both.
	created, err := s.CreateGitCredential(ctx, GitCredential{
		ID:        "ggc_bogus",
		RepoID:    repoID,
		Username:  "bot",
		Password:  "hunter2",
		CreatedAt: time.Unix(1, 0),
	})
	if err != nil {
		t.Fatalf("CreateGitCredential: %v", err)
	}
	if !regexp.MustCompile(`\Aggc_[0-9a-f]{16}\z`).MatchString(created.ID) {
		t.Errorf("ID = %q, want ggc_<16 hex>", created.ID)
	}
	if created.RepoID != repoID || created.Username != "bot" || created.Password != "hunter2" {
		t.Errorf("bad credential row: %+v", created)
	}
	if created.CreatedAt.Equal(time.Unix(1, 0)) || created.CreatedAt.IsZero() || created.CreatedAt.Location() != time.UTC {
		t.Errorf("CreatedAt = %v, want store-assigned non-zero UTC", created.CreatedAt)
	}

	got, err := s.GetGitCredential(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetGitCredential: %v", err)
	}
	if got != created {
		t.Errorf("get = %+v, want %+v", got, created)
	}
	byRepo, err := s.GitCredentialForRepo(ctx, repoID)
	if err != nil {
		t.Fatalf("GitCredentialForRepo: %v", err)
	}
	if byRepo != created {
		t.Errorf("for-repo = %+v, want %+v", byRepo, created)
	}

	// One credential per repo -> 409.
	if _, err := s.CreateGitCredential(ctx, GitCredential{RepoID: repoID, Password: "x"}); !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("duplicate create err = %v, want ErrAlreadyExists", err)
	}

	// Absent lookups -> 404.
	if _, err := s.GetGitCredential(ctx, "ggc_0000000000000000"); !errors.Is(err, ErrNotFound) {
		t.Errorf("get absent err = %v, want ErrNotFound", err)
	}
	if _, err := s.GitCredentialForRepo(ctx, testRepoID(t, s, "-none")); !errors.Is(err, ErrNotFound) {
		t.Errorf("for-repo absent err = %v, want ErrNotFound", err)
	}

	updated, err := s.UpdateGitCredential(ctx, created.ID, "", "rotated")
	if err != nil {
		t.Fatalf("UpdateGitCredential: %v", err)
	}
	if updated.Username != "" || updated.Password != "rotated" {
		t.Errorf("update = %+v, want username/password replaced", updated)
	}
	if updated.ID != created.ID || updated.RepoID != created.RepoID || !updated.CreatedAt.Equal(created.CreatedAt) {
		t.Errorf("update changed identity: %+v, want %+v", updated, created)
	}
	if got, err := s.GetGitCredential(ctx, created.ID); err != nil || got != updated {
		t.Errorf("get after update = %+v (err %v), want %+v", got, err, updated)
	}
	if _, err := s.UpdateGitCredential(ctx, "ggc_0000000000000000", "u", "p"); !errors.Is(err, ErrNotFound) {
		t.Errorf("update absent err = %v, want ErrNotFound", err)
	}

	if err := s.DeleteGitCredential(ctx, created.ID); err != nil {
		t.Fatalf("DeleteGitCredential: %v", err)
	}
	if _, err := s.GetGitCredential(ctx, created.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("get deleted err = %v, want ErrNotFound", err)
	}
	if err := s.DeleteGitCredential(ctx, created.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("double delete err = %v, want ErrNotFound", err)
	}
	// The repo slot is free again after delete.
	if _, err := s.CreateGitCredential(ctx, GitCredential{RepoID: repoID, Password: "new"}); err != nil {
		t.Errorf("re-create after delete err = %v, want nil", err)
	}
}

func TestRepoUpstream(t *testing.T) { runBackends(t, testRepoUpstream) }

func testRepoUpstream(t *testing.T, s Backend) {
	ctx := t.Context()
	repoID := testRepoID(t, s, "")

	if _, err := s.GetRepoUpstream(ctx, repoID); !errors.Is(err, ErrNotFound) {
		t.Errorf("get never-linked err = %v, want ErrNotFound", err)
	}
	if err := s.DeleteRepoUpstream(ctx, repoID); !errors.Is(err, ErrNotFound) {
		t.Errorf("delete never-linked err = %v, want ErrNotFound", err)
	}

	// Caller-set counters/flags/CreatedAt are ignored: the row starts zeroed.
	created, err := s.SetRepoUpstream(ctx, RepoUpstream{
		RepoID:       repoID,
		Provider:     "github",
		Owner:        "acme",
		Name:         "demo",
		UpstreamHost: "github.com",
		RunCount:     7,
		SuccessCount: 3,
		Syncing:      true,
		CreatedAt:    time.Unix(1, 0),
	})
	if err != nil {
		t.Fatalf("SetRepoUpstream: %v", err)
	}
	if created.RunCount != 0 || created.SuccessCount != 0 || created.Syncing {
		t.Errorf("new upstream not zeroed: %+v", created)
	}
	if created.Provider != "github" || created.Owner != "acme" || created.Name != "demo" || created.UpstreamHost != "github.com" {
		t.Errorf("bad upstream row: %+v", created)
	}
	if created.CreatedAt.Equal(time.Unix(1, 0)) || created.CreatedAt.IsZero() || created.CreatedAt.Location() != time.UTC {
		t.Errorf("CreatedAt = %v, want store-assigned non-zero UTC", created.CreatedAt)
	}

	got, err := s.GetRepoUpstream(ctx, repoID)
	if err != nil {
		t.Fatalf("GetRepoUpstream: %v", err)
	}
	if got != created {
		t.Errorf("get = %+v, want %+v", got, created)
	}

	// base_repo is set exactly once -> 409.
	if _, err := s.SetRepoUpstream(ctx, RepoUpstream{RepoID: repoID, Provider: "gitlab"}); !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("duplicate set err = %v, want ErrAlreadyExists", err)
	}

	if err := s.DeleteRepoUpstream(ctx, repoID); err != nil {
		t.Fatalf("DeleteRepoUpstream: %v", err)
	}
	if _, err := s.GetRepoUpstream(ctx, repoID); !errors.Is(err, ErrNotFound) {
		t.Errorf("get detached err = %v, want ErrNotFound", err)
	}
	if err := s.DeleteRepoUpstream(ctx, repoID); !errors.Is(err, ErrNotFound) {
		t.Errorf("double detach err = %v, want ErrNotFound", err)
	}
	// Detach + re-link starts a fresh zeroed row.
	if _, err := s.SetRepoUpstream(ctx, RepoUpstream{RepoID: repoID, Provider: "github", Owner: "acme", Name: "demo", UpstreamHost: "github.com"}); err != nil {
		t.Errorf("re-set after detach err = %v, want nil", err)
	}
}

func TestSyncLifecycle(t *testing.T) { runBackends(t, testSyncLifecycle) }

func testSyncLifecycle(t *testing.T, s Backend) {
	ctx := t.Context()
	repoID := testRepoID(t, s, "")

	if _, _, _, err := s.TryBeginSync(ctx, repoID, testClaimHolder, time.Minute); !errors.Is(err, ErrNotFound) {
		t.Errorf("claim never-linked err = %v, want ErrNotFound", err)
	}
	if _, err := s.FinishSync(ctx, repoID, testClaimHolder, true); !errors.Is(err, ErrNotFound) {
		t.Errorf("finish never-linked err = %v, want ErrNotFound", err)
	}

	if _, err := s.SetRepoUpstream(ctx, RepoUpstream{RepoID: repoID, Provider: "github", Owner: "acme", Name: "demo", UpstreamHost: "github.com"}); err != nil {
		t.Fatalf("SetRepoUpstream: %v", err)
	}

	up, claimed, _, err := s.TryBeginSync(ctx, repoID, testClaimHolder, time.Minute)
	if err != nil || !claimed {
		t.Fatalf("first claim = (%+v, %v, %v), want claimed", up, claimed, err)
	}
	if !up.Syncing || up.RunCount != 0 || up.SuccessCount != 0 {
		t.Errorf("claimed row = %+v, want syncing with zero counters", up)
	}

	// A second claim while the run is in flight is refused but returns the
	// current row.
	up, claimed, _, err = s.TryBeginSync(ctx, repoID, testClaimHolder, time.Minute)
	if err != nil || claimed {
		t.Fatalf("contended claim = (%+v, %v, %v), want claimed=false", up, claimed, err)
	}
	if !up.Syncing {
		t.Errorf("contended row = %+v, want syncing", up)
	}

	up, err = s.FinishSync(ctx, repoID, testClaimHolder, true)
	if err != nil {
		t.Fatalf("FinishSync success: %v", err)
	}
	if up.Syncing || up.RunCount != 1 || up.SuccessCount != 1 {
		t.Errorf("after success = %+v, want run=1 success=1 idle", up)
	}

	// Claim/finish a failed run: RunCount advances, SuccessCount stays.
	if _, claimed, _, err := s.TryBeginSync(ctx, repoID, testClaimHolder, time.Minute); err != nil || !claimed {
		t.Fatalf("reclaim after finish = (%v, %v), want claimed", claimed, err)
	}
	up, err = s.FinishSync(ctx, repoID, testClaimHolder, false)
	if err != nil {
		t.Fatalf("FinishSync failure: %v", err)
	}
	if up.Syncing || up.RunCount != 2 || up.SuccessCount != 1 {
		t.Errorf("after failure = %+v, want run=2 success=1 idle", up)
	}
	if got, err := s.GetRepoUpstream(ctx, repoID); err != nil || got != up {
		t.Errorf("get after runs = %+v (err %v), want %+v", got, err, up)
	}

	// Detach mid-run: the worker's FinishSync (and any further claim) is a
	// 404.
	if _, claimed, _, err := s.TryBeginSync(ctx, repoID, testClaimHolder, time.Minute); err != nil || !claimed {
		t.Fatalf("claim before detach = (%v, %v), want claimed", claimed, err)
	}
	if err := s.DeleteRepoUpstream(ctx, repoID); err != nil {
		t.Fatalf("detach mid-run: %v", err)
	}
	if _, err := s.FinishSync(ctx, repoID, testClaimHolder, true); !errors.Is(err, ErrNotFound) {
		t.Errorf("finish after detach err = %v, want ErrNotFound", err)
	}
	if _, _, _, err := s.TryBeginSync(ctx, repoID, testClaimHolder, time.Minute); !errors.Is(err, ErrNotFound) {
		t.Errorf("claim after detach err = %v, want ErrNotFound", err)
	}
}

func TestTryBeginSyncConcurrent(t *testing.T) { runBackends(t, testTryBeginSyncConcurrent) }

func testTryBeginSyncConcurrent(t *testing.T, s Backend) {
	ctx := t.Context()
	repoID := testRepoID(t, s, "")
	if _, err := s.SetRepoUpstream(ctx, RepoUpstream{RepoID: repoID, Provider: "github", Owner: "acme", Name: "demo", UpstreamHost: "github.com"}); err != nil {
		t.Fatalf("SetRepoUpstream: %v", err)
	}

	const claimers = 8
	var wg sync.WaitGroup
	claims := make(chan bool, claimers)
	errs := make(chan error, claimers)
	for range claimers {
		wg.Go(func() {
			_, claimed, _, err := s.TryBeginSync(ctx, repoID, testClaimHolder, time.Minute)
			claims <- claimed
			errs <- err
		})
	}
	wg.Wait()
	close(claims)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent TryBeginSync: %v", err)
		}
	}
	won := 0
	for claimed := range claims {
		if claimed {
			won++
		}
	}
	if won != 1 {
		t.Errorf("%d of %d claimers won, want exactly 1", won, claimers)
	}
}
