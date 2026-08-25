package sync

import (
	"context"
	"errors"
	stdsync "sync"
	"testing"
	"time"
)

func TestMemStoreCredentialCRUD(t *testing.T) {
	m := NewMemStore()
	ctx := context.Background()

	cred, err := m.CreateCredential(ctx, Credential{RepoID: "repo_a", Username: "u", Password: "p"})
	if err != nil {
		t.Fatal(err)
	}
	if cred.ID == "" || cred.CreatedAt.IsZero() {
		t.Fatalf("create did not assign ID/CreatedAt: %+v", cred)
	}

	if _, err := m.CreateCredential(ctx, Credential{RepoID: "repo_a", Password: "p2"}); !errors.Is(err, ErrCredentialExists) {
		t.Fatalf("second create = %v, want ErrCredentialExists", err)
	}

	got, err := m.GetCredential(ctx, cred.ID)
	if err != nil || got.Password != "p" || got.Username != "u" {
		t.Fatalf("get = %+v, %v", got, err)
	}
	byRepo, err := m.CredentialForRepo(ctx, "repo_a")
	if err != nil || byRepo.ID != cred.ID {
		t.Fatalf("byRepo = %+v, %v", byRepo, err)
	}

	upd, err := m.UpdateCredential(ctx, cred.ID, "u2", "p2")
	if err != nil || upd.Username != "u2" || upd.Password != "p2" {
		t.Fatalf("update = %+v, %v", upd, err)
	}
	if !upd.CreatedAt.Equal(cred.CreatedAt) {
		t.Fatalf("update changed CreatedAt: %v -> %v", cred.CreatedAt, upd.CreatedAt)
	}
	if _, err := m.UpdateCredential(ctx, "ggc_missing", "u", "p"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("update missing = %v, want ErrNotFound", err)
	}

	if err := m.DeleteCredential(ctx, cred.ID); err != nil {
		t.Fatal(err)
	}
	if err := m.DeleteCredential(ctx, cred.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second delete = %v, want ErrNotFound", err)
	}
	if _, err := m.CredentialForRepo(ctx, "repo_a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("byRepo after delete = %v, want ErrNotFound", err)
	}
	// The repo slot is free again after deletion.
	if _, err := m.CreateCredential(ctx, Credential{RepoID: "repo_a", Password: "p3"}); err != nil {
		t.Fatalf("re-create after delete: %v", err)
	}
}

func TestMemStoreUpstreamLifecycle(t *testing.T) {
	m := NewMemStore()
	ctx := context.Background()

	up, err := m.PutUpstream(ctx, Upstream{RepoID: "repo_b", Provider: "github", Owner: "o", Name: "n", UpstreamHost: "github.com"})
	if err != nil || up.CreatedAt.IsZero() {
		t.Fatalf("put = %+v, %v", up, err)
	}
	if _, err := m.PutUpstream(ctx, Upstream{RepoID: "repo_b", Provider: "github", Owner: "o", Name: "n", UpstreamHost: "github.com"}); !errors.Is(err, ErrUpstreamExists) {
		t.Fatalf("second put = %v, want ErrUpstreamExists", err)
	}

	claimed, _, err := m.BeginSync(ctx, "repo_b", "test-holder", time.Minute)
	if err != nil || claimed.RunCount != 0 || claimed.SuccessCount != 0 {
		t.Fatalf("begin = %+v, %v", claimed, err)
	}
	if _, _, err := m.BeginSync(ctx, "repo_b", "test-holder", time.Minute); !errors.Is(err, ErrSyncInProgress) {
		t.Fatalf("overlapping begin = %v, want ErrSyncInProgress", err)
	}
	fin, err := m.FinishSync(ctx, "repo_b", "test-holder", true)
	if err != nil || fin.RunCount != 1 || fin.SuccessCount != 1 || fin.Syncing {
		t.Fatalf("finish = %+v, %v", fin, err)
	}

	if _, _, err := m.BeginSync(ctx, "repo_b", "test-holder", time.Minute); err != nil {
		t.Fatal(err)
	}
	fin, err = m.FinishSync(ctx, "repo_b", "test-holder", false)
	if err != nil || fin.RunCount != 2 || fin.SuccessCount != 1 {
		t.Fatalf("failed finish = %+v, %v", fin, err)
	}

	if err := m.DeleteUpstream(ctx, "repo_b"); err != nil {
		t.Fatal(err)
	}
	if err := m.DeleteUpstream(ctx, "repo_b"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second delete = %v, want ErrNotFound", err)
	}
	if _, _, err := m.BeginSync(ctx, "repo_b", "test-holder", time.Minute); !errors.Is(err, ErrNotFound) {
		t.Fatalf("begin after detach = %v, want ErrNotFound", err)
	}
	if _, err := m.FinishSync(ctx, "repo_b", "test-holder", true); !errors.Is(err, ErrNotFound) {
		t.Fatalf("finish after detach = %v, want ErrNotFound", err)
	}
}

func TestMemStoreBeginSyncClaimIsExclusive(t *testing.T) {
	m := NewMemStore()
	ctx := context.Background()
	if _, err := m.PutUpstream(ctx, Upstream{RepoID: "repo_c", Provider: "github", Owner: "o", Name: "n", UpstreamHost: "github.com"}); err != nil {
		t.Fatal(err)
	}
	const n = 32
	var wg stdsync.WaitGroup
	var mu stdsync.Mutex
	claims := 0
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, err := m.BeginSync(ctx, "repo_c", "test-holder", time.Minute); err == nil {
				mu.Lock()
				claims++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if claims != 1 {
		t.Fatalf("%d concurrent BeginSync claims succeeded, want exactly 1", claims)
	}
}

func TestInProcessQueueRunsAndCoalesces(t *testing.T) {
	ran := make(chan Job, 16)
	block := make(chan struct{})
	q := NewInProcessQueue(func(_ context.Context, job Job) error {
		<-block
		ran <- job
		return nil
	}, func(context.Context, Job) error { return nil }, discardTestLogger())

	ctx := context.Background()
	if err := q.EnqueueSync(ctx, Job{Org: "acme", RepoID: "repo_q"}); err != nil {
		t.Fatal(err)
	}
	if err := q.EnqueueSync(ctx, Job{Org: "acme", RepoID: "repo_q2"}); err != nil {
		t.Fatal(err)
	}
	close(block)

	seen := map[string]bool{}
	for range 2 {
		select {
		case job := <-ran:
			seen[job.RepoID] = true
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for runs; seen=%v", seen)
		}
	}
	if !seen["repo_q"] || !seen["repo_q2"] {
		t.Fatalf("missing runs: %v", seen)
	}

	q.Close()
	if err := q.EnqueueSync(ctx, Job{Org: "acme", RepoID: "repo_q3"}); err == nil {
		t.Fatal("enqueue after Close succeeded")
	}
}

func TestNewCredentialIDShape(t *testing.T) {
	seen := map[string]bool{}
	for range 64 {
		id := NewCredentialID()
		if len(id) != len("ggc_")+16 || id[:4] != "ggc_" {
			t.Fatalf("bad credential ID %q", id)
		}
		if seen[id] {
			t.Fatalf("duplicate credential ID %q", id)
		}
		seen[id] = true
	}
}
