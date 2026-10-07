package meta

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/coder/openstorage/internal/secrets"
)

func TestCreateLinkedRepo(t *testing.T) { runBackends(t, testCreateLinkedRepo) }

// testCreateLinkedRepo: one atomic write publishes the repo, its credential
// and its upstream (issue #387). A taken name or a failing row writes
// nothing, and concurrent creates of one name have exactly one winner.
func testCreateLinkedRepo(t *testing.T, s Backend) {
	ctx := t.Context()
	org := seedOrg(t, s)
	up := RepoUpstream{RepoID: "ignored", Provider: "github", Owner: "acme", Name: "demo", UpstreamHost: "github.com", ForwardOnPush: true}
	repo, err := s.CreateLinkedRepo(ctx, org, "linked", "trunk", up, &GitCredential{Password: "tok"})
	if err != nil {
		t.Fatalf("CreateLinkedRepo: %v", err)
	}
	if got, err := s.GetRepoByURL(ctx, org, "linked"); err != nil || got.RepoID != repo.RepoID || got.DefaultBranch != "trunk" {
		t.Fatalf("GetRepoByURL = %+v, %v; want %s on trunk", got, err, repo.RepoID)
	}
	link, err := s.GetRepoUpstream(ctx, repo.RepoID)
	if err != nil || link.Provider != "github" || link.Owner != "acme" || link.Name != "demo" || link.UpstreamHost != "github.com" || !link.ForwardOnPush {
		t.Fatalf("GetRepoUpstream = %+v, %v", link, err)
	}
	if cred, err := s.GitCredentialForRepo(ctx, repo.RepoID); err != nil || cred.Password != "tok" {
		t.Fatalf("GitCredentialForRepo = %+v, %v", cred, err)
	}

	other := RepoUpstream{Provider: "gitlab", Owner: "x", Name: "y", UpstreamHost: "gitlab.com"}
	if _, err := s.CreateLinkedRepo(ctx, org, "linked", "main", other, nil); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("taken name err = %v, want ErrAlreadyExists", err)
	}
	// The name check comes first on every backend: a taken name is
	// ErrAlreadyExists even when the credential would be rejected.
	if _, err := s.CreateLinkedRepo(ctx, org, "linked", "main", other, &GitCredential{Password: secrets.ReservedPrefix + "x"}); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("taken name with a reserved secret err = %v, want ErrAlreadyExists", err)
	}
	if got, err := s.GetRepoUpstream(ctx, repo.RepoID); err != nil || got != link {
		t.Fatalf("upstream after taken-name create = %+v, %v; want unchanged %+v", got, err, link)
	}

	// The credential fails after the repo row was written in the same
	// transaction: the rollback leaves the name free.
	if _, err := s.CreateLinkedRepo(ctx, org, "rolled-back", "main", up, &GitCredential{Password: secrets.ReservedPrefix + "x"}); !errors.Is(err, ErrReservedSecret) {
		t.Fatalf("reserved secret err = %v, want ErrReservedSecret", err)
	}
	if _, err := s.CreateRepo(ctx, org, "rolled-back", "main"); err != nil {
		t.Fatalf("name taken after a failed linked create: %v", err)
	}

	const n = 8
	var wins atomic.Int32
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			switch _, err := s.CreateLinkedRepo(ctx, org, "raced", "main", up, nil); {
			case err == nil:
				wins.Add(1)
			case !errors.Is(err, ErrAlreadyExists):
				t.Errorf("concurrent create err = %v", err)
			}
		})
	}
	wg.Wait()
	if got := wins.Load(); got != 1 {
		t.Fatalf("concurrent creates won %d times, want 1", got)
	}
}
