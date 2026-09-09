package githttp

// Unit coverage for the update hook's sync claim fence (issue #86): a
// loopback sync push whose run no longer holds the repo's sync claim must
// be refused at the ref-update boundary — the token TTL fences new
// requests, but an in-flight receive-pack that authenticated before a
// steal would otherwise force-push its stale snapshot over the thief's
// newer refs.

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/meta"
)

func TestEnforceSyncClaimFence(t *testing.T) {
	ctx := context.Background()
	dbURL := "sqlite://" + filepath.Join(t.TempDir(), "meta.db")
	ms, err := meta.Open(ctx, dbURL)
	if err != nil {
		t.Fatalf("open meta: %v", err)
	}
	defer ms.Close()
	if err := ms.InitSchema(ctx); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	t.Setenv("OPENSTORAGE_PG_URL", dbURL)

	// SetRepoUpstream is guarded on a live repos row (issue #52).
	if _, err := ms.UpsertOrgWithKeys(ctx, "fence-org", []string{"-----BEGIN PUBLIC KEY-----\ndummy\n-----END PUBLIC KEY-----\n"}); err != nil {
		t.Fatalf("seed org: %v", err)
	}
	repo, err := ms.CreateRepo(ctx, "fence-org", "https://example.com/fence.git", "main")
	if err != nil {
		t.Fatalf("CreateRepo: %v", err)
	}
	repoID := repo.RepoID
	if _, err := ms.SetRepoUpstream(ctx, meta.RepoUpstream{
		RepoID: repoID, Provider: "github", Owner: "o", Name: "n", UpstreamHost: "github.com",
	}); err != nil {
		t.Fatalf("SetRepoUpstream: %v", err)
	}
	if _, claimed, _, err := ms.TryBeginSync(ctx, repoID, "holder-live", time.Minute); err != nil || !claimed {
		t.Fatalf("claim = (%v, %v), want claimed", claimed, err)
	}

	// The claim holder passes.
	if err := enforceSyncClaimFence(ctx, repoID, "holder-live"); err != nil {
		t.Errorf("fence refused the live holder: %v", err)
	}
	// A stale holder (its claim was stolen) is refused.
	if err := enforceSyncClaimFence(ctx, repoID, "holder-stale"); err == nil ||
		!strings.Contains(err.Error(), "refusing the stale sync push") {
		t.Errorf("fence for a stale holder = %v, want stale-push refusal", err)
	}
	// A detached repo fails closed too.
	if err := ms.DeleteRepoUpstream(ctx, repoID); err != nil {
		t.Fatal(err)
	}
	if err := enforceSyncClaimFence(ctx, repoID, "holder-live"); err == nil {
		t.Error("fence for a detached repo = nil, want fail-closed error")
	}
}
