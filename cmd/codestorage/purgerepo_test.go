package main

// Container-free tests for the purge-repo CLI (issue #52): confirmation and
// eligibility guards plus one full local-mode purge. The purge primitive
// itself is covered in internal/jobs; the store seams in internal/{meta,ref}.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/code-storage/internal/blob"
	"github.com/coder/code-storage/internal/jobs"
	"github.com/coder/code-storage/internal/meta"
	"github.com/coder/code-storage/internal/ref"
)

// seedPurgeDataDir builds a local-mode data dir holding one live repo and
// one repo soft-deleted two hours ago (via the injected store clock), each
// with a registered pack blob. Returns the deleted and live repos.
func seedPurgeDataDir(t *testing.T, dir string) (deleted, live meta.Repo) {
	t.Helper()
	ctx := context.Background()
	// Same URL derivation as resolveStorageURLs local mode.
	past := time.Now().Add(-2 * time.Hour)
	ms, err := meta.Open(ctx, "sqlite://"+filepath.Join(dir, "codestorage.db"),
		meta.WithNowFunc(func() time.Time { return past }))
	if err != nil {
		t.Fatalf("open meta store: %v", err)
	}
	defer ms.Close()
	if err := ms.InitSchema(ctx); err != nil {
		t.Fatalf("meta InitSchema: %v", err)
	}
	if _, err := ms.UpsertOrgWithKeys(ctx, "purge-org", []string{"-----BEGIN PUBLIC KEY-----\ndummy\n-----END PUBLIC KEY-----\n"}); err != nil {
		t.Fatalf("seed org: %v", err)
	}
	deleted, err = ms.CreateRepo(ctx, "purge-org", "https://example.com/deleted.git", "main")
	if err != nil {
		t.Fatalf("CreateRepo(deleted): %v", err)
	}
	live, err = ms.CreateRepo(ctx, "purge-org", "https://example.com/live.git", "main")
	if err != nil {
		t.Fatalf("CreateRepo(live): %v", err)
	}
	if _, err := ms.SoftDeleteRepo(ctx, "purge-org", deleted.URL); err != nil {
		t.Fatalf("SoftDeleteRepo: %v", err)
	}

	rs, err := ref.Open(ctx, "sqlite://"+filepath.Join(dir, "codestorage.db"))
	if err != nil {
		t.Fatalf("open ref store: %v", err)
	}
	defer rs.Close()
	if err := rs.InitSchema(ctx); err != nil {
		t.Fatalf("ref InitSchema: %v", err)
	}
	bl, err := blob.OpenFile("file://" + filepath.Join(dir, "blobs"))
	if err != nil {
		t.Fatalf("open blob store: %v", err)
	}
	defer bl.Close()
	for _, repo := range []meta.Repo{deleted, live} {
		key := blob.PackKey(repo.RepoID, "p1")
		src := filepath.Join(t.TempDir(), "pack")
		if err := os.WriteFile(src, []byte("pack bytes of "+repo.RepoID), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := bl.UploadFile(ctx, key, src); err != nil {
			t.Fatalf("UploadFile: %v", err)
		}
		if err := rs.RegisterPacks(ctx, repo.RepoID, []ref.Pack{{ID: "p1", Key: key}}); err != nil {
			t.Fatalf("RegisterPacks: %v", err)
		}
	}
	return deleted, live
}

func TestCmdPurgeRepo(t *testing.T) {
	// Isolate from ambient storage env: local mode only.
	for _, env := range []string{
		"CODESTORAGE_PG_URL", "SPIKE_PG_URL", "PG_URL",
		"CODESTORAGE_S3_URL", "SPIKE_S3_URL", "BLOB_URL",
		envDataDir, envSecretsKey, envPackQuota, envPackReservationTTL,
	} {
		t.Setenv(env, "")
	}
	ctx := context.Background()
	dir := t.TempDir()
	deleted, live := seedPurgeDataDir(t, dir)
	deletedKey := blob.PackKey(deleted.RepoID, "p1")

	if err := cmdPurgeRepo(ctx, []string{"--data-dir", dir}); err == nil || !strings.Contains(err.Error(), "exactly one <repo-id>") {
		t.Fatalf("no arg = %v, want usage error", err)
	}
	if err := cmdPurgeRepo(ctx, []string{"--data-dir", dir, deleted.RepoID, "extra"}); err == nil || !strings.Contains(err.Error(), "exactly one <repo-id>") {
		t.Fatalf("two args = %v, want usage error", err)
	}
	if err := cmdPurgeRepo(ctx, []string{"--data-dir", dir, deleted.RepoID}); err == nil || !strings.Contains(err.Error(), "--yes") {
		t.Fatalf("without --yes = %v, want confirmation refusal", err)
	}
	if err := cmdPurgeRepo(ctx, []string{"--data-dir", dir, "--yes", live.RepoID}); err == nil || !strings.Contains(err.Error(), "is live") {
		t.Fatalf("live repo = %v, want liveness refusal", err)
	}
	if err := cmdPurgeRepo(ctx, []string{"--data-dir", dir, "--yes", "repo_nonexistent0"}); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("absent repo = %v, want not-found", err)
	}

	// Nothing was purged by the refusals.
	bl, err := blob.OpenFile("file://" + filepath.Join(dir, "blobs"))
	if err != nil {
		t.Fatal(err)
	}
	defer bl.Close()
	if _, err := bl.Stat(ctx, deletedKey); err != nil {
		t.Fatalf("blob touched by refused purges: %v", err)
	}

	// The real purge: soft-deleted 2h ago clears jobs.MinRepoPurgeAge.
	if err := cmdPurgeRepo(ctx, []string{"--data-dir", dir, "--yes", deleted.RepoID}); err != nil {
		t.Fatalf("purge = %v", err)
	}
	if _, err := bl.Stat(ctx, deletedKey); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("purged blob still present: %v", err)
	}
	liveKey := blob.PackKey(live.RepoID, "p1")
	if _, err := bl.Stat(ctx, liveKey); err != nil {
		t.Fatalf("live repo's blob lost: %v", err)
	}
	ms, err := meta.Open(ctx, "sqlite://"+filepath.Join(dir, "codestorage.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Close()
	if _, found, err := ms.(jobs.RepoPurgeMeta).RepoPurgeState(ctx, deleted.RepoID); err != nil || found {
		t.Fatalf("purged repo row survived: found=%v err=%v", found, err)
	}
	// Re-purging reports not-found (idempotent completion).
	if err := cmdPurgeRepo(ctx, []string{"--data-dir", dir, "--yes", deleted.RepoID}); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("re-purge = %v, want not-found", err)
	}
}
