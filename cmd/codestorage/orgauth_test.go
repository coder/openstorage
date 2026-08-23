package main

// Tests for the org auth bootstrap seam (issues #45 and #47):
//
//   - two serve processes sharing one metadata DB must BOTH verify their own
//     loopback sync-push tokens after the second boot — before the fix, the
//     later boot's last-writer-wins upsert deleted the earlier process's
//     persisted loopback key (issue #45);
//   - loopback keys never appear in org_public_keys — the DB carries exactly
//     the configured file keys;
//   - retained org rows that no longer match the configured org produce a
//     startup warning (issue #47), with no cleanup machinery.
//
// The store is a real sqlite backend (zero containers); pg shares the exact
// same UpsertOrgWithKeys replacement semantics (internal/meta tests pin
// that), and the e2e suite covers both lanes with real serve processes.

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/code-storage/internal/auth"
	"github.com/coder/code-storage/internal/meta"
	upsync "github.com/coder/code-storage/internal/sync"
)

func TestBootstrapOrgAuth_TwoBootsKeepBothLoopbackKeys(t *testing.T) {
	ctx := context.Background()
	const org = "org1"

	ms, err := meta.Open(ctx, "sqlite://"+filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ms.Close)
	if err := ms.InitSchema(ctx); err != nil {
		t.Fatal(err)
	}

	fileKey, err := auth.GenerateKey("ES256")
	if err != nil {
		t.Fatal(err)
	}
	filePEM, err := auth.MarshalPublicKeyPEM(fileKey.Public())
	if err != nil {
		t.Fatal(err)
	}
	fileBlocks := []string{string(filePEM)}
	logger := slog.New(slog.DiscardHandler)

	// Boot A, then boot B against the same DB — the #45 ordering.
	keyA, verifierA, err := bootstrapOrgAuth(ctx, ms, org, fileBlocks, logger)
	if err != nil {
		t.Fatalf("boot A: %v", err)
	}
	keyB, verifierB, err := bootstrapOrgAuth(ctx, ms, org, fileBlocks, logger)
	if err != nil {
		t.Fatalf("boot B: %v", err)
	}

	repo := upsync.RepoInfo{RepoID: "repo_abc123", URL: "mirror"}
	tokenA, err := keyA.mint(org, repo, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	tokenB, err := keyB.mint(org, repo, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	// Both processes verify their own loopback tokens after B's boot.
	// Before the fix, boot B's upsert deleted A's persisted loopback key
	// and this first Verify failed.
	if _, err := verifierA.Verify(ctx, tokenA); err != nil {
		t.Fatalf("process A must verify its own loopback token after B booted: %v", err)
	}
	if _, err := verifierB.Verify(ctx, tokenB); err != nil {
		t.Fatalf("process B must verify its own loopback token: %v", err)
	}

	// The loopback key is process-local by design: pushes terminate at the
	// minting process's own listener (selfLoopbackURL), so no process ever
	// needs — or gets — another process's loopback trust.
	if _, err := verifierA.Verify(ctx, tokenB); err == nil {
		t.Fatal("process A must not verify process B's loopback token")
	}

	// Tokens signed with the shared configured file key verify everywhere.
	fileToken, err := auth.MintToken(fileKey, "ES256", auth.Claims{
		Iss: org, Sub: auth.SubjectLiteral, Repo: repo.URL,
		Scopes: []string{auth.ScopeGitRead},
		Iat:    1, Exp: 1<<62 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifierA.Verify(ctx, fileToken); err != nil {
		t.Fatalf("file-key token on A: %v", err)
	}
	if _, err := verifierB.Verify(ctx, fileToken); err != nil {
		t.Fatalf("file-key token on B: %v", err)
	}

	// org_public_keys carries exactly the configured file keys — loopback
	// keys are never persisted.
	pems, err := ms.PublicKeysForOrg(ctx, org)
	if err != nil {
		t.Fatal(err)
	}
	if len(pems) != 1 || pems[0] != string(filePEM) {
		t.Fatalf("org_public_keys = %d rows, want exactly the configured file key", len(pems))
	}
}

func TestBootstrapOrgAuth_WarnsAboutRetainedOrgRows(t *testing.T) {
	ctx := context.Background()

	ms, err := meta.Open(ctx, "sqlite://"+filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ms.Close)
	if err := ms.InitSchema(ctx); err != nil {
		t.Fatal(err)
	}

	fileKey, err := auth.GenerateKey("ES256")
	if err != nil {
		t.Fatal(err)
	}
	filePEM, err := auth.MarshalPublicKeyPEM(fileKey.Public())
	if err != nil {
		t.Fatal(err)
	}
	fileBlocks := []string{string(filePEM)}

	// A row retained from a previous CODESTORAGE_ORG value.
	if _, err := ms.UpsertOrgWithKeys(ctx, "old-org", fileBlocks); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	if _, _, err := bootstrapOrgAuth(ctx, ms, "new-org", fileBlocks, logger); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !bytes.Contains(buf.Bytes(), []byte("old-org")) || !bytes.Contains(buf.Bytes(), []byte("level=WARN")) {
		t.Fatalf("expected a WARN naming the retained org row, got:\n%s", out)
	}

	// The configured org itself never warns.
	buf.Reset()
	ms2, err := meta.Open(ctx, "sqlite://"+filepath.Join(t.TempDir(), "meta2.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ms2.Close)
	if err := ms2.InitSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := bootstrapOrgAuth(ctx, ms2, "only-org", fileBlocks, logger); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 0 {
		t.Fatalf("single matching org row must not warn, got:\n%s", buf.String())
	}
}
