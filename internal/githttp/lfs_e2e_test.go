package githttp

// End-to-end git-lfs round-trip (docs/explanation/git-protocol-and-namespaces.md): track a binary, push (the
// git-lfs pre-push hook uploads via the batch API), fresh clone (the smudge
// filter downloads it back), and assert the object landed in the blob store
// under blob.LFSKey. Runs on both TestMain lanes: pg/minio exercises
// presigned transfer URLs, local exercises the proxy transfer fallback.

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/coder/code-storage/internal/blob"
)

func TestLFS_GitLFSRoundTrip(t *testing.T) {
	if _, err := exec.LookPath("git-lfs"); err != nil {
		t.Skip("git-lfs not installed")
	}
	repo := registerRepo(t, "t-lfs")
	token := mintTestToken(t, repo, "git:read,git:write", "")
	remote := remoteURL(nodeA, repo, token)

	work := initWorkRepo(t)
	mustGit(t, work, "lfs", "install", "--local")
	mustGit(t, work, "lfs", "track", "*.bin")
	content := make([]byte, 1<<20)
	if _, err := rand.Read(content); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(work, "asset.bin"), content, 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(content)
	oid := hex.EncodeToString(sum[:])
	mustGit(t, work, "add", ".")
	mustGit(t, work, "commit", "-m", "add lfs asset")
	out := mustGit(t, work, "push", remote, "HEAD:refs/heads/main")
	t.Logf("push output:\n%s", out)

	// The pointer's content must be in the blob store under the LFS key.
	size, err := testBlob.Stat(context.Background(), blob.LFSKey(repo, oid))
	if err != nil {
		t.Fatalf("stat LFS object %s: %v", oid, err)
	}
	if size != int64(len(content)) {
		t.Fatalf("stored LFS object size = %d, want %d", size, len(content))
	}

	// A fresh clone must smudge the pointer back to the original bytes
	// (filters passed per-clone so the test does not depend on the host's
	// global git-lfs configuration).
	cloneDir := filepath.Join(t.TempDir(), "clone")
	mustGit(t, "", "clone",
		"-c", "filter.lfs.smudge=git-lfs smudge -- %f",
		"-c", "filter.lfs.process=git-lfs filter-process",
		"-c", "filter.lfs.required=true",
		remote, cloneDir)
	got, err := os.ReadFile(filepath.Join(cloneDir, "asset.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("cloned asset.bin differs from original (%d bytes vs %d)", len(got), len(content))
	}
}
