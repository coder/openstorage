// s3wal-lane drills (issue #133 Phase 3, plan §4): the zero-infra serve
// mode's two lane-specific proofs on top of the shared e2e suite —
// restart-from-bucket-only (the bucket is the SOLE durable authority) and
// the §2.11 instance lease's dual-serve refusal. Both skip outside
// CODESTORAGE_TEST_BACKEND=s3wal.
package githttp

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// skipUnlessS3WAL gates the drills to the zero-infra lane.
func skipUnlessS3WAL(t *testing.T) {
	t.Helper()
	if testBackend != backendS3WAL {
		t.Skipf("s3wal-lane drill (running the %s lane; set CODESTORAGE_TEST_BACKEND=s3wal)", testBackend)
	}
}

// tryServeExpectExit runs `codestorage serve` with the lane env expecting
// it to exit on its own (startup refusal), returning combined output and
// the exit error. A process that unexpectedly keeps serving is killed by
// the context after 30s and still reports an error.
func tryServeExpectExit(t *testing.T, cacheDir string, extraEnv ...string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binPath, "serve",
		"--listen", "127.0.0.1:0",
		"--cache-dir", cacheDir,
	)
	cmd.Env = append(os.Environ(),
		"CODESTORAGE_PG_URL="+dbURL,
		"CODESTORAGE_S3_URL="+blobURL,
		"CODESTORAGE_ORG="+testOrg,
		"CODESTORAGE_ORG_PUBKEYS="+orgKeysPath,
		"CODESTORAGE_TEST_MODE=1",
	)
	cmd.Env = append(cmd.Env, extraEnv...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// TestS3WAL_RestartFromBucketOnly is the §2.5 W10 restart drill: kill a
// node, DESTROY every scrap of its local state (the cache dir is all it
// has — the pairing rule in resolveServeConfig forbids local-disk
// authorities), and prove a cold process pointed only at the bucket
// reconstructs full service: clone succeeds, history is intact under
// `git fsck --strict`, and the authority read path agrees.
func TestS3WAL_RestartFromBucketOnly(t *testing.T) {
	skipUnlessS3WAL(t)

	repo := registerRepo(t, "t-s3wal-restart")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")

	cacheDir := filepath.Join(t.TempDir(), "cache")
	node, err := startNode(cacheDir)
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	defer node.stop()
	mustGit(t, work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/main")
	head := revParse(t, work, "HEAD")

	// Kill the node and wipe its only local directory.
	node.stop()
	if err := os.RemoveAll(cacheDir); err != nil {
		t.Fatal(err)
	}

	// A fresh process with a fresh (empty) cache dir and ONLY the bucket
	// URLs reconstructs everything: _wal/ (refs + meta) and packs/.
	restarted, err := startNode(filepath.Join(t.TempDir(), "fresh-cache"))
	if err != nil {
		t.Fatalf("restart from bucket only: %v", err)
	}
	defer restarted.stop()
	cloneDir := filepath.Join(t.TempDir(), "clone")
	mustGit(t, "", "clone", remoteURL(restarted, repo, token), cloneDir)
	if got := revParse(t, cloneDir, "HEAD"); got != head {
		t.Fatalf("clone HEAD after restart-from-bucket = %q, want %q", got, head)
	}
	if out, err := tryGit(cloneDir, "fsck", "--strict"); err != nil {
		t.Fatalf("git fsck: %v\n%s", err, out)
	}
	// The manifest agrees with what the client pushed.
	if got := pgRefs(t, repo)["refs/heads/main"]; got != head {
		t.Fatalf("manifest refs/heads/main = %q, want %q", got, head)
	}
}

// TestS3WAL_DualServeRefusal proves the §2.11 instance lease: a second
// serve process against the bucket refuses to start while the first
// holds a fresh lease, and starts once the dead holder's lease expires
// (the lane's regular nodes run lease-disabled — see startNode — so this
// drill re-enables it explicitly with a short TTL to keep the expiry half
// fast).
func TestS3WAL_DualServeRefusal(t *testing.T) {
	skipUnlessS3WAL(t)

	const leaseEnv = "CODESTORAGE_S3WAL_LEASE_TTL=2s"
	// Whatever happens, clear the lease object afterwards so reruns and
	// later drills see a clean slate (regular nodes never read it).
	t.Cleanup(func() { _ = testBlob.Delete(context.Background(), "_wal/instance-lease") })

	first, err := startNode(filepath.Join(t.TempDir(), "cache"), leaseEnv)
	if err != nil {
		t.Fatalf("first leased node: %v", err)
	}
	defer first.stop()

	// While the first node's heartbeat keeps the lease fresh, a second
	// serve against the same bucket must refuse to start with the
	// dual-serve error.
	out, err := tryServeExpectExit(t, filepath.Join(t.TempDir(), "cache2"), leaseEnv)
	if err == nil {
		t.Fatalf("second serve started under a held lease; output:\n%s", out)
	}
	if !strings.Contains(out, "instance lease") || !strings.Contains(out, "refusing to start") {
		t.Fatalf("refusal did not name the dual-serve lease; output:\n%s", out)
	}

	// Kill the first node: its heartbeat dies with it and the lease stamp
	// freezes. Once the 2s TTL lapses, a successor steals the stale lease
	// and starts.
	first.stop()
	deadline := time.Now().Add(30 * time.Second)
	for {
		second, err := startNode(filepath.Join(t.TempDir(), "cache3"), leaseEnv)
		if err == nil {
			second.stop()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no successor could steal the expired lease: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}
