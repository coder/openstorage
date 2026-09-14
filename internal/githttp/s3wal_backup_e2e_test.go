package githttp

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"maps"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
	"uuid"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref/s3wal/s3waltest"
)

// TestS3WAL_OfflineBackupRestore drills an offline, same-binary recovery from
// an opaque bucket copy, not from the original authority or either warm cache.
func TestS3WAL_OfflineBackupRestore(t *testing.T) {
	skipUnlessS3WAL(t)
	if _, err := exec.LookPath("git-lfs"); err != nil {
		t.Fatal("s3wal backup drill requires git-lfs: ", err)
	}
	// Isolate every Git subprocess, including shared helpers and serve children.
	for _, entry := range os.Environ() {
		key, value, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(key, "GIT_") {
			t.Setenv(key, value) // Register restoration before unsetting, not emptying.
			if err := os.Unsetenv(key); err != nil {
				t.Fatal(err)
			}
		}
	}
	for key, value := range map[string]string{
		"GIT_CONFIG_GLOBAL":   "/dev/null",
		"GIT_CONFIG_SYSTEM":   "/dev/null",
		"GIT_CONFIG_NOSYSTEM": "1",
		"GIT_AUTHOR_NAME":     "githttp",
		"GIT_AUTHOR_EMAIL":    "githttp@test.invalid",
		"GIT_AUTHOR_DATE":     "2000-01-01T00:00:00Z",
		"GIT_COMMITTER_NAME":  "githttp",
		"GIT_COMMITTER_EMAIL": "githttp@test.invalid",
		"GIT_COMMITTER_DATE":  "2000-01-01T00:00:00Z",
	} {
		t.Setenv(key, value)
	}
	ctx := t.Context()
	base, err := url.Parse(blobURL)
	if err != nil {
		t.Fatal(err)
	}
	client := s3waltest.RawS3Client(base.Query().Get("endpoint"), "us-east-1", s3AccessKey, s3SecretKey)
	id := uuid.New().String()
	source, backup, restore := "source-"+id, "backup-"+id, "restore-"+id
	for _, bucket := range []string{source, backup, restore} {
		if err := s3waltest.EnsureBucket(ctx, client, bucket); err != nil {
			t.Fatal(err)
		}
		if len(s3BackupInventory(t, client, bucket)) != 0 {
			t.Fatalf("fixture bucket %s is not empty", bucket)
		}
	}
	// These URLs never refer to nodeA's bucket or testMeta. TestMain owns
	// the disposable SeaweedFS container, including cleanup on test failure.
	blobURLFor := func(bucket string) string {
		u := base.Clone()
		u.Host = bucket
		return u.String()
	}
	metaURLFor := func(bucket string) string {
		u := base.Clone()
		u.Host, u.Scheme = bucket, "s3wal"
		return u.String()
	}
	start := func(bucket, cache string) *testNode {
		t.Helper()
		n, err := startNode(cache, "OPENSTORAGE_PG_URL="+metaURLFor(bucket),
			"OPENSTORAGE_S3_URL="+blobURLFor(bucket), "OPENSTORAGE_S3WAL_LEASE_TTL=1m")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(n.stop) // Safety net; the source must exit via SIGTERM before copying.
		return n
	}

	t.Log("1. Populate an isolated source bucket with real Git history and LFS bytes")
	m, err := meta.Open(ctx, metaURLFor(source))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.InitSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := m.UpsertOrgWithKeys(ctx, testOrg, []string{string(mustPubPEM(orgES256Key))}); err != nil {
		t.Fatal(err)
	}
	repo, err := m.CreateRepo(ctx, testOrg, "offline-backup", "main")
	if err != nil {
		t.Fatal(err)
	}
	m.Close()
	sourceCache := filepath.Join(t.TempDir(), "source-cache")
	node := start(source, sourceCache)
	token := mintTestToken(t, repo.RepoID, "git:read,git:write", "")
	// Unlike mustGit, never print token-bearing command arguments on failure.
	git := func(dir string, args ...string) string {
		t.Helper()
		out, err := tryGit(dir, args...)
		if err != nil {
			t.Fatalf("git failed: %v\n%s", err, strings.ReplaceAll(out, token, "[redacted]"))
		}
		return out
	}
	work := initWorkRepo(t)
	git(work, "lfs", "install", "--local")
	git(work, "lfs", "track", "*.bin")
	content := bytes.Repeat([]byte("offline backup LFS fixture\x00"), 4096)
	writeFile(t, work, "asset.bin", string(content))
	git(work, "add", ".")
	git(work, "commit", "-m", "add LFS asset")
	git(work, "push", remoteURL(node, repo.RepoID, token), "HEAD:refs/heads/main")
	head := revParse(t, work, "HEAD")
	lfsKey := blob.LFSKey(repo.RepoID, fmt.Sprintf("%x", sha256.Sum256(content)))
	const sentinelKey = "opaque-backup-fixture/do-not-filter"
	const sentinelBody = "opaque future state, not a hand-written manifest"
	sentinelMeta := map[string]string{"fixture-purpose": "offline-backup", "opaque-version": "1"}

	t.Log("2. SIGTERM and wait for clean source exit before inventory or copy")
	if err := node.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := waitNodeExit(t, node, 30*time.Second); err != nil {
		t.Fatalf("source did not exit cleanly: %v", err)
	}
	// Seed an unknown key only after all application writers have stopped.
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: new(source), Key: new(sentinelKey), Body: strings.NewReader(sentinelBody), Metadata: sentinelMeta,
	}); err != nil {
		t.Fatal(err)
	}
	want := s3BackupInventory(t, client, source)
	if _, ok := want["_wal/instance-lease"]; ok {
		t.Fatal("source retained its instance lease after clean shutdown")
	}
	if len(want) <= 2 || want[lfsKey].SHA256 != sha256.Sum256(content) ||
		!maps.Equal(want[sentinelKey].Metadata, sentinelMeta) {
		t.Fatal("source inventory lacks the expected LFS bytes, metadata, or pagination coverage")
	}
	s3CopyBackupFixture(t, client, source, backup)
	if !s3BackupInventoriesEqual(want, s3BackupInventory(t, client, backup)) {
		t.Fatal("source and backup key/SHA256/user-metadata inventories differ")
	}
	t.Logf("3. Backed up all %d keys with SHA256 and S3 user metadata verified", len(want))

	// All negative checks happen offline, before ANY restore node can start.
	for _, fault := range []string{"missing", "corrupt", "metadata"} {
		switch fault {
		case "missing":
			_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: new(backup), Key: new(lfsKey)})
		case "corrupt":
			bad := bytes.Clone(content)
			bad[0] ^= 0xff // Same size: only a content digest detects this change.
			_, err = client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: new(backup), Key: new(lfsKey), Body: bytes.NewReader(bad), Metadata: want[lfsKey].Metadata,
			})
		case "metadata":
			_, err = client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: new(backup), Key: new(sentinelKey), Body: strings.NewReader(sentinelBody),
				Metadata: map[string]string{"fixture-purpose": "changed"},
			})
		}
		if err != nil {
			t.Fatal(err)
		}
		if s3BackupInventoriesEqual(want, s3BackupInventory(t, client, backup)) {
			t.Fatalf("%s backup object was accepted before restore", fault)
		}
		t.Logf("4. Rejected %s backup object before restore startup", fault)
		s3CopyBackupFixture(t, client, source, backup)
		if !s3BackupInventoriesEqual(want, s3BackupInventory(t, client, backup)) {
			t.Fatalf("backup repair after %s did not reproduce the source inventory", fault)
		}
	}

	t.Log("5. Destroy source bucket contents and source cache; restore into an empty bucket")
	for key := range want {
		if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: new(source), Key: new(key)}); err != nil {
			t.Fatal(err)
		}
	}
	if len(s3BackupInventory(t, client, source)) != 0 {
		t.Fatal("source bucket was not emptied")
	}
	if err := os.RemoveAll(sourceCache); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(sourceCache); !os.IsNotExist(err) {
		t.Fatalf("source cache still exists or cannot be checked: %v", err)
	}
	if len(s3BackupInventory(t, client, restore)) != 0 {
		t.Fatal("restore destination is not empty")
	}
	s3CopyBackupFixture(t, client, backup, restore)
	if !s3BackupInventoriesEqual(want, s3BackupInventory(t, client, restore)) {
		t.Fatal("restored key/SHA256/user-metadata inventory differs before startup")
	}

	t.Log("6. Start the same binary with a fresh cache; clone, fsck, compare LFS bytes, then push")
	recovered := start(restore, filepath.Join(t.TempDir(), "fresh-cache"))
	clone := filepath.Join(t.TempDir(), "clone")
	git("", "clone",
		"-c", "filter.lfs.smudge=git-lfs smudge -- %f",
		"-c", "filter.lfs.process=git-lfs filter-process",
		"-c", "filter.lfs.required=true",
		remoteURL(recovered, repo.RepoID, token), clone)
	if revParse(t, clone, "HEAD") != head {
		t.Fatal("restored clone lost Git history")
	}
	git(clone, "fsck", "--strict")
	got, err := os.ReadFile(filepath.Join(clone, "asset.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Fatal("restored clone LFS bytes differ")
	}
	git(clone, "lfs", "install", "--local")
	followup := commitFile(t, clone, "after.txt", "after restore\n", "write after restore", nil)
	git(clone, "push", "origin", "HEAD:refs/heads/main")
	if got := git(clone, "ls-remote", "origin", "refs/heads/main"); strings.TrimSpace(got) != followup+"\trefs/heads/main" {
		t.Fatal("follow-up push did not update the restored authority")
	}
	t.Log("7. Offline recovery passed: original source absent, history and LFS intact, writes accepted")
}

type s3BackupEntry struct {
	SHA256   [sha256.Size]byte
	Metadata map[string]string
}

// The fixture is deliberately small: read each object's bytes, not its ETag.
// Two keys per page force the unfiltered listing through continuation tokens.
func s3BackupInventory(t *testing.T, client *s3.Client, bucket string) map[string]s3BackupEntry {
	t.Helper()
	inventory := make(map[string]s3BackupEntry)
	pages := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{Bucket: new(bucket), MaxKeys: new(int32(2))})
	for pages.HasMorePages() {
		page, err := pages.NextPage(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range page.Contents {
			if entry.Key == nil || *entry.Key == "" {
				t.Fatal("S3 listed an object without a key")
			}
			if _, exists := inventory[*entry.Key]; exists {
				t.Fatalf("S3 repeated inventory key %q", *entry.Key)
			}
			obj, err := client.GetObject(t.Context(), &s3.GetObjectInput{Bucket: new(bucket), Key: entry.Key})
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(obj.Body)
			closeErr := obj.Body.Close()
			if err != nil || closeErr != nil {
				t.Fatalf("read inventory object: %v; close: %v", err, closeErr)
			}
			inventory[*entry.Key] = s3BackupEntry{SHA256: sha256.Sum256(body), Metadata: obj.Metadata}
		}
	}
	return inventory
}

func s3BackupInventoriesEqual(a, b map[string]s3BackupEntry) bool {
	return maps.EqualFunc(a, b, func(a, b s3BackupEntry) bool {
		return a.SHA256 == b.SHA256 && maps.Equal(a.Metadata, b.Metadata)
	})
}

// Test-only small-fixture copier. Callers must stop every writer and verify
// the destination independently; this is not an online backup algorithm.
func s3CopyBackupFixture(t *testing.T, client *s3.Client, source, destination string) {
	t.Helper()
	if source == destination {
		t.Fatal("backup source and destination must differ")
	}
	for key := range s3BackupInventory(t, client, source) {
		obj, err := client.GetObject(t.Context(), &s3.GetObjectInput{Bucket: new(source), Key: new(key)})
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(obj.Body)
		closeErr := obj.Body.Close()
		if err != nil || closeErr != nil {
			t.Fatalf("read backup object: %v; close: %v", err, closeErr)
		}
		if _, err := client.PutObject(t.Context(), &s3.PutObjectInput{
			Bucket: new(destination), Key: new(key), Body: bytes.NewReader(body), Metadata: obj.Metadata,
		}); err != nil {
			t.Fatal(err)
		}
	}
}
