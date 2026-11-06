package githttp

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/coder/openstorage/internal/ref"
	"github.com/coder/openstorage/internal/ref/s3wal/s3waltest"
)

// TestS3WALHookRefusesUnusableMetadata: a v3 hook that meets a v1 or v2
// metadata document (issue #445: the binary was replaced under an older
// serve) or a corrupt one (issues #466, #478) refuses the push with a
// message that names the fix. It neither accepts the push without its
// events nor writes meta.json, and serve keeps running. The document changes while the update hook waits before its
// CAS, after the serve resolved the repo.
func TestS3WALHookRefusesUnusableMetadata(t *testing.T) {
	if testBackend != backendS3WAL {
		t.Skip("the metadata document is S3WAL-only (ADR-51)")
	}
	setField := func(field, value string) func(t *testing.T, current []byte) []byte {
		return func(t *testing.T, current []byte) []byte {
			var doc map[string]json.RawMessage
			if err := json.Unmarshal(current, &doc); err != nil {
				t.Fatal(err)
			}
			doc[field] = json.RawMessage(value)
			out, err := json.Marshal(doc)
			if err != nil {
				t.Fatal(err)
			}
			return out
		}
	}
	for _, tc := range []struct {
		name   string
		mutate func(t *testing.T, current []byte) []byte
		want   []string
	}{
		// Only the version matters here: the hook must stop at it.
		{"v1", setField("v", "1"),
			[]string{"document version 1 needs conversion", "openstorage s3wal-meta convert --to 3"}},
		{"v2", setField("v", "2"),
			[]string{"document version 2 needs conversion", "openstorage s3wal-meta convert --to 3"}},
		{"truncated", func(_ *testing.T, current []byte) []byte { return current[:len(current)/2] },
			[]string{"_wal/meta.json is corrupt (document decode:", "backup-restore-s3wal.md#restore-and-validate-before-cutover"}},
		// Issue #478: a decodable document with impossible id counters.
		{"negative counter", setField("next_org_id", "-1"),
			[]string{"_wal/meta.json is corrupt (", "negative id counters", "backup-restore-s3wal.md#restore-and-validate-before-cutover"}},
	} {
		t.Run(tc.name, func(t *testing.T) { hookRefusesMetadata(t, tc.mutate, tc.want) })
	}
}

func hookRefusesMetadata(t *testing.T, mutate func(t *testing.T, current []byte) []byte, want []string) {
	db, blobs := privateStores(t, t.TempDir())
	cache := t.TempDir()
	node, err := startNode(cache, "OPENSTORAGE_PG_URL="+db, "OPENSTORAGE_S3_URL="+blobs, EnvHookDelay+"=3000")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(node.stop)
	repoURL := "https://example.com/format-445.git"
	token := mintTestToken(t, repoURL, "repo:write,git:read,git:write", "")
	var created struct {
		RepoID string `json:"repo_id"`
	}
	if status := apiJSON(t, node, http.MethodPost, token, "/api/v1/repos", "{}", &created); status != http.StatusCreated || created.RepoID == "" {
		t.Fatalf("create: status=%d id=%q", status, created.RepoID)
	}

	type pushResult struct {
		out string
		err error
	}
	work := initWorkRepo(t)
	done := make(chan pushResult, 1)
	go func() {
		out, err := tryGit(work, "push", remoteURL(node, created.RepoID, token), "HEAD:refs/heads/main")
		done <- pushResult{out, err}
	}()
	waitForPushHold(t, cache, created.RepoID, "refs/heads/main", done)

	// Replace the stored v3 document, conditionally on the version read.
	u, err := url.Parse(db)
	if err != nil {
		t.Fatal(err)
	}
	bucket := u.Host
	client := s3waltest.RawS3Client(u.Query().Get("endpoint"), "us-east-1", s3AccessKey, s3SecretKey)
	readMeta := func() ([]byte, string) {
		t.Helper()
		obj, err := client.GetObject(t.Context(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("_wal/meta.json")})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = obj.Body.Close() }()
		body, err := io.ReadAll(obj.Body)
		if err != nil {
			t.Fatal(err)
		}
		return body, aws.ToString(obj.ETag)
	}
	current, etag := readMeta()
	var stored struct{ V int }
	if err := json.Unmarshal(current, &stored); err != nil || stored.V != 3 {
		t.Fatalf("stored document is v%d (%v), want 3", stored.V, err)
	}
	bad := mutate(t, current)
	if _, err := client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("_wal/meta.json"), Body: bytes.NewReader(bad), IfMatch: aws.String(etag),
	}); err != nil {
		t.Fatal(err)
	}

	res := <-done
	t.Logf("push on the replaced document:\n%s", res.out)
	if res.err == nil {
		t.Fatal("the push was accepted on the replaced document")
	}
	for _, w := range want {
		if !strings.Contains(res.out, w) {
			t.Fatalf("refusal lacks %q:\n%s", w, res.out)
		}
	}
	rs, err := ref.Open(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	if got, err := rs.GetRef(t.Context(), created.RepoID, "refs/heads/main"); !errors.Is(err, ref.ErrNotFound) {
		t.Fatalf("authority refs/heads/main = %q, %v; want unset", got, err)
	}
	// serve must survive the document too (issue #478): its org-key
	// refresher rereads it every 15 s, and a foreground metadata read
	// fails with an error, not a panic.
	time.Sleep(16 * time.Second)
	if status := apiGet(t, node, token, "/api/v1/repo", nil); status/100 == 2 {
		t.Fatalf("repo read on the replaced document answered %d, want an error", status)
	}
	if code, _ := httpGet(t, "http://"+node.addr+"/healthz", ""); code != http.StatusOK {
		t.Fatalf("serve /healthz = %d after the refusal, want 200 (serve died?)", code)
	}
	// Stop serve (and its hook children) before the final read, so no
	// late writer can hide behind it.
	if err := node.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := waitNodeExit(t, node, 30*time.Second); err != nil {
		t.Logf("serve exit on the replaced document: %v", err)
	}
	if got, _ := readMeta(); !bytes.Equal(got, bad) {
		t.Fatalf("meta.json changed after the refusal:\nwant %s\ngot  %s", bad, got)
	}
}

// TestS3WALServeSurvivesOlderMetadata (issue #488): when meta.json goes
// back to an older seq while serve runs (here: an older copy put back, as a
// restore into the live bucket would), serve keeps running. Metadata reads
// answer errors, serve writes nothing through the older document, and it
// recovers once the newer bytes come back (issue #516). Before the #488
// fix, serve's org-key refresher panicked within 15 s.
func TestS3WALServeSurvivesOlderMetadata(t *testing.T) {
	if testBackend != backendS3WAL {
		t.Skip("the metadata document is S3WAL-only (ADR-51)")
	}
	db, blobs := privateStores(t, t.TempDir())
	node, err := startNode(t.TempDir(), "OPENSTORAGE_PG_URL="+db, "OPENSTORAGE_S3_URL="+blobs)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(node.stop)
	u, err := url.Parse(db)
	if err != nil {
		t.Fatal(err)
	}
	bucket := u.Host
	client := s3waltest.RawS3Client(u.Query().Get("endpoint"), "us-east-1", s3AccessKey, s3SecretKey)
	readMeta := func() ([]byte, string) {
		t.Helper()
		obj, err := client.GetObject(t.Context(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("_wal/meta.json")})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = obj.Body.Close() }()
		body, err := io.ReadAll(obj.Body)
		if err != nil {
			t.Fatal(err)
		}
		return body, aws.ToString(obj.ETag)
	}
	create := func(name string) (string, string) {
		t.Helper()
		token := mintTestToken(t, "https://example.com/"+name+".git", "repo:write,git:read", "")
		var created struct {
			RepoID string `json:"repo_id"`
		}
		if status := apiJSON(t, node, http.MethodPost, token, "/api/v1/repos", "{}", &created); status != http.StatusCreated || created.RepoID == "" {
			t.Fatalf("create %s: status=%d id=%q", name, status, created.RepoID)
		}
		return created.RepoID, token
	}
	// Read back a repo that both documents hold: serve answers it only if
	// it adopted the older document.
	_, token := create("older-488")
	older, _ := readMeta()
	create("newer-488")
	newer, etag := readMeta()
	if _, err := client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("_wal/meta.json"), Body: bytes.NewReader(older), IfMatch: aws.String(etag),
	}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(16 * time.Second) // past one org-key refresher tick (15 s)
	if code, _ := httpGet(t, "http://"+node.addr+"/healthz", ""); code != http.StatusOK {
		t.Fatalf("serve /healthz = %d after meta.json went back, want 200 (serve died?)", code)
	}
	if status := apiGet(t, node, token, "/api/v1/repo", nil); status/100 == 2 {
		t.Fatalf("repo read through the older document answered %d, want an error", status)
	}
	if got, _ := readMeta(); !bytes.Equal(got, older) {
		t.Fatalf("serve wrote meta.json through the older document:\nwant %s\ngot  %s", older, got)
	}
	// The refusal ends when the bucket holds the exact bytes serve last
	// saw again (issue #516).
	_, etag = readMeta()
	if _, err := client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("_wal/meta.json"), Body: bytes.NewReader(newer), IfMatch: aws.String(etag),
	}); err != nil {
		t.Fatal(err)
	}
	if status := apiGet(t, node, token, "/api/v1/repo", nil); status != http.StatusOK {
		t.Fatalf("repo read after the exact bytes came back answered %d, want 200", status)
	}
}
