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

// TestS3WALHookRefusesV1Metadata (issue #445): a v2 hook that meets a v1
// metadata document, as when the binary is replaced under an older serve,
// refuses the push with the conversion hint. It neither accepts the push
// without its events nor writes meta.json. The bucket turns v1 while the
// update hook waits before its CAS, after the serve resolved the repo.
func TestS3WALHookRefusesV1Metadata(t *testing.T) {
	if testBackend != backendS3WAL {
		t.Skip("the metadata format version is S3WAL-only (ADR-51)")
	}
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

	// Render the stored v2 document as v1, conditionally on the version
	// read. Only the version matters here: the hook must stop at it.
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
	v2, etag := readMeta()
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(v2, &doc); err != nil {
		t.Fatal(err)
	}
	if string(doc["v"]) != "2" {
		t.Fatalf("stored document is v%s, want 2", doc["v"])
	}
	doc["v"] = json.RawMessage("1")
	v1, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("_wal/meta.json"), Body: bytes.NewReader(v1), IfMatch: aws.String(etag),
	}); err != nil {
		t.Fatal(err)
	}

	res := <-done
	t.Logf("push on a v1 document:\n%s", res.out)
	if res.err == nil {
		t.Fatal("the push was accepted on a v1 document")
	}
	for _, want := range []string{"document version 1 needs conversion", "openstorage s3wal-meta convert --to 2"} {
		if !strings.Contains(res.out, want) {
			t.Fatalf("refusal lacks %q:\n%s", want, res.out)
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
	// Stop serve (and its hook children) before the final read, so no
	// late writer can hide behind it.
	if err := node.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := waitNodeExit(t, node, 30*time.Second); err != nil {
		t.Logf("serve exit on a v1 document: %v", err)
	}
	if got, _ := readMeta(); !bytes.Equal(got, v1) {
		t.Fatalf("meta.json changed after the refusal:\nwant %s\ngot  %s", v1, got)
	}
}
