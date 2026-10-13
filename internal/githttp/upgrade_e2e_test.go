package githttp

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json/v2"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
	"uuid"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/coder/openstorage/internal/ref/s3wal/s3waltest"
)

var (
	upgradeAttempt string
	upgradePair    upgradeInputs
)

// Upgrade mode never enters the shared suite's build/bootstrap path. The exact
// selection prevents a require-mode invocation from passing with zero tests.
func upgradeTestMain(m *testing.M) (code int) {
	code = 1
	flag.Parse()
	upgradeAttempt = os.Getenv("OPENSTORAGE_UPGRADE_ATTEMPT")
	if upgradeAttempt == "" {
		fmt.Fprintln(os.Stderr, "upgrade requires OPENSTORAGE_UPGRADE_ATTEMPT (new private directory)")
		return
	}
	if err := os.Mkdir(upgradeAttempt, 0o700); err != nil {
		fmt.Fprintln(os.Stderr, "create exclusive upgrade attempt:", err)
		return
	}
	defer func() {
		if err := os.WriteFile(filepath.Join(upgradeAttempt, "inner-exit.txt"), fmt.Appendf(nil, "%d\n", code), 0o600); err != nil {
			fmt.Fprintln(os.Stderr, "record upgrade exit:", err)
			code = 1
		}
	}()
	if os.Getenv("OPENSTORAGE_TEST_UPGRADE") != "require" ||
		flag.Lookup("test.run").Value.String() != "^TestS3WAL_ReleaseUpgrade$" ||
		flag.Lookup("test.count").Value.String() != "1" ||
		flag.Lookup("test.skip").Value.String() != "" ||
		flag.Lookup("test.list").Value.String() != "" ||
		flag.Lookup("test.bench").Value.String() != "" {
		fmt.Fprintln(os.Stderr, "upgrade requires mode=require, -run '^TestS3WAL_ReleaseUpgrade$' and -count=1")
		return
	}
	var err error
	upgradePair, err = prepareUpgradeInputs(upgradeInputConfig{
		Archive: os.Getenv("OPENSTORAGE_UPGRADE_ARCHIVE"), Checksums: os.Getenv("OPENSTORAGE_UPGRADE_CHECKSUMS"),
		Candidate: os.Getenv("OPENSTORAGE_UPGRADE_CANDIDATE"), CandidateSHA256: os.Getenv("OPENSTORAGE_UPGRADE_CANDIDATE_SHA256"),
		Commit: os.Getenv("OPENSTORAGE_UPGRADE_COMMIT"), Tree: os.Getenv("OPENSTORAGE_UPGRADE_TREE"), SourceDir: "../..",
	}, upgradeAttempt)
	if err != nil {
		fmt.Fprintln(os.Stderr, "upgrade input integrity:", err)
		return
	}
	code = m.Run()
	return
}

// TestS3WAL_ReleaseUpgrade qualifies only the frozen official artifact and exact
// candidate with this small local fixture. It is not released-pair certification.
func TestS3WAL_ReleaseUpgrade(t *testing.T) {
	if upgradeAttempt == "" {
		t.Skip("standalone require-mode harness; see docs/how-to/test-release-upgrade.md")
	}
	// Neither serve nor Git may inherit ambient config, credentials, hooks,
	// identity, proxies or test fault injection. Keep Docker's transport context.
	for _, entry := range os.Environ() {
		key, value, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(key, "GIT_") || strings.HasPrefix(key, "AWS_") ||
			strings.HasPrefix(key, "OPENSTORAGE_") || strings.HasPrefix(key, "CODESTORAGE_") ||
			strings.HasSuffix(strings.ToUpper(key), "_PROXY") {
			t.Setenv(key, value)
			if err := os.Unsetenv(key); err != nil {
				t.Fatal(err)
			}
		}
	}
	home := t.TempDir()
	for key, value := range map[string]string{
		"HOME": home, "XDG_CONFIG_HOME": home,
		"GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_SYSTEM": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1",
		"GIT_AUTHOR_NAME": "upgrade-fixture", "GIT_AUTHOR_EMAIL": "upgrade@test.invalid", "GIT_AUTHOR_DATE": "2000-01-01T00:00:00Z",
		"GIT_COMMITTER_NAME": "upgrade-fixture", "GIT_COMMITTER_EMAIL": "upgrade@test.invalid", "GIT_COMMITTER_DATE": "2000-01-01T00:00:00Z",
		"AWS_ACCESS_KEY_ID": s3AccessKey, "AWS_SECRET_ACCESS_KEY": s3SecretKey,
	} {
		t.Setenv(key, value)
	}
	if out, err := exec.Command("git", "lfs", "version").CombinedOutput(); err != nil {
		t.Fatalf("upgrade requires working git-lfs: %v: %s", err, out)
	}
	receipt, err := os.OpenFile(filepath.Join(upgradeAttempt, "phases.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer receipt.Close()
	phase := func(message string) {
		t.Helper()
		if _, err := fmt.Fprintf(receipt, "%s %s\n", time.Now().UTC().Format(time.RFC3339Nano), message); err != nil {
			t.Fatal(err)
		}
		if err := receipt.Sync(); err != nil {
			t.Fatal(err)
		}
		t.Log(message)
	}
	phase("inputs verified and frozen; official " + upgradeOldTag + " -> exact candidate -> official (ADR-51 downgrade), local fixture only")
	// startSeaweedFS retains this context for container teardown. t.Context is
	// canceled before cleanup callbacks, so keep teardown independently live.
	containerCtx, cancelContainer := context.WithTimeout(context.WithoutCancel(t.Context()), 12*time.Minute)
	t.Cleanup(cancelContainer)
	addr, terminate := startSeaweedFS(containerCtx)
	t.Cleanup(terminate)
	bucket := "upgrade-" + uuid.New().String()
	client := s3waltest.RawS3Client("http://"+addr, "us-east-1", s3AccessKey, s3SecretKey)
	if err := s3waltest.EnsureBucket(t.Context(), client, bucket); err != nil {
		t.Fatal(err)
	}
	if len(s3BackupInventory(t, client, bucket)) != 0 {
		t.Fatal("new private upgrade bucket is not empty")
	}
	dbURL = fmt.Sprintf("s3wal://%s?endpoint=http://%s&region=us-east-1", bucket, addr)
	blobURL = fmt.Sprintf("s3://%s?endpoint=http://%s&s3ForcePathStyle=true&region=us-east-1", bucket, addr)
	testBackend = "upgrade" // Never inherit the shared s3wal lane's disabled lease.
	orgES256Key = mustGenKey("ES256")
	orgKeysPath = filepath.Join(home, "org.pub.pem")
	if err := os.WriteFile(orgKeysPath, mustPubPEM(orgES256Key), 0o600); err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	secretsKey := base64.StdEncoding.EncodeToString(key)
	_, _ = rand.Read(key)
	webhookSecret := hex.EncodeToString(key)
	// A real receiver proves the successor decrypts the old stored secret to
	// the same value, not merely that an envelope version remains unchanged.
	type delivery struct {
		valid bool
		body  []byte
	}
	deliveries := make(chan delivery, 8)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		stamp, signature, ok := strings.Cut(r.Header.Get("X-Pierre-Signature"), ",sha256=")
		stamp, prefix := strings.CutPrefix(stamp, "t=")
		mac := hmac.New(sha256.New, []byte(webhookSecret))
		_, _ = mac.Write(append([]byte(stamp+"."), body...))
		valid := err == nil && ok && prefix && hmac.Equal([]byte(signature), []byte(hex.EncodeToString(mac.Sum(nil))))
		select {
		case deliveries <- delivery{valid: valid, body: body}:
		default:
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(receiver.Close)
	awaitDelivery := func(head string) {
		t.Helper()
		deadline := time.After(30 * time.Second)
		for {
			select {
			case got := <-deliveries:
				if !got.valid {
					t.Fatal("webhook did not verify with the original secret")
				}
				if bytes.Contains(got.body, []byte(head)) {
					return
				}
			case <-deadline:
				t.Fatal("no signed webhook delivery for the new Git HEAD")
			}
		}
	}
	start := func(binary, name string) *testNode {
		t.Helper()
		if err := upgradePair.recheck(); err != nil {
			t.Fatal("pre-invocation input integrity: ", err)
		}
		log, err := os.OpenFile(filepath.Join(upgradeAttempt, name+".stderr"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = log.Close() })
		n, err := startBinaryNode(binary, t.TempDir(), log,
			"OPENSTORAGE_TEST_MODE=", "OPENSTORAGE_S3WAL_LEASE_TTL=1m",
			"OPENSTORAGE_SECRETS_KEY="+secretsKey, "OPENSTORAGE_WEBHOOK_ALLOW_PRIVATE=true")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(n.stop)
		obj, err := client.GetObject(t.Context(), &s3.GetObjectInput{Bucket: new(bucket), Key: new("_wal/instance-lease")})
		if err != nil {
			t.Fatal("active instance lease missing: ", err)
		}
		body, err := io.ReadAll(obj.Body)
		_ = obj.Body.Close()
		var lease struct {
			Holder string `json:"holder"`
			TTL    string `json:"ttl"`
		}
		if err != nil || json.Unmarshal(body, &lease) != nil || lease.Holder == "" || lease.TTL != "1m0s" {
			t.Fatal("instance lease identity/TTL invalid")
		}
		if err := os.WriteFile(filepath.Join(upgradeAttempt, name+".lease.json"), body, 0o600); err != nil {
			t.Fatal(err)
		}
		phase(name + ": fresh cache, active 1m instance lease observed")
		return n
	}
	stop := func(n *testNode, name string) {
		t.Helper()
		if err := n.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		if err := waitNodeExit(t, n, 30*time.Second); err != nil {
			t.Fatalf("%s clean stop: %v", name, err)
		}
		phase(name + ": SIGTERM reaped, process exit=0")
		before := s3BackupInventory(t, client, bucket)
		if _, held := before["_wal/instance-lease"]; held {
			t.Fatal("cleanly stopped process retained its lease; no manual deletion or retry")
		}
		time.Sleep(2 * time.Second) // Bounded observed quiet window, not a lease expiry bypass.
		if !s3BackupInventoriesEqual(before, s3BackupInventory(t, client, bucket)) {
			t.Fatal("bucket changed after writer was reaped")
		}
		encoded, err := json.Marshal(before)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(upgradeAttempt, name+".quiescent.json"), encoded, 0o600); err != nil {
			t.Fatal(err)
		}
		phase(name + ": lease absent; all-key hash/metadata inventory quiescent for 2s")
	}

	// ADR-51: the official release writes metadata format v1 and the
	// candidate format v2. Each refuses the other's bucket without writing
	// meta.json, and s3wal-meta convert moves the stopped bucket between
	// them.
	metaObject := func() (version int, etag string) {
		t.Helper()
		obj, err := client.GetObject(t.Context(), &s3.GetObjectInput{Bucket: new(bucket), Key: new("_wal/meta.json")})
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(obj.Body)
		_ = obj.Body.Close()
		var doc struct {
			V int `json:"v"`
		}
		if err != nil || json.Unmarshal(body, &doc) != nil {
			t.Fatal("meta.json unreadable")
		}
		return doc.V, *obj.ETag
	}
	refuse := func(binary, name, want string) {
		t.Helper()
		_, before := metaObject()
		log, err := os.OpenFile(filepath.Join(upgradeAttempt, name+".stderr"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		defer log.Close()
		if n, err := startBinaryNode(binary, t.TempDir(), log, "OPENSTORAGE_TEST_MODE=", "OPENSTORAGE_S3WAL_LEASE_TTL=1m", "OPENSTORAGE_SECRETS_KEY="+secretsKey); err == nil {
			n.stop()
			t.Fatalf("%s served a bucket of the other metadata format", name)
		}
		stderr, err := os.ReadFile(log.Name())
		if err != nil || !bytes.Contains(stderr, []byte(want)) {
			t.Fatalf("%s did not refuse with %q", name, want)
		}
		if _, after := metaObject(); after != before {
			t.Fatalf("%s changed meta.json while refusing", name)
		}
		if _, held := s3BackupInventory(t, client, bucket)["_wal/instance-lease"]; held {
			t.Fatalf("%s left its lease behind", name)
		}
		phase(name + ": refused with " + strconv.Quote(want) + "; meta.json unchanged; lease released")
	}
	convert := func(to string) {
		t.Helper()
		if err := upgradePair.recheck(); err != nil {
			t.Fatal("pre-invocation input integrity: ", err)
		}
		cmd := exec.Command(upgradePair.CandidateBinary, "s3wal-meta", "convert", "--to", to)
		cmd.Env = append(os.Environ(), "OPENSTORAGE_PG_URL="+dbURL)
		out, err := cmd.CombinedOutput()
		if werr := os.WriteFile(filepath.Join(upgradeAttempt, "convert-to-"+to+".out"), out, 0o600); werr != nil {
			t.Fatal(werr)
		}
		if err != nil || !bytes.Contains(out, []byte("verified")) {
			t.Fatalf("convert --to %s: %v", to, err)
		}
		if v, _ := metaObject(); strconv.Itoa(v) != to {
			t.Fatalf("convert --to %s left meta.json at v%d", to, v)
		}
		if _, held := s3BackupInventory(t, client, bucket)["_wal/instance-lease"]; held {
			t.Fatal("convert left its lease behind")
		}
		phase("candidate s3wal-meta convert --to " + to + ": verified; lease released")
	}

	old := start(upgradePair.OldBinary, "official")
	const repoName = "upgrade-fixture"
	token := mintTestToken(t, repoName, "repo:write,git:read,git:write", "")
	orgToken := mintTestToken(t, "org", "repo:write", "")
	var created struct {
		RepoID string `json:"repo_id"`
	}
	if status := apiJSON(t, old, http.MethodPost, token, "/api/v1/repos", `{"default_branch":"main"}`, &created); status != http.StatusCreated || created.RepoID == "" {
		t.Fatalf("official create repo status=%d", status)
	}
	type repoMetadata struct {
		RepoID        string `json:"repo_id"`
		RepoName      string `json:"repo_name"`
		URL           string `json:"url"`
		DefaultBranch string `json:"default_branch"`
		CreatedAt     string `json:"created_at"`
	}
	var wantRepo repoMetadata
	if apiGet(t, old, token, "/api/v1/repo", &wantRepo) != http.StatusOK || wantRepo.RepoID != created.RepoID || wantRepo.RepoName != repoName || wantRepo.URL != repoName || wantRepo.DefaultBranch != "main" || wantRepo.CreatedAt == "" {
		t.Fatal("official repository metadata invalid")
	}
	type webhookMetadata struct {
		ID, URL   string
		CreatedAt string `json:"created_at"`
	}
	var wantHook webhookMetadata
	if status := apiJSON(t, old, http.MethodPost, orgToken, "/api/v1/webhooks", fmt.Sprintf(`{"url":%q,"secret":%q}`, receiver.URL, webhookSecret), &wantHook); status != http.StatusOK || wantHook.ID == "" || wantHook.URL != receiver.URL || wantHook.CreatedAt == "" {
		t.Fatalf("official webhook create status=%d", status)
	}
	gitLog, err := os.OpenFile(filepath.Join(upgradeAttempt, "git.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer gitLog.Close()
	git := func(dir string, args ...string) string {
		t.Helper()
		out, err := tryGit(dir, args...)
		if _, logErr := fmt.Fprintf(gitLog, "operation=%s result=%v\n%s\n", args[0], err, out); logErr != nil {
			t.Fatal(logErr)
		}
		if err != nil {
			// Never print token-bearing argv or subprocess output publicly.
			t.Fatalf("fixture git failed: %v", err)
		}
		return strings.TrimSpace(out)
	}
	work := initWorkRepo(t)
	git(work, "lfs", "install", "--local")
	git(work, "lfs", "track", "*.bin")
	content := bytes.Repeat([]byte("official upgrade LFS fixture\x00"), 4096)
	writeFile(t, work, "asset.bin", string(content))
	git(work, "add", ".")
	git(work, "commit", "-m", "official LFS history")
	git(work, "push", remoteURL(old, created.RepoID, token), "HEAD:refs/heads/main")
	head := git(work, "rev-parse", "HEAD")
	awaitDelivery(head)
	phase("official: authenticated repo/webhook metadata, two Git commits and materialized LFS written; webhook HMAC verified")
	stop(old, "official")
	obj, err := client.GetObject(t.Context(), &s3.GetObjectInput{Bucket: new(bucket), Key: new("_wal/meta.json")})
	if err != nil {
		t.Fatal(err)
	}
	stored, err := io.ReadAll(obj.Body)
	_ = obj.Body.Close()
	if err != nil || bytes.Contains(stored, []byte(webhookSecret)) || !bytes.Contains(stored, []byte("enc:v1:")) {
		t.Fatal("old secret was not encrypted at rest")
	}
	check := func(n *testNode, label string) string {
		t.Helper()
		if apiGet(t, n, "invalid", "/api/v1/repo", nil) != http.StatusUnauthorized {
			t.Fatal("candidate did not reject unauthenticated access")
		}
		var got repoMetadata
		if apiGet(t, n, token, "/api/v1/repo", &got) != http.StatusOK || got != wantRepo {
			t.Fatal("candidate repository metadata differs")
		}
		var hooks struct {
			Webhooks []webhookMetadata `json:"webhooks"`
		}
		if apiGet(t, n, orgToken, "/api/v1/webhooks", &hooks) != http.StatusOK || len(hooks.Webhooks) != 1 || hooks.Webhooks[0] != wantHook {
			t.Fatal("candidate webhook metadata differs")
		}
		clone := filepath.Join(t.TempDir(), "clone")
		git("", "-c", "filter.lfs.smudge=", "-c", "filter.lfs.required=false", "clone", remoteURL(n, created.RepoID, token), clone)
		git(clone, "lfs", "install", "--local")
		git(clone, "lfs", "pull")
		git(clone, "fsck", "--strict")
		if git(clone, "rev-parse", "HEAD") != head {
			t.Fatal("candidate Git HEAD differs")
		}
		gotBytes, err := os.ReadFile(filepath.Join(clone, "asset.bin"))
		if err != nil || !bytes.Equal(gotBytes, content) {
			t.Fatal("candidate LFS bytes differ")
		}
		phase(label + ": old token accepted, invalid token rejected; repo/webhook metadata equal; Git HEAD/fsck and materialized LFS bytes equal")
		return clone
	}
	if v, _ := metaObject(); v != 1 {
		t.Fatalf("official release wrote meta.json v%d, want 1", v)
	}
	refuse(upgradePair.CandidateBinary, "candidate-on-v1", "document version 1 needs conversion")
	convert("2")
	refuse(upgradePair.OldBinary, "official-on-v2", "document version 2 unsupported")
	candidate := start(upgradePair.CandidateBinary, "candidate-first")
	clone := check(candidate, "candidate")
	content = append(content, []byte("new candidate bytes\n")...)
	writeFile(t, clone, "asset.bin", string(content))
	git(clone, "add", ".")
	git(clone, "commit", "-m", "candidate write")
	git(clone, "push", "origin", "HEAD:refs/heads/main")
	head = git(clone, "rev-parse", "HEAD")
	awaitDelivery(head)
	phase("candidate: new Git/LFS write accepted; original encrypted webhook secret verified through delivery HMAC")
	stop(candidate, "candidate-first")
	second := start(upgradePair.CandidateBinary, "candidate-second")
	check(second, "candidate")
	stop(second, "candidate-second")
	// The supported downgrade (ADR-51 D8): convert back, then the official
	// release serves everything the candidate wrote.
	convert("1")
	refuse(upgradePair.CandidateBinary, "candidate-on-v1-again", "document version 1 needs conversion")
	back := start(upgradePair.OldBinary, "official-downgrade")
	check(back, "official after --to 1")
	stop(back, "official-downgrade")
	phase("PASS: exact official-to-candidate-to-official fixture; conversion both ways verified; all application writers reaped")
}
