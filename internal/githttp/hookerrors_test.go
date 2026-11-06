package githttp

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/meta"
)

// captureServeLog sends the standard logger (serve's log.Printf) to a
// buffer that the handler's goroutines can write while the test reads.
func captureServeLog(t *testing.T) *syncBuffer {
	t.Helper()
	var buf syncBuffer
	prevOut, prevFlags := log.Writer(), log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() { log.SetOutput(prevOut); log.SetFlags(prevFlags) })
	return &buf
}

// The hook's log handler keeps error values, in attributes, in With
// attributes and in groups, off stderr (the pusher), and writes the whole
// record to the hook-errors file, which serve logs.
func TestHookLogHandlerKeepsErrorsFromThePusher(t *testing.T) {
	canary := "canary-" + rand.Text()
	path, err := createHookErrors(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvHookErrors, path)
	var pusher bytes.Buffer
	logger := slog.New(NewHookLogHandler(&pusher, nil)).With("err", errors.New("with "+canary), "hook", "update")
	logger.Error("open ref store failed", "err", fmt.Errorf("dial %s", canary), "repo", "r1",
		slog.Group("g", "err", errors.New("group "+canary), "k", "v"))
	logger.WithGroup("w").Warn("second", "err", errors.New("nested "+canary), "x", 1)

	for _, want := range []string{"open ref store failed", "hook=update", "repo=r1", "g.k=v", "second", "w.x=1"} {
		if !strings.Contains(pusher.String(), want) {
			t.Errorf("stderr lacks %q: %s", want, pusher.String())
		}
	}
	if n := strings.Count(pusher.String(), canary); n != 0 {
		t.Fatalf("stderr names the canary %d times: %s", n, pusher.String())
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Five: the With error in both records, and one more error each in
	// the first (attribute, group) and in the second.
	if n := strings.Count(string(b), canary); n != 5 {
		t.Fatalf("the hook-errors file names the canary %d times, want 5: %s", n, b)
	}

	logs := captureServeLog(t)
	logHookErrors("receive-pack r1", path)
	if got := strings.Count(logs.String(), "receive-pack r1: hook: "); got != 2 {
		t.Fatalf("serve logged %d hook records, want 2: %s", got, logs)
	}
	if n := strings.Count(logs.String(), canary); n != 5 {
		t.Fatalf("serve's log names the canary %d times, want 5: %s", n, logs)
	}
}

// Without a hook-errors file the error values are dropped, never written
// to stderr. A symlink in its place is not followed, and serve does not
// create the file over one that is already there.
func TestHookErrorsFileFailsClosed(t *testing.T) {
	canary := "canary-" + rand.Text()
	t.Setenv(EnvHookErrors, "")
	var pusher bytes.Buffer
	slog.New(NewHookLogHandler(&pusher, nil)).Error("unset", "err", errors.New(canary))
	if !strings.Contains(pusher.String(), "unset") || strings.Contains(pusher.String(), canary) {
		t.Fatalf("stderr without a hook-errors file = %q; want the message without the canary", pusher.String())
	}

	dir := t.TempDir()
	target := filepath.Join(dir, "elsewhere")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, hookErrorsName)
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvHookErrors, link)
	pusher.Reset()
	slog.New(NewHookLogHandler(&pusher, nil)).Error("symlinked", "err", errors.New(canary))
	if b, err := os.ReadFile(target); err != nil || len(b) != 0 {
		t.Fatalf("the symlink's target holds %q (%v); want the record dropped", b, err)
	}
	if strings.Contains(pusher.String(), canary) {
		t.Fatalf("stderr names the canary: %s", pusher.String())
	}
	if err := os.WriteFile(target, []byte("planted "+canary+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	logs := captureServeLog(t)
	logHookErrors("receive-pack r1", link)
	if strings.Contains(logs.String(), canary) || !strings.Contains(logs.String(), "not a regular file") {
		t.Fatalf("serve's log for a symlinked hook-errors path = %q; want a refusal without the target's content", logs)
	}
	if _, err := createHookErrors(dir); !errors.Is(err, os.ErrExist) {
		t.Fatalf("createHookErrors over a symlink = %v; want ErrExist", err)
	}
}

// serve logs at most maxLoggedHookErrors records of one push, and a count
// of the rest.
func TestLogHookErrorsBoundsOnePush(t *testing.T) {
	path, err := createHookErrors(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var recs strings.Builder
	for i := range maxLoggedHookErrors + 5 {
		fmt.Fprintf(&recs, "record %d\n", i)
	}
	if err := os.WriteFile(path, []byte(recs.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	logs := captureServeLog(t)
	logHookErrors("receive-pack r1", path)
	if got := strings.Count(logs.String(), "receive-pack r1: hook: record "); got != maxLoggedHookErrors {
		t.Fatalf("serve logged %d records, want %d", got, maxLoggedHookErrors)
	}
	if !strings.Contains(logs.String(), "receive-pack r1: hook: 5 more records not logged") {
		t.Fatalf("serve's log lacks the count of the rest: %s", logs)
	}
	if strings.Contains(logs.String(), "dropped the records past") {
		t.Fatalf("serve reports dropped records for a small file: %s", logs)
	}
}

// The hooks stop writing records once the hook-errors file holds
// maxHookErrorsBytes, and serve logs that they dropped some.
func TestHookErrorsFileBoundsItsSize(t *testing.T) {
	path, err := createHookErrors(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvHookErrors, path)
	logger := slog.New(NewHookLogHandler(io.Discard, nil))
	detail := strings.Repeat("x", 1000)
	for range 2 * maxHookErrorsBytes / len(detail) {
		logger.Error("hook eventing unavailable: open meta store", "err", errors.New(detail))
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() < maxHookErrorsBytes || fi.Size() > maxHookErrorsBytes+2*int64(len(detail)) {
		t.Fatalf("the hook-errors file holds %d bytes; want %d plus at most one record", fi.Size(), maxHookErrorsBytes)
	}
	logs := captureServeLog(t)
	logHookErrors("receive-pack r1", path)
	if !strings.Contains(logs.String(), fmt.Sprintf("receive-pack r1: hook: the hooks dropped the records past %d bytes", maxHookErrorsBytes)) {
		t.Fatalf("serve's log does not say that records were dropped: %s", logs)
	}
}

// A push through serve: the hook's error value reaches serve's log through
// the hook-errors file and never the pusher, and the file goes with the
// request. The test binary's hook dispatch (TestMain) logs the record.
func TestServeLogsHookErrors(t *testing.T) {
	t.Setenv("OPENSTORAGE_PG_URL", dbURL)
	t.Setenv("OPENSTORAGE_S3_URL", blobURL)
	canary := "canary-" + rand.Text()
	t.Setenv(envTestHookErrorRecord, "dial tcp: lookup "+canary)
	logs := captureServeLog(t)

	repo := "t-hook-errors"
	key, pemBlock := genKeyPEM(t, "ES256")
	laneStore, laneBlob := openLaneStores(t)
	srv := &Server{
		Store:    laneStore,
		Blob:     laneBlob,
		CacheDir: t.TempDir(),
		Verifier: auth.NewVerifier(memKeys{testOrg: {pemBlock}}),
		Org:      testOrg,
		ResolveRepo: func(_ context.Context, base string) (meta.Repo, bool) {
			if base != repo {
				return meta.Repo{}, false
			}
			return meta.Repo{RepoID: repo, URL: repo, DefaultBranch: "main"}, true
		},
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	work := initWorkRepo(t)
	token := mintUnitToken(t, key, "ES256", unitClaims(testOrg, repo, auth.ScopeGitRead, auth.ScopeGitWrite))
	url := strings.Replace(ts.URL, "http://", "http://t:"+token+"@", 1) + "/" + repo + ".git"
	out, err := tryGit(work, "push", url, "HEAD:refs/heads/main")
	if err != nil {
		t.Fatalf("push through the in-process server: %v\n%s", err, out)
	}
	m := regexp.MustCompile(`msg="test hook record" path=(\S+)`).FindStringSubmatch(out)
	if m == nil {
		t.Fatalf("the pusher's output lacks the hook's line: %s", out)
	}
	if n := strings.Count(out, canary); n != 0 {
		t.Fatalf("the pusher's output names the canary %d times: %s", n, out)
	}
	// serve logs the records when the handler returns, which can be after
	// the client finished.
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if _, err := os.Lstat(m[1]); errors.Is(err, os.ErrNotExist) && strings.Contains(logs.String(), canary) {
			break
		}
	}
	if !strings.Contains(logs.String(), "receive-pack "+repo+": hook: ") || !strings.Contains(logs.String(), canary) {
		t.Fatalf("serve's log lacks the hook's record: %s", logs)
	}
	if _, err := os.Lstat(m[1]); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the hook-errors file %s outlived the request: %v", m[1], err)
	}
}
