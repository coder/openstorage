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
	stdsync "sync"
	"syscall"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/blob"
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
	// Some libraries log err.Error() as a string. An error can also be a
	// LogValuer that resolves to its text, and a LogValuer can resolve to
	// an error.
	slog.New(NewHookLogHandler(&pusher, nil)).Error("third", slog.String("err", "string "+canary),
		"cause", valuerError{canary}, "lazy", lazyError{canary}, "y", 2)

	for _, want := range []string{"open ref store failed", "hook=update", "repo=r1", "g.k=v", "second", "w.x=1", "third", "y=2"} {
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
	// Eight: the With error in both records, one more error each in the
	// first (attribute, group) and in the second, and three in the third.
	if n := strings.Count(string(b), canary); n != 8 {
		t.Fatalf("the hook-errors file names the canary %d times, want 8: %s", n, b)
	}

	logs := captureServeLog(t)
	logHookErrors("receive-pack r1", path)
	if got := strings.Count(logs.String(), "receive-pack r1: hook: "); got != 3 {
		t.Fatalf("serve logged %d hook records, want 3: %s", got, logs)
	}
	if n := strings.Count(logs.String(), canary); n != 8 {
		t.Fatalf("serve's log names the canary %d times, want 8: %s", n, logs)
	}
}

// valuerError is an error that logs as its own text.
type valuerError struct{ canary string }

func (e valuerError) Error() string        { return "valuer " + e.canary }
func (e valuerError) LogValue() slog.Value { return slog.StringValue(e.Error()) }

// lazyError is a LogValuer that resolves to an error.
type lazyError struct{ canary string }

func (e lazyError) LogValue() slog.Value { return slog.AnyValue(errors.New("lazy " + e.canary)) }

// The handler is enabled at its level: a record below it reaches neither
// stderr nor the hook-errors file.
func TestHookLogHandlerHonorsItsLevel(t *testing.T) {
	path, err := createHookErrors(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvHookErrors, path)
	var pusher bytes.Buffer
	logger := slog.New(NewHookLogHandler(&pusher, &slog.HandlerOptions{Level: slog.LevelWarn}))
	logger.Info("below the level", "err", errors.New("info"))
	logger.Warn("at the level")
	if strings.Contains(pusher.String(), "below the level") || !strings.Contains(pusher.String(), "at the level") {
		t.Fatalf("stderr = %q; want only the Warn record", pusher.String())
	}
	if b, err := os.ReadFile(path); err != nil || strings.Contains(string(b), "below the level") || !strings.Contains(string(b), "at the level") {
		t.Fatalf("hook-errors = %q (%v); want only the Warn record", b, err)
	}
}

// The limits that docs/how-to/deploy-production.md states, the file's mode,
// and records longer than bufio.Scanner's default token.
func TestHookErrorsFileLimitsAndMode(t *testing.T) {
	if maxLoggedHookErrors != 100 || maxHookErrorsBytes != 1<<20 {
		t.Fatalf("limits %d records, %d bytes; the deploy guide says 100 and 1 MiB", maxLoggedHookErrors, maxHookErrorsBytes)
	}
	// The mode must not depend on the process's umask.
	prev := syscall.Umask(0)
	path, err := createHookErrors(t.TempDir())
	syscall.Umask(prev)
	if err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("hook-errors mode = %v (%v); want 0600", fi.Mode().Perm(), err)
	}
	long := strings.Repeat("x", 200<<10)
	if err := os.WriteFile(path, []byte(long+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	logs := captureServeLog(t)
	logHookErrors("receive-pack r1", path)
	if !strings.Contains(logs.String(), "receive-pack r1: hook: "+long) {
		t.Fatalf("serve did not log a %d-byte record whole (%d bytes logged)", len(long), len(logs.String()))
	}
}

// A directory or a FIFO in the hook-errors file's place is neither written
// nor read (a FIFO without a reader would block the hook and serve).
func TestHookErrorsFileRefusesNonRegularFiles(t *testing.T) {
	for _, kind := range []string{"directory", "fifo"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), hookErrorsName)
			var err error
			if kind == "directory" {
				err = os.Mkdir(path, 0o700)
			} else {
				err = syscall.Mkfifo(path, 0o600)
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv(EnvHookErrors, path)
			done := make(chan struct{})
			logs := captureServeLog(t)
			go func() {
				defer close(done)
				slog.New(NewHookLogHandler(io.Discard, nil)).Error("record", "err", errors.New("x"))
				logHookErrors("receive-pack r1", path)
			}()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("the hook or serve blocked on a non-regular hook-errors file")
			}
			if !strings.Contains(logs.String(), "is not a regular file") {
				t.Fatalf("serve's log = %q; want a refusal", logs)
			}
		})
	}
}

// retireFails is a pack ledger whose retire fails with err, and a blob store
// that has no blob.
type retireFails struct {
	err     error
	retired map[string]string
}

func (r *retireFails) RetirePackAttempts(_ context.Context, _ []string, gens map[string]string) error {
	r.retired = gens
	return r.err
}

func (r *retireFails) Stat(context.Context, string) (int64, error) { return 0, blob.ErrNotFound }

// After a failed upload, a retire that fails with a store error writes it
// to the hook-errors file only, never to the stderr that git relays.
func TestRetireUnuploadedKeepsItsErrorOffStderr(t *testing.T) {
	path, err := createHookErrors(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvHookErrors, path)
	canary := map[string]string{"user": "canary_user_" + rand.Text()[:8], "database": "canary_db_" + rand.Text()[:8],
		"host": "canary-host-" + strings.ToLower(rand.Text()[:8]) + ".invalid"}
	st := &retireFails{err: fmt.Errorf("pg retire: failed to connect to `user=%s database=%s`: lookup %s: no such host",
		canary["user"], canary["database"], canary["host"])}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	prev := os.Stderr
	os.Stderr = w
	retireUnuploaded(t.Context(), st, st, "r1", map[string]string{"packs/a.pack": "1"})
	os.Stderr = prev
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	stderr, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if st.retired["packs/a.pack"] != "1" {
		t.Fatalf("retired %v; want the absent blob's reservation", st.retired)
	}
	records, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(records), "pre-receive: retire reservations") {
		t.Fatalf("the hook-errors records lack the retire failure: %s", records)
	}
	for part, v := range canary {
		if n := strings.Count(string(stderr), v); n != 0 {
			t.Errorf("stderr names the canary %s %d times", part, n)
		}
		if !strings.Contains(string(records), v) {
			t.Errorf("the hook-errors records lack the canary %s", part)
		}
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

	repo := "t-hook-errors-" + strings.ToLower(rand.Text()[:8]) // the lane store outlives one run
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

// A record longer than maxHookErrorRecordBytes is cut, so serve reads it and
// every record after it.
func TestHookErrorsFileCutsLongRecords(t *testing.T) {
	path, err := createHookErrors(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvHookErrors, path)
	logger := slog.New(NewHookLogHandler(io.Discard, nil))
	logger.Error("huge", "err", errors.New(strings.Repeat("x", 4*maxHookErrorRecordBytes)))
	logger.Error("after the huge one")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	first, _, _ := strings.Cut(string(b), "\n")
	if len(first)+1 != maxHookErrorRecordBytes || !strings.HasSuffix(first, strings.TrimSuffix(hookErrorCut, "\n")) {
		t.Fatalf("the huge record is %d bytes ending %q; want %d bytes ending %q", len(first)+1, first[max(0, len(first)-20):], maxHookErrorRecordBytes, hookErrorCut)
	}
	logs := captureServeLog(t)
	logHookErrors("receive-pack r1", path)
	if !strings.Contains(logs.String(), "after the huge one") || strings.Contains(logs.String(), "hook errors:") {
		t.Fatalf("serve did not log the record after the cut one: %.300s", logs)
	}
}

// Concurrent writers of one hook, through different loggers, overshoot the
// file's cap by at most one record.
func TestHookErrorsFileCapHoldsAcrossGoroutines(t *testing.T) {
	path, err := createHookErrors(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(strings.Repeat("x", maxHookErrorsBytes-1)), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvHookErrors, path)
	// Every writer that passed the size check sits in the window long
	// enough for the others to pass it too, unless the check and the
	// append are one step.
	prev := hookErrorsChecked
	hookErrorsChecked = func() { time.Sleep(20 * time.Millisecond) }
	t.Cleanup(func() { hookErrorsChecked = prev })
	record := strings.Repeat("y", 1000)
	start := make(chan struct{})
	var wg stdsync.WaitGroup
	for range 50 {
		// One logger each: a slog handler already serializes its own
		// writes, but the hook has several loggers (its own, River's,
		// hookErrorsLogger's).
		logger := slog.New(NewHookLogHandler(io.Discard, nil))
		wg.Go(func() {
			<-start
			logger.Error("delivery failed", "err", errors.New(record))
		})
	}
	close(start)
	wg.Wait()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if over := fi.Size() - maxHookErrorsBytes; over > 2*int64(len(record)) {
		t.Fatalf("the file holds %d bytes past its cap; want at most one record (about %d)", over, len(record))
	}
}
