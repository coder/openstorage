package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/githttp"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref"
	"github.com/coder/openstorage/internal/ref/s3wal"
)

// fakeMetaS3 is a read-only S3 endpoint holding one fixed _wal/meta.json in
// bucket "b". It counts every request that is not a read, so a test can
// prove the hook wrote nothing. down answers every request 503.
type fakeMetaS3 struct {
	body   []byte
	down   bool
	gets   atomic.Int32
	writes atomic.Int32
}

func (f *fakeMetaS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		f.writes.Add(1)
	}
	if f.down {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`<Error><Code>ServiceUnavailable</Code></Error>`))
		return
	}
	if r.Method != http.MethodGet || r.URL.Path != "/b/"+s3walMetaTestKey {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`<Error><Code>AccessDenied</Code></Error>`))
		return
	}
	f.gets.Add(1)
	const etag = `"meta-v1"`
	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("ETag", etag)
	_, _ = w.Write(f.body)
}

// hookEventingEnv points wireHookEventing at metaURL, as serve's env would,
// and restores the hook's store wrapper afterwards.
func hookEventingEnv(t *testing.T, metaURL string) *bytes.Buffer {
	t.Helper()
	t.Setenv(envOrg, "hook-org")
	t.Setenv("OPENSTORAGE_PG_URL", metaURL)
	t.Setenv(envSecretsKey, "")
	t.Setenv("OPENSTORAGE_S3_EXPECTED_BUCKET_OWNER", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(t.TempDir(), "none"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "none"))
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	// A caller whose token may forward: the hook records intent for it.
	t.Setenv(githttp.EnvScopes, `["git:read","git:write"]`)
	t.Setenv(githttp.EnvRefsPolicy, "")
	t.Setenv(githttp.EnvOps, "")
	original := githttp.WrapHookRefStore
	t.Cleanup(func() { githttp.WrapHookRefStore = original })
	// serve's hook-errors file: the hook's log handler writes the error
	// values there, for the operator.
	records := filepath.Join(t.TempDir(), "hook-errors")
	if err := os.WriteFile(records, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(githttp.EnvHookErrors, records)
	return new(bytes.Buffer)
}

// hookErrorRecords reads the hook-errors file that hookEventingEnv set up.
func hookErrorRecords(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(os.Getenv(githttp.EnvHookErrors))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// hookRefStore is the authority the update hook would CAS into.
func hookRefStore(t *testing.T) ref.RefStore {
	t.Helper()
	rs, err := ref.Open(t.Context(), "sqlite://"+filepath.Join(t.TempDir(), "refs.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rs.Close)
	if err := rs.InitSchema(t.Context()); err != nil {
		t.Fatal(err)
	}
	return rs
}

// Issues #445 and #466: a v2 hook on a v1 bucket (binary replaced under an
// older serve), or on a corrupt document, refuses the ref update before its
// CAS, instead of accepting the push and dropping its events. The refusal
// names the fix, and the hook writes nothing.
func TestHookEventingRefusesUnusableMetaDocument(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want []string
	}{
		{"v1", `{"v":1,"seq":3,"writer":"0123456789abcdef0123456789abcdef","next_org_id":1,"next_key_id":0}`,
			[]string{"document version 1 needs conversion", "openstorage s3wal-meta convert --to 2"}},
		// A corrupt document's detail can name rows of the document: it goes
		// to the hook-errors file, and the pusher gets a fixed refusal.
		{"truncated", `{"v":2,"seq":3,"writer":"0123`,
			[]string{"_wal/meta.json is corrupt (the server log has the detail)", "backup-restore-s3wal.md#restore-and-validate-before-cutover", "!document decode:", "record:document decode:"}},
		{"no writer", `{"v":2,"seq":3,"next_org_id":1,"next_key_id":0}`,
			[]string{"_wal/meta.json is corrupt (the server log has the detail)", "backup-restore-s3wal.md", "!no writer nonce", "record:document seq 3 has no writer nonce"}},
		{"negative counter", `{"v":2,"seq":3,"writer":"0123456789abcdef0123456789abcdef","next_org_id":-1,"next_key_id":0}`,
			[]string{"_wal/meta.json is corrupt (the server log has the detail)", "backup-restore-s3wal.md", "!negative id counters", "record:document seq 3 has negative id counters (-1, 0)"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeMetaS3{body: []byte(tc.body)}
			srv := httptest.NewServer(fake)
			defer srv.Close()
			logs := hookEventingEnv(t, "s3wal://b?endpoint="+srv.URL)
			cleanup := wireHookEventing(t.Context(), slog.New(githttp.NewHookLogHandler(logs, nil)))
			defer cleanup()
			rs := hookRefStore(t)

			wrapped, err := githttp.WrapHookRefStore(t.Context(), rs)
			if err == nil || wrapped != nil {
				t.Fatalf("WrapHookRefStore = %v, %v; want a refusal", wrapped, err)
			}
			for _, want := range append([]string{"refusing the ref update"}, tc.want...) {
				switch {
				case strings.HasPrefix(want, "!"):
					if strings.Contains(err.Error(), want[1:]) {
						t.Fatalf("refusal %q shows the pusher %q", err, want[1:])
					}
				case strings.HasPrefix(want, "record:"):
					if !strings.Contains(hookErrorRecords(t), strings.TrimPrefix(want, "record:")) {
						t.Fatalf("the hook-errors file lacks %q: %s", want, hookErrorRecords(t))
					}
				case !strings.Contains(err.Error(), want):
					t.Fatalf("refusal %q lacks %q", err, want)
				}
			}
			if !githttp.ShownToPusher(err) {
				t.Fatalf("the hook would hide the refusal %q from the pusher", err)
			}
			if got, err := rs.GetRef(t.Context(), "repo-1", "refs/heads/main"); !errors.Is(err, ref.ErrNotFound) {
				t.Fatalf("authority ref = %q, %v; want unset", got, err)
			}
			if fake.gets.Load() == 0 {
				t.Fatal("the hook never read meta.json")
			}
			if n := fake.writes.Load(); n != 0 {
				t.Fatalf("the hook sent %d write requests to the bucket; want none", n)
			}
		})
	}
}

// On the local schedule a hook that cannot read the metadata document
// refuses an update that could record forward intent before its CAS: the
// format check fails open (issues #445, #466), but the push's forward intent
// must be in the row before the CAS (ADR-50 amendment), and without the
// document the hook cannot tell whether the repository forwards. A caller
// whose token may not forward records no intent, and its update goes
// through. The bucket sees no write, and neither the hook's log nor the
// refusal names the bucket's endpoint: both reach the pusher.
func TestHookEventingRefusesUnreadableLocalSchedule(t *testing.T) {
	for _, tc := range []struct {
		name, scopes string
		unresolvable bool // the endpoint's host does not resolve: its error names the host
		refused      bool
	}{
		{name: "may forward", refused: true},
		{name: "write-only token", scopes: `["git:write"]`},
		{name: "unresolvable endpoint", unresolvable: true, refused: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeMetaS3{down: true}
			srv := httptest.NewServer(fake)
			defer srv.Close()
			endpoint := srv.URL
			if tc.unresolvable {
				endpoint = "http://canary-s3-" + strings.ToLower(rand.Text()[:8]) + ".invalid"
			}
			logs := hookEventingEnv(t, "s3wal://b?endpoint="+endpoint)
			if tc.scopes != "" {
				t.Setenv(githttp.EnvScopes, tc.scopes)
			}
			cleanup := wireHookEventing(t.Context(), slog.New(githttp.NewHookLogHandler(logs, nil)))
			defer cleanup()
			rs := hookRefStore(t)
			wrapped, err := githttp.WrapHookRefStore(t.Context(), rs)
			if err != nil {
				t.Fatalf("format check did not fail open: %v", err)
			}
			if !strings.Contains(logs.String(), "metadata format check failed") {
				t.Fatalf("missing fail-open log: %s", logs)
			}
			won, err := wrapped.CASRef(t.Context(), "repo-1", "refs/heads/main", ref.ZeroSHA, strings.Repeat("a", 40))
			got, gerr := rs.GetRef(t.Context(), "repo-1", "refs/heads/main")
			if tc.refused {
				if won || err == nil || !strings.HasSuffix(err.Error(), "refusing the ref update: hook could not record the forward intent; push again later") || !errors.Is(gerr, ref.ErrNotFound) {
					t.Fatalf("CAS = (%v, %v), authority = (%q, %v); want the update refused before its CAS", won, err, got, gerr)
				}
				if !strings.Contains(logs.String(), "hook refuses ref updates: record forward intent") {
					t.Fatalf("missing refusal log: %s", logs)
				}
				if !githttp.ShownToPusher(err) {
					t.Fatalf("the hook would hide the refusal %q from the pusher", err)
				}
			} else if !won || err != nil || gerr != nil {
				t.Fatalf("CAS = (%v, %v), authority = (%q, %v); want the update accepted (it records no intent)", won, err, got, gerr)
			}
			seen := logs.String()
			if err != nil {
				seen += err.Error()
			}
			if n := strings.Count(seen, strings.TrimPrefix(endpoint, "http://")); n != 0 {
				t.Fatalf("the pusher's output names the bucket endpoint %d times", n)
			}
			// The operator gets the cause of both lines.
			if tc.unresolvable {
				for _, msg := range []string{"metadata format check failed", "hook refuses ref updates: record forward intent"} {
					var line string
					for l := range strings.Lines(hookErrorRecords(t)) {
						if strings.Contains(l, msg) {
							line = l
						}
					}
					if !strings.Contains(line, strings.TrimPrefix(endpoint, "http://")) {
						t.Fatalf("the hook-errors record %q lacks the endpoint: %q", msg, line)
					}
				}
			}
			if n := fake.writes.Load(); n != 0 {
				t.Fatalf("the hook sent %d write requests to the bucket; want none", n)
			}
		})
	}
}

// commitThenEOF commits each create or update and then reports an EOF, as a
// store whose connection drops after the commit does. Deletes pass through:
// ref.IdempotentCAS never recovers a delete.
type commitThenEOF struct{ ref.RefStore }

func (s commitThenEOF) CASRef(ctx context.Context, repo, name, old, new string) (bool, error) {
	won, err := s.RefStore.CASRef(ctx, repo, name, old, new)
	if err != nil || new == ref.ZeroSHA {
		return won, err
	}
	return false, io.ErrUnexpectedEOF
}

func (s commitThenEOF) CASRefGen(ctx context.Context, repo, name, old, new string) (bool, int64, error) {
	won, gen, err := s.RefStore.CASRefGen(ctx, repo, name, old, new)
	if err != nil || new == ref.ZeroSHA {
		return won, gen, err
	}
	return false, 0, io.ErrUnexpectedEOF
}

// A hook that cannot wire its eventing refuses every update that would
// record forward intent, on PostgreSQL and on the local schedule: branch and
// tag creations, updates and deletes. The eventless default would accept
// them with no intent and no events, and a failed step does not prove that
// the CAS fails too (the database can be back by then). Updates that record
// no intent keep failing open: ephemeral updates, a sync run's own loopback
// push, and a caller whose token may not forward. Every update that goes
// through keeps ref.IdempotentCAS, so a create or update that commits and
// then reports an EOF still wins, through CASRef and CASRefGen. Neither the
// log nor the refusal names the store: both reach the pusher.
func TestHookEventingRefusesIntentWithoutEventing(t *testing.T) {
	a, b := strings.Repeat("a", 40), strings.Repeat("b", 40)
	pgCanary := map[string]string{
		"user": "canary_user_" + rand.Text()[:8], "password": "canary_pass_" + rand.Text()[:8],
		"host": "canary-host-" + strings.ToLower(rand.Text()[:8]) + ".invalid", "database": "canary_db_" + rand.Text()[:8],
	}
	pgURL := "postgres://" + pgCanary["user"] + ":" + pgCanary["password"] + "@" + pgCanary["host"] + ":5432/" + pgCanary["database"] + "?connect_timeout=2"
	type shape struct {
		name, mode, scopes, repo, ref, claim string
		refusing, refused                    bool
	}
	shapes := []shape{
		{name: "branch", repo: "repo-1", ref: "refs/heads/main", refusing: true, refused: true},
		{name: "tag", repo: "repo-1", ref: "refs/tags/v1", refusing: true, refused: true},
		{name: "secrets config invalid", mode: "bogus", repo: "repo-1", ref: "refs/heads/main", refusing: true, refused: true},
		{name: "ephemeral namespace", repo: "repo-1", ref: "refs/namespaces/ephemeral/refs/heads/main", refusing: true},
		{name: "ephemeral key", repo: "repo-1+ephemeral", ref: "refs/heads/main", refusing: true},
		{name: "sync loopback", repo: "repo-1", ref: "refs/heads/main", claim: "holder"},
		{name: "write-only token", scopes: `["git:write"]`, repo: "repo-1", ref: "refs/heads/main"},
	}
	ops := []struct {
		name            string
		seed, old, new_ string // seed is the ref's value before the CAS ("" for none)
	}{
		{name: "create", old: ref.ZeroSHA, new_: a},
		{name: "update", seed: a, old: a, new_: b},
		{name: "delete", seed: a, old: a, new_: ref.ZeroSHA},
	}
	for _, backend := range []string{"pg", "sqlite"} {
		for _, tc := range shapes {
			t.Run(backend+"/"+tc.name, func(t *testing.T) {
				metaURL, canary := pgURL, pgCanary
				if backend == "sqlite" {
					dir := filepath.Join(t.TempDir(), "canary-path-"+strings.ToLower(rand.Text()[:8]))
					if err := os.WriteFile(dir, nil, 0o600); err != nil {
						t.Fatal(err)
					}
					metaURL, canary = "sqlite://"+filepath.Join(dir, "meta.db"), map[string]string{"path": filepath.Base(dir)}
				}
				// Control: the open error names the canary.
				if _, err := meta.Open(t.Context(), metaURL); err == nil {
					t.Fatalf("meta.Open(%s) succeeded; want it to fail", backend)
				} else {
					for part, v := range canary {
						if part != "password" && !strings.Contains(err.Error(), v) {
							t.Fatalf("meta.Open's error does not name the canary %s, so the absence check below proves nothing: %v", part, err)
						}
					}
				}
				logs := hookEventingEnv(t, metaURL)
				t.Setenv(envSecretsMode, tc.mode)
				t.Setenv(githttp.EnvSyncClaim, tc.claim)
				if tc.scopes != "" {
					t.Setenv(githttp.EnvScopes, tc.scopes)
				}
				cleanup := wireHookEventing(t.Context(), slog.New(githttp.NewHookLogHandler(logs, nil)))
				defer cleanup()
				rs := hookRefStore(t)
				wrapped, err := githttp.WrapHookRefStore(t.Context(), commitThenEOF{rs})
				if err != nil {
					t.Fatalf("WrapHookRefStore: %v", err)
				}
				stage := "open meta store"
				if tc.mode != "" {
					stage = "secrets key"
				}
				wantLog := "hook eventing disabled: " + stage
				if tc.refusing {
					wantLog = "hook eventing unavailable: " + stage + "; refusing only updates that could record forward intent"
				}
				if !strings.Contains(logs.String(), wantLog) {
					t.Fatalf("missing %q log: %s", wantLog, logs)
				}
				seen := logs.String()
				for _, op := range ops {
					for i, cas := range []func(name string) (bool, error){
						func(name string) (bool, error) { return wrapped.CASRef(t.Context(), tc.repo, name, op.old, op.new_) },
						func(name string) (bool, error) {
							won, _, err := wrapped.CASRefGen(t.Context(), tc.repo, name, op.old, op.new_)
							return won, err
						},
					} {
						name := fmt.Sprintf("%s-%s-%d", tc.ref, op.name, i)
						if op.seed != "" {
							if won, err := rs.CASRef(t.Context(), tc.repo, name, ref.ZeroSHA, op.seed); err != nil || !won {
								t.Fatalf("seed %s: %v, %v", name, won, err)
							}
						}
						won, err := cas(name)
						got, gerr := rs.GetRef(t.Context(), tc.repo, name)
						if err != nil {
							seen += err.Error()
						}
						if tc.refused {
							before := op.seed
							if before == "" {
								before = "absent"
							}
							if got == "" && errors.Is(gerr, ref.ErrNotFound) {
								got = "absent"
							}
							if won || err == nil || !strings.HasSuffix(err.Error(), "refusing the ref update: hook eventing unavailable ("+stage+"); push again later") || got != before || !githttp.ShownToPusher(err) {
								t.Fatalf("%s %d = (%v, %v), authority = %q; want the update refused before its CAS, authority %s", op.name, i, won, err, got, before)
							}
							continue
						}
						want := op.new_
						if want == ref.ZeroSHA {
							if won && err == nil && errors.Is(gerr, ref.ErrNotFound) {
								continue
							}
						} else if won && err == nil && got == want {
							continue
						}
						t.Fatalf("%s %d = (%v, %v), authority = (%q, %v); want the update accepted (it records no intent), a lost commit reply recovered", op.name, i, won, err, got, gerr)
					}
				}
				for part, v := range canary {
					if n := strings.Count(seen, v); n != 0 {
						t.Fatalf("the pusher's output names the canary %s %d times", part, n)
					}
				}
				// The operator gets the cause, without the password.
				records := hookErrorRecords(t)
				for part, v := range canary {
					switch n := strings.Count(records, v); {
					case part == "password" && n != 0:
						t.Fatalf("the hook-errors records name the canary password %d times", n)
					case part != "password" && stage == "open meta store" && n == 0:
						t.Fatalf("the hook-errors records do not name the canary %s: %s", part, records)
					}
				}
			})
		}
	}
}

// The hook's logger keeps error values off stderr, which git relays to the
// pusher, and writes them to serve's hook-errors file.
func TestSlogForHookKeepsErrorsOffStderr(t *testing.T) {
	hookEventingEnv(t, "")
	canary := "canary-" + rand.Text()
	var stderr bytes.Buffer
	slogForHook(&stderr).Error("hook eventing unavailable: open meta store", "err", errors.New("dial "+canary))
	if !strings.Contains(stderr.String(), "hook eventing unavailable: open meta store") || strings.Contains(stderr.String(), canary) {
		t.Fatalf("stderr = %q; want the message without the canary", stderr.String())
	}
	if n := strings.Count(hookErrorRecords(t), canary); n != 1 {
		t.Fatalf("the hook-errors file names the canary %d times, want 1", n)
	}
}

// runHook, the hook command, logs with the hook's logger and makes it the
// default logger: stderr gets no error value, the hook-errors file does.
func TestRunHookLogsWithTheHookLogger(t *testing.T) {
	host := "canary-host-" + strings.ToLower(rand.Text()[:8]) + ".invalid"
	hookEventingEnv(t, "postgres://u:p@"+host+":5432/db?connect_timeout=2")
	prev, prevOut, prevFlags := slog.Default(), log.Writer(), log.Flags()
	t.Cleanup(func() {
		slog.SetDefault(prev)
		log.SetOutput(prevOut)
		log.SetFlags(prevFlags)
	})
	var stderr bytes.Buffer
	if err := runHook(t.Context(), []string{"no-such-hook"}, &stderr); err == nil || !strings.Contains(err.Error(), "unknown hook") {
		t.Fatalf("runHook = %v; want the unknown-hook error", err)
	}
	slog.Warn("s3wal delete lease clear failed; lease will expire on its window", "err", errors.New("dial "+host))
	for _, msg := range []string{"hook eventing unavailable: open meta store", "s3wal delete lease clear failed"} {
		if !strings.Contains(stderr.String(), msg) {
			t.Fatalf("stderr lacks %q: %s", msg, stderr.String())
		}
	}
	if n := strings.Count(stderr.String(), host); n != 0 {
		t.Fatalf("stderr names the canary host %d times: %s", n, stderr.String())
	}
	if n := strings.Count(hookErrorRecords(t), host); n < 2 {
		t.Fatalf("the hook-errors file names the canary host %d times, want both records", n)
	}
}

// A store URL that does not parse keeps its password out of every error,
// so out of the hook-errors file and serve's log too.
func TestMalformedStoreURLHidesItsPassword(t *testing.T) {
	pw := "canary_pass_" + rand.Text()[:8]
	// The "/" ends the authority early, so url.Parse reads part of the
	// password as a port and quotes the whole URL in its error.
	pgURL := "postgres://user:" + pw + "/x@host:5432/db"
	logs := hookEventingEnv(t, pgURL)
	cleanup := wireHookEventing(t.Context(), slog.New(githttp.NewHookLogHandler(logs, nil)))
	defer cleanup()
	records := hookErrorRecords(t)
	if !strings.Contains(records, "malformed URL") {
		t.Fatalf("the hook-errors file lacks the parse failure: %s", records)
	}
	seen := logs.String() + records
	for _, open := range []func() error{
		func() error { _, err := meta.Open(t.Context(), pgURL); return err },
		func() error { _, err := ref.Open(t.Context(), pgURL); return err },
		func() error {
			_, err := ref.Open(t.Context(), "s3wal://user:"+pw+"/x@bucket?endpoint=http://h")
			return err
		},
		func() error { _, err := blob.Open(t.Context(), "s3://user:"+pw+"/x@bucket"); return err },
		func() error { _, err := blob.Open(t.Context(), "file://user:"+pw+"/x@host/dir"); return err },
		// The schemes' own opens parse again.
		func() error { _, err := blob.OpenS3(t.Context(), "s3://user:"+pw+"/x@bucket"); return err },
		func() error { _, err := blob.OpenFile("file://user:" + pw + "/x@host/dir"); return err },
		func() error {
			_, err := s3wal.Dial(t.Context(), "s3wal://user:"+pw+"/x@bucket?endpoint=http://h")
			return err
		},
	} {
		err := open()
		if err == nil {
			t.Fatal("a malformed URL opened")
		}
		seen += err.Error()
	}
	if n := strings.Count(seen, pw); n != 0 {
		t.Fatalf("the errors and hook records name the canary password %d times: %s", n, seen)
	}
}

// The hook logger writes at Warn level: an Info line reaches neither the
// pusher nor the hook-errors file.
func TestSlogForHookIsWarnLevel(t *testing.T) {
	hookEventingEnv(t, "")
	var stderr bytes.Buffer
	logger := slogForHook(&stderr)
	logger.Info("below the level")
	logger.Warn("at the level")
	if strings.Contains(stderr.String(), "below the level") || !strings.Contains(stderr.String(), "at the level") {
		t.Fatalf("stderr = %q; want only the Warn line", stderr.String())
	}
	if records := hookErrorRecords(t); strings.Contains(records, "below the level") {
		t.Fatalf("the hook-errors file has the Info line: %s", records)
	}
}
