package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/coder/openstorage/internal/githttp"
	"github.com/coder/openstorage/internal/ref"
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
	original := githttp.WrapHookRefStore
	t.Cleanup(func() { githttp.WrapHookRefStore = original })
	return new(bytes.Buffer)
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

// assertHookAccepts proves today's fail-open behavior: the update wins its
// CAS, and the authority holds the new value.
func assertHookAccepts(t *testing.T, ctx context.Context, rs ref.RefStore) {
	t.Helper()
	wrapped, err := githttp.WrapHookRefStore(ctx, rs)
	if err != nil {
		t.Fatalf("hook refused the update: %v", err)
	}
	sha := strings.Repeat("a", 40)
	if won, err := wrapped.CASRef(ctx, "repo-1", "refs/heads/main", ref.ZeroSHA, sha); err != nil || !won {
		t.Fatalf("CAS = %v, %v; want accepted", won, err)
	}
	if got, err := rs.GetRef(ctx, "repo-1", "refs/heads/main"); err != nil || got != sha {
		t.Fatalf("authority ref = %q, %v; want %s", got, err, sha)
	}
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
		{"truncated", `{"v":2,"seq":3,"writer":"0123`,
			[]string{"_wal/meta.json is corrupt (document decode:", "backup-restore-s3wal.md#restore-and-validate-before-cutover"}},
		{"no writer", `{"v":2,"seq":3,"next_org_id":1,"next_key_id":0}`,
			[]string{"_wal/meta.json is corrupt (document seq 3 has no writer nonce)", "backup-restore-s3wal.md"}},
		{"negative counter", `{"v":2,"seq":3,"writer":"0123456789abcdef0123456789abcdef","next_org_id":-1,"next_key_id":0}`,
			[]string{"_wal/meta.json is corrupt (document seq 3 has negative id counters (-1, 0))", "backup-restore-s3wal.md"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeMetaS3{body: []byte(tc.body)}
			srv := httptest.NewServer(fake)
			defer srv.Close()
			logs := hookEventingEnv(t, "s3wal://b?endpoint="+srv.URL)
			cleanup := wireHookEventing(t.Context(), slog.New(slog.NewTextHandler(logs, nil)))
			defer cleanup()
			rs := hookRefStore(t)

			wrapped, err := githttp.WrapHookRefStore(t.Context(), rs)
			if err == nil || wrapped != nil {
				t.Fatalf("WrapHookRefStore = %v, %v; want a refusal", wrapped, err)
			}
			for _, want := range append([]string{"refusing the ref update"}, tc.want...) {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("refusal %q lacks %q", err, want)
				}
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
// refuses the update before its CAS: the format check fails open (issues
// #445, #466), but the push's forward intent must be in the row before the
// CAS (ADR-50 amendment), and without the document the hook cannot tell
// whether the repository forwards. The bucket sees no write.
func TestHookEventingRefusesUnreadableLocalSchedule(t *testing.T) {
	fake := &fakeMetaS3{down: true}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	logs := hookEventingEnv(t, "s3wal://b?endpoint="+srv.URL)
	cleanup := wireHookEventing(t.Context(), slog.New(slog.NewTextHandler(logs, nil)))
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
	if won || err == nil || !strings.Contains(err.Error(), "refusing the ref update") {
		t.Fatalf("CAS = %v, %v; want the update refused before its CAS", won, err)
	}
	if got, err := rs.GetRef(t.Context(), "repo-1", "refs/heads/main"); !errors.Is(err, ref.ErrNotFound) {
		t.Fatalf("authority ref = %q, %v; want unset", got, err)
	}
	if n := fake.writes.Load(); n != 0 {
		t.Fatalf("the hook sent %d write requests to the bucket; want none", n)
	}
}

// A local-schedule metadata store the hook cannot open refuses every update
// that would record forward intent, because the intent before the CAS needs
// the store. Updates that record no intent keep failing open, as on main:
// ephemeral updates, and a sync run's own loopback push (its claim fence
// refuses it when the store is down).
func TestHookEventingRefusesUnopenableLocalSchedule(t *testing.T) {
	sha := strings.Repeat("a", 40)
	for _, tc := range []struct {
		name, repo, ref, claim string
		refused                bool
	}{
		{name: "persistent", repo: "repo-1", ref: "refs/heads/main", refused: true},
		{name: "ephemeral key", repo: "repo-1+ephemeral", ref: "refs/heads/main"},
		{name: "ephemeral namespace", repo: "repo-1", ref: "refs/namespaces/ephemeral/refs/heads/main"},
		{name: "sync loopback", repo: "repo-1", ref: "refs/heads/main", claim: "holder"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			notADir := filepath.Join(t.TempDir(), "file")
			if err := os.WriteFile(notADir, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			logs := hookEventingEnv(t, "sqlite://"+filepath.Join(notADir, "meta.db"))
			t.Setenv(githttp.EnvSyncClaim, tc.claim)
			cleanup := wireHookEventing(t.Context(), slog.New(slog.NewTextHandler(logs, nil)))
			defer cleanup()
			rs := hookRefStore(t)
			wrapped, err := githttp.WrapHookRefStore(t.Context(), rs)
			if err != nil {
				t.Fatalf("WrapHookRefStore: %v", err)
			}
			won, _, err := wrapped.CASRefGen(t.Context(), tc.repo, tc.ref, ref.ZeroSHA, sha)
			got, gerr := rs.GetRef(t.Context(), tc.repo, tc.ref)
			if tc.refused {
				if won || err == nil || !strings.Contains(err.Error(), "refusing the ref update: open metadata store") || !errors.Is(gerr, ref.ErrNotFound) {
					t.Fatalf("CAS = (%v, %v), authority = (%q, %v); want the update refused before its CAS", won, err, got, gerr)
				}
				if !strings.Contains(logs.String(), "hook refuses ref updates: open meta store") {
					t.Fatalf("missing refusal log: %s", logs)
				}
				return
			}
			if !won || err != nil || got != sha {
				t.Fatalf("CAS = (%v, %v), authority = (%q, %v); want the update accepted (it records no intent)", won, err, got, gerr)
			}
		})
	}
}

// A PostgreSQL metadata store the hook cannot reach keeps today's
// fail-open behavior (issues #445, #466): the push is accepted without
// events, because the ref store would fail too if the store were truly
// down. River schedules PostgreSQL forwards; the intent before the CAS
// belongs to the local schedule.
func TestHookEventingFailsOpenOnTransientMetaErrors(t *testing.T) {
	t.Run("open fails", func(t *testing.T) {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := l.Addr().String()
		_ = l.Close() // nothing listens: the open's ping is refused
		logs := hookEventingEnv(t, "postgres://u:p@"+addr+"/db?connect_timeout=5")
		cleanup := wireHookEventing(t.Context(), slog.New(slog.NewTextHandler(logs, nil)))
		defer cleanup()
		assertHookAccepts(t, t.Context(), hookRefStore(t))
		if !strings.Contains(logs.String(), "hook eventing disabled: open meta store") {
			t.Fatalf("missing fail-open log: %s", logs)
		}
	})
}
