package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
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

// Issues #445 and #466: a hook on a bucket with an older metadata version
// (binary replaced under an older serve), or on a corrupt document, refuses
// the ref update before its CAS, instead of accepting the push and dropping
// its events. The refusal names the fix, and the hook writes nothing.
func TestHookEventingRefusesUnusableMetaDocument(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want []string
	}{
		{"v1", `{"v":1,"seq":3,"writer":"0123456789abcdef0123456789abcdef","next_org_id":1,"next_key_id":0}`,
			[]string{"document version 1 needs conversion", "openstorage s3wal-meta convert --to 3"}},
		{"v2", `{"v":2,"seq":3,"writer":"0123456789abcdef0123456789abcdef","next_org_id":1,"next_key_id":0}`,
			[]string{"document version 2 needs conversion", "openstorage s3wal-meta convert --to 3"}},
		{"truncated", `{"v":3,"seq":3,"writer":"0123`,
			[]string{"_wal/meta.json is corrupt (document decode:", "backup-restore-s3wal.md#restore-and-validate-before-cutover"}},
		{"no writer", `{"v":3,"seq":3,"next_org_id":1,"next_key_id":0}`,
			[]string{"_wal/meta.json is corrupt (document seq 3 has no writer nonce)", "backup-restore-s3wal.md"}},
		{"negative counter", `{"v":3,"seq":3,"writer":"0123456789abcdef0123456789abcdef","next_org_id":-1,"next_key_id":0}`,
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

// A metadata store the hook cannot reach keeps today's fail-open behavior
// (issues #445, #466): the push is accepted without events, because the ref store
// would fail too if the store were truly down.
func TestHookEventingFailsOpenOnTransientMetaErrors(t *testing.T) {
	t.Run("format read fails", func(t *testing.T) {
		fake := &fakeMetaS3{down: true}
		srv := httptest.NewServer(fake)
		defer srv.Close()
		logs := hookEventingEnv(t, "s3wal://b?endpoint="+srv.URL)
		cleanup := wireHookEventing(t.Context(), slog.New(slog.NewTextHandler(logs, nil)))
		defer cleanup()
		assertHookAccepts(t, t.Context(), hookRefStore(t))
		if !strings.Contains(logs.String(), "metadata format check failed") {
			t.Fatalf("missing fail-open log: %s", logs)
		}
	})
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
