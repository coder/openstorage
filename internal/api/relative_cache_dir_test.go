package api

// Issue #363: --cache-dir accepts a relative path. The REST write
// pipelines run git with the (relative) cache repo as working directory
// and hand it the cache odb as an alternate through the environment, so a
// relative cache dir made git resolve the alternate against the repo
// itself and every commit-pack, merge, and notes request answered 500.
// These tests drive the full api.New handler with a relative CacheDir.

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/gitread"
)

// relCacheDir is the relative cache dir every test here hands api.New.
const relCacheDir = "relcache"

// newRelativeCacheServer chdirs into a fresh temp dir and serves the full
// API over the lane's stores with CacheDir "relcache". It returns the
// server and the absolute path of that cache dir.
func newRelativeCacheServer(t *testing.T, e *testEnv) (*httptest.Server, string) {
	t.Helper()
	cwd := t.TempDir()
	t.Chdir(cwd)
	// serve creates the cache dir at startup (cmd/openstorage).
	if err := os.MkdirAll(relCacheDir, 0o755); err != nil {
		t.Fatal(err)
	}
	handler, err := New(Config{
		Store:       e.store,
		Verifier:    auth.NewVerifier(e.store),
		Logger:      discardLogger(),
		HTTPBaseURL: testHTTPBaseURL,
		RefStore:    e.refStore,
		BlobStore:   e.blobStore,
		CacheDir:    relCacheDir,
		SyncQueue:   envSyncQueue{},
	})
	if err != nil {
		t.Fatalf("api.New: %v", err)
	}
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv, filepath.Join(cwd, relCacheDir)
}

// warmRelativeCache hydrates repoID into the relative cache through its
// absolute path, as a git push does in production, so the request under
// test starts from a warm cache repo.
func warmRelativeCache(t *testing.T, e *testEnv, absCache, repoID string) {
	t.Helper()
	if _, err := gitread.Materialize(context.Background(), e.refStore, e.blobStore, absCache, repoID, "main"); err != nil {
		t.Fatalf("warm relative cache: %v", err)
	}
}

func TestRelativeCacheDirCommitPack(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := wrefSeedFixture(t, e)
		srv, absCache := newRelativeCacheServer(t, e)
		warmRelativeCache(t, e, absCache, fx.repoID)

		body := wctLines(t,
			wctMeta("main", "relative cache dir", []map[string]any{
				{"path": "rel.txt", "content_id": "id-rel", "operation": "upsert", "mode": "100644"},
			}, nil),
			wctChunk("id-rel", []byte("rel\n"), true),
		)
		resp, respBody := wctPost(t, srv, "/api/v1/repos/commit-pack", fx.token, bytes.NewReader(body))
		env := wctRequireOK(t, resp, respBody)
		if env.Result.OldSHA != fx.c2 {
			t.Fatalf("old_sha = %q, want %s", env.Result.OldSHA, fx.c2)
		}
		if got := wrefRefs(t, e, fx.repoID)["refs/heads/main"]; got != env.Commit.CommitSHA {
			t.Fatalf("refs/heads/main = %q, want %s", got, env.Commit.CommitSHA)
		}
	})
}

func TestRelativeCacheDirMerge(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := wmSeedFixture(t, e)
		srv, absCache := newRelativeCacheServer(t, e)
		warmRelativeCache(t, e, absCache, fx.repoID)

		resp, raw := wmDo(t, srv, "POST", "/api/v1/repos/merge", fx.token,
			wmMergeBody("feature", "t-clean", "merge", map[string]any{"author": wmTestAuthor}))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, body %s", resp.StatusCode, raw)
		}
		if body := wmDecode(t, raw); body["result"] != "merge_commit" {
			t.Fatalf("result = %v", body["result"])
		}
	})
}

func TestRelativeCacheDirNotes(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := wrefSeedFixture(t, e)
		srv, absCache := newRelativeCacheServer(t, e)
		warmRelativeCache(t, e, absCache, fx.repoID)

		// The first note lands on an unborn notes ref and never reads the
		// cache odb; the second seeds its temp ref from the existing notes
		// commit, which only the cache odb (the alternate) holds.
		for _, sha := range []string{fx.c1, fx.c2} {
			resp, body := wrefDo(t, srv, "POST", "/api/v1/repos/notes", fx.token,
				map[string]any{"sha": sha, "action": "add", "note": "relative"})
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("note on %s: status = %d body %v", sha, resp.StatusCode, body)
			}
			if got := wrefRefs(t, e, fx.repoID)["refs/notes/commits"]; got == "" || got != body["new_ref_sha"] {
				t.Fatalf("refs/notes/commits = %q, want %v", got, body["new_ref_sha"])
			}
		}
	})
}

// A cold relative cache: hydration itself (index-pack of the downloaded
// pack) ran with a relative pack path from inside the repo.
func TestRelativeCacheDirColdRead(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := wrefSeedFixture(t, e)
		srv, _ := newRelativeCacheServer(t, e)
		readTok := e.mint(t, tokenSpec{iss: fx.org, repo: fx.url, scopes: []string{auth.ScopeGitRead}})

		req, err := http.NewRequest(http.MethodGet, srv.URL+"/api/v1/repos/file?path=a.txt&ref=main", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+readTok)
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var got bytes.Buffer
		if _, err := got.ReadFrom(resp.Body); err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusOK || got.String() != "v2\n" {
			t.Fatalf("a.txt = %d %q, want 200 \"v2\\n\"", resp.StatusCode, got.String())
		}
	})
}
