package sync

import (
	"context"
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"os/exec"
	stdsync "sync"
	"testing"
)

// gatedUpstream is a git smart-HTTP upstream (git http-backend over CGI)
// that answers 401 unless the request carries Basic user:pass. It records
// the username of every request that sent Basic credentials.
type gatedUpstream struct {
	root string
	url  string
	mu   stdsync.Mutex
	seen []string
}

func newGatedUpstream(t *testing.T, user, pass string) *gatedUpstream {
	t.Helper()
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Skip("git not on PATH")
	}
	u := &gatedUpstream{root: t.TempDir()}
	backend := &cgi.Handler{
		Path: gitPath,
		Args: []string{"http-backend"},
		Env:  []string{"GIT_PROJECT_ROOT=" + u.root, "GIT_HTTP_EXPORT_ALL=1", "GIT_CONFIG_NOSYSTEM=1", "HOME=" + u.root},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser, gotPass, ok := r.BasicAuth()
		if ok {
			u.mu.Lock()
			u.seen = append(u.seen, gotUser)
			u.mu.Unlock()
		}
		if !ok || gotUser != user || gotPass != pass {
			w.Header().Set("WWW-Authenticate", `Basic realm="git"`)
			http.Error(w, "auth required", http.StatusUnauthorized)
			return
		}
		backend.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	u.url = srv.URL
	return u
}

func (u *gatedUpstream) usernames() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]string(nil), u.seen...)
}

// TestCredentialUsernameOnTheWire drives real pull and push transfers
// against an upstream that accepts exactly one Basic user:password. A
// stored username is sent unchanged. An omitted one (git sends no
// credentials for an empty username) becomes x-token-auth for provider
// bitbucket and "git" for every other provider (issue #396).
func TestCredentialUsernameOnTheWire(t *testing.T) {
	const token = "tok-secret"
	for _, tc := range []struct{ provider, stored, wire string }{
		{"gitea", "", "git"},
		{"bitbucket", "", "x-token-auth"},
		{"gitea", "oauth2", "oauth2"},
		{"bitbucket", "jdoe", "jdoe"},
	} {
		name := tc.provider + "/" + tc.wire
		putUpstream := func(t *testing.T, f *workerFixture, repoID, host string) {
			t.Helper()
			if _, err := f.store.PutUpstream(context.Background(), Upstream{
				RepoID: repoID, Provider: tc.provider, Owner: "own", Name: "proj", UpstreamHost: host,
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.CreateCredential(context.Background(), Credential{RepoID: repoID, Username: tc.stored, Password: token}); err != nil {
				t.Fatal(err)
			}
		}
		t.Run(name+"/pull", func(t *testing.T) {
			f := newWorkerFixture(t)
			up := newGatedUpstream(t, tc.wire, token)
			head := seedUpstream(t, up.root, "own", "proj")
			loop := f.addLoopbackRepo(t, "repo_cu")
			putUpstream(t, f, "repo_cu", up.url)
			if err := f.worker.Run(context.Background(), Job{Org: "acme", RepoID: "repo_cu"}); err != nil {
				t.Fatalf("Run: %v (usernames seen: %q)", err, up.usernames())
			}
			if got := testGit(t, f.loopbackDir, "-C", loop, "rev-parse", "refs/heads/main"); got != head {
				t.Fatalf("loopback main = %s, want %s", got, head)
			}
			requireOnlyUsername(t, up.usernames(), tc.wire)
		})
		t.Run(name+"/push", func(t *testing.T) {
			f := newWorkerFixture(t)
			up := newGatedUpstream(t, tc.wire, token)
			head := seedLocalRepo(t, f, "repo_cp")
			bare := addEmptyUpstream(t, up.root, "own", "proj")
			testGit(t, bare, "config", "http.receivepack", "true")
			putUpstream(t, f, "repo_cp", up.url)
			if err := f.store.RequestPush(context.Background(), "repo_cp", false); err != nil {
				t.Fatal(err)
			}
			if err := f.worker.RunPush(context.Background(), Job{Org: "acme", RepoID: "repo_cp"}); err != nil {
				t.Fatalf("RunPush: %v (usernames seen: %q)", err, up.usernames())
			}
			if got := testGit(t, up.root, "-C", bare, "rev-parse", "refs/heads/main"); got != head {
				t.Fatalf("upstream main = %s, want %s", got, head)
			}
			requireOnlyUsername(t, up.usernames(), tc.wire)
		})
	}

	// No stored credential: git sends no Basic credentials and the gated
	// upstream refuses the pull.
	t.Run("no credential/pull", func(t *testing.T) {
		f := newWorkerFixture(t)
		up := newGatedUpstream(t, "git", token)
		seedUpstream(t, up.root, "own", "proj")
		f.addLoopbackRepo(t, "repo_cn")
		f.putUpstream(t, "repo_cn", "own", "proj", up.url)
		if err := f.worker.Run(context.Background(), Job{Org: "acme", RepoID: "repo_cn"}); err == nil {
			t.Fatal("Run succeeded against a gated upstream without a credential")
		}
		if got := up.usernames(); len(got) != 0 {
			t.Fatalf("usernames seen without a credential: %q", got)
		}
	})
}

func requireOnlyUsername(t *testing.T, seen []string, want string) {
	t.Helper()
	if len(seen) == 0 {
		t.Fatal("upstream saw no Basic credentials")
	}
	for _, got := range seen {
		if got != want {
			t.Fatalf("upstream saw username %q, want only %q (all: %q)", got, want, seen)
		}
	}
}
