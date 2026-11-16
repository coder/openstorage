package sync

import (
	"context"
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"
)

// longPathUpstream is a git smart-HTTP upstream (git http-backend over CGI)
// that serves root/own/proj.git under any repository name, so a URL of any
// length can name a real repository. It answers protocol v0, where git
// hands the URL with a "/" added to fetch-pack as well as to send-pack.
func longPathUpstream(t *testing.T, root string) string {
	t.Helper()
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Skip("git not on PATH")
	}
	backend := &cgi.Handler{
		Path: gitPath,
		Args: []string{"http-backend"},
		Env:  []string{"GIT_PROJECT_ROOT=" + root, "GIT_HTTP_EXPORT_ALL=1", "GIT_CONFIG_NOSYSTEM=1", "HOME=" + root},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if name, rest, ok := strings.Cut(strings.TrimPrefix(r.URL.Path, "/own/"), ".git/"); ok && name != "" {
			r.URL.Path = "/own/proj.git/" + rest
		}
		r.Header.Del("Git-Protocol")
		backend.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// TestSyncArgLimitsAreTheDocumentedNumbers pins the limits that
// docs/reference/http-api.md states.
func TestSyncArgLimitsAreTheDocumentedNumbers(t *testing.T) {
	if maxUpstreamURLBytes != 131070 || maxCredentialBytes != 131042 {
		t.Fatalf("maxUpstreamURLBytes = %d, maxCredentialBytes = %d; the docs say 131070 and 131042", maxUpstreamURLBytes, maxCredentialBytes)
	}
}

// TestUpstreamValuesPastGitArgLimit: an upstream URL or a stored credential
// that git cannot take fails a pull and a push as a configuration error,
// and the error does not name the value. A run would fail the same way
// every time. At the limit the run succeeds: git takes the URL as an
// argument (and its HTTP transport hands it on with a "/" added), and the
// credential as an environment entry.
func TestUpstreamValuesPastGitArgLimit(t *testing.T) {
	type values struct{ urlBytes, username, password int }
	for _, tc := range []struct {
		name   string
		v      values
		config bool
	}{
		{"URL at the limit", values{urlBytes: maxUpstreamURLBytes}, false},
		{"URL one byte over", values{urlBytes: maxUpstreamURLBytes + 1}, true},
		{"URL of 200,000 bytes", values{urlBytes: 200_000}, true},
		{"password at the limit", values{password: maxCredentialBytes}, false},
		{"password one byte over", values{password: maxCredentialBytes + 1}, true},
		{"password of 200,000 bytes", values{password: 200_000}, true},
		{"username at the limit", values{username: maxCredentialBytes}, false},
		{"password at the limit next to a username", values{username: 1, password: maxCredentialBytes}, false},
		{"username one byte over", values{username: maxCredentialBytes + 1}, true},
	} {
		for _, lane := range []string{"pull", "push"} {
			t.Run(lane+"/"+tc.name, func(t *testing.T) {
				f := newWorkerFixture(t)
				host, name := "file://"+f.upstreamDir, "proj"
				if tc.v.urlBytes > 0 {
					host = longPathUpstream(t, f.upstreamDir)
					name = strings.Repeat("n", tc.v.urlBytes-len(host+"/own/.git"))
				}
				if lane == "pull" {
					seedUpstream(t, f.upstreamDir, "own", "proj")
					f.addLoopbackRepo(t, "repo_arg")
				} else {
					seedLocalRepo(t, f, "repo_arg")
					bare := addEmptyUpstream(t, f.upstreamDir, "own", "proj")
					testGit(t, bare, "config", "http.receivepack", "true")
				}
				f.putUpstream(t, "repo_arg", "own", name, host)
				up, err := f.store.GetUpstream(context.Background(), "repo_arg")
				if err != nil {
					t.Fatal(err)
				}
				if tc.v.urlBytes > 0 && len(up.CloneURL()) != tc.v.urlBytes {
					t.Fatalf("URL is %d bytes, want %d", len(up.CloneURL()), tc.v.urlBytes)
				}
				if tc.v.username+tc.v.password > 0 {
					cred := Credential{RepoID: "repo_arg", Username: strings.Repeat("u", tc.v.username), Password: "p"}
					if tc.v.password > 0 {
						// With no username, gitea sends the token as the
						// username: git gets the password entry only next to one.
						cred.Password = strings.Repeat("p", tc.v.password)
					}
					if _, err := f.store.CreateCredential(context.Background(), cred); err != nil {
						t.Fatal(err)
					}
				}
				var runErr error
				if lane == "pull" {
					runErr = f.worker.Run(context.Background(), Job{Org: "acme", RepoID: "repo_arg"})
				} else {
					if err := f.store.RequestPush(context.Background(), "repo_arg", false); err != nil {
						t.Fatal(err)
					}
					runErr = f.worker.RunPush(context.Background(), Job{Org: "acme", RepoID: "repo_arg"})
				}
				events := f.rec.list()
				if len(events) != 2 {
					t.Fatalf("got %d events (%+v), want 2", len(events), events)
				}
				if !tc.config {
					if events[1].Error != "" || runErr != nil {
						t.Fatalf("error = %q, run %v; want a clean run", events[1].Error, runErr)
					}
					return
				}
				if events[1].Error != FailConfig {
					t.Fatalf("error = %q (run %v), want %q", events[1].Error, runErr, FailConfig)
				}
				for _, c := range "nup" {
					if runErr != nil && strings.Contains(runErr.Error(), strings.Repeat(string(c), 64)) {
						t.Fatalf("the failure names the value: %.200s", runErr)
					}
				}
			})
		}
	}
}
