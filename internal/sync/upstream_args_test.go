package sync

import (
	"context"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/gitlimit"
)

// TestUpstreamValuesPastGitArgLimit: an upstream URL that git cannot take
// as one argument, or a stored credential that git cannot take in its
// environment, fails a pull and a push as a configuration error before git
// starts, and the error does not name the value. A run would fail the same
// way every time. At the limit git runs: a credential that fits syncs, and
// a URL that fits reaches git, which finds no repository there.
func TestUpstreamValuesPastGitArgLimit(t *testing.T) {
	for _, tc := range []struct {
		name                string
		urlBytes, credBytes int
		config              bool
	}{
		{"URL at the limit", gitlimit.MaxArgBytes, 0, false},
		{"URL one byte over", gitlimit.MaxArgBytes + 1, 0, true},
		{"URL of 200,000 bytes", 200_000, 0, true},
		{"credential at the limit", 0, maxCredentialBytes, false},
		{"credential one byte over", 0, maxCredentialBytes + 1, true},
		{"credential of 200,000 bytes", 0, 200_000, true},
	} {
		for _, lane := range []string{"pull", "push"} {
			t.Run(lane+"/"+tc.name, func(t *testing.T) {
				f := newWorkerFixture(t)
				host := "file://" + f.upstreamDir
				name := "proj"
				if tc.urlBytes > 0 {
					name = strings.Repeat("n", tc.urlBytes-len(host+"/own/.git"))
				}
				if lane == "pull" {
					seedUpstream(t, f.upstreamDir, "own", "proj")
					f.addLoopbackRepo(t, "repo_arg")
				} else {
					seedLocalRepo(t, f, "repo_arg")
					addEmptyUpstream(t, f.upstreamDir, "own", "proj")
				}
				f.putUpstream(t, "repo_arg", "own", name, host)
				up, err := f.store.GetUpstream(context.Background(), "repo_arg")
				if err != nil {
					t.Fatal(err)
				}
				if tc.urlBytes > 0 && len(up.CloneURL()) != tc.urlBytes {
					t.Fatalf("URL is %d bytes, want %d", len(up.CloneURL()), tc.urlBytes)
				}
				if tc.credBytes > 0 {
					// Gitea sends the token as the username (credentialAuth).
					if _, err := f.store.CreateCredential(context.Background(), Credential{RepoID: "repo_arg", Password: strings.Repeat("p", tc.credBytes)}); err != nil {
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
				failure := events[1].Error
				switch {
				case tc.config:
					if failure != FailConfig {
						t.Fatalf("error = %q (run %v), want %q", failure, runErr, FailConfig)
					}
					if runErr != nil && (strings.Contains(runErr.Error(), strings.Repeat("n", 64)) || strings.Contains(runErr.Error(), strings.Repeat("p", 64))) {
						t.Fatalf("the failure names the value: %.200s", runErr)
					}
				case tc.credBytes > 0:
					if failure != "" || runErr != nil {
						t.Fatalf("a credential that fits: error = %q, run %v; want a clean run", failure, runErr)
					}
				default:
					// git ran and found no repository at the long URL.
					if failure == "" || failure == FailConfig || runErr != nil && strings.Contains(runErr.Error(), "argument list too long") {
						t.Fatalf("a URL that fits: error = %q, run %v; want git's own failure", failure, runErr)
					}
				}
			})
		}
	}
}
