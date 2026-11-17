package sync

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// TestViaEgressProxy: an http or https upstream URL needs the egress proxy
// whatever the letter case of its scheme; no other scheme does.
func TestViaEgressProxy(t *testing.T) {
	for url, want := range map[string]bool{
		"http://forge.example/o/n.git":  true,
		"https://forge.example/o/n.git": true,
		"HTTP://forge.example/o/n.git":  true,
		"HTTPS://forge.example/o/n.git": true,
		"Https://forge.example/o/n.git": true,
		"hTtP://forge.example/o/n.git":  true,
		"file:///srv/o/n.git":           false,
		"ssh://forge.example/o/n.git":   false,
		"httpx://forge.example/o/n.git": false,
		"xhttps://forge.example/o.git":  false,
		"ext::sh -c true":               false,
	} {
		if got := viaEgressProxy(url); got != want {
			t.Errorf("viaEgressProxy(%q) = %v, want %v", url, got, want)
		}
	}
}

// takeUpperCaseHTTP makes the worker's git take an `HTTP://` upstream: its
// GIT_ALLOW_PROTOCOL lists HTTP, and git finds a git-remote-HTTP helper,
// the real git-remote-http, as it would on a case-insensitive file system.
// The production default ("http:https") refuses the transport before any
// dial ("transport 'HTTP' not allowed"), so this is the only way a test
// sees where git would dial such a link.
func takeUpperCaseHTTP(t *testing.T, w *Worker) {
	t.Helper()
	out, err := exec.Command("git", "--exec-path").Output()
	if err != nil {
		t.Fatalf("git --exec-path: %v", err)
	}
	helper := filepath.Join(strings.TrimSpace(string(out)), "git-remote-http")
	if _, err := os.Stat(helper); err != nil {
		t.Fatalf("git http helper: %v", err)
	}
	bin := t.TempDir()
	if err := os.Symlink(helper, filepath.Join(bin, "git-remote-HTTP")); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	w.AllowProtocol = "HTTP:" + w.AllowProtocol
}

// unreachedServer is a loopback upstream that records whether anything
// reached it.
func unreachedServer(t *testing.T) (url string, reached *atomic.Bool) {
	t.Helper()
	reached = new(atomic.Bool)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached.Store(true)
		http.Error(w, "unreachable", http.StatusTeapot)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, reached
}

// TestWorkerUpperCaseSchemeUpstreamDialsThroughProxy: a stored upstream
// whose scheme is upper case (`HTTP://`) fetches through the egress proxy
// like a lower-case one, so the strict policy refuses its loopback host
// and the upstream is never contacted. Before, only a lower-case scheme
// started the proxy, and a git that took the transport dialed the link
// directly.
func TestWorkerUpperCaseSchemeUpstreamDialsThroughProxy(t *testing.T) {
	f := newWorkerFixture(t)
	takeUpperCaseHTTP(t, f.worker)
	f.worker.PullEgressAllowPrivate = false // strict policy
	url, reached := unreachedServer(t)
	f.putUpstream(t, "repo_uc", "own", "proj", "HTTP://"+strings.TrimPrefix(url, "http://"))

	if err := f.worker.Run(context.Background(), Job{Org: "acme", RepoID: "repo_uc"}); err == nil {
		t.Fatal("Run succeeded against a policy-denied private upstream")
	}
	if reached.Load() {
		t.Fatal("the HTTP:// upstream was dialed around the egress proxy")
	}
	events := f.rec.list()
	assertEventTypes(t, events, EventSyncStarted, EventSyncFailed)
	if events[1].Error != FailConfig { // a refused transport would be FailClone
		t.Fatalf("error = %q, want %q", events[1].Error, FailConfig)
	}
}

// TestPushRunUpperCaseSchemeUpstreamDialsThroughProxy is the push lane's
// twin of TestWorkerUpperCaseSchemeUpstreamDialsThroughProxy.
func TestPushRunUpperCaseSchemeUpstreamDialsThroughProxy(t *testing.T) {
	f := newWorkerFixture(t)
	takeUpperCaseHTTP(t, f.worker)
	f.worker.PushEgressAllowPrivate = false // production default
	seedLocalRepo(t, f, "repo_ucp")
	url, reached := unreachedServer(t)
	f.putUpstream(t, "repo_ucp", "own", "proj", "HTTP://"+strings.TrimPrefix(url, "http://"))

	if err := f.worker.RunPush(context.Background(), Job{Org: "acme", RepoID: "repo_ucp"}); err == nil {
		t.Fatal("RunPush succeeded against a policy-denied private upstream")
	}
	if reached.Load() {
		t.Fatal("the HTTP:// upstream was dialed around the egress proxy")
	}
	events := f.rec.list()
	assertEventTypes(t, events, EventPushUpstreamStarted, EventPushUpstreamFailed)
	if events[1].Error != FailConfig { // a refused transport would be FailForward
		t.Fatalf("error = %q, want %q", events[1].Error, FailConfig)
	}
}
