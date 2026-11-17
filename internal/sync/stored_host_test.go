package sync

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	stdsync "sync"
	"sync/atomic"
	"testing"
)

// lockedBuffer is a bytes.Buffer that a logger can write from several
// goroutines.
type lockedBuffer struct {
	mu  stdsync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestRunRefusesStoredInvalidHost: a pull or push run whose stored host
// POST /repos now refuses (ValidUpstreamHost), as a link that an older
// release stored can hold, fails as configuration before any git starts.
// The run's error and the server log give POST /repos's reason, never the
// host. Before, the run handed such a host to git: the loopback forms here
// reached the upstream through the allow-private egress policy.
func TestRunRefusesStoredInvalidHost(t *testing.T) {
	var reached atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached.Store(true)
		http.Error(w, "not a git server", http.StatusNotFound)
	}))
	defer srv.Close()
	port := srv.URL[strings.LastIndexByte(srv.URL, ':'):]

	const cause = "repository configuration error: upstream host is not a valid host"
	for _, host := range []string{
		"bücher.example",
		"https://bücher.example",
		"http://2130706433" + port, // curl dials 127.0.0.1
		"http://0x7f.1" + port,     // curl dials 127.0.0.1
		"http://127.0.0.1" + port + "?",
		"http://127.0.0.1" + port + "/a b",
		"HTTP://127.0.0.1" + port,
		"forge.example:65536",
		"https://[fe80::1%25eth0]",
	} {
		for _, lane := range []string{"pull", "push"} {
			// A % in the name would reach the file:// URL of the
			// fixture's t.TempDir.
			t.Run(lane+" "+strings.ReplaceAll(host, "%", "_"), func(t *testing.T) {
				reached.Store(false)
				f := newWorkerFixture(t)
				var logs lockedBuffer
				f.worker.Logger = slog.New(slog.NewTextHandler(&logs, nil))
				f.putUpstream(t, "repo_ih", "own", "proj", host)
				var err error
				want := []string{EventSyncStarted, EventSyncFailed}
				if lane == "pull" {
					err = f.worker.Run(t.Context(), Job{Org: "acme", RepoID: "repo_ih"})
				} else {
					seedLocalRepo(t, f, "repo_ih")
					err = f.worker.RunPush(t.Context(), Job{Org: "acme", RepoID: "repo_ih"})
					want = []string{EventPushUpstreamStarted, EventPushUpstreamFailed}
				}
				if err == nil || err.Error() != cause {
					t.Fatalf("run = %v; want %q", err, cause)
				}
				events := f.rec.list()
				assertEventTypes(t, events, want...)
				if events[1].Error != FailConfig {
					t.Fatalf("error = %q, want %q", events[1].Error, FailConfig)
				}
				if reached.Load() {
					t.Fatal("the run dialed the upstream of a host that POST /repos refuses")
				}
				if got := logs.String(); !strings.Contains(got, `cause="`+cause+`"`) || strings.Contains(got, host) {
					t.Fatalf("the failure log lacks the reason or names the host: %s", got)
				}
			})
		}
	}
}
