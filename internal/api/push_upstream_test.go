package api

// POST /repos/push-upstream lifecycle tests (issue #90) over the
// usyncStack: real git transfers against a JWT-gated CGI loopback and a
// credential-gated CGI upstream whose bare repos accept receive-pack.

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/coder/code-storage/internal/auth"
	upsync "github.com/coder/code-storage/internal/sync"
)

// usyncAllowPush enables http.receivepack on a seeded upstream bare repo so
// the CGI server accepts pushes (the pull-only tests never need it).
func usyncAllowPush(t *testing.T, root, owner, name string) {
	t.Helper()
	usyncGit(t, root, "-C", filepath.Join(root, owner, name+".git"), "config", "http.receivepack", "true")
}

// pushLocalCommit adds one commit on main directly to the loopback bare
// repo — a "local push" the outbound run should publish.
func pushLocalCommit(t *testing.T, s *usyncStack, repoID string) string {
	t.Helper()
	bare := filepath.Join(s.loopRoot, repoID+".git")
	work := t.TempDir()
	usyncGit(t, work, "clone", "file://"+bare, ".")
	if err := os.WriteFile(filepath.Join(work, "local.txt"), []byte("local\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	usyncGit(t, work, "add", ".")
	usyncGit(t, work, "commit", "-m", "local change")
	usyncGit(t, work, "push", "origin", "main")
	return usyncGit(t, work, "rev-parse", "HEAD")
}

// upstreamSHA resolves a ref in the upstream bare repo ("" when absent).
func upstreamSHA(t *testing.T, root, owner, name, ref string) string {
	t.Helper()
	return usyncGit(t, root, "-C", filepath.Join(root, owner, name+".git"), "rev-parse", "--verify", ref)
}

// TestPushUpstreamLifecycle drives the outbound story end to end: base_repo
// create -> first inbound sync -> local commit -> push-upstream 202 ->
// direction "push" events + refs on the upstream -> policy fail-closed ->
// non-fast-forward rejection with per-ref reasons -> force override.
func TestPushUpstreamLifecycle(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		s := newUsyncStack(t, e)
		upRoot := t.TempDir()
		const upstreamPass = "up-secret"
		upstream := usyncCGIServer(t, upRoot, func(r *http.Request) bool {
			user, pass, ok := r.BasicAuth()
			return ok && basicAuthEquals(user, pass, "git", upstreamPass)
		}, false)
		usyncSeedUpstream(t, upRoot, "own", "proj")
		usyncAllowPush(t, upRoot, "own", "proj")

		repoURL := "team/forward-" + randomHex(t, 4)
		body, err := json.Marshal(map[string]any{
			"base_repo": map[string]any{
				"provider":      "gitea",
				"owner":         "own",
				"name":          "proj",
				"upstream_host": upstream.URL,
				"auth":          map[string]string{"token": upstreamPass},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		repoID := s.createRepo(t, repoURL, body)
		s.waitEvent(t, upsync.EventSyncStarted)
		s.waitEvent(t, upsync.EventSyncSucceeded)

		// Local commit, then the explicit outbound trigger: exact 202 body.
		localHead := pushLocalCommit(t, s, repoID)
		// A write-only token is refused before any work: the forward
		// exports every local head/tag, so it demands the caller's own
		// git:read (a write-only principal must not exfiltrate content
		// it cannot clone to an upstream it can read).
		writeOnly := s.e.mint(t, tokenSpec{iss: s.org, repo: repoURL, scopes: []string{auth.ScopeGitWrite}})
		resp, respBody := s.req(t, http.MethodPost, "/api/v1/repos/push-upstream", writeOnly, nil)
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("write-only push-upstream status %d: %s", resp.StatusCode, respBody)
		}
		s.assertNoEvent(t)

		writeToken := s.e.mint(t, tokenSpec{iss: s.org, repo: repoURL, scopes: []string{auth.ScopeGitWrite, auth.ScopeGitRead}})
		resp, respBody = s.req(t, http.MethodPost, "/api/v1/repos/push-upstream", writeToken, nil)
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("push-upstream status %d: %s", resp.StatusCode, respBody)
		}
		var accepted map[string]string
		if err := json.Unmarshal(respBody, &accepted); err != nil {
			t.Fatal(err)
		}
		if accepted["message"] != "Repository push initiated successfully" || len(accepted) != 1 {
			t.Fatalf("202 body = %s", respBody)
		}
		started := s.waitEvent(t, upsync.EventSyncStarted)
		if started.Direction != upsync.DirectionPush || started.RunCount != 0 || !started.IsFirstSync || started.Forced {
			t.Fatalf("bad push started event: %+v", started)
		}
		done := s.waitEvent(t, upsync.EventSyncSucceeded)
		if done.Direction != upsync.DirectionPush || done.RunCount != 1 || done.Error != "" {
			t.Fatalf("bad push succeeded event: %+v", done)
		}
		if got := upstreamSHA(t, upRoot, "own", "proj", "refs/heads/main"); got != localHead {
			t.Fatalf("upstream main = %q, want forwarded %q", got, localHead)
		}

		// Policed callers fail closed, mirroring pull-upstream: the run
		// would publish every local head/tag with the stored credential.
		policedToken := s.e.mint(t, tokenSpec{iss: s.org, repo: repoURL,
			scopes: []string{auth.ScopeGitWrite, auth.ScopeGitRead}, refs: `[["main", ["no-push"]]]`})
		resp, respBody = s.req(t, http.MethodPost, "/api/v1/repos/push-upstream", policedToken, nil)
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("policed push-upstream status %d: %s", resp.StatusCode, respBody)
		}
		var denied map[string]string
		if err := json.Unmarshal(respBody, &denied); err != nil || denied["error"] != `ref policy op "no-push" denies push-upstream` {
			t.Fatalf("policed push-upstream body = %s (err %v)", respBody, err)
		}
		s.assertNoEvent(t)

		// Upstream moves ahead: a non-force push of the now-stale local
		// state must fail per ref without rewriting upstream history.
		upstreamHead := usyncAddUpstreamCommit(t, upRoot, "own", "proj")
		resp, respBody = s.req(t, http.MethodPost, "/api/v1/repos/push-upstream", writeToken, nil)
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("push-upstream status %d: %s", resp.StatusCode, respBody)
		}
		s.waitEvent(t, upsync.EventSyncStarted)
		failed := s.waitEvent(t, upsync.EventSyncFailed)
		if failed.Error != upsync.FailRejected || failed.Direction != upsync.DirectionPush {
			t.Fatalf("bad rejected event: %+v", failed)
		}
		if len(failed.RejectedRefs) != 1 || failed.RejectedRefs[0].Ref != "refs/heads/main" || failed.RejectedRefs[0].Reason == "" {
			t.Fatalf("rejected_refs = %+v", failed.RejectedRefs)
		}
		if got := upstreamSHA(t, upRoot, "own", "proj", "refs/heads/main"); got != upstreamHead {
			t.Fatalf("upstream main = %q, want untouched %q", got, upstreamHead)
		}

		// force: true opts into +refs and rewinds the upstream to local.
		resp, respBody = s.req(t, http.MethodPost, "/api/v1/repos/push-upstream", writeToken, []byte(`{"force":true}`))
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("forced push-upstream status %d: %s", resp.StatusCode, respBody)
		}
		forcedStarted := s.waitEvent(t, upsync.EventSyncStarted)
		if !forcedStarted.Forced {
			t.Fatalf("forced run not marked forced: %+v", forcedStarted)
		}
		s.waitEvent(t, upsync.EventSyncSucceeded)
		if got := upstreamSHA(t, upRoot, "own", "proj", "refs/heads/main"); got != localHead {
			t.Fatalf("upstream main = %q, want force-updated %q", got, localHead)
		}
	})
}

// TestPushUpstreamRequiresBaseRepo: repos without an upstream link get the
// documented 400, symmetric with pull-upstream.
func TestPushUpstreamRequiresBaseRepo(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		s := newUsyncStack(t, e)
		repoURL := "team/plain-" + randomHex(t, 4)
		s.createRepo(t, repoURL, nil)
		token := s.e.mint(t, tokenSpec{iss: s.org, repo: repoURL, scopes: []string{auth.ScopeGitWrite, auth.ScopeGitRead}})
		resp, respBody := s.req(t, http.MethodPost, "/api/v1/repos/push-upstream", token, nil)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status %d: %s", resp.StatusCode, respBody)
		}
		var out map[string]string
		if err := json.Unmarshal(respBody, &out); err != nil || out["error"] != "repository has no upstream base_repo" {
			t.Fatalf("400 body = %s (err %v)", respBody, err)
		}
	})
}

// TestCreateRepoForwardPushesFlag: base_repo.forward_pushes persists on the
// upstream row (default off).
func TestCreateRepoForwardPushesFlag(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		s := newUsyncStack(t, e)
		upRoot := t.TempDir()
		upstream := usyncCGIServer(t, upRoot, func(*http.Request) bool { return true }, false)
		usyncSeedUpstream(t, upRoot, "own", "proj")

		for _, tc := range []struct {
			name    string
			forward bool
		}{
			{name: "team/fwd-" + randomHex(t, 4), forward: true},
			{name: "team/nofwd-" + randomHex(t, 4), forward: false},
		} {
			body, err := json.Marshal(map[string]any{
				"base_repo": map[string]any{
					"provider":       "gitea",
					"owner":          "own",
					"name":           "proj",
					"upstream_host":  upstream.URL,
					"forward_pushes": tc.forward,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			repoID := s.createRepo(t, tc.name, body)
			s.waitEvent(t, upsync.EventSyncStarted)
			s.waitEvent(t, upsync.EventSyncSucceeded)
			up, err := s.mem.GetUpstream(t.Context(), repoID)
			if err != nil {
				t.Fatal(err)
			}
			if up.ForwardOnPush != tc.forward {
				t.Fatalf("ForwardOnPush = %v for %s, want %v", up.ForwardOnPush, tc.name, tc.forward)
			}
		}
	})
}
