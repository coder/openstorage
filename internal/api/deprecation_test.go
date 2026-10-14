package api

// Deprecation header on the deprecated pull-upstream and base routes (issue
// #456): every response of the four routes carries it, error responses
// included. The path-style replacements share the v1 handlers but carry no
// header, and neither does push-upstream.

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/events"
	"github.com/coder/openstorage/internal/meta"
	upsync "github.com/coder/openstorage/internal/sync"
)

func TestDeprecatedSyncRoutesHeader(t *testing.T) {
	// RFC 9745: an sf-date, "@" + Unix seconds of upstream's changelog date.
	wantValue := "@" + strconv.FormatInt(time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC).Unix(), 10)
	if deprecationHeader != wantValue {
		t.Fatalf("deprecationHeader = %q, want %q", deprecationHeader, wantValue)
	}

	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		unlinkedURL := "team/dep-" + randomHex(t, 4)
		linkedURL := "team/dep-linked-" + randomHex(t, 4)
		if _, err := e.store.CreateRepo(t.Context(), org, unlinkedURL, "main"); err != nil {
			t.Fatal(err)
		}
		linked, err := e.store.CreateRepo(t.Context(), org, linkedURL, "main")
		if err != nil {
			t.Fatal(err)
		}
		mem := upsync.NewMemStore()
		if _, err := mem.PutUpstream(t.Context(), upsync.Upstream{RepoID: linked.RepoID,
			Provider: "github", Owner: "own", Name: "proj"}); err != nil {
			t.Fatal(err)
		}
		verifier := auth.NewVerifier(e.store)
		// newAPI registers the real sync and alias routes. store backs the
		// handlers and the alias lookup; auth always uses e.store.
		newAPI := func(store meta.Store, rl *rateLimiter) *httptest.Server {
			ss := newSyncServer(store, mem, mem, envSyncQueue{}, testHTTPBaseURL, events.TargetGuard{}, discardLogger())
			mux := http.NewServeMux()
			registerSyncRoutes(mux, verifier, org, ss, rl)
			registerAliasRoutes(mux, &server{store: store, logger: discardLogger()}, ss, verifier, org, rl)
			srv := httptest.NewServer(withProblemInstance(recoverPanics(discardLogger())(mux)))
			t.Cleanup(srv.Close)
			return srv
		}
		tok := func(repoURL string, scopes ...string) string {
			return e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: scopes})
		}
		pullTok := tok(unlinkedURL, auth.ScopeGitWrite)
		linkedPullTok := tok(linkedURL, auth.ScopeGitWrite)
		repoTok := tok(unlinkedURL, auth.ScopeRepoWrite)
		readTok := tok(unlinkedURL, auth.ScopeGitRead)
		alias := func(repoURL, suffix string) string {
			return "/api/repos/" + url.PathEscape(repoURL) + suffix
		}

		type call struct {
			name       string
			method     string
			path       string
			token      string
			wantStatus int
			deprecated bool
		}
		check := func(t *testing.T, srv *httptest.Server, c call) {
			t.Helper()
			req, err := http.NewRequest(c.method, srv.URL+c.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			if c.token != "" {
				req.Header.Set("Authorization", "Bearer "+c.token)
			}
			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != c.wantStatus {
				t.Errorf("%s: status %d, want %d", c.name, resp.StatusCode, c.wantStatus)
			}
			got := resp.Header.Values("Deprecation")
			switch {
			case c.deprecated && (len(got) != 1 || got[0] != wantValue):
				t.Errorf("%s: Deprecation = %q, want [%q]", c.name, got, wantValue)
			case !c.deprecated && len(got) != 0:
				t.Errorf("%s: Deprecation = %q, want none", c.name, got)
			}
		}

		t.Run("success and client errors", func(t *testing.T) {
			srv := newAPI(e.store, nil)
			for _, c := range []call{
				{"v1 pull 202", http.MethodPost, "/api/v1/repos/pull-upstream", linkedPullTok, http.StatusAccepted, true},
				{"alias pull 202", http.MethodPost, alias(linkedURL, "/pull-upstream"), linkedPullTok, http.StatusAccepted, true},
				{"v1 pull no upstream 400", http.MethodPost, "/api/v1/repos/pull-upstream", pullTok, http.StatusBadRequest, true},
				{"alias pull no upstream 400", http.MethodPost, alias(unlinkedURL, "/pull-upstream"), pullTok, http.StatusBadRequest, true},
				{"v1 pull no token 401", http.MethodPost, "/api/v1/repos/pull-upstream", "", http.StatusUnauthorized, true},
				{"alias pull no token 401", http.MethodPost, alias(unlinkedURL, "/pull-upstream"), "", http.StatusUnauthorized, true},
				{"v1 pull git:read 403", http.MethodPost, "/api/v1/repos/pull-upstream", readTok, http.StatusForbidden, true},
				{"alias pull unknown repo 404", http.MethodPost, alias("team/missing", "/pull-upstream"), pullTok, http.StatusNotFound, true},
				{"v1 base 200", http.MethodDelete, "/api/v1/repos/base", repoTok, http.StatusOK, true},
				{"alias base 200", http.MethodDelete, alias(unlinkedURL, "/base"), repoTok, http.StatusOK, true},
				{"v1 base no token 401", http.MethodDelete, "/api/v1/repos/base", "", http.StatusUnauthorized, true},
				{"alias base git:write 403", http.MethodDelete, alias(unlinkedURL, "/base"), pullTok, http.StatusForbidden, true},
				{"alias base unknown repo 404", http.MethodDelete, alias("team/missing", "/base"), repoTok, http.StatusNotFound, true},
				// The replacements and push-upstream run the same chains
				// without the header.
				{"upstream/pull 202", http.MethodPost, alias(linkedURL, "/upstream/pull"), linkedPullTok, http.StatusAccepted, false},
				{"upstream/pull 400", http.MethodPost, alias(unlinkedURL, "/upstream/pull"), pullTok, http.StatusBadRequest, false},
				{"upstream/pull 401", http.MethodPost, alias(unlinkedURL, "/upstream/pull"), "", http.StatusUnauthorized, false},
				{"DELETE upstream 200", http.MethodDelete, alias(unlinkedURL, "/upstream"), repoTok, http.StatusOK, false},
				{"DELETE upstream 404", http.MethodDelete, alias("team/missing", "/upstream"), repoTok, http.StatusNotFound, false},
				{"v1 push-upstream 401", http.MethodPost, "/api/v1/repos/push-upstream", "", http.StatusUnauthorized, false},
				{"alias push-upstream 401", http.MethodPost, alias(unlinkedURL, "/push-upstream"), "", http.StatusUnauthorized, false},
			} {
				check(t, srv, c)
			}
		})

		t.Run("500 from the handler and the alias lookup", func(t *testing.T) {
			srv := newAPI(failingStore{err: errors.New("store down")}, nil)
			for _, c := range []call{
				{"v1 pull 500", http.MethodPost, "/api/v1/repos/pull-upstream", pullTok, http.StatusInternalServerError, true},
				{"alias pull 500", http.MethodPost, alias(unlinkedURL, "/pull-upstream"), pullTok, http.StatusInternalServerError, true},
				{"v1 base 500", http.MethodDelete, "/api/v1/repos/base", repoTok, http.StatusInternalServerError, true},
				{"alias base 500", http.MethodDelete, alias(unlinkedURL, "/base"), repoTok, http.StatusInternalServerError, true},
				{"upstream/pull 500", http.MethodPost, alias(unlinkedURL, "/upstream/pull"), pullTok, http.StatusInternalServerError, false},
				{"DELETE upstream 500", http.MethodDelete, alias(unlinkedURL, "/upstream"), repoTok, http.StatusInternalServerError, false},
			} {
				check(t, srv, c)
			}
		})

		t.Run("429 from the limiter", func(t *testing.T) {
			rl, _ := newTestLimiter(RateLimitConfig{RPS: 0.001, Burst: 1}, time.Unix(1000, 0))
			srv := newAPI(e.store, rl)
			// The first request spends the only token of this client.
			check(t, srv, call{"spend token", http.MethodDelete, "/api/v1/repos/base", repoTok, http.StatusOK, true})
			for _, c := range []call{
				{"v1 pull 429", http.MethodPost, "/api/v1/repos/pull-upstream", pullTok, http.StatusTooManyRequests, true},
				{"alias pull 429", http.MethodPost, alias(unlinkedURL, "/pull-upstream"), pullTok, http.StatusTooManyRequests, true},
				{"v1 base 429", http.MethodDelete, "/api/v1/repos/base", repoTok, http.StatusTooManyRequests, true},
				{"alias base 429", http.MethodDelete, alias(unlinkedURL, "/base"), repoTok, http.StatusTooManyRequests, true},
				{"upstream/pull 429", http.MethodPost, alias(unlinkedURL, "/upstream/pull"), pullTok, http.StatusTooManyRequests, false},
				{"DELETE upstream 429", http.MethodDelete, alias(unlinkedURL, "/upstream"), repoTok, http.StatusTooManyRequests, false},
			} {
				check(t, srv, c)
			}
		})
	})
}
