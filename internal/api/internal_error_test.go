package api

// Container-free tests for 500 sanitization: raw store errors (typically
// verbatim DB/driver strings) must be logged server-side and never appear
// in the response body.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/githttp"
	"github.com/coder/openstorage/internal/meta"
)

// failingStore returns err from every method; the leaked-secret marker in
// the tests stands in for driver detail like "pq: SQLSTATE 28P01".
type failingStore struct{ err error }

func (f failingStore) UpsertOrgWithKeys(context.Context, string, []string) (meta.Org, error) {
	return meta.Org{}, f.err
}

func (f failingStore) PublicKeysForOrg(context.Context, string) ([]string, error) {
	return nil, f.err
}

func (f failingStore) OrgNames(context.Context) ([]string, error) {
	return nil, f.err
}

func (f failingStore) CreateRepo(context.Context, string, string, string) (meta.Repo, error) {
	return meta.Repo{}, f.err
}

func (f failingStore) CreateRepoWithState(context.Context, string, string, string, meta.CreateState) (meta.Repo, error) {
	return meta.Repo{}, f.err
}

func (f failingStore) CompleteRepoCreate(context.Context, string, string) (meta.CreateOutcome, error) {
	return 0, f.err
}

func (f failingStore) GetRepoByURL(context.Context, string, string) (meta.Repo, error) {
	return meta.Repo{}, f.err
}

func (f failingStore) GetRepoByID(context.Context, string, string) (meta.Repo, error) {
	return meta.Repo{}, f.err
}

func (f failingStore) UpdateRepoDefaultBranch(context.Context, string, string, string, string) (bool, error) {
	return false, f.err
}

func (f failingStore) SoftDeleteRepo(context.Context, string, string) (meta.Repo, error) {
	return meta.Repo{}, f.err
}

func (f failingStore) ListRepos(context.Context, string, meta.ListParams) (meta.ListResult, error) {
	return meta.ListResult{}, f.err
}

func (f failingStore) AddSigningKey(context.Context, string, string) (meta.SigningKey, error) {
	return meta.SigningKey{}, f.err
}

func (f failingStore) SigningKeysForOrg(context.Context, string) ([]meta.SigningKey, error) {
	return nil, f.err
}

func (f failingStore) RemoveSigningKey(context.Context, string, int64) error { return f.err }

func (f failingStore) CreateWebhook(context.Context, string, string, string) (meta.Webhook, error) {
	return meta.Webhook{}, f.err
}

func (f failingStore) ListWebhooks(context.Context, string) ([]meta.Webhook, error) {
	return nil, f.err
}

func (f failingStore) GetWebhook(context.Context, string) (meta.Webhook, error) {
	return meta.Webhook{}, f.err
}

func (f failingStore) DeleteWebhook(context.Context, string, string) error { return f.err }

func (f failingStore) CreateGitCredential(context.Context, meta.GitCredential) (meta.GitCredential, error) {
	return meta.GitCredential{}, f.err
}

func (f failingStore) GetGitCredential(context.Context, string) (meta.GitCredential, error) {
	return meta.GitCredential{}, f.err
}

func (f failingStore) GitCredentialMeta(context.Context, string) (meta.GitCredential, error) {
	return meta.GitCredential{}, f.err
}

func (f failingStore) UpdateGitCredential(context.Context, string, string, string) (meta.GitCredential, error) {
	return meta.GitCredential{}, f.err
}

func (f failingStore) DeleteGitCredential(context.Context, string) error { return f.err }

func (f failingStore) GitCredentialForRepo(context.Context, string) (meta.GitCredential, error) {
	return meta.GitCredential{}, f.err
}

func (f failingStore) SetRepoUpstream(context.Context, meta.RepoUpstream) (meta.RepoUpstream, error) {
	return meta.RepoUpstream{}, f.err
}

func (f failingStore) CreateLinkedRepo(context.Context, string, string, string, meta.RepoUpstream, *meta.GitCredential) (meta.Repo, error) {
	return meta.Repo{}, f.err
}

func (f failingStore) AdoptUpstreamDefaultBranch(context.Context, string, string) (bool, error) {
	return false, f.err
}

func (f failingStore) GetRepoUpstream(context.Context, string) (meta.RepoUpstream, error) {
	return meta.RepoUpstream{}, f.err
}

func (f failingStore) DeleteRepoUpstream(context.Context, string) error { return f.err }

func (f failingStore) RequestSync(context.Context, string) error { return f.err }

func (f failingStore) TryBeginSync(context.Context, string, string, time.Duration) (meta.RepoUpstream, bool, string, error) {
	return meta.RepoUpstream{}, false, "", f.err
}

func (f failingStore) FinishSync(context.Context, string, string, bool) (meta.RepoUpstream, error) {
	return meta.RepoUpstream{}, f.err
}

func (f failingStore) RequestPush(context.Context, string, bool) error { return f.err }

func (f failingStore) TryBeginPush(context.Context, string, string, time.Duration) (meta.RepoUpstream, bool, bool, string, error) {
	return meta.RepoUpstream{}, false, false, "", f.err
}

func (f failingStore) FinishPush(context.Context, string, string, bool) (meta.RepoUpstream, error) {
	return meta.RepoUpstream{}, f.err
}

func TestInternalErrorsAreSanitized(t *testing.T) {
	const secret = "SQLSTATE 28P01: password authentication failed"

	repoClaims := &auth.Claims{Iss: "org1", Repo: "acme/widgets", Scopes: []string{"repo:write", "git:read"}}
	orgClaims := &auth.Claims{Iss: "org1", Repo: auth.RepoClaimOrg, Scopes: []string{"org:read"}}

	cases := []struct {
		name    string
		method  string
		path    string
		claims  *auth.Claims
		handler func(*server) http.HandlerFunc
	}{
		{"create", "POST", "/api/v1/repos", repoClaims, func(s *server) http.HandlerFunc { return s.handleCreateRepo }},
		{"get", "GET", "/api/v1/repo", repoClaims, func(s *server) http.HandlerFunc { return s.handleGetRepo }},
		{"list", "GET", "/api/v1/repos", orgClaims, func(s *server) http.HandlerFunc { return s.handleListRepos }},
		{"delete", "DELETE", "/api/v1/repos/delete", repoClaims, func(s *server) http.HandlerFunc { return s.handleDeleteRepo }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var logBuf bytes.Buffer
			s := &server{
				store:       failingStore{err: errors.New(secret)},
				httpBaseURL: "https://selfhost.open.storage",
				logger:      slog.New(slog.NewTextHandler(&logBuf, nil)),
			}
			req := httptest.NewRequest(tc.method, tc.path, nil)
			req = req.WithContext(auth.WithClaims(req.Context(), tc.claims))
			rr := httptest.NewRecorder()
			tc.handler(s)(rr, req)

			if rr.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d, want 500 (body %q)", rr.Code, rr.Body.String())
			}
			if msg := decodeEnvelope(t, rr); msg != internalErrorMsg {
				t.Fatalf("error = %q, want %q", msg, internalErrorMsg)
			}
			if strings.Contains(rr.Body.String(), "SQLSTATE") {
				t.Fatalf("raw store error leaked into response body: %s", rr.Body.String())
			}
			if !strings.Contains(logBuf.String(), secret) {
				t.Fatalf("raw store error missing from server log: %s", logBuf.String())
			}
		})
	}
}

// TestRefHeldIsServiceUnavailable: a ref write that gave up waiting for a
// concurrent push's or ref write's hold on its ref (githttp.ErrRefHeld) is
// transient. Every 500 writer of the write routes answers it with 503,
// Retry-After and a body that says the ref is busy, and logs no internal
// error. Other errors stay a plain 500.
func TestRefHeldIsServiceUnavailable(t *testing.T) {
	held := fmt.Errorf("cas refs/heads/main: refusing the ref update: hold refs/heads/main: %w", githttp.ErrRefHeld)
	for _, writer := range []string{"internalErr", "wcInternal", "wmInternal"} {
		for _, err := range []error{held, errors.New("disk on fire")} {
			var logBuf bytes.Buffer
			s := &server{logger: slog.New(slog.NewTextHandler(&logBuf, nil))}
			rr := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodPost, "/api/v1/repos/branches/create", nil)
			switch writer {
			case "internalErr":
				s.internalErr(rr, r, "create branch ref", err)
			case "wcInternal":
				s.wcInternal(rr, r, "publish commit", err)
			case "wmInternal":
				s.wmInternal(rr, r, "publish merge", err)
			}
			msg := decodeEnvelope(t, rr)
			if err == held {
				if rr.Code != http.StatusServiceUnavailable || rr.Header().Get("Retry-After") != "1" || msg != refBusyMsg {
					t.Errorf("%s(held ref) = %d, Retry-After %q, %q; want 503, 1, %q", writer, rr.Code, rr.Header().Get("Retry-After"), msg, refBusyMsg)
				}
				if strings.Contains(logBuf.String(), "internal error") {
					t.Errorf("%s(held ref) logged an internal error: %s", writer, logBuf.String())
				}
				continue
			}
			if rr.Code != http.StatusInternalServerError || rr.Header().Get("Retry-After") != "" || msg != internalErrorMsg {
				t.Errorf("%s(other error) = %d, Retry-After %q, %q; want a plain 500", writer, rr.Code, rr.Header().Get("Retry-After"), msg)
			}
		}
	}
}
