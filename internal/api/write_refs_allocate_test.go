package api

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/ref"
)

func TestWriteRefsAllocateBranch(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := wrefSeedFixture(t, e)
		paths := []string{"/api/v1/repos/branches/create", "/api/repos/" + url.PathEscape(fx.url) + "/branches/create"}
		// Branch creation requires write, not read, scope on both routes.
		token := e.mint(t, tokenSpec{iss: fx.org, repo: fx.url, scopes: []string{auth.ScopeGitWrite}})
		seen := map[string]bool{}
		for _, path := range paths {
			for _, baseEphemeral := range []bool{false, true} {
				for _, targetEphemeral := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/base=%v/target=%v", path, baseEphemeral, targetEphemeral), func(t *testing.T) {
						base, wantSHA := "main", fx.c2
						if baseEphemeral {
							base, wantSHA = "scratch", fx.c1
						}
						body := fmt.Appendf(nil, `{"base_ref":%q,"base_is_ephemeral":%t,"target_is_ephemeral":%t}`, base, baseEphemeral, targetEphemeral)
						for range 2 {
							resp, raw := e.doReq(t, http.MethodPost, path, token, body, "")
							if resp.StatusCode != http.StatusOK {
								t.Fatalf("status %d: %s", resp.StatusCode, raw)
							}
							var got wrefCreateBranchResponse
							if err := json.Unmarshal(raw, &got); err != nil {
								t.Fatal(err)
							}
							if !wrefValidBranchInput(got.TargetBranch) || seen[got.TargetBranch] || got.Message != "branch created" || got.CommitSHA != wantSHA || got.TargetIsEphemeral != targetEphemeral {
								t.Fatalf("unexpected allocation: %+v", got)
							}
							seen[got.TargetBranch] = true
							refs := wrefRefs(t, e, fx.repoID)
							if refs[wrefRefName(got.TargetBranch, targetEphemeral)] != wantSHA || refs[wrefRefName(got.TargetBranch, !targetEphemeral)] != "" {
								t.Fatalf("incorrect namespace/commit for %+v: %v", got, refs)
							}
							readPath := fmt.Sprintf("/api/v1/repos/branch?name=%s&ephemeral=%t", url.QueryEscape(got.TargetBranch), targetEphemeral)
							rresp, readBody := e.doReq(t, http.MethodGet, readPath, fx.token, nil, "")
							var read struct {
								Branch struct {
									Name string `json:"name"`
									SHA  string `json:"head_sha"`
								} `json:"branch"`
							}
							if err := json.Unmarshal(readBody, &read); err != nil || rresp.StatusCode != http.StatusOK || read.Branch.Name != got.TargetBranch || read.Branch.SHA != wantSHA {
								t.Fatalf("readback %d: %s (err %v)", rresp.StatusCode, readBody, err)
							}
						}
					})
				}
			}
			for _, body := range []string{
				`{"base_ref":"main","force":true}`,
				`{"base_ref":"main","target_branch":""}`,
				`{"base_ref":"main","target_branch":null}`,
				`{"base_ref":"main","target_branch":42}`,
			} {
				before := wrefRefs(t, e, fx.repoID)
				resp, raw := e.doReq(t, http.MethodPost, path, token, []byte(body), "")
				if resp.StatusCode != http.StatusBadRequest || !maps.Equal(before, wrefRefs(t, e, fx.repoID)) {
					t.Fatalf("%s: status %d: %s or refs changed", body, resp.StatusCode, raw)
				}
			}
		}
	})
}

func TestWriteRefsAllocateAuth(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := wrefSeedFixture(t, e)
		paths := []string{"/api/v1/repos/branches/create", "/api/repos/" + url.PathEscape(fx.url) + "/branches/create"}
		for _, path := range paths {
			for _, tc := range []struct {
				name, refs string
				ephemeral  bool
				want       int
			}{
				{"persistent no-push", `[["refs/heads/*",["no-push"]]]`, false, http.StatusForbidden},
				{"ephemeral no-push", `[["refs/namespaces/ephemeral/refs/heads/*",["no-push"]]]`, true, http.StatusForbidden},
				{"persistent verify-sig", `[["refs/heads/*",["verify-sig"]]]`, false, http.StatusForbidden},
				{"ephemeral verify-sig", `[["refs/namespaces/ephemeral/refs/heads/*",["verify-sig"]]]`, true, http.StatusForbidden},
				{"malformed policy", `[["refs/heads/*"]]`, false, http.StatusForbidden},
				{"creation is fast-forward", `[["refs/heads/*",["no-force-push"]]]`, false, http.StatusOK},
				{"persistent policy does not match ephemeral", `[["refs/heads/*",["no-push"]]]`, true, http.StatusOK},
			} {
				t.Run(path+"/"+tc.name, func(t *testing.T) {
					token := e.mint(t, tokenSpec{iss: fx.org, repo: fx.url, scopes: []string{auth.ScopeGitWrite}, refs: tc.refs})
					before := wrefRefs(t, e, fx.repoID)
					body := fmt.Appendf(nil, `{"base_ref":"main","target_is_ephemeral":%t}`, tc.ephemeral)
					resp, raw := e.doReq(t, http.MethodPost, path, token, body, "")
					if resp.StatusCode != tc.want {
						t.Fatalf("status %d, want %d: %s", resp.StatusCode, tc.want, raw)
					}
					if tc.want != http.StatusOK && !maps.Equal(before, wrefRefs(t, e, fx.repoID)) {
						t.Fatal("denied request changed refs")
					}
				})
			}
			for _, tc := range []struct {
				name, token string
				want        int
			}{
				{"missing token", "", http.StatusUnauthorized},
				{"read only", e.mint(t, tokenSpec{iss: fx.org, repo: fx.url, scopes: []string{auth.ScopeGitRead}}), http.StatusForbidden},
				{"wrong repository", e.mint(t, tokenSpec{iss: fx.org, repo: fx.url + "-other", scopes: []string{auth.ScopeGitWrite}}), http.StatusNotFound},
			} {
				before := wrefRefs(t, e, fx.repoID)
				resp, raw := e.doReq(t, http.MethodPost, path, tc.token, []byte(`{"base_ref":"main"}`), "")
				if resp.StatusCode != tc.want || !maps.Equal(before, wrefRefs(t, e, fx.repoID)) {
					t.Fatalf("%s: status %d, want %d: %s or refs changed", tc.name, resp.StatusCode, tc.want, raw)
				}
			}
		}
	})
}

func TestWriteRefsAllocateConcurrent(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := wrefSeedFixture(t, e)
		// Isolate allocation/CAS concurrency from the pre-existing cold-cache
		// git init/config race, which also affects named-target creation.
		resp, body := e.doReq(t, http.MethodGet, "/api/v1/repos/commit?ref=main", fx.token, nil, "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("materialize base: status %d: %s", resp.StatusCode, body)
		}
		const count = 12
		type result struct {
			resp      *http.Response
			err       error
			ephemeral bool
		}
		results := make(chan result, count)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := range count {
			wg.Go(func() {
				<-start
				path := "/api/v1/repos/branches/create"
				if i%2 != 0 {
					path = "/api/repos/" + url.PathEscape(fx.url) + "/branches/create"
				}
				ephemeral := i%3 == 0
				body := fmt.Sprintf(`{"base_ref":"main","target_is_ephemeral":%t}`, ephemeral)
				req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, e.srv.URL+path, strings.NewReader(body))
				if err != nil {
					results <- result{err: err}
					return
				}
				req.Header.Set("Authorization", "Bearer "+fx.token)
				req.Header.Set("Content-Type", "application/json")
				resp, err := e.srv.Client().Do(req)
				results <- result{resp: resp, err: err, ephemeral: ephemeral}
			})
		}
		close(start)
		wg.Wait()
		close(results)
		seen := map[string]bool{}
		refs := wrefRefs(t, e, fx.repoID)
		for result := range results {
			if result.err != nil {
				t.Errorf("request: %v", result.err)
				continue
			}
			body, err := io.ReadAll(result.resp.Body)
			result.resp.Body.Close()
			if err != nil {
				t.Errorf("read response: %v", err)
				continue
			}
			var got wrefCreateBranchResponse
			if err := json.Unmarshal(body, &got); err != nil || result.resp.StatusCode != http.StatusOK {
				t.Errorf("allocation status %d: %s (err %v)", result.resp.StatusCode, body, err)
				continue
			}
			if !wrefValidBranchInput(got.TargetBranch) || seen[got.TargetBranch] || got.CommitSHA != fx.c2 || got.TargetIsEphemeral != result.ephemeral || refs[wrefRefName(got.TargetBranch, result.ephemeral)] != fx.c2 {
				t.Errorf("incorrect or duplicate allocation: %+v", got)
			}
			seen[got.TargetBranch] = true
		}
		if len(seen) != count || len(refs) != count+4 || refs["refs/heads/feature"] != fx.c1 || refs["refs/heads/main"] != fx.c2 {
			t.Fatalf("got %d allocations, refs %v", len(seen), refs)
		}
	})
}

// Install a competing ref at the exact candidate before delegating the real
// create-only CAS. Collisions are deterministic without replacing the RNG.
type wrefAllocateStore struct {
	ref.Store
	cas func(context.Context, string, string, string, string) (bool, error)
}

func (s wrefAllocateStore) CASRef(ctx context.Context, repo, name, old, next string) (bool, error) {
	return s.cas(ctx, repo, name, old, next)
}

func TestWriteRefsAllocateCollisions(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		for _, ephemeral := range []bool{false, true} {
			for _, tc := range []struct {
				name                    string
				collisions, calls, want int
				fail                    bool
			}{
				{"retry collision", 2, 3, http.StatusOK, false},
				{"exhausted", 8, 8, http.StatusConflict, false},
				{"storage error", 0, 1, http.StatusInternalServerError, true},
			} {
				t.Run(fmt.Sprintf("%s/ephemeral=%t", tc.name, ephemeral), func(t *testing.T) {
					fx := wrefSeedFixture(t, e)
					names := []string{}
					wrapped := *e
					wrapped.refStore = wrefAllocateStore{Store: e.refStore, cas: func(ctx context.Context, repo, name, old, next string) (bool, error) {
						names = append(names, name)
						if repo != fx.repoID || old != ref.ZeroSHA || next != fx.c2 {
							return false, fmt.Errorf("not create-only CAS: repo=%q name=%q old=%q next=%q", repo, name, old, next)
						}
						if tc.fail {
							return false, errors.New("injected ambiguous storage failure")
						}
						if len(names) <= tc.collisions {
							won, err := e.refStore.CASRef(ctx, repo, name, ref.ZeroSHA, fx.c1)
							if err != nil || !won {
								return false, fmt.Errorf("install collision: won=%t err=%v", won, err)
							}
						}
						return e.refStore.CASRef(ctx, repo, name, old, next)
					}}
					srv := newWrefServer(t, &wrapped)
					resp, body := wrefDo(t, srv, http.MethodPost, "/api/v1/repos/branches/create", fx.token, map[string]any{"base_ref": "main", "target_is_ephemeral": ephemeral})
					if resp.StatusCode != tc.want || len(names) != tc.calls {
						t.Fatalf("status %d want %d, calls %d want %d: %v", resp.StatusCode, tc.want, len(names), tc.calls, body)
					}
					refs := wrefRefs(t, e, fx.repoID)
					for _, name := range names[:tc.collisions] {
						if refs[name] != fx.c1 {
							t.Fatalf("collision overwritten: %s = %s", name, refs[name])
						}
					}
					wantRefs := 4 + tc.collisions
					if tc.want == http.StatusOK {
						wantRefs++
						name, ok := body["target_branch"].(string)
						if !ok || !wrefValidBranchInput(name) || wrefRefName(name, ephemeral) != names[len(names)-1] || refs[wrefRefName(name, ephemeral)] != fx.c2 || body["commit_sha"] != fx.c2 || body["target_is_ephemeral"] != ephemeral {
							t.Fatalf("incorrect winning target: %v", body)
						}
					}
					if len(refs) != wantRefs || refs["refs/heads/main"] != fx.c2 || refs["refs/heads/feature"] != fx.c1 {
						t.Fatalf("unexpected ref mutations: %v", refs)
					}
				})
			}
		}
	})
}
