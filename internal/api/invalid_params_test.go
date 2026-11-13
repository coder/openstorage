package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/auth"
)

// ipParam is one expected invalid_params item.
type ipParam struct{ name, reason string }

// ipCase is one request whose 400 must name every listed field, in order.
type ipCase struct {
	name, method, path, token, body string
	// wantError is the whole `error` member: the message of each problem,
	// joined by "; ".
	wantError string
	// want is the invalid_params list; nil means the member is absent.
	want []ipParam
}

// ipRun sends each case through the full server and checks the 400: the
// exact message, and the exact invalid_params list (a spec-shaped body,
// so requireEnvelope also runs the problem-body oracle).
func ipRun(t *testing.T, e *testEnv, cases []ipCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, body := e.doReq(t, tc.method, tc.path, tc.token, []byte(tc.body), "")
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body %s)", resp.StatusCode, body)
			}
			if msg := requireEnvelope(t, resp, body); msg != tc.wantError {
				t.Fatalf("error = %q, want %q", msg, tc.wantError)
			}
			var p problemBody
			if err := json.Unmarshal(body, &p); err != nil {
				t.Fatal(err)
			}
			if p.Detail != tc.wantError {
				t.Fatalf("detail = %q, want %q", p.Detail, tc.wantError)
			}
			var got []ipParam
			for _, ip := range p.InvalidParams {
				got = append(got, ipParam{ip.Name, ip.Reason})
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("invalid_params = %+v, want %+v (body %s)", got, tc.want, body)
			}
		})
	}
}

// ipFixture is a repo with the tokens the route tests of this file need.
type ipFixture struct {
	url, repoID string
	gitTok      string
	repoTok     string
}

func ipNewFixture(t *testing.T, e *testEnv) ipFixture {
	t.Helper()
	org := e.newOrg(t)
	url := "ip-" + randomHex(t, 4)
	created := createRepo(t, e, org, url, []byte(`{"default_branch":"main"}`), http.StatusCreated)
	return ipFixture{
		url:     url,
		repoID:  created.RepoID,
		gitTok:  e.mint(t, tokenSpec{iss: org, repo: url, scopes: []string{auth.ScopeGitRead, auth.ScopeGitWrite}}),
		repoTok: e.mint(t, tokenSpec{iss: org, repo: url, scopes: []string{auth.ScopeRepoWrite}}),
	}
}

func TestBodyErrors(t *testing.T) {
	// The docs and the answer say 100: pin the number, not the constant.
	if maxInvalidParams != 100 {
		t.Fatalf("maxInvalidParams = %d, the docs say 100", maxInvalidParams)
	}
	field := func(name string) error {
		return newFieldError(name, "is required", name+" is required")
	}
	plain := errors.New("squash is incompatible with the ff_only strategy")

	t.Run("none is nil", func(t *testing.T) {
		var b bodyErrors
		b.add(nil)
		if err := b.err(); err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
	})

	t.Run("one problem comes back unchanged", func(t *testing.T) {
		only := field("a")
		var b bodyErrors
		b.add(only)
		b.add(nil)
		if got := b.err(); got != only {
			t.Fatalf("err = %v (%p), want the same error %p", got, got, only)
		}
	})

	t.Run("several fields keep their order and each message", func(t *testing.T) {
		var b bodyErrors
		b.add(field("b"))
		b.add(newFieldErrors("author name and email are required",
			invalidParam{Name: "author.name", Reason: "is required"},
			invalidParam{Name: "author.email", Reason: "is required"}))
		b.add(field("a"))
		err := b.err()
		if want := "b is required; author name and email are required; a is required"; err.Error() != want {
			t.Fatalf("message = %q, want %q", err.Error(), want)
		}
		want := []invalidParam{
			{"b", "is required"}, {"author.name", "is required"}, {"author.email", "is required"}, {"a", "is required"},
		}
		if got := invalidParamsFor(err); !slices.Equal(got, want) {
			t.Fatalf("params = %+v, want %+v", got, want)
		}
	})

	t.Run("a problem without a field adds its message only", func(t *testing.T) {
		var b bodyErrors
		b.add(field("a"))
		b.add(plain)
		err := b.err()
		if want := "a is required; " + plain.Error(); err.Error() != want {
			t.Fatalf("message = %q, want %q", err.Error(), want)
		}
		if got := invalidParamsFor(err); !slices.Equal(got, []invalidParam{{"a", "is required"}}) {
			t.Fatalf("params = %+v", got)
		}
		var onlyPlain bodyErrors
		onlyPlain.add(plain)
		onlyPlain.add(errors.New("second"))
		if err := onlyPlain.err(); invalidParamsFor(err) != nil || err.Error() != plain.Error()+"; second" {
			t.Fatalf("err = %v, params %+v", err, invalidParamsFor(err))
		}
	})

	t.Run("a field named twice counts once", func(t *testing.T) {
		var b bodyErrors
		b.add(field("a"))
		b.add(newFieldError("a", "must be x", "a must be x"))
		b.add(field("c"))
		err := b.err()
		if want := "a is required; c is required"; err.Error() != want {
			t.Fatalf("message = %q, want %q", err.Error(), want)
		}
		if got := invalidParamsFor(err); !slices.Equal(got, []invalidParam{{"a", "is required"}, {"c", "is required"}}) {
			t.Fatalf("params = %+v", got)
		}
	})

	t.Run("the answer is bounded", func(t *testing.T) {
		var b bodyErrors
		for i := range maxInvalidParams + 25 {
			b.add(field(fmt.Sprintf("paths.%d", i)))
		}
		err := b.err()
		got := invalidParamsFor(err)
		if len(got) != maxInvalidParams || got[0].Name != "paths.0" || got[maxInvalidParams-1].Name != fmt.Sprintf("paths.%d", maxInvalidParams-1) {
			t.Fatalf("got %d params, first %v, last %v", len(got), got[0], got[len(got)-1])
		}
		if !strings.HasSuffix(err.Error(), "; and 25 more") || strings.Count(err.Error(), "; ") != maxInvalidParams {
			t.Fatalf("message tail = %q, separators %d", err.Error()[len(err.Error())-30:], strings.Count(err.Error(), "; "))
		}
		if len(b.errs) != maxInvalidParams {
			t.Fatalf("collector kept %d problems, want %d", len(b.errs), maxInvalidParams)
		}
	})

	t.Run("a loop counts the entries it skips once the collector is full", func(t *testing.T) {
		var b bodyErrors
		built := 0
		for i := range maxInvalidParams + 25 {
			if b.full() {
				b.skip()
				continue
			}
			built++
			b.add(field(fmt.Sprintf("paths.%d", i)))
		}
		if built != maxInvalidParams {
			t.Fatalf("the loop built %d problems, want %d", built, maxInvalidParams)
		}
		err := b.err()
		got := invalidParamsFor(err)
		if len(got) != maxInvalidParams || got[maxInvalidParams-1].Name != fmt.Sprintf("paths.%d", maxInvalidParams-1) {
			t.Fatalf("got %d params, last %v", len(got), got[len(got)-1])
		}
		if !strings.HasSuffix(err.Error(), "; and 25 more") {
			t.Fatalf("message tail = %q", err.Error()[len(err.Error())-30:])
		}
		var one bodyErrors
		one.add(field("a"))
		if one.full() {
			t.Fatal("a collector with one problem is full")
		}
	})

	t.Run("a dropped problem counts each field that it names", func(t *testing.T) {
		var b bodyErrors
		for i := range maxInvalidParams {
			b.add(field(fmt.Sprintf("paths.%d", i)))
		}
		b.add(newFieldErrors("context lines must be non-negative",
			invalidParam{Name: "context.before", Reason: "must be at least 0"},
			invalidParam{Name: "context.after", Reason: "must be at least 0"}))
		b.add(fmt.Errorf("a rule across fields"))
		b.add(field("limits.max_lines"))
		err := b.err()
		if got := invalidParamsFor(err); len(got) != maxInvalidParams {
			t.Fatalf("got %d params, want %d", len(got), maxInvalidParams)
		}
		// Two fields, a rule that names none (one), and one field.
		if !strings.HasSuffix(err.Error(), "; and 4 more") {
			t.Fatalf("message tail = %q", err.Error()[len(err.Error())-30:])
		}
	})

	t.Run("a problem with two fields can cross the bound", func(t *testing.T) {
		var b bodyErrors
		for i := range maxInvalidParams - 1 {
			b.add(field(fmt.Sprintf("paths.%d", i)))
		}
		b.add(newFieldErrors("author name and email are required",
			invalidParam{Name: "author.name", Reason: "is required"},
			invalidParam{Name: "author.email", Reason: "is required"}))
		err := b.err()
		got := invalidParamsFor(err)
		if len(got) != maxInvalidParams || got[len(got)-1].Name != "author.name" {
			t.Fatalf("got %d params, last %v", len(got), got[len(got)-1])
		}
		if !strings.HasSuffix(err.Error(), "; and 1 more") {
			t.Fatalf("message tail = %q", err.Error()[len(err.Error())-30:])
		}
	})

	t.Run("one problem that names too many fields is bounded", func(t *testing.T) {
		items := make([]invalidParam, maxInvalidParams+7)
		for i := range items {
			items[i] = invalidParam{Name: fmt.Sprintf("paths.%d", i), Reason: "is required"}
		}
		var b bodyErrors
		b.add(newFieldErrors("paths are required", items...))
		err := b.err()
		if got := invalidParamsFor(err); len(got) != maxInvalidParams {
			t.Fatalf("got %d params, want %d", len(got), maxInvalidParams)
		}
		if want := "paths are required; and 7 more"; err.Error() != want {
			t.Fatalf("message = %q, want %q", err.Error(), want)
		}
		var small bodyErrors
		only := newFieldErrors("paths are required", items[:maxInvalidParams]...)
		small.add(only)
		if got := small.err(); got != only {
			t.Fatalf("a problem within the bound must come back unchanged")
		}
	})

	t.Run("problems without a field are bounded too", func(t *testing.T) {
		var b bodyErrors
		for i := range maxInvalidParams + 50 {
			b.add(fmt.Errorf("rule %d", i))
		}
		err := b.err()
		if invalidParamsFor(err) != nil || !strings.HasSuffix(err.Error(), "; and 50 more") || strings.Count(err.Error(), "; ") != maxInvalidParams {
			t.Fatalf("message = %q", err.Error())
		}
	})
}

// TestMergeNamesEveryInvalidField: POST /merge names each invalid body
// field (upstream: `invalid_params` "names each invalid field"), in the
// order of the docs table, and a body with one problem answers as before.
func TestMergeNamesEveryInvalidField(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := ipNewFixture(t, e)
		const path = "/api/v1/repos/merge"
		ipRun(t, e, []ipCase{
			{
				name: "every field", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"target_branch":"refs/heads/main","strategy":"rebase","expected_target_sha":"x","expected_source_sha":5,` +
					`"squash":true,"author":{"email":"e"},"committer":{"name":"c","email":"e\n"}}`,
				wantError: "missing source_ref; target_branch must not include the refs/ prefix" +
					`; invalid strategy "rebase" (want merge|ff_only|ff_prefer)` +
					"; expected_target_sha must be a 40-hex sha; expected_source_sha must be a 40-hex sha" +
					"; commit_message is required with squash; author name and email are required; committer contains newlines",
				want: []ipParam{
					{"source_ref", "is required"},
					{"target_branch", "must not include the refs/ prefix"},
					{"strategy", "must be merge, ff_only or ff_prefer"},
					{"expected_target_sha", "must be a 40-hex sha"},
					{"expected_source_sha", "must be a 40-hex sha"},
					{"commit_message", "is required with squash"},
					{"author.name", "is required"},
					{"committer", "must not contain newlines"},
				},
			},
			{
				name: "ephemeral HEAD target and a source", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"source_branch":"bad..name","target_branch":"HEAD","target_is_ephemeral":true,"strategy":"merge"}`,
				wantError: "invalid source_branch; invalid target_branch",
				want: []ipParam{
					{"source_branch", "must be a valid branch name"},
					{"target_branch", "must not be HEAD for an ephemeral branch"},
				},
			},
			{
				name: "a rule across fields adds its message only", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"source_ref":"f","target_branch":"refs/heads/main","strategy":"ff_only","squash":true,"commit_message":"m"}`,
				wantError: "target_branch must not include the refs/ prefix; squash is incompatible with the ff_only strategy",
				want:      []ipParam{{"target_branch", "must not include the refs/ prefix"}},
			},
			{
				name: "one problem answers as before", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"source_ref":"f","target_branch":"main","strategy":"rebase"}`,
				wantError: `invalid strategy "rebase" (want merge|ff_only|ff_prefer)`,
				want:      []ipParam{{"strategy", "must be merge, ff_only or ff_prefer"}},
			},
			{
				name: "one rule across fields answers as before", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"source_ref":"f","target_branch":"main","strategy":"ff_only","squash":true,"commit_message":"m"}`,
				wantError: "squash is incompatible with the ff_only strategy",
			},
			{
				name: "an ident with an empty name and a newline", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"source_ref":"f","target_branch":"main","strategy":"merge","author":{"name":"","email":"x\n"},"committer":{"name":"c\r","email":""}}`,
				wantError: "author name and email are required; author contains newlines; committer name and email are required; committer contains newlines",
				want: []ipParam{
					{"author.name", "is required"}, {"author", "must not contain newlines"},
					{"committer.email", "is required"}, {"committer", "must not contain newlines"},
				},
			},
			{
				name: "a raw expected_source_sha is checked with the others", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"target_branch":"main","strategy":"merge","expected_source_sha":5}`,
				wantError: "missing source_ref; expected_source_sha must be a 40-hex sha",
				want:      []ipParam{{"source_ref", "is required"}, {"expected_source_sha", "must be a 40-hex sha"}},
			},
			{
				name: "squash with ff_only also lacks its message", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"source_ref":"f","target_branch":"main","strategy":"ff_only","squash":true}`,
				wantError: "squash is incompatible with the ff_only strategy; commit_message is required with squash",
				want:      []ipParam{{"commit_message", "is required with squash"}},
			},
		})

		// A wrong-type field stops the decode: only that field is named, and
		// the checks of the other fields run once every type is right.
		t.Run("a wrong-type field is named alone", func(t *testing.T) {
			resp, body := e.doReq(t, http.MethodPost, path, fx.gitTok,
				[]byte(`{"target_branch":"refs/heads/main","strategy":5,"author":{"email":"e"}}`), "")
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body %s)", resp.StatusCode, body)
			}
			requireEnvelope(t, resp, body)
			var p problemBody
			if err := json.Unmarshal(body, &p); err != nil {
				t.Fatal(err)
			}
			if len(p.InvalidParams) != 1 || p.InvalidParams[0].Name != "strategy" || p.InvalidParams[0].Reason != "must be a string" {
				t.Fatalf("invalid_params = %+v, want only strategy (body %s)", p.InvalidParams, body)
			}
		})
	})
}
