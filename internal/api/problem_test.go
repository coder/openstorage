package api

// Tests for the RFC 9457 problem-details fields on the plain {"error"}
// envelope (issue #349). The expected generic codes come from the vendored
// upstream spec, not from the implementation, so a snapshot refresh that
// changes upstream's table fails here until the map is updated.

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/coder/openstorage/internal/auth"
)

// upstreamSpecPath is the vendored upstream OpenAPI snapshot, relative to
// this package directory.
const upstreamSpecPath = "../../docs/upstream/openapi.yaml"

var (
	upstreamCodesOnce sync.Once
	upstreamCodes     map[int]string
	upstreamCodesErr  string
)

// upstreamGenericCodes parses the "Generic codes:" line of the vendored
// spec's "Error codes" section into status -> code.
func upstreamGenericCodes(t *testing.T) map[int]string {
	t.Helper()
	upstreamCodesOnce.Do(func() {
		raw, err := os.ReadFile(upstreamSpecPath)
		if err != nil {
			upstreamCodesErr = err.Error()
			return
		}
		var line string
		for l := range strings.SplitSeq(string(raw), "\n") {
			if trimmed := strings.TrimSpace(l); strings.HasPrefix(trimmed, "Generic codes:") {
				if line != "" {
					upstreamCodesErr = "spec has more than one \"Generic codes:\" line"
					return
				}
				line = trimmed
			}
		}
		if line == "" {
			upstreamCodesErr = "spec has no \"Generic codes:\" line"
			return
		}
		pairs := regexp.MustCompile("`([a-z0-9_]+)` \\(([0-9]{3})\\)").FindAllStringSubmatch(line, -1)
		codes := make(map[int]string, len(pairs))
		for _, p := range pairs {
			status, _ := strconv.Atoi(p[2])
			if _, dup := codes[status]; dup {
				upstreamCodesErr = "spec lists status " + p[2] + " twice"
				return
			}
			codes[status] = p[1]
		}
		upstreamCodes = codes
	})
	if upstreamCodesErr != "" {
		t.Fatalf("parse %s: %s", upstreamSpecPath, upstreamCodesErr)
	}
	return upstreamCodes
}

// TestGenericProblemCodeMatchesUpstream pins the generic per-status code
// table to the vendored spec, in both directions.
func TestGenericProblemCodeMatchesUpstream(t *testing.T) {
	spec := upstreamGenericCodes(t)
	// Guard the parser itself: upstream's 2026-09-24 table has 17 entries.
	if len(spec) != 17 {
		t.Fatalf("parsed %d generic codes from the spec, want 17: %v", len(spec), spec)
	}
	for status, want := range spec {
		if got := genericProblemCode(status); got != want {
			t.Errorf("genericProblemCode(%d) = %q, want %q (upstream table)", status, got, want)
		}
	}
	for status, code := range genericProblemCodes {
		if spec[status] != code {
			t.Errorf("genericProblemCodes[%d] = %q is not in the upstream table (upstream: %q)", status, code, spec[status])
		}
	}
}

// TestGenericProblemCodeFallback covers "Any other status uses its status
// text in snake_case" for statuses outside upstream's table.
func TestGenericProblemCodeFallback(t *testing.T) {
	for status, want := range map[int]string{
		http.StatusGone:                    "gone",
		http.StatusLengthRequired:          "length_required",
		http.StatusUnsupportedMediaType:    "unsupported_media_type",
		http.StatusHTTPVersionNotSupported: "http_version_not_supported",
		http.StatusInsufficientStorage:     "insufficient_storage",
	} {
		if got := genericProblemCode(status); got != want {
			t.Errorf("genericProblemCode(%d) = %q, want %q", status, got, want)
		}
	}
	snake := regexp.MustCompile(`^[a-z0-9]+(_[a-z0-9]+)*$`)
	for status := 400; status < 600; status++ {
		if http.StatusText(status) == "" {
			continue
		}
		if got := genericProblemCode(status); !snake.MatchString(got) {
			t.Errorf("genericProblemCode(%d) = %q, want snake_case", status, got)
		}
	}
}

var (
	upstreamSpecificOnce sync.Once
	upstreamSpecific     map[string]int
	upstreamSpecificErr  string
)

// upstreamSpecificCodes parses the "Specific codes:" list of the vendored
// spec's "Error codes" section into code -> status. An item with several
// statuses ("412, or the 4xx status from the upstream") maps to the first.
func upstreamSpecificCodes(t *testing.T) map[string]int {
	t.Helper()
	upstreamSpecificOnce.Do(func() {
		raw, err := os.ReadFile(upstreamSpecPath)
		if err != nil {
			upstreamSpecificErr = err.Error()
			return
		}
		item := regexp.MustCompile("^- `([a-z0-9_]+)` \\(([0-9]{3})[,)]")
		codes := map[string]int{}
		in := false
		for l := range strings.SplitSeq(string(raw), "\n") {
			l = strings.TrimSpace(l)
			switch {
			case l == "Specific codes:":
				if in || len(codes) > 0 {
					upstreamSpecificErr = "spec has more than one \"Specific codes:\" line"
					return
				}
				in = true
			case !in || l == "":
			case strings.HasPrefix(l, "- "):
				m := item.FindStringSubmatch(l)
				if m == nil {
					upstreamSpecificErr = "unparsed specific-code item: " + l
					return
				}
				if _, dup := codes[m[1]]; dup {
					upstreamSpecificErr = "spec lists code " + m[1] + " twice"
					return
				}
				codes[m[1]], _ = strconv.Atoi(m[2])
			default:
				in = false
			}
		}
		upstreamSpecific = codes
	})
	if upstreamSpecificErr != "" {
		t.Fatalf("parse %s: %s", upstreamSpecPath, upstreamSpecificErr)
	}
	return upstreamSpecific
}

// TestSpecificProblemCodesMatchUpstream pins every specific code
// open.storage emits to upstream's list, with the same status.
func TestSpecificProblemCodesMatchUpstream(t *testing.T) {
	spec := upstreamSpecificCodes(t)
	// Guard the parser itself: upstream's 2026-10-08 list has 22 items.
	if len(spec) != 22 {
		t.Fatalf("parsed %d specific codes from the spec, want 22: %v", len(spec), spec)
	}
	for code, status := range specificProblemCodes {
		if want, ok := spec[code]; !ok || want != status {
			t.Errorf("specificProblemCodes[%q] = %d, upstream lists it as %d (present %v)", code, status, want, ok)
		}
		if genericProblemCode(status) == code {
			t.Errorf("specific code %q equals the generic code for %d", code, status)
		}
	}
}

// TestWriteProblemCodeAssertions: writeProblem refuses a specific code on
// a status upstream does not list for it, and an unknown code.
func TestWriteProblemCodeAssertions(t *testing.T) {
	for _, tc := range []struct {
		status int
		code   string
	}{
		{http.StatusForbidden, codeRepositoryNotFound},
		{http.StatusNotFound, "not_a_code"},
		{http.StatusNotFound, "conflict"},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("writeProblem(%d, %q) did not panic", tc.status, tc.code)
				}
			}()
			writeProblem(httptest.NewRecorder(), tc.status, tc.code, "boom")
		}()
	}
	rr := httptest.NewRecorder()
	writeProblem(rr, http.StatusNotFound, codeRepositoryNotFound, repoNotFoundMsg)
	if msg := assertProblemFields(t, rr.Code, rr.Body.Bytes()); msg != "repository not found" {
		t.Fatalf("error = %q", msg)
	}
}

// expectedProblemCode is the test oracle for `code`: upstream's specific
// code for the failures open.storage maps (issue #352), keyed on status
// and message independently of the implementation, else the generic code.
func expectedProblemCode(t *testing.T, status int, msg string) string {
	t.Helper()
	code := ""
	switch {
	case status == http.StatusNotFound && (msg == "repository not found" || msg == "fork source repository not found"):
		code = "repository_not_found"
	case status == http.StatusForbidden && (strings.HasPrefix(msg, "missing required scope ") ||
		strings.HasPrefix(msg, "base_repo.auth.token is missing required scope ")):
		code = "insufficient_scope"
	case status == http.StatusBadRequest && msg == "repo_name must equal the JWT repo claim":
		code = "repo_name_mismatch"
	case status == http.StatusNotFound && (msg == "branch not found" ||
		(strings.HasPrefix(msg, "source branch ") || strings.HasPrefix(msg, "target branch ")) && strings.HasSuffix(msg, " not found")):
		code = "branch_not_found"
	case status == http.StatusNotFound && slices.Contains([]string{"ref not found", "tag not found", "commit not found",
		"base commit not found", "base not found", "base ref not found", "ref does not resolve to a commit"}, msg):
		code = "ref_not_found"
	case status == http.StatusNotFound && msg == "credential not found":
		code = "credential_not_found"
	case status == http.StatusConflict && msg == "credential already exists":
		code = "credential_exists"
	case status == http.StatusConflict && msg == "repository already deleted":
		code = "repository_deleted"
	}
	if code != "" {
		if got := upstreamSpecificCodes(t)[code]; got != status {
			t.Fatalf("upstream lists %q for status %d, not %d", code, got, status)
		}
		return code
	}
	generic, ok := upstreamGenericCodes(t)[status]
	if !ok {
		t.Fatalf("status %d is not in upstream's generic table; add an explicit expectation", status)
	}
	return generic
}

// requireProblemCode asserts a plain problem body (requireEnvelope) with
// exactly the given `code`.
func requireProblemCode(t *testing.T, resp *http.Response, body []byte, want string) {
	t.Helper()
	requireEnvelope(t, resp, body)
	var p problemBody
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatal(err)
	}
	if p.Code != want {
		t.Fatalf("code = %q, want %q (body %s)", p.Code, want, body)
	}
}

// problemBody is the wire shape the tests expect. It is declared here, not
// shared with the implementation, so the tests pin the JSON names.
type problemBody struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Code     string `json:"code"`
	Detail   string `json:"detail"`
	Error    string `json:"error"`
	Instance string `json:"instance"`
}

// assertProblemFields checks the RFC 9457 fields of a plain-envelope error
// body against its HTTP status and returns the legacy `error` message.
// `code` must be the one expectedProblemCode derives from the status and
// message.
func assertProblemFields(t *testing.T, status int, body []byte) string {
	t.Helper()
	var p problemBody
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatalf("error body %q is not JSON: %v", body, err)
	}
	wantCode := expectedProblemCode(t, status, p.Error)
	if p.Type != "about:blank" || p.Title != http.StatusText(status) || p.Status != status || p.Code != wantCode {
		t.Fatalf("problem fields = {type %q, title %q, status %d, code %q}, want {about:blank, %q, %d, %q} (body %s)",
			p.Type, p.Title, p.Status, p.Code, http.StatusText(status), status, wantCode, body)
	}
	if p.Error == "" || p.Detail != p.Error {
		t.Fatalf("detail = %q, error = %q: want equal and non-empty (body %s)", p.Detail, p.Error, body)
	}
	return p.Error
}

// TestWriteErrProblemFields: writeErr adds the problem fields for every
// status it is called with, and keeps `error` and the JSON content type.
func TestWriteErrProblemFields(t *testing.T) {
	for _, status := range []int{400, 401, 403, 404, 409, 422, 429, 500, 503} {
		rr := httptest.NewRecorder()
		writeErr(rr, status, "boom "+strconv.Itoa(status))
		if rr.Code != status {
			t.Fatalf("status = %d, want %d", rr.Code, status)
		}
		if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
			t.Fatalf("Content-Type = %q, want application/json", ct)
		}
		if msg := assertProblemFields(t, status, rr.Body.Bytes()); msg != "boom "+strconv.Itoa(status) {
			t.Fatalf("error = %q", msg)
		}
	}
}

// TestProblemInstanceThroughWrappers: writeErr finds `instance` under the
// package's writer wrappers (statusRecorder here) through Unwrap, including
// the recovered-panic 500, and omits it without withProblemInstance.
func TestProblemInstanceThroughWrappers(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	for _, tc := range []struct {
		name string
		h    http.HandlerFunc
		want int
	}{
		{"writeErr", func(w http.ResponseWriter, _ *http.Request) { writeErr(w, http.StatusNotFound, "nope") }, http.StatusNotFound},
		{"panic", func(http.ResponseWriter, *http.Request) { panic("boom") }, http.StatusInternalServerError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := withProblemInstance(recoverPanics(logger)(logRequests(logger)(tc.h)))
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/repos/team%2Fapp/branches?cursor=x", nil))
			if rr.Code != tc.want {
				t.Fatalf("status = %d, want %d", rr.Code, tc.want)
			}
			assertProblemFields(t, rr.Code, rr.Body.Bytes())
			var p problemBody
			if err := json.Unmarshal(rr.Body.Bytes(), &p); err != nil {
				t.Fatal(err)
			}
			if p.Instance != "/api/repos/team%2Fapp/branches" {
				t.Fatalf("instance = %q, want the escaped path without the query", p.Instance)
			}
		})
	}
	rr := httptest.NewRecorder()
	writeErr(rr, http.StatusNotFound, "nope")
	if strings.Contains(rr.Body.String(), `"instance"`) {
		t.Fatalf("body without withProblemInstance = %s, want no instance member", rr.Body)
	}
}

// TestErrorBodiesCarryProblemFields exercises representative routes and
// failure classes through the full server on both backends: the auth
// middleware (401, 403), repo resolution (404), body validation (400), and
// the org-level create path (409), with the exact `code` of each.
func TestErrorBodiesCarryProblemFields(t *testing.T) {
	runBackends(t, testErrorBodiesCarryProblemFields)
}

func testErrorBodiesCarryProblemFields(t *testing.T, e *testEnv) {
	org := e.newOrg(t)
	url := "pd-" + randomHex(t, 4)
	createRepo(t, e, org, url, []byte(`{"default_branch":"main"}`), http.StatusCreated)
	writeTok := e.mint(t, tokenSpec{iss: org, repo: url, scopes: []string{auth.ScopeRepoWrite}})
	readTok := e.mint(t, tokenSpec{iss: org, repo: url, scopes: []string{auth.ScopeGitRead}})
	missing := "pd-missing-" + randomHex(t, 4)
	missingTok := e.mint(t, tokenSpec{iss: org, repo: missing, scopes: []string{auth.ScopeGitRead}})
	nested := "pd/missing-" + randomHex(t, 4)
	nestedTok := e.mint(t, tokenSpec{iss: org, repo: nested, scopes: []string{auth.ScopeGitRead}})
	nestedPath := "/api/repos/" + strings.ReplaceAll(nested, "/", "%2F") + "/branches"
	orgTok := e.mint(t, tokenSpec{iss: org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeGitRead}})

	for _, tc := range []struct {
		name, method, path, token string
		body                      []byte
		want                      int
		code                      string
	}{
		{"no token", http.MethodGet, "/api/v1/repo", "", nil, http.StatusUnauthorized, "unauthorized"},
		{"missing scope", http.MethodGet, "/api/v1/repo", writeTok, nil, http.StatusForbidden, "insufficient_scope"},
		{"repo not found", http.MethodGet, "/api/v1/repo", missingTok, nil, http.StatusNotFound, "repository_not_found"},
		// `instance` leaves out the query.
		{"bad cursor", http.MethodGet, "/api/v1/repos/branches?cursor=%21%21", readTok, nil, http.StatusBadRequest, "bad_request"},
		// `instance` is the alias path the client sent, both for the
		// alias's own repo lookup and after it dispatches to the v1 route.
		{"alias repo not found", http.MethodGet, "/api/repos/" + missing + "/branches", missingTok, nil, http.StatusNotFound, "repository_not_found"},
		{"encoded alias name", http.MethodGet, nestedPath, nestedTok, nil, http.StatusNotFound, "repository_not_found"},
		{"alias bad cursor", http.MethodGet, "/api/repos/" + url + "/branches?cursor=%21%21", readTok, nil, http.StatusBadRequest, "bad_request"},
		{"invalid JSON", http.MethodPost, "/api/v1/repos", writeTok, []byte(`{`), http.StatusBadRequest, "bad_request"},
		{"repo exists", http.MethodPost, "/api/v1/repos", writeTok, []byte(`{"default_branch":"main"}`), http.StatusConflict, "conflict"},
		// The alias's own lookup of the token's repo claim.
		{"alias claim not found", http.MethodGet, "/api/repos/" + url + "/branches", missingTok, nil, http.StatusNotFound, "repository_not_found"},
		{"repo name mismatch", http.MethodPost, "/api/v1/repos", writeTok, []byte(`{"repo_name":"pd-other"}`), http.StatusBadRequest, "repo_name_mismatch"},
		// Other 403s keep the generic code.
		{"org token on repo route", http.MethodGet, "/api/v1/repo", orgTok, nil, http.StatusForbidden, "forbidden"},
		{"branch not found", http.MethodGet, "/api/v1/repos/branch?name=nope", readTok, nil, http.StatusNotFound, "branch_not_found"},
		{"alias branch not found", http.MethodGet, "/api/repos/" + url + "/branch?name=nope", readTok, nil, http.StatusNotFound, "branch_not_found"},
		{"ref not found", http.MethodGet, "/api/v1/repos/commit?ref=nope", readTok, nil, http.StatusNotFound, "ref_not_found"},
		{"tag not found", http.MethodGet, "/api/v1/repos/tag?name=nope", readTok, nil, http.StatusNotFound, "ref_not_found"},
		// "path not found" and similar keep the generic code.
		{"read ok control", http.MethodGet, "/api/v1/repo", readTok, nil, http.StatusOK, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, body := e.doReq(t, tc.method, tc.path, tc.token, tc.body, "")
			if resp.StatusCode != tc.want {
				t.Fatalf("%s %s status = %d, want %d (body %s)", tc.method, tc.path, resp.StatusCode, tc.want, body)
			}
			if tc.want == http.StatusOK {
				return
			}
			requireEnvelope(t, resp, body)
			var p problemBody
			if err := json.Unmarshal(body, &p); err != nil {
				t.Fatal(err)
			}
			if want, _, _ := strings.Cut(tc.path, "?"); p.Instance != want {
				t.Fatalf("instance = %q, want %q", p.Instance, want)
			}
			if p.Code != tc.code {
				t.Fatalf("code = %q, want %q (body %s)", p.Code, tc.code, body)
			}
		})
	}
}
