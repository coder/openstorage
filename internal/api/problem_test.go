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
// Every status the API emits through writeErr is in upstream's generic
// table; a status outside it needs an explicit expectation here.
func assertProblemFields(t *testing.T, status int, body []byte) string {
	t.Helper()
	var p problemBody
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatalf("error body %q is not JSON: %v", body, err)
	}
	wantCode, ok := upstreamGenericCodes(t)[status]
	if !ok {
		t.Fatalf("status %d is not in upstream's generic table; add an explicit expectation (body %s)", status, body)
	}
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
// the org-level create path (409).
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

	for _, tc := range []struct {
		name, method, path, token string
		body                      []byte
		want                      int
	}{
		{"no token", http.MethodGet, "/api/v1/repo", "", nil, http.StatusUnauthorized},
		{"missing scope", http.MethodGet, "/api/v1/repo", writeTok, nil, http.StatusForbidden},
		{"repo not found", http.MethodGet, "/api/v1/repo", missingTok, nil, http.StatusNotFound},
		// `instance` leaves out the query.
		{"bad cursor", http.MethodGet, "/api/v1/repos/branches?cursor=%21%21", readTok, nil, http.StatusBadRequest},
		// `instance` is the alias path the client sent, both for the
		// alias's own repo lookup and after it dispatches to the v1 route.
		{"alias repo not found", http.MethodGet, "/api/repos/" + missing + "/branches", missingTok, nil, http.StatusNotFound},
		{"encoded alias name", http.MethodGet, nestedPath, nestedTok, nil, http.StatusNotFound},
		{"alias bad cursor", http.MethodGet, "/api/repos/" + url + "/branches?cursor=%21%21", readTok, nil, http.StatusBadRequest},
		{"invalid JSON", http.MethodPost, "/api/v1/repos", writeTok, []byte(`{`), http.StatusBadRequest},
		{"repo exists", http.MethodPost, "/api/v1/repos", writeTok, []byte(`{"default_branch":"main"}`), http.StatusConflict},
		{"read ok control", http.MethodGet, "/api/v1/repo", readTok, nil, http.StatusOK},
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
		})
	}
}
