package api

// Tests for the RFC 9457 problem-details fields on the plain {"error"}
// envelope (issue #349). The expected generic codes come from the vendored
// upstream spec, not from the implementation, so a snapshot refresh that
// changes upstream's table fails here until the map is updated.

import (
	"encoding/json"
	"fmt"
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
	"time"

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
	// Guard the parser itself: upstream's 2026-10-27 list has 23 items.
	if len(spec) != 23 {
		t.Fatalf("parsed %d specific codes from the spec, want 23: %v", len(spec), spec)
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

// TestWriteRefGuardFailed: ref_guard_failed always carries a known guard
// and a full current_sha, and writeProblem cannot send it without them.
func TestWriteRefGuardFailed(t *testing.T) {
	zero := strings.Repeat("0", 40)
	for name, write := range map[string]func(){
		"writeProblem":  func() { writeProblem(httptest.NewRecorder(), http.StatusConflict, "ref_guard_failed", "boom") },
		"unknown guard": func() { writeRefGuardFailed(httptest.NewRecorder(), "expected_ref_sha", zero, "boom") },
		"short sha":     func() { writeRefGuardFailed(httptest.NewRecorder(), "expected_target_sha", "abc", "boom") },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s did not panic", name)
				}
			}()
			write()
		}()
	}
	rr := httptest.NewRecorder()
	msg := "source branch moved: the source branch does not contain commit " + zero
	writeRefGuardFailed(rr, "expected_source_sha", zero, msg)
	if got := assertProblemFields(t, rr.Code, rr.Body.Bytes()); got != msg {
		t.Fatalf("error = %q", got)
	}
	if body := rr.Body.String(); !strings.Contains(body, `"guard":"expected_source_sha","current_sha":"`+zero+`"`) {
		t.Fatalf("body = %s", body)
	}
}

// expectedProblemCodes is the test oracle for `code`: upstream's specific
// code for the failures open.storage maps (issue #352), keyed on status
// and message independently of the implementation, else the generic code.
// "branch not found" has two answers: list commits says it for a missing
// `ref` (ref_not_found) and for a missing branch (branch_not_found);
// TestErrorBodiesCarryProblemFields pins each.
func expectedProblemCodes(t *testing.T, status int, msg string) []string {
	t.Helper()
	if status == http.StatusNotFound && msg == "branch not found" {
		return []string{"branch_not_found", "ref_not_found"}
	}
	code := ""
	switch {
	case status == http.StatusNotFound && (msg == "repository not found" || msg == "fork source repository not found"):
		code = "repository_not_found"
	case status == http.StatusForbidden && (strings.HasPrefix(msg, "missing required scope ") ||
		strings.HasPrefix(msg, "base_repo.auth.token is missing required scope ")):
		code = "insufficient_scope"
	case status == http.StatusBadRequest && msg == "repo_name must equal the JWT repo claim":
		code = "repo_name_mismatch"
	case status == http.StatusNotFound &&
		(strings.HasPrefix(msg, "source branch ") || strings.HasPrefix(msg, "target branch ")) && strings.HasSuffix(msg, " not found"):
		code = "branch_not_found"
	case status == http.StatusNotFound && (slices.Contains([]string{"ref not found", "tag not found", "commit not found",
		"base commit not found", "base not found", "base ref not found", "ref does not resolve to a commit"}, msg) ||
		strings.HasPrefix(msg, "object_ref does not resolve to a commit: ")):
		code = "ref_not_found"
	case status == http.StatusNotFound && msg == "credential not found":
		code = "credential_not_found"
	case status == http.StatusConflict && msg == "credential already exists":
		code = "credential_exists"
	case status == http.StatusConflict && msg == "repository already deleted":
		code = "repository_deleted"
	case status == http.StatusConflict && (strings.HasPrefix(msg, "source branch moved: ") ||
		strings.HasPrefix(msg, "expected_target_sha ") && strings.Contains(msg, " does not match current tip ") ||
		strings.HasPrefix(msg, "expected_notes_ref_sha ") && strings.Contains(msg, " does not match ")):
		code = "ref_guard_failed"
	}
	if code != "" {
		if got := upstreamSpecificCodes(t)[code]; got != status {
			t.Fatalf("upstream lists %q for status %d, not %d", code, got, status)
		}
		return []string{code}
	}
	generic, ok := upstreamGenericCodes(t)[status]
	if !ok {
		t.Fatalf("status %d is not in upstream's generic table; add an explicit expectation", status)
	}
	return []string{generic}
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
	Type          string `json:"type"`
	Title         string `json:"title"`
	Status        int    `json:"status"`
	Code          string `json:"code"`
	Detail        string `json:"detail"`
	Error         string `json:"error"`
	Instance      string `json:"instance"`
	Guard         string `json:"guard"`
	CurrentSHA    string `json:"current_sha"`
	InvalidParams []struct {
		Name   string `json:"name"`
		Reason string `json:"reason"`
	} `json:"invalid_params"`
}

// problemMembers is every member a plain problem body may carry:
// ProblemDetails plus ValidationProblemDetails' invalid_params
// (TestProblemMembersMatchUpstream pins it to the spec).
var problemMembers = []string{"code", "detail", "error", "instance", "invalid_params", "status", "title", "type"}

// refGuardMembers are the members RefGuardProblemDetails adds to
// ProblemDetails. A body with code ref_guard_failed must carry both, and
// no other body may (TestProblemMembersMatchUpstream pins them).
var refGuardMembers = []string{"current_sha", "guard"}

// notesGuardMembers are the note-write members that
// NotesWriteRefGuardProblemDetails keeps next to the problem members
// (TestProblemMembersMatchUpstream pins them).
var notesGuardMembers = []string{"base_commit", "new_ref_sha", "notes_ref", "result", "sha", "target_ref"}

// upstreamRefGuardSchema returns the properties of RefGuardProblemDetails'
// own allOf item and the `guard` enum.
func upstreamRefGuardSchema(t *testing.T) (props, guards []string) {
	t.Helper()
	return upstreamAllOfProperties(t, "RefGuardProblemDetails")
}

// upstreamAllOfProperties returns the properties of a schema's own allOf
// item and any `guard` enum, by indentation: the schema at 4 spaces,
// "- properties:" at 8, each property at 12, enum items at 16.
func upstreamAllOfProperties(t *testing.T, schema string) (props, guards []string) {
	t.Helper()
	raw, err := os.ReadFile(upstreamSpecPath)
	if err != nil {
		t.Fatal(err)
	}
	inSchema, inProps, inEnum := false, false, false
	for l := range strings.SplitSeq(string(raw), "\n") {
		indent := len(l) - len(strings.TrimLeft(l, " "))
		trimmed := strings.TrimSpace(l)
		switch {
		case indent == 4 && trimmed == schema+":":
			inSchema = true
		case inSchema && indent <= 4 && trimmed != "":
			inSchema, inProps = false, false
		case inSchema && indent == 8:
			inProps = trimmed == "- properties:"
		case inProps && indent == 12 && strings.HasSuffix(trimmed, ":"):
			props = append(props, strings.TrimSuffix(trimmed, ":"))
			inEnum = false
		case inProps && indent == 14:
			inEnum = props[len(props)-1] == "guard" && trimmed == "enum:"
		case inEnum && indent == 16 && strings.HasPrefix(trimmed, "- "):
			guards = append(guards, strings.TrimPrefix(trimmed, "- "))
		}
	}
	if len(props) == 0 || (schema == "RefGuardProblemDetails") != (len(guards) > 0) {
		t.Fatalf("%s has properties %v and guards %v in %s", schema, props, guards, upstreamSpecPath)
	}
	slices.Sort(props)
	return props, guards
}

// upstreamSchemaProperties returns the property names of one schema under
// components/schemas in the vendored spec, by indentation: the schema at
// 4 spaces, "properties:" at 6, each property at 8.
func upstreamSchemaProperties(t *testing.T, schema string) []string {
	t.Helper()
	raw, err := os.ReadFile(upstreamSpecPath)
	if err != nil {
		t.Fatal(err)
	}
	var props []string
	inSchema, inProps := false, false
	for l := range strings.SplitSeq(string(raw), "\n") {
		indent := len(l) - len(strings.TrimLeft(l, " "))
		switch {
		case indent == 4 && strings.TrimSpace(l) == schema+":":
			inSchema = true
		case inSchema && indent <= 4 && strings.TrimSpace(l) != "":
			inSchema, inProps = false, false
		case inSchema && indent == 6:
			inProps = strings.TrimSpace(l) == "properties:"
		case inProps && indent == 8 && strings.HasSuffix(l, ":"):
			props = append(props, strings.TrimSuffix(strings.TrimSpace(l), ":"))
		}
	}
	if len(props) == 0 {
		t.Fatalf("schema %s has no properties in %s", schema, upstreamSpecPath)
	}
	slices.Sort(props)
	return props
}

// TestProblemMembersMatchUpstream pins the problem members and the
// InvalidParam item to the spec, and the invalid_params example that
// upstream documents for a type error.
func TestProblemMembersMatchUpstream(t *testing.T) {
	want := append(upstreamSchemaProperties(t, "ProblemDetails"), "invalid_params")
	slices.Sort(want)
	if !slices.Equal(want, problemMembers) {
		t.Fatalf("problem members = %v, upstream ProblemDetails + invalid_params = %v", problemMembers, want)
	}
	if got := upstreamSchemaProperties(t, "InvalidParam"); !slices.Equal(got, []string{"name", "reason"}) {
		t.Fatalf("InvalidParam properties = %v, want [name reason]", got)
	}
	props, guards := upstreamRefGuardSchema(t)
	if !slices.Equal(props, refGuardMembers) {
		t.Fatalf("RefGuardProblemDetails properties = %v, want %v", props, refGuardMembers)
	}
	if want := []string{"expected_target_sha", "expected_source_sha", "expected_notes_ref_sha"}; !slices.Equal(guards, want) {
		t.Fatalf("RefGuardProblemDetails guard enum = %v, want %v", guards, want)
	}
	if got, _ := upstreamAllOfProperties(t, "NotesWriteRefGuardProblemDetails"); !slices.Equal(got, notesGuardMembers) {
		t.Fatalf("NotesWriteRefGuardProblemDetails properties = %v, want %v", got, notesGuardMembers)
	}
	raw, err := os.ReadFile(upstreamSpecPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "invalid_params:\n                  - name: default_branch\n                    reason: must be a string\n") {
		t.Fatal("the spec no longer has the default_branch / must be a string invalid_params example")
	}
	type patch struct {
		DefaultBranch string `json:"default_branch"`
	}
	err = json.Unmarshal([]byte(`{"default_branch":5}`), new(patch))
	if got := invalidParamsFor(err); len(got) != 1 || got[0] != (invalidParam{Name: "default_branch", Reason: "must be a string"}) {
		t.Fatalf("invalidParamsFor(%v) = %+v, want upstream's example", err, got)
	}
}

// TestInvalidParamsFor covers each decode-failure class: the ones that
// name a field, and the ones that cannot (no invalid_params upstream).
func TestInvalidParamsFor(t *testing.T) {
	type auth struct {
		Token string `json:"token"`
	}
	type req struct {
		Name  string            `json:"name"`
		Force bool              `json:"force"`
		Limit int               `json:"limit"`
		Ratio float64           `json:"ratio"`
		Auth  *auth             `json:"auth"`
		Tags  []string          `json:"tags"`
		Meta  map[string]string `json:"meta"`
		When  time.Time         `json:"when"`
		Raw   []byte            `json:"raw"`
		Any   any               `json:"any"`
	}
	for _, tc := range []struct {
		body, name, reason string
	}{
		{`{"name":1}`, "name", "must be a string"},
		{`{"force":"yes"}`, "force", "must be a boolean"},
		{`{"limit":"x"}`, "limit", "must be an integer"},
		{`{"ratio":"x"}`, "ratio", "must be a number"},
		{`{"auth":{"token":1}}`, "auth.token", "must be a string"},
		{`{"auth":3}`, "auth", "must be an object"},
		{`{"tags":"x"}`, "tags", "must be an array"},
		// An element error names the element (Go 1.27 encoding/json).
		{`{"tags":["a",1]}`, "tags.1", "must be a string"},
		{`{"meta":{"k":1}}`, "meta.k", "must be a string"},
		{`{"meta":[]}`, "meta", "must be an object"},
		// Types that decode from a JSON string.
		{`{"when":1}`, "when", "must be a string"},
		{`{"raw":1}`, "raw", "must be a string"},
		{`[]`, "", ""},             // the body itself has the wrong type
		{`{`, "", ""},              // syntax error
		{`{"name":"a"} x`, "", ""}, // trailing data
	} {
		err := json.Unmarshal([]byte(tc.body), new(req))
		if err == nil {
			t.Fatalf("%s: decode succeeded", tc.body)
		}
		got := invalidParamsFor(err)
		if tc.name == "" {
			if got != nil {
				t.Errorf("%s: invalidParamsFor = %+v, want none", tc.body, got)
			}
			continue
		}
		if len(got) != 1 || got[0] != (invalidParam{Name: tc.name, Reason: tc.reason}) {
			t.Errorf("%s: invalidParamsFor = %+v, want [{%s %s}]", tc.body, got, tc.name, tc.reason)
		}
	}
	// encoding/json's unknown-field error has no path: no invalid_params.
	dec := json.NewDecoder(strings.NewReader(`{"auth":{"x":1}}`))
	dec.DisallowUnknownFields()
	if err := dec.Decode(new(req)); err == nil || invalidParamsFor(err) != nil {
		t.Errorf("DisallowUnknownFields error %v: invalidParamsFor = %+v, want none", err, invalidParamsFor(err))
	}
	if invalidParamsFor(nil) != nil {
		t.Error("invalidParamsFor(nil) != nil")
	}
	// strictJSON names the member path, and keeps its message.
	type strictReq struct {
		Ref    string `json:"ref"`
		Author *struct {
			Name string `json:"name"`
		} `json:"author"`
	}
	for _, tc := range []struct{ body, name, reason, msg string }{
		{`{"ref":"a","x":1}`, "x", "is not a known field", `unknown field "x"`},
		{`{"author":{"name":"a","x":1}}`, "author.x", "is not a known field", `unknown field "author.x"`},
		{`{"ref":"a","ref":"b"}`, "ref", "appears more than once", `duplicate field "ref"`},
		{`{"ref":null}`, "ref", "must not be null", "ref must not be null"},
		{`{"author":3}`, "author", "must be an object", "author must be a JSON object"},
		{`{"ref":3}`, "ref", "must be a string", ""},
	} {
		err := strictJSON([]byte(tc.body), new(strictReq))
		if err == nil {
			t.Fatalf("%s: strictJSON succeeded", tc.body)
		}
		if tc.msg != "" && err.Error() != tc.msg {
			t.Errorf("%s: message = %q, want %q (unchanged)", tc.body, err, tc.msg)
		}
		if got := invalidParamsFor(err); len(got) != 1 || got[0] != (invalidParam{Name: tc.name, Reason: tc.reason}) {
			t.Errorf("%s: invalidParamsFor = %+v, want [{%s %s}]", tc.body, got, tc.name, tc.reason)
		}
	}
	if err := strictJSON([]byte(`[]`), new(strictReq)); err == nil || invalidParamsFor(err) != nil {
		t.Errorf("non-object body: %v, invalidParamsFor = %+v, want none", err, invalidParamsFor(err))
	}
	// An empty member name has no dotted path: the old message, no item.
	for _, body := range []string{`{"":1}`, `{"author":{"":1}}`} {
		err := strictJSON([]byte(body), new(strictReq))
		if err == nil || !strings.HasPrefix(err.Error(), "unknown field ") || invalidParamsFor(err) != nil {
			t.Errorf("%s: err %v, invalidParamsFor = %+v, want an unknown-field error and none", body, err, invalidParamsFor(err))
		}
	}
	// A member decoded on its own names fields below its prefix.
	type sub struct {
		Auth struct {
			Token string `json:"token"`
		} `json:"auth"`
	}
	for _, tc := range []struct{ body, name, reason string }{
		{`{"auth":{"token":1}}`, "base_repo.auth.token", "must be a string"},
		{`3`, "base_repo", "must be an object"},
		{`{`, "", ""},
	} {
		err := fieldErrorUnder("base_repo", "invalid base_repo", json.Unmarshal([]byte(tc.body), new(sub)))
		got := invalidParamsFor(err)
		if err.Error() != "invalid base_repo" {
			t.Errorf("%s: message = %q", tc.body, err)
		}
		if tc.name == "" {
			if got != nil {
				t.Errorf("%s: invalidParamsFor = %+v, want none", tc.body, got)
			}
			continue
		}
		if len(got) != 1 || got[0] != (invalidParam{Name: tc.name, Reason: tc.reason}) {
			t.Errorf("%s: invalidParamsFor = %+v, want [{%s %s}]", tc.body, got, tc.name, tc.reason)
		}
	}
}

// TestWriteProblemInvalidParamsAssertions: invalid_params only on a 400,
// and only with complete items.
func TestWriteProblemInvalidParamsAssertions(t *testing.T) {
	for _, tc := range []struct {
		status int
		params []invalidParam
	}{
		{http.StatusConflict, []invalidParam{{"a", "b"}}},
		{http.StatusBadRequest, []invalidParam{{"", "b"}}},
		{http.StatusBadRequest, []invalidParam{{"a", ""}}},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("writeProblemParams(%d, %+v) did not panic", tc.status, tc.params)
				}
			}()
			writeProblemParams(httptest.NewRecorder(), tc.status, genericProblemCode(tc.status), "boom", tc.params)
		}()
	}
}

// fullSHARE matches a full lowercase SHA-1 object id.
var fullSHARE = regexp.MustCompile(`^[0-9a-f]{40}$`)

// assertProblemFields checks the RFC 9457 fields of a plain-envelope error
// body against its HTTP status and returns the legacy `error` message.
// `code` must be one expectedProblemCodes derives from the status and
// message.
func assertProblemFields(t *testing.T, status int, body []byte) string {
	t.Helper()
	var p problemBody
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatalf("error body %q is not JSON: %v", body, err)
	}
	wantCodes := expectedProblemCodes(t, status, p.Error)
	if p.Type != "about:blank" || p.Title != http.StatusText(status) || p.Status != status || !slices.Contains(wantCodes, p.Code) {
		t.Fatalf("problem fields = {type %q, title %q, status %d, code %q}, want {about:blank, %q, %d, one of %q} (body %s)",
			p.Type, p.Title, p.Status, p.Code, http.StatusText(status), status, wantCodes, body)
	}
	if p.Error == "" || p.Detail != p.Error {
		t.Fatalf("detail = %q, error = %q: want equal and non-empty (body %s)", p.Detail, p.Error, body)
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(body, &members); err != nil {
		t.Fatal(err)
	}
	refGuard := p.Code == "ref_guard_failed"
	for m := range members {
		if !slices.Contains(problemMembers, m) && !(refGuard && slices.Contains(refGuardMembers, m)) {
			t.Fatalf("problem body has member %q outside upstream's schema (body %s)", m, body)
		}
	}
	if refGuard {
		_, guards := upstreamRefGuardSchema(t)
		if _, ok := members["current_sha"]; !ok || !slices.Contains(guards, p.Guard) || !fullSHARE.MatchString(p.CurrentSHA) {
			t.Fatalf("ref_guard_failed body needs guard in %v and a 40-hex current_sha (body %s)", guards, body)
		}
	}
	if raw, ok := members["invalid_params"]; ok {
		if status != http.StatusBadRequest || len(p.InvalidParams) == 0 || string(raw) == "null" {
			t.Fatalf("invalid_params on status %d must be a non-empty list on a 400 (body %s)", status, body)
		}
		for _, ip := range p.InvalidParams {
			if ip.Name == "" || ip.Reason == "" {
				t.Fatalf("incomplete invalid_params item %+v (body %s)", ip, body)
			}
		}
	}
	return p.Error
}

// assertNotesRefGuard checks a stale expected_notes_ref_sha on a notes
// route with upstream's 409 (#568): a NotesWriteRefGuardProblemDetails
// body, that is the note-write members with a failed result plus the
// problem members, which the oracle checks once the note members are
// removed.
func assertNotesRefGuard(t *testing.T, status int, body map[string]any, sha, notesRef, currentSHA string) {
	t.Helper()
	if status != http.StatusConflict {
		t.Fatalf("status = %d, want 409 (body %v)", status, body)
	}
	res, _ := body["result"].(map[string]any)
	if body["sha"] != sha || body["notes_ref"] != notesRef || body["target_ref"] != notesRef || body["new_ref_sha"] != currentSHA ||
		res["success"] != false || res["status"] != "conflict" || res["message"] != body["detail"] {
		t.Fatalf("note members = %v, want sha %s, notes_ref %s, new_ref_sha %s and a failed conflict result", body, sha, notesRef, currentSHA)
	}
	if body["guard"] != "expected_notes_ref_sha" || body["current_sha"] != currentSHA {
		t.Fatalf("guard members = %v, want expected_notes_ref_sha and current_sha %s", body, currentSHA)
	}
	problem := map[string]any{}
	for k, v := range body {
		if !slices.Contains(notesGuardMembers, k) {
			problem[k] = v
		}
	}
	raw, err := json.Marshal(problem)
	if err != nil {
		t.Fatal(err)
	}
	assertProblemFields(t, status, raw)
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
		// A sync create of a plain repo (a repeat plain create is 200, #388).
		{"repo exists", http.MethodPost, "/api/v1/repos", writeTok, []byte(`{"upstream":{"provider":"gitea","owner":"o","name":"n","host":"upstream.invalid"}}`), http.StatusConflict, "conflict"},
		// The alias's own lookup of the token's repo claim.
		{"alias claim not found", http.MethodGet, "/api/repos/" + url + "/branches", missingTok, nil, http.StatusNotFound, "repository_not_found"},
		{"repo name mismatch", http.MethodPost, "/api/v1/repos", writeTok, []byte(`{"repo_name":"pd-other"}`), http.StatusBadRequest, "repo_name_mismatch"},
		// Other 403s keep the generic code.
		{"org token on repo route", http.MethodGet, "/api/v1/repo", orgTok, nil, http.StatusForbidden, "forbidden"},
		{"branch not found", http.MethodGet, "/api/v1/repos/branch?name=nope", readTok, nil, http.StatusNotFound, "branch_not_found"},
		{"alias branch not found", http.MethodGet, "/api/repos/" + url + "/branch?name=nope", readTok, nil, http.StatusNotFound, "branch_not_found"},
		{"ref not found", http.MethodGet, "/api/v1/repos/commit?ref=nope", readTok, nil, http.StatusNotFound, "ref_not_found"},
		// List commits: `ref` is any revision, the deprecated `branch` a branch.
		{"commits ref not found", http.MethodGet, "/api/v1/repos/commits?ref=nope", readTok, nil, http.StatusNotFound, "ref_not_found"},
		{"commits branch not found", http.MethodGet, "/api/v1/repos/commits?branch=nope", readTok, nil, http.StatusNotFound, "branch_not_found"},
		{"commits default branch not found", http.MethodGet, "/api/v1/repos/commits", readTok, nil, http.StatusNotFound, "branch_not_found"},
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

// TestInvalidParamsOnBodyFailures: request-body decode failures name the
// field in invalid_params through the full server on both backends; the
// failures that cannot name one leave the member out.
func TestInvalidParamsOnBodyFailures(t *testing.T) {
	runBackends(t, testInvalidParamsOnBodyFailures)
}

func testInvalidParamsOnBodyFailures(t *testing.T, e *testEnv) {
	org := e.newOrg(t)
	url := "ip-" + randomHex(t, 4)
	createRepo(t, e, org, url, []byte(`{"default_branch":"main"}`), http.StatusCreated)
	repoTok := e.mint(t, tokenSpec{iss: org, repo: url, scopes: []string{auth.ScopeRepoWrite}})
	gitTok := e.mint(t, tokenSpec{iss: org, repo: url, scopes: []string{auth.ScopeGitRead, auth.ScopeGitWrite}})
	freshTok := e.mint(t, tokenSpec{iss: org, repo: "ip-fresh-" + randomHex(t, 4), scopes: []string{auth.ScopeRepoWrite}})

	type param struct{ name, reason string }
	for _, tc := range []struct {
		name, method, path, token string
		body                      string
		want                      *param // nil: no invalid_params
	}{
		// Upstream's own example (PATCH /repos, ValidationProblemDetails).
		{"patch type error", http.MethodPatch, "/api/v1/repo", repoTok, `{"default_branch":5}`, &param{"default_branch", "must be a string"}},
		{"alias patch type error", http.MethodPatch, "/api/repos/" + url, repoTok, `{"default_branch":5}`, &param{"default_branch", "must be a string"}},
		{"create type error", http.MethodPost, "/api/v1/repos", freshTok, `{"default_branch":true}`, &param{"default_branch", "must be a string"}},
		{"create branch type error", http.MethodPost, "/api/v1/repos/branches/create", gitTok, `{"target_branch":"x","force":"yes"}`, &param{"force", "must be a boolean"}},
		{"strict unknown field", http.MethodPost, "/api/repos/" + url + "/notes/append", gitTok, `{"sha":"main","note":"n","bogus":1}`, &param{"bogus", "is not a known field"}},
		{"nested base_repo type", http.MethodPost, "/api/v1/repos", freshTok, `{"base_repo":{"provider":"github","owner":"o","name":"n","auth":{"token":1}}}`,
			&param{"base_repo.auth.token", "must be a string"}},
		{"nested upstream type", http.MethodPost, "/api/v1/repos", freshTok, `{"upstream":{"provider":"github","owner":1,"name":"n"}}`,
			&param{"upstream.owner", "must be a string"}},
		{"upstream not an object", http.MethodPost, "/api/v1/repos", freshTok, `{"upstream":3}`, &param{"upstream", "must be an object"}},
		{"upstream unknown field", http.MethodPost, "/api/v1/repos", freshTok, `{"upstream":{"provider":"github","owner":"o","name":"n","x":1}}`, nil},
		{"strict empty member name", http.MethodPost, "/api/repos/" + url + "/notes/append", gitTok, `{"":1}`, nil},
		{"syntax error", http.MethodPatch, "/api/v1/repo", repoTok, `{`, nil},
		{"wrong top-level type", http.MethodPatch, "/api/v1/repo", repoTok, `[]`, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, body := e.doReq(t, tc.method, tc.path, tc.token, []byte(tc.body), "")
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body %s)", resp.StatusCode, body)
			}
			requireEnvelope(t, resp, body)
			var p problemBody
			if err := json.Unmarshal(body, &p); err != nil {
				t.Fatal(err)
			}
			if tc.want == nil {
				if strings.Contains(string(body), `"invalid_params"`) {
					t.Fatalf("body %s has invalid_params, want none", body)
				}
				return
			}
			if len(p.InvalidParams) != 1 || p.InvalidParams[0].Name != tc.want.name || p.InvalidParams[0].Reason != tc.want.reason {
				t.Fatalf("invalid_params = %+v, want [{%s %s}] (body %s)", p.InvalidParams, tc.want.name, tc.want.reason, body)
			}
			if !strings.HasPrefix(p.Error, "invalid ") {
				t.Fatalf("error = %q, want the unchanged decode message", p.Error)
			}
		})
	}
}

// TestInvalidParamsOnValidators: the single-field validators of upstream's
// ValidationProblemDetails routes name their fields through the full
// server on both backends. Rules across fields, and query parameters,
// name none.
func TestInvalidParamsOnValidators(t *testing.T) {
	runBackends(t, testInvalidParamsOnValidators)
}

func testInvalidParamsOnValidators(t *testing.T, e *testEnv) {
	org := e.newOrg(t)
	url := "iv-" + randomHex(t, 4)
	created := createRepo(t, e, org, url, []byte(`{"default_branch":"main"}`), http.StatusCreated)
	repoTok := e.mint(t, tokenSpec{iss: org, repo: url, scopes: []string{auth.ScopeRepoWrite}})
	gitTok := e.mint(t, tokenSpec{iss: org, repo: url, scopes: []string{auth.ScopeGitRead, auth.ScopeGitWrite}})
	orgTok := e.mint(t, tokenSpec{iss: org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeRepoWrite}})
	fresh := func() string {
		return e.mint(t, tokenSpec{iss: org, repo: "iv-fresh-" + randomHex(t, 4), scopes: []string{auth.ScopeRepoWrite}})
	}
	type param struct{ name, reason string }
	req := func(n, r string) param { return param{n, r} }
	for _, tc := range []struct {
		name, method, path, token, body string
		want                            []param // nil: no invalid_params
	}{
		{"patch required", http.MethodPatch, "/api/v1/repo", repoTok, `{}`, []param{req("default_branch", "is required")}},
		{"patch invalid", http.MethodPatch, "/api/v1/repo", repoTok, `{"default_branch":"bad..name"}`, []param{req("default_branch", "is not a valid branch name")}},
		{"create invalid", http.MethodPost, "/api/v1/repos", fresh(), `{"default_branch":"bad..name"}`, []param{req("default_branch", "is not a valid branch name")}},
		{"create query param", http.MethodPost, "/api/v1/repos?default_branch=bad..name", fresh(), `{}`, nil},
		{"upstream required", http.MethodPost, "/api/v1/repos", fresh(), `{"upstream":{"provider":"github"}}`,
			[]param{req("upstream.owner", "is required"), req("upstream.name", "is required")}},
		{"upstream provider", http.MethodPost, "/api/v1/repos", fresh(), `{"upstream":{"provider":"nope","owner":"o","name":"n"}}`,
			[]param{req("upstream.provider", "is not supported")}},
		{"upstream access", http.MethodPost, "/api/v1/repos", fresh(), `{"upstream":{"provider":"github","owner":"o","name":"n","access":"x"}}`,
			[]param{req("upstream.access", "must be app or public")}},
		{"upstream type", http.MethodPost, "/api/v1/repos", fresh(), `{"upstream":{"provider":"github","owner":1,"name":"n"}}`,
			[]param{req("upstream.owner", "must be a string")}},
		{"base_repo required", http.MethodPost, "/api/v1/repos", fresh(), `{"base_repo":{"provider":"github","owner":"o"}}`,
			[]param{req("base_repo.name", "is required")}},
		{"base_repo operation", http.MethodPost, "/api/v1/repos", fresh(), `{"base_repo":{"provider":"github","owner":"o","name":"n","operation":"bogus"}}`,
			[]param{req("base_repo.operation", "must be sync or fork")}},
		{"base_repo not an object", http.MethodPost, "/api/v1/repos", fresh(), `{"base_repo":{"provider":"github","owner":"o","name":"n","auth":3}}`,
			[]param{req("base_repo.auth", "must be an object")}},
		{"both forms", http.MethodPost, "/api/v1/repos", fresh(), `{"base_repo":{"provider":"github"},"upstream":{"provider":"github"}}`, nil},
		{"tag name", http.MethodPost, "/api/v1/repos/tags", gitTok, `{"name":"bad..name","ref":"main"}`, []param{req("name", "must be a valid tag name")}},
		{"tag ref", http.MethodPost, "/api/v1/repos/tags", gitTok, `{"name":"v1"}`, []param{req("ref", "is required")}},
		{"target_branch", http.MethodPost, "/api/v1/repos/branches/create", gitTok, `{"target_branch":"bad..name","base_ref":"main"}`,
			[]param{req("target_branch", "must be a valid branch name")}},
		{"target_branch type", http.MethodPost, "/api/v1/repos/branches/create", gitTok, `{"target_branch":5,"base_ref":"main"}`,
			[]param{req("target_branch", "must be a valid branch name")}},
		{"force without target", http.MethodPost, "/api/v1/repos/branches/create", gitTok, `{"force":true,"base_ref":"main"}`, nil},
		{"grep pattern", http.MethodPost, "/api/v1/repos/grep", gitTok, `{"query":{"pattern":""}}`, []param{req("query.pattern", "is required")}},
		{"grep context", http.MethodPost, "/api/v1/repos/grep", gitTok, `{"query":{"pattern":"x"},"context":{"before":-1,"after":-2}}`,
			[]param{req("context.before", "must be at least 0"), req("context.after", "must be at least 0")}},
		{"grep max_lines", http.MethodPost, "/api/v1/repos/grep", gitTok, `{"query":{"pattern":"x"},"limits":{"max_lines":0}}`,
			[]param{req("limits.max_lines", fmt.Sprintf("must be between 1 and %d", grepMaxLinesCap))}},
		{"grep paths", http.MethodPost, "/api/v1/repos/grep", gitTok, `{"query":{"pattern":"x"},"paths":["src",""]}`,
			[]param{req("paths.1", "must be a non-empty path")}},
		{"grep cursor", http.MethodPost, "/api/v1/repos/grep", gitTok, `{"query":{"pattern":"x"},"pagination":{"cursor":"!!"}}`,
			[]param{req("pagination.cursor", "is not a valid cursor")}},
		{"archive max_blob_size", http.MethodPost, "/api/v1/repos/archive", gitTok, `{"max_blob_size":-1}`, []param{req("max_blob_size", "must be at least 0")}},
		{"archive glob", http.MethodPost, "/api/v1/repos/archive", gitTok, `{"exclude_globs":["a",""]}`, []param{req("exclude_globs.1", "must be a non-empty glob")}},
		{"archive prefix", http.MethodPost, "/api/v1/repos/archive", gitTok, `{"archive":{"prefix":"../x"}}`,
			[]param{req("archive.prefix", "must be a relative path inside the archive")}},
		{"credential password", http.MethodPost, "/api/v1/repos/git-credentials", repoTok, `{"repo_id":"` + created.RepoID + `"}`, []param{req("password", "is required")}},
		{"credential id", http.MethodPut, "/api/v1/repos/git-credentials", orgTok, `{"password":"p"}`, []param{req("id", "is required")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, body := e.doReq(t, tc.method, tc.path, tc.token, []byte(tc.body), "")
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body %s)", resp.StatusCode, body)
			}
			requireEnvelope(t, resp, body)
			var p problemBody
			if err := json.Unmarshal(body, &p); err != nil {
				t.Fatal(err)
			}
			var got []param
			for _, ip := range p.InvalidParams {
				got = append(got, param{ip.Name, ip.Reason})
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("invalid_params = %+v, want %+v (body %s)", got, tc.want, body)
			}
		})
	}
}
