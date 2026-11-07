package api

import (
	"bufio"
	"bytes"
	"encoding"
	"encoding/json"
	"errors"
	"iter"
	"net/http"
	"reflect"
	"slices"
	"strings"

	"github.com/coder/openstorage/internal/ref"
)

// internalErrorMsg is the only 500 body clients ever see: raw error strings
// (often verbatim DB/driver output) are logged server-side, never leaked
// onto the wire (docs/reference/http-api.md: clients branch on status codes only).
const internalErrorMsg = "internal error"

// internalErr logs the real error with request context and writes the
// generic {"error":"internal error"} envelope with status 500 — except for
// read-side git limit errors, which get their documented answer
// (writeGitLimitErr).
func (s *server) internalErr(w http.ResponseWriter, r *http.Request, op string, err error) {
	assertf(err != nil, "internalErr called with nil error")
	assertf(op != "", "internalErr called with empty op")
	// Read-side git limits (#313) are not internal errors: every handler
	// reports its git failures through here, so this is the one place that
	// maps them to their documented 422/503 answers.
	if s.writeGitLimitErr(w, r, op, err) {
		return
	}
	s.logger.Error("internal error", "op", op, "method", r.Method, "path", logPath(r), "err", err)
	writeErr(w, http.StatusInternalServerError, internalErrorMsg)
}

// errorEnvelope is the plain wire error shape (docs/reference/http-api.md):
// the legacy {"error": "<unstable human string>"} plus the RFC 9457
// problem-details fields upstream documents (issue #349). `code` is the
// generic per-status code only; `detail` mirrors `error`; `instance` is the
// request path (issue #352). Clients branch on the status code or `code`,
// never on the message. writeProblemJSON sends it as
// application/problem+json, the type upstream documents for problem bodies.
type errorEnvelope struct {
	Error    string `json:"error"`
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Code     string `json:"code"`
	Detail   string `json:"detail"`
	Instance string `json:"instance,omitempty"`
	// InvalidParams is upstream's ValidationProblemDetails member: only on
	// a 400 for a request body, only when the server can name the field.
	InvalidParams []invalidParam `json:"invalid_params,omitempty"`
	// Guard and CurrentSHA are upstream's RefGuardProblemDetails members,
	// set together and only on code ref_guard_failed: the request field of
	// the failed guard, and the commit of the guarded ref (40 zeros when
	// the ref does not exist).
	Guard      string `json:"guard,omitempty"`
	CurrentSHA string `json:"current_sha,omitempty"`
}

// invalidParam is one item of upstream's `invalid_params` (InvalidParam):
// the dotted wire path of one invalid request-body field, and the rule
// that its value breaks.
type invalidParam struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

// writeErr writes the plain error envelope with the given status and
// upstream's generic code for it.
// 401 responses additionally carry `WWW-Authenticate: Bearer` (docs/reference/authentication.md).
func writeErr(w http.ResponseWriter, status int, msg string) {
	writeProblem(w, status, genericProblemCode(status), msg)
}

// writeProblem writes the plain error envelope. code is the generic code
// for status, or a specific code from specificProblemCodes whose upstream
// status is status.
func writeProblem(w http.ResponseWriter, status int, code, msg string) {
	writeProblemParams(w, status, code, msg, nil)
}

// writeProblemParams is writeProblem with `invalid_params`, which only a
// 400 may carry.
func writeProblemParams(w http.ResponseWriter, status int, code, msg string, params []invalidParam) {
	writeProblemBody(w, status, code, msg, params, "", "")
}

// writeRefGuardFailed writes the 409 ref_guard_failed problem body of a
// failed ref guard (upstream's RefGuardProblemDetails). guard is the
// standard name of the request field; currentSHA is the guarded ref's
// commit, or ref.ZeroSHA when the ref does not exist.
func writeRefGuardFailed(w http.ResponseWriter, guard, currentSHA, msg string) {
	writeProblemJSON(w, http.StatusConflict, refGuardProblem(w, guard, currentSHA, msg))
}

// refGuardProblem builds the 409 ref_guard_failed problem members for a
// body that keeps other members next to them (the notes routes).
func refGuardProblem(w http.ResponseWriter, guard, currentSHA, msg string) errorEnvelope {
	assertf(guard == "expected_target_sha" || guard == "expected_source_sha" || guard == "expected_notes_ref_sha",
		"refGuardProblem: unknown guard %q", guard)
	assertf(ref.ValidSHA(currentSHA), "refGuardProblem: current_sha %q is not a full sha", currentSHA)
	return newProblem(w, http.StatusConflict, codeRefGuardFailed, msg, nil, guard, currentSHA)
}

func writeProblemBody(w http.ResponseWriter, status int, code, msg string, params []invalidParam, guard, currentSHA string) {
	writeProblemJSON(w, status, newProblem(w, status, code, msg, params, guard, currentSHA))
}

// newProblem is the only builder of errorEnvelope. It checks every member
// against upstream's rules and sets WWW-Authenticate on a 401.
func newProblem(w http.ResponseWriter, status int, code, msg string, params []invalidParam, guard, currentSHA string) errorEnvelope {
	assertf(status >= 400 && status < 600, "writeProblem called with non-error status %d", status)
	assertf((guard != "") == (code == codeRefGuardFailed) && (guard != "") == (currentSHA != ""),
		"writeProblem: code %q with guard %q and current_sha %q", code, guard, currentSHA)
	assertf(len(params) == 0 || status == http.StatusBadRequest, "writeProblem: invalid_params on status %d", status)
	for _, p := range params {
		assertf(p.Name != "" && p.Reason != "", "writeProblem: incomplete invalid param %+v", p)
	}
	assertf(msg != "", "writeProblem called with empty message")
	// RFC 9457 `title` is the status text, so only registered statuses are
	// valid here (499 is never emitted: ADR-26).
	title := http.StatusText(status)
	assertf(title != "", "writeProblem called with unregistered status %d", status)
	if specific, ok := specificProblemCodes[code]; ok {
		assertf(specific == status, "writeProblem: code %q is upstream's code for %d, not %d", code, specific, status)
	} else {
		assertf(code == genericProblemCode(status), "writeProblem: code %q is neither specific nor the generic code for %d", code, status)
	}
	if status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", "Bearer")
	}
	return errorEnvelope{
		Error:         msg,
		Type:          "about:blank",
		Title:         title,
		Status:        status,
		Code:          code,
		Detail:        msg,
		Instance:      problemInstance(w),
		InvalidParams: params,
		Guard:         guard,
		CurrentSHA:    currentSHA,
	}
}

var textUnmarshalerType = reflect.TypeFor[encoding.TextUnmarshaler]()

// writeInvalid writes the 400 for a request body that is not valid. msg
// is the message; cause adds `invalid_params` when it names a field
// (invalidParamsFor).
func writeInvalid(w http.ResponseWriter, msg string, cause error) {
	writeProblemParams(w, http.StatusBadRequest, genericProblemCode(http.StatusBadRequest), msg, invalidParamsFor(cause))
}

// fieldError is a request-body error that names its invalid fields.
// Error() returns msg, the message the body already carries.
type fieldError struct {
	params []invalidParam
	msg    string
}

func (e *fieldError) Error() string { return e.msg }

// newFieldError returns a fieldError for the field at the dotted wire path
// name.
func newFieldError(name, reason, msg string) *fieldError {
	return newFieldErrors(msg, invalidParam{Name: name, Reason: reason})
}

// newFieldErrors returns a fieldError for several fields, one item each.
func newFieldErrors(msg string, params ...invalidParam) *fieldError {
	assertf(msg != "" && len(params) > 0, "newFieldErrors(%q, %d params): empty argument", msg, len(params))
	for _, p := range params {
		assertf(p.Name != "" && p.Reason != "", "newFieldErrors: incomplete param %+v", p)
	}
	return &fieldError{params: params, msg: msg}
}

// requiredFields returns a fieldError naming each empty field of fields
// (dotted wire path -> value), in the order given, or nil when none is
// empty.
func requiredFields(msg string, fields ...[2]string) error {
	var params []invalidParam
	for _, f := range fields {
		if f[1] == "" {
			params = append(params, invalidParam{Name: f[0], Reason: "is required"})
		}
	}
	if len(params) == 0 {
		return nil
	}
	return newFieldErrors(msg, params...)
}

// writeInvalidField writes the 400 for one invalid request-body field.
func writeInvalidField(w http.ResponseWriter, name, reason, msg string) {
	writeInvalid(w, msg, newFieldError(name, reason, msg))
}

// fieldErrorUnder returns an error with message msg that names the field
// cause names, below the request-body member prefix: for a member that
// is decoded on its own from a json.RawMessage. A type error on the
// member itself names prefix. When cause names no field, the error names
// none either.
func fieldErrorUnder(prefix, msg string, cause error) error {
	assertf(prefix != "" && msg != "", "fieldErrorUnder(%q, %q): empty argument", prefix, msg)
	if te, ok := errors.AsType[*json.UnmarshalTypeError](cause); ok && te.Field == "" && te.Type != nil {
		if kind := jsonKind(te.Type); kind != "" {
			return newFieldError(prefix, "must be "+kind, msg)
		}
	}
	params := invalidParamsFor(cause)
	if len(params) == 0 {
		return errors.New(msg)
	}
	for i := range params {
		params[i].Name = prefix + "." + params[i].Name
	}
	return newFieldErrors(msg, params...)
}

// invalidParamsFor names the request-body field that err is about: a
// *fieldError, or encoding/json's *UnmarshalTypeError with a field path
// and a JSON kind (encoding/json reports an array element or map value
// by its index or key, as in `paths.0`). It returns nil when it cannot name one (syntax errors,
// a wrong top-level type, encoding/json's unknown-field error, which has
// no path): upstream then leaves `invalid_params` out.
func invalidParamsFor(err error) []invalidParam {
	if fe, ok := errors.AsType[*fieldError](err); ok {
		return slices.Clone(fe.params)
	}
	if te, ok := errors.AsType[*json.UnmarshalTypeError](err); ok && te.Field != "" && te.Type != nil {
		if kind := jsonKind(te.Type); kind != "" {
			return []invalidParam{{Name: te.Field, Reason: "must be " + kind}}
		}
	}
	return nil
}

// jsonKind names the JSON kind that Go type t decodes from, with its
// article ("a string"), or "" when t accepts several kinds.
func jsonKind(t reflect.Type) string {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	// encoding/json decodes these from a JSON string, whatever their kind.
	if reflect.PointerTo(t).Implements(textUnmarshalerType) || (t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8) {
		return "a string"
	}
	switch t.Kind() {
	case reflect.String:
		return "a string"
	case reflect.Bool:
		return "a boolean"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "an integer"
	case reflect.Float32, reflect.Float64:
		return "a number"
	case reflect.Struct, reflect.Map:
		return "an object"
	case reflect.Slice, reflect.Array:
		return "an array"
	}
	return ""
}

// Specific problem codes open.storage emits, from the "Specific codes:"
// list in the "Error codes" section of docs/upstream/openapi.yaml.
// A specific code replaces the generic one only where the meaning matches
// upstream's exactly (issue #352).
const (
	codeAuthenticationUnavailable = "authentication_unavailable"
	codeBranchNotFound            = "branch_not_found"
	codeCredentialExists          = "credential_exists"
	codeCredentialNotFound        = "credential_not_found"
	codeInsufficientScope         = "insufficient_scope"
	codeRefGuardFailed            = "ref_guard_failed"
	codeRefNotFound               = "ref_not_found"
	codeRepoNameMismatch          = "repo_name_mismatch"
	codeRepositoryDeleted         = "repository_deleted"
	codeRepositoryNotFound        = "repository_not_found"
	codeRouteNotFound             = "route_not_found"
)

// specificProblemCodes maps each specific code to its upstream status
// (TestSpecificProblemCodesMatchUpstream pins it to the snapshot).
var specificProblemCodes = map[string]int{
	codeAuthenticationUnavailable: http.StatusForbidden,
	codeBranchNotFound:            http.StatusNotFound,
	codeCredentialExists:          http.StatusConflict,
	codeCredentialNotFound:        http.StatusNotFound,
	codeInsufficientScope:         http.StatusForbidden,
	codeRefGuardFailed:            http.StatusConflict,
	codeRefNotFound:               http.StatusNotFound,
	codeRepoNameMismatch:          http.StatusBadRequest,
	codeRepositoryDeleted:         http.StatusConflict,
	codeRepositoryNotFound:        http.StatusNotFound,
	codeRouteNotFound:             http.StatusNotFound,
}

// repoNotFoundMsg is the message of every repository-lookup 404.
const repoNotFoundMsg = "repository not found"

// writeRepoNotFound writes the repository-lookup 404: the repository does
// not exist or the token cannot access it (upstream's
// `repository_not_found`).
func writeRepoNotFound(w http.ResponseWriter) {
	writeProblem(w, http.StatusNotFound, codeRepositoryNotFound, repoNotFoundMsg)
}

// instanceWriter carries the RFC 9457 `instance` of one request: the
// escaped request path the client sent, without the query (upstream's
// example answers a bad ?cursor on GET /api/repos with `instance:
// /api/repos`).
type instanceWriter struct {
	http.ResponseWriter
	instance string
}

// Unwrap exposes the wrapped writer to http.ResponseController.
func (w *instanceWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// withProblemInstance records the request path for writeErr. It must wrap
// the whole handler: the /api/repos/{repo_name}/... aliases rewrite
// r.URL.Path to the v1 path before they dispatch (alias.go), and `instance`
// names the path the client sent.
func withProblemInstance(next http.Handler) http.Handler {
	assertf(next != nil, "withProblemInstance: nil handler")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(&instanceWriter{ResponseWriter: w, instance: r.URL.EscapedPath()}, r)
	})
}

// problemInstance finds the instanceWriter under w through the Unwrap
// chain, the http.ResponseController convention that every writer wrapper
// in this package follows. It returns "" (no `instance` member) when w was
// not served through withProblemInstance, as in unit tests.
func problemInstance(w http.ResponseWriter) string {
	for {
		switch t := w.(type) {
		case *instanceWriter:
			return t.instance
		case interface{ Unwrap() http.ResponseWriter }:
			w = t.Unwrap()
		default:
			return ""
		}
	}
}

// genericProblemCodes is upstream's generic per-status `code` table,
// verbatim from the "Error codes" section of docs/upstream/openapi.yaml
// (TestGenericProblemCodeMatchesUpstream pins it to the snapshot).
var genericProblemCodes = map[int]string{
	http.StatusBadRequest:            "bad_request",
	http.StatusUnauthorized:          "unauthorized",
	http.StatusForbidden:             "forbidden",
	http.StatusNotFound:              "not_found",
	http.StatusMethodNotAllowed:      "method_not_allowed",
	http.StatusRequestTimeout:        "request_timeout",
	http.StatusConflict:              "conflict",
	http.StatusPreconditionFailed:    "precondition_failed",
	http.StatusRequestEntityTooLarge: "payload_too_large",
	http.StatusUnprocessableEntity:   "unprocessable_entity",
	http.StatusTooManyRequests:       "rate_limited",
	499:                              "request_canceled",
	http.StatusInternalServerError:   "internal_error",
	http.StatusNotImplemented:        "not_implemented",
	http.StatusBadGateway:            "bad_gateway",
	http.StatusServiceUnavailable:    "service_unavailable",
	http.StatusGatewayTimeout:        "gateway_timeout",
}

// genericProblemCode returns upstream's generic `code` for status. Upstream:
// "Any other status uses its status text in snake_case"; that fallback uses
// Go's status text, which may name a few statuses differently from upstream.
// Every status writeErr emits today is in the table.
func genericProblemCode(status int) string {
	if code, ok := genericProblemCodes[status]; ok {
		return code
	}
	text := http.StatusText(status)
	assertf(text != "", "genericProblemCode: unregistered status %d", status)
	var b strings.Builder
	sep := false
	for _, r := range strings.ToLower(text) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			if sep && b.Len() > 0 {
				b.WriteByte('_')
			}
			b.WriteRune(r)
			sep = false
			continue
		}
		sep = true
	}
	return b.String()
}

// writeJSON writes v as the JSON response body with the given status.
func writeJSON(w http.ResponseWriter, status int, v any) {
	assertf(status >= 200 && status < 600, "writeJSON called with invalid status %d", status)
	writeJSONAs(w, status, "application/json", v)
}

// problemMediaType is the content type of every body that carries the
// problem members (newProblem). Upstream's spec says problem details come
// "under application/problem+json", and its write 409s use that type for
// the problem body and application/json for the result envelope. Result
// envelopes and success bodies stay application/json.
const problemMediaType = "application/problem+json"

// writeProblemJSON writes v, a body built on newProblem, as
// application/problem+json.
func writeProblemJSON(w http.ResponseWriter, status int, v any) {
	assertf(status >= 400 && status < 600, "writeProblemJSON called with non-error status %d", status)
	writeJSONAs(w, status, problemMediaType, v)
}

func writeJSONAs(w http.ResponseWriter, status int, contentType string, v any) {
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(status)
	// Encoding a value we constructed cannot fail; a write error just means
	// the client went away, which is not actionable here.
	_ = json.NewEncoder(w).Encode(v)
}

// jsonList is one list or object of a writeJSONLists body: key's value,
// an empty list or object in the template, is filled with items. An
// object's items are jsonMembers, in the order json.Marshal sorts a map's
// keys.
type jsonList struct {
	key   string
	items iter.Seq[any]
}

// jsonMember is one name and value of a streamed JSON object.
type jsonMember struct {
	name  string
	value any
}

// writeJSONLists writes the same bytes writeJSON writes for v with every
// list in lists filled in, but marshals one item at a time, so a body with
// hundreds of thousands of entries is never buffered whole. v is the
// response with those lists and objects empty; lists are in the order v
// encodes them.
func writeJSONLists(w http.ResponseWriter, status int, v any, lists ...jsonList) {
	assertf(status >= 200 && status < 300, "writeJSONLists called with status %d", status)
	tmpl, err := json.Marshal(v)
	assertf(err == nil, "writeJSONLists: marshal template: %v", err)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// A write error means the client went away or missed a delivery
	// deadline: stop at once, so the handler releases its git-read slot
	// and the listing. bufio.Writer keeps failing after its first error.
	bw := bufio.NewWriterSize(w, 64<<10)
	for _, l := range lists {
		// A string value cannot contain these markers: its quotes would be
		// escaped. Each list key appears once in our response types.
		list, object := []byte(`"`+l.key+`":[]`), []byte(`"`+l.key+`":{}`)
		isObject := bytes.Contains(tmpl, object)
		marker := list
		if isObject {
			marker = object
		}
		i := bytes.Index(tmpl, marker)
		assertf(i >= 0 && bytes.Count(tmpl, list)+bytes.Count(tmpl, object) == 1, "writeJSONLists: %s is not one empty list or object in %.200s", l.key, tmpl)
		cut := i + len(marker) - 1 // just after "[" or "{"
		if _, err := bw.Write(tmpl[:cut]); err != nil {
			return
		}
		sep := false
		for item := range l.items {
			var b []byte
			m, ok := item.(jsonMember)
			assertf(ok == isObject, "writeJSONLists: %s: member %v, object %v", l.key, ok, isObject)
			if ok {
				name, err := json.Marshal(m.name)
				assertf(err == nil, "writeJSONLists: marshal %s name: %v", l.key, err)
				value, err := json.Marshal(m.value)
				assertf(err == nil, "writeJSONLists: marshal %s value: %v", l.key, err)
				b = append(append(name, ':'), value...)
			} else {
				b, err = json.Marshal(item)
				assertf(err == nil, "writeJSONLists: marshal %s item: %v", l.key, err)
			}
			if sep {
				_ = bw.WriteByte(',') // a failure sticks: the next Write reports it
			}
			if _, err := bw.Write(b); err != nil {
				return
			}
			sep = true
		}
		tmpl = tmpl[cut:]
	}
	_, _ = bw.Write(tmpl)
	_ = bw.WriteByte('\n') // json.Encoder ends each value with a newline
	_ = bw.Flush()
}

// statusForHTTP maps an HTTP failure code onto the docs/explanation/streaming-commit-protocols.md result-envelope
// status string the SDKs convert into typed RefUpdateErrors. This is the
// single authoritative table for the write surface (wcFail, wmFailConflict,
// wrefNoteGuardFail); an unmapped code is a programming error and fails fast.
// One documented exception sends another status: the lost-CAS 409 of
// commit-pack and diff-commit says "failed" (wcFailLostCAS, #596).
func statusForHTTP(httpStatus int) string {
	switch httpStatus {
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		return "invalid"
	case http.StatusForbidden:
		return "forbidden"
	case http.StatusNotFound:
		return "not_found"
	case http.StatusConflict:
		return "conflict"
	case http.StatusPreconditionFailed:
		return "precondition_failed"
	case http.StatusRequestEntityTooLarge:
		return "payload_too_large" // commit-pack total size limit (#597)
	case http.StatusTooManyRequests:
		return "unavailable" // docs/reference/http-api.md: rate limit ("unavailable" to ref-update clients)
	case http.StatusInternalServerError:
		return "internal"
	default:
		assertf(false, "statusForHTTP: unmapped status %d", httpStatus)
		return ""
	}
}
