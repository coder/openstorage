package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
)

// maxDecodeWork bounds the bytes that unmarshalBody decodes for one body
// (the sum over its decodes). Each decode and each search for the member
// to replace cost about 0.2 ms for each KiB of body, so the bound keeps one
// request under about 100 ms. A body of 2 KiB or less can name 100 wrong-type
// fields, a body of 10 KiB about 25, and a body of more than 128 KiB only the
// first. The answer does not say that it stopped early.
const maxDecodeWork = 256 << 10

// unmarshalBody is json.Unmarshal for a request body that names every field
// of the wrong JSON type. encoding/json keeps decoding after a type error
// but returns only the first one. unmarshalBody replaces the member that
// failed by null, which every type accepts as no change, and decodes again
// until the body decodes or a bound is reached. It also stops when null
// does not fix the field (a map key that does not decode, an UnmarshalJSON
// that refuses null): when a pass leaves the body as it was, or the next
// decode names the same field again. A body with one wrong-type field
// returns the error of encoding/json unchanged. Any other error (syntax, a
// wrong top-level type) is returned unchanged.
func unmarshalBody(body []byte, dst any) error {
	err := json.Unmarshal(body, dst)
	te, ok := errors.AsType[*json.UnmarshalTypeError](err)
	if !ok || te.Field == "" {
		return err
	}
	var errs bodyErrors
	errs.add(te)
	work, budget := body, maxDecodeWork-len(body) // the first decode counts
	for !errs.full() && budget >= len(work) {
		next, ok := nullMembers(work, te.Field)
		// null is longer than a value of 1 to 3 bytes, so the body can grow.
		if !ok || len(next) > budget || bytes.Equal(next, work) {
			break
		}
		work, budget = next, budget-len(next)
		prev := te.Field
		te, ok = errors.AsType[*json.UnmarshalTypeError](json.Unmarshal(work, dst))
		if !ok || te.Field == "" || te.Field == prev {
			break
		}
		errs.add(te)
	}
	return errs.err()
}

// writeInvalidBody writes the 400 for a request body that did not decode.
// err is what encoding/json, unmarshalBody or strictJSON returned. The
// message of encoding/json names Go types and structs, so it never reaches
// the answer (decodeError): the answer names each field of the wrong type as
// invalid_params does, as upstream's example `default_branch must be a
// string`. strictJSON's own errors hold no such text and keep their message.
func writeInvalidBody(w http.ResponseWriter, err error) {
	if _, ok := errors.AsType[strictError](err); ok {
		writeInvalid(w, "invalid JSON body: "+err.Error(), err)
		return
	}
	derr := decodeError("", err)
	writeInvalid(w, derr.Error(), derr)
}

// strictError is an error that strictJSON wrote itself (an unknown, repeated
// or null member, trailing data) as opposed to one of encoding/json.
type strictError struct{ error }

func (e strictError) Unwrap() error { return e.error }

// decodeError returns the error that answers a request body, or the member of
// it that is decoded on its own (member is its name, "" for the whole body),
// when decoding it returned err. Its message holds no text of encoding/json:
//
//   - for a field of the wrong type it is `name reason`, as in
//     invalid_params, one for each field, joined by "; ",
//   - for an unknown field it is `invalid JSON body: unknown field "x"`,
//   - for anything else (a syntax error, a wrong top-level type) it is
//     `invalid JSON body`.
//
// For a member, "invalid JSON body" is "invalid <member>".
func decodeError(member string, err error) error {
	assertf(err != nil, "decodeError: nil error")
	what := "invalid JSON body"
	params := invalidParamsFor(err)
	if member != "" {
		what = "invalid " + member
		params = invalidParamsFor(fieldErrorUnder(member, what, err))
	}
	if len(params) > 0 {
		return newFieldErrors(paramsText(params), params...)
	}
	if field := unknownFieldText(err); field != "" {
		return errors.New(what + ": " + field)
	}
	return errors.New(what)
}

// frameMsg is the message for an NDJSON frame that did not decode. Like
// decodeError, it holds no text of encoding/json.
func frameMsg(err error) string {
	if params := invalidParamsFor(err); len(params) > 0 {
		return "malformed NDJSON frame: " + paramsText(params)
	}
	return "malformed NDJSON frame"
}

// paramsText returns `name reason` for each of params, joined by "; ".
func paramsText(params []invalidParam) string {
	items := make([]string, len(params))
	for i, p := range params {
		items[i] = p.Name + " " + p.Reason
	}
	return strings.Join(items, "; ")
}

// unknownFieldText returns `unknown field "x"` for the error that
// encoding/json gives a member that DisallowUnknownFields refuses, and ""
// for any other error. That error has no type: its text is fixed (pinned by
// TestDecodeError), and it holds the member name only.
func unknownFieldText(err error) string {
	rest, ok := strings.CutPrefix(err.Error(), "json: unknown field ")
	if !ok || !strings.HasPrefix(rest, `"`) {
		return ""
	}
	if _, uerr := strconv.Unquote(rest); uerr != nil {
		return ""
	}
	return "unknown field " + rest
}

// nullMembers returns body with every value at the path field (an
// UnmarshalTypeError.Field, see memberSpans) replaced by null. It reports
// false when no value is found at the path.
func nullMembers(body []byte, field string) ([]byte, bool) {
	spans := memberSpans(body, strings.Split(field, "."))
	if len(spans) == 0 {
		return nil, false
	}
	var out bytes.Buffer
	prev := 0
	for _, sp := range spans {
		out.Write(body[prev:sp[0]])
		out.WriteString("null")
		prev = sp[1]
	}
	out.Write(body[prev:])
	return out.Bytes(), true
}

// memberSpans returns the byte ranges of the values at path in the JSON
// value b. path is an UnmarshalTypeError.Field split at each ".":
// encoding/json spells the path of a value as its JSON Pointer (RFC 6901)
// with each "/" written as ".", so a member is its name with "~" as "~0"
// and "/" as "~1", and an array element is its index. A name that holds
// "." takes several segments of path, and two values can have the same
// path (`{"a.b":1}` and `{"a":{"b":1}}`): memberSpans returns both.
// encoding/json matches a member name to a field without regard to case,
// and so does this.
func memberSpans(b []byte, path []string) [][2]int {
	dec := json.NewDecoder(bytes.NewReader(b))
	open, err := dec.Token()
	if err != nil {
		return nil
	}
	var spans [][2]int
	for index := 0; dec.More(); index++ {
		name := strconv.Itoa(index)
		if open == json.Delim('{') {
			key, err := dec.Token()
			if err != nil {
				return nil
			}
			name, _ = key.(string)
		}
		var raw json.RawMessage
		if dec.Decode(&raw) != nil {
			return nil
		}
		end := int(dec.InputOffset())
		start := end - len(raw)
		n := pathSegments(name, path)
		if n == 0 {
			continue
		}
		if n == len(path) {
			spans = append(spans, [2]int{start, end})
			continue
		}
		for _, sp := range memberSpans(raw, path[n:]) {
			spans = append(spans, [2]int{start + sp[0], start + sp[1]})
		}
	}
	return spans
}

// pointerEscaper escapes a member name as a JSON Pointer does.
var pointerEscaper = strings.NewReplacer("~", "~0", "/", "~1")

// pathSegments returns how many segments at the start of path the member
// name spells, or 0 when it does not spell them.
func pathSegments(name string, path []string) int {
	n := 0
	for seg := range strings.SplitSeq(pointerEscaper.Replace(name), ".") {
		if n == len(path) || !strings.EqualFold(seg, path[n]) {
			return 0
		}
		n++
	}
	return n
}
