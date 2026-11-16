package api

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
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
// decode names the same field with the same type again. A body with one
// wrong-type field returns the error of encoding/json unchanged. Any other
// error (syntax, a wrong top-level type) is returned unchanged.
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
		next, ok := nullMembers(work, reflect.TypeOf(dst), te)
		// null is longer than a value of 1 to 3 bytes, so the body can grow.
		if !ok || len(next) > budget || bytes.Equal(next, work) {
			break
		}
		work, budget = next, budget-len(next)
		prev := te
		te, ok = errors.AsType[*json.UnmarshalTypeError](json.Unmarshal(work, dst))
		if !ok || te.Field == "" || te.Field == prev.Field && te.Type == prev.Type {
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

// maxUnknownNameBytes bounds the member name that unknownFieldText repeats.
// The longest field name of a request body has 25 bytes, so a misspelled
// name is repeated whole. A longer name is no field name, and the body
// limit lets it reach 1 MiB: `error` and `detail` repeat it with escapes
// of up to 6 bytes for each byte, which made answers of 12 MB.
const maxUnknownNameBytes = 256

// unknownFieldText returns `unknown field "x"` for the error that
// encoding/json gives a member that DisallowUnknownFields refuses, and ""
// for any other error. That error has no type: its text is fixed (pinned by
// TestDecodeError), and it holds the member name only. A name of more than
// maxUnknownNameBytes is cut at a character to at most that many bytes, and
// ends with an ellipsis and its length: `unknown field "xx…" (1000000 bytes)`.
func unknownFieldText(err error) string {
	rest, ok := strings.CutPrefix(err.Error(), "json: unknown field ")
	if !ok || !strings.HasPrefix(rest, `"`) {
		return ""
	}
	name, uerr := strconv.Unquote(rest)
	if uerr != nil {
		return ""
	}
	if len(name) <= maxUnknownNameBytes {
		return "unknown field " + rest
	}
	cut := maxUnknownNameBytes
	for cut > 0 && !utf8.RuneStart(name[cut]) {
		cut--
	}
	return "unknown field " + strconv.Quote(name[:cut]+"…") + " (" + strconv.Itoa(len(name)) + " bytes)"
}

// nullMembers returns body with every value that the type error te names
// replaced by null; t is the type that body decodes into. It reports false
// when it finds no such value.
func nullMembers(body []byte, t reflect.Type, te *json.UnmarshalTypeError) ([]byte, bool) {
	w := spanWalk{body: body, dec: json.NewDecoder(bytes.NewReader(body))}
	if w.value(t, strings.Split(te.Field, ".")) != nil || len(w.spans) == 0 {
		return nil, false
	}
	// A map key that holds "." gives two values one path (the keys "a.b"
	// and "a" then "b"): the one that te names has the type it names. When
	// none has it (te names a map key, or comes from an UnmarshalJSON),
	// every value at the path is null.
	if slices.ContainsFunc(w.spans, func(s valueSpan) bool { return s.typ == te.Type }) {
		w.spans = slices.DeleteFunc(w.spans, func(s valueSpan) bool { return s.typ != te.Type })
	}
	var out bytes.Buffer
	prev := 0
	for _, sp := range w.spans {
		out.Write(body[prev:sp.start])
		out.WriteString("null")
		prev = sp.end
	}
	out.Write(body[prev:])
	return out.Bytes(), true
}

// valueSpan is the byte range of a value in a body, and the type (without
// pointers) that the value decodes into.
type valueSpan struct {
	start, end int
	typ        reflect.Type
}

// spanWalk finds the values at a path in one pass over a body, following
// the Go type that the body decodes into. The path is an
// UnmarshalTypeError.Field split at each ".": encoding/json spells the path
// of a value as its JSON Pointer (RFC 6901) with each "/" written as ".", so
// a member is its name with "~" as "~0" and "/" as "~1", and an array
// element is its index. A name takes one segment more than it holds ".". A
// member of a struct is the field that encoding/json decodes it into (see
// fieldFor), and members that differ in case can be one field; a member
// that is no field is on the path only as a key of the struct's `,embed`
// map (see bodyFields). A member of a map is its exact key.
// A type with its own UnmarshalJSON is walked as its fields, which is right
// for a method that decodes into them (wrefCreateBranchRequest).
type spanWalk struct {
	body  []byte
	dec   *json.Decoder
	spans []valueSpan
}

// value reads the next value of the body, which decodes into t, and adds
// the span of each value at path in it.
func (w *spanWalk) value(t reflect.Type, path []string) error {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	// The value starts after the white space, ":" or "," that follow the
	// last token.
	start := len(w.body) - len(bytes.TrimLeft(w.body[w.dec.InputOffset():], " \t\r\n:,"))
	if len(path) == 0 {
		err := w.skip()
		w.spans = append(w.spans, valueSpan{start, int(w.dec.InputOffset()), t})
		return err
	}
	switch k := t.Kind(); {
	case start == len(w.body):
	case w.body[start] == '{' && (k == reflect.Struct || k == reflect.Map):
		return w.members(t, path)
	case w.body[start] == '[' && (k == reflect.Slice || k == reflect.Array):
		return w.elements(t.Elem(), path)
	}
	return w.skip()
}

// members reads an object that decodes into the struct or map t.
func (w *spanWalk) members(t reflect.Type, path []string) error {
	st := structFields{rest: t} // a map takes every member
	if t.Kind() == reflect.Struct {
		st = bodyFields(t)
	}
	if _, err := w.dec.Token(); err != nil {
		return err
	}
	for w.dec.More() {
		tok, err := w.dec.Token()
		if err != nil {
			return err
		}
		name, _ := tok.(string)
		n := strings.Count(name, ".") + 1
		var next reflect.Type // nil: the member is not on the path
		if n <= len(path) {
			seg := strings.Join(path[:n], ".")
			if i := fieldFor(st.fields, name); i >= 0 {
				if i == fieldFor(st.fields, strings.ReplaceAll(strings.ReplaceAll(seg, "~1", "/"), "~0", "~")) {
					next = st.fields[i].typ
				}
			} else if st.rest != nil && st.rest.Kind() == reflect.Map && strings.ReplaceAll(strings.ReplaceAll(name, "~", "~0"), "/", "~1") == seg {
				next = st.rest.Elem()
			}
		}
		if next == nil {
			err = w.skip()
		} else {
			err = w.value(next, path[n:])
		}
		if err != nil {
			return err
		}
	}
	_, err := w.dec.Token()
	return err
}

// elements reads an array whose elements decode into t.
func (w *spanWalk) elements(t reflect.Type, path []string) error {
	if _, err := w.dec.Token(); err != nil {
		return err
	}
	for i := 0; w.dec.More(); i++ {
		var err error
		if strconv.Itoa(i) == path[0] {
			err = w.value(t, path[1:])
		} else {
			err = w.skip()
		}
		if err != nil {
			return err
		}
	}
	_, err := w.dec.Token()
	return err
}

// skip reads the next value of the body.
func (w *spanWalk) skip() error {
	var raw json.RawMessage
	return w.dec.Decode(&raw)
}

// bodyField is a field of a struct type that encoding/json decodes: its
// JSON name, its type, its depth (more than 0 for a field of an embedded
// struct), whether its json tag gives the name, and whether the name
// matches in its own case only (the tag option case:strict).
type bodyField struct {
	name           string
	typ            reflect.Type
	depth          int
	tagged, strict bool
}

// structFields is what bodyFields returns for a struct type.
type structFields struct {
	fields []bodyField
	rest   reflect.Type // the `,embed` map or jsontext.Value that takes the members that no field takes, or nil
}

// bodyFields returns the fields of the struct type t that encoding/json
// decodes, in breadth-first order, by the rules of encoding/json (on
// json/v2, as Go 1.27 builds it): a struct or a pointer to one that is
// embedded without a JSON name, or has the option embed and no name, adds
// its fields one level deeper, and of the fields with one name only the
// shallowest one counts, or at one depth the only one whose json tag gives
// the name. Of the fields that take the members that no field takes (see
// takesRest), the shallowest one does, unless two share its depth.
func bodyFields(t reflect.Type) structFields {
	if fields, ok := bodyFieldsCache.Load(t); ok {
		return fields.(structFields)
	}
	type embedded struct {
		t      reflect.Type
		depth  int
		follow bool // false for a type seen before: its embedded structs are not followed
	}
	queue := []embedded{{t, 0, true}}
	seen := map[reflect.Type]bool{t: true}
	var all, rests []bodyField
	for i := 0; i < len(queue); i++ {
		q := queue[i]
		for j := range q.t.NumField() {
			sf := q.t.Field(j)
			tag := sf.Tag.Get("json")
			if tag == "-" || !sf.IsExported() && !sf.Anonymous {
				continue
			}
			name, embed, strict := jsonTag(tag)
			ft := sf.Type
			if ft.Kind() == reflect.Pointer && ft.Name() == "" {
				ft = ft.Elem()
			}
			embed = name == "" && (embed || sf.Anonymous && ft.Kind() == reflect.Struct)
			switch {
			case embed && ft.Kind() == reflect.Struct:
				if q.follow {
					queue = append(queue, embedded{ft, q.depth + 1, !seen[ft]})
				}
				seen[ft] = true
			case embed && sf.IsExported() && takesRest(ft):
				rests = append(rests, bodyField{typ: ft, depth: q.depth})
			case sf.IsExported() || ft.Kind() == reflect.Struct:
				all = append(all, bodyField{cmp.Or(name, sf.Name), sf.Type, q.depth, name != "", strict})
			}
		}
	}
	var fields structFields
	for i, f := range all {
		counts := true
		for j, g := range all {
			if j != i && g.name == f.name && (g.depth < f.depth || g.depth == f.depth && (g.tagged || !f.tagged)) {
				counts = false
				break
			}
		}
		if counts {
			fields.fields = append(fields.fields, f)
		}
	}
	if len(rests) == 1 || len(rests) > 1 && rests[0].depth < rests[1].depth {
		fields.rest = rests[0].typ
	}
	bodyFieldsCache.Store(t, fields)
	return fields
}

// bodyFieldsCache holds the bodyFields of each struct type.
var bodyFieldsCache sync.Map

// takesRest reports whether encoding/json gives the members that no field
// takes to an exported field of type t (without pointers) that has the
// option embed and no name: a jsontext.Value takes them as they are, and so
// does a map whose key type is a string kind without marshal methods. Any
// other such field is a plain field.
func takesRest(t reflect.Type) bool {
	if t.PkgPath() == "encoding/json/jsontext" && t.Name() == "Value" {
		return true
	}
	return t.Kind() == reflect.Map && t.Key().Kind() == reflect.String && !slices.ContainsFunc(marshalMethods, func(m string) bool {
		_, ok := reflect.PointerTo(t.Key()).MethodByName(m)
		return ok
	})
}

// marshalMethods are the methods by which a type marshals or unmarshals
// itself in encoding/json.
var marshalMethods = []string{"MarshalJSON", "MarshalJSONTo", "AppendText", "MarshalText", "UnmarshalJSON", "UnmarshalJSONFrom", "UnmarshalText"}

// jsonTag returns the JSON name that a json struct tag gives, "" for none,
// and whether its options hold embed, and case:strict without case:ignore,
// as encoding/json reads the tag (parseFieldOptions of json/v2). The name
// ends at the first ",". When a backslash, a quote or a backtick comes
// first, the name is the run of letters, digits and "_" that starts the
// tag, and none when that run is empty or starts with a digit. Invalid
// UTF-8 in the name reads as U+FFFD, as in a member name.
func jsonTag(tag string) (name string, embed, strict bool) {
	name, opts, _ := strings.Cut(tag, ",")
	if strings.ContainsAny(name, "\\'\"`") {
		name = name[:len(name)-len(strings.TrimLeftFunc(name, func(r rune) bool {
			return r == '_' || unicode.IsLetter(r) || unicode.IsNumber(r)
		}))]
		if r, _ := utf8.DecodeRuneInString(name); unicode.IsNumber(r) {
			name = ""
		}
	}
	ignore := false
	for opt := range strings.SplitSeq(opts, ",") {
		embed = embed || opt == "embed"
		strict = strict || opt == "case:strict"
		ignore = ignore || opt == "case:ignore"
	}
	return string([]rune(name)), embed, strict && !ignore
}

// fieldFor returns the index in fields of the field that encoding/json
// decodes a member name into: the field of that name, else the first one
// whose name matches it without regard to case and has no case:strict. It
// returns -1 when no field takes the name.
func fieldFor(fields []bodyField, name string) int {
	if i := slices.IndexFunc(fields, func(f bodyField) bool { return f.name == name }); i >= 0 {
		return i
	}
	return slices.IndexFunc(fields, func(f bodyField) bool { return !f.strict && strings.EqualFold(f.name, name) })
}
