package api

import (
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
)

type decodeAuthor struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

type decodeBodyReq struct {
	Strategy string          `json:"strategy"`
	Count    int             `json:"count"`
	Flag     bool            `json:"flag"`
	Author   *decodeAuthor   `json:"author"`
	Paths    []string        `json:"paths"`
	Items    []decodeAuthor  `json:"items"`
	Raw      json.RawMessage `json:"raw"`
}

func TestUnmarshalBody(t *testing.T) {
	type param = invalidParam
	for _, tc := range []struct {
		name, body string
		wantParams []param
		// wantSame: the error must be exactly what json.Unmarshal returns.
		wantSame bool
	}{
		{name: "valid", body: `{"strategy":"merge","count":3}`},
		{name: "one wrong-type field is json's own error", body: `{"strategy":5,"count":3}`,
			wantParams: []param{{"strategy", "must be a string"}}, wantSame: true},
		{name: "a nested wrong-type field", body: `{"author":{"name":1,"email":"e"}}`,
			wantParams: []param{{"author.name", "must be a string"}}, wantSame: true},
		{name: "every wrong-type field, in body order", body: `{"count":"x","strategy":5,"author":{"email":[],"name":true},"flag":"no"}`,
			wantParams: []param{{"count", "must be an integer"}, {"strategy", "must be a string"},
				{"author.email", "must be a string"}, {"author.name", "must be a string"}, {"flag", "must be a boolean"}}},
		{name: "array elements by index", body: `{"paths":["a",1,"b",{},null,true]}`,
			wantParams: []param{{"paths.1", "must be a string"}, {"paths.3", "must be a string"}, {"paths.5", "must be a string"}}},
		{name: "objects inside an array", body: `{"items":[{"name":1},{"name":"ok"},{"email":2,"name":3}]}`,
			wantParams: []param{{"items.0.name", "must be a string"}, {"items.2.email", "must be a string"}, {"items.2.name", "must be a string"}}},
		{name: "member names match without regard to case", body: `{"STRATEGY":5,"Count":"x","strategy":6}`,
			wantParams: []param{{"STRATEGY", "must be a string"}, {"Count", "must be an integer"}}},
		{name: "white space around the members", body: "{ \"strategy\" :\n\t5 ,\r\n \"count\" : \"x\" , \"paths\" : [ 1 ,\n \"a\", 2 ] }",
			wantParams: []param{{"strategy", "must be a string"}, {"count", "must be an integer"}, {"paths.0", "must be a string"}, {"paths.2", "must be a string"}}},
		{name: "escaped member names", body: `{"\u0073trategy":5,"\u0063ount":"x"}`,
			wantParams: []param{{"strategy", "must be a string"}, {"count", "must be an integer"}}},
		{name: "a duplicated member counts once", body: `{"strategy":5,"count":1,"strategy":6}`,
			wantParams: []param{{"strategy", "must be a string"}}, wantSame: true},
		{name: "a whole object of the wrong type", body: `{"author":"x","strategy":5}`,
			wantParams: []param{{"author", "must be an object"}, {"strategy", "must be a string"}}},
		{name: "a raw member is never a type error", body: `{"raw":5,"strategy":5}`,
			wantParams: []param{{"strategy", "must be a string"}}, wantSame: true},
		{name: "a syntax error", body: `{"strategy":5`},
		{name: "a wrong top-level type", body: `[1,2]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got decodeBodyReq
			err := unmarshalBody([]byte(tc.body), &got)
			var want decodeBodyReq
			base := json.Unmarshal([]byte(tc.body), &want)
			if tc.wantParams == nil {
				if (err == nil) != (base == nil) || (err != nil && err.Error() != base.Error()) {
					t.Fatalf("err = %v, json.Unmarshal says %v", err, base)
				}
				return
			}
			if err == nil {
				t.Fatalf("no error, want %+v", tc.wantParams)
			}
			if tc.wantSame && err.Error() != base.Error() {
				t.Fatalf("err = %q, json.Unmarshal says %q", err, base)
			}
			items := invalidParamsFor(err)
			if fmt.Sprint(items) != fmt.Sprint(tc.wantParams) {
				t.Fatalf("items = %+v, want %+v (err %q)", items, tc.wantParams, err)
			}
			if _, ok := errors.AsType[*json.UnmarshalTypeError](err); ok != tc.wantSame {
				t.Fatalf("err %T: a lone json error is expected exactly when wantSame (%v)", err, tc.wantSame)
			}
		})
	}
}

func TestUnmarshalBodyBounds(t *testing.T) {
	// 150 wrong-type array entries: the answer holds 100 items.
	var b strings.Builder
	b.WriteString(`{"paths":[`)
	for i := range 150 {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString("7")
	}
	b.WriteString(`]}`)
	err := unmarshalBody([]byte(b.String()), new(decodeBodyReq))
	items := invalidParamsFor(err)
	if len(items) != maxInvalidParams || items[0].Name != "paths.0" || items[len(items)-1].Name != fmt.Sprintf("paths.%d", maxInvalidParams-1) {
		t.Fatalf("got %d items, first %v, last %v", len(items), items[0], items[len(items)-1])
	}

	// A body past the work bound names its first wrong-type field only,
	// and the answer is json's own error.
	big := `{"paths":[` + strings.Repeat("7,", 400_000) + `7]}`
	err = unmarshalBody([]byte(big), new(decodeBodyReq))
	if _, ok := errors.AsType[*json.UnmarshalTypeError](err); !ok || len(invalidParamsFor(err)) != 1 {
		t.Fatalf("a large body: err %T %q", err, err)
	}

	// The work bound is pinned in bytes: 10 KiB of wrong-type entries names
	// fewer than 100 of them, and 2 KiB names 100.
	for _, tc := range []struct{ entries, want int }{{500, 100}, {5000, 26}} {
		body := `{"paths":[` + strings.Repeat("7,", tc.entries) + `7]}`
		got := len(invalidParamsFor(unmarshalBody([]byte(body), new(decodeBodyReq))))
		if got < tc.want-2 || got > tc.want+2 {
			t.Fatalf("%d entries (%d bytes) named %d items, want about %d", tc.entries, len(body), got, tc.want)
		}
	}
	// The bound is hard: nulling many short values makes the body longer
	// (7 becomes null), and a decode that the budget cannot pay for does not run.
	// The count field comes after the repeated members: a decode of the longer
	// body would name it.
	grown := `{"strategy":5,` + strings.Repeat(`"flag":7,`, 9500) + `"count":"x"}`
	got := invalidParamsFor(unmarshalBody([]byte(grown), new(decodeBodyReq)))
	if len(got) != 2 || got[0].Name != "strategy" || got[1].Name != "flag" {
		t.Fatalf("a body that grows past the work bound named %+v, want strategy and flag", got)
	}
	// The search for the member to replace is as long as the body, so the
	// budget must cover it before the search starts, even when the member is
	// long and the body gets short once it is null: this body names only
	// its first wrong-type member.
	long := `{"paths":"` + strings.Repeat("x", 135_000) + `","strategy":1}`
	if got := invalidParamsFor(unmarshalBody([]byte(long), new(decodeBodyReq))); len(got) != 1 || got[0].Name != "paths" {
		t.Fatalf("a body of %d bytes with a long first member named %+v, want paths only", len(long), got)
	}
	// A member that is no field is never the wrong-type value, even when its
	// name spells the path of one, so nulling it cannot change how many
	// fields the work bound lets a body name.
	for _, unknown := range []string{`"K":"` + strings.Repeat("x", 60<<10) + `"`, strings.Repeat(`"K":1,`, 499) + `"K":1`} {
		named := func(name string) int {
			body := `{"paths":[` + strings.Repeat("7,", 149) + `7],` + strings.ReplaceAll(unknown, `"K"`, `"`+name+`"`) + `}`
			return len(invalidParamsFor(unmarshalBody([]byte(body), new(decodeBodyReq))))
		}
		if got, want := named("paths.0"), named("paths_0"); got != want {
			t.Fatalf("with unknown members paths.0 the body named %d items, with paths_0 %d", got, want)
		}
	}
	// The first decode counts too: a body of more than 128 KiB names only
	// its first wrong-type field, and one of 126 KiB names two.
	for _, tc := range []struct{ entries, want int }{{70_000, 1}, {63_000, 2}} {
		body := `{"paths":[` + strings.Repeat("7,", tc.entries) + `7]}`
		if got := len(invalidParamsFor(unmarshalBody([]byte(body), new(decodeBodyReq)))); got != tc.want {
			t.Fatalf("%d bytes named %d items, want %d", len(body), got, tc.want)
		}
	}
}

// TestDecodeError: the message of a decode failure holds no text of
// encoding/json, whatever the failure (issue #841). A field of the wrong
// type is `name reason`, as in invalid_params, and a member that
// DisallowUnknownFields refuses is named; the text of that error is fixed in
// encoding/json, so this test also fails when a Go release changes it.
func TestDecodeError(t *testing.T) {
	type param = invalidParam
	strictDecode := func(body []byte, dst any) error {
		dec := json.NewDecoder(strings.NewReader(string(body)))
		dec.DisallowUnknownFields()
		return dec.Decode(dst)
	}
	for _, tc := range []struct {
		name, member, body string
		decode             func([]byte, any) error
		want               string
		wantParams         []param
	}{
		{name: "one field", body: `{"strategy":5}`, want: "strategy must be a string", wantParams: []param{{"strategy", "must be a string"}}},
		{name: "a nested field", body: `{"author":{"name":1}}`, want: "author.name must be a string", wantParams: []param{{"author.name", "must be a string"}}},
		{name: "an array element", body: `{"paths":["a",1]}`, want: "paths.1 must be a string", wantParams: []param{{"paths.1", "must be a string"}}},
		{name: "several fields", body: `{"count":"x","flag":1}`, decode: unmarshalBody, want: "count must be an integer; flag must be a boolean",
			wantParams: []param{{"count", "must be an integer"}, {"flag", "must be a boolean"}}},
		{name: "a syntax error", body: `{"strategy":`, want: "invalid JSON body"},
		{name: "trailing bytes", body: `{} x`, want: "invalid JSON body"},
		{name: "a wrong top-level type", body: `[1]`, want: "invalid JSON body"},
		{name: "an unknown field", body: `{"x":1}`, decode: strictDecode, want: `invalid JSON body: unknown field "x"`},
		{name: "an unknown field with a name that needs quoting", body: `{"a\"b\n\u00e9\u0000":1}`, decode: strictDecode, want: `invalid JSON body: unknown field "a\"b\né\x00"`},
		{name: "a wrong type before an unknown field", body: `{"strategy":5,"x":1}`, decode: strictDecode, want: "strategy must be a string", wantParams: []param{{"strategy", "must be a string"}}},
		{name: "an unknown field before a wrong type", body: `{"x":1,"strategy":5}`, decode: strictDecode, want: `invalid JSON body: unknown field "x"`},
		{name: "a member", member: "upstream", body: `{"strategy":5}`, want: "upstream.strategy must be a string", wantParams: []param{{"upstream.strategy", "must be a string"}}},
		{name: "a member that is not an object", member: "base_repo", body: `"x"`, want: "base_repo must be an object", wantParams: []param{{"base_repo", "must be an object"}}},
		{name: "a member with an unknown field", member: "base_repo", body: `{"reff":1}`, decode: strictDecode, want: `invalid base_repo: unknown field "reff"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			decode := tc.decode
			if decode == nil {
				decode = func(b []byte, dst any) error { return json.Unmarshal(b, dst) }
			}
			err := decode([]byte(tc.body), new(decodeBodyReq))
			if err == nil {
				t.Fatal("the body decodes")
			}
			got := decodeError(tc.member, err)
			if got.Error() != tc.want {
				t.Fatalf("message = %q, want %q (decoder: %q)", got, tc.want, err)
			}
			if params := invalidParamsFor(got); fmt.Sprint(params) != fmt.Sprint(tc.wantParams) {
				t.Fatalf("invalid_params = %+v, want %+v", params, tc.wantParams)
			}
		})
	}
	// Only the text of the unknown-field error counts: a look-alike does not.
	for _, text := range []string{"json: unknown field x", `json: unknown field "x`, "unknown field \"x\"", "json: unknown field `x`", "json: unknown field 'x'"} {
		if got := decodeError("", errors.New(text)).Error(); got != "invalid JSON body" {
			t.Errorf("error %q: message = %q, want the generic one", text, got)
		}
	}
	if got := frameMsg(json.Unmarshal([]byte(`{"strategy":5}`), new(decodeBodyReq))); got != "malformed NDJSON frame: strategy must be a string" {
		t.Errorf("frame with a wrong type: %q", got)
	}
	if got := frameMsg(json.Unmarshal([]byte(`{`), new(decodeBodyReq))); got != "malformed NDJSON frame" {
		t.Errorf("frame with a syntax error: %q", got)
	}
}

// decodePathReq has members that encoding/json names by their JSON
// Pointer with each "/" written as ".": map keys that hold "." (kept as
// is), "/" (written ~1) or "~" (written ~0), and members that null does
// not fix. It has fields of embedded structs, and a field X in two of them,
// which encoding/json drops, so that a member "X" is the field x. Each
// map from Dp on is the field that encoding/json decodes a member into by
// one of its field rules, beside a field of another type that the member
// does not reach: the walk stops when it takes that one.
type decodePathReq struct {
	Decodes decodeCount               `json:"n"`
	M       map[string]int            `json:"m"`
	Nested  map[string]map[string]int `json:"nested"`
	Keys    map[int]int               `json:"keys"`
	Strict  refusesNull               `json:"strict"`
	Count   int                       `json:"count"`
	Dotted  int                       `json:"a.b/c"`
	Lower   int                       `json:"x"`
	decodeEmbedded
	*DecodePtrEmbedded
	Dp          map[string]int `json:"dp"`
	Skip        int            `json:"-"`
	Dash        map[string]int `json:"-,"` //nolint:staticcheck // the name "-"
	Ex0         int            `json:"Ex"`
	Ex          map[string]int `json:"ex"`
	Fo          map[string]int `json:"fo"`
	Fo1         int            `json:"Fo"`
	Op          map[string]int `json:"op,omitempty"`
	Tilde       map[string]int `json:"t~1"`
	decodeNamed `json:"named"`
	decodeLevel                 //nolint:unused // encoding/json skips it
	decodelevel struct{ Q int } //nolint:unused // encoding/json skips it
	Level       map[string]int  `json:"DecodeLevel"`
	Lf          map[string]int  `json:"LF"`
	Backslash   map[string]int  `json:"bs\\x"` //nolint:staticcheck // the name "bs"
	Nine        map[string]int  `json:"9\\x"`  //nolint:staticcheck // no name
	Quote       int             `json:"'q'"`
	QuoteK      map[string]int  `json:"Quote"`
	Utf0        int             `json:"U\ufffdV"`
	Utf         map[string]int  `json:"u\xffv"` //nolint:staticcheck // the name "u\ufffdv"
	Cs          int             `json:"cs,case:strict"`
	Ce          map[string]int  `json:"ce,case:strict"`
	CsK         map[string]int  `json:"Cs"`
	Ci          map[string]int  `json:"ci,case:strict,case:ignore"` //nolint:staticcheck // not strict
	Inline      struct {
		In map[string]int `json:"in"`
	} `json:",embed"`
	Rest struct {
		K    int            `json:"k"`
		More map[string]int `json:",embed"` // not the deeper Less
		DecodeDeeper
	} `json:"rest"`
	Raw  jsontext.Value    `json:",embed"`
	RawK map[string]int    `json:"raw"`
	KM   map[decodeKey]int `json:",embed"` // plain fields: no string keys
	IK   map[int]int       `json:",embed"` // or keys that unmarshal
}

type decodeKey string

func (k *decodeKey) UnmarshalText(b []byte) error { *k = decodeKey(b); return nil }

type decodeEmbedded struct {
	E  int `json:"e"`
	X  int
	Dp int            `json:"dp"`
	Tg map[string]int `json:"Tg"`
	DecodeLeaf
}

type DecodePtrEmbedded struct {
	P  int `json:"p"`
	X  int
	Tg int
	DecodeLeaf
}

// DecodeLeaf is embedded twice at one depth, so encoding/json drops lf.
type DecodeLeaf struct {
	Lf int `json:"lf"`
	*DecodeLeaf
}

type DecodeDeeper struct {
	Less map[string]bool `json:",embed"`
}

type decodeNamed struct {
	K int `json:"k"`
}

type decodeLevel int //nolint:unused // embedded in decodePathReq

// decodeCount counts the decodes of the body that holds it.
type decodeCount int

func (c *decodeCount) UnmarshalJSON([]byte) error { *c++; return nil }

// refusesNull takes a JSON string only. Like a hand-written UnmarshalJSON
// can, it refuses null too, with a type error that names its member.
type refusesNull struct{}

func (*refusesNull) UnmarshalJSON(b []byte) error {
	if b[0] == '"' {
		return nil
	}
	return &json.UnmarshalTypeError{Value: "number", Type: reflect.TypeFor[string](), Field: "strict"}
}

// TestUnmarshalBodyPaths: unmarshalBody names each wrong-type field
// whatever its member names hold, and decodes the body once for each error
// of encoding/json and once more. A struct member is the field that
// encoding/json decodes it into, a map member its exact key; of two values
// with one path, the wrong one is the one of the type that the error names.
// When null does not fix a field (a map key that does not decode, an
// UnmarshalJSON that refuses null), it names that field, with the error of
// encoding/json as for any lone field, and stops: at once when the field is
// null already.
func TestUnmarshalBodyPaths(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		want       []string
		decodes    int
	}{
		{"a key with a dot", `{"m":{"a.b":"x","c":"y"},"count":"z"}`, []string{"m.a.b", "m.c", "count"}, 4},
		{"a key with a slash", `{"m":{"a/b":"x","c":"y"}}`, []string{"m.a~1b", "m.c"}, 3},
		{"a key with a tilde", `{"m":{"a~b":"x","c":"y"}}`, []string{"m.a~0b", "m.c"}, 3},
		{"a key that reads as an escaped slash", `{"m":{"a/b":"x","a~1b":"y","c":"z"}}`, []string{"m.a~1b", "m.a~01b", "m.c"}, 4},
		{"nested maps", `{"nested":{"a.b":{"c/d":"x"},"e":{"f":"y"}},"count":"z"}`, []string{"nested.a.b.c~1d", "nested.e.f", "count"}, 4},
		{"two values with one path", `{"nested":{"a.b":{"c":"x"},"a":{"b.c":"y"}},"count":"z"}`, []string{"nested.a.b.c", "count"}, 3},
		{"a map key that does not decode", `{"keys":{"x":1}}`, []string{"keys.x"}, 2},
		{"a map key that does not decode, null already", `{"keys":{"x":null}}`, []string{"keys.x"}, 1},
		{"an UnmarshalJSON that refuses null", `{"strict":5}`, []string{"strict"}, 2},
		{"a longer key than the path", `{"m":{"a":"x","a.b":1}}`, []string{"m.a"}, 2},
		{"a map key with the name of a field", `{"count":"z","m":{"count":"y"}}`, []string{"count", "m.count"}, 3},
		{"map keys that differ in case", `{"m":{"A":"x","a":"y"}}`, []string{"m.A", "m.a"}, 3},
		{"a key with a dot in another case", `{"nested":{"A":{"b":{}},"a.b":7}}`, []string{"nested.A.b", "nested.a.b"}, 3},
		{"a value with the path of a wrong one", `{"nested":{"a":{"b":"x"},"a.b":{"x":"y"}}}`, []string{"nested.a.b", "nested.a.b.x"}, 3},
		{"two wrong values with one path", `{"nested":{"a":{"b":"x"},"a.b":5},"count":"z"}`, []string{"nested.a.b", "count"}, 4},
		{"fields of embedded structs", `{"e":"x","p":"y","count":"z"}`, []string{"e", "p", "count"}, 4},
		{"a field that two embedded structs hide", `{"X":"a","x":"b"}`, []string{"X"}, 2},
		{"a field name with a dot and a slash", `{"a.b/c":"x","A.B/C":"y","count":"z"}`, []string{"a.b~1c", "count"}, 3},
		{"the shallower or only tagged field of a name, a dash", `{"dp":{"k":"x"},"Tg":{"k":"y"},"-":{"k":"w"},"count":"z"}`, []string{"dp.k", "Tg.k", "-.k", "count"}, 5},
		{"the exact name, then the first in another case", `{"ex":{"k":"x"},"FO":{"k":"y"},"count":"z"}`, []string{"ex.k", "FO.k", "count"}, 4},
		{"options, a named embedded struct, a name with ~1", `{"op":{"k":"x"},"named":{"k":"y"},"t~1":{"k":"w"},"count":"z"}`, []string{"op.k", "named.k", "t~01.k", "count"}, 5},
		{"an embedded int that is not exported, a dropped name", `{"DECODELEVEL":{"k":"x"},"lf":{"k":"y"},"count":"z"}`, []string{"DECODELEVEL.k", "lf.k", "count"}, 4},
		{"a tag name with a backslash, a quote, bad UTF-8", `{"bs":{"k":"x"},"Nine":{"k":"v"},"Quote":{"k":"y"},"u\ufffdv":{"k":"w"},"count":"z"}`, []string{"bs.k", "Nine.k", "Quote.k", "u\ufffdv.k", "count"}, 6},
		{"case:strict", `{"CS":{"k":"x"},"ce":{"k":"v"},"CI":{"k":"y"},"count":"z"}`, []string{"CS.k", "ce.k", "CI.k", "count"}, 5},
		{",embed", `{"in":{"k":"x"},"rest":{"zz":"y","k":"w"},"count":"z"}`, []string{"in.k", "rest.zz", "rest.k", "count"}, 5},
		{",embed jsontext.Value, maps without string keys", `{"Raw":{"k":"x"},"KM":{"k":"y"},"IK":{"7":"w"},"count":"z"}`, []string{"Raw.k", "KM.k", "IK.7", "count"}, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var dst decodePathReq
			err := unmarshalBody([]byte(`{"n":0,`+tc.body[1:]), &dst) // n counts the decodes
			var got []string
			for _, p := range invalidParamsFor(err) {
				got = append(got, p.Name)
			}
			_, lone := errors.AsType[*json.UnmarshalTypeError](err)
			if !slices.Equal(got, tc.want) || int(dst.Decodes) != tc.decodes || lone != (len(tc.want) == 1) {
				t.Fatalf("named %q in %d decodes (error %T), want %q in %d", got, dst.Decodes, err, tc.want, tc.decodes)
			}
		})
	}
}
