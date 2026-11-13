package api

import (
	"encoding/json"
	"errors"
	"fmt"
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
		{name: "member names match without regard to case", body: `{"STRATEGY":5,"Count":"x"}`,
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

func TestUnmarshalBodyJoinsMessages(t *testing.T) {
	err := unmarshalBody([]byte(`{"strategy":5,"count":"x"}`), new(decodeBodyReq))
	parts := strings.Split(err.Error(), "; ")
	if len(parts) != 2 || !strings.Contains(parts[0], "strategy") || !strings.Contains(parts[1], "count") {
		t.Fatalf("message = %q", err)
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
	// The first decode counts too: a body of more than 128 KiB names only
	// its first wrong-type field, and one of 126 KiB names two.
	for _, tc := range []struct{ entries, want int }{{70_000, 1}, {63_000, 2}} {
		body := `{"paths":[` + strings.Repeat("7,", tc.entries) + `7]}`
		if got := len(invalidParamsFor(unmarshalBody([]byte(body), new(decodeBodyReq)))); got != tc.want {
			t.Fatalf("%d bytes named %d items, want %d", len(body), got, tc.want)
		}
	}
}
