package api

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// TestStrictJSONRejectsDuplicateMembers covers issue #381: encoding/json
// lets the last duplicate member win, so strictJSON must reject one.
func TestStrictJSONRejectsDuplicateMembers(t *testing.T) {
	for name, body := range map[string]string{
		"top level":         `{"object_ref":"a","note":"x","note":null}`,
		"top level, equal":  `{"object_ref":"a","note":"x","note":"x"}`,
		"escaped duplicate": `{"object_ref":"a","note":"x","no\u0074e":"y"}`,
		"nested":            `{"object_ref":"a","note":"x","author":{"name":"a","name":"b","email":"e"}}`,
	} {
		var dst wrefNoteAppendRequest
		err := strictJSON([]byte(body), &dst)
		if err == nil || !strings.Contains(err.Error(), "duplicate field") {
			t.Errorf("%s: err = %v, want a duplicate field error", name, err)
		}
	}
	var dst wrefNoteAppendRequest
	if err := strictJSON([]byte(`{"object_ref":"a","note":"x","author":{"name":"a","email":"e"}}`), &dst); err != nil {
		t.Fatalf("valid body: %v", err)
	}
	if dst.Note != "x" || dst.Author == nil || dst.Author.Name != "a" {
		t.Fatalf("decoded %+v", dst)
	}
}

// TestStrictJSONAssertsOnUncheckedStructElements covers issue #381:
// checkStrictObject does not recurse into slices or maps, so a schema
// whose slice or map holds structs must fail loudly instead of skipping
// the strict checks for its elements.
func TestStrictJSONAssertsOnUncheckedStructElements(t *testing.T) {
	type item struct {
		Name string `json:"name"`
	}
	for name, dst := range map[string]any{
		"slice of structs": &struct {
			Items []item `json:"items"`
		}{},
		"slice of struct pointers": &struct {
			Items []*item `json:"items"`
		}{},
		"map of structs": &struct {
			Items map[string]item `json:"items"`
		}{},
	} {
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("%s: strictJSON did not panic", name)
				}
			}()
			_ = strictJSON([]byte(`{}`), dst)
		}()
	}
	// Slices of scalars stay supported.
	var ok struct {
		Tags []string `json:"tags"`
	}
	if err := strictJSON([]byte(`{"tags":["a"]}`), &ok); err != nil || len(ok.Tags) != 1 {
		t.Fatalf("scalar slice: err %v, got %v", err, ok.Tags)
	}
}

// strictStructElemFields returns the fields of struct type rt, and of the
// structs it nests directly or through pointers, whose slice, array, or
// map elements are structs: checkStrictObject does not check those.
func strictStructElemFields(rt reflect.Type, path string) []string {
	var bad []string
	for i := range rt.NumField() {
		f := rt.Field(i)
		ft, p := f.Type, path+"."+f.Name
		switch ft.Kind() {
		case reflect.Slice, reflect.Array, reflect.Map:
			et := ft.Elem()
			if et.Kind() == reflect.Pointer {
				et = et.Elem()
			}
			if et.Kind() == reflect.Struct {
				bad = append(bad, p)
			}
		case reflect.Pointer:
			if ft.Elem().Kind() == reflect.Struct {
				bad = append(bad, strictStructElemFields(ft.Elem(), p)...)
			}
		case reflect.Struct:
			bad = append(bad, strictStructElemFields(ft, p)...)
		}
	}
	return bad
}

// TestStrictRequestTypesHaveNoStructElements covers issue #385. The
// checkStrictObject assertion fires only when a body decodes, and for a
// nested struct only when that member is present. This walks every
// strictJSON destination type once, so a new slice-of-struct field fails
// here even when no test sends it.
func TestStrictRequestTypesHaveNoStructElements(t *testing.T) {
	type item struct {
		Name string `json:"name"`
	}
	type nested struct {
		Inner *struct {
			Items map[string]*item `json:"items"`
		} `json:"inner"`
	}
	if got := strictStructElemFields(reflect.TypeFor[nested](), "nested"); len(got) != 1 || got[0] != "nested.Inner.Items" {
		t.Fatalf("walker control: got %v, want [nested.Inner.Items]", got)
	}

	// Every strictJSON destination type. The use count below fails when
	// a strict decode is added, so add its type here too.
	for _, rt := range []reflect.Type{
		reflect.TypeFor[wrefNoteAppendRequest](),
		reflect.TypeFor[usyncPullUpstreamRequest](),
	} {
		if bad := strictStructElemFields(rt, rt.Name()); len(bad) != 0 {
			t.Errorf("%s: fields %v hold structs, which checkStrictObject does not check", rt, bad)
		}
	}

	// Uses of strictJSON and wrefDecodeStrictBody outside their own
	// declarations: one per destination type above, plus the call inside
	// wrefDecodeStrictBody.
	fset := token.NewFileSet()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	uses := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			if fd, ok := n.(*ast.FuncDecl); ok && fd.Body != nil {
				ast.Inspect(fd.Body, func(n ast.Node) bool {
					if id, ok := n.(*ast.Ident); ok && (id.Name == "strictJSON" || id.Name == "wrefDecodeStrictBody") {
						uses++
					}
					return true
				})
				return false
			}
			return true
		})
	}
	if uses != 3 {
		t.Fatalf("found %d uses of strictJSON/wrefDecodeStrictBody, want 3: update the type list above", uses)
	}
}
