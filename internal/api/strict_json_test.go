package api

import (
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
