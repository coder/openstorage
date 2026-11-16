package gitread

import (
	"slices"
	"strings"
	"testing"
)

// TestParseRaw reads `diff --raw --no-abbrev -z` records: each file's
// object is its new side, or its old side for a deletion, and any record
// git would never print is an error.
func TestParseRaw(t *testing.T) {
	a, b, z := strings.Repeat("a", 40), strings.Repeat("b", 40), strings.Repeat("0", 40)
	rec := func(head string, paths ...string) string {
		return ":" + head + "\x00" + strings.Join(paths, "\x00") + "\x00"
	}
	got, err := parseRaw(rec("000000 100644 "+z+" "+a+" A", "new.txt") +
		rec("100644 000000 "+a+" "+z+" D", "gone\nname") +
		rec("100644 100755 "+a+" "+b+" M", "mode.sh") +
		rec("120000 100644 "+a+" "+b+" T", "link") +
		rec("160000 000000 "+a+" "+z+" D", "sub") +
		rec("100644 100644 "+a+" "+a+" R100", "old name", "new name") +
		rec("100644 100644 "+a+" "+b+" C075", "src", "copy"))
	if err != nil {
		t.Fatal(err)
	}
	want := []FileDiff{
		{Status: "A", Path: "new.txt", Mode: "100644", OID: a},
		{Status: "D", Path: "gone\nname", Mode: "100644", OID: a},
		{Status: "M", Path: "mode.sh", Mode: "100755", OID: b},
		{Status: "T", Path: "link", Mode: "100644", OID: b},
		{Status: "D", Path: "sub", Mode: "160000", OID: a},
		{Status: "R", OldPath: "old name", Path: "new name", Mode: "100644", OID: a},
		{Status: "C", OldPath: "src", Path: "copy", Mode: "100644", OID: b},
	}
	if !slices.EqualFunc(got, want, func(x, y FileDiff) bool {
		return x.Status == y.Status && x.Path == y.Path && x.OldPath == y.OldPath && x.Mode == y.Mode && x.OID == y.OID
	}) {
		t.Fatalf("parseRaw = %+v, want %+v", got, want)
	}
	if got, err := parseRaw(""); err != nil || len(got) != 0 {
		t.Fatalf("parseRaw(\"\") = %v, %v; want no files", got, err)
	}

	for name, out := range map[string]string{
		"no colon":         "100644 100644 " + a + " " + b + " M\x00f\x00",
		"four fields":      rec("100644 "+a+" "+b+" M", "f"),
		"short object id":  rec("100644 100644 "+a[:7]+" "+b+" M", "f"),
		"no path":          ":100644 100644 " + a + " " + b + " M\x00",
		"rename one path":  rec("100644 100644 "+a+" "+a+" R100", "old"),
		"unmerged":         rec("100644 100644 "+a+" "+b+" U", "f"),
		"empty status":     rec("100644 100644 "+a+" "+b+" ", "f"),
		"name-status form": "M\x00f\x00",
	} {
		if _, err := parseRaw(out); err == nil {
			t.Errorf("%s: parseRaw(%q) = nil error, want one", name, out)
		}
	}
}
