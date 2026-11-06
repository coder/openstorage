package api

// #587 guard: a JSON field that carries a repository path has a _base64
// sibling (rawpath.go). The scan reads the package's own struct types, so a
// new path field without a sibling fails here, in the PR that adds it.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"strings"
	"testing"
)

// rpGuardExempt lists path fields without a sibling, as Type.json_name. The
// first two take a path in a request body, which has no response sibling to
// fill. The rest are merge responses, which the merge change covers next.
var rpGuardExempt = map[string]string{
	"grepRequestBody.paths":            "grep request input",
	"wcFileOp.path":                    "commit-pack request input",
	"wmConflictBody.conflict_paths":    "merge 409 body",
	"wmPreviewConflict.path":           "merge preview",
	"wmPreviewFiltered.path":           "merge preview",
	"wmPreviewResponse.conflict_paths": "merge preview",
}

// isPathFieldName reports whether a JSON name carries a repository path.
func isPathFieldName(name string) bool {
	return name == "path" || name == "paths" || strings.HasSuffix(name, "_path") || strings.HasSuffix(name, "_paths")
}

// rawPathViolations returns one message per path field in src that has no
// valid sibling, and how many path fields it checked.
func rawPathViolations(t *testing.T, filename, src string, exempt map[string]string) (violations []string, checked int) {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filename, src, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", filename, err)
	}
	for _, decl := range f.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range gen.Specs {
			ts, ok := spec.(*ast.TypeSpec)
			if !ok {
				continue
			}
			// Anonymous structs inside the type belong to its name.
			ast.Inspect(ts.Type, func(n ast.Node) bool {
				st, ok := n.(*ast.StructType)
				if !ok {
					return true
				}
				v, c := structPathViolations(fset, ts.Name.Name, st, exempt)
				violations, checked = append(violations, v...), checked+c
				return true
			})
		}
	}
	return violations, checked
}

type jsonField struct {
	name string
	opts string
	kind string // "string", "[]string" or "other"
	pos  token.Pos
}

func structJSONFields(st *ast.StructType) map[string]jsonField {
	out := map[string]jsonField{}
	for _, field := range st.Fields.List {
		if field.Tag == nil || len(field.Names) != 1 {
			continue
		}
		tag := reflect.StructTag(strings.Trim(field.Tag.Value, "`")).Get("json")
		name, opts, _ := strings.Cut(tag, ",")
		if name == "" || name == "-" {
			continue
		}
		kind := "other"
		switch ft := field.Type.(type) {
		case *ast.Ident:
			if ft.Name == "string" {
				kind = "string"
			}
		case *ast.ArrayType:
			if elt, ok := ft.Elt.(*ast.Ident); ok && ft.Len == nil && elt.Name == "string" {
				kind = "[]string"
			}
		}
		out[name] = jsonField{name: name, opts: opts, kind: kind, pos: field.Pos()}
	}
	return out
}

func structPathViolations(fset *token.FileSet, typeName string, st *ast.StructType, exempt map[string]string) (violations []string, checked int) {
	fields := structJSONFields(st)
	for name, f := range fields {
		if !isPathFieldName(name) || f.kind == "other" || exempt[typeName+"."+name] != "" {
			continue
		}
		checked++
		where := fset.Position(f.pos).String() + ": " + typeName + "." + name
		sib, ok := fields[name+"_base64"]
		switch {
		case !ok:
			violations = append(violations, where+" has no "+name+"_base64 sibling")
		case sib.kind != f.kind:
			violations = append(violations, where+": sibling is "+sib.kind+", want "+f.kind)
		case !strings.Contains(","+sib.opts+",", ",omitempty,") && !strings.Contains(","+sib.opts+",", ",omitzero,"):
			violations = append(violations, where+": sibling is not omitempty or omitzero")
		}
	}
	return violations, checked
}

func TestEveryResponsePathFieldHasABase64Sibling(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var all []string
	checked := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		v, c := rawPathViolations(t, name, string(src), rpGuardExempt)
		all, checked = append(all, v...), checked+c
	}
	for _, v := range all {
		t.Error(v)
	}
	if checked < 10 {
		t.Errorf("the scan checked %d path fields, want at least 10: it reads too little", checked)
	}
}

// The scan has teeth: it names each way a path field can lack its sibling.
func TestRawPathGuardFindsMissingSiblings(t *testing.T) {
	const src = "package x\n" +
		"type ok struct {\n" +
		"\tPath string `json:\"path\"`\n\tPathBase64 string `json:\"path_base64,omitempty\"`\n" +
		"\tPaths []string `json:\"conflict_paths\"`\n\tPathsBase64 []string `json:\"conflict_paths_base64,omitzero\"`\n" +
		"\tNested struct {\n\t\tOld string `json:\"old_path\"`\n\t\tOldBase64 string `json:\"old_path_base64,omitempty\"`\n\t} `json:\"nested\"`\n" +
		"}\n" +
		"type missing struct {\n\tPath string `json:\"path\"`\n}\n" +
		"type missingList struct {\n\tPaths []string `json:\"paths\"`\n}\n" +
		"type wrongKind struct {\n\tPath string `json:\"path\"`\n\tPathBase64 []string `json:\"path_base64,omitempty\"`\n}\n" +
		"type alwaysPresent struct {\n\tPath string `json:\"original_path\"`\n\tPathBase64 string `json:\"original_path_base64\"`\n}\n" +
		"type nestedMissing struct {\n\tInner struct {\n\t\tP string `json:\"old_path\"`\n\t} `json:\"inner\"`\n}\n" +
		"type notAPath struct {\n\tName string `json:\"name\"`\n\tCount int `json:\"path\"`\n}\n" +
		"type exempt struct {\n\tPaths []string `json:\"paths\"`\n}\n"
	got, checked := rawPathViolations(t, "x.go", src, map[string]string{"exempt.paths": "request input"})
	for _, want := range []string{
		"missing.path has no path_base64 sibling",
		"missingList.paths has no paths_base64 sibling",
		"wrongKind.path: sibling is []string, want string",
		"alwaysPresent.original_path: sibling is not omitempty or omitzero",
		"nestedMissing.old_path has no old_path_base64 sibling",
	} {
		if !slicesContainsSuffix(got, want) {
			t.Errorf("no violation %q in %q", want, got)
		}
	}
	// ok holds 3 path fields (one nested) and 5 types above hold 1 each.
	if len(got) != 5 || checked != 8 {
		t.Errorf("violations %d (%q), checked %d, want 5 and 8", len(got), got, checked)
	}
}

func slicesContainsSuffix(list []string, suffix string) bool {
	for _, s := range list {
		if strings.HasSuffix(s, suffix) {
			return true
		}
	}
	return false
}
