package api

// #587 guard: a JSON field that carries a repository path or a path glob has
// a _base64 sibling (rawpath.go). The scan reads every struct type in the
// package's own sources, so a new such field without a sibling fails here, in
// the PR that adds it. It does not check that a handler fills the sibling:
// the route tests do.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// isPathFieldName reports whether a JSON name carries a repository path or
// a glob over paths.
func isPathFieldName(name string) bool {
	return name == "path" || name == "paths" || strings.HasSuffix(name, "_path") ||
		strings.HasSuffix(name, "_paths") || strings.HasSuffix(name, "_globs")
}

// isPathGoName reports whether the Go name of a field suggests a path.
// encoding/json names an untagged field by its Go name, which the rule above
// cannot see.
func isPathGoName(name string) bool {
	return strings.HasSuffix(name, "Path") || strings.HasSuffix(name, "Paths") || strings.HasSuffix(name, "Globs")
}

type jsonField struct {
	opts string
	typ  string // the Go type as written, from types.ExprString
	pos  token.Pos
}

// structJSONFields maps JSON name to field for the tagged, exported fields of
// st. In a struct that has json tags, an untagged field with a path-like Go
// name is a violation of its own. A struct with no json tag at all is not a
// JSON shape, such as an index entry.
func structJSONFields(fset *token.FileSet, typeName string, st *ast.StructType) (fields map[string]jsonField, violations []string) {
	fields = map[string]jsonField{}
	tagged := slices.ContainsFunc(st.Fields.List, func(f *ast.Field) bool {
		return f.Tag != nil && reflect.StructTag(strings.Trim(f.Tag.Value, "`")).Get("json") != ""
	})
	for _, field := range st.Fields.List {
		tag := ""
		if field.Tag != nil {
			tag = reflect.StructTag(strings.Trim(field.Tag.Value, "`")).Get("json")
		}
		name, opts, _ := strings.Cut(tag, ",")
		for _, id := range field.Names {
			switch {
			case !id.IsExported() || name == "-":
			case name == "":
				if tagged && isPathGoName(id.Name) {
					violations = append(violations, fset.Position(id.Pos()).String()+": "+typeName+"."+id.Name+" has no json name")
				}
			default:
				fields[name] = jsonField{opts: opts, typ: types.ExprString(field.Type), pos: id.Pos()}
			}
		}
	}
	return fields, violations
}

// rawPathViolations returns one message per path field in src that has no
// valid sibling, and how many path fields it checked.
func rawPathViolations(t *testing.T, filename, src string) (violations []string, checked int) {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filename, src, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", filename, err)
	}
	var stack []ast.Node
	ast.Inspect(f, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		stack = append(stack, n)
		if st, ok := n.(*ast.StructType); ok {
			// A struct belongs to the nearest type declaration around it,
			// wherever that sits in the file: package level, inside a
			// function, or inside a var.
			typeName := "struct at " + fset.Position(st.Pos()).String()
		nearest:
			for _, outer := range slices.Backward(stack) {
				switch spec := outer.(type) {
				case *ast.TypeSpec:
					typeName = spec.Name.Name
					break nearest
				case *ast.ValueSpec:
					typeName = spec.Names[0].Name
					break nearest
				}
			}
			v, c := structPathViolations(fset, typeName, st)
			violations, checked = append(violations, v...), checked+c
		}
		return true
	})
	return violations, checked
}

func structPathViolations(fset *token.FileSet, typeName string, st *ast.StructType) (violations []string, checked int) {
	fields, violations := structJSONFields(fset, typeName, st)
	for name, f := range fields {
		if !isPathFieldName(name) {
			continue
		}
		checked++
		where := fset.Position(f.pos).String() + ": " + typeName + "." + name
		sib, ok := fields[name+"_base64"]
		switch {
		case !ok:
			violations = append(violations, where+" has no "+name+"_base64 sibling")
		case sib.typ != f.typ && sib.typ != "*"+f.typ:
			violations = append(violations, where+": sibling is "+sib.typ+", want "+f.typ)
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
		v, c := rawPathViolations(t, name, string(src))
		all, checked = append(all, v...), checked+c
	}
	for _, v := range all {
		t.Error(v)
	}
	if checked < 10 {
		t.Errorf("the scan checked %d path fields, want at least 10: it reads too little", checked)
	}
}

// The scan has teeth: it names each way a path field can lack its sibling,
// and it reads every place a struct type can sit.
func TestRawPathGuardFindsMissingSiblings(t *testing.T) {
	const src = `package x
type ok struct {
	Path string ` + "`json:\"path\"`" + `
	PathBase64 string ` + "`json:\"path_base64,omitempty\"`" + `
	Paths []string ` + "`json:\"conflict_paths\"`" + `
	PathsBase64 []string ` + "`json:\"conflict_paths_base64,omitzero\"`" + `
	Nested struct {
		Old string ` + "`json:\"old_path\"`" + `
		OldBase64 string ` + "`json:\"old_path_base64,omitempty\"`" + `
	} ` + "`json:\"nested\"`" + `
	Optional string ` + "`json:\"file_path\"`" + `
	OptionalBase64 *string ` + "`json:\"file_path_base64,omitempty\"`" + `
	Ptr *string ` + "`json:\"ptr_path\"`" + `
	PtrBase64 *string ` + "`json:\"ptr_path_base64,omitempty\"`" + `
	Hidden string ` + "`json:\"-\"`" + `
	hidden string
}
type missing struct {
	Path string ` + "`json:\"path\"`" + `
}
type missingList struct {
	Paths []string ` + "`json:\"paths\"`" + `
}
type wrongType struct {
	Path string ` + "`json:\"path\"`" + `
	PathBase64 []string ` + "`json:\"path_base64,omitempty\"`" + `
}
type alwaysPresent struct {
	Path string ` + "`json:\"original_path\"`" + `
	PathBase64 string ` + "`json:\"original_path_base64\"`" + `
}
type nestedMissing struct {
	Inner struct {
		P string ` + "`json:\"old_path\"`" + `
	} ` + "`json:\"inner\"`" + `
}
type namedType string
type namedMissing struct {
	Path namedType ` + "`json:\"path\"`" + `
}
type pointerMissing struct {
	Paths []*string ` + "`json:\"paths\"`" + `
}
type untagged struct {
	Name string ` + "`json:\"name\"`" + `
	OldPath string
	Count int
}
type notJSON struct {
	OldPath string
}
type multi struct {
	A, B string ` + "`json:\"a_path\"`" + `
}
type notAPath struct {
	Name string ` + "`json:\"name\"`" + `
}
type okGlobs struct {
	Include []string ` + "`json:\"include_globs\"`" + `
	IncludeBase64 []string ` + "`json:\"include_globs_base64,omitempty\"`" + `
}
type missingGlobs struct {
	Include []string ` + "`json:\"include_globs\"`" + `
}
type optionalPath struct {
	Path string ` + "`json:\"path\"`" + `
	PathBase64 *string ` + "`json:\"path_base64,omitempty\"`" + `
}
var packageVar = struct {
	P string ` + "`json:\"path\"`" + `
}{}
func f() {
	type local struct {
		P string ` + "`json:\"path\"`" + `
	}
	_ = struct {
		Q []string ` + "`json:\"paths\"`" + `
	}{}
}
`
	got, checked := rawPathViolations(t, "x.go", src)
	for _, want := range []string{
		"missing.path has no path_base64 sibling",
		"missingList.paths has no paths_base64 sibling",
		"wrongType.path: sibling is []string, want string",
		"alwaysPresent.original_path: sibling is not omitempty or omitzero",
		"nestedMissing.old_path has no old_path_base64 sibling",
		"missingGlobs.include_globs has no include_globs_base64 sibling",
		"namedMissing.path has no path_base64 sibling",
		"pointerMissing.paths has no paths_base64 sibling",
		"untagged.OldPath has no json name",
		"multi.a_path has no a_path_base64 sibling",
		"packageVar.path has no path_base64 sibling",
		"local.path has no path_base64 sibling",
	} {
		if !slicesContainsSuffix(got, want) {
			t.Errorf("no violation %q in %q", want, got)
		}
	}
	// A struct that no type or var names is named by its position.
	if !slicesContainsSuffix(got, "has no paths_base64 sibling") || !strings.Contains(strings.Join(got, "\n"), "struct at x.go:") {
		t.Errorf("no violation for the unnamed struct in %q", got)
	}
	if len(got) != 13 {
		t.Errorf("violations %d (%q), want 13", len(got), got)
	}
	// ok holds 5 path fields (one nested), okGlobs and optionalPath 1 each,
	// and 12 other types 1 each.
	if checked != 19 {
		t.Errorf("checked %d path fields, want 19", checked)
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
