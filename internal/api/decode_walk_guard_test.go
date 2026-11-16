package api

import (
	"encoding"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
)

// What the walk of unmarshalBody can name. It finds the member that failed
// by the dotted path that encoding/json reports (nullMembers, memberSpans),
// and that is right for the members of a struct, a slice and an array whose
// names and tags walkProblems accepts. It is wrong for a map, whose path holds
// the key (a key can hold a dot, be a path escape or fail to decode), and for
// a type that decodes itself (the path of its errors is its own, and null can
// be an error). For such a type the walk nulls another member or none, and the
// answer misses fields. Two ways were tried to cover them: teaching the walk
// each case (pull request #883, whose rounds of review kept finding another
// type or another path) and this guard, which keeps such a type out of the
// request types until a person has read what the walk does with it.

// walkEntries are the functions that decode a request body with
// unmarshalBody, directly or for a caller.
var walkEntries = []string{"unmarshalBody", "wrefDecodeBody", "wrefDecodeOptionalBody", "gcredDecodeBody"}

// decodeWalkSite is a function that names an entry of walkEntries (a call, or
// a value as in `decode := unmarshalBody`): the number of such places in it
// and the request types that it decodes there. A helper whose caller owns the
// type has none, and its callers have them.
type decodeWalkSite struct {
	places int
	types  []reflect.Type
}

// decodeWalkSites is every function that decodes a request body with the
// walk. A new one, or a new place in one, fails
// TestRequestTypesStayInsideTheDecodeWalk until it is listed here with its
// request types, and the types are held to walkProblems.
var decodeWalkSites = map[string]decodeWalkSite{
	"gcredDecodeBody":                         {1, nil},
	"wrefDecodeBody":                          {1, nil},
	"wrefDecodeOptionalBody":                  {1, nil},
	"server.handleArchive":                    {1, []reflect.Type{reflect.TypeFor[archiveRequestBody]()}},
	"server.handleCreateBranch":               {1, []reflect.Type{reflect.TypeFor[wrefCreateBranchRequest]()}},
	"server.handleCreateTag":                  {1, []reflect.Type{reflect.TypeFor[wrefCreateTagRequest]()}},
	"server.handleDeleteBranch":               {1, []reflect.Type{reflect.TypeFor[wrefDeleteBranchRequest]()}},
	"server.handleDeleteTag":                  {1, []reflect.Type{reflect.TypeFor[wrefDeleteTagRequest]()}},
	"server.handleGrep":                       {1, []reflect.Type{reflect.TypeFor[grepRequestBody]()}},
	"server.handleMerge":                      {1, []reflect.Type{reflect.TypeFor[wmMergeRequest]()}},
	"server.wrefHandleNote":                   {2, []reflect.Type{reflect.TypeFor[wrefNoteRequest]()}},
	"syncServer.handleCreateCredential":       {1, []reflect.Type{reflect.TypeFor[gcredCreateRequest]()}},
	"syncServer.handleCreateRepo":             {2, []reflect.Type{reflect.TypeFor[createRepoRequest](), reflect.TypeFor[usyncBaseRepo]()}},
	"syncServer.handleDeleteCredential":       {1, []reflect.Type{reflect.TypeFor[gcredDeleteRequest]()}},
	"syncServer.handlePullUpstream":           {1, []reflect.Type{reflect.TypeFor[usyncPullUpstreamRequest]()}},
	"syncServer.handleUpdateCredential":       {1, []reflect.Type{reflect.TypeFor[gcredUpdateRequest]()}},
	"syncServer.handleUpdateCredentialByPath": {1, []reflect.Type{reflect.TypeFor[gcredPathUpdateRequest]()}},
	"webhooksAPI.handleCreate":                {1, []reflect.Type{reflect.TypeFor[createWebhookRequest]()}},
}

// decodeWalkUnmarshalers are the types with their own UnmarshalJSON or
// UnmarshalText that a request type may hold, and why the walk is right for
// them. Any other such type fails walkProblems.
var decodeWalkUnmarshalers = map[reflect.Type]string{
	reflect.TypeFor[json.RawMessage]():         "keeps the bytes, never fails",
	reflect.TypeFor[wrefCreateBranchRequest](): "decodes the whole object as a copy of itself, so the paths of its errors are the same",
}

var walkNameRE = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// walkProblems lists what in the type t, a request type that unmarshalBody
// decodes, is outside what its walk handles. Each problem starts with the Go
// path to it.
func walkProblems(t reflect.Type) []string {
	var (
		out  []string
		seen = make(map[reflect.Type]bool)
		walk func(t reflect.Type, path string)
	)
	add := func(path, format string, args ...any) { out = append(out, path+": "+fmt.Sprintf(format, args...)) }
	walk = func(t reflect.Type, path string) {
		for t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		custom := reflect.PointerTo(t).Implements(reflect.TypeFor[json.Unmarshaler]()) ||
			reflect.PointerTo(t).Implements(reflect.TypeFor[encoding.TextUnmarshaler]())
		if _, ok := decodeWalkUnmarshalers[t]; custom && !ok {
			add(path, "%s decodes itself (UnmarshalJSON or UnmarshalText), and the path of its errors is its own", t)
			return
		}
		switch t.Kind() {
		case reflect.Bool, reflect.String,
			reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.Float32, reflect.Float64:
		case reflect.Slice, reflect.Array:
			walk(t.Elem(), path+"[]")
		case reflect.Map:
			add(path, "a map: the path of an error holds the key, which can hold a dot, be an escape or fail to decode")
		case reflect.Interface:
			add(path, "an interface decodes any JSON, maps included")
		case reflect.Struct:
			if seen[t] {
				return
			}
			seen[t] = true
			byName := make(map[string]string)
			for f := range t.Fields() {
				fpath := path + "." + f.Name
				if f.Anonymous {
					add(fpath, "an embedded field: its members are promoted into the parent under Go's rules for which member wins a name, and the name checks here do not model them")
					continue
				}
				if !f.IsExported() {
					continue
				}
				tag := f.Tag.Get("json")
				if tag == "-" {
					continue
				}
				name, opts, _ := strings.Cut(tag, ",")
				if name == "" {
					name = f.Name
				}
				for _, opt := range strings.Split(opts, ",") {
					if opt != "" && opt != "omitempty" && opt != "omitzero" && opt != "string" {
						add(fpath, "the json tag %q has the option %q, which the walk was not checked for", tag, opt)
					}
				}
				if !walkNameRE.MatchString(name) {
					add(fpath, "the member name %q has a character outside [A-Za-z0-9_]: the path splits at dots and matches names by case folding", name)
				}
				if other, dup := byName[strings.ToLower(name)]; dup {
					add(fpath, "the member name %q is the name of %s without regard to case: the walk replaces both", name, other)
				}
				byName[strings.ToLower(name)] = f.Name
				walk(f.Type, fpath)
			}
		default:
			add(path, "%s is no JSON type", t.Kind())
		}
	}
	walk(t, t.String())
	return out
}

// decodeWalkSitesInSource returns, for each function of the package that is
// not in a test file, as `Recv.Name` or `Name`, how many places in it name an
// entry of walkEntries. It reads the syntax only, so it does not see the type
// of the destination.
func decodeWalkSitesInSource(t *testing.T) map[string]int {
	t.Helper()
	files, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	sites := make(map[string]int)
	fset := token.NewFileSet()
	for _, e := range files {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, e.Name(), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			name := fn.Name.Name
			if fn.Recv != nil && len(fn.Recv.List) == 1 {
				recv := fn.Recv.List[0].Type
				if star, ok := recv.(*ast.StarExpr); ok {
					recv = star.X
				}
				if id, ok := recv.(*ast.Ident); ok {
					name = id.Name + "." + name
				}
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok && slices.Contains(walkEntries, id.Name) {
					sites[name]++
				}
				return true
			})
		}
	}
	return sites
}

// TestRequestTypesStayInsideTheDecodeWalk fails when a request type that the
// server decodes with unmarshalBody gains a map, an embedded field, an
// interface, a member name or a tag option outside what the walk handles, or
// when a function starts to decode a body with the walk without being listed.
func TestRequestTypesStayInsideTheDecodeWalk(t *testing.T) {
	const fix = "list the function in decodeWalkSites with the request types it decodes; a type outside the walk needs its own decode, not a change of the walk (see the top of decode_walk_guard_test.go)"
	got := decodeWalkSitesInSource(t)
	for _, name := range slices.Sorted(maps.Keys(got)) {
		site, ok := decodeWalkSites[name]
		switch {
		case !ok:
			t.Errorf("%s names unmarshalBody (or a helper that calls it) %d times and is not in decodeWalkSites: %s", name, got[name], fix)
		case site.places != got[name]:
			t.Errorf("%s names unmarshalBody (or a helper that calls it) %d times, decodeWalkSites says %d: %s", name, got[name], site.places, fix)
		}
	}
	for _, name := range slices.Sorted(maps.Keys(decodeWalkSites)) {
		if got[name] == 0 {
			t.Errorf("decodeWalkSites lists %s, which no longer names unmarshalBody: remove it", name)
		}
		for _, typ := range decodeWalkSites[name].types {
			for _, problem := range walkProblems(typ) {
				t.Errorf("%s decodes %s: %s", name, typ, problem)
			}
		}
	}
}

type (
	walkLeaf struct {
		Name string `json:"name"`
	}
	walkSelfRef struct {
		Next *walkSelfRef `json:"next"`
		Name string       // no tag: the Go name is the member name
	}
	walkOwnDecode  struct{ N int }
	walkTextDecode struct{ N int }
	walkInner      struct {
		Tags map[string]string `json:"tags"`
	}
	walkEmbedded struct{ walkLeaf }
	// walkShapes has one field of each shape that walkProblems allows.
	walkShapes struct {
		Plain    int             `json:"plain"`
		Omit     string          `json:"omit,omitempty"`
		Zero     bool            `json:"zero,omitzero"`
		Quoted   int             `json:"quoted,string"`
		Ptr      *walkLeaf       `json:"ptr"`
		Items    []walkLeaf      `json:"items"`
		Grid     [2][]string     `json:"grid"`
		Raw      json.RawMessage `json:"raw"`
		Self     walkSelfRef     `json:"self"`
		Skipped  int             `json:"-"`
		Untagged float64
		Under9   uint8             `json:"under_9"`
		_        map[string]string // unexported: encoding/json ignores it
	}
)

func (*walkOwnDecode) UnmarshalJSON([]byte) error { return nil }
func (walkTextDecode) UnmarshalText([]byte) error { return nil }

// TestDecodeWalkProblems: walkProblems flags each kind of type that the walk
// does not handle, at any depth, and nothing that it handles.
func TestDecodeWalkProblems(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  reflect.Type
		want string // a part of one problem; "" for a clean type
	}{
		{"clean: every shape the request types use", reflect.TypeFor[walkShapes](), ""},
		{"clean: a type that holds itself", reflect.TypeFor[walkSelfRef](), ""},
		{"clean: the allowed raw member", reflect.TypeFor[struct {
			R json.RawMessage `json:"r"`
		}](), ""},
		{"a map", reflect.TypeFor[struct {
			M map[string]int `json:"m"`
		}](), "a map"},
		{"a map in a pointer, a slice and an array", reflect.TypeFor[struct {
			M *[]*[3]walkInner `json:"m"`
		}](), "a map"},
		{"an embedded struct", reflect.TypeFor[walkEmbedded](), "an embedded field"},
		{"an embedded pointer", reflect.TypeFor[struct{ *walkLeaf }](), "an embedded field"},
		{"an interface", reflect.TypeFor[struct {
			V any `json:"v"`
		}](), "an interface"},
		{"a member name with a dot", reflect.TypeFor[struct {
			A int `json:"a.b"`
		}](), "outside [A-Za-z0-9_]"},
		{"a member name that is a path escape", reflect.TypeFor[struct {
			A int `json:"a/b"`
		}](), "outside [A-Za-z0-9_]"},
		{"a member name that is not ASCII", reflect.TypeFor[struct {
			A int `json:"\u00e4"`
		}](), "outside [A-Za-z0-9_]"},
		{"a tag option", reflect.TypeFor[struct {
			A int `json:"a,format:x"`
		}](), `the option "format:x"`},
		{"a tag option after omitempty", reflect.TypeFor[struct {
			A int `json:"a,omitempty,inline"`
		}](), `the option "inline"`},
		{"names that differ in case", reflect.TypeFor[struct {
			A int `json:"abc"`
			B int `json:"ABC"`
		}](), "without regard to case"},
		{"a name from the Go field that differs in case", reflect.TypeFor[struct {
			Abc int
			B   int `json:"aBC"`
		}](), "without regard to case"},
		{"a type with its own UnmarshalJSON", reflect.TypeFor[walkOwnDecode](), "decodes itself"},
		{"a field with its own UnmarshalText", reflect.TypeFor[struct {
			T walkTextDecode `json:"t"`
		}](), "decodes itself"},
		{"time.Time", reflect.TypeFor[struct {
			At *time.Time `json:"at"`
		}](), "decodes itself"},
		{"a channel", reflect.TypeFor[struct {
			C chan int `json:"c"`
		}](), "is no JSON type"},
		{"a map under a field of a nested struct", reflect.TypeFor[struct {
			Inner walkInner `json:"inner"`
		}](), "a map"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			problems := walkProblems(tc.typ)
			switch {
			case tc.want == "" && len(problems) > 0:
				t.Fatalf("a clean type has problems: %q", problems)
			case tc.want != "" && !slices.ContainsFunc(problems, func(p string) bool { return strings.Contains(p, tc.want) }):
				t.Fatalf("problems %q hold no %q", problems, tc.want)
			}
		})
	}
	// Each problem starts with the Go path to it.
	if got := walkProblems(reflect.TypeFor[struct {
		Inner walkInner `json:"inner"`
	}]()); len(got) != 1 || !strings.Contains(got[0], ".Inner.Tags: a map") {
		t.Fatalf("path of a problem: %q", got)
	}
}

// TestDecodeWalkHandlesWhatTheGuardAllows: for a type with every shape that
// walkProblems allows, unmarshalBody names each field of the wrong type, in
// body order, with one decode for each and one more. This is what lets the
// guard pass a type.
func TestDecodeWalkHandlesWhatTheGuardAllows(t *testing.T) {
	body := `{"plain":"x","omit":1,"zero":2,"quoted":"x","ptr":{"name":3},"items":[{"name":4},{"name":"ok"},{"NAME":5}],` +
		`"grid":[["a",6],[7]],"raw":8,"self":{"next":{"name":9}},"skipped":"s","UNTAGGED":"u","under_9":"v"}`
	want := []string{"plain", "omit", "zero", "quoted", "ptr.name", "items.0.name", "items.2.NAME", "grid.0.1", "grid.1.0",
		"self.next.name", "UNTAGGED", "under_9"}
	var calls int
	err := unmarshalBody([]byte(body), &decodeCount[walkShapes]{calls: &calls})
	var got []string
	for _, p := range invalidParamsFor(err) {
		got = append(got, p.Name)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("named %q, want %q (err %v)", got, want, err)
	}
	if calls != len(want)+1 {
		t.Fatalf("%d decodes for %d wrong fields", calls, len(want))
	}
}
