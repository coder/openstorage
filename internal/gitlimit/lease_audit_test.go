package gitlimit

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// This audit is the static half of the lease coverage proof (LeaseFile). It
// reads every non-test Go file of the module and fails on a git start that
// takes no lease and is not listed below with a reason. The other half is the
// openstorage_viewcheck panic (leasecheck_viewcheck.go), which sees a start
// that runs in a repository at run time.
//
// Scope, stated honestly. Two layers:
//   - File level: a non-test file that imports os/exec must be in
//     execImporters. A new file, with any way of starting a program, fails
//     until someone reads its git starts and lists it with a reason.
//   - Function level, by name inside one function (or one package-level var
//     initializer): a git command is built with exec.Command, exec.CommandContext,
//     an exec.Cmd literal, exec.Command held as a value, or a shell or launcher
//     program. The function must then start it through StartIn, RunIn,
//     OutputIn, CombinedOutputIn or OpenLease and Attach. A function that
//     builds git for two commands and leases only one passes.
//
// It does not see a helper in another package that starts git for its caller,
// or git started without os/exec (os.StartProcess, syscall.Exec).

const (
	repoRoot    = "../.."
	leaseImport = "github.com/coder/openstorage/internal/gitlimit"
	execImport  = "os/exec"
)

// leaseExempt lists the functions allowed to start git without the lease,
// keyed "<file>:<func>" ("<file>:*" for every function of a file, and
// "<dir>/*" for every file under a directory). A "builder" only builds the
// command: the audit treats a call to it like building git, so its callers
// need a lease-aware starter.
var leaseExempt = map[string]string{
	"internal/gitlimit/*":                                       "the package that implements the lease and the plain starters",
	"internal/githttp/hook.go:*":                                "runs inside a hook of a git that serve started with the lease: the hook and its git children inherit the lease descriptor (LeaseFD)",
	"internal/api/handlers.go:validRefFormat":                   "git check-ref-format reads no repository",
	"internal/api/handlers_write_merge.go:wmEnsureMergeTreeGit": "git version reads no repository",
	"internal/sync/worker.go:runGitTo":                          "runs in a temporary mirror under os.TempDir, never in a cache repository",
	"internal/simtest/gitenv.go:simGit":                         "test support: runs in the scratch repositories of simulations",
	"internal/gitread/heaplimit.go:heapLimitedGit":              "builder: gitread.Run starts the command through RunIn",
	"internal/api/handlers_write_commit.go:gitCmd":              "builder: its callers start the command through OutputIn or StartIn",
}

// execImporters lists every non-test file outside internal/gitlimit that
// imports os/exec, with the reason its starts are safe. The function-level
// checks below judge the starts themselves.
var execImporters = map[string]string{
	"internal/api/handlers.go":              "validRefFormat starts git check-ref-format, which reads no repository (leaseExempt)",
	"internal/api/handlers_read_query.go":   "only *exec.ExitError, to read an exit status",
	"internal/api/handlers_write_commit.go": "gitCmd builds the commands, which start through OutputIn and StartIn in the scratch repository",
	"internal/api/handlers_write_merge.go":  "git version reads no repository (leaseExempt); the rest reads exit statuses",
	"internal/api/handlers_write_refs.go":   "the ref commands start through OutputIn",
	"internal/githttp/hook.go":              "runs inside a hook and inherits the lease (leaseExempt)",
	"internal/githttp/hydrate.go":           "every start goes through OutputIn, StartIn or CombinedOutputIn",
	"internal/githttp/server.go":            "upload-pack and receive-pack start through RunIn and StartIn",
	"internal/gitread/budget.go":            "only *exec.ExitError, to read an exit status",
	"internal/gitread/gitread.go":           "only *exec.ExitError, to read an exit status",
	"internal/gitread/grep.go":              "only *exec.ExitError, to read an exit status",
	"internal/gitread/heaplimit.go":         "builder: gitread.Run starts the command through RunIn (leaseExempt)",
	"internal/gitread/notes.go":             "only *exec.ExitError, to read an exit status",
	"internal/gitread/tree_resume.go":       "only *exec.ExitError, to read an exit status",
	"internal/jobs/repack.go":               "runGitStdin and streamShowIndex open the lease and attach it",
	"internal/lfsscan/scan.go":              "ReachableLFSOIDs opens one lease and attaches it to all three commands",
	"internal/simtest/gitenv.go":            "test support: runs in the scratch repositories of simulations (leaseExempt)",
	"internal/sync/procgroup_other.go":      "only the exec.Cmd type, to set the process group of the sync worker's git",
	"internal/sync/procgroup_unix.go":       "only the exec.Cmd type, to set the process group of the sync worker's git",
	"internal/sync/worker.go":               "runs git in a temporary mirror under os.TempDir, never in a cache repository (leaseExempt)",
}

var leaseBuilders = map[string]bool{"heapLimitedGit": true, "gitCmd": true}

var plainStarters = map[string]bool{"Start": true, "Run": true, "Output": true, "CombinedOutput": true}

var leaseStarters = map[string]bool{"StartIn": true, "RunIn": true, "OutputIn": true, "CombinedOutputIn": true, "OpenLease": true}

// nonGitPrograms are the programs that exec.Command may start by name
// without a lease: they read no cache repository. Any other program named by
// a string literal is treated like git, because a launcher (sh -c, env,
// flock, taskset, ...) can start git for its arguments, and no list of
// launchers is complete.
var nonGitPrograms = map[string]bool{"gpg": true, "ssh-keygen": true}

type auditFunc struct {
	key      string // "<file>:<func>"
	builds   []token.Position
	plain    []token.Position
	leased   bool
	attaches bool
	direct   []token.Position // cmd.Start()/Run()/Output()/CombinedOutput()
	dot      []token.Position // import . "os/exec" or import . gitlimit
}

// problems lists what is wrong with the way f starts git, if anything.
func (f *auditFunc) problems() []string {
	var bad []string
	for _, p := range f.dot {
		bad = append(bad, p.String()+": a dot import hides its calls from the audit; import the package by name")
	}
	for _, p := range f.plain {
		bad = append(bad, p.String()+": a plain gitlimit start takes no lease; use StartIn, RunIn, OutputIn or CombinedOutputIn")
	}
	if len(f.builds) > 0 && !f.leased {
		for _, p := range f.builds {
			bad = append(bad, p.String()+": builds git but the function starts none through StartIn, RunIn, OutputIn, CombinedOutputIn or OpenLease")
		}
	}
	if len(f.builds) > 0 && !f.attaches {
		for _, p := range f.direct {
			bad = append(bad, p.String()+": cmd method starts git without a lease; use StartIn, RunIn, OutputIn or CombinedOutputIn, or OpenLease and Attach")
		}
	}
	return bad
}

func TestEveryGitStartTakesTheLease(t *testing.T) {
	funcs, importers := auditModule(t)
	for _, b := range auditProblems(funcs, importers, leaseExempt, execImporters) {
		t.Error(b)
	}
}

// auditProblems judges the records of a module: what each function does with
// git, which files import os/exec, and the two allow-lists. A stale entry of
// either list is a problem, so a list shrinks with the code.
func auditProblems(funcs []*auditFunc, importers []string, exemptions, listed map[string]string) []string {
	used := map[string]bool{}
	exempt := func(f *auditFunc) bool {
		file, _, _ := strings.Cut(f.key, ":")
		for _, k := range []string{f.key, file + ":*", path.Dir(file) + "/*"} {
			if _, ok := exemptions[k]; ok {
				used[k] = true
				return true
			}
		}
		return false
	}
	var bad []string
	for _, f := range funcs {
		if !exempt(f) {
			bad = append(bad, f.problems()...)
		}
	}
	for k := range exemptions {
		if !used[k] {
			bad = append(bad, "leaseExempt entry "+k+" matches no git start: delete it")
		}
	}
	imports := map[string]bool{}
	for _, rel := range importers {
		imports[rel] = true
		_, exempt := exemptions[path.Dir(rel)+"/*"]
		if _, ok := listed[rel]; !ok && !exempt {
			bad = append(bad, rel+": imports os/exec and is not in execImporters; read its git starts, then list it with a reason")
		}
	}
	for k := range listed {
		if !imports[k] {
			bad = append(bad, "execImporters entry "+k+" imports no os/exec: delete it")
		}
	}
	sort.Strings(bad)
	return bad
}

// The two allow-lists are checked in both directions: a file that imports
// os/exec and is not listed, and a list entry that matches nothing. The
// module has neither today, so synthetic input pins the checks.
func TestAuditChecksTheAllowLists(t *testing.T) {
	pos := token.Position{Filename: "x.go", Line: 1, Column: 1}
	funcs := []*auditFunc{
		{key: "a/x.go:f", plain: []token.Position{pos}},
		{key: "internal/gitlimit/z.go:g", plain: []token.Position{pos}},
		{key: "b/y.go:h", plain: []token.Position{{Filename: "b/y.go", Line: 1, Column: 1}}},
	}
	bad := auditProblems(funcs,
		[]string{"a/x.go", "b/y.go", "c/new.go", "internal/gitlimit/z.go"},
		map[string]string{"a/x.go:f": "r", "a/stale.go:*": "r", "internal/gitlimit/*": "r"},
		map[string]string{"a/x.go": "r", "b/y.go": "r", "a/gone.go": "r"})
	want := []string{
		"b/y.go:1:1: a plain gitlimit start takes no lease; use StartIn, RunIn, OutputIn or CombinedOutputIn",
		"c/new.go: imports os/exec and is not in execImporters; read its git starts, then list it with a reason",
		"execImporters entry a/gone.go imports no os/exec: delete it",
		"leaseExempt entry a/stale.go:* matches no git start: delete it",
	}
	if strings.Join(bad, "\n") != strings.Join(want, "\n") {
		t.Fatalf("problems:\n%s\nwant:\n%s", strings.Join(bad, "\n"), strings.Join(want, "\n"))
	}
}

// TestAuditSeesEveryWayToBuildGit runs the audit on small sources, so the
// detectors stay proven when the module no longer holds an example: each way
// to build or start git without a lease is flagged, and a lease-aware start
// clears it.
func TestAuditSeesEveryWayToBuildGit(t *testing.T) {
	const header = "package p\nimport (\n\t\"context\"\n\t\"os/exec\"\n\t\"github.com/coder/openstorage/internal/gitlimit\"\n)\nvar _ = context.Background\nvar _ = gitlimit.LeaseFD\n"
	cases := []struct {
		name string
		src  string
		bad  bool
	}{
		{"exec.Command run directly", `func f() { _ = exec.Command("git", "gc").Run() }`, true},
		{"exec.CommandContext run directly", `func f(ctx context.Context) { _ = exec.CommandContext(ctx, "git", "gc").Run() }`, true},
		{"command name in a variable", `func f(name string) { _ = exec.Command(name).Run() }`, true},
		{"exec.Cmd literal", `func f() { cmd := &exec.Cmd{Path: "/usr/bin/git"}; _ = cmd.Run() }`, true},
		{"exec.Command held as a value", `func f() { run := exec.Command; _ = run("git").Run() }`, true},
		{"shell command line", `func f() { _ = exec.Command("sh", "-c", "git gc").Run() }`, true},
		{"launcher", `func f() { _ = exec.Command("/usr/bin/env", "git", "gc").Run() }`, true},
		{"launcher that no list names", `func f() { _ = exec.Command("flock", "x.lock", "git", "gc").Run() }`, true},
		{"launcher with options", `func f() { _ = exec.Command("taskset", "-c", "0", "git", "gc").Run() }`, true},
		{"package-level initializer", `var run = func() error { return exec.Command("git", "gc").Run() }`, true},
		{"plain gitlimit start", `func f() { _ = gitlimit.Run(exec.Command("git", "gc")) }`, true},
		{"leased start", `func f(ctx context.Context, dir string) { _ = gitlimit.RunIn(ctx, dir, exec.Command("git", "gc")) }`, false},
		{"lease and attach", `func f(ctx context.Context, dir string) { l, _ := gitlimit.OpenLease(ctx, dir); defer l.Close(); cmd := exec.Command("git", "gc"); l.Attach(cmd); _ = cmd.Run() }`, false},
		{"another program", `func f() { _ = exec.Command("gpg", "--import").Run() }`, false},
		{"another program by path", `func f() { _ = exec.Command("/usr/bin/ssh-keygen", "-Y", "verify").Run() }`, false},
	}
	for _, c := range cases {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, "p.go", header+c.src, 0)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		funcs, importsExec := auditFile(fset, file, "p.go")
		if !importsExec {
			t.Errorf("%s: the audit did not see the os/exec import", c.name)
		}
		var bad []string
		for _, f := range funcs {
			bad = append(bad, f.problems()...)
		}
		if got := len(bad) > 0; got != c.bad {
			t.Errorf("%s: flagged = %v, want %v (%v)", c.name, got, c.bad, bad)
		}
	}
	// Whole files, for the cases that need other imports.
	for _, c := range []struct {
		name string
		src  string
	}{
		{"an aliased os/exec import", "package p\nimport osexec \"os/exec\"\nfunc f() { _ = osexec.Command(\"git\").Run() }"},
		{"a dot import of os/exec", "package p\nimport . \"os/exec\"\nfunc f() { _ = Command(\"git\").Run() }"},
		{"a dot import of gitlimit", "package p\nimport . \"github.com/coder/openstorage/internal/gitlimit\"\nfunc f() { _ = LeaseFD }"},
		{"os.StartProcess", "package p\nimport \"os\"\nfunc f() { _, _ = os.StartProcess(\"/usr/bin/git\", nil, nil) }"},
		{"syscall.Exec", "package p\nimport \"syscall\"\nfunc f() { _ = syscall.Exec(\"/usr/bin/git\", nil, nil) }"},
	} {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, "p.go", c.src, 0)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		funcs, _ := auditFile(fset, file, "p.go")
		var bad []string
		for _, f := range funcs {
			bad = append(bad, f.problems()...)
		}
		if len(bad) == 0 {
			t.Errorf("%s was not flagged", c.name)
		}
	}
}

// auditModule parses the module's non-test Go files and returns one record
// per function that builds, starts or leases git, and the files that import
// os/exec.
func auditModule(t *testing.T) ([]*auditFunc, []string) {
	t.Helper()
	root, err := filepath.Abs(repoRoot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("module root %s: %v", root, err)
	}
	fset := token.NewFileSet()
	var out []*auditFunc
	var importers []string
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := d.Name()
		if d.IsDir() {
			if path != root && (name == "vendor" || name == "testdata" || strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_")) {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		file, err := parser.ParseFile(fset, filepath.ToSlash(rel), src, 0)
		if err != nil {
			return err
		}
		funcs, importsExec := auditFile(fset, file, filepath.ToSlash(rel))
		out = append(out, funcs...)
		if importsExec {
			importers = append(importers, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) < 8 {
		t.Fatalf("the audit found %d functions that touch git; it no longer reads the module", len(out))
	}
	return out, importers
}

// auditFile returns one record per function (and per package-level var
// declaration) of file that builds, starts or leases git, and whether the file
// imports os/exec.
func auditFile(fset *token.FileSet, file *ast.File, rel string) ([]*auditFunc, bool) {
	names := map[string]string{} // import path -> local name
	dots := &auditFunc{key: rel + ":import"}
	for _, imp := range file.Imports {
		p, _ := strconv.Unquote(imp.Path.Value)
		local := path.Base(p)
		if imp.Name != nil {
			local = imp.Name.Name
		}
		names[p] = local
		if local == "." && (p == execImport || p == leaseImport) {
			dots.dot = append(dots.dot, fset.Position(imp.Pos()))
		}
	}
	execName := names[execImport]
	_, importsExec := names[execImport]
	var out []*auditFunc
	if len(dots.dot) > 0 {
		out = append(out, dots)
	}
	scan := func(key string, root ast.Node) {
		f := &auditFunc{key: key}
		callees := map[ast.Expr]bool{} // the Fun of each call seen so far
		ast.Inspect(root, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.CompositeLit:
				if isPkgSel(n.Type, execName, "Cmd") { // &exec.Cmd{Path: "/usr/bin/git"}
					f.builds = append(f.builds, fset.Position(n.Pos()))
				}
			case *ast.SelectorExpr:
				if isExecCommand(n, execName) && !callees[n] { // run := exec.Command
					f.builds = append(f.builds, fset.Position(n.Pos()))
				}
			case *ast.CallExpr:
				callees[n.Fun] = true
				auditCall(fset, f, n, names)
			}
			return true
		})
		if len(f.builds)+len(f.plain)+len(f.direct) > 0 || f.leased {
			out = append(out, f)
		}
	}
	for _, decl := range file.Decls {
		switch decl := decl.(type) {
		case *ast.FuncDecl:
			if decl.Body != nil {
				scan(rel+":"+decl.Name.Name, decl.Body)
			}
		case *ast.GenDecl:
			if decl.Tok != token.VAR {
				continue
			}
			for _, spec := range decl.Specs {
				vs := spec.(*ast.ValueSpec)
				scan(rel+":var "+vs.Names[0].Name, vs)
			}
		}
	}
	return out, importsExec
}

// auditCall records what one call expression does to f. names maps the
// import paths of the file to their local names.
func auditCall(fset *token.FileSet, f *auditFunc, call *ast.CallExpr, names map[string]string) {
	execName, limitName := names[execImport], names[leaseImport]
	pos := fset.Position(call.Pos())
	sel, isSel := call.Fun.(*ast.SelectorExpr)
	switch {
	case isExecCommand(call.Fun, execName):
		arg := 0
		if sel.Sel.Name == "CommandContext" {
			arg = 1
		}
		if len(call.Args) > arg && isNonGitProgram(call.Args[arg]) {
			return
		}
		f.builds = append(f.builds, pos)
	case isPkgSel(call.Fun, limitName, "Command"):
		f.builds = append(f.builds, pos)
	case isPkgSel(call.Fun, names["os"], "StartProcess"), isPkgSel(call.Fun, names["syscall"], "Exec"),
		isPkgSel(call.Fun, names["syscall"], "ForkExec"), isPkgSel(call.Fun, names["syscall"], "StartProcess"):
		f.builds = append(f.builds, pos) // a process started without os/exec
	case isSel && isPkgSel(call.Fun, limitName, sel.Sel.Name) && plainStarters[sel.Sel.Name]:
		f.plain = append(f.plain, pos)
	case isSel && isPkgSel(call.Fun, limitName, sel.Sel.Name) && leaseStarters[sel.Sel.Name]:
		f.leased = true
	case isSel && sel.Sel.Name == "Attach" && len(call.Args) == 1:
		f.attaches = true
	case isSel && plainStarters[sel.Sel.Name] && len(call.Args) == 0:
		f.direct = append(f.direct, pos)
	}
	if leaseBuilders[calleeName(call)] {
		f.builds = append(f.builds, pos)
	}
}

// isPkgSel reports whether e is <pkg>.<name>, where pkg is the local name of
// an import that the file has (empty when it has none).
func isPkgSel(e ast.Expr, pkg, name string) bool {
	sel, ok := e.(*ast.SelectorExpr)
	if !ok || pkg == "" || sel.Sel.Name != name {
		return false
	}
	id, ok := sel.X.(*ast.Ident)
	return ok && id.Name == pkg
}

func isExecCommand(e ast.Expr, execName string) bool {
	return isPkgSel(e, execName, "Command") || isPkgSel(e, execName, "CommandContext")
}

// isNonGitProgram reports whether e is a string literal that names a program
// of nonGitPrograms.
func isNonGitProgram(e ast.Expr) bool {
	lit, ok := e.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return false
	}
	s, err := strconv.Unquote(lit.Value)
	return err == nil && nonGitPrograms[path.Base(s)]
}

func calleeName(call *ast.CallExpr) string {
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		return fun.Name
	case *ast.SelectorExpr:
		return fun.Sel.Name
	}
	return ""
}
