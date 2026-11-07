package gitlimit

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
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
// Scope, stated honestly: the audit matches calls by name inside one
// function. It cannot see git started through a command name held in a
// variable (it fails on any exec.Command whose name is not a string literal)
// or a helper in another package that starts git for its caller. A new
// function that builds git without a lease-aware starter, or starts it plain,
// fails here; a function that does both for different commands passes.

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
	"internal/githttp/hook.go:*":                                "runs inside a hook of a git that serve started with the lease: the hook and its git children inherit the lease descriptor",
	"internal/api/handlers.go:validRefFormat":                   "git check-ref-format reads no repository",
	"internal/api/handlers_write_merge.go:wmEnsureMergeTreeGit": "git version reads no repository",
	"internal/sync/worker.go:runGitTo":                          "runs in a temporary mirror under os.TempDir, never in a cache repository",
	"internal/simtest/gitenv.go:simGit":                         "test support: runs in the scratch repositories of simulations",
	"internal/gitread/heaplimit.go:heapLimitedGit":              "builder: gitread.Run starts the command through RunIn",
	"internal/api/handlers_write_commit.go:gitCmd":              "builder: its callers start the command through OutputIn or StartIn",
}

var leaseBuilders = map[string]bool{"heapLimitedGit": true, "gitCmd": true}

var plainStarters = map[string]bool{"Start": true, "Run": true, "Output": true, "CombinedOutput": true}

var leaseStarters = map[string]bool{"StartIn": true, "RunIn": true, "OutputIn": true, "CombinedOutputIn": true, "OpenLease": true}

type auditFunc struct {
	key      string // "<file>:<func>"
	builds   []token.Position
	plain    []token.Position
	leased   bool
	attaches bool
	direct   []token.Position // cmd.Start()/Run()/Output()/CombinedOutput()
}

func TestEveryGitStartTakesTheLease(t *testing.T) {
	funcs := auditModule(t)
	used := map[string]bool{}
	exempt := func(f *auditFunc) bool {
		file, _, _ := strings.Cut(f.key, ":")
		for _, k := range []string{f.key, file + ":*", filepath.Dir(file) + "/*"} {
			if _, ok := leaseExempt[k]; ok {
				used[k] = true
				return true
			}
		}
		return false
	}
	var bad []string
	for _, f := range funcs {
		if exempt(f) {
			continue
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
	}
	for k := range leaseExempt {
		if !used[k] {
			bad = append(bad, "leaseExempt entry "+k+" matches no git start: delete it")
		}
	}
	sort.Strings(bad)
	for _, b := range bad {
		t.Error(b)
	}
}

// auditModule parses the module's non-test Go files and returns one record
// per function that builds, starts or leases git.
func auditModule(t *testing.T) []*auditFunc {
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
		out = append(out, auditFile(fset, file, filepath.ToSlash(rel))...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) < 8 {
		t.Fatalf("the audit found %d functions that touch git; it no longer reads the module", len(out))
	}
	return out
}

func auditFile(fset *token.FileSet, file *ast.File, rel string) []*auditFunc {
	names := map[string]string{} // import path -> local name
	for _, imp := range file.Imports {
		p, _ := strconv.Unquote(imp.Path.Value)
		local := filepath.Base(p)
		if imp.Name != nil {
			local = imp.Name.Name
		}
		names[p] = local
	}
	execName, limitName := names[execImport], names[leaseImport]
	var out []*auditFunc
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		f := &auditFunc{key: rel + ":" + fn.Name.Name}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			pos := fset.Position(call.Pos())
			sel, isSel := call.Fun.(*ast.SelectorExpr)
			var recv string
			if isSel {
				if id, ok := sel.X.(*ast.Ident); ok {
					recv = id.Name
				}
			}
			switch {
			case isSel && recv != "" && recv == execName && (sel.Sel.Name == "Command" || sel.Sel.Name == "CommandContext"):
				arg := 0
				if sel.Sel.Name == "CommandContext" {
					arg = 1
				}
				if len(call.Args) > arg && !isStringLit(call.Args[arg], "git") && isStringLitAny(call.Args[arg]) {
					return true // another program, such as gpg
				}
				f.builds = append(f.builds, pos)
			case isSel && recv != "" && recv == limitName && sel.Sel.Name == "Command":
				f.builds = append(f.builds, pos)
			case isSel && recv != "" && recv == limitName && plainStarters[sel.Sel.Name]:
				f.plain = append(f.plain, pos)
			case isSel && recv != "" && recv == limitName && leaseStarters[sel.Sel.Name]:
				f.leased = true
			case isSel && sel.Sel.Name == "Attach" && len(call.Args) == 1:
				f.attaches = true
			case isSel && plainStarters[sel.Sel.Name] && len(call.Args) == 0:
				f.direct = append(f.direct, pos)
			}
			if name := calleeName(call); leaseBuilders[name] {
				f.builds = append(f.builds, pos)
			}
			return true
		})
		if len(f.builds)+len(f.plain)+len(f.direct) > 0 || f.leased {
			out = append(out, f)
		}
	}
	return out
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

func isStringLit(e ast.Expr, want string) bool {
	lit, ok := e.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return false
	}
	s, err := strconv.Unquote(lit.Value)
	return err == nil && s == want
}

func isStringLitAny(e ast.Expr) bool {
	lit, ok := e.(*ast.BasicLit)
	return ok && lit.Kind == token.STRING
}
