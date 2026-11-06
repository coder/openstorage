// facts.go reads the Go source that model/PackSweep.tla describes and
// prints two kinds of lines:
//
//	FACT <Name> <TRUE|FALSE> <evidence>
//	FP <label> <sha256>
//
// A FACT is one guard the model depends on, checked by call order in the
// AST; check.sh writes it into the TLC config, so breaking the guard in the
// code flips the model. An FP fingerprints one covered function's tokens
// (comments and formatting ignored); check.sh compares it with
// model/fingerprints.txt to tell when covered code changed.
//
// Run from the repository root:
//
//	go run .agents/skills/verify-orphan-sweep/facts/facts.go
//
// It uses only the standard library. Directories that start with a dot are
// outside `./...`, so the module's builds, tests and lint never see it.
package main

import (
	"bytes"
	"crypto/sha256"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/scanner"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var root = flag.String("root", ".", "repository root")

type source struct {
	path string
	fset *token.FileSet
	file *ast.File
	src  []byte
}

func load(rel string) *source {
	path := filepath.Join(*root, rel)
	src, err := os.ReadFile(path)
	if err != nil {
		fail("read %s: %v", rel, err)
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, src, parser.ParseComments)
	if err != nil {
		fail("parse %s: %v", rel, err)
	}
	return &source{path: rel, fset: fset, file: file, src: src}
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "facts: "+format+"\n", args...)
	os.Exit(3)
}

// fn finds a function, or a method when recv is non-empty. A covered
// function that no longer exists is a hard error: the model's anchor moved.
func (s *source) fn(recv, name string) *ast.FuncDecl {
	for _, d := range s.file.Decls {
		f, ok := d.(*ast.FuncDecl)
		if !ok || f.Name.Name != name {
			continue
		}
		if recvName(f) == recv {
			return f
		}
	}
	if recv != "" {
		fail("%s: method (%s).%s not found", s.path, recv, name)
	}
	fail("%s: func %s not found", s.path, name)
	return nil
}

func recvName(f *ast.FuncDecl) string {
	if f.Recv == nil || len(f.Recv.List) == 0 {
		return ""
	}
	t := f.Recv.List[0].Type
	if st, ok := t.(*ast.StarExpr); ok {
		t = st.X
	}
	if id, ok := t.(*ast.Ident); ok {
		return id.Name
	}
	return ""
}

func (s *source) text(n ast.Node) string {
	var b bytes.Buffer
	if err := printer.Fprint(&b, s.fset, n); err != nil {
		fail("print node in %s: %v", s.path, err)
	}
	return b.String()
}

type call struct {
	pos     token.Pos
	fun     string // printed callee, e.g. "s.blobs.Copy"
	args    string // printed arguments
	inClose bool   // inside a func literal
}

// calls lists every call in n in source order.
func (s *source) calls(n ast.Node) []call {
	var out []call
	var walk func(n ast.Node, inClose bool)
	walk = func(n ast.Node, inClose bool) {
		ast.Inspect(n, func(m ast.Node) bool {
			switch m := m.(type) {
			case *ast.FuncLit:
				if m != n {
					walk(m.Body, true)
					return false
				}
			case *ast.CallExpr:
				var args []string
				for _, a := range m.Args {
					args = append(args, s.text(a))
				}
				out = append(out, call{pos: m.Pos(), fun: s.text(m.Fun), args: strings.Join(args, ", "), inClose: inClose})
			}
			return true
		})
	}
	walk(n, false)
	return out
}

// first returns the first call to fun (outside func literals unless
// closures is set) whose arguments contain argHas, or token.NoPos.
func first(cs []call, fun, argHas string, closures bool) token.Pos {
	for _, c := range cs {
		if c.fun == fun && strings.Contains(c.args, argHas) && (closures || !c.inClose) {
			return c.pos
		}
	}
	return token.NoPos
}

func firstAfter(cs []call, fun string, after token.Pos) token.Pos {
	for _, c := range cs {
		if c.fun == fun && !c.inClose && c.pos > after {
			return c.pos
		}
	}
	return token.NoPos
}

func all(cs []call, fun string) []token.Pos {
	var out []token.Pos
	for _, c := range cs {
		if c.fun == fun {
			out = append(out, c.pos)
		}
	}
	return out
}

// exitBetween reports a return, break, continue or goto outside func
// literals strictly between a and b: any of them can skip the call at b.
func exitBetween(n ast.Node, a, b token.Pos) bool {
	found := false
	ast.Inspect(n, func(m ast.Node) bool {
		switch m := m.(type) {
		case *ast.FuncLit:
			return false
		case *ast.ReturnStmt, *ast.BranchStmt:
			if m.Pos() > a && m.Pos() < b {
				found = true
			}
		}
		return true
	})
	return found
}

// guardedBy reports whether the call at pos sits inside an if statement
// whose condition mentions name.
func (s *source) guardedBy(n ast.Node, pos token.Pos, name string) bool {
	var stack []ast.Node
	found := false
	ast.Inspect(n, func(m ast.Node) bool {
		if m == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		stack = append(stack, m)
		if c, ok := m.(*ast.CallExpr); ok && c.Pos() == pos {
			for _, p := range stack {
				if is, ok := p.(*ast.IfStmt); ok && strings.Contains(s.text(is.Cond), name) {
					found = true
				}
			}
		}
		return true
	})
	return found
}

type fact struct {
	name     string
	ok       bool
	evidence []string
}

func (f *fact) need(ok bool, format string, args ...any) {
	if !ok {
		f.ok = false
		f.evidence = append(f.evidence, "missing: "+fmt.Sprintf(format, args...))
	}
}

func newFact(name string) *fact { return &fact{name: name, ok: true} }

// writerVerifies: RegisterPacks, then EnsurePublished, in both writers.
func writerPositions(s *source, f *ast.FuncDecl, register string) (rp, ep token.Pos) {
	cs := s.calls(f.Body)
	rp = first(cs, register, "", false)
	ep = firstAfter(cs, "blob.EnsurePublished", rp)
	return rp, ep
}

func main() {
	flag.Parse()
	sweep := load("internal/jobs/sweep.go")
	blobGo := load("internal/blob/blob.go")
	publish := load("internal/api/publish.go")
	hook := load("internal/githttp/hook.go")
	repack := load("internal/jobs/repack.go")

	preReceive := hook.fn("", "preReceive")
	publishPack := publish.fn("server", "publishPack")
	ensure := blobGo.fn("", "EnsurePublished")
	safeDelete := sweep.fn("Sweeper", "safeDelete")
	sweepFn := sweep.fn("Sweeper", "sweep")
	healEntry := sweep.fn("Sweeper", "healQuarantineEntry")
	purgeAge := sweep.fn("Sweeper", "quarantinePurgeAge")

	var facts []*fact

	// WriterVerifies and VerifyOnRegisterError.
	wv := newFact("WriterVerifies")
	vr := newFact("VerifyOnRegisterError")
	for _, w := range []struct {
		src      *source
		fn       *ast.FuncDecl
		register string
		label    string
	}{
		{hook, preReceive, "st.RegisterPacks", "hook.go preReceive"},
		{publish, publishPack, "s.refStore.RegisterPacks", "publish.go publishPack"},
	} {
		rp, ep := writerPositions(w.src, w.fn, w.register)
		wv.need(rp.IsValid(), "%s calls %s", w.label, w.register)
		wv.need(ep.IsValid(), "%s calls blob.EnsurePublished after %s", w.label, w.register)
		if rp.IsValid() && ep.IsValid() {
			vr.need(!exitBetween(w.fn.Body, rp, ep), "%s has no return, break, continue or goto between %s and blob.EnsurePublished", w.label, w.register)
			vr.need(!w.src.guardedBy(w.fn.Body, ep, "regErr"), "%s does not make blob.EnsurePublished conditional on regErr", w.label)
		} else {
			vr.need(false, "%s: RegisterPacks/EnsurePublished pair not found", w.label)
		}
	}
	{
		cs := blobGo.calls(ensure.Body)
		st := first(cs, "b.Stat", "key", false)
		nf := token.NoPos
		for _, c := range cs {
			if c.fun == "errors.Is" && strings.Contains(c.args, "ErrNotFound") && c.pos > st {
				nf = c.pos
				break
			}
		}
		up := firstAfter(cs, "b.UploadFile", nf)
		wv.need(st.IsValid(), "EnsurePublished calls b.Stat(ctx, key)")
		wv.need(nf.IsValid(), "EnsurePublished checks errors.Is(err, ErrNotFound) after b.Stat")
		wv.need(up.IsValid(), "EnsurePublished calls b.UploadFile after the ErrNotFound check")
	}
	{
		// The repack publishes its consolidated pack the same way:
		// UploadFile, ReplacePacks (the registration), verifyPublished.
		// It verifies on the won path, the ambiguous-error path and the
		// lost-but-registered path.
		rp := repack.fn("Repacker", "repack")
		cs := repack.calls(rp.Body)
		swap := first(cs, "r.store.ReplacePacks", "", false)
		wv.need(swap.IsValid(), "Repacker.repack calls r.store.ReplacePacks")
		n := 0
		for _, c := range cs {
			if c.fun == "verifyPublished" && c.pos > swap && !c.inClose {
				n++
			}
		}
		wv.need(n >= 3, "Repacker.repack calls verifyPublished after ReplacePacks on its won, ambiguous and lost-but-registered paths (found %d calls, want 3)", n)
		vp := repack.fn("", "verifyPublished")
		vcs := repack.calls(vp.Body)
		st := first(vcs, "bl.Stat", "key", false)
		nf := token.NoPos
		for _, c := range vcs {
			if c.fun == "errors.Is" && strings.Contains(c.args, "ErrNotFound") && c.pos > st {
				nf = c.pos
				break
			}
		}
		wv.need(st.IsValid() && nf.IsValid() && firstAfter(vcs, "bl.UploadFile", nf).IsValid(),
			"verifyPublished calls bl.Stat, checks ErrNotFound, then bl.UploadFile")
	}
	facts = append(facts, wv, vr)

	sdCalls := sweep.calls(safeDelete.Body)

	// CopyBeforeDelete.
	cb := newFact("CopyBeforeDelete")
	cp := first(sdCalls, "s.blobs.Copy", "qKey, key", false)
	cb.need(cp.IsValid(), "safeDelete calls s.blobs.Copy(ctx, qKey, key)")
	dels := all(sdCalls, "s.blobs.Delete")
	cb.need(len(dels) > 0, "safeDelete calls s.blobs.Delete")
	for _, d := range dels {
		cb.need(cp.IsValid() && cp < d, "s.blobs.Copy comes before every s.blobs.Delete in safeDelete")
	}
	facts = append(facts, cb)

	// RecheckAfterDelete: the pack delete (outside the LFS closure), then
	// s.registered, then s.restoreFromQuarantine.
	ra := newFact("RecheckAfterDelete")
	packDel := first(sdCalls, "s.blobs.Delete", "hctx, key", false)
	reg := firstAfter(sdCalls, "s.registered", packDel)
	rest := firstAfter(sdCalls, "s.restoreFromQuarantine", reg)
	ra.need(packDel.IsValid(), "safeDelete calls s.blobs.Delete(hctx, key)")
	ra.need(packDel.IsValid() && reg.IsValid(), "safeDelete calls s.registered after the delete")
	ra.need(reg.IsValid() && rest.IsValid(), "safeDelete calls s.restoreFromQuarantine after the recheck")
	facts = append(facts, ra)

	// HealEverySweep: healQuarantine before the packs/ listing.
	he := newFact("HealEverySweep")
	swCalls := sweep.calls(sweepFn.Body)
	heal := first(swCalls, "s.healQuarantine", "", false)
	list := first(swCalls, "s.blobs.ListPages", "blob.PacksRoot", false)
	he.need(heal.IsValid(), "sweep calls s.healQuarantine")
	he.need(list.IsValid(), "sweep lists blob.PacksRoot")
	he.need(heal.IsValid() && list.IsValid() && heal < list, "s.healQuarantine comes before the packs/ listing")
	facts = append(facts, he)

	// HealRestores and PurgeAgeGated.
	hr := newFact("HealRestores")
	pg := newFact("PurgeAgeGated")
	hCalls := sweep.calls(healEntry.Body)
	hreg := first(hCalls, "s.registered", "orig", false)
	hrest := firstAfter(hCalls, "s.restoreFromQuarantine", hreg)
	purge := first(hCalls, "s.blobs.Delete", "q.Key", false)
	hr.need(hreg.IsValid(), "healQuarantineEntry calls s.registered(ctx, repo, orig)")
	hr.need(hrest.IsValid(), "healQuarantineEntry calls s.restoreFromQuarantine after the registry read")
	hr.need(purge.IsValid() && hrest.IsValid() && hrest < purge, "the restore comes before the purge s.blobs.Delete(ctx, q.Key)")
	gate := token.NoPos
	ast.Inspect(healEntry.Body, func(n ast.Node) bool {
		if is, ok := n.(*ast.IfStmt); ok && strings.Contains(sweep.text(is.Cond), "s.quarantinePurgeAge()") {
			for _, st := range is.Body.List {
				if _, ok := st.(*ast.ReturnStmt); ok {
					gate = is.Pos()
				}
			}
		}
		return true
	})
	pg.need(gate.IsValid(), "healQuarantineEntry returns early when the backup is younger than s.quarantinePurgeAge()")
	pg.need(gate.IsValid() && purge.IsValid() && gate < purge, "the age gate comes before the purge")
	facts = append(facts, hr, pg)

	// PurgeFloorCoversHandshake: QuarantinePurgeFloor = N * sweepRecoveryTimeout
	// with N >= 2, and both safeDelete phases are deadline-bounded by it.
	pf := newFact("PurgeFloorCoversHandshake")
	mult := constMultiplier(sweep, "QuarantinePurgeFloor", "sweepRecoveryTimeout")
	pf.need(mult >= 2, "QuarantinePurgeFloor = N * sweepRecoveryTimeout with N >= 2 (found N=%d)", mult)
	timeouts := 0
	for _, c := range sdCalls {
		if c.fun == "context.WithTimeout" && strings.Contains(c.args, "sweepRecoveryTimeout") {
			timeouts++
		}
	}
	pf.need(timeouts >= 2, "safeDelete bounds two phases with context.WithTimeout(..., sweepRecoveryTimeout) (found %d)", timeouts)
	facts = append(facts, pf)

	// PurgeAgeCoversThreshold: return max(s.cfg.Threshold, QuarantinePurgeFloor).
	pt := newFact("PurgeAgeCoversThreshold")
	ret := ""
	if len(purgeAge.Body.List) == 1 {
		if r, ok := purgeAge.Body.List[0].(*ast.ReturnStmt); ok && len(r.Results) == 1 {
			ret = sweep.text(r.Results[0])
		}
	}
	pt.need(strings.HasPrefix(ret, "max(") && strings.Contains(ret, "s.cfg.Threshold") && strings.Contains(ret, "QuarantinePurgeFloor"),
		"quarantinePurgeAge returns max(s.cfg.Threshold, QuarantinePurgeFloor) (found %q)", ret)
	facts = append(facts, pt)

	facts = append(facts, purgeFacts(sweep, sweepFn, healEntry)...)

	for _, f := range facts {
		ev := "ok"
		if !f.ok {
			ev = strings.Join(f.evidence, "; ")
		}
		fmt.Printf("FACT %s %s %s\n", f.name, strings.ToUpper(strconv.FormatBool(f.ok)), ev)
	}

	// Fingerprints of every covered function.
	var covers []cover
	for _, m := range []string{"sweep", "classify", "revalidateOrphan", "freshModTime", "safeDelete",
		"restoreFromQuarantine", "healQuarantine", "healQuarantineEntry", "healPresentOriginal",
		"quarantinePurgeAge", "registered"} {
		covers = append(covers, cover{sweep, "internal/jobs/sweep.go:Sweeper." + m, sweep.fn("Sweeper", m)})
	}
	covers = append(covers,
		cover{sweep, "internal/jobs/sweep.go:quarantineKey", sweep.fn("", "quarantineKey")},
		cover{sweep, "internal/jobs/sweep.go:const QuarantinePurgeFloor", constSpec(sweep, "QuarantinePurgeFloor")},
		cover{sweep, "internal/jobs/sweep.go:const sweepRecoveryTimeout", constSpec(sweep, "sweepRecoveryTimeout")},
		cover{blobGo, "internal/blob/blob.go:EnsurePublished", ensure},
		cover{publish, "internal/api/publish.go:server.publishPack", publishPack},
		cover{repack, "internal/jobs/repack.go:Repacker.repack", repack.fn("Repacker", "repack")},
		cover{repack, "internal/jobs/repack.go:verifyPublished", repack.fn("", "verifyPublished")},
		cover{sweep, "internal/jobs/sweep.go:SweepSchedule.runOnce", sweep.fn("SweepSchedule", "runOnce")},
	)
	covers = append(covers, purgeCovers()...)
	for _, c := range covers {
		fmt.Printf("FP %s %s\n", strings.ReplaceAll(c.label, " ", "_"), c.src.fingerprint(c.node.Pos(), c.node.End()))
	}
	// preReceive is long and changes often: fingerprint only its
	// publication block, from `published := ...` through the
	// EnsurePublished loop.
	start, end := publicationBlock(hook, preReceive)
	fmt.Printf("FP internal/githttp/hook.go:preReceive[publication] %s\n", hook.fingerprint(start, end))
}

func constSpec(s *source, name string) ast.Node {
	for _, d := range s.file.Decls {
		g, ok := d.(*ast.GenDecl)
		if !ok || g.Tok != token.CONST {
			continue
		}
		for _, sp := range g.Specs {
			v := sp.(*ast.ValueSpec)
			for _, n := range v.Names {
				if n.Name == name {
					return v
				}
			}
		}
	}
	fail("%s: const %s not found", s.path, name)
	return nil
}

// constMultiplier returns N for `const name = N * unit`, or 0.
func constMultiplier(s *source, name, unit string) int {
	v := constSpec(s, name).(*ast.ValueSpec)
	if len(v.Values) != 1 {
		return 0
	}
	be, ok := v.Values[0].(*ast.BinaryExpr)
	if !ok || be.Op != token.MUL {
		return 0
	}
	lit, ok1 := be.X.(*ast.BasicLit)
	id, ok2 := be.Y.(*ast.Ident)
	if !ok1 || !ok2 || id.Name != unit || lit.Kind != token.INT {
		return 0
	}
	n, err := strconv.Atoi(lit.Value)
	if err != nil {
		return 0
	}
	return n
}

func publicationBlock(s *source, f *ast.FuncDecl) (token.Pos, token.Pos) {
	start, end := token.NoPos, token.NoPos
	for _, st := range f.Body.List {
		if as, ok := st.(*ast.AssignStmt); ok && !start.IsValid() {
			for _, l := range as.Lhs {
				if id, ok := l.(*ast.Ident); ok && id.Name == "published" {
					start = st.Pos()
				}
			}
		}
		if start.IsValid() && strings.Contains(s.text(st), "blob.EnsurePublished") {
			end = st.End()
			break
		}
	}
	if !start.IsValid() || !end.IsValid() {
		fail("%s: preReceive publication block (published := ... through blob.EnsurePublished) not found", s.path)
	}
	return start, end
}

// fingerprint hashes the token stream in [pos, end): comments and layout
// changes keep the fingerprint; any code change moves it.
func (s *source) fingerprint(pos, end token.Pos) string {
	tf := s.fset.File(pos)
	lo, hi := tf.Offset(pos), tf.Offset(end)
	var sc scanner.Scanner
	fs := token.NewFileSet()
	sc.Init(fs.AddFile("", -1, hi-lo), s.src[lo:hi], nil, 0)
	h := sha256.New()
	for {
		_, tok, lit := sc.Scan()
		if tok == token.EOF {
			break
		}
		if tok == token.SEMICOLON && lit == "\n" {
			continue // automatic semicolons follow the layout
		}
		fmt.Fprintf(h, "%s %s\n", tok, lit)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

type cover struct {
	src   *source
	label string
	node  ast.Node
}

// Purge sources: model/RepoPurge.tla.
var purgeSrcs = map[string]*source{}

func src(rel string) *source {
	if s, ok := purgeSrcs[rel]; ok {
		return s
	}
	s := load(rel)
	purgeSrcs[rel] = s
	return s
}

type ifInfo struct {
	stmt             *ast.IfStmt
	init, cond, body string
	returns          bool
}

// ifs lists every if statement in n, func literals and else-ifs included.
func (s *source) ifs(n ast.Node) []ifInfo {
	var out []ifInfo
	ast.Inspect(n, func(m ast.Node) bool {
		is, ok := m.(*ast.IfStmt)
		if !ok {
			return true
		}
		i := ifInfo{stmt: is, cond: s.text(is.Cond), body: s.text(is.Body)}
		if is.Init != nil {
			i.init = s.text(is.Init)
		}
		for _, st := range is.Body.List {
			if _, ok := st.(*ast.ReturnStmt); ok {
				i.returns = true
			}
		}
		out = append(out, i)
		return true
	})
	return out
}

// assignsFalse reports an assignment of false to name directly in body.
func (s *source) assignsFalse(body *ast.BlockStmt, name string) bool {
	for _, st := range body.List {
		as, ok := st.(*ast.AssignStmt)
		if !ok || len(as.Lhs) != len(as.Rhs) {
			continue
		}
		for i, l := range as.Lhs {
			if s.text(l) == name && s.text(as.Rhs[i]) == "false" {
				return true
			}
		}
	}
	return false
}

// writesAfter reports that every call to exec whose arguments write a
// table (INSERT, UPDATE, DELETE) sits after pos, and that one of them
// writes stmt.
func writesAfter(cs []call, exec, stmt string, pos token.Pos) bool {
	found := false
	for _, c := range cs {
		if c.fun != exec {
			continue
		}
		if !strings.Contains(c.args, "INSERT INTO") && !strings.Contains(c.args, "UPDATE ") && !strings.Contains(c.args, "DELETE FROM") {
			continue
		}
		if c.pos < pos {
			return false
		}
		found = found || strings.Contains(c.args, stmt)
	}
	return found
}

// fencedSQL checks a SQL registry write: lock (a call that must come before
// the fence read), the fence read on tx, an if that returns ErrRepoPurged,
// then every write statement.
func fencedSQL(s *source, f *ast.FuncDecl, lock, fence, exec, stmt string) bool {
	cs := s.calls(f.Body)
	fp := first(cs, fence, "tx", true)
	lp := token.NoPos
	for _, c := range cs {
		if (c.fun == lock || strings.HasSuffix(c.fun, lock)) && c.pos < fp {
			lp = c.pos
			break
		}
	}
	ret := token.NoPos
	for _, i := range s.ifs(f.Body) {
		if i.returns && strings.Contains(i.body, "ErrRepoPurged") && i.stmt.Pos() > fp {
			ret = i.stmt.Pos()
			break
		}
	}
	return fp.IsValid() && lp.IsValid() && ret.IsValid() && writesAfter(cs, exec, stmt, ret)
}

// mutateFn returns the fn closure of the s.mutate call on the base
// repository's manifest in f, or nil.
func mutateFn(s *source, f *ast.FuncDecl) *ast.FuncLit {
	baseVar := strings.Contains(s.text(f.Body), "base := baseRepoOf(repo)")
	var out *ast.FuncLit
	ast.Inspect(f.Body, func(n ast.Node) bool {
		c, ok := n.(*ast.CallExpr)
		if !ok || s.text(c.Fun) != "s.mutate" || len(c.Args) != 3 {
			return true
		}
		key := s.text(c.Args[1])
		if key != "baseRepoOf(repo)" && key != "baseRepoOf(group[0])" && !(key == "base" && (baseVar || f.Name.Name == "FenceRepoPurge")) {
			return true
		}
		if lit, ok := c.Args[2].(*ast.CompositeLit); ok {
			for _, e := range lit.Elts {
				if kv, ok := e.(*ast.KeyValueExpr); ok && s.text(kv.Key) == "fn" {
					out, _ = kv.Value.(*ast.FuncLit)
				}
			}
		}
		return true
	})
	return out
}

// firstGate reports that fn's closure starts with an if on cond that
// returns (bodyHas in its body), before every other call in the closure.
func firstGate(s *source, fn *ast.FuncLit, cond, bodyHas string) bool {
	if fn == nil {
		return false
	}
	for _, i := range s.ifs(fn.Body) {
		if i.cond != cond || !i.returns || !strings.Contains(i.body, bodyHas) {
			continue
		}
		for _, c := range s.calls(fn.Body) {
			if c.pos < i.stmt.Pos() {
				return false
			}
		}
		return true
	}
	return false
}

// fencedFold checks a SQL ReplacePacks: the fence is read on tx after lock,
// an if that returns ErrRepoPurged follows, and the read sits in the
// transaction (PG: the pgx.BeginFunc closure; SQLite: before tx.Commit).
// A write before the read is rolled back by that return, so it may come
// first (the fold's old-row delete is its precondition).
func fencedFold(s *source, f *ast.FuncDecl, lock, fence string, pg bool) bool {
	cs := s.calls(f.Body)
	fi := -1
	for i, c := range cs {
		if c.fun == fence && strings.Contains(c.args, "tx") {
			fi = i
			break
		}
	}
	if fi < 0 {
		return false
	}
	fp := cs[fi].pos
	locked, ret, inTx := false, false, false
	for _, c := range cs {
		if (c.fun == lock || strings.HasSuffix(c.fun, lock)) && c.pos < fp {
			locked = true
		}
		if !pg && c.fun == "tx.Commit" && c.pos > fp {
			inTx = true
		}
	}
	if pg {
		inTx = cs[fi].inClose && first(cs, "pgx.BeginFunc", "", false).IsValid()
	}
	for _, i := range s.ifs(f.Body) {
		if i.returns && strings.Contains(i.body, "ErrRepoPurged") && i.stmt.Pos() > fp {
			ret = true
		}
	}
	return locked && ret && inTx
}

// gateBeforeWrites reports that fn's closure has an if on cond that
// returns (bodyHas in its body) before its first write to the manifest: an
// assignment or increment of an m. or mem. field, or m.EnsureMember.
func gateBeforeWrites(s *source, fn *ast.FuncLit, cond, bodyHas string) bool {
	if fn == nil {
		return false
	}
	gate := token.NoPos
	for _, i := range s.ifs(fn.Body) {
		if i.cond == cond && i.returns && strings.Contains(i.body, bodyHas) {
			gate = i.stmt.Pos()
			break
		}
	}
	if !gate.IsValid() {
		return false
	}
	ok := true
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		var lhs []ast.Expr
		switch st := n.(type) {
		case *ast.FuncLit:
			return false
		case *ast.AssignStmt:
			lhs = st.Lhs
		case *ast.IncDecStmt:
			lhs = []ast.Expr{st.X}
		case *ast.CallExpr:
			if s.text(st.Fun) == "m.EnsureMember" && st.Pos() < gate {
				ok = false
			}
		}
		for _, l := range lhs {
			if t := s.text(l); (strings.HasPrefix(t, "m.") || strings.HasPrefix(t, "mem.")) && n.Pos() < gate {
				ok = false
			}
		}
		return true
	})
	return ok
}

func purgeFacts(sweep *source, sweepFn, healEntry *ast.FuncDecl) []*fact {
	pj := src("internal/jobs/purge.go")
	expired := pj.fn("RepoPurger", "PurgeExpired")
	purgeAPI := pj.fn("RepoPurger", "PurgeRepo")
	purgeRepo := pj.fn("RepoPurger", "purgeRepo")
	var facts []*fact

	// PurgeGuarded: DeleteGuard first, once per pass, return on error.
	pg := newFact("PurgeGuarded")
	for _, f := range []struct {
		fn     *ast.FuncDecl
		label  string
		before []string
	}{
		{expired, "PurgeExpired", []string{"p.meta.PurgeableRepos", "p.purgeRepo"}},
		{purgeAPI, "PurgeRepo", []string{"p.purgeRepo"}},
	} {
		g := token.NoPos
		for _, i := range pj.ifs(f.fn.Body) {
			if strings.Contains(i.init, "guard(ctx)") && i.cond == "err != nil" && i.returns {
				g = i.stmt.Pos()
				break
			}
		}
		pg.need(g.IsValid() && strings.Contains(pj.text(f.fn.Body), "p.sweeper.cfg.DeleteGuard"),
			"%s returns when p.sweeper.cfg.DeleteGuard fails", f.label)
		cs := pj.calls(f.fn.Body)
		for _, b := range f.before {
			c := first(cs, b, "", false)
			pg.need(g.IsValid() && c.IsValid() && g < c, "%s consults the guard before %s", f.label, b)
		}
	}
	facts = append(facts, pg)

	// SweepGuardStopsPurgeMode: a guard error turns purgeMode off before
	// the heal and the listings use it.
	sg := newFact("SweepGuardStopsPurgeMode")
	gpos := token.NoPos
	for _, i := range sweep.ifs(sweepFn.Body) {
		if !strings.Contains(i.cond, "purgeMode") || !strings.Contains(i.cond, "s.cfg.DeleteGuard != nil") {
			continue
		}
		for _, j := range sweep.ifs(i.stmt.Body) {
			if strings.Contains(j.init, "s.cfg.DeleteGuard(ctx)") && j.cond == "err != nil" && sweep.assignsFalse(j.stmt.Body, "purgeMode") {
				gpos = i.stmt.Pos()
			}
		}
	}
	swCalls := sweep.calls(sweepFn.Body)
	hq := first(swCalls, "s.healQuarantine", "", false)
	cl := first(swCalls, "s.classify", "purgeMode", true)
	sg.need(gpos.IsValid(), "sweep sets purgeMode to false when s.cfg.DeleteGuard(ctx) fails")
	sg.need(gpos.IsValid() && hq.IsValid() && cl.IsValid() && gpos < hq && gpos < cl,
		"the guard comes before s.healQuarantine and s.classify get purgeMode")
	facts = append(facts, sg)

	pr := pj.calls(purgeRepo.Body)
	fence := first(pr, "p.registry.FenceRepoPurge", "", false)
	drop := first(pr, "p.registry.DropRepoRefs", "", false)
	retire := first(pr, "p.registry.RetireRepoPacks", "", false)
	reap := first(pr, "p.reapPrefix", "", false)

	// RefusesLive and RefusesYoung: purgeRepo's eligibility returns.
	rl := newFact("RefusesLive")
	ry := newFact("RefusesYoung")
	live, young := token.NoPos, token.NoPos
	for _, i := range pj.ifs(purgeRepo.Body) {
		if i.cond == "state.Repo.DeletedAt == nil" && i.returns && !live.IsValid() {
			live = i.stmt.Pos()
		}
		if strings.Contains(i.init, "state.Repo.DeletedAt") && strings.Contains(i.cond, "< MinRepoPurgeAge") && i.returns && !young.IsValid() {
			young = i.stmt.Pos()
		}
	}
	rl.need(live.IsValid() && fence.IsValid() && live < fence, "purgeRepo returns when state.Repo.DeletedAt == nil, before FenceRepoPurge")
	ry.need(young.IsValid() && fence.IsValid() && young < fence, "purgeRepo returns when the deletion is younger than MinRepoPurgeAge, before FenceRepoPurge")
	facts = append(facts, rl, ry)

	// CutoffCoversRetention: the background listing's cutoff.
	cc := newFact("CutoffCoversRetention")
	cutoff := ""
	ast.Inspect(expired.Body, func(n ast.Node) bool {
		if as, ok := n.(*ast.AssignStmt); ok && len(as.Lhs) == 1 && pj.text(as.Lhs[0]) == "cutoff" {
			cutoff = pj.text(as.Rhs[0])
		}
		return true
	})
	cc.need(cutoff == "p.now().Add(-max(p.retention, MinRepoPurgeAge))" || cutoff == "p.now().Add(-max(MinRepoPurgeAge, p.retention))",
		"PurgeExpired sets cutoff := p.now().Add(-max(p.retention, MinRepoPurgeAge)) (found %q)", cutoff)
	lists := 0
	for _, c := range pj.calls(expired.Body) {
		if c.fun == "p.meta.PurgeableRepos" {
			lists++
			cc.need(strings.HasPrefix(c.args, "ctx, cutoff,"), "every p.meta.PurgeableRepos call lists up to cutoff")
		}
	}
	cc.need(lists > 0, "PurgeExpired calls p.meta.PurgeableRepos")
	facts = append(facts, cc)

	// CursorWraps: an empty batch past a cursor relists from the start.
	cw := newFact("CursorWraps")
	wraps := false
	for _, i := range pj.ifs(expired.Body) {
		if strings.Contains(i.cond, "len(batch) == 0") && strings.Contains(i.cond, "p.cursor") &&
			strings.Contains(i.body, "p.cursor = meta.PurgeCandidate{}") && strings.Contains(i.body, "p.meta.PurgeableRepos") {
			wraps = true
		}
	}
	cw.need(wraps, "PurgeExpired resets p.cursor and relists when the batch is empty and the cursor is not")
	facts = append(facts, cw)

	// Phase order.
	ff := newFact("FenceFirst")
	ff.need(fence.IsValid() && drop.IsValid() && retire.IsValid() && reap.IsValid() && fence < drop && fence < retire && fence < reap,
		"purgeRepo calls p.registry.FenceRepoPurge before DropRepoRefs, RetireRepoPacks and p.reapPrefix")
	rd := newFact("RefsDieFirst")
	rd.need(drop.IsValid() && retire.IsValid() && reap.IsValid() && drop < retire && drop < reap,
		"purgeRepo calls p.registry.DropRepoRefs before RetireRepoPacks and p.reapPrefix")
	facts = append(facts, ff, rd)

	// RegistrationsFenced and RefWritesFenced: the fence read sits in the
	// write's own transaction, ordered against FenceRepoPurge.
	refGo, ledger, purgePG := src("internal/ref/ref.go"), src("internal/ref/ledger.go"), src("internal/ref/purge.go")
	sq, ledgerSQ, purgeSQ := src("internal/ref/sqlite.go"), src("internal/ref/ledger_sqlite.go"), src("internal/ref/purge_sqlite.go")
	wal, walLFS, walPurge := src("internal/ref/s3walstore.go"), src("internal/ref/s3walstore_lfs.go"), src("internal/ref/s3walstore_purge.go")
	immediate := false
	ast.Inspect(sq.file, func(n ast.Node) bool {
		if bl, ok := n.(*ast.BasicLit); ok && bl.Kind == token.STRING && strings.Contains(bl.Value, "_txlock=immediate") {
			immediate = true
		}
		return true
	})
	fenceSerialized := func(f *fact) {
		cs := purgePG.calls(purgePG.fn("PG", "FenceRepoPurge").Body)
		lp := first(cs, "lockBaseLedger", "tx", true)
		f.need(lp.IsValid() && writesAfter(cs, "tx.Exec", "INSERT INTO purged_repos", lp), "PG FenceRepoPurge takes lockBaseLedger before its insert")
		f.need(immediate, "sqlite.go opens the database with _txlock=immediate (BEGIN IMMEDIATE serializes writers)")
		f.need(first(purgeSQ.calls(purgeSQ.fn("SQLite", "FenceRepoPurge").Body), "s.db.ExecContext", "INSERT INTO purged_repos", false).IsValid(),
			"SQLite FenceRepoPurge inserts the purged_repos row (one write, ordered by the database write lock)")
		fn := mutateFn(walPurge, walPurge.fn("S3WAL", "FenceRepoPurge"))
		f.need(fn != nil && strings.Contains(walPurge.text(fn.Body), "m.PurgedAt = &stamp"),
			"S3WAL FenceRepoPurge sets PurgedAt in a mutation of the base manifest")
	}
	rf := newFact("RegistrationsFenced")
	fenceSerialized(rf)
	rf.need(fencedSQL(refGo, refGo.fn("PG", "RegisterPacks"), "lockBaseLedger", "repoPurgeFencedPG", "tx.Exec", "INSERT INTO packs"),
		"PG RegisterPacks: lockBaseLedger, repoPurgeFencedPG, return ErrRepoPurged, then its writes")
	rf.need(fencedSQL(ledger, ledger.fn("PG", "registerLFSObjectTx"), "lockBaseLedger", "repoPurgeFencedPG", "tx.Exec", "INSERT INTO lfs_objects"),
		"PG registerLFSObjectTx: lockBaseLedger, repoPurgeFencedPG, return ErrRepoPurged, then its writes")
	rf.need(fencedSQL(sq, sq.fn("SQLite", "RegisterPacks"), ".BeginTx", "repoPurgeFencedSQLite", "tx.ExecContext", "INSERT INTO packs"),
		"SQLite RegisterPacks: BeginTx, repoPurgeFencedSQLite, return ErrRepoPurged, then its writes")
	rf.need(fencedSQL(ledgerSQ, ledgerSQ.fn("SQLite", "registerLFSObjectTx"), ".BeginTx", "repoPurgeFencedSQLite", "tx.ExecContext", "INSERT INTO lfs_objects"),
		"SQLite registerLFSObjectTx: BeginTx, repoPurgeFencedSQLite, return ErrRepoPurged, then its writes")
	rf.need(firstGate(wal, mutateFn(wal, wal.fn("S3WAL", "RegisterPacks")), "m.PurgedAt != nil", "ErrRepoPurged"),
		"S3WAL RegisterPacks returns ErrRepoPurged first in its base-manifest mutation")
	rf.need(firstGate(walLFS, mutateFn(walLFS, walLFS.fn("S3WAL", "registerLFSObject")), "m.PurgedAt != nil", "ErrRepoPurged"),
		"S3WAL registerLFSObject returns ErrRepoPurged first in its base-manifest mutation")
	rw := newFact("RefWritesFenced")
	fenceSerialized(rw)
	rw.need(fencedSQL(refGo, refGo.fn("PG", "casRef"), "lockBaseLedger", "repoPurgeFencedPG", "tx.Exec", "INSERT INTO refs"),
		"PG casRef: lockBaseLedger, repoPurgeFencedPG, return ErrRepoPurged, then its writes")
	rw.need(fencedSQL(sq, sq.fn("SQLite", "casRef"), ".BeginTx", "repoPurgeFencedSQLite", "tx.ExecContext", "INSERT INTO refs"),
		"SQLite casRef: BeginTx, repoPurgeFencedSQLite, return ErrRepoPurged, then its writes")
	rw.need(firstGate(wal, mutateFn(wal, wal.fn("S3WAL", "casRef")), "m.PurgedAt != nil", "ErrRepoPurged"),
		"S3WAL casRef returns ErrRepoPurged first in its base-manifest mutation")
	facts = append(facts, rf, rw)

	// FoldsFenced: ReplacePacks inserts a pack row, so it reads the fence
	// in its own transaction like RegisterPacks.
	fo := newFact("FoldsFenced")
	fenceSerialized(fo)
	fo.need(fencedFold(refGo, refGo.fn("PG", "ReplacePacks"), "lockBaseLedger", "repoPurgeFencedPG", true),
		"PG ReplacePacks: lockBaseLedger, then repoPurgeFencedPG in its pgx.BeginFunc transaction, then return ErrRepoPurged")
	fo.need(fencedFold(sq, sq.fn("SQLite", "ReplacePacks"), ".BeginTx", "repoPurgeFencedSQLite", false),
		"SQLite ReplacePacks: BeginTx, then repoPurgeFencedSQLite, then return ErrRepoPurged, before tx.Commit")
	fo.need(gateBeforeWrites(wal, mutateFn(wal, wal.fn("S3WAL", "ReplacePacks")), "m.PurgedAt != nil", "ErrRepoPurged"),
		"S3WAL ReplacePacks returns ErrRepoPurged in its base-manifest mutation before it writes the manifest")
	facts = append(facts, fo)

	// SweepReapsFenced: classify's PurgedRepoProbe branch.
	classify := sweep.fn("Sweeper", "classify")
	sr := newFact("SweepReapsFenced")
	branch := token.NoPos
	for _, i := range sweep.ifs(classify.Body) {
		if strings.Contains(i.cond, "!canDelete && purgeMode") && strings.Contains(i.body, "s.cfg.PurgedRepoProbe") && strings.Contains(i.body, "canDelete = purged") {
			branch = i.stmt.Pos()
		}
	}
	sd := first(sweep.calls(classify.Body), "s.safeDelete", "", false)
	sr.need(branch.IsValid() && sd.IsValid() && branch < sd, "classify sets canDelete from s.cfg.PurgedRepoProbe under purgeMode, before s.safeDelete")
	facts = append(facts, sr)

	// HealPurgesFenced: purgeMode reaches the heal's purge gate.
	hp := newFact("HealPurgesFenced")
	hpArgs := ""
	for _, c := range swCalls {
		if c.fun == "s.healQuarantine" {
			hpArgs = c.args
		}
	}
	hp.need(strings.HasSuffix(hpArgs, ", purgeMode"), "sweep passes purgeMode as healQuarantine's purgeFenced (found %q)", hpArgs)
	healQ := sweep.fn("Sweeper", "healQuarantine")
	hp.need(first(sweep.calls(healQ.Body), "s.healQuarantineEntry", "purgeAll, purgeFenced", true).IsValid(),
		"healQuarantine passes purgeAll, purgeFenced to healQuarantineEntry")
	gate := false
	for _, i := range sweep.ifs(healEntry.Body) {
		if strings.Contains(i.cond, "!purgeAll && !purgeFenced") && i.returns {
			gate = true
		}
	}
	hp.need(gate, "healQuarantineEntry's purge gate lets purgeFenced alone through (!purgeAll && !purgeFenced)")
	facts = append(facts, hp)
	return facts
}

// purgeCovers lists the code model/RepoPurge.tla describes.
func purgeCovers() []cover {
	var out []cover
	add := func(rel, recv string, names ...string) {
		s := src(rel)
		for _, n := range names {
			label := rel + ":" + n
			if recv != "" {
				label = rel + ":" + recv + "." + n
			}
			out = append(out, cover{s, label, s.fn(recv, n)})
		}
	}
	add("internal/jobs/purge.go", "RepoPurger", "PurgeExpired", "PurgeRepo", "purgeRepo", "rescanPurged", "reapPrefix")
	pj := src("internal/jobs/purge.go")
	out = append(out, cover{pj, "internal/jobs/purge.go:const MinRepoPurgeAge", constSpec(pj, "MinRepoPurgeAge")})
	add("internal/ref/ref.go", "PG", "casRef", "RegisterPacks", "ReplacePacks")
	add("internal/ref/ref.go", "", "validateReplacePacks")
	add("internal/ref/ledger.go", "PG", "registerLFSObjectTx")
	add("internal/ref/ledger.go", "", "lockBaseLedger")
	add("internal/ref/purge.go", "PG", "FenceRepoPurge", "IsRepoPurged", "DropRepoRefs", "RetireRepoPacks", "CountRepoRows")
	add("internal/ref/purge.go", "", "repoPurgeFencedPG")
	add("internal/ref/sqlite.go", "SQLite", "casRef", "RegisterPacks", "ReplacePacks")
	add("internal/ref/ledger_sqlite.go", "SQLite", "registerLFSObjectTx")
	add("internal/ref/purge_sqlite.go", "SQLite", "FenceRepoPurge", "IsRepoPurged", "DropRepoRefs", "RetireRepoPacks", "CountRepoRows")
	add("internal/ref/purge_sqlite.go", "", "repoPurgeFencedSQLite")
	add("internal/ref/s3walstore.go", "S3WAL", "casRef", "RegisterPacks", "ReplacePacks")
	add("internal/ref/s3walstore_lfs.go", "S3WAL", "registerLFSObject")
	add("internal/ref/s3walstore_purge.go", "S3WAL", "FenceRepoPurge", "IsRepoPurged", "DropRepoRefs", "RetireRepoPacks", "CountRepoRows")
	for _, b := range []struct{ rel, recv string }{
		{"internal/meta/purge.go", "PG"}, {"internal/meta/purge_sqlite.go", "SQLite"}, {"internal/meta/s3walmeta_purge.go", "S3WAL"},
	} {
		add(b.rel, b.recv, "PurgeableRepos", "RepoPurgeState", "PurgeRepoMeta")
	}
	return out
}
