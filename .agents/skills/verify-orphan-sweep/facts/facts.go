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

	// HealEntryBounded: healQuarantineEntry's first statement starts a bound
	// that covers an unregistered key from its registry read to its purge. A
	// registered key restores or heals its original on the caller's context,
	// which no fixed bound fits, then opens a new bound and stats the original
	// again: the purge relies on that read. No other call passes the bare
	// context. The bound leaves the purge floor room for the handshake's two
	// phases besides.
	hb := newFact("HealEntryBounded")
	first0 := ""
	if len(healEntry.Body.List) > 0 {
		first0 = sweep.text(healEntry.Body.List[0])
	}
	hb.need(first0 == "ectx, cancel := context.WithTimeout(ctx, healEntryTimeout)",
		"healQuarantineEntry starts with ectx, cancel := context.WithTimeout(ctx, healEntryTimeout) (found %q)", first0)
	hb.need(first(hCalls, "s.registered", "ectx, repo, orig", false).IsValid(),
		"healQuarantineEntry reads the registry as s.registered(ectx, repo, orig)")
	var regBlock *ast.BlockStmt
	rebound := token.NoPos
	ast.Inspect(healEntry.Body, func(n ast.Node) bool {
		if is, ok := n.(*ast.IfStmt); ok && sweep.text(is.Cond) == "registered" {
			regBlock = is.Body
			for _, st := range is.Body.List {
				if sweep.text(st) == "bound, boundCancel := context.WithTimeout(ctx, healEntryTimeout)" {
					rebound = st.Pos()
				}
			}
		}
		return true
	})
	hb.need(regBlock != nil && rebound.IsValid() && hreg.IsValid() && hreg < rebound,
		"after the registry read, `if registered` opens a new bound: bound, boundCancel := context.WithTimeout(ctx, healEntryTimeout)")
	stat2 := first(hCalls, "s.blobs.Stat", "ectx, orig", false)
	hb.need(rebound.IsValid() && stat2.IsValid() && rebound < stat2 && regBlock != nil && stat2 < regBlock.End(),
		"the registered block stats the original again as s.blobs.Stat(ectx, orig) after the new bound")
	bare := 0
	bad := 0
	ast.Inspect(healEntry.Body, func(n ast.Node) bool {
		c, ok := n.(*ast.CallExpr)
		if !ok || len(c.Args) == 0 || sweep.text(c.Fun) == "context.WithTimeout" {
			return true
		}
		if id, ok := c.Args[0].(*ast.Ident); ok && id.Name == "ctx" {
			bare++
			if regBlock == nil || c.Pos() < regBlock.Pos() || c.Pos() > regBlock.End() || !rebound.IsValid() || c.Pos() > rebound {
				bad++
			}
		}
		return true
	})
	hb.need(bare > 0 && bad == 0,
		"only the registered block, before its new bound, passes bare ctx as a first argument (%d calls do, %d outside it)", bare, bad)
	healUnits := constUnits(sweep, "healEntryTimeout", "sweepRecoveryTimeout")
	floorUnits := constUnits(sweep, "QuarantinePurgeFloor", "sweepRecoveryTimeout")
	hb.need(healUnits >= 1 && healUnits+2 <= floorUnits,
		"healEntryTimeout + 2 x sweepRecoveryTimeout <= QuarantinePurgeFloor (healEntryTimeout is %d x, the floor %d x sweepRecoveryTimeout)", healUnits, floorUnits)
	facts = append(facts, hb)

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

	for _, f := range facts {
		ev := "ok"
		if !f.ok {
			ev = strings.Join(f.evidence, "; ")
		}
		fmt.Printf("FACT %s %s %s\n", f.name, strings.ToUpper(strconv.FormatBool(f.ok)), ev)
	}

	// Fingerprints of every covered function.
	type cover struct {
		src   *source
		label string
		node  ast.Node
	}
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
		cover{sweep, "internal/jobs/sweep.go:const healEntryTimeout", constSpec(sweep, "healEntryTimeout")},
		cover{blobGo, "internal/blob/blob.go:EnsurePublished", ensure},
		cover{publish, "internal/api/publish.go:server.publishPack", publishPack},
		cover{repack, "internal/jobs/repack.go:Repacker.repack", repack.fn("Repacker", "repack")},
		cover{repack, "internal/jobs/repack.go:verifyPublished", repack.fn("", "verifyPublished")},
	)
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

// constUnits returns N for `const name = N * unit`, 1 for `const name = unit`,
// or 0.
func constUnits(s *source, name, unit string) int {
	if n := constMultiplier(s, name, unit); n > 0 {
		return n
	}
	v := constSpec(s, name).(*ast.ValueSpec)
	if len(v.Values) == 1 {
		if id, ok := v.Values[0].(*ast.Ident); ok && id.Name == unit {
			return 1
		}
	}
	return 0
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
