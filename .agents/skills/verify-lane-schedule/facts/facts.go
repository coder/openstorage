// facts.go reads the Go source that model/LaneSchedule.tla describes and
// prints two kinds of lines:
//
//	FACT <Name> <TRUE|FALSE> <evidence>
//	FP <label> <sha256>
//
// A FACT is one guard the model depends on, checked in the AST (call
// order, SQL text, assignments) on both local-schedule backends, SQLite and
// S3-WAL; check.sh writes it into the TLC config, so breaking the guard in
// the code flips the model. An FP fingerprints one covered function's
// tokens (comments and formatting ignored); check.sh compares it with
// model/fingerprints.txt to tell when covered code changed.
//
// Run from the repository root:
//
//	go run .agents/skills/verify-lane-schedule/facts/facts.go
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

// calls lists every call in n in source order as (position, printed callee).
func (s *source) calls(n ast.Node) []call {
	var out []call
	ast.Inspect(n, func(m ast.Node) bool {
		if c, ok := m.(*ast.CallExpr); ok {
			out = append(out, call{pos: c.Pos(), fun: s.text(c.Fun), node: c})
		}
		return true
	})
	return out
}

type call struct {
	pos  token.Pos
	fun  string
	node *ast.CallExpr
}

func first(cs []call, fun string) token.Pos {
	for _, c := range cs {
		if c.fun == fun {
			return c.pos
		}
	}
	return token.NoPos
}

// strs returns the value of every string literal inside n.
func strs(n ast.Node) []string {
	var out []string
	ast.Inspect(n, func(m ast.Node) bool {
		if l, ok := m.(*ast.BasicLit); ok && l.Kind == token.STRING {
			if v, err := strconv.Unquote(l.Value); err == nil {
				out = append(out, v)
			}
		}
		return true
	})
	return out
}

func anyContains(ss []string, sub string) bool {
	for _, s := range ss {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// caseOf returns the first case clause in n whose labels mention label.
func (s *source) caseOf(n ast.Node, label string) *ast.CaseClause {
	var found *ast.CaseClause
	ast.Inspect(n, func(m ast.Node) bool {
		if found != nil {
			return false
		}
		if cc, ok := m.(*ast.CaseClause); ok {
			for _, e := range cc.List {
				if strings.Contains(s.text(e), label) {
					found = cc
					return false
				}
			}
		}
		return true
	})
	return found
}

type assign struct {
	pos      token.Pos
	lhs, rhs string
}

// assigns pairs each left-hand side of every assignment in n with its
// right-hand side (a, b = x, y gives a=x and b=y).
func (s *source) assigns(n ast.Node) []assign {
	var out []assign
	ast.Inspect(n, func(m ast.Node) bool {
		as, ok := m.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for i, l := range as.Lhs {
			r := ""
			if len(as.Rhs) == len(as.Lhs) {
				r = s.text(as.Rhs[i])
			} else if len(as.Rhs) == 1 {
				r = s.text(as.Rhs[0])
			}
			out = append(out, assign{pos: as.Pos(), lhs: s.text(l), rhs: r})
		}
		return true
	})
	return out
}

func assigned(as []assign, lhs, rhs string) bool {
	for _, a := range as {
		if a.lhs == lhs && a.rhs == rhs {
			return true
		}
	}
	return false
}

func inside(n ast.Node, pos token.Pos) bool {
	return n != nil && pos >= n.Pos() && pos < n.End()
}

// literals returns every composite literal of type typ inside n.
func (s *source) literals(n ast.Node, typ string) []string {
	var out []string
	ast.Inspect(n, func(m ast.Node) bool {
		if cl, ok := m.(*ast.CompositeLit); ok && cl.Type != nil && s.text(cl.Type) == typ {
			out = append(out, s.text(cl))
		}
		return true
	})
	return out
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

func main() {
	flag.Parse()
	sqlSched := load("internal/meta/sqlite_schedule.go")
	sqlEvt := load("internal/meta/sqlite_eventing.go")
	walSched := load("internal/meta/s3walmeta_schedule.go")
	walEvt := load("internal/meta/s3walmeta_eventing.go")
	push := load("internal/sync/worker_push.go")
	worker := load("internal/sync/worker.go")
	sched := load("internal/sync/local_schedule.go")
	refstore := load("internal/events/refstore.go")
	wireEvt := load("cmd/openstorage/wire_eventing.go")
	wireLocal := load("cmd/openstorage/wire_eventing_local.go")
	pub := load("internal/githttp/publication.go")
	hydrate := load("internal/githttp/hydrate.go")
	hook := load("internal/githttp/hook.go")

	sqlBegin := sqlEvt.fn("SQLite", "beginPush")
	walBegin := walEvt.fn("S3WAL", "beginPush")
	sqlFinish := sqlSched.fn("SQLite", "FinishPushOutcome")
	walFinish := walSched.fn("s3walLocalSchedule", "FinishPushOutcome")
	runPush := push.fn("Worker", "RunPush")
	sqlBeginAs, walBeginAs := sqlEvt.assigns(sqlBegin.Body), walEvt.assigns(walBegin.Body)
	walFinishAs := walSched.assigns(walFinish.Body)

	var facts []*fact

	// ClaimConsumesPending: the claim write zeroes the pending level.
	cc := newFact("ClaimConsumesPending")
	claimSQL := false
	for _, q := range strs(sqlBegin.Body) {
		if strings.Contains(q, "UPDATE repo_upstreams") && strings.Contains(q, "push_syncing = 1") && strings.Contains(q, "push_pending = 0") {
			claimSQL = true
		}
	}
	cc.need(claimSQL, "SQLite.beginPush's claim UPDATE sets push_syncing = 1 and push_pending = 0")
	cc.need(assigned(walBeginAs, "u.PushPending", "0"), "S3WAL.beginPush assigns u.PushPending = 0 with the claim")
	facts = append(facts, cc)

	// ClaimMarksDue: a scheduled claim writes its claim time into push_due.
	cm := newFact("ClaimMarksDue")
	markSQL := false
	for _, c := range sqlEvt.calls(sqlBegin.Body) {
		if c.fun == "tx.ExecContext" && len(c.node.Args) >= 3 &&
			anyContains(strs(c.node.Args[1]), "SET push_due = ?") && sqlEvt.text(c.node.Args[2]) == "nowNanos" {
			markSQL = true
		}
	}
	cm.need(markSQL, "SQLite.beginPush runs UPDATE ... SET push_due = ? with nowNanos")
	cm.need(assigned(walBeginAs, "u.PushDue", "nowNanos"), "S3WAL.beginPush assigns u.PushDue = nowNanos")
	facts = append(facts, cm)

	// DoneKeepsPending: a done outcome never clears the pending level, so
	// a request recorded during the run survives it.
	dk := newFact("DoneKeepsPending")
	if done := sqlSched.caseOf(sqlFinish.Body, "OutcomeDone"); done != nil {
		dk.need(!anyContains(strs(done), "push_pending"), "SQLite.FinishPushOutcome's OutcomeDone UPDATE does not write push_pending")
	} else {
		dk.need(false, "SQLite.FinishPushOutcome has a case OutcomeDone")
	}
	for _, d := range sqlFinish.Body.List {
		if g, ok := d.(*ast.DeclStmt); ok {
			dk.need(!anyContains(strs(g), "push_pending"), "SQLite.FinishPushOutcome's shared release clause does not write push_pending")
		}
	}
	walRetry := walSched.caseOf(walFinish.Body, "OutcomeRetry")
	dk.need(walRetry != nil, "s3walLocalSchedule.FinishPushOutcome has a case OutcomeRetry, OutcomeRestore")
	for _, a := range walFinishAs {
		if a.lhs == "u.PushPending" {
			dk.need(inside(walRetry, a.pos), "s3walLocalSchedule.FinishPushOutcome assigns u.PushPending only in its retry/restore case")
		}
	}
	facts = append(facts, dk)

	// RetryRemarks: retry and restore raise pending back to the run's
	// level, which is 2 when the run's outcome carries Force.
	rr := newFact("RetryRemarks")
	for _, k := range []string{"OutcomeRetry", "OutcomeRestore"} {
		c := sqlSched.caseOf(sqlFinish.Body, k)
		rr.need(c != nil && anyContains(strs(c), "push_pending = MAX(push_pending, ?)") &&
			strings.Contains(sqlSched.text(c), "[]any{level,"), "SQLite.FinishPushOutcome's %s UPDATE sets push_pending = MAX(push_pending, ?) with level first", k)
	}
	rr.need(walRetry != nil && assigned(walSched.assigns(walRetry), "u.PushPending", "max(u.PushPending, level)"),
		"s3walLocalSchedule.FinishPushOutcome's retry/restore case assigns u.PushPending = max(u.PushPending, level)")
	for _, f := range []struct {
		src *source
		fn  *ast.FuncDecl
	}{{sqlSched, sqlFinish}, {walSched, walFinish}} {
		t := f.src.text(f.fn.Body)
		rr.need(strings.Contains(t, "if o.Force {\n\t\tlevel = 2\n\t}"), "%s FinishPushOutcome sets level = 2 when o.Force", f.src.path)
	}
	facts = append(facts, rr)

	// FinishKeepsExplicit: retry and restore keep push_due = 0, the mark of
	// an explicit request made during the run.
	fe := newFact("FinishKeepsExplicit")
	for _, k := range []string{"OutcomeRetry", "OutcomeRestore"} {
		c := sqlSched.caseOf(sqlFinish.Body, k)
		fe.need(c != nil && anyContains(strs(c), "push_due = CASE WHEN push_due = 0 THEN 0 ELSE ? END"),
			"SQLite.FinishPushOutcome's %s UPDATE sets push_due = CASE WHEN push_due = 0 THEN 0 ELSE ? END", k)
	}
	guarded, unguarded := 0, 0
	if walRetry != nil {
		var ifs []*ast.IfStmt
		ast.Inspect(walRetry, func(m ast.Node) bool {
			if is, ok := m.(*ast.IfStmt); ok && walSched.text(is.Cond) == "u.PushDue != 0" {
				ifs = append(ifs, is)
			}
			return true
		})
		for _, a := range walSched.assigns(walRetry) {
			if a.lhs != "u.PushDue" {
				continue
			}
			in := false
			for _, is := range ifs {
				in = in || inside(is.Body, a.pos)
			}
			if in {
				guarded++
			} else {
				unguarded++
			}
		}
	}
	fe.need(guarded > 0 && unguarded == 0, "s3walLocalSchedule.FinishPushOutcome assigns u.PushDue in its retry/restore case only under if u.PushDue != 0 (guarded %d, unguarded %d)", guarded, unguarded)
	facts = append(facts, fe)

	// StealInheritsForce: a claim that steals an expired claim keeps its
	// force mandate, and the claim persists it.
	sf := newFact("StealInheritsForce")
	sf.need(assigned(sqlBeginAs, "force", "prev.PushPending == 2 || (prev.PushSyncing && prev.PushClaimForce)"),
		"SQLite.beginPush computes force = prev.PushPending == 2 || (prev.PushSyncing && prev.PushClaimForce)")
	sf.need(anyContains(strs(sqlBegin.Body), "push_claim_force = ?"), "SQLite.beginPush's claim UPDATE sets push_claim_force = ?")
	sf.need(assigned(walBeginAs, "force", "u.PushPending == 2 || (u.PushSyncing && u.PushClaimForce)"),
		"S3WAL.beginPush computes force = u.PushPending == 2 || (u.PushSyncing && u.PushClaimForce)")
	sf.need(assigned(walBeginAs, "u.PushClaimForce", "force"), "S3WAL.beginPush assigns u.PushClaimForce = force")
	facts = append(facts, sf)

	// RunPassesForce: every retry and restore outcome of a run carries the
	// run's force flag, which comes from its claim.
	rf := newFact("RunPassesForce")
	rf.need(assigned(push.assigns(runPush.Body), "force", "c.Force"), "Worker.RunPush takes force from its local claim (force = c.Force)")
	restores := 0
	for _, l := range push.literals(runPush.Body, "meta.PushOutcome") {
		if strings.Contains(l, "meta.OutcomeRestore") {
			restores++
			rf.need(strings.Contains(l, "Force: force"), "Worker.RunPush passes Force: force in %s", l)
		}
	}
	rf.need(restores >= 2, "Worker.RunPush builds a restore outcome for the publication wait and for shutdown (found %d)", restores)
	retries := 0
	for _, l := range push.literals(push.fn("", "localPushOutcome").Body, "meta.PushOutcome") {
		if strings.Contains(l, "meta.OutcomeRetry") {
			retries++
			rf.need(strings.Contains(l, "Force: force"), "localPushOutcome passes Force: force in %s", l)
		}
	}
	rf.need(retries == 1, "localPushOutcome builds one retry outcome (found %d)", retries)
	facts = append(facts, rf)

	// WaitBeforeSnapshot: a local run waits for push publication before
	// its loopback fetch, and serve wires the wait to the push holds.
	ws := newFact("WaitBeforeSnapshot")
	rcs := push.calls(runPush.Body)
	wp, tp := first(rcs, "w.waitPublication"), first(rcs, "w.transferPush")
	ws.need(wp.IsValid() && tp.IsValid() && wp < tp, "Worker.RunPush calls w.waitPublication before w.transferPush")
	ws.need(first(worker.calls(worker.fn("Worker", "waitPublication").Body), "w.WaitPublication").IsValid(),
		"Worker.waitPublication calls w.WaitPublication")
	wired := false
	ast.Inspect(wireLocal.fn("", "buildLocalSyncQueue").Body, func(m ast.Node) bool {
		if as, ok := m.(*ast.AssignStmt); ok && len(as.Lhs) == 1 && wireLocal.text(as.Lhs[0]) == "worker.WaitPublication" &&
			strings.Contains(wireLocal.text(as.Rhs[0]), "githttp.WaitForPushPublication(") {
			wired = true
		}
		return true
	})
	ws.need(wired, "buildLocalSyncQueue sets worker.WaitPublication to call githttp.WaitForPushPublication")
	facts = append(facts, ws)

	// ShutdownRestores: a run that the shutdown drain cancelled restores
	// its request before localPushOutcome can count it as done.
	sr := newFact("ShutdownRestores")
	shut := token.NoPos
	ast.Inspect(runPush.Body, func(m ast.Node) bool {
		if is, ok := m.(*ast.IfStmt); ok && push.text(is.Cond) == "runErr != nil && shutdownAborted(ctx)" {
			body := push.text(is.Body)
			if strings.Contains(body, "w.restoreForShutdown(") && strings.Contains(body, "meta.OutcomeRestore") {
				shut = is.Pos()
			}
		}
		return true
	})
	lo := first(rcs, "localPushOutcome")
	sr.need(shut.IsValid(), "Worker.RunPush has if runErr != nil && shutdownAborted(ctx) that calls w.restoreForShutdown with meta.OutcomeRestore")
	sr.need(shut.IsValid() && lo.IsValid() && shut < lo, "that branch comes before localPushOutcome")
	facts = append(facts, sr)

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
		recv  string
		names []string
	}
	for _, c := range []cover{
		{sqlSched, "SQLite", []string{"RequestPushNow", "ClaimDuePush", "FinishPushOutcome", "finishLane", "ListLocalWork"}},
		{sqlSched, "PushOutcome", []string{"validate"}},
		{sqlEvt, "SQLite", []string{"RequestPush", "beginPush"}},
		{walSched, "s3walLocalSchedule", []string{"RequestPushNow", "ClaimDuePush", "FinishPushOutcome", "ListLocalWork"}},
		{walSched, "", []string{"laneHeld"}},
		{walEvt, "S3WAL", []string{"RequestPush", "beginPush"}},
		{push, "Worker", []string{"RunPush", "transferPush"}},
		{push, "", []string{"localPushOutcome", "TransientPushFailure", "PushRetryDelay"}},
		{worker, "Worker", []string{"waitPublication", "restoreLocal", "claimLocal", "restoreForShutdown", "retryFinish", "retryFinishWithin"}},
		{worker, "", []string{"claimContext", "claimLease"}},
		{sched, "LocalScheduler", []string{"offer", "SweepOnce", "Start", "Drain"}},
		{sched, "", []string{"shutdownAborted"}},
		{refstore, "RefStore", []string{"CASRef", "emitPush"}},
		{wireEvt, "", []string{"enqueueSyncOrForward"}},
		{wireLocal, "", []string{"buildLocalSyncQueue"}},
		{pub, "", []string{"WaitForPushPublication"}},
		{pub, "publicationWait", []string{"wait"}},
		{hydrate, "", []string{"Hydrate", "reservePushHolds", "removePushHolds", "appendPushReleased", "pushHoldLive", "readPushHolds"}},
		{hydrate, "refCmdBatch", []string{"flush"}},
		{hook, "", []string{"referenceTransactionHook", "updateHook"}},
	} {
		for _, n := range c.names {
			f := c.src.fn(c.recv, n)
			label := c.src.path + ":" + n
			if c.recv != "" {
				label = c.src.path + ":" + c.recv + "." + n
			}
			fmt.Printf("FP %s %s\n", label, c.src.fingerprint(f.Pos(), f.End()))
		}
	}
	for _, k := range []string{"MaxPushTransientRetries"} {
		v := constSpec(push, k)
		fmt.Printf("FP %s:const_%s %s\n", push.path, k, push.fingerprint(v.Pos(), v.End()))
	}
	// transferPush names these refspecs; their values decide what a plain
	// and a forced forward may do upstream.
	for _, v := range []struct {
		src  *source
		name string
	}{{push, "pushRefspecs"}, {worker, "mirrorRefspecs"}} {
		n := varSpec(v.src, v.name)
		fmt.Printf("FP %s:var_%s %s\n", v.src.path, v.name, v.src.fingerprint(n.Pos(), n.End()))
	}
	// preReceive is long: fingerprint only the closure that reserves the
	// push's holds (reserveHoldsAndProbe = func() ...).
	pr := hook.fn("", "preReceive")
	var reserve ast.Node
	ast.Inspect(pr.Body, func(m ast.Node) bool {
		if as, ok := m.(*ast.AssignStmt); ok && len(as.Lhs) == 1 && hook.text(as.Lhs[0]) == "reserveHoldsAndProbe" {
			if _, ok := as.Rhs[0].(*ast.FuncLit); ok {
				reserve = as
			}
		}
		return true
	})
	if reserve == nil {
		fail("%s: preReceive's reserveHoldsAndProbe = func() ... not found", hook.path)
	}
	fmt.Printf("FP %s:preReceive[reserveHoldsAndProbe] %s\n", hook.path, hook.fingerprint(reserve.Pos(), reserve.End()))
}

func varSpec(s *source, name string) ast.Node {
	for _, d := range s.file.Decls {
		g, ok := d.(*ast.GenDecl)
		if !ok || g.Tok != token.VAR {
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
	fail("%s: var %s not found", s.path, name)
	return nil
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
