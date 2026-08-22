package simtest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"time"

	"codestorage/internal/blob"
	"codestorage/internal/ref"
)

// Config parameterizes one deterministic simulation run. The zero value of
// every field defaults sensibly (see withDefaults); Seed 0 is a valid seed.
type Config struct {
	Seed     uint64
	Steps    int // scheduler steps (default 150)
	Nodes    int // simulated server nodes sharing one SQLite+fileblob (default 3)
	Repos    int // simulated repos (default 2)
	Branches int // branches per repo (default 3)
	// FullCheckEvery runs the expensive cold-hydrate oracle every N steps
	// (default 50). It always also runs once after the last step.
	FullCheckEvery int
	// Dir is the run's scratch root; empty means a fresh os.MkdirTemp that
	// is removed afterwards unless KeepDir is set.
	Dir     string
	KeepDir bool
	// Trace, when non-nil, receives the live trace lines (the same bytes
	// that feed TraceHash).
	Trace io.Writer
}

// withDefaults returns cfg with zero fields defaulted.
func (c Config) withDefaults() Config {
	if c.Steps == 0 {
		c.Steps = 150
	}
	if c.Nodes == 0 {
		c.Nodes = 3
	}
	if c.Repos == 0 {
		c.Repos = 2
	}
	if c.Branches == 0 {
		c.Branches = 3
	}
	if c.FullCheckEvery == 0 {
		c.FullCheckEvery = 50
	}
	assertf(c.Steps > 0 && c.Nodes > 0 && c.Repos > 0 && c.Branches > 0 && c.FullCheckEvery > 0,
		"Config: non-positive dimension %+v", c)
	return c
}

// Result summarizes one run. Violations non-empty means an invariant broke;
// TraceHash is a digest of the full deterministic trace (two runs with the
// same Config must produce identical hashes).
type Result struct {
	Seed        uint64
	Steps       int // steps executed (< Config.Steps when a violation stopped the run)
	TraceHash   string
	Violations  []string
	Transitions int // distinct committed ref transitions observed
	Deliveries  int // webhook deliveries recorded
	Faults      int // faults injected
	OpCounts    map[string]int
}

// defaultFaultProfile arms the injector with the standing probabilistic
// fault mix. Transient not-found faults are NOT probabilistic: HydratePacks
// deliberately restarts its whole pass on one, so independent per-download
// draws would occasionally exhaust the retry bound on healthy code. The
// scheduler injects them as targeted one-shots instead (opHydrateNotFound).
func defaultFaultProfile(inj *Injector) {
	inj.SetProbs(OpRefCASRef, Probs{Clean: 0.04, Ambiguous: 0.08})
	inj.SetProbs(OpRefRegisterPacks, Probs{Clean: 0.02, Ambiguous: 0.04})
	inj.SetProbs(OpRefReplacePacks, Probs{Clean: 0.02, Ambiguous: 0.04})
	inj.SetProbs(OpRefGetRef, Probs{Clean: 0.02})
	inj.SetProbs(OpRefListPacks, Probs{Clean: 0.02})
	inj.SetProbs(OpRefListRefs, Probs{Clean: 0.02})
	inj.SetProbs(OpBlobUploadFile, Probs{Clean: 0.02, Ambiguous: 0.03})
	inj.SetProbs(OpBlobDownloadFile, Probs{Clean: 0.02})
	inj.SetProbs(OpRefReservePacks, Probs{Clean: 0.03, Ambiguous: 0.05})
}

// Scheduler op kinds and weights. The table is fixed (never map-ordered) so
// the pick sequence is a pure function of the seed.
type opKind int

const (
	opPush opKind = iota
	opDelete
	opHydrate
	opHydrateNotFound
	opCrashWarm
	opCrashCold
	opPushCrashUpload
	opPushCrashRegister
	opRepack
	opRepackRace
	opReserveCrash
	opReserveSweep
)

var opTable = []struct {
	kind   opKind
	name   string
	weight int
}{
	{opPush, "push", 45},
	{opDelete, "delete", 5},
	{opHydrate, "hydrate", 15},
	{opHydrateNotFound, "hydrate-notfound", 8},
	{opCrashWarm, "crash-warm", 5},
	{opCrashCold, "crash-cold", 7},
	{opPushCrashUpload, "push-crash-after-upload", 8},
	{opPushCrashRegister, "push-crash-after-register", 7},
	{opRepack, "repack", 8},
	{opRepackRace, "repack-race", 4},
	{opReserveCrash, "reserve-crash", 5},
	{opReserveSweep, "reserve-sweep", 3},
}

// simRepo is one simulated repository: its storage key plus the scratch
// object-factory repo commits are minted in.
type simRepo struct {
	key     string
	scratch string
}

// sim is one running simulation.
type sim struct {
	cfg     Config
	rng     *rand.Rand // scheduler stream, separate from the injector's
	inj     *Injector
	cl      *cluster
	repos   []*simRepo
	dir     string
	tmpDir  string
	step    int
	packSeq int

	hasher     hash.Hash
	violations []string
	stopped    bool

	observed     map[pushTransition]bool // ground-truth committed transitions
	reportedWins []pushTransition        // transitions the writer saw won=true

	// prevPacks is the per-repo pack-row baseline for the "rows only grow,
	// modulo repack" oracle (checkPackRows): repo key -> pack id -> blob
	// key as of the last check. The repack ops rewrite their repo's
	// baseline in-op after pinning the exact fold transition.
	prevPacks map[string]map[string]string
}

// Run executes one deterministic simulation and reports the outcome.
// The returned error is an infrastructure failure (harness bug, git or
// filesystem trouble) — invariant breaches land in Result.Violations.
func Run(cfg Config) (Result, error) {
	cfg = cfg.withDefaults()
	dir := cfg.Dir
	if dir == "" {
		var err error
		dir, err = os.MkdirTemp("", "dst-*")
		if err != nil {
			return Result{}, err
		}
		if !cfg.KeepDir {
			defer os.RemoveAll(dir)
		}
	}

	inj := NewInjector(cfg.Seed)
	defaultFaultProfile(inj)
	s := &sim{
		cfg:       cfg,
		rng:       rand.New(rand.NewPCG(cfg.Seed, 0x5C4E_D01E_5EED_0001)), //nolint:gosec // deterministic scheduler PRNG
		inj:       inj,
		dir:       dir,
		tmpDir:    filepath.Join(dir, "tmp"),
		hasher:    sha256.New(),
		observed:  make(map[pushTransition]bool),
		prevPacks: make(map[string]map[string]string),
	}
	if err := os.MkdirAll(s.tmpDir, 0o755); err != nil {
		return Result{}, err
	}

	cl, err := newCluster(dir, cfg.Nodes, inj, func() time.Time {
		return time.Unix(simEpoch+int64(s.step), 0).UTC()
	})
	if err != nil {
		return Result{}, err
	}
	defer cl.close()
	s.cl = cl

	for i := 0; i < cfg.Repos; i++ {
		r := &simRepo{
			key:     fmt.Sprintf("simrepo%d", i),
			scratch: filepath.Join(dir, fmt.Sprintf("scratch-simrepo%d.git", i)),
		}
		if err := initScratchRepo(r.scratch); err != nil {
			return Result{}, fmt.Errorf("init scratch repo: %w", err)
		}
		s.repos = append(s.repos, r)
	}

	for s.step = 1; s.step <= cfg.Steps; s.step++ {
		if err := s.doStep(); err != nil {
			return s.result(), err
		}
		if s.stopped {
			break
		}
		if err := s.checkPackBlobs(); err != nil {
			return s.result(), err
		}
		if s.stopped {
			break
		}
		if err := s.checkPackRows(); err != nil {
			return s.result(), err
		}
		if s.stopped {
			break
		}
		if err := s.checkQuotaLedger(); err != nil {
			return s.result(), err
		}
		if !s.stopped && s.step%cfg.FullCheckEvery == 0 {
			if err := s.checkFull(); err != nil {
				return s.result(), err
			}
		}
		if s.stopped {
			break
		}
	}
	if !s.stopped {
		if err := s.checkFull(); err != nil {
			return s.result(), err
		}
		s.checkDeliveries()
	}
	return s.result(), nil
}

func (s *sim) result() Result {
	executed := s.step
	if executed > s.cfg.Steps {
		executed = s.cfg.Steps
	}
	return Result{
		Seed:        s.cfg.Seed,
		Steps:       executed,
		TraceHash:   hex.EncodeToString(s.hasher.Sum(nil)),
		Violations:  s.violations,
		Transitions: len(s.observed),
		Deliveries:  len(s.cl.rec.all()),
		Faults:      len(s.inj.Fired()),
		OpCounts:    s.inj.Counts(),
	}
}

// tracef appends one line to the deterministic trace (hash + optional live
// sink). Lines must never contain filesystem paths or wall-clock values.
func (s *sim) tracef(format string, args ...any) {
	line := fmt.Sprintf(format, args...) + "\n"
	_, err := io.WriteString(s.hasher, line)
	assertf(err == nil, "trace hash write: %v", err)
	if s.cfg.Trace != nil {
		_, _ = io.WriteString(s.cfg.Trace, line)
	}
}

// violate records one invariant breach (sanitized of run-dir paths) and
// stops the run: state after a breach is not worth simulating further.
func (s *sim) violate(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	msg = strings.ReplaceAll(msg, s.dir, "$RUN")
	s.violations = append(s.violations, msg)
	s.tracef("VIOLATION %s", msg)
	s.stopped = true
}

// groundTip reads the authoritative tip of repo/refName through the clean
// oracle handle (ZeroSHA when absent).
func (s *sim) groundTip(repoKey, refName string) (string, error) {
	sha, err := s.cl.oracle.GetRef(context.Background(), repoKey, refName)
	if errors.Is(err, ref.ErrNotFound) {
		return ref.ZeroSHA, nil
	}
	if err != nil {
		return "", fmt.Errorf("oracle GetRef %s/%s: %w", repoKey, refName, err)
	}
	return sha, nil
}

// doStep picks and executes one scheduler op. Draw order is fixed (kind,
// node, repo, branch — always all four) so the PRNG stream stays aligned
// regardless of which draws an op consumes.
func (s *sim) doStep() error {
	total := 0
	for _, e := range opTable {
		total += e.weight
	}
	pick := s.rng.IntN(total)
	var kind opKind
	var name string
	for _, e := range opTable {
		if pick < e.weight {
			kind, name = e.kind, e.name
			break
		}
		pick -= e.weight
	}
	node := s.cl.nodes[s.rng.IntN(len(s.cl.nodes))]
	repo := s.repos[s.rng.IntN(len(s.repos))]
	branch := fmt.Sprintf("refs/heads/b%d", s.rng.IntN(s.cfg.Branches))

	switch kind {
	case opPush:
		return s.doPush(name, node, repo, branch, "")
	case opPushCrashUpload:
		return s.doPush(name, node, repo, branch, "after-upload")
	case opPushCrashRegister:
		return s.doPush(name, node, repo, branch, "after-register")
	case opDelete:
		return s.doDelete(name, node, repo, branch)
	case opHydrate:
		return s.doHydrate(name, node, repo, false)
	case opHydrateNotFound:
		return s.doHydrate(name, node, repo, true)
	case opCrashWarm:
		return s.doCrash(name, node, false)
	case opCrashCold:
		return s.doCrash(name, node, true)
	case opRepack:
		return s.doRepack(name, node, repo)
	case opRepackRace:
		return s.doRepackRace(name, node, repo, branch)
	case opReserveCrash:
		return s.doReserveCrash(name, node, repo)
	case opReserveSweep:
		return s.doReserveSweep(name, node)
	}
	panic(fmt.Sprintf("simtest: unhandled op kind %d", kind))
}

// faultToken renders fault events for trace lines.
func faultToken(evs []FaultEvent) string {
	if len(evs) == 0 {
		return "-"
	}
	parts := make([]string, 0, len(evs))
	for _, ev := range evs {
		t := ev.Op + "/" + ev.Class.String()
		if ev.InnerOutcome != "" {
			t += "(" + ev.InnerOutcome + ")"
		}
		parts = append(parts, t)
	}
	return strings.Join(parts, ",")
}

// errToken classifies an op error for trace lines without leaking paths.
func errToken(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, ErrInjectedClean):
		return "err-clean"
	case errors.Is(err, ErrInjectedAmbiguous):
		return "err-ambiguous"
	case errors.Is(err, blob.ErrNotFound):
		return "err-notfound"
	default:
		return "err-other"
	}
}

// doPush mints one commit on the node's believed tip and drives it through
// the production write pipeline: blob upload → RegisterPacks → CASRef.
// crashPoint simulates the pushing node dying mid-pipeline ("" = no crash).
func (s *sim) doPush(name string, node *simNode, repo *simRepo, branch, crashPoint string) error {
	ctx := context.Background()
	old := node.believedTip(repo.key, branch)
	parent := ""
	if old != ref.ZeroSHA {
		parent = old
	}
	content := fmt.Sprintf("%s %s step %d", repo.key, branch, s.step)
	mc, err := mintCommit(repo.scratch, parent, content, s.step)
	if err != nil {
		return fmt.Errorf("step %d: mint commit: %w", s.step, err)
	}
	packPath, err := packObjects(repo.scratch, s.tmpDir, []string{mc.blobSHA, mc.treeSHA, mc.commitSHA})
	if err != nil {
		return fmt.Errorf("step %d: pack objects: %w", s.step, err)
	}
	defer os.Remove(packPath)
	s.packSeq++
	packID := fmt.Sprintf("%06d", s.packSeq)
	key := blob.PackKey(repo.key, packID)

	mark := s.inj.Seq()
	outcome, err := s.pushPipeline(ctx, node, repo, branch, old, mc.commitSHA, packID, key, packPath, crashPoint)
	if err != nil {
		return err
	}
	evs, _ := s.inj.FiredSince(mark)
	tip, err := s.groundTip(repo.key, branch)
	if err != nil {
		return err
	}
	s.tracef("step=%04d op=%s node=%d repo=%s ref=%s old=%.8s new=%.8s pack=%s out=%s faults=[%s] tip=%.8s",
		s.step, name, node.id, repo.key, branch, old, mc.commitSHA, packID, outcome, faultToken(evs), tip)
	return nil
}

// pushPipeline runs upload → register → CAS and returns a trace outcome
// token. Aborts (fault or simulated crash) leave orphan blobs/pack rows —
// legal states the oracles must tolerate; the pipeline ORDER is what
// guarantees invariant (c).
func (s *sim) pushPipeline(ctx context.Context, node *simNode, repo *simRepo, branch, old, new, packID, key, packPath, crashPoint string) (string, error) {
	if err := node.simBlob.UploadFile(ctx, key, packPath); err != nil {
		s.refreshBelief(node, repo.key, branch)
		return "upload-" + errToken(err), s.checkTipUnchanged(repo.key, branch, old, new)
	}
	if crashPoint == "after-upload" {
		return "crashed-after-upload", s.checkTipUnchanged(repo.key, branch, old, new)
	}
	if err := node.simRef.RegisterPacks(ctx, repo.key, []ref.Pack{{ID: packID, Key: key, Size: packFileSize(packPath)}}); err != nil {
		s.refreshBelief(node, repo.key, branch)
		return "register-" + errToken(err), s.checkTipUnchanged(repo.key, branch, old, new)
	}
	if crashPoint == "after-register" {
		return "crashed-after-register", s.checkTipUnchanged(repo.key, branch, old, new)
	}
	return s.runCAS(node, repo.key, branch, old, new)
}

// checkTipUnchanged asserts a pipeline abort before CAS left the ref alone.
func (s *sim) checkTipUnchanged(repoKey, branch, old, new string) error {
	tip, err := s.groundTip(repoKey, branch)
	if err != nil {
		return err
	}
	if tip == new {
		s.violate("step %d: %s/%s reached target %s without a CAS attempt", s.step, repoKey, branch, new)
	}
	return nil
}

// doDelete drives a ref delete (CAS tip -> ZeroSHA) from the node's belief.
func (s *sim) doDelete(name string, node *simNode, repo *simRepo, branch string) error {
	old := node.believedTip(repo.key, branch)
	if old == ref.ZeroSHA {
		s.tracef("step=%04d op=%s node=%d repo=%s ref=%s out=skip-unborn", s.step, name, node.id, repo.key, branch)
		return nil
	}
	mark := s.inj.Seq()
	outcome, err := s.runCAS(node, repo.key, branch, old, ref.ZeroSHA)
	if err != nil {
		return err
	}
	evs, _ := s.inj.FiredSince(mark)
	tip, err := s.groundTip(repo.key, branch)
	if err != nil {
		return err
	}
	s.tracef("step=%04d op=%s node=%d repo=%s ref=%s old=%.8s out=%s faults=[%s] tip=%.8s",
		s.step, name, node.id, repo.key, branch, old, outcome, faultToken(evs), tip)
	return nil
}

// runCAS performs one CAS through the full production chain (events →
// IdempotentCAS → sim decorator → SQLite) with per-attempt invariant
// checks, retrying once on error like a real client. Returns a trace token.
func (s *sim) runCAS(node *simNode, repoKey, refName, old, new string) (string, error) {
	won, cerr, err := s.casAttempt(node, repoKey, refName, old, new)
	if err != nil {
		return "", err
	}
	token := casToken(won, cerr)
	if cerr != nil && !s.stopped {
		won, cerr, err = s.casAttempt(node, repoKey, refName, old, new)
		if err != nil {
			return "", err
		}
		token += "+retry-" + casToken(won, cerr)
	}
	if won {
		node.setBelief(repoKey, refName, new)
		s.reportedWins = append(s.reportedWins, pushTransition{
			Repo: repoKey, Ref: refName, Before: old, After: new,
		})
	} else {
		s.refreshBelief(node, repoKey, refName)
	}
	return token, nil
}

func casToken(won bool, cerr error) string {
	if won {
		return "won"
	}
	if cerr == nil {
		return "conflict"
	}
	return errToken(cerr)
}

// casAttempt runs one CAS attempt and checks the step-level invariants:
//
//   - atomicity: the ref moved to the target or not at all;
//   - a reported win means the ref IS at the target;
//   - issue #2's contract: an ambiguous-but-committed create/update whose
//     recovery recheck was not starved must be reported as a win;
//   - a clean update conflict with the ref already at the target means the
//     idempotent-recovery seam failed (the visible symptom of a broken #2:
//     the retrying client gets a spurious conflict against its own write).
func (s *sim) casAttempt(node *simNode, repoKey, refName, old, new string) (won bool, cerr error, err error) {
	preTip, err := s.groundTip(repoKey, refName)
	if err != nil {
		return false, nil, err
	}
	mark := s.inj.Seq()
	won, cerr = node.casStore.CASRef(context.Background(), repoKey, refName, old, new)
	evs, _ := s.inj.FiredSince(mark)
	postTip, err := s.groundTip(repoKey, refName)
	if err != nil {
		return false, nil, err
	}

	if postTip != preTip {
		if postTip != new {
			s.violate("step %d: CAS atomicity: %s/%s moved %s -> %s during CAS(%s -> %s)",
				s.step, repoKey, refName, preTip, postTip, old, new)
			return won, cerr, nil
		}
		s.observed[pushTransition{Repo: repoKey, Ref: refName, Before: preTip, After: postTip}] = true
	}
	if won && postTip != new {
		s.violate("step %d: %s/%s reported won but tip=%s, target=%s",
			s.step, repoKey, refName, postTip, new)
		return won, cerr, nil
	}

	recheckStarved := false
	ambCommitted := false
	for _, ev := range evs {
		if ev.Op == OpRefGetRef {
			recheckStarved = true
		}
		if ev.Op == OpRefCASRef && ev.Class == FaultAmbiguous && strings.HasPrefix(ev.InnerOutcome, "won=true") {
			ambCommitted = true
		}
	}
	// Deletes are exempt: the recovery matrix never recovers them (absence
	// is not positive evidence).
	if new != ref.ZeroSHA && !recheckStarved {
		if ambCommitted && !won {
			s.violate("step %d: issue-#2 class: ambiguous-but-committed CAS %s/%s (%s -> %s) not recovered to a win (got won=%v err=%v)",
				s.step, repoKey, refName, old, new, won, cerr)
			return won, cerr, nil
		}
		if !won && cerr == nil && old != ref.ZeroSHA && postTip == new {
			s.violate("step %d: spurious conflict: CAS %s/%s (%s -> %s) reported a clean conflict with the ref already at the target",
				s.step, repoKey, refName, old, new)
			return won, cerr, nil
		}
	}
	return won, cerr, nil
}

// refreshBelief re-learns the ground-truth tip after a failed or conflicted
// op (models the client re-fetching before its next attempt). Reads through
// the oracle: a clean read keeps the scheduler's PRNG streams independent
// of belief maintenance.
func (s *sim) refreshBelief(node *simNode, repoKey, refName string) {
	tip, err := s.groundTip(repoKey, refName)
	assertf(err == nil, "refreshBelief: %v", err)
	node.setBelief(repoKey, refName, tip)
}

// doCrash kills and restarts a node (cold = cache wiped too).
func (s *sim) doCrash(name string, node *simNode, cold bool) error {
	if err := s.cl.crashNode(node, cold); err != nil {
		return fmt.Errorf("step %d: crash node %d: %w", s.step, node.id, err)
	}
	s.tracef("step=%04d op=%s node=%d", s.step, name, node.id)
	return nil
}
