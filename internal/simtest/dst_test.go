package simtest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/githttp"
	"github.com/coder/openstorage/internal/ref"
)

// reportRun logs a run's summary and fails the test on violations with
// reproduction instructions.
func reportRun(t *testing.T, res Result) {
	t.Helper()
	t.Logf("seed=%d steps=%d trace=%s transitions=%d deliveries=%d faults=%d cas-faults=%d steals=%d crash-steals=%d push-steals=%d push-crash-steals=%d push-forced-rescues=%d",
		res.Seed, res.Steps, res.TraceHash, res.Transitions, res.Deliveries, res.Faults, res.CASFaults, res.Steals, res.CrashSteals,
		res.PushSteals, res.PushCrashSteals, res.PushForcedRescues)
	if len(res.Violations) == 0 {
		return
	}
	for _, v := range res.Violations {
		t.Errorf("violation: %s", v)
	}
	t.Errorf("reproduce: DST_SEED=%d DST_STEPS=%d DST_REF_BACKEND=%s go test ./internal/simtest -run 'TestDSTSeed' -v  (or: go run ./cmd/dst -seed %d -steps %d -backend %s -v)",
		res.Seed, res.Steps, res.Backend, res.Seed, res.Steps, res.Backend)
}

// dstBackends are the authority-store lanes every DST entry point covers.
var dstBackends = []RefBackend{RefBackendSQLite, RefBackendS3WAL}

// envRefBackend resolves the DST_REF_BACKEND lane override ("" = sqlite).
func envRefBackend(t *testing.T) RefBackend {
	t.Helper()
	switch v := os.Getenv("DST_REF_BACKEND"); v {
	case "", string(RefBackendSQLite):
		return RefBackendSQLite
	case string(RefBackendS3WAL):
		return RefBackendS3WAL
	default:
		t.Fatalf("bad DST_REF_BACKEND %q (want sqlite or s3wal)", v)
		return ""
	}
}

// TestDSTFast is the PR-gate run: a fixed handful of seeds per backend
// lane inside a small time budget. The extended sweep lives in
// TestDSTSweep (nightly).
func TestDSTFast(t *testing.T) {
	for _, backend := range dstBackends {
		t.Run(string(backend), func(t *testing.T) {
			crashSteals, pushCrashSteals, pushForcedRescues, casFaults := 0, 0, 0, 0
			casCalls := make(map[string]int)
			casFaultOps := make(map[string]int)
			for _, seed := range []uint64{1, 2, 3} {
				res, err := Run(Config{Seed: seed, Steps: 120, RefBackend: backend})
				if err != nil {
					t.Fatalf("seed %d: infra error: %v", seed, err)
				}
				reportRun(t, res)
				// Guard against a vacuous simulation: faults must fire and refs
				// must actually move.
				if res.Transitions < 10 || res.Deliveries < 10 || res.Faults < 5 {
					t.Errorf("seed %d: suspiciously quiet run: %+v", seed, res)
				}
				crashSteals += res.CrashSteals
				pushCrashSteals += res.PushCrashSteals
				pushForcedRescues += res.PushForcedRescues
				casFaults += res.CASFaults
				for op, n := range res.OpCounts {
					casCalls[op] += n
				}
				for op, n := range res.CASFaultOps {
					casFaultOps[op] += n
				}
			}
			// The sync claim-lease lane must not be vacuous either: across the
			// fixed fast seeds, at least one claim whose holder actually CRASHED
			// mid-run must have been stolen (issue #88 — the dead-holder class the
			// oracle exists for; live runs merely outliving their lease do not
			// count, so this fails if crash injection goes dead).
			if crashSteals == 0 {
				t.Errorf("no dead-holder sync claim steals across the fast seeds: the #88 crash-injection lane is vacuous")
			}
			// Same for the outbound lane (issue #136): dead-holder push steals
			// must occur, and at least one of them must have rescued a force
			// mandate carried ONLY by the dead claim's push_claim_force — the
			// inheritance class the force-monotonicity oracle exists for.
			if pushCrashSteals == 0 {
				t.Errorf("no dead-holder push claim steals across the fast seeds: the #136 crash-injection lane is vacuous")
			}
			if pushForcedRescues == 0 {
				t.Errorf("no force-inheriting push claim steals across the fast seeds: the #136 rescue lane is vacuous")
			}
			// The s3wal lane must actually inject conditional-store transport
			// faults (plan §4 Phase 2's fault plan); the SQLite lane must not
			// see any.
			if backend == RefBackendS3WAL && casFaults == 0 {
				t.Errorf("no cas.* transport faults across the fast seeds: the s3wal fault plan is vacuous")
			}
			if backend == RefBackendSQLite && casFaults != 0 {
				t.Errorf("cas.* transport faults on the SQLite lane: %d", casFaults)
			}
			// Per-op coverage: every advertised cas.* fault-plan surface
			// must be CALLED — cas.List in particular only flows through
			// the node-routed reservation sweep (Codex round 2 on PR
			// #159) — and must FIRE at least once, which the
			// deterministic seedCASFaultFloor one-shots guarantee per
			// run; an aggregate-only fault guard would pass with a
			// surface's failure path disabled (Codex round 3).
			if backend == RefBackendS3WAL {
				for _, op := range []string{OpCASGet, OpCASCreate, OpCASAdvance, OpCASList} {
					if casCalls[op] == 0 {
						t.Errorf("no %s calls across the fast seeds: that fault-plan surface is vacuous", op)
					}
					if casFaultOps[op] == 0 {
						t.Errorf("no %s faults fired across the fast seeds: that failure path is untested", op)
					}
				}
			}
		})
	}
}

// TestDSTDeterminism runs the same seed twice per backend lane and demands
// identical traces (acceptance criterion: same seed, same op sequence,
// same outcome).
func TestDSTDeterminism(t *testing.T) {
	for _, backend := range dstBackends {
		t.Run(string(backend), func(t *testing.T) {
			run := func() Result {
				res, err := Run(Config{Seed: 7, Steps: 80, RefBackend: backend})
				if err != nil {
					t.Fatalf("infra error: %v", err)
				}
				return res
			}
			a, b := run(), run()
			if a.TraceHash != b.TraceHash {
				t.Fatalf("same seed diverged: %s vs %s", a.TraceHash, b.TraceHash)
			}
			if a.Transitions != b.Transitions || a.Deliveries != b.Deliveries || a.Faults != b.Faults {
				t.Fatalf("same seed diverged in stats: %+v vs %+v", a, b)
			}
			reportRun(t, a)
			reportRun(t, b)
		})
	}
}

// TestDSTSeed reproduces one seed exactly: the triage entry point.
//
//	DST_SEED=<seed> [DST_STEPS=<n>] go test ./internal/simtest -run 'TestDSTSeed' -v
func TestDSTSeed(t *testing.T) {
	seedStr := os.Getenv("DST_SEED")
	if seedStr == "" {
		t.Skip("set DST_SEED to reproduce a specific simulation run")
	}
	seed, err := strconv.ParseUint(seedStr, 10, 64)
	if err != nil {
		t.Fatalf("bad DST_SEED %q: %v", seedStr, err)
	}
	cfg := Config{Seed: seed, RefBackend: envRefBackend(t)}
	if v := os.Getenv("DST_STEPS"); v != "" {
		cfg.Steps, err = strconv.Atoi(v)
		if err != nil {
			t.Fatalf("bad DST_STEPS %q: %v", v, err)
		}
	}
	if testing.Verbose() {
		cfg.Trace = os.Stdout
	}
	res, err := Run(cfg)
	if err != nil {
		t.Fatalf("infra error: %v", err)
	}
	reportRun(t, res)
}

// dstRegressions is the pinned-seed corpus: every entry is a seed that once
// violated an oracle (found by the nightly sweep), replayed at its recorded
// step count on its lane by TestDSTRegressions inside the PR gate
// (`go test ./...`). An entry is regression armor for the harness+driver
// pair that produced it: a harness change that legitimately re-schedules
// the PRNG stream must re-validate the entry against its issue, never drop
// it silently.
var dstRegressions = []struct {
	seed    uint64
	steps   int
	backend RefBackend
	issue   string // the violation the seed reproduced
}{
	// A rebooted node's first manifest read served a superseded version
	// (the since-retired SimCAS stale-read arm), so its
	// SweepExpiredPackReservations concluded "nothing to prune" from a
	// snapshot that predated a competitor's reservation — a read-only
	// verdict no conditional PUT could catch. The s3wal store contract
	// requires strong read-after-write consistency; the arm modelled a
	// store the design excludes.
	{833534285, 103, RefBackendS3WAL, "#216 reserved bytes survive a full reservation sweep"},
}

// TestDSTRegressions replays the pinned-seed corpus (see dstRegressions).
func TestDSTRegressions(t *testing.T) {
	for _, tc := range dstRegressions {
		t.Run(fmt.Sprintf("%s/%d", tc.backend, tc.seed), func(t *testing.T) {
			res, err := Run(Config{Seed: tc.seed, Steps: tc.steps, RefBackend: tc.backend})
			if err != nil {
				t.Fatalf("infra error: %v", err)
			}
			reportRun(t, res)
			// Only a violation stops a run early; an early stop without
			// one is a harness bug, not a passing replay.
			if res.Steps != tc.steps && len(res.Violations) == 0 {
				t.Errorf("%s: ran %d of %d steps without a violation", tc.issue, res.Steps, tc.steps)
			}
		})
	}
}

// TestDSTSweep is the extended seed sweep (nightly CI):
//
//	DST_SWEEP_SEEDS=<count> [DST_SWEEP_BASE=<base>] [DST_STEPS=<n>] \
//	  go test ./internal/simtest -run 'TestDSTSweep' -v -timeout=...
func TestDSTSweep(t *testing.T) {
	countStr := os.Getenv("DST_SWEEP_SEEDS")
	if countStr == "" {
		t.Skip("set DST_SWEEP_SEEDS to run the extended sweep")
	}
	count, err := strconv.Atoi(countStr)
	if err != nil || count < 1 {
		t.Fatalf("bad DST_SWEEP_SEEDS %q: %v", countStr, err)
	}
	var base uint64
	if v := os.Getenv("DST_SWEEP_BASE"); v != "" {
		base, err = strconv.ParseUint(v, 10, 64)
		if err != nil {
			t.Fatalf("bad DST_SWEEP_BASE %q: %v", v, err)
		}
	}
	steps := 300
	if v := os.Getenv("DST_STEPS"); v != "" {
		steps, err = strconv.Atoi(v)
		if err != nil {
			t.Fatalf("bad DST_STEPS %q: %v", v, err)
		}
	}
	backend := envRefBackend(t)
	for i := range count {
		seed := base + uint64(i)
		res, err := Run(Config{Seed: seed, Steps: steps, RefBackend: backend})
		if err != nil {
			t.Fatalf("seed %d: infra error: %v", seed, err)
		}
		reportRun(t, res)
		if t.Failed() {
			return // first failing seed is enough; keep the log focused
		}
	}
}

// TestHydrateAbsorbsInjectedNotFound ties the harness to issue #6 directly:
// a transient missing pack blob during hydration must be absorbed by the
// bounded re-list retry.
func TestHydrateAbsorbsInjectedNotFound(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st := openRefStore(t)
	bl := openBlobStore(t)

	// Mint one commit and publish it through the real pipeline shape.
	scratch := filepath.Join(dir, "scratch.git")
	if err := initScratchRepo(scratch); err != nil {
		t.Fatal(err)
	}
	mc, err := mintCommit(scratch, "", "hydrate retry test", 1)
	if err != nil {
		t.Fatal(err)
	}
	packPath, err := packObjects(scratch, dir, []string{mc.blobSHA, mc.treeSHA, mc.commitSHA})
	if err != nil {
		t.Fatal(err)
	}
	key := blob.PackKey("r", "000001")
	if err := bl.UploadFile(ctx, key, packPath); err != nil {
		t.Fatal(err)
	}
	if err := st.RegisterPacks(ctx, "r", []ref.Pack{{ID: "000001", Key: key}}); err != nil {
		t.Fatal(err)
	}
	if won, err := st.CASRef(ctx, "r", "refs/heads/main", ref.ZeroSHA, mc.commitSHA); !won || err != nil {
		t.Fatalf("CASRef = (%v, %v)", won, err)
	}

	inj := NewInjector(1)
	inj.FailNext(OpBlobDownloadFile, FaultNotFound)
	cacheDir := filepath.Join(dir, "cache")
	repoPath, refsErr, err := githttp.Hydrate(ctx,
		NewSimRefStore(st, inj), NewSimBlobStore(bl, inj), cacheDir, "r", "main")
	if err != nil || refsErr != nil {
		t.Fatalf("Hydrate = (err=%v, refsErr=%v), want transient fault absorbed", err, refsErr)
	}
	if got := inj.Count(OpBlobDownloadFile); got < 2 {
		t.Errorf("DownloadFile count = %d, want >= 2 (faulted attempt + retry)", got)
	}
	refs, err := forEachRef(repoPath)
	if err != nil {
		t.Fatal(err)
	}
	if refs["refs/heads/main"] != mc.commitSHA {
		t.Errorf("hydrated main = %q, want %q", refs["refs/heads/main"], mc.commitSHA)
	}
}
