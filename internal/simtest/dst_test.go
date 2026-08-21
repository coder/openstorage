package simtest

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"codestorage/internal/blob"
	"codestorage/internal/githttp"
	"codestorage/internal/ref"
)

// reportRun logs a run's summary and fails the test on violations with
// reproduction instructions.
func reportRun(t *testing.T, res Result) {
	t.Helper()
	t.Logf("seed=%d steps=%d trace=%s transitions=%d deliveries=%d faults=%d",
		res.Seed, res.Steps, res.TraceHash, res.Transitions, res.Deliveries, res.Faults)
	if len(res.Violations) == 0 {
		return
	}
	for _, v := range res.Violations {
		t.Errorf("violation: %s", v)
	}
	t.Errorf("reproduce: DST_SEED=%d DST_STEPS=%d go test ./internal/simtest -run 'TestDSTSeed' -v  (or: go run ./cmd/dst -seed %d -steps %d -v)",
		res.Seed, res.Steps, res.Seed, res.Steps)
}

// TestDSTFast is the PR-gate run: a fixed handful of seeds inside a small
// time budget. The extended sweep lives in TestDSTSweep (nightly).
func TestDSTFast(t *testing.T) {
	for _, seed := range []uint64{1, 2, 3} {
		res, err := Run(Config{Seed: seed, Steps: 120})
		if err != nil {
			t.Fatalf("seed %d: infra error: %v", seed, err)
		}
		reportRun(t, res)
		// Guard against a vacuous simulation: faults must fire and refs
		// must actually move.
		if res.Transitions < 10 || res.Deliveries < 10 || res.Faults < 5 {
			t.Errorf("seed %d: suspiciously quiet run: %+v", seed, res)
		}
	}
}

// TestDSTDeterminism runs the same seed twice and demands identical traces
// (acceptance criterion: same seed, same op sequence, same outcome).
func TestDSTDeterminism(t *testing.T) {
	run := func() Result {
		res, err := Run(Config{Seed: 7, Steps: 80})
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
	cfg := Config{Seed: seed}
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
	for i := 0; i < count; i++ {
		seed := base + uint64(i)
		res, err := Run(Config{Seed: seed, Steps: steps})
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
