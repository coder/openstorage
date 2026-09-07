// Command dst reproduces deterministic simulation runs (issue #3): given a
// seed it replays the exact op trace the DST tests execute, printing the
// trace and any invariant violations. Triage loop:
//
//	go run ./cmd/dst -seed <seed> [-steps N] [-v] [-keep]
//
// Exit status: 0 clean, 1 invariant violation, 2 infrastructure failure.
// See internal/simtest/README.md.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/coder/open-storage/internal/simtest"
)

func main() {
	seed := flag.Uint64("seed", 0, "simulation seed (DST_SEED)")
	steps := flag.Int("steps", 0, "scheduler steps (0 = default)")
	nodes := flag.Int("nodes", 0, "simulated nodes (0 = default)")
	backend := flag.String("backend", "", "authority-store lane: sqlite or s3wal (DST_REF_BACKEND; empty = sqlite)")
	verbose := flag.Bool("v", false, "print the trace while running")
	keep := flag.Bool("keep", false, "keep the run directory for inspection (prints its path)")
	flag.Parse()

	cfg := simtest.Config{Seed: *seed, Steps: *steps, Nodes: *nodes, KeepDir: *keep,
		RefBackend: simtest.RefBackend(*backend)}
	if *verbose {
		cfg.Trace = os.Stdout
	}
	if *keep {
		dir, err := os.MkdirTemp("", fmt.Sprintf("dst-seed%d-*", *seed))
		if err != nil {
			fmt.Fprintln(os.Stderr, "dst:", err)
			os.Exit(2)
		}
		cfg.Dir = dir
		fmt.Println("run dir:", dir)
	}

	res, err := simtest.Run(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "dst: infrastructure failure:", err)
		os.Exit(2)
	}
	fmt.Printf("seed=%d backend=%s steps=%d trace=%s transitions=%d deliveries=%d faults=%d cas-faults=%d steals=%d crash-steals=%d push-steals=%d push-crash-steals=%d push-forced-rescues=%d\n",
		res.Seed, res.Backend, res.Steps, res.TraceHash, res.Transitions, res.Deliveries, res.Faults, res.CASFaults, res.Steals, res.CrashSteals,
		res.PushSteals, res.PushCrashSteals, res.PushForcedRescues)
	if len(res.Violations) > 0 {
		for _, v := range res.Violations {
			fmt.Println("VIOLATION:", v)
		}
		os.Exit(1)
	}
	fmt.Println("ok: no invariant violations")
}
