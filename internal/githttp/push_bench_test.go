// Phase 4 push-path e2e benchmark (issue #133, plan §4): REAL `git push`
// round-trips over loopback against the TestMain-booted serve node, on
// whichever backend lane CODESTORAGE_TEST_BACKEND selected (s3wal / local /
// pg). Unlike the hydrate benches, this one deliberately DOES use the
// booted e2e environment — that is the point: each sample is the full
// client → smart-HTTP → receive-pack → hook re-exec → ref-CAS → blob path
// of a production push. Run once per lane; the lane name lands in the
// sub-benchmark name so results from separate invocations stay
// distinguishable.
//
// Cells: writers=1 is the uncontended floor (a single pusher on its own
// fresh repo — every cell registers a distinct repo); writers=2/8/32 put N
// concurrent pushers on ONE repo, each pushing its own branch, so every
// push contends on the repo group's ref authority (the s3wal manifest CAS,
// the SQLite write lock, or PG row locks) without git-level
// non-fast-forward rejections.
//
// Only the `git push` subprocess runs (and is timed) inside the sustained
// window: every writer's commit chain is pre-generated during setup, so no
// other git subprocess shares the measured interval. Each sample still
// includes one git process startup (a few ms), documented in
// docs/plans/s3wal-bench-results.md. The standard benchmark timer is
// stopped for all orchestration (registration, seeding, clones, chain
// generation, warmup, verification) and runs only across the sustained
// windows; the reported ns/op is overridden to timed-window wall time per
// successful push (the reciprocal of push/s) so benchstat tracks
// throughput, not orchestration. Per-op CSVs land in
// CODESTORAGE_BENCH_CSV_DIR when set (same contract as the s3wal
// microbenches). The sustained window defaults to 5 s per cell to keep
// nightly runs bounded; CODESTORAGE_BENCH_PUSH_SECS=60 gives the plan §3
// campaign windows.
//
// The bench has two mutually exclusive campaign modes (Codex round-3: a
// mid-window ls-remote observer is itself a reader contending with the
// measured pushes, and each poll can hydrate the shared cache repo — the
// very mechanism behind issue #170 — so observation must not share a
// window with performance measurement):
//   - performance mode (default): no observer runs inside the window; the
//     end-of-window accepted-tips check still verifies durability. These
//     are the published performance numbers and the nightly benchstat
//     series.
//   - correctness mode (CODESTORAGE_BENCH_OBSERVE=1): the pushObserver
//     polls ls-remote mid-window (~4/s) and validates every observation
//     against the pre-generated chains. Latency/throughput from these
//     runs are NOT performance results and are reported separately.
//
// A truly noninterfering observation path does not exist: any read
// through the serve node shares the hydration path with the pushers, and
// reading the bucket directly would bypass the authority semantics
// (hydration, generation reads) the detector must witness.
package githttp

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/coder/code-storage/internal/auth"
)

const (
	// envBenchPushSecs overrides the per-cell sustained push window.
	envBenchPushSecs = "CODESTORAGE_BENCH_PUSH_SECS"
	// envBenchCSVDir mirrors the s3wal microbench contract: one raw
	// per-op CSV per bench cell when set (plan §3: raw CSVs published
	// alongside the summary tables).
	envBenchCSVDir = "CODESTORAGE_BENCH_CSV_DIR"
	// benchPushWarmup is the untimed collective warmup before each
	// measured window (plan §3 method: ≥ 20 ops discarded per cell).
	benchPushWarmup = 20
	// benchChainPushRate is the assumed per-writer ceiling on sustained
	// push throughput, used to size the pre-generated commit chains (no
	// `git commit` may run inside the window). Uncontended loopback
	// pushes bottom out around 100 ms (≈ 10/s); 18/s adds headroom. A
	// writer that still exhausts its chain ends participation early and
	// is counted in exhausted-writers.
	benchChainPushRate = 18
	// benchObserverInterval is the mid-window ls-remote poll cadence
	// (correctness mode only).
	benchObserverInterval = 250 * time.Millisecond
	// benchSetupPushTimeout bounds git subprocesses outside the sustained
	// window (seed push, warmup pushes, observer polls): generous, but a
	// stalled serve node must never hang the bench.
	benchSetupPushTimeout = 2 * time.Minute
	// benchPushGrace bounds a window push past the window end. The window
	// deadline is only checked between pushes, so without a per-subprocess
	// deadline one stalled response would block the writer forever; a push
	// killed at the deadline records as an ordinary ambiguous failure.
	benchPushGrace = 30 * time.Second
	// envBenchObserve selects correctness mode: "1" runs the mid-window
	// pushObserver, anything else keeps the window observer-free (the
	// performance mode; see the package comment).
	envBenchObserve = "CODESTORAGE_BENCH_OBSERVE"
)

// benchObserveMode reports whether this run is a correctness campaign.
func benchObserveMode() bool { return os.Getenv(envBenchObserve) == "1" }

func benchPushWindow(b *testing.B) time.Duration {
	b.Helper()
	v := os.Getenv(envBenchPushSecs)
	if v == "" {
		return 5 * time.Second
	}
	secs, err := strconv.Atoi(v)
	if err != nil || secs <= 0 {
		b.Fatalf("invalid %s=%q", envBenchPushSecs, v)
	}
	return time.Duration(secs) * time.Second
}

// pushSample is one successful push: which writer made it and how long the
// `git push` subprocess took.
type pushSample struct {
	writer  int
	latency time.Duration
}

// pushWindowResult aggregates one sustained window. ambiguous counts
// writers whose final accepted-tip set held more than one value (trailing
// failed pushes — the detector accepted any of them; see runPushWindow);
// exhausted counts writers that ran out of pre-generated commits before
// the deadline; polls counts successful observer ls-remote rounds.
type pushWindowResult struct {
	samples   []pushSample
	failures  int64
	maxStarve time.Duration
	elapsed   time.Duration
	packBytes int
	ambiguous int64
	exhausted int64
	polls     int64
	// pollsInWindow counts polls whose ls-remote COMPLETED before the
	// window deadline: only those can witness in-window transients, so
	// correctness runs require a coverage floor of them (a stalled node
	// whose polls all finish after the window would otherwise "pass"
	// having observed nothing).
	pollsInWindow int64
}

func benchMS(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// pushPercentile returns the nearest-rank percentile of sorted latencies.
func pushPercentile(b *testing.B, sorted []time.Duration, p float64) time.Duration {
	b.Helper()
	if len(sorted) == 0 || p <= 0 || p > 100 {
		b.Fatalf("pushPercentile: %d samples, p=%v", len(sorted), p)
	}
	idx := int(float64(len(sorted))*p/100+0.999999) - 1
	idx = max(0, min(idx, len(sorted)-1))
	return sorted[idx]
}

// tryGitDeadline runs git like tryGit but kills the WHOLE process group
// once the timeout expires: `git push` forks helpers (pack-objects, the
// transport) that a bare CommandContext kill would orphan, leaving them
// holding client-repo locks. Production git callers use plain
// CommandContext; the group kill is bench-specific because the harness
// owns its client repos outright. A killed push surfaces as an ordinary
// error to the caller (plus the context cause).
func tryGitDeadline(dir string, timeout time.Duration, args ...string) (string, error) {
	full := append(append([]string{}, gitIdentity...), args...)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", full...)
	cmd.Dir = dir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	out, err := cmd.CombinedOutput()
	if err != nil && ctx.Err() != nil {
		err = fmt.Errorf("%w (process group killed at the %s deadline)", ctx.Err(), timeout)
	}
	return string(out), err
}

// benchGit runs git (with the shared test identity) and fails the benchmark
// on error. Only safe on the benchmark goroutine (Fatalf); writer
// goroutines use tryGit directly and report via b.Errorf.
func benchGit(b *testing.B, dir string, args ...string) string {
	b.Helper()
	out, err := tryGit(dir, args...)
	if err != nil {
		b.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

func benchRegisterRepo(b *testing.B, name string) string {
	b.Helper()
	repo, err := testMeta.CreateRepo(context.Background(), testOrg, name, "main")
	if err != nil {
		b.Fatalf("register repo %q: %v", name, err)
	}
	return repo.RepoID
}

func benchMintToken(b *testing.B, repo string) string {
	b.Helper()
	token, err := auth.MintToken(orgES256Key, "ES256", testClaims(repo, "git:read,git:write"))
	if err != nil {
		b.Fatalf("mint token: %v", err)
	}
	return token
}

// benchSeedRepo creates the client repo every writer clones: one commit on
// main carrying data.txt, the file each per-op commit rewrites.
func benchSeedRepo(b *testing.B) string {
	b.Helper()
	dir := b.TempDir()
	benchGit(b, dir, "init", "-b", "main", ".")
	if err := os.WriteFile(filepath.Join(dir, "data.txt"), []byte("seed\n"), 0o644); err != nil {
		b.Fatal(err)
	}
	benchGit(b, dir, "add", ".")
	benchGit(b, dir, "commit", "-m", "seed")
	return dir
}

// benchCommitOp rewrites the tracked data file and commits, returning the
// new HEAD (used for the pack-size measurement on the seed repo).
func benchCommitOp(dir string, writer, op int) (string, error) {
	content := fmt.Sprintf("writer %d op %d\n", writer, op)
	if err := os.WriteFile(filepath.Join(dir, "data.txt"), []byte(content), 0o644); err != nil {
		return "", err
	}
	if out, err := tryGit(dir, "commit", "-a", "-m", fmt.Sprintf("w%d op %d", writer, op)); err != nil {
		return "", fmt.Errorf("commit: %w\n%s", err, out)
	}
	out, err := tryGit(dir, "rev-parse", "HEAD")
	if err != nil {
		return "", fmt.Errorf("rev-parse: %w\n%s", err, out)
	}
	return strings.TrimSpace(out), nil
}

// benchPackBytes builds the pack a one-commit push transfers (commit + tree
// + small blob on top of base) via git pack-objects and returns its byte
// size — the documented per-push pack size in the results tables.
func benchPackBytes(b *testing.B, dir, base, head string) int {
	b.Helper()
	args := append(append([]string{}, gitIdentity...), "pack-objects", "-q", "--stdout", "--revs")
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Stdin = strings.NewReader(head + "\n^" + base + "\n")
	out, err := cmd.Output()
	if err != nil {
		b.Fatalf("git pack-objects: %v", err)
	}
	if len(out) == 0 {
		b.Fatal("git pack-objects: empty pack")
	}
	return len(out)
}

// benchGenerateChain pre-generates n linear commits on the writer's
// checked-out branch and returns their shas oldest-first: the writers only
// ever run `git push` inside the sustained window, so client-side commit
// generation can never leak onto the timed path.
func benchGenerateChain(dir string, writer, n int) ([]string, error) {
	if n <= 0 {
		return nil, fmt.Errorf("benchGenerateChain: n=%d", n)
	}
	for op := 1; op <= n; op++ {
		content := fmt.Sprintf("writer %d op %d\n", writer, op)
		if err := os.WriteFile(filepath.Join(dir, "data.txt"), []byte(content), 0o644); err != nil {
			return nil, err
		}
		if out, err := tryGit(dir, "commit", "-a", "-q", "-m", fmt.Sprintf("w%d op %d", writer, op)); err != nil {
			return nil, fmt.Errorf("commit %d: %w\n%s", op, err, out)
		}
	}
	// Limiting applies before --reverse: the newest n commits, oldest
	// first — exactly the generated chain.
	out, err := tryGit(dir, "log", "-n", strconv.Itoa(n), "--reverse", "--format=%H")
	if err != nil {
		return nil, fmt.Errorf("log: %w\n%s", err, out)
	}
	shas := strings.Fields(out)
	if len(shas) != n {
		return nil, fmt.Errorf("chain length %d, want %d", len(shas), n)
	}
	return shas, nil
}

// pushObserver validates mid-window ls-remote observations against the
// writers' pre-generated commit chains. The end-of-window detector alone
// cannot see an acknowledged update that is transiently dropped and later
// restored by a descendant push; the observer closes that gap up to its
// poll cadence (a rewind fully contained between two polls remains
// undetectable — stated in the results doc rather than overclaimed).
// Rules per writer branch:
//  1. the observed tip must be "" (absent) or on the writer's chain —
//     anything else is a foreign write;
//  2. the tip's chain index must not exceed the writer's highest attempted
//     push (the server cannot hold a commit never sent);
//  3. observations must never fall below the floor min(maxObserved,
//     ackedBeforePoll): every index that was OBSERVED on the server at any
//     earlier poll — in-flight observations are retained, not discarded —
//     and whose acknowledgement completed BEFORE the current poll started
//     is committed history predating the snapshot (chains are linear and
//     pushes fast-forward only, so acked ≥ m proves chain[m-1] is an
//     acknowledged ancestor, and a prior observation of m proves the
//     server served it), so an observation below it is a lost
//     acknowledged update even if the final state ends up legal.
type pushObserver struct {
	idx []map[string]int // per writer: sha → 1-based chain index
	// maxObserved retains the highest chain index ever observed per
	// writer, unconditionally: an in-flight tip observed before its ack
	// is promoted into the rewind floor by the pre-poll acked snapshot of
	// a LATER poll (discarding it would mask a rewind that happens after
	// the ack arrives — Codex round 4), so detection of a rewind under a
	// just-acked tip lags by at most one poll but never false-positives.
	maxObserved []int
}

func newPushObserver(chains [][]string) *pushObserver {
	o := &pushObserver{idx: make([]map[string]int, len(chains)), maxObserved: make([]int, len(chains))}
	for i, chain := range chains {
		m := make(map[string]int, len(chain))
		for j, sha := range chain {
			m[sha] = j + 1
		}
		o.idx[i] = m
	}
	return o
}

// observe validates one observation for writer i; sha is the server tip
// ("" = branch absent). The two indices have OPPOSITE load-ordering
// requirements, each safe on its own terms:
//   - attempted must be loaded AFTER the ls-remote completed: it can only
//     OVERCOUNT what the server could have shown when the snapshot was
//     taken, so the tip-beyond-attempted rule never false-positives (a
//     genuine violation — a commit never sent — stays a violation under
//     any larger attempted value);
//   - acked must be SNAPSHOTTED BEFORE the poll starts: only an ack that
//     completed before the snapshot proves the server held that commit at
//     snapshot time. A post-poll load would reject a legal pre-ack
//     snapshot when the ack lands between the ls-remote and the load
//     (Codex round 7) — bounded-stale hydrate reads make a pre-ack tip
//     legal, and an ack being true at load time does not make it true at
//     snapshot time. The pre-poll snapshot means the floor lags acks by
//     at most one poll (a real rewind under a just-acked tip is caught by
//     the NEXT poll, whose snapshot includes the ack), delaying detection
//     but never false-positive.
func (o *pushObserver) observe(writer int, sha string, attempted, acked int) error {
	idx := 0
	if sha != "" {
		var ok bool
		if idx, ok = o.idx[writer][sha]; !ok {
			return fmt.Errorf("writer %d: foreign tip %q on the server", writer, sha)
		}
	}
	if idx > attempted {
		return fmt.Errorf("writer %d: server tip %q is chain index %d, beyond the %d attempted pushes", writer, sha, idx, attempted)
	}
	// The floor is evaluated per poll from the PRIOR maxObserved (this
	// observation cannot vouch for itself) and the pre-poll acked
	// snapshot: an index is floor-eligible once it has been observed at
	// any time AND acknowledged before this poll began.
	if floor := min(o.maxObserved[writer], acked); idx < floor {
		return fmt.Errorf("writer %d: server tip rewound to chain index %d, below floor %d (observed max %d, acked %d)", writer, idx, floor, o.maxObserved[writer], acked)
	}
	o.maxObserved[writer] = max(o.maxObserved[writer], idx)
	return nil
}

// TestPushObserverLegality red-checks the observer's legality rules with a
// synthetic observation sequence. The decisive case is the transient
// rewind: an acknowledged tip is observed, the branch rewinds below it,
// and a later descendant push restores a legal-looking final state — the
// end-of-window accepted-tips check alone would pass such a window, so
// only the observer can flag it.
func TestPushObserverLegality(t *testing.T) {
	chain := []string{"aaa", "bbb", "ccc", "ddd"}
	o := newPushObserver([][]string{chain})
	steps := []struct {
		name             string
		sha              string
		attempted, acked int
		wantErr          bool
	}{
		{"absent before any push", "", 0, 0, false},
		{"attempted-but-unacked tip visible", "aaa", 1, 0, false},
		{"acked tip observed (raises the floor)", "bbb", 2, 2, false},
		{"foreign tip", "zzz", 2, 2, true},
		{"tip beyond attempted pushes", "ddd", 3, 2, true},
		{"holding at the floor", "bbb", 3, 2, false},
		{"transient rewind below the observed-acked tip", "aaa", 3, 2, true},
		{"branch vanished below the observed-acked tip", "", 3, 2, true},
		{"descendant restores a legal final state (final check alone would pass)", "ccc", 3, 3, false},
	}
	for _, s := range steps {
		if err := o.observe(0, s.sha, s.attempted, s.acked); (err != nil) != s.wantErr {
			t.Errorf("%s: observe(%q, attempted=%d, acked=%d) = %v, wantErr=%v",
				s.name, s.sha, s.attempted, s.acked, err, s.wantErr)
		}
	}

	// Codex round-4 retention + round-7 pre-poll snapshot interleaving:
	// poll1 observes an in-flight tip k while acked is still k−1
	// (retained into maxObserved); poll2 legitimately snapshots the
	// pre-ack server state k−1 while the ack for k lands only after
	// poll2's floor snapshot — LEGAL (a post-poll acked load would
	// false-positive here, the round-7 finding); poll3's pre-poll
	// snapshot includes the ack, so the still-rewound k−1 is now a
	// violation — the real rewind is caught one poll later, never
	// falsely.
	o2 := newPushObserver([][]string{chain})
	steps2 := []struct {
		name             string
		sha              string
		attempted, acked int // acked = the PRE-POLL snapshot
		wantErr          bool
	}{
		{"acked baseline observed", "aaa", 1, 1, false},
		{"in-flight tip observed before its ack (retained)", "bbb", 2, 1, false},
		{"pre-ack snapshot with the ack landing after the floor snapshot (legal)", "aaa", 2, 1, false},
		{"rewind still visible once the ack precedes the poll (caught)", "aaa", 2, 2, true},
	}
	for _, s := range steps2 {
		if err := o2.observe(0, s.sha, s.attempted, s.acked); (err != nil) != s.wantErr {
			t.Errorf("%s: observe(%q, attempted=%d, acked=%d) = %v, wantErr=%v",
				s.name, s.sha, s.attempted, s.acked, err, s.wantErr)
		}
	}
}

// runPushWindow registers a fresh repo, seeds refs/heads/main, clones one
// working dir per writer (local clone — object transfer is untimed),
// pre-generates every writer's commit chain, runs the collective warmup,
// then lets the writers push continuously for the window while the
// pushObserver polls. Each writer owns refs/heads/w<i>. After the window
// one ls-remote acts as the final lost-update detector: a writer's branch
// must hold one of the writer's ACCEPTED tips. On a successful push the
// set collapses to that acknowledged tip; a failed push APPENDS its
// attempted tip instead of being dropped, because the client cannot
// distinguish a definitive rejection from a transport failure — or the
// issue #170 spurious-rejection race — whose ref update already landed.
// Every legal server state (the last ack, or any tip stacked by
// ambiguously-failed pushes since) is therefore in the set, every writer
// is always verified, and any THIRD value is a violation. This keeps such
// windows detector-valid instead of inconclusive; the count of writers
// that ended on an ambiguous set is reported as ambiguous-final-tips.
// Mid-window legality (including transient rewinds the final check cannot
// see) is the observer's job.
func runPushWindow(b *testing.B, writers int, window time.Duration) pushWindowResult {
	b.Helper()
	if writers <= 0 {
		b.Fatalf("runPushWindow: %d writers", writers)
	}
	// Orchestration stays off the standard benchmark timer; only the
	// sustained window below is timed.
	b.StopTimer()
	repo := benchRegisterRepo(b, fmt.Sprintf("bench-push-w%d-%d", writers, time.Now().UnixNano()))
	token := benchMintToken(b, repo)
	url := remoteURL(nodeA, repo, token)

	seed := benchSeedRepo(b)
	if out, err := tryGitDeadline(seed, benchSetupPushTimeout, "push", url, "HEAD:refs/heads/main"); err != nil {
		b.Fatalf("seed push: %v\n%s", err, out)
	}

	perWriter := (benchPushWarmup + writers - 1) / writers
	chainLen := int(window.Seconds()*benchChainPushRate) + perWriter + 5
	dirs := make([]string, writers)
	chains := make([][]string, writers)
	var genWG sync.WaitGroup
	for i := range dirs {
		dirs[i] = filepath.Join(b.TempDir(), fmt.Sprintf("w%d", i))
		benchGit(b, "", "clone", seed, dirs[i])
		benchGit(b, dirs[i], "checkout", "-b", fmt.Sprintf("w%d", i))
		genWG.Add(1)
		go func(i int) {
			defer genWG.Done()
			chain, err := benchGenerateChain(dirs[i], i, chainLen)
			if err != nil {
				b.Errorf("writer %d chain: %v", i, err)
				return
			}
			chains[i] = chain
		}(i)
	}
	genWG.Wait()
	if b.Failed() {
		b.FailNow()
	}

	var res pushWindowResult

	// Representative pack size, measured on an un-pushed seed commit so it
	// never perturbs the writers' branches.
	base := strings.TrimSpace(benchGit(b, seed, "rev-parse", "HEAD"))
	head, err := benchCommitOp(seed, -1, 0)
	if err != nil {
		b.Fatalf("pack-size commit: %v", err)
	}
	res.packBytes = benchPackBytes(b, seed, base, head)

	var (
		mu sync.Mutex
		// acceptedTips[i] holds every tip the server may legally end on
		// for writer i (see the function comment). "" means the branch
		// may not exist (no push ever acknowledged or attempted).
		acceptedTips = make([][]string, writers)
		// attempted/acked publish each writer's progress as 1-based chain
		// indices for the observer (see pushObserver.observe for the
		// required load ordering).
		attempted = make([]atomic.Int64, writers)
		acked     = make([]atomic.Int64, writers)
	)
	var start time.Time // written before close(startCh); read after receive
	startCh := make(chan struct{})
	var warmed, done sync.WaitGroup
	for i := range writers {
		warmed.Add(1)
		done.Add(1)
		go func(i int) {
			defer done.Done()
			dir, chain := dirs[i], chains[i]
			branch := fmt.Sprintf("refs/heads/w%d", i)
			accepted := []string{""}
			k := 0 // chain commits pushed so far; chain[k] is next
			var (
				localSamples []pushSample
				failures     int64
				maxStarve    time.Duration
				exhausted    int64
				lastSuccess  time.Time // zero until the measured window starts
			)
			// Merge in a defer so a writer that stops at ANY point still
			// contributes its recorded pushes and its accepted-tip set
			// (dropping the set would let an unaccounted server update
			// pass the detector).
			defer func() {
				if !lastSuccess.IsZero() {
					maxStarve = max(maxStarve, time.Since(lastSuccess))
				}
				mu.Lock()
				res.samples = append(res.samples, localSamples...)
				res.failures += failures
				res.maxStarve = max(res.maxStarve, maxStarve)
				res.exhausted += exhausted
				acceptedTips[i] = accepted
				mu.Unlock()
			}()
			// pushNext sends the next pre-generated commit, publishing the
			// attempt index BEFORE contacting the server so the observer
			// can bound what it may legally see there. timeout bounds the
			// push subprocess (a kill records as an ordinary ambiguous
			// failure: the attempted tip stays in the accepted set and
			// the published attempt index stays pending for the observer,
			// exactly like any other failed push).
			pushNext := func(timeout time.Duration) (time.Duration, string, error) {
				sha := chain[k]
				k++
				attempted[i].Store(int64(k))
				t0 := time.Now()
				out, err := tryGitDeadline(dir, timeout, "push", url, sha+":"+branch)
				d := time.Since(t0)
				if err != nil {
					// The attempted tip stays accepted: the rejection may
					// have been ambiguous — a transport failure or the
					// issue #170 spurious-rejection race — and the ref
					// update may have landed. The next push fast-forwards
					// over it either way.
					accepted = append(accepted, sha)
					return d, out, err
				}
				acked[i].Store(int64(k))
				accepted = []string{sha}
				return d, out, nil
			}
			for range perWriter {
				if _, out, err := pushNext(benchSetupPushTimeout); err != nil {
					// Tolerated like the measured loop (see pushNext).
					b.Logf("writer %d warmup push rejected (op %d): %v\n%s", i, k, err, out)
				}
			}
			warmed.Done()
			<-startCh
			deadline := start.Add(window)
			lastSuccess = start
			for time.Now().Before(deadline) {
				if k >= len(chain) {
					// Chain exhaustion INVALIDATES the cell: the writer
					// stops pushing early, so the numbers would silently
					// understate aggregate capacity exactly when a
					// backend outruns the benchChainPushRate sizing
					// assumption (i.e. right when an optimization lands).
					// Sizing stays static — generating commits here would
					// put git subprocesses back inside the timed window —
					// so the honest behavior is a loud failure telling us
					// to raise the constant. The exhausted-writers metric
					// stays as belt and suspenders for output scraping.
					exhausted = 1
					b.Errorf("writer %d exhausted its %d-commit chain after %d pushes before the deadline — raise benchChainPushRate (currently assumes ≤ %d push/s per writer)", i, len(chain), k, benchChainPushRate)
					break
				}
				// Bounded by the remaining window plus a grace margin: a
				// stalled serve response must not hang the writer past
				// the window end.
				d, out, err := pushNext(max(time.Until(deadline), 0) + benchPushGrace)
				if err != nil {
					// A rejected push is a real result under sustained
					// contention (e.g. SQLITE_BUSY surfacing through the
					// hook), not a harness bug: count it and keep going.
					failures++
					if failures == 1 {
						b.Logf("writer %d first push failure (op %d): %v\n%s", i, k, err, out)
					}
					continue
				}
				now := time.Now()
				localSamples = append(localSamples, pushSample{writer: i, latency: d})
				maxStarve = max(maxStarve, now.Sub(lastSuccess))
				lastSuccess = now
			}
		}(i)
	}
	warmed.Wait()

	// Mid-window observer — correctness mode only: the observer is itself
	// a reader contending with the measured pushes, and each poll may
	// hydrate the shared cache repo (the issue #170 mechanism), so it
	// never shares a window with performance measurement (a transient
	// contained between two polls stays undetectable either way).
	obsStop := make(chan struct{})
	var obsDone sync.WaitGroup
	if benchObserveMode() {
		obs := newPushObserver(chains)
		obsDone.Go(func() {
			// The window start/deadline are read after startCh closes
			// (same happens-before pattern as the writers).
			<-startCh
			windowDeadline := start.Add(window)
			ticker := time.NewTicker(benchObserverInterval)
			defer ticker.Stop()
			for {
				select {
				case <-obsStop:
					return
				case <-ticker.C:
				}
				// The acked snapshot MUST precede the poll (observe's
				// load-ordering contract): an ack landing mid-poll may
				// postdate the ls-remote snapshot, and using it for the
				// floor would reject a legal pre-ack observation.
				ackedBefore := make([]int, writers)
				for i := range writers {
					ackedBefore[i] = int(acked[i].Load())
				}
				out, err := tryGitDeadline(seed, benchSetupPushTimeout, "ls-remote", url)
				if err != nil {
					// A failed poll silently widens the observation gap,
					// so the correctness verdict would be unreliable: the
					// run is inconclusive, and in correctness mode
					// inconclusive means FAILED. Loopback ls-remote
					// failing is abnormal — tolerance is zero, the first
					// error fails the bench (Errorf: FailNow is illegal
					// off the benchmark goroutine) and stops observing.
					b.Errorf("observer ls-remote failed — correctness run inconclusive: %v\n%s", err, out)
					return
				}
				res.polls++
				if time.Now().Before(windowDeadline) {
					res.pollsInWindow++
				}
				tips := map[string]string{}
				for line := range strings.Lines(out) {
					if sha, ref, ok := strings.Cut(strings.TrimSpace(line), "\t"); ok {
						tips[ref] = sha
					}
				}
				// attempted is loaded after the poll, acked was
				// snapshotted before it — see observe's load-ordering
				// contract.
				for i := range writers {
					tip := tips[fmt.Sprintf("refs/heads/w%d", i)]
					if err := obs.observe(i, tip, int(attempted[i].Load()), ackedBefore[i]); err != nil {
						b.Errorf("observer poll %d: %v", res.polls, err)
					}
				}
			}
		})
	}

	start = time.Now()
	b.StartTimer()
	close(startCh)
	done.Wait()
	b.StopTimer()
	res.elapsed = time.Since(start)
	// A poll still in flight here completes, is legality-checked, and
	// counts in polls (not pollsInWindow) before the goroutine exits.
	// That is sound: the writers have already joined, so every ack
	// precedes the straggler's pre-poll snapshot, its floor is maximal
	// and correct, and any tip it can observe is one the final
	// accepted-tips check independently admits. It cannot race the final
	// ls-remote or teardown because obsDone.Wait() precedes both.
	close(obsStop)
	obsDone.Wait()
	if benchObserveMode() {
		// Coverage floor (Codex round 8): a stalled or overloaded node can
		// make every ls-remote outlast the window, in which case the run
		// observed NOTHING in-window and zero violations would be
		// vacuous. Correctness therefore requires that a meaningful
		// fraction of the cadence-expected polls — half, minimum one —
		// complete inside the window: 60 s campaigns expect ~240 and
		// tolerate genuine slow-poll loss down to 120 (the recorded
		// campaigns completed ≥ 151), while a 2 s smoke window expects 8
		// and requires 4. Below that the run is inconclusive, which in
		// correctness mode means FAILED.
		expected := int64(window / benchObserverInterval)
		if floor := max(int64(1), expected/2); res.pollsInWindow < floor {
			b.Errorf("correctness run inconclusive: only %d observer polls completed inside the %s window (%d total; floor %d of %d expected at the %s cadence)",
				res.pollsInWindow, window, res.polls, floor, expected, benchObserverInterval)
		}
	}

	// Lost-update detector: every writer's branch on the server must hold
	// one of that writer's accepted tips — no writer is ever skipped, and
	// a value outside the set is always a violation.
	lsOut, err := tryGitDeadline(seed, benchSetupPushTimeout, "ls-remote", url)
	if err != nil {
		// Bounded like the observer polls: a stalled serve node must fail
		// the cell promptly, not eat the whole go-test timeout.
		b.Fatalf("final ls-remote: %v\n%s", err, lsOut)
	}
	server := map[string]string{}
	for line := range strings.Lines(lsOut) {
		if sha, ref, ok := strings.Cut(strings.TrimSpace(line), "\t"); ok {
			server[ref] = sha
		}
	}
	for i := range writers {
		if len(acceptedTips[i]) > 1 {
			res.ambiguous++
		}
		branch := fmt.Sprintf("refs/heads/w%d", i)
		if got := server[branch]; !slices.Contains(acceptedTips[i], got) {
			b.Errorf("lost update: server %s = %q, want one of writer %d's accepted tips %q", branch, got, i, acceptedTips[i])
		}
	}
	// Hand a running timer back to b.Loop (which stops it itself after the
	// final iteration); the caller's metric aggregation costs microseconds
	// and the published ns/op is overridden regardless.
	b.StartTimer()
	return res
}

// pushCSVDump writes the raw per-push latencies for this bench cell when
// envBenchCSVDir is set (columns: op index, writer, latency).
func pushCSVDump(b *testing.B, samples []pushSample) {
	b.Helper()
	dir := os.Getenv(envBenchCSVDir)
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		b.Fatalf("csv dir: %v", err)
	}
	name := strings.NewReplacer("/", "__", "=", "-").Replace(b.Name()) + ".csv"
	var sb strings.Builder
	sb.WriteString("op,writer,latency_ns\n")
	for i, s := range samples {
		fmt.Fprintf(&sb, "%d,%d,%d\n", i, s.writer, s.latency.Nanoseconds())
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(sb.String()), 0o644); err != nil {
		b.Fatalf("csv write: %v", err)
	}
	b.Logf("raw per-op CSV: %s (%d samples)", path, len(samples))
}

func BenchmarkPushE2E(b *testing.B) {
	window := benchPushWindow(b)
	for _, writers := range []int{1, 2, 8, 32} {
		b.Run(fmt.Sprintf("backend=%s/writers=%d", testBackend, writers), func(b *testing.B) {
			var (
				all           []pushSample
				failures      int64
				maxStarve     time.Duration
				elapsed       time.Duration
				packBytes     int
				ambiguous     int64
				exhausted     int64
				polls         int64
				pollsInWindow int64
			)
			for b.Loop() {
				res := runPushWindow(b, writers, window)
				all = append(all, res.samples...)
				failures += res.failures
				maxStarve = max(maxStarve, res.maxStarve)
				elapsed += res.elapsed
				packBytes = res.packBytes
				ambiguous += res.ambiguous
				exhausted += res.exhausted
				polls += res.polls
				pollsInWindow += res.pollsInWindow
			}
			if len(all) == 0 {
				b.Fatal("no successful pushes recorded")
			}
			lat := make([]time.Duration, len(all))
			for i, s := range all {
				lat[i] = s.latency
			}
			slices.Sort(lat)
			b.ReportMetric(benchMS(pushPercentile(b, lat, 50)), "p50-ms")
			b.ReportMetric(benchMS(pushPercentile(b, lat, 95)), "p95-ms")
			b.ReportMetric(benchMS(pushPercentile(b, lat, 99)), "p99-ms")
			b.ReportMetric(float64(len(all)), "samples")
			if elapsed <= 0 {
				b.Fatal("zero elapsed window time")
			}
			// Override the built-in ns/op with timed-window wall time per
			// successful push (the reciprocal of push/s): the standard
			// timer already covers only the sustained windows, but per
			// window-iteration it would report the window duration —
			// per-push time is the benchstat-meaningful unit.
			b.ReportMetric(float64(elapsed.Nanoseconds())/float64(len(all)), "ns/op")
			// Neutralize the memory series: under -benchmem (the nightly
			// sweep) testing.B would divide the whole window's
			// client+harness allocations by b.N (window iterations) and
			// label that one "push" — an invalid benchstat series. The
			// harness's client-side allocations are meaningless for the
			// server push path anyway, so both built-ins are overridden
			// to zero rather than published.
			b.ReportMetric(0, "B/op")
			b.ReportMetric(0, "allocs/op")
			b.ReportMetric(float64(len(all))/elapsed.Seconds(), "push/s")
			b.ReportMetric(elapsed.Seconds(), "elapsed-s")
			b.ReportMetric(float64(failures), "push-failures")
			b.ReportMetric(float64(ambiguous), "ambiguous-final-tips")
			b.ReportMetric(float64(exhausted), "exhausted-writers")
			b.ReportMetric(float64(polls), "observer-polls")
			b.ReportMetric(float64(pollsInWindow), "observer-polls-in-window")
			b.ReportMetric(benchMS(maxStarve), "max-starve-ms")
			b.ReportMetric(float64(packBytes), "pack-bytes")
			pushCSVDump(b, all)
		})
	}
}
