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
// Only the `git push` subprocess is timed — commit creation is untimed —
// so each sample includes one git process startup (a few ms), documented
// in docs/plans/s3wal-bench-results.md. Per-op CSVs land in
// CODESTORAGE_BENCH_CSV_DIR when set (same contract as the s3wal
// microbenches). The sustained window defaults to 5 s per cell to keep
// nightly runs bounded; CODESTORAGE_BENCH_PUSH_SECS=60 gives the plan §3
// campaign windows.
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
)

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
// failed pushes — the detector accepted any of them; see runPushWindow).
type pushWindowResult struct {
	samples   []pushSample
	failures  int64
	maxStarve time.Duration
	elapsed   time.Duration
	packBytes int
	ambiguous int64
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
// new HEAD. Errors are returned, not fataled: writer goroutines may not
// call FailNow.
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

// runPushWindow registers a fresh repo, seeds refs/heads/main, clones one
// working dir per writer (local clone — object transfer is untimed), runs
// the collective warmup, then lets the writers push continuously for the
// window. Each writer owns refs/heads/w<i>. After the window one ls-remote
// acts as the lost-update detector: a writer's branch must hold one of the
// writer's ACCEPTED tips. On a successful push the set collapses to that
// acknowledged tip; a failed push APPENDS its attempted tip instead of
// being dropped, because the client cannot distinguish a definitive
// rejection from a transport failure — or the issue #170 spurious-rejection
// race — whose ref update already landed. Every legal server state (the
// last ack, or any tip stacked by ambiguously-failed pushes since) is
// therefore in the set, every writer is always verified, and any THIRD
// value is a violation. This keeps such windows detector-valid instead of
// inconclusive; the count of writers that ended on an ambiguous set is
// reported as ambiguous-final-tips.
func runPushWindow(b *testing.B, writers int, window time.Duration) pushWindowResult {
	b.Helper()
	if writers <= 0 {
		b.Fatalf("runPushWindow: %d writers", writers)
	}
	repo := benchRegisterRepo(b, fmt.Sprintf("bench-push-w%d-%d", writers, time.Now().UnixNano()))
	token := benchMintToken(b, repo)
	url := remoteURL(nodeA, repo, token)

	seed := benchSeedRepo(b)
	benchGit(b, seed, "push", url, "HEAD:refs/heads/main")

	dirs := make([]string, writers)
	for i := range dirs {
		dirs[i] = filepath.Join(b.TempDir(), fmt.Sprintf("w%d", i))
		benchGit(b, "", "clone", seed, dirs[i])
		benchGit(b, dirs[i], "checkout", "-b", fmt.Sprintf("w%d", i))
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
	)
	perWriter := (benchPushWarmup + writers - 1) / writers
	var start time.Time // written before close(startCh); read after receive
	startCh := make(chan struct{})
	var warmed, done sync.WaitGroup
	for i := range writers {
		warmed.Add(1)
		done.Add(1)
		go func(i int) {
			defer done.Done()
			dir := dirs[i]
			refspec := fmt.Sprintf("HEAD:refs/heads/w%d", i)
			accepted := []string{""}
			var (
				localSamples []pushSample
				failures     int64
				maxStarve    time.Duration
				lastSuccess  time.Time // zero until the measured window starts
			)
			// Merge in a defer so a writer that errors out at ANY point —
			// warmup included — still contributes its recorded pushes and
			// its accepted-tip set (mirrors the s3wal contention runner's
			// accounting; dropping the set would let an unaccounted server
			// update pass the detector).
			defer func() {
				if !lastSuccess.IsZero() {
					maxStarve = max(maxStarve, time.Since(lastSuccess))
				}
				mu.Lock()
				res.samples = append(res.samples, localSamples...)
				res.failures += failures
				res.maxStarve = max(res.maxStarve, maxStarve)
				acceptedTips[i] = accepted
				mu.Unlock()
			}()
			op := 0
			warmupOK := true
			for range perWriter {
				op++
				head, err := benchCommitOp(dir, i, op)
				if err != nil {
					b.Errorf("writer %d warmup: %v", i, err)
					warmupOK = false
					break
				}
				if out, err := tryGit(dir, "push", url, refspec); err != nil {
					// Tolerated like the measured loop: under hot-group
					// load a push can be spuriously rejected even though
					// the hook committed the ref (issue #170: a concurrent
					// request's hydration syncs the just-committed ref
					// into the cache repo before receive-pack's own local
					// ref update — "cannot lock ref: reference already
					// exists" on branch creation). The next op stacks a
					// commit and fast-forwards over it.
					accepted = append(accepted, head)
					b.Logf("writer %d warmup push rejected (op %d): %v\n%s", i, op, err, out)
				} else {
					accepted = []string{head}
				}
			}
			warmed.Done()
			<-startCh
			if !warmupOK {
				return
			}
			deadline := start.Add(window)
			lastSuccess = start
			for time.Now().Before(deadline) {
				op++
				head, err := benchCommitOp(dir, i, op)
				if err != nil {
					b.Errorf("writer %d: %v", i, err)
					return
				}
				t0 := time.Now()
				out, err := tryGit(dir, "push", url, refspec)
				d := time.Since(t0)
				if err != nil {
					// A rejected push is a real result under sustained
					// contention (e.g. SQLITE_BUSY surfacing through the
					// hook), not a harness bug: count it and keep going —
					// the next op stacks another commit on this branch.
					// Its tip stays accepted: the rejection may have been
					// ambiguous and the ref update may have landed.
					failures++
					accepted = append(accepted, head)
					if failures == 1 {
						b.Logf("writer %d first push failure (op %d): %v\n%s", i, op, err, out)
					}
					continue
				}
				now := time.Now()
				localSamples = append(localSamples, pushSample{writer: i, latency: d})
				maxStarve = max(maxStarve, now.Sub(lastSuccess))
				lastSuccess = now
				accepted = []string{head}
			}
		}(i)
	}
	warmed.Wait()
	if b.Failed() {
		close(startCh)
		done.Wait()
		b.FailNow()
	}
	start = time.Now()
	close(startCh)
	done.Wait()
	res.elapsed = time.Since(start)

	// Lost-update detector: every writer's branch on the server must hold
	// one of that writer's accepted tips — no writer is ever skipped, and
	// a value outside the set is always a violation.
	server := map[string]string{}
	for line := range strings.Lines(benchGit(b, seed, "ls-remote", url)) {
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
				all       []pushSample
				failures  int64
				maxStarve time.Duration
				elapsed   time.Duration
				packBytes int
				ambiguous int64
			)
			for b.Loop() {
				res := runPushWindow(b, writers, window)
				all = append(all, res.samples...)
				failures += res.failures
				maxStarve = max(maxStarve, res.maxStarve)
				elapsed += res.elapsed
				packBytes = res.packBytes
				ambiguous += res.ambiguous
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
			b.ReportMetric(float64(len(all))/elapsed.Seconds(), "push/s")
			b.ReportMetric(elapsed.Seconds(), "elapsed-s")
			b.ReportMetric(float64(failures), "push-failures")
			b.ReportMetric(float64(ambiguous), "ambiguous-final-tips")
			b.ReportMetric(benchMS(maxStarve), "max-starve-ms")
			b.ReportMetric(float64(packBytes), "pack-bytes")
			pushCSVDump(b, all)
		})
	}
}
