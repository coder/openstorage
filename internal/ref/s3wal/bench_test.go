// Bench harness for the s3wal go/no-go gate (issue #133, plan §3): manifest
// CAS advance latency, simulated push, hot-repo contention (doubling as the
// sustained lost-update detector), and generation reads — against a
// SeaweedFS container, an optional real-S3 endpoint, and the existing
// SQLite RefStore as the zero-infra cost baseline.
//
// The real-S3 lane is gated on OPENSTORAGE_BENCH_S3_URL and skips cleanly
// when unset; SeaweedFS-local numbers are directional only and are NOT the
// Gate 1a-B verdict (the §3 kill thresholds are defined against real S3
// from same-region compute). Per-op CSVs land in OPENSTORAGE_BENCH_CSV_DIR
// when set — the results campaign requires them (plan §3 method).
//
// Benches follow the nightly conventions (bench-nightly.yml): heavy lanes
// skip under -short/no Docker; the contention window defaults to 5 s per N
// (OPENSTORAGE_BENCH_CONTENTION_SECS=60 for the plan §3 sustained runs).
package s3wal_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	"github.com/coder/open-storage/internal/envconfig"
	"github.com/coder/open-storage/internal/ref"
	"github.com/coder/open-storage/internal/ref/s3wal"
	"github.com/coder/open-storage/internal/ref/s3wal/s3waltest"
)

// assertf is the local fail-fast invariant check (the package-internal
// one is invisible to this external test package).
func assertf(cond bool, format string, args ...any) {
	if !cond {
		panic(fmt.Sprintf("s3wal bench assertion failed: "+format, args...))
	}
}

const (
	// EnvBenchS3URL names the real-S3 endpoint for the §3 Gate 1a-B lane
	// (same URL grammar as s3waltest.EnvTestS3WALURL). Unset skips — results are
	// never fabricated.
	EnvBenchS3URL = "OPENSTORAGE_BENCH_S3_URL"
	// EnvBenchCSVDir, when set, receives one raw per-op CSV per bench cell
	// (plan §3: raw CSVs published alongside the summary tables).
	EnvBenchCSVDir = "OPENSTORAGE_BENCH_CSV_DIR"
	// EnvBenchContentionSecs overrides the per-N contention window (default
	// 5 s to keep nightly runs bounded; the §3 campaign uses 60).
	EnvBenchContentionSecs = "OPENSTORAGE_BENCH_CONTENTION_SECS"

	// benchWarmupOps is the untimed warmup before recording (plan §3
	// method: ≥ 20 samples discarded per cell).
	benchWarmupOps = 20
)

// ---------------------------------------------------------------------------
// Latency recording: percentile summary + optional raw CSV dump.

type latencyRecorder struct{ samples []time.Duration }

func (r *latencyRecorder) record(d time.Duration) { r.samples = append(r.samples, d) }

// percentile returns the nearest-rank percentile of the recorded samples.
func (r *latencyRecorder) percentile(p float64) time.Duration {
	assertf(len(r.samples) > 0, "percentile: no samples")
	assertf(p > 0 && p <= 100, "percentile: p=%v out of range", p)
	sorted := slices.Clone(r.samples)
	slices.Sort(sorted)
	idx := int(float64(len(sorted))*p/100+0.999999) - 1
	idx = max(0, min(idx, len(sorted)-1))
	return sorted[idx]
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// report emits pNN-ms metrics plus the recorded sample count so the results
// tables can state how many samples back each cell.
func (r *latencyRecorder) report(b *testing.B, ps ...float64) {
	b.Helper()
	for _, p := range ps {
		b.ReportMetric(ms(r.percentile(p)), fmt.Sprintf("p%.0f-ms", p))
	}
	b.ReportMetric(float64(len(r.samples)), "samples")
}

// csvDump writes the raw per-op latencies for this bench cell when
// EnvBenchCSVDir is set.
func (r *latencyRecorder) csvDump(b *testing.B) {
	b.Helper()
	dir := envconfig.Getenv(EnvBenchCSVDir)
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		b.Fatalf("csv dir: %v", err)
	}
	name := strings.NewReplacer("/", "__", "=", "-").Replace(b.Name()) + ".csv"
	var sb strings.Builder
	sb.WriteString("op,latency_ns\n")
	for i, d := range r.samples {
		fmt.Fprintf(&sb, "%d,%d\n", i, d.Nanoseconds())
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(sb.String()), 0o644); err != nil {
		b.Fatalf("csv write: %v", err)
	}
	b.Logf("raw per-op CSV: %s (%d samples)", path, len(r.samples))
}

// ---------------------------------------------------------------------------
// Backend lanes.

// s3BenchLane is one S3-protocol bench backend. skip != "" marks an
// unavailable lane whose sub-runs skip with that reason (the real-S3 lane
// without credentials, containers under -short).
type s3BenchLane struct {
	name   string
	skip   string
	cas    *s3wal.Client
	raw    *s3.Client
	bucket string
	prefix string
}

// benchS3Lanes builds the SeaweedFS + real-S3 lanes. Containers and
// external cleanups are parent-scoped so every sub-run of one benchmark
// shares them.
func benchS3Lanes(b *testing.B) []*s3BenchLane {
	b.Helper()
	lanes := []*s3BenchLane{}
	if testing.Short() {
		lanes = append(lanes, &s3BenchLane{name: "seaweedfs", skip: "-short skips the SeaweedFS container lane"})
	} else {
		endpoint := s3waltest.StartSeaweedFS(b)
		raw := s3waltest.RawS3Client(endpoint, "us-east-1", "seaweedfs", "seaweedfs")
		s3waltest.MustEnsureBucket(b, raw, "s3wal-bench")
		cas := s3wal.New(raw, "s3wal-bench")
		cas.DecodeSeq = s3waltest.DecodeSeq
		lanes = append(lanes, &s3BenchLane{name: "seaweedfs", cas: cas, raw: raw, bucket: "s3wal-bench"})
	}
	if blobURL := envconfig.Getenv(EnvBenchS3URL); blobURL == "" {
		lanes = append(lanes, &s3BenchLane{
			name: "s3",
			skip: fmt.Sprintf("set %s (s3://bucket?region=…) plus AWS credentials to run the real-S3 gate lane — results are never fabricated", EnvBenchS3URL),
		})
	} else {
		raw, bucket := s3waltest.OpenExternalS3(b, blobURL)
		prefix := "bench/" + s3waltest.RandomHex(b, 8) + "/"
		b.Cleanup(func() { s3waltest.DeleteAllPrefix(b, raw, bucket, prefix) })
		cas := s3wal.New(raw, bucket)
		cas.DecodeSeq = s3waltest.DecodeSeq
		lanes = append(lanes, &s3BenchLane{name: "s3", cas: cas, raw: raw, bucket: bucket, prefix: prefix})
	}
	return lanes
}

// benchSQLite opens the existing SQLite RefStore baseline.
func benchSQLite(b *testing.B) *ref.SQLite {
	b.Helper()
	ctx := context.Background()
	st, err := ref.OpenSQLite(ctx, "sqlite://"+filepath.Join(b.TempDir(), "refs.sqlite"))
	if err != nil {
		b.Fatalf("OpenSQLite: %v", err)
	}
	b.Cleanup(st.Close)
	if err := st.InitSchema(ctx); err != nil {
		b.Fatalf("InitSchema: %v", err)
	}
	return st
}

// seqSHA encodes a counter as a 40-hex sha so the SQLite CAS analog carries
// the same strictly-monotonic seq the S3 payloads do.
func seqSHA(seq int64) string {
	assertf(seq > 0, "seqSHA: non-positive seq %d", seq)
	return fmt.Sprintf("%040x", seq)
}

func parseSeqSHA(tb testing.TB, sha string) int64 {
	tb.Helper()
	seq, err := strconv.ParseInt(sha, 16, 64)
	if err != nil {
		tb.Fatalf("parseSeqSHA(%q): %v", sha, err)
	}
	return seq
}

func contentionWindow(b *testing.B) time.Duration {
	b.Helper()
	v := envconfig.Getenv(EnvBenchContentionSecs)
	if v == "" {
		return 5 * time.Second
	}
	secs, err := strconv.Atoi(v)
	if err != nil || secs <= 0 {
		b.Fatalf("invalid %s=%q", EnvBenchContentionSecs, v)
	}
	return time.Duration(secs) * time.Second
}

// ---------------------------------------------------------------------------
// Scenario 1: manifest CAS advance latency (1 KB and 1 MB bodies).

func BenchmarkCASAdvance(b *testing.B) {
	ctx := context.Background()
	sizes := []struct {
		name  string
		bytes int
	}{{"1KB", 1 << 10}, {"1MB", 1 << 20}}
	for _, lane := range benchS3Lanes(b) {
		for _, size := range sizes {
			b.Run(fmt.Sprintf("store=%s/size=%s", lane.name, size.name), func(b *testing.B) {
				if lane.skip != "" {
					b.Skip(lane.skip)
				}
				key := lane.prefix + "_wal/repo/adv-" + s3waltest.RandomHex(b, 6)
				seq := int64(1)
				etag, err := lane.cas.Create(ctx, key, s3waltest.EncodePayload(b, seq, size.bytes))
				if err != nil {
					b.Fatalf("create: %v", err)
				}
				for range benchWarmupOps {
					seq++
					if etag, err = lane.cas.Advance(ctx, key, s3waltest.EncodePayload(b, seq, size.bytes), etag); err != nil {
						b.Fatalf("warmup advance: %v", err)
					}
				}
				rec := &latencyRecorder{}
				b.ReportAllocs()
				b.SetBytes(int64(size.bytes))
				for b.Loop() {
					seq++
					body := s3waltest.EncodePayload(b, seq, size.bytes)
					t0 := time.Now()
					etag, err = lane.cas.Advance(ctx, key, body, etag)
					rec.record(time.Since(t0))
					if err != nil {
						b.Fatalf("advance: %v", err)
					}
				}
				rec.report(b, 50, 95, 99)
				rec.csvDump(b)
			})
		}
	}
	b.Run("store=sqlite", func(b *testing.B) {
		st := benchSQLite(b)
		const repo, name = "benchrepo", "refs/heads/main"
		seq := int64(1)
		if won, err := st.CASRef(ctx, repo, name, ref.ZeroSHA, seqSHA(seq)); err != nil || !won {
			b.Fatalf("seed CASRef = (%v, %v), want won", won, err)
		}
		advance := func() {
			b.Helper()
			cur, err := st.GetRef(ctx, repo, name)
			if err != nil {
				b.Fatalf("GetRef: %v", err)
			}
			next := parseSeqSHA(b, cur) + 1
			won, err := st.CASRef(ctx, repo, name, cur, seqSHA(next))
			if err != nil || !won {
				b.Fatalf("CASRef = (%v, %v), want won", won, err)
			}
		}
		for range benchWarmupOps {
			advance()
		}
		rec := &latencyRecorder{}
		b.ReportAllocs()
		for b.Loop() {
			t0 := time.Now()
			advance()
			rec.record(time.Since(t0))
		}
		rec.report(b, 50, 95, 99)
		rec.csvDump(b)
	})
}

// ---------------------------------------------------------------------------
// Scenario 2: simulated push = manifest GET + 8 MB pack PUT + 2 CAS
// advances, single writer.

func BenchmarkSimulatedPush(b *testing.B) {
	ctx := context.Background()
	const packSize = 8 << 20
	pack := make([]byte, packSize)
	if _, err := rand.Read(pack); err != nil {
		b.Fatal(err)
	}
	for _, lane := range benchS3Lanes(b) {
		b.Run("store="+lane.name, func(b *testing.B) {
			if lane.skip != "" {
				b.Skip(lane.skip)
			}
			id := s3waltest.RandomHex(b, 6)
			manifest := lane.prefix + "_wal/repo/push-" + id
			seq := int64(1)
			if _, err := lane.cas.Create(ctx, manifest, s3waltest.EncodePayload(b, seq, 1<<10)); err != nil {
				b.Fatalf("create manifest: %v", err)
			}
			i := 0
			push := func() time.Duration {
				b.Helper()
				i++
				packKey := fmt.Sprintf("%spacks/push-%s/%06d.pack", lane.prefix, id, i)
				t0 := time.Now()
				obj, err := lane.cas.Get(ctx, manifest)
				if err != nil {
					b.Fatalf("get manifest: %v", err)
				}
				if _, err := lane.raw.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(lane.bucket), Key: aws.String(packKey), Body: bytes.NewReader(pack),
				}); err != nil {
					b.Fatalf("put pack: %v", err)
				}
				seq++
				etag, err := lane.cas.Advance(ctx, manifest, s3waltest.EncodePayload(b, seq, 1<<10), obj.ETag)
				if err != nil {
					b.Fatalf("advance 1: %v", err)
				}
				seq++
				if _, err := lane.cas.Advance(ctx, manifest, s3waltest.EncodePayload(b, seq, 1<<10), etag); err != nil {
					b.Fatalf("advance 2: %v", err)
				}
				d := time.Since(t0)
				// Untimed: bound storage growth across iterations.
				if err := lane.cas.Delete(ctx, packKey); err != nil {
					b.Fatalf("delete pack: %v", err)
				}
				return d
			}
			for range benchWarmupOps {
				push()
			}
			rec := &latencyRecorder{}
			b.ReportAllocs()
			b.SetBytes(packSize)
			for b.Loop() {
				rec.record(push())
			}
			rec.report(b, 50, 99)
			rec.csvDump(b)
		})
	}
	// SQLite push analog: GetRef + an 8 MiB fsynced local blob write
	// (mirroring local mode's pack path) + 2 CASRefs. The durability
	// asymmetry (local disk vs S3 PUT) is documented in the results table.
	b.Run("store=sqlite", func(b *testing.B) {
		st := benchSQLite(b)
		dir := b.TempDir()
		const repo, name = "benchrepo", "refs/heads/main"
		seq := int64(1)
		if won, err := st.CASRef(ctx, repo, name, ref.ZeroSHA, seqSHA(seq)); err != nil || !won {
			b.Fatalf("seed CASRef = (%v, %v), want won", won, err)
		}
		i := 0
		push := func() time.Duration {
			b.Helper()
			i++
			path := filepath.Join(dir, fmt.Sprintf("%06d.pack", i))
			t0 := time.Now()
			cur, err := st.GetRef(ctx, repo, name)
			if err != nil {
				b.Fatalf("GetRef: %v", err)
			}
			f, err := os.Create(path)
			if err != nil {
				b.Fatalf("create pack: %v", err)
			}
			if _, err := f.Write(pack); err != nil {
				b.Fatalf("write pack: %v", err)
			}
			if err := f.Sync(); err != nil {
				b.Fatalf("sync pack: %v", err)
			}
			if err := f.Close(); err != nil {
				b.Fatalf("close pack: %v", err)
			}
			for range 2 {
				next := parseSeqSHA(b, cur) + 1
				won, err := st.CASRef(ctx, repo, name, cur, seqSHA(next))
				if err != nil || !won {
					b.Fatalf("CASRef = (%v, %v), want won", won, err)
				}
				cur = seqSHA(next)
			}
			d := time.Since(t0)
			if err := os.Remove(path); err != nil {
				b.Fatalf("remove pack: %v", err)
			}
			return d
		}
		for range benchWarmupOps {
			push()
		}
		rec := &latencyRecorder{}
		b.ReportAllocs()
		b.SetBytes(packSize)
		for b.Loop() {
			rec.record(push())
		}
		rec.report(b, 50, 99)
		rec.csvDump(b)
	})
}

// ---------------------------------------------------------------------------
// Scenario 3: hot-repo contention — N concurrent pushers on ONE object.
// Doubles as the §3 sustained lost-update detector (runContentionWindow
// enforces exactly-once winners and no lost writes on every run).

func BenchmarkHotRepoContention(b *testing.B) {
	writersMatrix := []int{2, 8, 32}
	window := contentionWindow(b)
	for _, lane := range benchS3Lanes(b) {
		for _, writers := range writersMatrix {
			b.Run(fmt.Sprintf("store=%s/writers=%d", lane.name, writers), func(b *testing.B) {
				if lane.skip != "" {
					b.Skip(lane.skip)
				}
				rec := &latencyRecorder{}
				var wins, lostRaces, contentionErrs int64
				var maxStarve time.Duration
				var elapsed time.Duration
				for b.Loop() {
					key := lane.prefix + "_wal/repo/hot-" + s3waltest.RandomHex(b, 6)
					res := runContentionWindow(b, lane.cas, key, writers, window, 1<<10)
					rec.samples = append(rec.samples, res.latencies...)
					wins += int64(res.wins)
					lostRaces += res.lostRaces
					contentionErrs += res.contentionErrs
					maxStarve = max(maxStarve, res.maxStarve)
					// Throughput divides by the runner's ACTUAL elapsed time
					// (window start → last worker completion), not the
					// configured window: an advance started just before the
					// deadline can retry for seconds and its late success
					// still counts in wins, which would overstate sustained
					// adv/s against the configured duration.
					elapsed += res.elapsed
				}
				rec.report(b, 50, 99)
				assertf(elapsed > 0, "contention: zero elapsed time")
				b.ReportMetric(float64(wins)/elapsed.Seconds(), "adv/s")
				b.ReportMetric(float64(wins), "wins")
				b.ReportMetric(elapsed.Seconds(), "elapsed-s")
				b.ReportMetric(float64(lostRaces), "lost-races")
				b.ReportMetric(float64(contentionErrs), "contention-errors")
				b.ReportMetric(ms(maxStarve), "max-starve-ms")
				rec.csvDump(b)
			})
		}
	}
	for _, writers := range writersMatrix {
		b.Run(fmt.Sprintf("store=sqlite/writers=%d", writers), func(b *testing.B) {
			rec := &latencyRecorder{}
			var wins, lostRaces, busyRetries int64
			var maxStarve time.Duration
			var elapsed time.Duration
			for b.Loop() {
				st := benchSQLite(b)
				res := runSQLiteContentionWindow(b, st, "benchrepo", "refs/heads/hot-"+s3waltest.RandomHex(b, 4), writers, window)
				rec.samples = append(rec.samples, res.latencies...)
				wins += int64(res.wins)
				lostRaces += res.lostRaces
				busyRetries += res.busyRetries
				maxStarve = max(maxStarve, res.maxStarve)
				// Actual elapsed, not the configured window — see the S3
				// loop above (a near-deadline CAS can win late).
				elapsed += res.elapsed
			}
			rec.report(b, 50, 99)
			assertf(elapsed > 0, "contention: zero elapsed time")
			b.ReportMetric(float64(wins)/elapsed.Seconds(), "adv/s")
			b.ReportMetric(float64(wins), "wins")
			b.ReportMetric(elapsed.Seconds(), "elapsed-s")
			b.ReportMetric(float64(lostRaces), "lost-races")
			b.ReportMetric(float64(busyRetries), "busy-retries")
			b.ReportMetric(ms(maxStarve), "max-starve-ms")
			rec.csvDump(b)
		})
	}
}

// warmupSQLiteContention is the SQLite twin of warmupContention: `writers`
// goroutines race a bounded number of GetRef+CASRef rounds on a separate
// warmup ref (collectively ≥ benchWarmupOps operations) so page-cache,
// WAL, and statement first-touch costs land before the measured timer.
// No accounting of any kind; busy rejections are simply retried.
func warmupSQLiteContention(tb testing.TB, st *ref.SQLite, repo, name string, writers int) {
	tb.Helper()
	assertf(writers > 0, "warmupSQLiteContention: %d writers", writers)
	ctx := context.Background()
	if won, err := st.CASRef(ctx, repo, name, ref.ZeroSHA, seqSHA(1)); err != nil || !won {
		tb.Fatalf("warmup seed CASRef = (%v, %v), want won", won, err)
	}
	perWorker := (benchWarmupOps + writers - 1) / writers
	var wg sync.WaitGroup
	for range writers {
		wg.Go(func() {
			cur := seqSHA(1)
			for range perWorker {
				next := parseSeqSHA(tb, cur) + 1
				won, err := st.CASRef(ctx, repo, name, cur, seqSHA(next))
				switch {
				case err != nil && isSQLiteBusy(err):
					continue
				case err != nil:
					tb.Errorf("warmup CASRef: %v", err)
					return
				case won:
					cur = seqSHA(next)
					continue
				}
				reread, err := st.GetRef(ctx, repo, name)
				switch {
				case err == nil:
					cur = reread
				case !isSQLiteBusy(err):
					tb.Errorf("warmup re-read: %v", err)
					return
				}
			}
		})
	}
	wg.Wait()
}

// isSQLiteBusy reports whether err is a definitive SQLITE_BUSY rejection
// (the busy_timeout expired while another writer held the file lock). The
// backend responded, so the statement provably did not commit (see
// internal/ref's ambiguousError taxonomy) — the contention runner may
// safely retry without corrupting the lost-update detector's accounting.
func isSQLiteBusy(err error) bool {
	var se *sqlite.Error
	return errors.As(err, &se) && se.Code() == sqlite3.SQLITE_BUSY
}

// runSQLiteContentionWindow is the SQLite twin of runContentionWindow: N
// goroutines race GetRef+CASRef on one ref whose sha encodes the seq, with
// the same lost-update detector (won=false is the lost race; SQLite has no
// ambiguous class on this path — errors fail the run, except SQLITE_BUSY,
// which is a definitive not-committed rejection and is retried/counted).
// Like the S3 runner, the measured window is preceded by an untimed warmup
// on a separate scratch ref (page cache, WAL, statement paths) with no
// accounting; the measured seed and detector baseline start fresh after.
func runSQLiteContentionWindow(tb testing.TB, st *ref.SQLite, repo, name string, writers int, window time.Duration) contentionResult {
	tb.Helper()
	assertf(writers > 0, "runSQLiteContentionWindow: %d writers", writers)
	warmupSQLiteContention(tb, st, repo, name+"-warmup", writers)
	ctx := context.Background()
	const initialSeq = int64(1)
	if won, err := st.CASRef(ctx, repo, name, ref.ZeroSHA, seqSHA(initialSeq)); err != nil || !won {
		tb.Fatalf("seed CASRef = (%v, %v), want won", won, err)
	}
	var (
		mu          sync.Mutex
		transitions []seqTransition
		res         contentionResult
	)
	start := time.Now()
	deadline := start.Add(window)
	var wg sync.WaitGroup
	for range writers {
		wg.Go(func() {
			var (
				localLat    []time.Duration
				localTrans  []seqTransition
				lostRaces   int64
				busyRetries int64
				maxStarve   time.Duration
			)
			lastSuccess := start
			// Merge in a defer so a writer that errors out mid-window
			// still contributes its recorded wins: discarding them would
			// desync the detector's final-seq accounting and report a
			// phantom update that never happened.
			defer func() {
				maxStarve = max(maxStarve, time.Since(lastSuccess))
				mu.Lock()
				res.latencies = append(res.latencies, localLat...)
				transitions = append(transitions, localTrans...)
				res.lostRaces += lostRaces
				res.busyRetries += busyRetries
				res.maxStarve = max(res.maxStarve, maxStarve)
				mu.Unlock()
			}()
			cur, err := st.GetRef(ctx, repo, name)
			if err != nil {
				tb.Errorf("contention: GetRef: %v", err)
				return
			}
			for time.Now().Before(deadline) {
				next := parseSeqSHA(tb, cur) + 1
				won, err := st.CASRef(ctx, repo, name, cur, seqSHA(next))
				switch {
				case err != nil && isSQLiteBusy(err):
					// The 5 s busy_timeout expired while another writer
					// held the lock — expected under sustained hot-key
					// hammering. A *sqlite.Error is a definitive backend
					// rejection (the CAS did NOT commit; see internal/ref
					// ambiguousError), so retrying the same attempt is
					// accounting-safe for the lost-update detector.
					busyRetries++
					continue
				case err != nil:
					tb.Errorf("contention: CASRef: %v", err)
					return
				case won:
					now := time.Now()
					// Per-push success latency: previous win (or worker
					// start) → this win, spanning intervening lost races,
					// busy retries, and re-reads (see contentionResult).
					localLat = append(localLat, now.Sub(lastSuccess))
					localTrans = append(localTrans, seqTransition{to: next})
					maxStarve = max(maxStarve, now.Sub(lastSuccess))
					lastSuccess = now
					cur = seqSHA(next)
					continue
				}
				lostRaces++
				reread, err := st.GetRef(ctx, repo, name)
				if err != nil {
					if !isSQLiteBusy(err) {
						tb.Errorf("contention: re-read: %v", err)
						return
					}
					// Busy re-read: retry the CAS with the stale sha —
					// it just loses the race again and re-reads (may
					// slightly inflate lost-races under busy storms).
					busyRetries++
				} else {
					cur = reread
				}
			}
		})
	}
	wg.Wait()
	res.elapsed = time.Since(start)
	res.wins = len(transitions)
	final, err := st.GetRef(ctx, repo, name)
	if err != nil {
		tb.Fatalf("contention: final GetRef: %v", err)
	}
	verifyTransitions(tb, initialSeq, parseSeqSHA(tb, final), "", transitions, 0)
	return res
}

// ---------------------------------------------------------------------------
// Scenario 4: generation-read latency — unconditional GET vs the
// If-None-Match 304 path (the hydration-check hot read).

func BenchmarkGenerationRead(b *testing.B) {
	ctx := context.Background()
	for _, lane := range benchS3Lanes(b) {
		for _, mode := range []string{"get", "304"} {
			b.Run(fmt.Sprintf("store=%s/mode=%s", lane.name, mode), func(b *testing.B) {
				if lane.skip != "" {
					b.Skip(lane.skip)
				}
				key := lane.prefix + "_wal/repo/gen-" + s3waltest.RandomHex(b, 6)
				etag, err := lane.cas.Create(ctx, key, s3waltest.EncodePayload(b, 1, 1<<10))
				if err != nil {
					b.Fatalf("create: %v", err)
				}
				read := func() {
					b.Helper()
					if mode == "get" {
						if _, err := lane.cas.Get(ctx, key); err != nil {
							b.Fatalf("get: %v", err)
						}
						return
					}
					if _, err := lane.cas.GetIfChanged(ctx, key, etag); !errors.Is(err, s3wal.ErrNotModified) {
						b.Fatalf("GetIfChanged = %v, want s3wal.ErrNotModified", err)
					}
				}
				for range benchWarmupOps {
					read()
				}
				rec := &latencyRecorder{}
				b.ReportAllocs()
				for b.Loop() {
					t0 := time.Now()
					read()
					rec.record(time.Since(t0))
				}
				rec.report(b, 50, 99)
				rec.csvDump(b)
			})
		}
	}
	// SQLite generation read ≙ GetRef (no 304 analog: the read IS the
	// local database lookup).
	b.Run("store=sqlite/mode=get", func(b *testing.B) {
		st := benchSQLite(b)
		const repo, name = "benchrepo", "refs/heads/main"
		if won, err := st.CASRef(ctx, repo, name, ref.ZeroSHA, seqSHA(1)); err != nil || !won {
			b.Fatalf("seed CASRef = (%v, %v), want won", won, err)
		}
		read := func() {
			b.Helper()
			if _, err := st.GetRef(ctx, repo, name); err != nil {
				b.Fatalf("GetRef: %v", err)
			}
		}
		for range benchWarmupOps {
			read()
		}
		rec := &latencyRecorder{}
		b.ReportAllocs()
		for b.Loop() {
			t0 := time.Now()
			read()
			rec.record(time.Since(t0))
		}
		rec.report(b, 50, 99)
		rec.csvDump(b)
	})
}
