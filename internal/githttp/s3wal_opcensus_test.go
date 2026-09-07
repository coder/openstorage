// Per-push S3 round-trip census + regression guard for the s3wal
// zero-infra lane (issue #182): the op-log seam (internal/s3oplog,
// CODESTORAGE_S3_OP_LOG) records one line per S3 HTTP attempt across the
// serve process AND the hook subprocesses, so this drill can count — and
// bound — the sequential round trips one `git push` costs. On real S3
// every op is a WAN round trip, so the count is the latency model:
// regressions here are user-visible push latency.
package githttp

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// opLogLine is one parsed s3oplog line:
// <unixnano> <pid> <role> <operation> <status> <dur_us> <path>.
type opLogLine struct {
	role, op, key string
	status        int
	durUS         int64
}

func parseOpLog(t *testing.T, raw string) []opLogLine {
	t.Helper()
	var out []opLogLine
	for line := range strings.SplitSeq(raw, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		f := strings.Fields(line)
		if len(f) != 7 {
			t.Fatalf("malformed op-log line (want 7 fields): %q", line)
		}
		var l opLogLine
		l.role, l.op, l.key = f[2], f[3], f[6]
		if _, err := fmt.Sscanf(f[4], "%d", &l.status); err != nil {
			t.Fatalf("malformed status in op-log line %q: %v", line, err)
		}
		if _, err := fmt.Sscanf(f[5], "%d", &l.durUS); err != nil {
			t.Fatalf("malformed duration in op-log line %q: %v", line, err)
		}
		out = append(out, l)
	}
	return out
}

// opLogWindow reads the op-log lines appended since *offset and advances
// the offset — one push's window when called around a single git command.
func opLogWindow(t *testing.T, path string, offset *int64) []opLogLine {
	t.Helper()
	// Hook subprocesses have exited by the time `git push` returns (git
	// waits for them) and the census node disables the async repack
	// probe (see the startNodeT env), so every op of a push has been
	// logged by the time the push command exits; a short settle covers
	// OS buffering only.
	time.Sleep(200 * time.Millisecond)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read op log: %v", err)
	}
	if int64(len(raw)) < *offset {
		t.Fatalf("op log shrank: len %d < offset %d", len(raw), *offset)
	}
	window := raw[*offset:]
	*offset = int64(len(raw))
	return parseOpLog(t, string(window))
}

// censusTable renders one push window as a readable table for the census.
func censusTable(lines []opLogLine) string {
	var b strings.Builder
	for i, l := range lines {
		role := strings.TrimPrefix(l.role, "openstorage:")
		fmt.Fprintf(&b, "%2d. %-22s %-16s %3d %6dus %s\n", i+1, role, l.op, l.status, l.durUS, l.key)
	}
	return b.String()
}

// TestS3WAL_PushOpCensus counts every S3 round trip per push shape and
// guards the steady-state budget. The bounds are deliberately EXACT
// ceilings for the current implementation: an op sneaking onto the push
// hot path must show up here as a diff to review, not vanish into slack.
func TestS3WAL_PushOpCensus(t *testing.T) {
	skipUnlessS3WAL(t)

	// The repack threshold probe is disabled for determinism: it runs
	// asynchronously since issue #182 (dispatchRepackProbe), so its
	// single manifest GET could land in either push's log window and
	// make exact budgets flaky. Its cost is documented in the issue #182
	// census; the budgets here guard the synchronous push path.
	//
	// The periodic sweep is disabled for the same reason (issue #209):
	// the local-mode ticker runs its first pass immediately at boot on a
	// background goroutine (StartSweepTicker's run-on-start), and that
	// pass scans the WHOLE shared lane bucket — a fleet walk reading
	// every other test's `_wal/repo/<name>` manifest plus `lfs/` prefix
	// listings — so its ops race into whichever census window they land
	// in. Interval 0 swaps the schedule for a strictly synchronous
	// startup Heal that finishes before the node announces its address,
	// i.e. before the offset snapshot below. The remaining background
	// scanners are already off by default: LFS reclamation and absence
	// reconciliation ship interval 0, and the reconcile ticker needs the
	// non-default v0.2.0 write-compat flag (or a partial Heal) to start.
	opLog := filepath.Join(t.TempDir(), "s3ops.log")
	node := startNodeT(t, "CODESTORAGE_S3_OP_LOG="+opLog, "CODESTORAGE_REPACK_THRESHOLD=0",
		"CODESTORAGE_SWEEP_INTERVAL=0")

	repo := registerRepo(t, "t-s3wal-op-census")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	url := remoteURL(node, repo, token)

	var offset int64
	if fi, err := os.Stat(opLog); err == nil {
		offset = fi.Size() // skip boot-time ops (InitSchema probe, …)
	}

	// Shape 1: genesis push — first pack into an empty repo.
	mustGit(t, work, "push", url, "HEAD:refs/heads/main")
	genesis := opLogWindow(t, opLog, &offset)
	t.Logf("genesis push (%d S3 round trips):\n%s", len(genesis), censusTable(genesis))

	// Shape 2: steady-state small-pack push (an empty commit ships a
	// ~200-byte pack) — the shape the issue #182 latency numbers measure.
	mustGit(t, work, "commit", "--allow-empty", "-m", "steady")
	mustGit(t, work, "push", url, "HEAD:refs/heads/main")
	steady := opLogWindow(t, opLog, &offset)
	t.Logf("steady-state small-pack push (%d S3 round trips):\n%s", len(steady), censusTable(steady))

	// Shape 3: delete-only push — git invokes pre-receive without a
	// quarantine, so the pack pipeline (upload, quota, registration) is
	// skipped entirely. The branch creation beforehand is not measured.
	mustGit(t, work, "push", url, "HEAD:refs/heads/todelete")
	_ = opLogWindow(t, opLog, &offset)
	mustGit(t, work, "push", url, ":refs/heads/todelete")
	deleteOnly := opLogWindow(t, opLog, &offset)
	t.Logf("delete-only push (%d S3 round trips):\n%s", len(deleteOnly), censusTable(deleteOnly))

	// Regression guard (issue #182): the per-shape budgets. Raising a
	// bound requires justifying the new round trip against the census in
	// that issue — on real S3 each one is user-visible push latency.
	// 19 = ReservePackBytes reads fresh by design (s3walMutation.noSeed:
	// its lapse-evidence write must never derive from a seed).
	const steadyBudget = 19
	if len(steady) > steadyBudget {
		t.Errorf("steady-state push cost %d S3 round trips, budget is %d:\n%s",
			len(steady), steadyBudget, censusTable(steady))
	}
	const deleteBudget = 9
	if len(deleteOnly) > deleteBudget {
		t.Errorf("delete-only push cost %d S3 round trips, budget is %d:\n%s",
			len(deleteOnly), deleteBudget, censusTable(deleteOnly))
	}
}
