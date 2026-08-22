package githttp

// End-to-end tests for issue #29's durable admission reservations over real
// serve processes, hooks, and git clients, on both backend lanes: the exact
// check-then-act exploit (two pushes that individually fit, together
// exceed) now loses deterministically, and a reservation leaked by a
// crashed push denies admission only until the sweep-side pruning runs.

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"codestorage/internal/blob"
)

// measureBranchPack pushes work's HEAD to a throwaway branch of measureRepo
// through nodeA and returns the pack bytes it added. Pack bytes depend only
// on the pushed objects (content-addressed), so the measurement transfers
// to any other repo receiving the same objects.
func measureBranchPack(t *testing.T, measureRepo, work, branch, token string) int64 {
	t.Helper()
	ctx := context.Background()
	before, err := blob.PackUsageBytes(ctx, testBlob, measureRepo)
	if err != nil {
		t.Fatalf("PackUsageBytes before: %v", err)
	}
	mustGit(t, work, "push", remoteURL(nodeA, measureRepo, token), "HEAD:refs/heads/"+branch)
	after, err := blob.PackUsageBytes(ctx, testBlob, measureRepo)
	if err != nil {
		t.Fatalf("PackUsageBytes after: %v", err)
	}
	if after <= before {
		t.Fatalf("measurement push added no bytes (%d -> %d)", before, after)
	}
	return after - before
}

// twoBranchWorlds prepares two independent work repos with distinct content
// plus the target and measurement repos, and returns the measured pack
// sizes. The quota callers derive must admit either branch alone but not
// both.
func twoBranchWorlds(t *testing.T, name string) (repo, workB1, workB2, token string, s1, s2 int64) {
	t.Helper()
	repo = registerRepo(t, name)
	measure := registerRepo(t, name+"-measure")
	token = mintTestToken(t, repo, "git:read,git:write", "")
	measureTok := mintTestToken(t, measure, "git:read,git:write", "")
	workB1, workB2 = initWorkRepo(t), initWorkRepo(t)
	commitFile(t, workB1, "b1.txt", "branch-one payload\n", "b1", nil)
	commitFile(t, workB2, "b2.txt", "branch-two payload, distinct\n", "b2", nil)
	s1 = measureBranchPack(t, measure, workB1, "m1", measureTok)
	s2 = measureBranchPack(t, measure, workB2, "m2", measureTok)
	return repo, workB1, workB2, token, s1, s2
}

// eitherAloneQuota returns a quota admitting either measured pack alone but
// never both.
func eitherAloneQuota(t *testing.T, s1, s2 int64) int64 {
	t.Helper()
	quota := max(s1, s2) + min(s1, s2) - 1
	if s1+s2 <= quota || s1 > quota || s2 > quota {
		t.Fatalf("quota arithmetic broken: s1=%d s2=%d quota=%d", s1, s2, quota)
	}
	return quota
}

// TestPackQuota_ConcurrentOvershootOneLoses is the issue-#29 exploit run
// for real: two concurrent pushes that individually fit a fresh repo's
// quota but together exceed it. Under listing-only admission both could
// pass their checks before either uploads; the durable reservation
// serializes admission, so EXACTLY one push must win regardless of
// interleaving (a reservation-blocked loser, or a counter-blocked one
// after the winner registered).
func TestPackQuota_ConcurrentOvershootOneLoses(t *testing.T) {
	repo, workB1, workB2, token, s1, s2 := twoBranchWorlds(t, "quota-concurrent")
	node := startNodeT(t, fmt.Sprintf("CODESTORAGE_PACK_QUOTA_BYTES=%d", eitherAloneQuota(t, s1, s2)))

	// Warm the node's cache repo before racing: concurrent FIRST contacts
	// race the bare-init (a pre-existing hydration wart outside this
	// test's scope), and the race under test is the quota admission.
	mustGit(t, workB1, "ls-remote", remoteURL(node, repo, token))

	type result struct {
		out string
		err error
	}
	results := make([]result, 2)
	var wg sync.WaitGroup
	for i, w := range []struct{ work, branch string }{{workB1, "b1"}, {workB2, "b2"}} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := tryGit(w.work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/"+w.branch)
			results[i] = result{out, err}
		}()
	}
	wg.Wait()

	var wins, quotaDenials int
	for i, r := range results {
		switch {
		case r.err == nil:
			wins++
		case strings.Contains(r.out, "pack quota exceeded"):
			quotaDenials++
			t.Logf("push %d denied:\n%s", i, r.out)
		default:
			t.Fatalf("push %d failed without a quota denial:\n%s", i, r.out)
		}
	}
	if wins != 1 || quotaDenials != 1 {
		t.Fatalf("concurrent overshoot: %d wins + %d quota denials, want exactly 1 + 1", wins, quotaDenials)
	}

	// The loser's retry against the now-registered winner still loses:
	// the counter (not just a fresh listing) gates it.
	for i, w := range []struct{ work, branch string }{{workB1, "b1"}, {workB2, "b2"}} {
		if results[i].err != nil {
			out, err := tryGit(w.work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/"+w.branch)
			if err == nil || !strings.Contains(out, "pack quota exceeded") {
				t.Fatalf("loser retry outcome (err=%v):\n%s", err, out)
			}
		}
	}
}

// TestPackQuota_ReservationCrashSweepRecovery: a push that crashes after
// committing its reservation but before uploading anything leaves ONLY a
// durable reservation row (no blob, no registry row) — which must still
// deny a competing push that would otherwise fit (proof the admission state
// survived the hook process). Sweep-side pruning then reclaims the leak and
// the competing push succeeds.
func TestPackQuota_ReservationCrashSweepRecovery(t *testing.T) {
	ctx := context.Background()
	repo, workB1, workB2, token, s1, s2 := twoBranchWorlds(t, "quota-reserve-crash")
	node := startNodeT(t, fmt.Sprintf("CODESTORAGE_PACK_QUOTA_BYTES=%d", eitherAloneQuota(t, s1, s2)))

	// Crash push 1 right after its reservation commits.
	out, err := tryGit(workB1,
		"-c", "http.extraHeader="+CrashHeader+": after-reserve",
		"push", remoteURL(node, repo, token), "HEAD:refs/heads/b1")
	if err == nil {
		t.Fatalf("push should have failed under crash knob:\n%s", out)
	}
	if objs := minioPacks(t, repo); len(objs) != 0 {
		t.Fatalf("after-reserve crash uploaded packs: %v", objs)
	}
	if packs := pgPacks(t, repo); len(packs) != 0 {
		t.Fatalf("after-reserve crash registered packs: %v", packs)
	}

	// The leaked reservation alone (no bytes anywhere) denies push 2.
	out, err = tryGit(workB2, "push", remoteURL(node, repo, token), "HEAD:refs/heads/b2")
	if err == nil {
		t.Fatalf("push admitted despite leaked reservation:\n%s", out)
	}
	if !strings.Contains(out, "pack quota exceeded") || !strings.Contains(out, "reserved by in-flight pushes") {
		t.Fatalf("denial does not name the reservation:\n%s", out)
	}
	t.Logf("reservation-blocked push output:\n%s", out)

	// Sweep-side reclamation (what the periodic sweeper runs on its
	// cadence, issue #29): prune expired rows — olderThan=0 is the
	// deterministic test/recovery form — and the competing push passes.
	if n, err := testStore.SweepExpiredPackReservations(ctx, 0); err != nil || n < 1 {
		t.Fatalf("SweepExpiredPackReservations = %d, %v; want >= 1", n, err)
	}
	out = mustGit(t, workB2, "push", remoteURL(node, repo, token), "HEAD:refs/heads/b2")
	t.Logf("post-sweep push output:\n%s", out)
	if _, ok := pgRefs(t, repo)["refs/heads/b2"]; !ok {
		t.Fatal("post-sweep push did not publish refs/heads/b2")
	}
}

// TestPackQuota_CrashRetrySameKeyNotWedged: the crashed reservation's OWN
// retry must never be wedged by its leaked row — same-key re-reservation
// refreshes instead of conflicting (the wedge class issue #21's reviews
// eliminated, preserved by the ledger).
func TestPackQuota_CrashRetrySameKeyNotWedged(t *testing.T) {
	repo := registerRepo(t, "quota-reserve-retry")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	node := startNodeT(t, "CODESTORAGE_PACK_QUOTA_BYTES=1048576")

	out, err := tryGit(work,
		"-c", "http.extraHeader="+CrashHeader+": after-reserve",
		"push", remoteURL(node, repo, token), "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("push should have failed under crash knob:\n%s", out)
	}
	// Identical retry: same pack, same key, same bytes — must pass with
	// the leaked reservation still live (refresh, not double-count).
	out = mustGit(t, work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/main")
	t.Logf("same-key retry output:\n%s", out)
	head := revParse(t, work, "HEAD")
	if refs := pgRefs(t, repo); refs["refs/heads/main"] != head {
		t.Fatalf("refs/heads/main = %q, want %q", refs["refs/heads/main"], head)
	}
}
