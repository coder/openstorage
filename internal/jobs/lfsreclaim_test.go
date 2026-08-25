package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/code-storage/internal/blob"
	"github.com/coder/code-storage/internal/lfsscan"
	"github.com/coder/code-storage/internal/ref"
)

// lfsReclaimGitEnv pins identity and neuters host config so fixture commits
// are hermetic (host GIT_* vars and global hooks otherwise leak in).
var lfsReclaimGitEnv = []string{
	"GIT_AUTHOR_NAME=Alice Author",
	"GIT_AUTHOR_EMAIL=alice@example.com",
	"GIT_COMMITTER_NAME=Bob Committer",
	"GIT_COMMITTER_EMAIL=bob@example.com",
	"GIT_CONFIG_GLOBAL=/dev/null",
	"GIT_CONFIG_SYSTEM=/dev/null",
}

func reclaimGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), lfsReclaimGitEnv...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

func lfsPointerFile(oid string) string {
	return fmt.Sprintf("version https://git-lfs.github.com/spec/v1\noid sha256:%s\nsize 42\n", oid)
}

// reclaimFixture builds one git worktree whose main branch references the
// given OIDs from pointer files, serving as the injected "hydrated cache".
func reclaimFixture(t *testing.T, oids ...string) string {
	t.Helper()
	dir := t.TempDir()
	reclaimGit(t, dir, "init", "-q", "--initial-branch=main")
	for i, oid := range oids {
		name := fmt.Sprintf("asset-%d.bin", i)
		if err := os.WriteFile(filepath.Join(dir, name), []byte(lfsPointerFile(oid)), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("# fixture\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	reclaimGit(t, dir, "add", "-A")
	reclaimGit(t, dir, "commit", "-q", "-m", "fixture")
	return dir
}

func newReclaimStore(t *testing.T) (ref.Store, LFSReclaimRegistry) {
	t.Helper()
	ctx := context.Background()
	store, err := ref.Open(ctx, "sqlite://"+filepath.Join(t.TempDir(), "refs.db"))
	if err != nil {
		t.Fatalf("ref.Open: %v", err)
	}
	t.Cleanup(store.Close)
	if err := store.InitSchema(ctx); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	reg, ok := store.(LFSReclaimRegistry)
	if !ok {
		t.Fatalf("store %T lacks the reclamation seams", store)
	}
	// Retirement gates on a fresh destructive-reap watermark (issue
	// #120); seed one so scan tests exercise their own guards. The
	// watermark-staleness tests delete it again explicitly.
	seedReapWatermark(t, reg)
	return store, reg
}

func seedReapWatermark(t *testing.T, reg LFSReclaimRegistry) {
	t.Helper()
	// Legacy-armed payload: the scan fixtures model a fully-armed fleet
	// (testReclaimer passes legacyDelete=true) so rows of BOTH key
	// spellings retire; the legacy-unarmed regime has its own test.
	// A real begin/complete pair: retirements pin the epoch row, and a
	// completion fabricated without its begin would spuriously cover
	// them (Codex round 33).
	simulateCoveringSweep(t, reg, true)
}

// simulateCoveringSweep advances the destructive-sweep epoch and records a
// clean completion at it — the covering pair that lets SettleLFSRefunds
// release buckets parked under earlier epochs (Codex round 33).
func simulateCoveringSweep(t *testing.T, reg LFSReclaimRegistry, legacy bool) {
	t.Helper()
	epoch, err := reg.BeginLFSReapEpoch(context.Background())
	if err != nil {
		t.Fatalf("begin reap epoch: %v", err)
	}
	if err := reg.RecordLFSReapComplete(context.Background(), epoch, legacy); err != nil {
		t.Fatalf("record reap completion: %v", err)
	}
}

// staticHydrate returns each repo's pre-built fixture path; repos without a
// fixture fail the test (the reclaimer must not hydrate ref-less members).
func staticHydrate(t *testing.T, fixtures map[string]string) HydrateFunc {
	t.Helper()
	return func(_ context.Context, repo, defaultBranch string) (string, error) {
		t.Helper()
		if defaultBranch != "main" {
			t.Errorf("hydrate %s: default branch %q, want main", repo, defaultBranch)
		}
		path, ok := fixtures[repo]
		if !ok {
			t.Errorf("unexpected hydration of %s", repo)
			return "", fmt.Errorf("unexpected hydration of %s", repo)
		}
		return path, nil
	}
}

func alwaysMain(context.Context, string) (string, bool, error) { return "main", true, nil }

func testReclaimer(t *testing.T, reg LFSReclaimRegistry, hydrate HydrateFunc, grace time.Duration, budget int64, deleteEnabled bool) *LFSReclaimer {
	t.Helper()
	return NewLFSReclaimer(reg, hydrate, lfsscan.ReachableLFSOIDs, alwaysMain,
		grace, budget, deleteEnabled, true, slog.New(slog.DiscardHandler))
}

// TestLFSReclaim_EndToEnd: the full mark → grace → retire lifecycle over a
// real SQLite ledger and a real git reachability walk. A referenced object
// survives every pass; junk rows (both key spellings) are marked on the
// first pass and retired once the grace window elapses; the object count
// and usage bytes converge (the 507 unwedge).
func TestLFSReclaim_EndToEnd(t *testing.T) {
	ctx := context.Background()
	store, reg := newReclaimStore(t)
	base := "e2e-repo"
	refOID := strings.Repeat("aa", 32)
	junkOID := strings.Repeat("bb", 32)
	legacyOID := strings.Repeat("cc", 32)

	keep := "lfs/" + base + "/o2/" + refOID
	junk := "lfs/" + base + "/o2/" + junkOID
	legacy := "lfs/" + base + "/" + legacyOID // v0.2.0-era row spelling
	for _, key := range []string{keep, junk, legacy} {
		if err := store.RegisterLFSObject(ctx, base, key, 100); err != nil {
			t.Fatal(err)
		}
	}
	// The repo's refs live in the DB authority; the fixture is its cache.
	if won, err := store.CASRef(ctx, base, "refs/heads/main", ref.ZeroSHA, strings.Repeat("11", 20)); err != nil || !won {
		t.Fatalf("CASRef = (%v, %v)", won, err)
	}
	fixtures := map[string]string{base: reclaimFixture(t, refOID)}

	grace := 300 * time.Millisecond
	r := testReclaimer(t, reg, staticHydrate(t, fixtures), grace, 1_000, true)

	// Pass 1: junk rows get marked, nothing retires (grace running).
	rep, err := r.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rep.ReposScanned != 1 || rep.RowsMarked != 2 || rep.RowsReferenced != 1 || rep.RowsRetired != 0 {
		t.Fatalf("pass 1 = %+v, want 2 marked / 1 referenced / 0 retired", rep)
	}
	if n, _ := store.CountLFSObjects(ctx, base); n != 3 {
		t.Fatalf("count after pass 1 = %d, want 3", n)
	}

	// Pass 2 after the grace window: junk rows retire, the referenced row
	// survives, the count converges.
	// SQLite stamps marks at whole-second resolution and the guard demands
	// strictly more than the (rounded-up) grace: two boundary crossings.
	time.Sleep(grace + 2100*time.Millisecond)
	rep, err = r.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rep.RowsRetired != 2 || rep.BytesRetired != 200 {
		t.Fatalf("pass 2 = %+v, want 2 retired / 200 bytes", rep)
	}
	if has, _ := store.HasLFSObject(ctx, base, keep); !has {
		t.Fatal("referenced row retired")
	}
	for _, key := range []string{junk, legacy} {
		if has, _ := store.HasLFSObject(ctx, base, key); has {
			t.Fatalf("junk row %s survived", key)
		}
	}
	// Refunds are DEFERRED past retirement (Codex round 33): the count
	// converges only after a destructive sweep that STARTED after the
	// retirement completes and the next pass settles the buckets.
	if n, _ := store.CountLFSObjects(ctx, base); n != 3 {
		t.Fatalf("count after pass 2 = %d, want still 3 (deferred refunds)", n)
	}
	// Pass 2's settle attempt ran at visit start, BEFORE its own
	// retirements parked buckets: nothing settled.
	if rep.RefundRowsSettled != 0 {
		t.Fatalf("pass 2 refunds = %+v, want none settled", rep)
	}
	simulateCoveringSweep(t, reg, true)
	// Pass 3 settles the covered buckets at the repo visit; the
	// referenced row keeps the repo queued; retirement stays steady.
	rep, err = r.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rep.RowsRetired != 0 || rep.RowsReferenced != 1 {
		t.Fatalf("pass 3 = %+v, want steady state", rep)
	}
	if rep.RefundRowsSettled != 2 || rep.RefundBytesSettled != 200 || rep.RefundBucketsPending != 0 {
		t.Fatalf("pass 3 refunds = %+v, want both rows settled", rep)
	}
	if n, _ := store.CountLFSObjects(ctx, base); n != 1 {
		t.Fatalf("count after settlement = %d, want 1", n)
	}
}

// TestLFSReclaim_DryRunReportsOnly: with deletes disabled the scan marks and
// reports eligible rows but retires nothing — the staged-rollout mode.
func TestLFSReclaim_DryRunReportsOnly(t *testing.T) {
	ctx := context.Background()
	store, reg := newReclaimStore(t)
	base := "dryrun-repo"
	junk := "lfs/" + base + "/o2/" + strings.Repeat("bb", 32)
	if err := store.RegisterLFSObject(ctx, base, junk, 10); err != nil {
		t.Fatal(err)
	}
	if won, err := store.CASRef(ctx, base, "refs/heads/main", ref.ZeroSHA, strings.Repeat("22", 20)); err != nil || !won {
		t.Fatalf("CASRef = (%v, %v)", won, err)
	}
	fixtures := map[string]string{base: reclaimFixture(t)}
	r := testReclaimer(t, reg, staticHydrate(t, fixtures), 300*time.Millisecond, 1_000, false)

	if rep, err := r.Run(ctx); err != nil || rep.RowsMarked != 1 || !rep.DryRun {
		t.Fatalf("dry pass 1 = (%+v, %v), want 1 marked", rep, err)
	}
	time.Sleep(1500 * time.Millisecond)
	rep, err := r.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rep.RowsEligible != 1 || rep.RowsRetired != 0 {
		t.Fatalf("dry pass 2 = %+v, want 1 eligible / 0 retired", rep)
	}
	if has, _ := store.HasLFSObject(ctx, base, junk); !has {
		t.Fatal("dry run deleted a row")
	}
}

// TestLFSReclaim_ReReferenceClearsMark: a pointer pushed between passes
// re-references a marked row — the next scan clears the mark instead of
// letting it age toward retirement.
func TestLFSReclaim_ReReferenceClearsMark(t *testing.T) {
	ctx := context.Background()
	store, reg := newReclaimStore(t)
	base := "rereference-repo"
	oid := strings.Repeat("dd", 32)
	key := "lfs/" + base + "/o2/" + oid
	if err := store.RegisterLFSObject(ctx, base, key, 10); err != nil {
		t.Fatal(err)
	}
	if won, err := store.CASRef(ctx, base, "refs/heads/main", ref.ZeroSHA, strings.Repeat("33", 20)); err != nil || !won {
		t.Fatalf("CASRef = (%v, %v)", won, err)
	}
	fixture := reclaimFixture(t) // no pointers yet
	fixtures := map[string]string{base: fixture}
	grace := 300 * time.Millisecond
	r := testReclaimer(t, reg, staticHydrate(t, fixtures), grace, 1_000, true)

	if rep, err := r.Run(ctx); err != nil || rep.RowsMarked != 1 {
		t.Fatalf("pass 1 = (%+v, %v), want 1 marked", rep, err)
	}
	// A push lands a pointer referencing the marked OID.
	if err := os.WriteFile(filepath.Join(fixture, "late.bin"), []byte(lfsPointerFile(oid)), 0o644); err != nil {
		t.Fatal(err)
	}
	reclaimGit(t, fixture, "add", "late.bin")
	reclaimGit(t, fixture, "commit", "-q", "-m", "late pointer")

	time.Sleep(grace + 1100*time.Millisecond)
	rep, err := r.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rep.RowsCleared != 1 || rep.RowsRetired != 0 {
		t.Fatalf("pass 2 = %+v, want 1 cleared / 0 retired", rep)
	}
	if has, _ := store.HasLFSObject(ctx, base, key); !has {
		t.Fatal("re-referenced row retired")
	}
}

// TestLFSReclaim_BudgetSkipsRepo: a repo whose walk exceeds the full run
// budget is skipped loudly — nothing marked, nothing retired, and the run
// does not error (the report carries the skip).
func TestLFSReclaim_BudgetSkipsRepo(t *testing.T) {
	ctx := context.Background()
	store, reg := newReclaimStore(t)
	base := "budget-repo"
	junk := "lfs/" + base + "/o2/" + strings.Repeat("ee", 32)
	if err := store.RegisterLFSObject(ctx, base, junk, 10); err != nil {
		t.Fatal(err)
	}
	if won, err := store.CASRef(ctx, base, "refs/heads/main", ref.ZeroSHA, strings.Repeat("44", 20)); err != nil || !won {
		t.Fatalf("CASRef = (%v, %v)", won, err)
	}
	fixtures := map[string]string{base: reclaimFixture(t)} // 1 commit+tree+blobs > budget 1
	r := testReclaimer(t, reg, staticHydrate(t, fixtures), time.Millisecond, 1, true)

	rep, err := r.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rep.ReposOverBudget != 1 || rep.ReposScanned != 0 || rep.RowsMarked != 0 {
		t.Fatalf("over-budget run = %+v, want skip with no row work", rep)
	}
	rows, err := reg.ListRepoLFSObjects(ctx, base, "", 10)
	if err != nil || len(rows) != 1 || !rows[0].UnreferencedSince.IsZero() {
		t.Fatalf("rows after over-budget run = (%+v, %v), want untouched", rows, err)
	}
}

// TestLFSReclaim_BudgetResumesNextRun: when a later repo runs out of the
// pass's REMAINDER, the cursor holds so the next run retries it first with
// a full budget — bounded AND resumable.
func TestLFSReclaim_BudgetResumesNextRun(t *testing.T) {
	ctx := context.Background()
	store, reg := newReclaimStore(t)
	// Two repos, alphabetical: a-repo then b-repo.
	fixtures := map[string]string{}
	for _, base := range []string{"a-repo", "b-repo"} {
		key := "lfs/" + base + "/o2/" + strings.Repeat("ab", 32)
		if err := store.RegisterLFSObject(ctx, base, key, 10); err != nil {
			t.Fatal(err)
		}
		if won, err := store.CASRef(ctx, base, "refs/heads/main", ref.ZeroSHA, strings.Repeat("55", 20)); err != nil || !won {
			t.Fatalf("CASRef = (%v, %v)", won, err)
		}
		fixtures[base] = reclaimFixture(t)
	}
	// Measure one fixture's walk units, then grant exactly one repo's full
	// cost — walk + one ref + one settled row — so the second repo defers.
	_, perRepo, err := lfsscan.ReachableLFSOIDs(ctx, fixtures["a-repo"], 1_000)
	if err != nil {
		t.Fatal(err)
	}
	r := testReclaimer(t, reg, staticHydrate(t, fixtures), time.Hour, perRepo+2, true)

	rep, err := r.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// b-repo's deferral is budget exhaustion, not a repo-over-budget skip.
	if rep.ReposScanned != 1 || rep.ReposOverBudget != 0 {
		t.Fatalf("run 1 = %+v, want a-repo scanned, b-repo deferred", rep)
	}
	if rows, err := reg.ListRepoLFSObjects(ctx, "b-repo", "", 10); err != nil || !rows[0].UnreferencedSince.IsZero() {
		t.Fatalf("b-repo rows touched by deferred run: (%+v, %v)", rows, err)
	}
	// Run 2 starts at the deferred repo with a full budget.
	rep, err = r.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rep.ReposScanned != 1 || rep.ReposOverBudget != 0 {
		t.Fatalf("run 2 = %+v, want b-repo scanned", rep)
	}
	for _, base := range []string{"a-repo", "b-repo"} {
		rows, err := reg.ListRepoLFSObjects(ctx, base, "", 10)
		if err != nil || len(rows) != 1 || rows[0].UnreferencedSince.IsZero() {
			t.Fatalf("%s rows = (%+v, %v), want marked", base, rows, err)
		}
	}
}

// TestLFSReclaim_SettlementChargesBudget (Codex round 4): settlement work is
// attacker-scaled too — a ref-less repo walks zero objects yet can hold a
// cap's worth of registry rows, each potentially one retirement transaction.
// Examined rows charge the run budget, so one pass touches at most budget
// rows and surfaces the budget verdict for the rest.
func TestLFSReclaim_SettlementChargesBudget(t *testing.T) {
	ctx := context.Background()
	store, reg := newReclaimStore(t)
	base := "settle-repo"
	for _, b := range []string{"aa", "bb", "cc", "dd", "ee"} {
		key := "lfs/" + base + "/o2/" + strings.Repeat(b, 32)
		if err := store.RegisterLFSObject(ctx, base, key, 10); err != nil {
			t.Fatal(err)
		}
	}
	// No refs anywhere in the group: the walk is skipped entirely and the
	// whole budget is settlement allowance (staticHydrate fails the test on
	// any hydration attempt).
	r := testReclaimer(t, reg, staticHydrate(t, nil), time.Hour, 3, true)

	rep, err := r.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// 5 rows, budget 3: exactly 3 rows examined and marked; the repo needs
	// more than a full budget, so it is counted over budget, not failed.
	if rep.RowsExamined != 3 || rep.RowsMarked != 3 || rep.ReposOverBudget != 1 {
		t.Fatalf("bounded settlement run = %+v, want 3 examined/marked and over-budget", rep)
	}

	// Pass 2 resumes at the stored row cursor (Codex round 8) instead of
	// re-paging the non-retiring prefix forever: the tail rows get marked.
	rep, err = r.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rep.RowsMarked != 2 {
		t.Fatalf("cursor pass = %+v, want the 2 remaining rows marked", rep)
	}
}

// purgeOnProbeRegistry commits a purge (fence + ledger cleanup) the
// moment the budget-exhaustion probe (the 1-row listing) observes the
// target repo — the exact window Codex round 26 flagged: the probe sees
// remaining rows, the purge deletes the repo's scan state, and an
// unfenced cursor park would then recreate a row nothing ever removes.
type purgeOnProbeRegistry struct {
	LFSReclaimRegistry
	t     *testing.T
	store ref.Store
	base  string
	fired bool
}

func (p *purgeOnProbeRegistry) ListRepoLFSObjects(ctx context.Context, repo, after string, limit int) ([]ref.LFSObjectRow, error) {
	rows, err := p.LFSReclaimRegistry.ListRepoLFSObjects(ctx, repo, after, limit)
	if err == nil && !p.fired && repo == p.base && limit == 1 {
		p.fired = true
		purger, ok := p.store.(interface {
			FenceRepoPurge(ctx context.Context, base string) error
			PurgeRepoLedger(ctx context.Context, group []string) (int64, error)
		})
		if !ok {
			p.t.Fatalf("store %T lacks the purge seams", p.store)
		}
		if err := purger.FenceRepoPurge(ctx, p.base); err != nil {
			p.t.Fatalf("FenceRepoPurge: %v", err)
		}
		if _, err := purger.PurgeRepoLedger(ctx, []string{p.base}); err != nil {
			p.t.Fatalf("PurgeRepoLedger: %v", err)
		}
	}
	return rows, err
}

// TestLFSReclaim_ParkRacingPurgeLeavesNoCursor (Codex round 26): a purge
// committing between the settlement probe and the cursor park must not
// leave an orphaned job_scan_state row — the purged repo is never listed
// again, so nothing else would ever remove it.
func TestLFSReclaim_ParkRacingPurgeLeavesNoCursor(t *testing.T) {
	ctx := context.Background()
	store, reg := newReclaimStore(t)
	base := "race-repo"
	for _, b := range []string{"aa", "bb", "cc", "dd", "ee"} {
		key := "lfs/" + base + "/o2/" + strings.Repeat(b, 32)
		if err := store.RegisterLFSObject(ctx, base, key, 10); err != nil {
			t.Fatal(err)
		}
	}
	// Ref-less: the whole budget (3 < 5 rows) is settlement allowance,
	// so the probe fires — and the hook purges before the park.
	wrapped := &purgeOnProbeRegistry{LFSReclaimRegistry: reg, t: t, store: store, base: base}
	r := testReclaimer(t, wrapped, staticHydrate(t, nil), time.Hour, 3, true)
	rep, err := r.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !wrapped.fired {
		t.Fatal("probe hook never fired: the park path was not exercised")
	}
	if rep.ReposOverBudget != 1 {
		t.Fatalf("run = %+v, want the repo counted over budget", rep)
	}
	if _, ok, err := reg.GetJobScanState(ctx, ref.ScanJobLFSReclaim, ref.ScanScopeReclaimRepo(base)); err != nil || ok {
		t.Fatalf("cursor after racing purge = (ok=%v, %v), want no orphaned row", ok, err)
	}
}

// TestLFSReclaim_ExactExhaustionBetweenMembers (Codex round 3): when one
// namespace-group member's walk consumes EXACTLY the remaining budget and a
// later member also has refs, the reclaimer must surface the over-budget
// verdict instead of hydrating that member and calling the walker with a
// zero budget — the walker asserts budget > 0, and that panic would kill the
// local-mode server process.
func TestLFSReclaim_ExactExhaustionBetweenMembers(t *testing.T) {
	ctx := context.Background()
	store, reg := newReclaimStore(t)
	base := "exact-repo"
	junk := "lfs/" + base + "/o2/" + strings.Repeat("ab", 32)
	if err := store.RegisterLFSObject(ctx, base, junk, 10); err != nil {
		t.Fatal(err)
	}
	// Both the base member and its +ephemeral twin hold refs.
	for _, member := range []string{base, base + "+ephemeral"} {
		if won, err := store.CASRef(ctx, member, "refs/heads/main", ref.ZeroSHA, strings.Repeat("66", 20)); err != nil || !won {
			t.Fatalf("CASRef %s = (%v, %v)", member, won, err)
		}
	}
	// Only the base member gets a fixture: the ephemeral member must never
	// be hydrated once the remainder hits zero (staticHydrate fails the
	// test on any unexpected hydration).
	fixtures := map[string]string{base: reclaimFixture(t)}
	// The budget exactly matches the base member's walk, so the remainder
	// is zero when the ephemeral member comes up.
	_, exact, err := lfsscan.ReachableLFSOIDs(ctx, fixtures[base], 1_000)
	if err != nil {
		t.Fatal(err)
	}
	r := testReclaimer(t, reg, staticHydrate(t, fixtures), time.Hour, exact, true)

	rep, err := r.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// The group needs more than a full budget, so it is skipped loudly with
	// no row touched — and above all, nothing panicked.
	if rep.ReposOverBudget != 1 || rep.ReposScanned != 0 || rep.RowsMarked != 0 {
		t.Fatalf("exact-exhaustion run = %+v, want over-budget skip", rep)
	}
	rows, err := reg.ListRepoLFSObjects(ctx, base, "", 10)
	if err != nil || len(rows) != 1 || !rows[0].UnreferencedSince.IsZero() {
		t.Fatalf("rows after exact-exhaustion run = (%+v, %v), want untouched", rows, err)
	}
}

// TestLFSReclaim_HydrationDeadline (Codex round 7): hydration downloads and
// indexes packs before the budgeted walk can start, so each member runs
// under a fixed deadline — a hydration that cannot finish fails the repo
// like any other hydration error instead of monopolizing the pass.
func TestLFSReclaim_HydrationDeadline(t *testing.T) {
	ctx := context.Background()
	store, reg := newReclaimStore(t)
	base := "cold-repo"
	key := "lfs/" + base + "/o2/" + strings.Repeat("ab", 32)
	if err := store.RegisterLFSObject(ctx, base, key, 10); err != nil {
		t.Fatal(err)
	}
	if won, err := store.CASRef(ctx, base, "refs/heads/main", ref.ZeroSHA, strings.Repeat("88", 20)); err != nil || !won {
		t.Fatalf("CASRef = (%v, %v)", won, err)
	}
	stuck := func(ctx context.Context, _, _ string) (string, error) {
		<-ctx.Done() // a hydration that never finishes on its own
		return "", ctx.Err()
	}
	r := NewLFSReclaimer(reg, stuck, lfsscan.ReachableLFSOIDs, alwaysMain,
		time.Hour, 1_000, true, true, slog.New(slog.DiscardHandler))
	r.hydrateTimeout = 50 * time.Millisecond

	_, err := r.Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "deadline") {
		t.Fatalf("run err = %v, want hydration deadline", err)
	}
	rows, err := reg.ListRepoLFSObjects(ctx, base, "", 10)
	if err != nil || len(rows) != 1 || !rows[0].UnreferencedSince.IsZero() {
		t.Fatalf("rows = (%+v, %v), want untouched", rows, err)
	}
}

// TestLFSReclaim_WalkDeadline (Codex security round 18): the budget meters
// objects only as cat-file reports them, but rev-list inflates each tree
// before its children stream — a giant compressible tree burns its whole
// expansion before budget cancellation can land. The compiled-in walk
// deadline hard-caps a member's walk wall-clock; expiry fails the repo
// like a hydration timeout and nothing is marked from the partial walk.
func TestLFSReclaim_WalkDeadline(t *testing.T) {
	ctx := context.Background()
	store, reg := newReclaimStore(t)
	base := "slow-walk-repo"
	key := "lfs/" + base + "/o2/" + strings.Repeat("ab", 32)
	if err := store.RegisterLFSObject(ctx, base, key, 10); err != nil {
		t.Fatal(err)
	}
	if won, err := store.CASRef(ctx, base, "refs/heads/main", ref.ZeroSHA, strings.Repeat("88", 20)); err != nil || !won {
		t.Fatalf("CASRef = (%v, %v)", won, err)
	}
	instantHydrate := func(context.Context, string, string) (string, error) {
		return t.TempDir(), nil
	}
	stuckWalk := func(ctx context.Context, _ string, _ int64) (map[string]struct{}, int64, error) {
		<-ctx.Done() // a walk that never finishes on its own
		return nil, 0, ctx.Err()
	}
	r := NewLFSReclaimer(reg, instantHydrate, stuckWalk, alwaysMain,
		time.Hour, 1_000, true, true, slog.New(slog.DiscardHandler))
	r.walkTimeout = 50 * time.Millisecond

	_, err := r.Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "deadline") {
		t.Fatalf("run err = %v, want walk deadline", err)
	}
	rows, err := reg.ListRepoLFSObjects(ctx, base, "", 10)
	if err != nil || len(rows) != 1 || !rows[0].UnreferencedSince.IsZero() {
		t.Fatalf("rows = (%+v, %v), want untouched", rows, err)
	}
}

// TestLFSReclaim_PassDeadline (Codex round 19): per-member walk deadlines
// reset for every repository and a deadline-expired walk charges almost no
// budget, so many wedged repositories could hold one pass for ten minutes
// each. The whole-pass cap ends the run instead; the rotation cursor makes
// the next pass resume after the last attempted repository.
func TestLFSReclaim_PassDeadline(t *testing.T) {
	ctx := context.Background()
	store, reg := newReclaimStore(t)
	for _, base := range []string{"wedged-a", "wedged-b", "wedged-c"} {
		key := "lfs/" + base + "/o2/" + strings.Repeat("ab", 32)
		if err := store.RegisterLFSObject(ctx, base, key, 10); err != nil {
			t.Fatal(err)
		}
		if won, err := store.CASRef(ctx, base, "refs/heads/main", ref.ZeroSHA, strings.Repeat("88", 20)); err != nil || !won {
			t.Fatalf("CASRef = (%v, %v)", won, err)
		}
	}
	instantHydrate := func(context.Context, string, string) (string, error) {
		return t.TempDir(), nil
	}
	stuckWalk := func(ctx context.Context, _ string, _ int64) (map[string]struct{}, int64, error) {
		<-ctx.Done() // every walk hangs until some deadline fires
		return nil, 0, ctx.Err()
	}
	r := NewLFSReclaimer(reg, instantHydrate, stuckWalk, alwaysMain,
		time.Hour, 1_000, true, true, slog.New(slog.DiscardHandler))
	// Per-member deadline generous, pass deadline tiny: the pass cap must
	// end the run — without it, three wedged repos would serially consume
	// three per-member windows.
	r.walkTimeout = time.Hour
	r.passTimeout = 50 * time.Millisecond

	start := time.Now()
	_, err := r.Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "deadline") {
		t.Fatalf("run err = %v, want pass deadline", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("pass ran %v, want the pass cap to end it promptly", elapsed)
	}
	if st, ok, err := reg.GetJobScanState(ctx, ref.ScanJobLFSReclaim, ref.ScanScopeReclaimRotation); err != nil || !ok || st.Payload == "" {
		t.Fatalf("rotation cursor = (%+v, %v, %v); want a durably parked advance so the next pass does not retry the same repo first", st, ok, err)
	}
}

// TestLFSReclaim_VanishedRefundsSettleDespiteBurnedDeadline (Codex round
// 34 on PR #132, P2): a base whose LAST row retired vanishes from
// LFSReposWithObjects, so its deferred refunds settle EXCLUSIVELY through
// the end-of-pass vanished-refund path — which must run on a detached
// bounded context: the main rotation regularly burns the whole pass
// deadline, and inheriting the expired context would leave such refunds
// charged indefinitely, blocking uploads against phantom usage.
func TestLFSReclaim_VanishedRefundsSettleDespiteBurnedDeadline(t *testing.T) {
	ctx := context.Background()
	store, reg := newReclaimStore(t)

	// The vanished base: one row registered, retired (refund parked in
	// its epoch bucket, counters still charged), then a covering sweep.
	vanished := "vanished-repo"
	vkey := "lfs/" + vanished + "/o2/" + strings.Repeat("cd", 32)
	if err := store.RegisterLFSObject(ctx, vanished, vkey, 100); err != nil {
		t.Fatal(err)
	}
	if n, err := reg.MarkLFSObjectsUnreferenced(ctx, vanished, []string{vkey}); err != nil || n != 1 {
		t.Fatalf("mark = (%d, %v)", n, err)
	}
	gens := make(map[string]int64)
	for _, member := range blob.QuotaRepoGroup(vanished) {
		g, err := store.GetGeneration(ctx, member)
		if err != nil {
			t.Fatalf("GetGeneration(%q): %v", member, err)
		}
		gens[member] = g
	}
	if retired, err := reg.RetireUnreferencedLFSObject(ctx, vanished, vkey, gens, 0, time.Hour); err != nil || !retired {
		t.Fatalf("retire = (%v, %v), want retired", retired, err)
	}
	if n, err := store.CountLFSObjects(ctx, vanished); err != nil || n != 1 {
		t.Fatalf("count after retire = (%d, %v), want still 1 (deferred)", n, err)
	}
	simulateCoveringSweep(t, reg, true)

	// A wedged repo that sorts FIRST burns the whole pass deadline
	// before the rotation can reach the vanished base — and because a
	// remainder-exhausted rotation HOLDS its cursor to retry the wedged
	// repo first next pass, the visit-time settlement in reclaimRepo
	// never runs for the vanished base: the end-of-pass vanished path is
	// its exclusive settlement route, exactly the degraded-walk scenario
	// (Codex round 34).
	wedged := "a-wedged"
	wkey := "lfs/" + wedged + "/o2/" + strings.Repeat("ab", 32)
	if err := store.RegisterLFSObject(ctx, wedged, wkey, 10); err != nil {
		t.Fatal(err)
	}
	if won, err := store.CASRef(ctx, wedged, "refs/heads/main", ref.ZeroSHA, strings.Repeat("88", 20)); err != nil || !won {
		t.Fatalf("CASRef = (%v, %v)", won, err)
	}
	instantHydrate := func(context.Context, string, string) (string, error) {
		return t.TempDir(), nil
	}
	stuckWalk := func(ctx context.Context, _ string, _ int64) (map[string]struct{}, int64, error) {
		<-ctx.Done()
		return nil, 0, ctx.Err()
	}
	r := NewLFSReclaimer(reg, instantHydrate, stuckWalk, alwaysMain,
		time.Hour, 1_000, true, true, slog.New(slog.DiscardHandler))
	r.walkTimeout = time.Hour
	r.passTimeout = 50 * time.Millisecond

	report, err := r.Run(ctx)
	if err == nil {
		t.Fatal("want the burned pass deadline reported")
	}
	// The vanished base's refund settled anyway, on the detached context.
	if report.RefundRowsSettled != 1 || report.RefundBytesSettled != 100 {
		t.Fatalf("settled = (%d rows, %d bytes), want the vanished base's refund", report.RefundRowsSettled, report.RefundBytesSettled)
	}
	if n, err := store.CountLFSObjects(ctx, vanished); err != nil || n != 0 {
		t.Fatalf("count after settle = (%d, %v), want 0", n, err)
	}
}

// TestLFSReclaim_RefFanoutChargesBudget (Codex round 6): a git:write holder
// can mint arbitrarily many refs naming one commit — registry
// materialization, cache sync, and rev-list enumeration all scale with ref
// count while the walk's unique objects stay tiny. Every ref charges a work
// unit, so fanout trips the budget before hydration.
func TestLFSReclaim_RefFanoutChargesBudget(t *testing.T) {
	ctx := context.Background()
	store, reg := newReclaimStore(t)
	base := "fanout-repo"
	key := "lfs/" + base + "/o2/" + strings.Repeat("ab", 32)
	if err := store.RegisterLFSObject(ctx, base, key, 10); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if won, err := store.CASRef(ctx, base, fmt.Sprintf("refs/heads/b%02d", i), ref.ZeroSHA, strings.Repeat("77", 20)); err != nil || !won {
			t.Fatalf("CASRef = (%v, %v)", won, err)
		}
	}
	// Budget 5 < 10 refs: skipped loudly before any hydration
	// (staticHydrate fails the test on any hydration attempt).
	r := testReclaimer(t, reg, staticHydrate(t, nil), time.Hour, 5, true)

	rep, err := r.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rep.ReposOverBudget != 1 || rep.ReposScanned != 0 || rep.RowsMarked != 0 {
		t.Fatalf("fanout run = %+v, want over-budget skip", rep)
	}
}

// countCapRegistry captures the limit passed to every CountRefs call.
type countCapRegistry struct {
	LFSReclaimRegistry
	caps []int64
}

func (c *countCapRegistry) CountRefs(ctx context.Context, repo string, limit int64) (int64, error) {
	c.caps = append(c.caps, limit)
	return c.LFSReclaimRegistry.CountRefs(ctx, repo, limit)
}

// TestLFSReclaim_MaxBudgetRefCountCap (Codex round 17): MaxInt64 is a valid
// object budget, and the count's one-past-the-remainder sentinel must
// saturate instead of overflowing — the overflow clamped the cap to 1, so
// CountRefs reported at most one ref and hydration could materialize an
// arbitrarily large ref set charged as a single unit.
func TestLFSReclaim_MaxBudgetRefCountCap(t *testing.T) {
	ctx := context.Background()
	store, reg := newReclaimStore(t)
	base := "max-budget-repo"
	key := "lfs/" + base + "/o2/" + strings.Repeat("ab", 32)
	if err := store.RegisterLFSObject(ctx, base, key, 10); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if won, err := store.CASRef(ctx, base, fmt.Sprintf("refs/heads/b%02d", i), ref.ZeroSHA, strings.Repeat("77", 20)); err != nil || !won {
			t.Fatalf("CASRef = (%v, %v)", won, err)
		}
	}
	capReg := &countCapRegistry{LFSReclaimRegistry: reg}
	// Hydration failing keeps the pass short; the captured caps are the
	// assertion, not the walk outcome.
	failHydrate := func(context.Context, string, string) (string, error) {
		return "", fmt.Errorf("hydration unavailable")
	}
	r := testReclaimer(t, capReg, failHydrate, time.Hour, math.MaxInt64, true)
	if _, err := r.Run(ctx); err != nil {
		t.Logf("run: %v (hydration failure expected)", err)
	}
	if len(capReg.caps) == 0 {
		t.Fatal("CountRefs was never consulted")
	}
	for i, c := range capReg.caps {
		if c < math.MaxInt64-10 {
			t.Fatalf("CountRefs cap %d = %d, want a saturated near-MaxInt64 cap (overflow clamped it)", i, c)
		}
	}
}

// TestLFSReclaim_SkippedReposChargeBudget (Codex round 5): soft-deleted
// repositories keep their registered LFS rows but lose their metadata, so
// the reclaimer skips them — and each skip must charge the run budget, or
// an unbounded skipped population (several queries per repo, an entry in
// the scanned set) would be paged in full on every single pass.
func TestLFSReclaim_SkippedReposChargeBudget(t *testing.T) {
	ctx := context.Background()
	store, reg := newReclaimStore(t)
	for _, base := range []string{"gone-a", "gone-b", "gone-c", "gone-d", "gone-e"} {
		key := "lfs/" + base + "/o2/" + strings.Repeat("ab", 32)
		if err := store.RegisterLFSObject(ctx, base, key, 10); err != nil {
			t.Fatal(err)
		}
	}
	noMetadata := func(context.Context, string) (string, bool, error) { return "", false, nil }
	r := NewLFSReclaimer(reg, staticHydrate(t, nil), lfsscan.ReachableLFSOIDs, noMetadata,
		time.Hour, 3, true, true, slog.New(slog.DiscardHandler))

	rep, err := r.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// 5 metadata-less repos, budget 3: exactly 3 attempted-and-skipped.
	if rep.ReposSkipped != 3 {
		t.Fatalf("skip run = %+v, want 3 budget-bounded skips", rep)
	}
}

// TestLFSReclaim_LegacyRowsDeferredWhenUnarmed (Codex round 17 on PR
// #132): legacy-spelled registry rows retire only when legacy reaping is
// armed END-TO-END — this replica's CODESTORAGE_SWEEP_DELETE_LFS_LEGACY
// AND a watermark payload certifying a legacy-armed sweep. Either half
// missing defers the row with the distinct counter: its blob would never
// be physically reaped, so the slot refund would be unbacked.
func TestLFSReclaim_LegacyRowsDeferredWhenUnarmed(t *testing.T) {
	ctx := context.Background()
	store, reg := newReclaimStore(t) // seeds a legacy-armed watermark
	base := "legacy-defer-repo"
	canonical := "lfs/" + base + "/o2/" + strings.Repeat("aa", 32)
	legacy := "lfs/" + base + "/" + strings.Repeat("bb", 32)
	for _, key := range []string{canonical, legacy} {
		if err := store.RegisterLFSObject(ctx, base, key, 100); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := reg.MarkLFSObjectsUnreferenced(ctx, base, []string{canonical, legacy}); err != nil || n != 2 {
		t.Fatalf("mark = (%d, %v)", n, err)
	}

	run := func(legacyDelete bool) LFSReclaimReport {
		t.Helper()
		// No refs, so nothing hydrates; the marks are grace-expired on
		// the scan's (injected) clock. The retire transaction's DB-clock
		// grace recheck spares the canonical row — irrelevant here: the
		// deferral under test fires BEFORE any transaction.
		r := NewLFSReclaimer(reg, staticHydrate(t, nil), lfsscan.ReachableLFSOIDs, alwaysMain,
			time.Hour, 1_000, true, legacyDelete, slog.New(slog.DiscardHandler))
		r.now = func() time.Time { return time.Now().Add(2 * time.Hour) }
		rep, err := r.Run(ctx)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		return rep
	}

	// Config half off: the legacy row defers even under a legacy-armed
	// watermark — a fresh pre-flip watermark must not keep legacy
	// refunds flowing for its freshness window.
	if rep := run(false); rep.RowsDeferredLegacyUnarmed != 1 || rep.RowsRetired != 0 {
		t.Fatalf("config-off run = %+v, want the legacy row deferred and nothing retired", rep)
	}

	// Watermark half unarmed: same deferral with the config flag on.
	if err := reg.RecordLFSReapComplete(ctx, 1, false); err != nil {
		t.Fatal(err)
	}
	if rep := run(true); rep.RowsDeferredLegacyUnarmed != 1 || rep.RowsRetired != 0 {
		t.Fatalf("unarmed-watermark run = %+v, want the legacy row deferred and nothing retired", rep)
	}
	if has, err := store.HasLFSObject(ctx, base, legacy); err != nil || !has {
		t.Fatalf("legacy row = (%v, %v), want intact after both deferrals", has, err)
	}
}

// TestLFSReclaim_PurgedRepoSkipped: purge-fenced repos are the purge
// pipeline's problem; the reclaimer never hydrates or touches them.
func TestLFSReclaim_PurgedRepoSkipped(t *testing.T) {
	ctx := context.Background()
	store, reg := newReclaimStore(t)
	base := "purged-repo"
	key := "lfs/" + base + "/o2/" + strings.Repeat("ff", 32)
	if err := store.RegisterLFSObject(ctx, base, key, 10); err != nil {
		t.Fatal(err)
	}
	fencer, ok := store.(interface {
		FenceRepoPurge(ctx context.Context, base string) error
	})
	if !ok {
		t.Fatalf("store %T lacks FenceRepoPurge", store)
	}
	if err := fencer.FenceRepoPurge(ctx, base); err != nil {
		t.Fatal(err)
	}
	r := testReclaimer(t, reg, staticHydrate(t, map[string]string{}), time.Millisecond, 1_000, true)
	rep, err := r.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rep.ReposSkipped != 1 || rep.ReposScanned != 0 {
		t.Fatalf("purged run = %+v, want skipped", rep)
	}
}

// TestLFSReclaim_SettlementHandoffResumes (issue #121): the per-repo
// row-settlement cursor is fleet-persistent, so a budget-limited pass on
// worker A parks it durably and a FRESH reclaimer (River scheduling the
// next pass on another replica) resumes strictly past the settled prefix
// instead of re-examining it forever.
func TestLFSReclaim_SettlementHandoffResumes(t *testing.T) {
	ctx := context.Background()
	store, reg := newReclaimStore(t)
	base := "handoff-repo"
	for i := 0; i < 6; i++ {
		key := "lfs/" + base + "/o2/" + strings.Repeat(string(rune('a'+i)), 64)
		if err := store.RegisterLFSObject(ctx, base, key, 10); err != nil {
			t.Fatal(err)
		}
	}
	gens := map[string]int64{base: 0}
	for _, member := range blob.QuotaRepoGroup(base) {
		g, err := reg.GetGeneration(ctx, member)
		if err != nil {
			t.Fatal(err)
		}
		gens[member] = g
	}

	// Worker A: allowance 3 settles half the rows and parks the cursor.
	workerA := testReclaimer(t, reg, staticHydrate(t, nil), time.Hour, 1_000, false)
	var report LFSReclaimReport
	if _, err := workerA.settleRepoRows(ctx, base, gens, nil, 3, &report); err == nil || !errors.Is(err, lfsscan.ErrLFSScanBudget) {
		t.Fatalf("worker A settle err = %v, want the budget verdict", err)
	}
	if report.RowsExamined != 3 || report.RowsMarked != 3 {
		t.Fatalf("worker A report = %+v, want 3 rows settled", report)
	}
	if st, ok, err := reg.GetJobScanState(ctx, ref.ScanJobLFSReclaim, ref.ScanScopeReclaimRepo(base)); err != nil || !ok || st.Payload == "" {
		t.Fatalf("row cursor = (%+v, %v, %v), want a durably parked cursor", st, ok, err)
	}

	// Worker B (fresh process): resumes past the settled prefix — only
	// the 3 tail rows are examined — and completion clears the cursor.
	workerB := testReclaimer(t, reg, staticHydrate(t, nil), time.Hour, 1_000, false)
	report = LFSReclaimReport{}
	if _, err := workerB.settleRepoRows(ctx, base, gens, nil, 100, &report); err != nil {
		t.Fatalf("worker B settle: %v", err)
	}
	if report.RowsExamined != 3 || report.RowsMarked != 3 {
		t.Fatalf("worker B report = %+v, want ONLY the 3 unsettled tail rows", report)
	}
	if _, ok, err := reg.GetJobScanState(ctx, ref.ScanJobLFSReclaim, ref.ScanScopeReclaimRepo(base)); err != nil || ok {
		t.Fatalf("row cursor survived settlement completion (ok=%v, err=%v)", ok, err)
	}

	// Worker C: a completed settlement starts the next rotation from the
	// top of the keyspace.
	workerC := testReclaimer(t, reg, staticHydrate(t, nil), time.Hour, 1_000, false)
	report = LFSReclaimReport{}
	if _, err := workerC.settleRepoRows(ctx, base, gens, nil, 100, &report); err != nil {
		t.Fatalf("worker C settle: %v", err)
	}
	if report.RowsExamined != 6 {
		t.Fatalf("worker C report = %+v, want a fresh full scan", report)
	}
}

// TestLFSReclaim_RowCursorGenerationFence (issue #121): a parked settlement
// cursor is fenced by the pinned generations — refs moving invalidates it,
// restarting settlement from the top of the keyspace instead of resuming
// across the fence.
func TestLFSReclaim_RowCursorGenerationFence(t *testing.T) {
	ctx := context.Background()
	store, reg := newReclaimStore(t)
	base := "fence-repo"
	for i := 0; i < 4; i++ {
		key := "lfs/" + base + "/o2/" + strings.Repeat(string(rune('a'+i)), 64)
		if err := store.RegisterLFSObject(ctx, base, key, 10); err != nil {
			t.Fatal(err)
		}
	}
	pin := func() map[string]int64 {
		gens := map[string]int64{}
		for _, member := range blob.QuotaRepoGroup(base) {
			g, err := reg.GetGeneration(ctx, member)
			if err != nil {
				t.Fatal(err)
			}
			gens[member] = g
		}
		return gens
	}

	r := testReclaimer(t, reg, staticHydrate(t, nil), time.Hour, 1_000, false)
	var report LFSReclaimReport
	if _, err := r.settleRepoRows(ctx, base, pin(), nil, 2, &report); !errors.Is(err, lfsscan.ErrLFSScanBudget) {
		t.Fatalf("settle err = %v, want the budget verdict", err)
	}
	if report.RowsExamined != 2 {
		t.Fatalf("report = %+v, want 2 rows settled before parking", report)
	}

	// Refs move: the generation bump (schema trigger) invalidates the
	// parked cursor, so the next attempt re-pins and restarts from the
	// top — never a resume across the fence.
	if won, err := store.CASRef(ctx, base, "refs/heads/main", ref.ZeroSHA, strings.Repeat("77", 20)); err != nil || !won {
		t.Fatalf("CASRef = (%v, %v)", won, err)
	}
	report = LFSReclaimReport{}
	if _, err := r.settleRepoRows(ctx, base, pin(), nil, 100, &report); err != nil {
		t.Fatalf("settle after fence bump: %v", err)
	}
	if report.RowsExamined != 4 {
		t.Fatalf("report = %+v, want a FULL restart after the fence bump", report)
	}
}

// TestLFSReclaim_WatermarkStaleDefersRetirement (issue #120): with deletes
// enabled but no destructive LFS sweep completed within the grace window,
// eligible rows are deferred — loudly and distinctly — and retire again
// once a completed sweep refreshes the watermark.
func TestLFSReclaim_WatermarkStaleDefersRetirement(t *testing.T) {
	ctx := context.Background()
	store, reg := newReclaimStore(t)
	base := "wm-stale-repo"
	junk := "lfs/" + base + "/o2/" + strings.Repeat("dd", 32)
	if err := store.RegisterLFSObject(ctx, base, junk, 10); err != nil {
		t.Fatal(err)
	}
	// No refs: nothing reachable, so the row is junk without any git
	// fixture; grace 300ms makes it eligible after a short sleep.
	grace := 300 * time.Millisecond
	r := testReclaimer(t, reg, staticHydrate(t, nil), grace, 1_000, true)

	if rep, err := r.Run(ctx); err != nil || rep.RowsMarked != 1 {
		t.Fatalf("pass 1 = (%+v, %v), want the row marked", rep, err)
	}
	time.Sleep(grace + 2100*time.Millisecond)

	// The sweeper stops completing destructive passes: the watermark
	// disappears past the grace bound, so the pass demotes itself.
	if err := reg.DeleteJobScanState(ctx, ref.ScanJobSweep, ref.ScanScopeLFSDestructiveReap); err != nil {
		t.Fatal(err)
	}
	rep, err := r.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.ReapWatermarkStale || rep.RowsDeferredReapStale != 1 || rep.RowsRetired != 0 {
		t.Fatalf("stale pass = %+v, want the eligible row deferred distinctly", rep)
	}
	if has, _ := store.HasLFSObject(ctx, base, junk); !has {
		t.Fatal("row retired while the reap watermark was stale")
	}

	// A destructive sweep completes again: refunds resume.
	seedReapWatermark(t, reg)
	rep, err = r.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rep.ReapWatermarkStale || rep.RowsRetired != 1 {
		t.Fatalf("recovered pass = %+v, want the row retired", rep)
	}
	if has, _ := store.HasLFSObject(ctx, base, junk); has {
		t.Fatal("row survived the recovered pass")
	}
}

// TestLFSReclaim_RotationHandoffResumes (issue #121): the rotation cursor
// is fleet-persistent — a fresh reclaimer resumes strictly after the last
// attempted repository instead of restarting its own rotation.
func TestLFSReclaim_RotationHandoffResumes(t *testing.T) {
	ctx := context.Background()
	store, reg := newReclaimStore(t)
	// Two ref-less junk repos: each pass scans within budget; budget 3
	// covers one repo's charge (1 unit skip floor + rows).
	for _, base := range []string{"rot-a", "rot-b"} {
		key := "lfs/" + base + "/o2/" + strings.Repeat("ee", 32)
		if err := store.RegisterLFSObject(ctx, base, key, 10); err != nil {
			t.Fatal(err)
		}
	}

	workerA := testReclaimer(t, reg, staticHydrate(t, nil), time.Hour, 1, false)
	if rep, err := workerA.Run(ctx); err != nil || rep.ReposScanned != 1 {
		t.Fatalf("worker A = (%+v, %v), want exactly one repo scanned before the budget ran out", rep, err)
	}
	st, ok, err := reg.GetJobScanState(ctx, ref.ScanJobLFSReclaim, ref.ScanScopeReclaimRotation)
	if err != nil || !ok || st.Payload != "rot-a" {
		t.Fatalf("rotation cursor = (%+v, %v, %v), want parked on rot-a", st, ok, err)
	}

	// A fresh worker resumes after rot-a and scans rot-b first.
	workerB := testReclaimer(t, reg, staticHydrate(t, nil), time.Hour, 1_000, false)
	rep, err := workerB.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rep.ReposScanned != 2 {
		t.Fatalf("worker B = %+v, want the full rotation (resumed tail + wrap)", rep)
	}
	rows, err := reg.ListRepoLFSObjects(ctx, "rot-b", "", 10)
	if err != nil || len(rows) != 1 || rows[0].UnreferencedSince.IsZero() {
		t.Fatalf("rot-b rows = (%+v, %v), want marked by the resumed rotation", rows, err)
	}
}
