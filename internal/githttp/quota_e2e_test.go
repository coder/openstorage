package githttp

// End-to-end tests for issue #21 over real serve processes and real git
// clients, on both backend lanes (see e2e_test.go's TestMain): the shared
// rate-limit knob now covers the git smart-HTTP surface without starving
// the REST surface, and the per-repo pack-bytes quota denies pushes at
// pre-receive without storing anything. Config plumbing is exercised for
// real: every knob here travels env -> serveConfig -> Server/hook.

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/open-storage/internal/blob"
	"github.com/coder/open-storage/internal/ref"
)

// TestGitSurfaceRateLimit: with the rate limit enabled, a push loop from one
// client IP hits 429 on the git surface (each push spends several smart-HTTP
// requests), while the first push — and the REST surface with its separate
// bucket — keep working.
func TestGitSurfaceRateLimit(t *testing.T) {
	node := startNodeT(t, "OPENSTORAGE_RATE_RPS=0.01", "OPENSTORAGE_RATE_BURST=20")
	repo := registerRepo(t, "quota-ratelimit")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")

	// Under the limit, pushes work normally.
	out := mustGit(t, work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/main")
	t.Logf("first push under limiter:\n%s", out)

	// Hammer: with burst 20 and ~no refill (0.01 rps), the loop must trip
	// the limiter well before 24 attempts. git surfaces the HTTP status.
	limited := false
	for i := 0; i < 24 && !limited; i++ {
		pushOut, err := tryGit(work, "push", remoteURL(node, repo, token), "--force", "HEAD:refs/heads/main")
		if err != nil && strings.Contains(pushOut, "429") {
			limited = true
			t.Logf("push %d rate-limited:\n%s", i, pushOut)
		} else if err != nil {
			t.Fatalf("push %d failed without a 429:\n%s", i, pushOut)
		}
	}
	if !limited {
		t.Fatal("push loop never hit the git-surface rate limit")
	}

	// The REST surface holds separate bucket state: with the git budget
	// exhausted, an API read from the same client IP still passes.
	if status := apiGet(t, node, mintTestToken(t, repo, "git:read", ""), "/api/v1/repos/branches", nil); status == http.StatusTooManyRequests {
		t.Fatal("REST route answered 429 after git-surface exhaustion: bucket state is shared")
	}
}

// TestPackQuota_PreReceiveDeniesAndStoresNothing: a push that would take the
// repo past its quota is denied inside pre-receive BEFORE any upload or
// registration — the client sees the quota message, and the pack neither
// reaches the blob store nor the pack registry (the issue #21 growth vector:
// even rejected pushes used to grow packs/ durably).
func TestPackQuota_PreReceiveDeniesAndStoresNothing(t *testing.T) {
	// Cap off (OPENSTORAGE_MAX_PUSH_BYTES=0): byte-scale quotas would trip
	// the quota-derived ingress cap (#55) before pre-receive, and these
	// tests pin QUOTA semantics.
	node := startNodeT(t, "OPENSTORAGE_PACK_QUOTA_BYTES=100", "OPENSTORAGE_MAX_PUSH_BYTES=0")
	repo := registerRepo(t, "quota-denied")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")

	out, err := tryGit(work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("push succeeded past a 100-byte quota:\n%s", out)
	}
	if !strings.Contains(out, "pack quota exceeded") {
		t.Fatalf("push failure does not surface the quota message:\n%s", out)
	}
	t.Logf("quota-denied push output:\n%s", out)

	if objs := s3Packs(t, repo); len(objs) != 0 {
		t.Fatalf("denied push left pack objects in the blob store: %v", objs)
	}
	if packs := pgPacks(t, repo); len(packs) != 0 {
		t.Fatalf("denied push left registered packs: %v", packs)
	}
	if refs := pgRefs(t, repo); len(refs) != 0 {
		t.Fatalf("denied push moved refs: %v", refs)
	}
}

// TestPackQuota_OverQuotaDeniedAtPreReceive: a repo already over quota
// refuses growth at pre-receive (the sole enforcement point — Codex rounds
// 3–6 established that HTTP-time rejection is unsound because any incoming
// pack may dedup to zero growth), while zero-growth operations keep working
// as the recovery path.
func TestPackQuota_OverQuotaDeniedAtPreReceive(t *testing.T) {
	// Seed real usage through nodeA (default generous quota).
	repo := registerRepo(t, "quota-overquota")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")
	if objs := s3Packs(t, repo); len(objs) == 0 {
		t.Fatal("seed push stored no packs")
	}
	packsBefore := len(pgPacks(t, repo))

	// A 1-byte-quota node now sees the repo over quota: new bytes are
	// denied inside pre-receive, before anything is stored.
	node := startNodeT(t, "OPENSTORAGE_PACK_QUOTA_BYTES=1", "OPENSTORAGE_MAX_PUSH_BYTES=0") // cap off, see TestPackQuota_PreReceiveDeniesAndStoresNothing
	commitFile(t, work, "b.txt", "two\n", "two", nil)
	out, err := tryGit(work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("push succeeded on over-quota repo:\n%s", out)
	}
	if !strings.Contains(out, "pack quota exceeded") {
		t.Fatalf("push failure does not surface the quota denial:\n%s", out)
	}
	t.Logf("over-quota push output:\n%s", out)
	if got := len(pgPacks(t, repo)); got != packsBefore {
		t.Fatalf("denied push changed registered packs: %d -> %d", packsBefore, got)
	}

	// Zero-growth recovery (Codex round 3): a delete-only push carries no
	// packfile and must keep working on the same over-quota repo — it is
	// how an operator/writer shrinks a wedged repo. Seed a second branch
	// through nodeA first, then delete it through the 1-byte-quota node.
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD~:refs/heads/todelete")
	out = mustGit(t, work, "push", remoteURL(node, repo, token), "--delete", "refs/heads/todelete")
	t.Logf("delete-only push on over-quota repo:\n%s", out)
	if _, exists := pgRefs(t, repo)["refs/heads/todelete"]; exists {
		t.Fatal("delete-only push did not delete the ref through the over-quota node")
	}
}

// TestPackQuota_RegisteredDuplicateRetryCompletes (Codex round 6): a crash
// between RegisterPacks and the update-hook CAS leaves a REGISTERED but
// unreferenced pack. The retry re-sends the identical content-addressed
// pack while registered usage alone reaches the quota; pre-receive's
// Stat-dedup classifies it as zero growth and the retry must complete the
// interrupted ref publication — no layer may reject it first.
func TestPackQuota_RegisteredDuplicateRetryCompletes(t *testing.T) {
	repo := registerRepo(t, "quota-registered-dup")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")

	// after-s3 crash publishes the pack without registering it; register
	// it manually to reproduce the registered-but-unreferenced window
	// (crash after PG commit, before ref CAS).
	out, err := tryGit(work,
		"-c", "http.extraHeader="+CrashHeader+": after-s3",
		"push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("push should have failed under crash knob:\n%s", out)
	}
	orphans := s3Packs(t, repo)
	if len(orphans) == 0 {
		t.Fatal("crash left no published pack; the knob did not fire")
	}
	var packs []ref.Pack
	var packBytes int64
	for _, o := range orphans {
		id := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(o.Key), "pack-"), ".pack")
		packs = append(packs, ref.Pack{ID: id, Key: o.Key})
		packBytes += o.Size
	}
	if err := testStore.RegisterPacks(context.Background(), repo, packs); err != nil {
		t.Fatalf("register crash-window packs: %v", err)
	}
	if refs := pgRefs(t, repo); len(refs) != 0 {
		t.Fatalf("setup: refs unexpectedly moved: %v", refs)
	}

	// Registered bytes == quota: the identical retry must still complete
	// (pre-receive dedups incoming to zero; the CAS then lands).
	node := startNodeT(t, fmt.Sprintf("OPENSTORAGE_PACK_QUOTA_BYTES=%d", packBytes), "OPENSTORAGE_MAX_PUSH_BYTES=0") // cap off, see TestPackQuota_PreReceiveDeniesAndStoresNothing
	out = mustGit(t, work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/main")
	t.Logf("registered-duplicate retry output:\n%s", out)
	head := revParse(t, work, "HEAD")
	if refs := pgRefs(t, repo); refs["refs/heads/main"] != head {
		t.Fatalf("refs/heads/main = %q, want %q (retry must complete publication)", refs["refs/heads/main"], head)
	}

	// Genuinely new bytes on the same at-quota node are still refused.
	commitFile(t, work, "c.txt", "three\n", "three", nil)
	out, err = tryGit(work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("new-bytes push succeeded at quota:\n%s", out)
	}
	if !strings.Contains(out, "pack quota exceeded") {
		t.Fatalf("expected a quota denial:\n%s", out)
	}
}

// TestPackQuota_PolicyDeniedPushStoresNothing (Codex security round): a
// push whose ref updates are ALL denied by the token's ref policy is
// refused at pre-receive BEFORE publication — otherwise its pack would be
// uploaded and registered (registered packs are not sweeper orphans) and
// permanently consume the repo's quota, letting a policy-restricted token
// wedge the quota shut for legitimate writers. A mixed push keeps working:
// the pack is needed for the surviving ref.
func TestPackQuota_PolicyDeniedPushStoresNothing(t *testing.T) {
	repo := registerRepo(t, "quota-policy-denied")
	work := initWorkRepo(t)

	// Catch-all no-push policy: every command is denied.
	denied := mintTestToken(t, repo, "git:read,git:write", `[["*",["no-push"]]]`)
	out, err := tryGit(work, "push", remoteURL(nodeA, repo, denied), "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("wholly policy-denied push succeeded:\n%s", out)
	}
	if !strings.Contains(out, "refusing pack publication") {
		t.Fatalf("denial does not name the publication gate:\n%s", out)
	}
	if objs := s3Packs(t, repo); len(objs) != 0 {
		t.Fatalf("policy-denied push left pack objects: %v", objs)
	}
	if packs := pgPacks(t, repo); len(packs) != 0 {
		t.Fatalf("policy-denied push left registered packs: %v", packs)
	}

	// Mixed push: main denied, everything else allowed. The pack must be
	// published for the surviving ref; the update hook still rejects main.
	mixed := mintTestToken(t, repo, "git:read,git:write", `[["refs/heads/main",["no-push"]],["*",[]]]`)
	out, err = tryGit(work, "push", remoteURL(nodeA, repo, mixed), "HEAD:refs/heads/main", "HEAD:refs/heads/feature")
	if err == nil {
		t.Fatalf("mixed push should exit non-zero (main is rejected):\n%s", out)
	}
	t.Logf("mixed push output:\n%s", out)
	head := revParse(t, work, "HEAD")
	refs := pgRefs(t, repo)
	if refs["refs/heads/feature"] != head {
		t.Fatalf("feature = %q, want %q (allowed ref must land)", refs["refs/heads/feature"], head)
	}
	if _, exists := refs["refs/heads/main"]; exists {
		t.Fatal("main exists; the no-push policy did not fire")
	}
	if packs := pgPacks(t, repo); len(packs) == 0 {
		t.Fatal("mixed push registered no packs; the surviving ref's pack is missing")
	}
}

// TestPackQuota_CrashRetryNotDoubleCounted (Codex round 4): a push retried
// after the after-s3 crash window re-sends a content-addressed pack whose
// bytes the usage listing already counts as an orphan. Counting them again
// as incoming would reject the only operation that completes the
// interrupted publication; the Stat-dedup in pre-receive must let the
// retry through a quota sized below 2x the pack.
func TestPackQuota_CrashRetryNotDoubleCounted(t *testing.T) {
	repo := registerRepo(t, "quota-crash-retry")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")

	// Crash the pre-receive hook after upload, before registration.
	out, err := tryGit(work,
		"-c", "http.extraHeader="+CrashHeader+": after-s3",
		"push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("push should have failed under crash knob:\n%s", out)
	}
	orphans := s3Packs(t, repo)
	if len(orphans) == 0 {
		t.Fatal("crash left no orphaned pack; the knob did not fire")
	}
	var orphanBytes int64
	for _, o := range orphans {
		orphanBytes += o.Size
	}

	// Quota admits the pack once but NOT twice: naive double-counting
	// (usage=P + incoming=P > P+16) rejects; deduped (incoming=0) passes.
	node := startNodeT(t, fmt.Sprintf("OPENSTORAGE_PACK_QUOTA_BYTES=%d", orphanBytes+16), "OPENSTORAGE_MAX_PUSH_BYTES=0") // cap off, see TestPackQuota_PreReceiveDeniesAndStoresNothing
	out = mustGit(t, work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/main")
	t.Logf("retry push output:\n%s", out)

	head := revParse(t, work, "HEAD")
	if refs := pgRefs(t, repo); refs["refs/heads/main"] != head {
		t.Fatalf("refs/heads/main = %q, want %q (retry must complete publication)", refs["refs/heads/main"], head)
	}
	if packs := pgPacks(t, repo); len(packs) == 0 {
		t.Fatal("retry registered no packs")
	}
}

// TestPackQuota_OrphanInflatedFastFailDefers (Codex round 5): when orphan
// bytes ALONE reach the quota (after-s3 crash window), the HTTP fast-fail
// must not wedge the identical retry — the incoming pack IS the orphan,
// Stat-dedups to zero growth in pre-receive, and completes the interrupted
// publication.
func TestPackQuota_OrphanInflatedRetryCompletes(t *testing.T) {
	repo := registerRepo(t, "quota-orphan-inflated")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")

	out, err := tryGit(work,
		"-c", "http.extraHeader="+CrashHeader+": after-s3",
		"push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("push should have failed under crash knob:\n%s", out)
	}
	var orphanBytes int64
	for _, o := range s3Packs(t, repo) {
		orphanBytes += o.Size
	}
	if orphanBytes == 0 {
		t.Fatal("crash left no orphaned pack; the knob did not fire")
	}

	// Quota exactly at the orphan bytes: pre-receive dedups the re-sent
	// orphan to zero incoming and completes publication.
	node := startNodeT(t, fmt.Sprintf("OPENSTORAGE_PACK_QUOTA_BYTES=%d", orphanBytes), "OPENSTORAGE_MAX_PUSH_BYTES=0") // cap off, see TestPackQuota_PreReceiveDeniesAndStoresNothing
	out = mustGit(t, work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/main")
	t.Logf("orphan-inflated retry output:\n%s", out)
	head := revParse(t, work, "HEAD")
	if refs := pgRefs(t, repo); refs["refs/heads/main"] != head {
		t.Fatalf("refs/heads/main = %q, want %q (deferred retry must complete)", refs["refs/heads/main"], head)
	}
	if packs := pgPacks(t, repo); len(packs) == 0 {
		t.Fatal("retry registered no packs")
	}

	// Genuinely new bytes are still refused at quota by pre-receive.
	commitFile(t, work, "c.txt", "three\n", "three", nil)
	out, err = tryGit(work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("new-bytes push succeeded at quota:\n%s", out)
	}
	if !strings.Contains(out, "pack quota exceeded") {
		t.Fatalf("expected a quota rejection:\n%s", out)
	}
}

// TestPackQuota_DecoyMixedPushRefused (Codex round 7): a push pairing a
// policy-DENIED update that carries the pack with an allowed decoy create
// at an already-stored SHA must be refused whole — publishing would
// register a pack only the denied ref needs, a repeatable quota-filling
// vector (registered packs are not sweeper orphans). The remedy in the
// denial message works: pushing the allowed update alone succeeds.
func TestPackQuota_DecoyMixedPushRefused(t *testing.T) {
	repo := registerRepo(t, "quota-decoy")
	work := initWorkRepo(t)
	full := mintTestToken(t, repo, "git:read,git:write", "")

	// Seed commit1 on main so its objects are already stored.
	mustGit(t, work, "push", remoteURL(nodeA, repo, full), "HEAD:refs/heads/main")
	seedHead := revParse(t, work, "HEAD")
	packsBefore := len(s3Packs(t, repo))

	// commit2 carries new bytes; policy denies main, allows everything
	// else. Push commit2->main (denied, needs the pack) + decoy create at
	// the stored commit1 (allowed, needs nothing).
	commitFile(t, work, "big.txt", strings.Repeat("x", 4096), "big", nil)
	mixed := mintTestToken(t, repo, "git:read,git:write", `[["refs/heads/main",["no-push"]],["*",[]]]`)
	out, err := tryGit(work, "push", remoteURL(nodeA, repo, mixed),
		"HEAD:refs/heads/main", seedHead+":refs/heads/decoy")
	if err == nil {
		t.Fatalf("decoy mixed push succeeded:\n%s", out)
	}
	if !strings.Contains(out, "refusing pack publication") {
		t.Fatalf("denial does not name the publication gate:\n%s", out)
	}
	t.Logf("decoy mixed push output:\n%s", out)
	if got := len(s3Packs(t, repo)); got != packsBefore {
		t.Fatalf("decoy push grew pack storage: %d -> %d", packsBefore, got)
	}
	if _, exists := pgRefs(t, repo)["refs/heads/decoy"]; exists {
		t.Fatal("decoy ref landed despite the refused push")
	}

	// Remedy: the allowed create alone (no denied passenger) succeeds.
	out = mustGit(t, work, "push", remoteURL(nodeA, repo, mixed), seedHead+":refs/heads/decoy")
	t.Logf("split allowed push output:\n%s", out)
	if got := pgRefs(t, repo)["refs/heads/decoy"]; got != seedHead {
		t.Fatalf("decoy = %q, want %q after split push", got, seedHead)
	}

	// A genuine mixed push (allowed ref NEEDS the pack) still publishes:
	// commit2 to an allowed branch alongside the denied main update.
	out, err = tryGit(work, "push", remoteURL(nodeA, repo, mixed),
		"HEAD:refs/heads/main", "HEAD:refs/heads/feature")
	if err == nil {
		t.Fatalf("mixed push should exit non-zero (main is denied):\n%s", out)
	}
	if got, want := pgRefs(t, repo)["refs/heads/feature"], revParse(t, work, "HEAD"); got != want {
		t.Fatalf("feature = %q, want %q (justified pack must publish)", got, want)
	}
}

// TestPackDedup_NoOverwriteOnRepush (Codex round 10): re-sending an
// already-published content-addressed pack (ref delete + re-create) must
// not re-upload it — on a versioned S3 bucket every overwrite retains a
// billed noncurrent version invisible to usage listings. Observable on
// fileblob as an unchanged ModTime.
func TestPackDedup_NoOverwriteOnRepush(t *testing.T) {
	repo := registerRepo(t, "quota-no-overwrite")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")

	// Use a non-default branch: default-branch deletion is refused by
	// design (invariant 4), and the delete/re-create dance needs a
	// deletable ref that is the sole referrer of the pushed objects.
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/scratch")
	before := s3Packs(t, repo)
	if len(before) != 1 {
		t.Fatalf("seed push published %d packs, want 1", len(before))
	}

	// Delete the only ref, then re-create it: the client re-sends the same
	// content-addressed pack (nothing references its objects anymore).
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "--delete", "refs/heads/scratch")
	time.Sleep(100 * time.Millisecond) // make an overwrite's fresh mtime detectable
	out := mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/scratch")
	t.Logf("re-create push output:\n%s", out)

	after := s3Packs(t, repo)
	if len(after) != 1 {
		t.Fatalf("re-push left %d pack objects, want 1", len(after))
	}
	if after[0].Key != before[0].Key {
		t.Fatalf("pack key changed: %q -> %q (expected identical content-addressed key)", before[0].Key, after[0].Key)
	}
	if !after[0].ModTime.Equal(before[0].ModTime) {
		t.Fatalf("pack was re-uploaded: mtime %v -> %v", before[0].ModTime, after[0].ModTime)
	}
	if got, want := pgRefs(t, repo)["refs/heads/scratch"], revParse(t, work, "HEAD"); got != want {
		t.Fatalf("scratch = %q, want %q", got, want)
	}
}

// TestPackQuota_NamespaceSharesQuota (Codex round 15): pushes through the
// `+ephemeral` namespace count against the BASE repository's quota — auth
// strips the suffix, so per-namespace quotas would hand one token 3x the
// advertised per-repository bound.
func TestPackQuota_NamespaceSharesQuota(t *testing.T) {
	repo := registerRepo(t, "quota-namespace")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")

	// Fill the base repo through nodeA (generous quota).
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")
	if objs := s3Packs(t, repo); len(objs) == 0 {
		t.Fatal("seed push stored no packs")
	}

	// A 1-byte-quota node must deny a namespace push: base usage already
	// exceeds the shared quota, and the ephemeral prefix being empty must
	// not grant a fresh allowance.
	node := startNodeT(t, "OPENSTORAGE_PACK_QUOTA_BYTES=1", "OPENSTORAGE_MAX_PUSH_BYTES=0") // cap off, see TestPackQuota_PreReceiveDeniesAndStoresNothing
	commitFile(t, work, "ns.txt", "ns\n", "ns", nil)
	out, err := tryGit(work, "push", remoteURL(node, repo+"+ephemeral", token), "HEAD:refs/heads/scratch")
	if err == nil {
		t.Fatalf("namespace push succeeded past the base repo's quota:\n%s", out)
	}
	if !strings.Contains(out, "pack quota exceeded") {
		t.Fatalf("namespace push failure does not name the quota:\n%s", out)
	}
	t.Logf("namespace over-quota push output:\n%s", out)
	if objs, err := testBlob.List(context.Background(), blob.PackPrefix(repo+"+ephemeral")); err != nil || len(objs) != 0 {
		t.Fatalf("denied namespace push left packs: %v (err %v)", objs, err)
	}
}

// TestPackQuota_UnderThresholdPasses: with a quota comfortably above the
// repo's footprint, pushes pass through the pre-receive gate untouched —
// including a second push on non-zero existing usage.
func TestPackQuota_UnderThresholdPasses(t *testing.T) {
	node := startNodeT(t, "OPENSTORAGE_PACK_QUOTA_BYTES=10485760") // 10 MiB
	repo := registerRepo(t, "quota-under")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")

	mustGit(t, work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/main")
	commitFile(t, work, "b.txt", "two\n", "two", nil)
	mustGit(t, work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/main")

	head := revParse(t, work, "HEAD")
	if refs := pgRefs(t, repo); refs["refs/heads/main"] != head {
		t.Fatalf("refs/heads/main = %q, want %q", refs["refs/heads/main"], head)
	}
}
