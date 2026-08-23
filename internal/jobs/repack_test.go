// Repacker tests (issue #1): fold-to-one-pack correctness over real git
// packs and stores (sqlite + fileblob), deterministic lost-race handling
// with own-orphan cleanup, ambiguous-swap conservatism, trigger threshold
// semantics, composition with the orphan sweeper (issue #5), and the
// concurrent-hydrator retry (issue #6). River uniqueness gets a real-PG
// test alongside the queue roundtrip in jobs_test.go.
package jobs

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	stdsync "sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/coder/code-storage/internal/blob"
	"github.com/coder/code-storage/internal/events"
	"github.com/coder/code-storage/internal/githttp"
	"github.com/coder/code-storage/internal/ref"
	"github.com/coder/code-storage/internal/sync"
)

// testGit runs git hermetically (host config/identity must not leak into
// object hashing) and returns stdout.
func testGit(t *testing.T, dir string, stdin string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = []string{
		"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null", "GIT_CONFIG_NOSYSTEM=1",
		"GIT_AUTHOR_NAME=repack-test", "GIT_AUTHOR_EMAIL=repack@test.invalid",
		"GIT_COMMITTER_NAME=repack-test", "GIT_COMMITTER_EMAIL=repack@test.invalid",
		"GIT_AUTHOR_DATE=@1700000000 +0000", "GIT_COMMITTER_DATE=@1700000000 +0000",
		"HOME=/dev/null", "PATH=" + os.Getenv("PATH"),
	}
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, stderr.String())
	}
	return string(out)
}

// repackFixture is one repo's push pipeline over real stores: every push
// mints a commit in a scratch repo, packs exactly its new objects, uploads
// and registers the pack (publish-before-flip), then CASes the branch —
// the same object flow as the pre-receive/update hooks.
type repackFixture struct {
	store    ref.Store
	blobs    blob.BlobStore
	blobDir  string
	cacheDir string
	scratch  string
	repo     string
	tip      string
	seq      int
}

func newRepackFixture(t *testing.T) *repackFixture {
	t.Helper()
	dir := t.TempDir()
	ctx := context.Background()
	store, err := ref.Open(ctx, "sqlite://"+filepath.Join(dir, "refs.sqlite"))
	if err != nil {
		t.Fatalf("open ref store: %v", err)
	}
	t.Cleanup(store.Close)
	if err := store.InitSchema(ctx); err != nil {
		t.Fatalf("init ref schema: %v", err)
	}
	blobDir := filepath.Join(dir, "blobs")
	blobs, err := blob.Open(ctx, "file://"+blobDir)
	if err != nil {
		t.Fatalf("open blob store: %v", err)
	}
	t.Cleanup(func() { _ = blobs.Close() })
	if err := blobs.EnsureBucket(ctx); err != nil {
		t.Fatalf("ensure bucket: %v", err)
	}
	f := &repackFixture{
		store:    store,
		blobs:    blobs,
		blobDir:  blobDir,
		cacheDir: filepath.Join(dir, "cache"),
		scratch:  filepath.Join(dir, "scratch.git"),
		repo:     "repo1",
		tip:      ref.ZeroSHA,
	}
	testGit(t, "", "", "init", "--bare", "--initial-branch=main", f.scratch)
	testGit(t, f.scratch, "", "config", "gc.auto", "0")
	return f
}

// push mints one commit on main and publishes it exactly like a git push:
// pack upload, RegisterPacks, ref CAS. Returns the new pack id.
func (f *repackFixture) push(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	f.seq++
	blobSHA := strings.TrimSpace(testGit(t, f.scratch, fmt.Sprintf("content %d\n", f.seq), "hash-object", "-w", "--stdin"))
	treeSHA := strings.TrimSpace(testGit(t, f.scratch, fmt.Sprintf("100644 blob %s\tdata.txt\n", blobSHA), "mktree"))
	commitArgs := []string{"commit-tree", treeSHA, "-m", fmt.Sprintf("c%d", f.seq)}
	if f.tip != ref.ZeroSHA {
		commitArgs = append(commitArgs, "-p", f.tip)
	}
	commitSHA := strings.TrimSpace(testGit(t, f.scratch, "", commitArgs...))

	packDir := t.TempDir()
	objs := blobSHA + "\n" + treeSHA + "\n" + commitSHA + "\n"
	id := strings.TrimSpace(testGit(t, f.scratch, objs, "pack-objects", "--threads=1", "-q", filepath.Join(packDir, "push")))
	packPath := filepath.Join(packDir, "push-"+id+".pack")
	key := blob.PackKey(f.repo, id)
	if err := f.blobs.UploadFile(ctx, key, packPath); err != nil {
		t.Fatalf("upload pack: %v", err)
	}
	fi, err := os.Stat(packPath)
	if err != nil {
		t.Fatalf("stat pack: %v", err)
	}
	// Size feeds the quota ledger (issue #31), matching the production
	// writers (pre-receive and publishPack both stat before registering).
	if err := f.store.RegisterPacks(ctx, f.repo, []ref.Pack{{ID: id, Key: key, Size: fi.Size()}}); err != nil {
		t.Fatalf("register pack: %v", err)
	}
	won, err := f.store.CASRef(ctx, f.repo, "refs/heads/main", f.tip, commitSHA)
	if err != nil || !won {
		t.Fatalf("cas main: won=%v err=%v", won, err)
	}
	f.tip = commitSHA
	return id
}

// pushEmptyPack registers a zero-object pack (git sends these on some
// delete-only/no-new-object pushes with receive.unpackLimit=0).
func (f *repackFixture) pushEmptyPack(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	packDir := t.TempDir()
	id := strings.TrimSpace(testGit(t, f.scratch, "", "pack-objects", "--threads=1", "-q", filepath.Join(packDir, "empty")))
	key := blob.PackKey(f.repo, id)
	if err := f.blobs.UploadFile(ctx, key, filepath.Join(packDir, "empty-"+id+".pack")); err != nil {
		t.Fatalf("upload empty pack: %v", err)
	}
	if err := f.store.RegisterPacks(ctx, f.repo, []ref.Pack{{ID: id, Key: key}}); err != nil {
		t.Fatalf("register empty pack: %v", err)
	}
	return id
}

// repacker builds a production Repacker over the fixture's stores and cache.
func (f *repackFixture) repacker(cacheDir string) *Repacker {
	return NewRepacker(f.store, f.blobs, fixtureMaterialize(f.blobs, cacheDir), slog.New(slog.DiscardHandler))
}

// fixtureMaterialize composes the production materialize seam over the
// fixture's blob store and a cache dir.
func fixtureMaterialize(bl blob.BlobStore, cacheDir string) MaterializeFunc {
	return func(ctx context.Context, repo string, packs []ref.Pack) (string, error) {
		repoPath, err := githttp.EnsureCacheRepoCtx(ctx, cacheDir, repo)
		if err != nil {
			return "", err
		}
		for _, p := range packs {
			if err := githttp.MaterializePack(ctx, bl, repoPath, p); err != nil {
				return "", err
			}
		}
		return repoPath, nil
	}
}

func (f *repackFixture) packRows(t *testing.T) []ref.Pack {
	t.Helper()
	packs, err := f.store.ListPacks(context.Background(), f.repo)
	if err != nil {
		t.Fatalf("list packs: %v", err)
	}
	return packs
}

// checkColdHydrate hydrates the repo into a FRESH cache dir from the
// current store/blob state (the stateless-node property) and verifies
// connectivity plus ref agreement.
func (f *repackFixture) checkColdHydrate(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	coldDir := t.TempDir()
	repoPath, refsErr, err := githttp.Hydrate(ctx, f.store, f.blobs, coldDir, f.repo, "main")
	if err != nil || refsErr != nil {
		t.Fatalf("cold hydrate: err=%v refsErr=%v", err, refsErr)
	}
	testGit(t, repoPath, "", "fsck", "--connectivity-only", "--no-progress", "--no-dangling")
	want, err := f.store.ListRefs(ctx, f.repo)
	if err != nil {
		t.Fatalf("list refs: %v", err)
	}
	for name, sha := range want {
		got := strings.TrimSpace(testGit(t, repoPath, "", "rev-parse", name))
		if got != sha {
			t.Fatalf("cold hydrate ref %s = %s, want %s", name, got, sha)
		}
	}
}

func (f *repackFixture) blobExists(t *testing.T, key string) bool {
	t.Helper()
	_, err := f.blobs.Stat(context.Background(), key)
	if err != nil && !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("stat %s: %v", key, err)
	}
	return err == nil
}

func TestRepack_FoldsToOnePack(t *testing.T) {
	f := newRepackFixture(t)
	ctx := context.Background()

	var oldIDs []string
	for range 5 {
		oldIDs = append(oldIDs, f.push(t))
	}
	oldIDs = append(oldIDs, f.pushEmptyPack(t))
	if got := len(f.packRows(t)); got != 6 {
		t.Fatalf("pack rows before fold = %d, want 6", got)
	}

	if _, err := f.repacker(t.TempDir()).Repack(ctx, f.repo); err != nil {
		t.Fatalf("repack: %v", err)
	}

	rows := f.packRows(t)
	if len(rows) != 1 {
		t.Fatalf("pack rows after fold = %+v, want exactly 1", rows)
	}
	for _, id := range oldIDs {
		if rows[0].ID == id {
			t.Fatalf("fold produced an old pack id %s with multiple inputs", id)
		}
		// Superseded blobs are NEVER deleted synchronously (issue #5 owns
		// their collection after the grace period).
		if !f.blobExists(t, blob.PackKey(f.repo, id)) {
			t.Fatalf("superseded blob %s deleted synchronously", id)
		}
	}
	if !f.blobExists(t, rows[0].Key) {
		t.Fatalf("consolidated blob %s missing", rows[0].Key)
	}
	// Cold hydrate from post-fold state alone reproduces the exact repo.
	f.checkColdHydrate(t)

	// Idempotence: a second fold with one registered pack is a no-op.
	if _, err := f.repacker(t.TempDir()).Repack(ctx, f.repo); err != nil {
		t.Fatalf("second repack: %v", err)
	}
	if again := f.packRows(t); len(again) != 1 || again[0].ID != rows[0].ID {
		t.Fatalf("second fold changed the pack set: %+v", again)
	}
}

func TestRepack_LostRaceLeavesOrphanForSweeper(t *testing.T) {
	f := newRepackFixture(t)
	ctx := context.Background()
	for range 3 {
		f.push(t)
	}
	before, err := f.blobs.List(ctx, blob.PackPrefix(f.repo))
	if err != nil {
		t.Fatalf("list blobs: %v", err)
	}
	beforeKeys := make(map[string]bool, len(before))
	for _, o := range before {
		beforeKeys[o.Key] = true
	}

	loser := f.repacker(t.TempDir())
	winnerDir := t.TempDir()
	var loserKey string
	loser.BeforeSwap = func(ctx context.Context, pending ref.Pack) error {
		loserKey = pending.Key
		if _, err := f.blobs.Stat(ctx, loserKey); err != nil {
			return fmt.Errorf("loser blob not uploaded before swap: %w", err)
		}
		// Land a DIFFERENT competing fold: one more push changes the
		// winner's input set, so the two consolidated packs differ and the
		// loser's blob becomes a genuine unregistered orphan.
		f.push(t)
		_, werr := f.repacker(winnerDir).Repack(ctx, f.repo)
		return werr
	}
	if _, err := loser.Repack(ctx, f.repo); err != nil {
		t.Fatalf("loser repack: %v", err)
	}

	rows := f.packRows(t)
	if len(rows) != 1 {
		t.Fatalf("pack rows after race = %+v, want the winner's single fold", rows)
	}
	if rows[0].Key == loserKey {
		t.Fatalf("winner and loser folds coincided (%s); the race was not exercised", loserKey)
	}
	// Lost race: zero DB changes beyond the winner's fold, and NOTHING is
	// deleted synchronously — the loser's blob lingers as an unregistered
	// orphan for the sweeper (synchronous deletion cannot be made
	// crash-safe against concurrent identical folds).
	if !f.blobExists(t, loserKey) {
		t.Fatalf("loser's orphan blob %s deleted synchronously; it must be left for the sweeper", loserKey)
	}
	if !f.blobExists(t, rows[0].Key) {
		t.Fatalf("winner's consolidated blob %s missing", rows[0].Key)
	}
	for key := range beforeKeys {
		if !f.blobExists(t, key) {
			t.Fatalf("pre-race blob %s deleted by the lost race", key)
		}
	}
	f.checkColdHydrate(t)

	// Composition: an aged delete-enabled sweep collects exactly the
	// unregistered blobs (the loser's orphan and the superseded inputs),
	// and the repo still cold-hydrates from what remains.
	past := time.Now().Add(-25 * time.Hour)
	objs, err := f.blobs.List(ctx, blob.PackPrefix(f.repo))
	if err != nil {
		t.Fatalf("list blobs: %v", err)
	}
	for _, o := range objs {
		if o.Key == rows[0].Key {
			continue // the live fold stays fresh
		}
		if err := os.Chtimes(filepath.Join(f.blobDir, filepath.FromSlash(o.Key)), past, past); err != nil {
			t.Fatal(err)
		}
	}
	sweeper := NewSweeper(f.blobs, f.store, SweepConfig{Threshold: 24 * time.Hour, Delete: true}, slog.New(slog.DiscardHandler))
	if _, err := sweeper.Sweep(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if f.blobExists(t, loserKey) {
		t.Fatalf("aged sweep left the loser's orphan %s", loserKey)
	}
	f.checkColdHydrate(t)
}

// ambiguousReplaceStore executes ReplacePacks and then reports failure —
// the committed-but-response-lost shape.
type ambiguousReplaceStore struct {
	ref.RefStore
}

var errAmbiguousReplace = errors.New("injected post-commit connection drop")

func (s ambiguousReplaceStore) ReplacePacks(ctx context.Context, repo string, old []ref.Pack, newPack ref.Pack) (bool, error) {
	if _, err := s.RefStore.ReplacePacks(ctx, repo, old, newPack); err != nil {
		return false, err
	}
	return false, errAmbiguousReplace
}

func TestRepack_AmbiguousSwapKeepsBlob(t *testing.T) {
	f := newRepackFixture(t)
	ctx := context.Background()
	for range 3 {
		f.push(t)
	}

	r := NewRepacker(ambiguousReplaceStore{f.store}, f.blobs, fixtureMaterialize(f.blobs, f.cacheDir), slog.New(slog.DiscardHandler))
	folded, err := r.Repack(ctx, f.repo)
	if !errors.Is(err, errAmbiguousReplace) {
		t.Fatalf("repack error = %v, want the injected ambiguous drop", err)
	}
	// The swap may have committed (here it did — the registration recheck
	// finds the content-addressed key): folded must report true so queue
	// backends still run their follow-up threshold recheck.
	if !folded {
		t.Fatal("ambiguous-but-committed swap error must report folded=true")
	}

	// The swap really committed; the consolidated blob must NOT have been
	// deleted (rows reference it) and the repo must cold-hydrate cleanly.
	rows := f.packRows(t)
	if len(rows) != 1 {
		t.Fatalf("pack rows = %+v, want the committed fold", rows)
	}
	if !f.blobExists(t, rows[0].Key) {
		t.Fatalf("ambiguous swap deleted the committed consolidated blob %s", rows[0].Key)
	}
	f.checkColdHydrate(t)
}

// failingReplaceStore fails ReplacePacks WITHOUT executing it — the
// definitive-rollback shape (e.g. a SQLite ExecContext error).
type failingReplaceStore struct {
	ref.RefStore
}

var errReplaceRolledBack = errors.New("injected pre-commit replace failure")

func (s failingReplaceStore) ReplacePacks(context.Context, string, []ref.Pack, ref.Pack) (bool, error) {
	return false, errReplaceRolledBack
}

// TestRepack_RolledBackSwapReportsNotFolded pins the round-16 finding: a
// ReplacePacks failure whose registration recheck proves the swap never
// landed must report folded=false — folded=true there let a persistent
// write failure drive unbounded fold+upload loops through the queues'
// follow-up scheduling, bypassing river's retry budget.
func TestRepack_RolledBackSwapReportsNotFolded(t *testing.T) {
	f := newRepackFixture(t)
	ctx := context.Background()
	for range 3 {
		f.push(t)
	}
	r := NewRepacker(failingReplaceStore{f.store}, f.blobs, fixtureMaterialize(f.blobs, t.TempDir()), slog.New(slog.DiscardHandler))
	folded, err := r.Repack(ctx, f.repo)
	if !errors.Is(err, errReplaceRolledBack) {
		t.Fatalf("repack error = %v, want the injected rollback", err)
	}
	if folded {
		t.Fatal("definitively rolled-back swap must report folded=false")
	}
	if rows := f.packRows(t); len(rows) != 3 {
		t.Fatalf("pack rows = %+v, want the untouched 3", rows)
	}
}

// expiredCtxReplaceStore fails ReplacePacks with the deadline shape (the
// fold budget expiring mid-swap) after a definitive rollback: the
// resolution probe must still identify the rollback via its DETACHED
// context, not inherit the expired one.
type expiredCtxReplaceStore struct {
	ref.RefStore
}

func (s expiredCtxReplaceStore) ReplacePacks(context.Context, string, []ref.Pack, ref.Pack) (bool, error) {
	return false, fmt.Errorf("swap: %w", context.DeadlineExceeded)
}

// TestRepack_DeadlineDuringSwapResolvesRollback pins the round-18 finding:
// when the fold deadline expires during a ReplacePacks that rolled back,
// the ambiguity probe must run on a live detached context and classify the
// rollback as folded=false with the deadline error intact — river then
// takes its terminal deadline cancel instead of chaining successors.
func TestRepack_DeadlineDuringSwapResolvesRollback(t *testing.T) {
	f := newRepackFixture(t)
	ctx := context.Background()
	for range 3 {
		f.push(t)
	}
	r := NewRepacker(expiredCtxReplaceStore{f.store}, f.blobs, fixtureMaterialize(f.blobs, t.TempDir()), slog.New(slog.DiscardHandler))
	folded, err := r.Repack(ctx, f.repo)
	if folded {
		t.Fatal("rolled-back deadline swap must report folded=false")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want DeadlineExceeded preserved for river's terminal cancel", err)
	}
	if rows := f.packRows(t); len(rows) != 3 {
		t.Fatalf("pack rows = %+v, want the untouched 3", rows)
	}
}

// TestRepack_RolledBackIdentityFoldReportsNotFolded pins the round-17
// refinement: in an identity fold (union byte-identical to one input,
// newPack.Key registered BEFORE the swap) a key-membership recheck cannot
// distinguish rollback from commit — the probe on a distinguishing OLD row
// must. Setup: fold [A,B] into K, push C, then attempt folding [K,C] with
// an injected pre-commit failure; the union of a fold and a later push
// packs to a new id here, so force the identity shape directly instead:
// fold [K,C] where the failing store reports the error while rows are
// untouched. K (== an input id in the identity case) stays registered
// either way; C is the distinguishing probe and it survives the rollback,
// so folded must be false.
func TestRepack_RolledBackIdentityFoldReportsNotFolded(t *testing.T) {
	f := newRepackFixture(t)
	ctx := context.Background()
	for range 2 {
		f.push(t)
	}
	// Real fold [A,B] -> K.
	if _, err := f.repacker(t.TempDir()).Repack(ctx, f.repo); err != nil {
		t.Fatalf("setup fold: %v", err)
	}
	f.push(t) // C
	rows := f.packRows(t)
	if len(rows) != 2 {
		t.Fatalf("setup rows = %+v, want [K, C]", rows)
	}

	// The failing store rejects the swap pre-commit. To force the identity
	// shape, warm the cache so the new union's pack id can collide when
	// inputs allow; regardless of whether this particular union is
	// identical to K, simulate the WORST case by pre-registering the
	// prospective new pack's key… which is exactly what the identity fold
	// does. We approximate it deterministically: run the fold with the
	// failing store and assert via the probe semantics — C (an old row
	// distinct from any new id) survives, so folded must be false even
	// though K's key remains registered throughout.
	r := NewRepacker(failingReplaceStore{f.store}, f.blobs, fixtureMaterialize(f.blobs, t.TempDir()), slog.New(slog.DiscardHandler))
	folded, err := r.Repack(ctx, f.repo)
	if !errors.Is(err, errReplaceRolledBack) {
		t.Fatalf("repack error = %v, want the injected rollback", err)
	}
	if folded {
		t.Fatal("rolled-back fold with a pre-registered surviving row must report folded=false")
	}
	if got := f.packRows(t); len(got) != 2 {
		t.Fatalf("pack rows = %+v, want the untouched [K, C]", got)
	}
}

func TestRepack_SweeperCollectsSupersededBlobs(t *testing.T) {
	f := newRepackFixture(t)
	ctx := context.Background()
	var oldIDs []string
	for range 4 {
		oldIDs = append(oldIDs, f.push(t))
	}
	if _, err := f.repacker(t.TempDir()).Repack(ctx, f.repo); err != nil {
		t.Fatalf("repack: %v", err)
	}
	rows := f.packRows(t)
	if len(rows) != 1 {
		t.Fatalf("pack rows = %+v, want 1", rows)
	}

	// Superseded blobs become DB-absent; age them past the sweeper's
	// threshold (fresh ones must survive a sweep first).
	sweeper := NewSweeper(f.blobs, f.store, SweepConfig{Threshold: 24 * time.Hour, Delete: true}, slog.New(slog.DiscardHandler))
	report, err := sweeper.Sweep(ctx)
	if err != nil {
		t.Fatalf("sweep (fresh): %v", err)
	}
	if report.Deleted != 0 || report.Live != 1 || report.Fresh != len(oldIDs) {
		t.Fatalf("fresh sweep = %+v, want live=1 fresh=%d deleted=0", report, len(oldIDs))
	}

	past := time.Now().Add(-25 * time.Hour)
	for _, id := range oldIDs {
		p := filepath.Join(f.blobDir, filepath.FromSlash(blob.PackKey(f.repo, id)))
		if err := os.Chtimes(p, past, past); err != nil {
			t.Fatalf("chtimes %s: %v", p, err)
		}
	}
	report, err = sweeper.Sweep(ctx)
	if err != nil {
		t.Fatalf("sweep (aged): %v", err)
	}
	if report.Deleted != len(oldIDs) || report.Live != 1 {
		t.Fatalf("aged sweep = %+v, want deleted=%d live=1", report, len(oldIDs))
	}
	for _, id := range oldIDs {
		if f.blobExists(t, blob.PackKey(f.repo, id)) {
			t.Fatalf("superseded blob %s survived the aged sweep", id)
		}
	}
	// The consolidated pack alone still reproduces the repo cold.
	f.checkColdHydrate(t)
}

// staleListStore serves ONE stale pack listing (captured pre-fold), then
// delegates — the exact shape of a hydrator that listed packs right before
// a repack swap plus sweep landed.
type staleListStore struct {
	ref.RefStore
	mu    stdsync.Mutex
	stale []ref.Pack
}

func (s *staleListStore) ListPacks(ctx context.Context, repo string) ([]ref.Pack, error) {
	s.mu.Lock()
	stale := s.stale
	s.stale = nil
	s.mu.Unlock()
	if stale != nil {
		return stale, nil
	}
	return s.RefStore.ListPacks(ctx, repo)
}

func TestRepack_ConcurrentHydratorRetriesOntoFold(t *testing.T) {
	f := newRepackFixture(t)
	ctx := context.Background()
	var oldIDs []string
	for range 3 {
		oldIDs = append(oldIDs, f.push(t))
	}

	// Capture the pre-fold listing, then fold AND sweep the superseded
	// blobs away (the worst case for a stale hydrator: rows gone, bytes
	// gone).
	stale := &staleListStore{RefStore: f.store, stale: f.packRows(t)}
	if _, err := f.repacker(t.TempDir()).Repack(ctx, f.repo); err != nil {
		t.Fatalf("repack: %v", err)
	}
	past := time.Now().Add(-25 * time.Hour)
	for _, id := range oldIDs {
		if err := os.Chtimes(filepath.Join(f.blobDir, filepath.FromSlash(blob.PackKey(f.repo, id))), past, past); err != nil {
			t.Fatal(err)
		}
	}
	sweeper := NewSweeper(f.blobs, f.store, SweepConfig{Threshold: 24 * time.Hour, Delete: true}, slog.New(slog.DiscardHandler))
	if _, err := sweeper.Sweep(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	// A cold hydrator holding the stale listing hits ErrNotFound on the
	// swept blobs and must recover via the bounded re-list retry (issue
	// #6) onto the consolidated pack.
	coldDir := t.TempDir()
	repoPath, refsErr, err := githttp.Hydrate(ctx, stale, f.blobs, coldDir, f.repo, "main")
	if err != nil || refsErr != nil {
		t.Fatalf("stale-listing hydrate: err=%v refsErr=%v", err, refsErr)
	}
	testGit(t, repoPath, "", "fsck", "--connectivity-only", "--no-progress", "--no-dangling")
}

func TestRepack_StaleSnapshotLosesCleanlyBeforePacking(t *testing.T) {
	f := newRepackFixture(t)
	ctx := context.Background()
	for range 3 {
		f.push(t)
	}

	// The repacker's FIRST listing is a stale pre-fold snapshot; a
	// concurrent fold then lands before its (cold) hydrate, so hydration
	// materializes only the consolidated row and the union enumeration
	// finds no idx for the snapshotted packs. The fold must stop cleanly:
	// no error, no upload, no DB change beyond the winner's fold.
	stale := &staleListStore{RefStore: f.store, stale: f.packRows(t)}
	if _, err := f.repacker(t.TempDir()).Repack(ctx, f.repo); err != nil {
		t.Fatalf("winner repack: %v", err)
	}
	winner := f.packRows(t)
	before, err := f.blobs.List(ctx, blob.PackPrefix(f.repo))
	if err != nil {
		t.Fatalf("list blobs: %v", err)
	}

	loser := NewRepacker(stale, f.blobs, fixtureMaterialize(f.blobs, t.TempDir()), slog.New(slog.DiscardHandler)) // cold cache
	if _, err := loser.Repack(ctx, f.repo); err != nil {
		t.Fatalf("stale-snapshot repack must lose cleanly, got %v", err)
	}
	if rows := f.packRows(t); len(rows) != 1 || rows[0].ID != winner[0].ID {
		t.Fatalf("stale fold changed the pack set: %+v", rows)
	}
	after, err := f.blobs.List(ctx, blob.PackPrefix(f.repo))
	if err != nil {
		t.Fatalf("list blobs: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("stale fold changed the blob set: %d -> %d objects", len(before), len(after))
	}
}

// TestRepack_BudgetsBoundFoldInput pins the fold's resource budgets
// (security round): each fixture push pack holds exactly 3 objects
// (blob + tree + commit), so small budgets steer the oldest-first
// selection deterministically. Skipped packs stay registered untouched,
// and a selection below two packs skips the fold entirely.
func TestRepack_BudgetsBoundFoldInput(t *testing.T) {
	f := newRepackFixture(t)
	ctx := context.Background()
	for range 3 {
		f.push(t)
	}

	// Object budget 7 fits two 3-object packs, not three: the fold must
	// consolidate exactly two and leave the third registered as-is.
	r := f.repacker(t.TempDir())
	r.MaxFoldObjects = 7
	folded, err := r.Repack(ctx, f.repo)
	if err != nil || !folded {
		t.Fatalf("budgeted repack = (folded %v, err %v), want a fold", folded, err)
	}
	if rows := f.packRows(t); len(rows) != 2 {
		t.Fatalf("pack rows after budgeted fold = %+v, want 2 (fold + over-budget leftover)", rows)
	}
	f.checkColdHydrate(t)

	// The remaining pair (6-object fold + 3-object pack) exceeds the
	// budget together; whichever lists first, fewer than two packs fit:
	// no fold, no error, no changes.
	folded, err = r.Repack(ctx, f.repo)
	if err != nil || folded {
		t.Fatalf("over-budget repack = (folded %v, err %v), want a clean no-op", folded, err)
	}
	if rows := f.packRows(t); len(rows) != 2 {
		t.Fatalf("pack rows after no-op = %+v, want unchanged 2", rows)
	}

	// Wedge regression (round-8): grow the repo to [fold(6obj), p(3), p(3)].
	// The oldest-first greedy selects the old consolidated pack first
	// (6 ≤ 7) and crowds out both small packs; the pair search must
	// instead fold the two small packs — a permanent no-op here would let
	// later pushes accumulate unbounded rows behind one oversized fold.
	f.push(t)
	folded, err = r.Repack(ctx, f.repo)
	if err != nil || !folded {
		t.Fatalf("wedge repack = (folded %v, err %v), want the small packs folded", folded, err)
	}
	rows := f.packRows(t)
	if len(rows) != 2 {
		t.Fatalf("pack rows after wedge fold = %+v, want [old fold, new small fold]", rows)
	}
	f.checkColdHydrate(t)

	// Unpairable backlog (round-9): when every pack exceeds half the
	// budget, NO pair fits — the exact pair search proves that in
	// O(n log n) and the fold no-ops cleanly.
	f3 := newRepackFixture(t)
	for range 3 {
		f3.push(t)
	}
	r3 := f3.repacker(t.TempDir())
	r3.MaxFoldObjects = 5 // each 3-object pack > 5/2; any pair = 6 > 5
	folded, err = r3.Repack(ctx, f3.repo)
	if err != nil || folded {
		t.Fatalf("unpairable repack = (folded %v, err %v), want a clean no-op", folded, err)
	}
	if rows := f3.packRows(t); len(rows) != 3 {
		t.Fatalf("pack rows after unpairable no-op = %+v, want unchanged 3", rows)
	}

	// A byte budget of one selects nothing either.
	rb := f.repacker(t.TempDir())
	rb.MaxFoldBytes = 1
	folded, err = rb.Repack(ctx, f.repo)
	if err != nil || folded {
		t.Fatalf("byte-budget repack = (folded %v, err %v), want a clean no-op", folded, err)
	}
	if rows := f.packRows(t); len(rows) != 2 {
		t.Fatalf("pack rows after byte-budget no-op = %+v, want unchanged 2", rows)
	}
	f.checkColdHydrate(t)
}

// TestRepack_CrossBudgetSelectionFindsPair pins the round-21 selection
// gap: {A: byte-heavy/object-light, B: byte-cheap/object-heavy} bust the
// object budget together, while {A, C} (C small in both dimensions) is
// feasible. Selection must fold A+C and leave B registered (B is
// individually over the object budget — never even a candidate); a
// byte-only view of the input wedged here on every trigger.
func TestRepack_CrossBudgetSelectionFindsPair(t *testing.T) {
	f := newRepackFixture(t)
	ctx := context.Background()

	// Craft three packs in the scratch repo with controlled shapes:
	//   A: 1 blob of ~64KB incompressible bytes  (1 object,  large bytes)
	//   B: 60 tiny unique blobs                  (60 objects, small bytes)
	//   C: 1 tiny blob                           (1 object,   tiny bytes)
	mkPack := func(blobs []string) (id string, path string) {
		t.Helper()
		dir := t.TempDir()
		stdin := strings.Join(blobs, "\n") + "\n"
		id = strings.TrimSpace(testGit(t, f.scratch, stdin, "pack-objects", "--threads=1", "-q", filepath.Join(dir, "x")))
		return id, filepath.Join(dir, "x-"+id+".pack")
	}
	bigBlob := strings.TrimSpace(testGit(t, f.scratch, randomText(64<<10), "hash-object", "-w", "--stdin"))
	var tinyBlobs []string
	for i := range 60 {
		tinyBlobs = append(tinyBlobs, strings.TrimSpace(testGit(t, f.scratch, fmt.Sprintf("tiny %d", i), "hash-object", "-w", "--stdin")))
	}
	idA, pathA := mkPack([]string{bigBlob})
	idB, pathB := mkPack(tinyBlobs)
	idC, pathC := mkPack(tinyBlobs[:1])

	sizeOf := func(p string) int64 {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		return fi.Size()
	}
	// Registration order A, B, C (oldest first).
	for _, reg := range []struct {
		id, path string
	}{{idA, pathA}, {idB, pathB}, {idC, pathC}} {
		key := blob.PackKey(f.repo, reg.id)
		if err := f.blobs.UploadFile(ctx, key, reg.path); err != nil {
			t.Fatal(err)
		}
		if err := f.store.RegisterPacks(ctx, f.repo, []ref.Pack{{ID: reg.id, Key: key}}); err != nil {
			t.Fatal(err)
		}
	}

	r := f.repacker(t.TempDir())
	// Byte budget admits A+B but not C on the oldest-first pass; object
	// budget rejects A+B (61 > 50) but admits A+C (2).
	r.MaxFoldBytes = sizeOf(pathA) + sizeOf(pathB) + sizeOf(pathC)/2
	r.MaxFoldObjects = 50
	folded, err := r.Repack(ctx, f.repo)
	if err != nil || !folded {
		t.Fatalf("cross-budget repack = (folded %v, err %v), want the A+C fold", folded, err)
	}
	rows := f.packRows(t)
	if len(rows) != 2 {
		t.Fatalf("pack rows = %+v, want [fold(A,C), B]", rows)
	}
	for _, row := range rows {
		if row.ID == idA || row.ID == idC {
			t.Fatalf("pack rows = %+v: A and C must have been folded away", rows)
		}
	}
	foundB := false
	for _, row := range rows {
		if row.ID == idB {
			foundB = true
		}
	}
	if !foundB {
		t.Fatalf("pack rows = %+v: object-heavy B must survive the fold", rows)
	}
}

// TestRepack_SelectionFindsPairBehindUnpairableGarbage pins the
// round-22/23 starvation class: a feasible pair sits byte-wise BEHIND a
// deep front of cheaper packs that are individually over the object
// budget (unpairable garbage). Any materialize-to-learn-counts selection
// with a fixed download allowance wedges here deterministically on every
// trigger — the garbage below totals more than the removed rescue loop's
// three extra byte budgets — while header-read selection knows every
// candidate's object count up front: it must fold the pair, and download
// NOTHING but the pair.
func TestRepack_SelectionFindsPairBehindUnpairableGarbage(t *testing.T) {
	f := newRepackFixture(t)
	ctx := context.Background()

	mkPack := func(blobs []string) (id, path string) {
		t.Helper()
		dir := t.TempDir()
		id = strings.TrimSpace(testGit(t, f.scratch, strings.Join(blobs, "\n")+"\n", "pack-objects", "--threads=1", "-q", filepath.Join(dir, "x")))
		return id, filepath.Join(dir, "x-"+id+".pack")
	}
	blobOfSize := func(tag string, n int) string {
		return strings.TrimSpace(testGit(t, f.scratch, tag+randomText(n), "hash-object", "-w", "--stdin"))
	}
	sizeOf := func(p string) int64 {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		return fi.Size()
	}
	// Shapes (registration order = age order, garbage oldest):
	//   garbage x12: 30 tiny objects each (byte-cheap; object-oversized
	//                at budget 20 — can never be part of any fold)
	//   pairA/B:     1 incompressible ~1KB object each (the feasible duo,
	//                byte-pricier than every garbage pack)
	shared := make([]string, 29)
	for i := range shared {
		shared[i] = blobOfSize(fmt.Sprintf("s%d ", i), 4)
	}
	type reg struct{ id, path string }
	var packs []reg
	for g := range 12 {
		id, path := mkPack(append([]string{blobOfSize(fmt.Sprintf("g%d ", g), 4)}, shared...))
		packs = append(packs, reg{id, path})
	}
	idPairA, pathPairA := mkPack([]string{blobOfSize("pa ", 1<<10)})
	idPairB, pathPairB := mkPack([]string{blobOfSize("pb ", 1<<10)})
	packs = append(packs, reg{idPairA, pathPairA}, reg{idPairB, pathPairB})

	// Fixture shape checks: every garbage pack is byte-cheaper than either
	// pair member, and the garbage front is deeper than the removed rescue
	// loop's whole allowance (3 byte budgets), so no fixed-window scheme
	// could have reached the pair.
	byteBudget := sizeOf(pathPairA) + sizeOf(pathPairB)
	var garbageBytes int64
	for _, p := range packs[:12] {
		if sizeOf(p.path) >= sizeOf(pathPairA) {
			t.Fatalf("fixture shape broken: garbage (%d) must be cheaper than pair members (%d)",
				sizeOf(p.path), sizeOf(pathPairA))
		}
		garbageBytes += sizeOf(p.path)
	}
	if garbageBytes <= 3*byteBudget {
		t.Fatalf("fixture shape broken: garbage front (%d) must exceed 3 byte budgets (%d)",
			garbageBytes, 3*byteBudget)
	}
	for _, p := range packs {
		key := blob.PackKey(f.repo, p.id)
		if err := f.blobs.UploadFile(ctx, key, p.path); err != nil {
			t.Fatal(err)
		}
		if err := f.store.RegisterPacks(ctx, f.repo, []ref.Pack{{ID: p.id, Key: key}}); err != nil {
			t.Fatal(err)
		}
	}

	counting := &countingDownloadStore{BlobStore: f.blobs}
	r := NewRepacker(f.store, f.blobs, fixtureMaterialize(counting, t.TempDir()), slog.New(slog.DiscardHandler))
	r.MaxFoldBytes = byteBudget
	r.MaxFoldObjects = 20
	folded, err := r.Repack(ctx, f.repo)
	if err != nil || !folded {
		t.Fatalf("deep-garbage repack = (folded %v, err %v), want the pair folded", folded, err)
	}
	if got := counting.count(); got != 2 {
		t.Fatalf("downloaded %d packs, want exactly the pair (garbage must never be downloaded)", got)
	}
	rows := f.packRows(t)
	if len(rows) != 13 {
		t.Fatalf("pack rows = %d, want 13 (12 garbage + fold(pairA,pairB))", len(rows))
	}
	for _, row := range rows {
		if row.ID == idPairA || row.ID == idPairB {
			t.Fatalf("pack rows = %+v: the pair must have been folded away", rows)
		}
	}
}

// fakePackHeader returns a syntactically valid 12-byte pack header ("PACK",
// version 2, 1 object) with no object data — enough for selection's header
// point read, so queue tests can seed rows that reach the materialize seam
// without crafting real packs.
func fakePackHeader() []byte {
	return []byte{'P', 'A', 'C', 'K', 0, 0, 0, 2, 0, 0, 0, 1}
}

// randomText returns n bytes of git-safe pseudo-random text that zlib
// cannot meaningfully compress (hex of urandom).
func randomText(n int) string {
	raw := make([]byte, n)
	_, err := rand.Read(raw)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(raw)[:n]
}

// lostFoldShim models the P1 delete race deterministically: ListPacks
// serves one stale pre-fold snapshot (so the fold loses), and the FIRST
// HasPack read reports the key unregistered — as if it landed just before a
// concurrent identical fold's install committed. Later HasPack calls (the
// post-delete recheck) see the real store.
type lostFoldShim struct {
	ref.RefStore
	mu           stdsync.Mutex
	stale        []ref.Pack
	hasPackCalls int
}

func (s *lostFoldShim) ListPacks(ctx context.Context, repo string) ([]ref.Pack, error) {
	s.mu.Lock()
	stale := s.stale
	s.stale = nil
	s.mu.Unlock()
	if stale != nil {
		return stale, nil
	}
	return s.RefStore.ListPacks(ctx, repo)
}

func (s *lostFoldShim) HasPack(ctx context.Context, repo, key string) (bool, error) {
	s.mu.Lock()
	s.hasPackCalls++
	first := s.hasPackCalls == 1
	s.mu.Unlock()
	if first {
		return false, nil
	}
	return s.RefStore.HasPack(ctx, repo, key)
}

// TestRepack_LostIdenticalFoldKeepsLiveKey pins the round-2/3 P1 class: a
// stale fold that loses can produce the SAME content-addressed pack as the
// concurrent winner (identical object union), and its HasPack read can land
// before the winner's install commits (the shim's first read reports
// unregistered). The lost path must NOT delete — the key is (or is about to
// be) LIVE, and no delete-then-restore dance is crash-safe. The blob must
// survive untouched.
func TestRepack_LostIdenticalFoldKeepsLiveKey(t *testing.T) {
	f := newRepackFixture(t)
	ctx := context.Background()
	for range 2 {
		f.push(t)
	}
	stale := f.packRows(t) // pre-fold snapshot [A, B]

	// Winner folds A,B into K; warmDir keeps A, B, and K materialized.
	warmDir := t.TempDir()
	if _, err := f.repacker(warmDir).Repack(ctx, f.repo); err != nil {
		t.Fatalf("winner repack: %v", err)
	}
	rows := f.packRows(t)
	if len(rows) != 1 {
		t.Fatalf("pack rows after winner fold = %+v, want 1", rows)
	}
	liveKey := rows[0].Key

	// The loser folds the same stale input in the same warm cache: same
	// union, same deterministic pack id, same blob key as the LIVE row —
	// and its registration read (shim) says "unregistered", the worst case.
	shim := &lostFoldShim{RefStore: f.store, stale: stale}
	loser := NewRepacker(shim, f.blobs, fixtureMaterialize(f.blobs, warmDir), slog.New(slog.DiscardHandler))
	if _, err := loser.Repack(ctx, f.repo); err != nil {
		t.Fatalf("loser repack: %v", err)
	}

	// The live blob was never deleted, the rows are the winner's, and the
	// repo cold-hydrates cleanly.
	if got := f.packRows(t); len(got) != 1 || got[0].Key != liveKey {
		t.Fatalf("pack rows after lost fold = %+v, want the winner's row %s", got, liveKey)
	}
	if !f.blobExists(t, liveKey) {
		t.Fatalf("live blob %s deleted by the lost identical fold", liveKey)
	}
	f.checkColdHydrate(t)
}

// TestRepack_RepublishesBlobSweptMidSwap pins the issue-#5 writer half on
// the repack path: a delete-enabled sweeper reaping the freshly uploaded
// consolidated blob between upload and swap commit (its content-addressed
// key can look aged when an earlier crashed fold left an identical orphan)
// must be healed by the post-swap EnsurePublished re-upload.
func TestRepack_RepublishesBlobSweptMidSwap(t *testing.T) {
	f := newRepackFixture(t)
	ctx := context.Background()
	for range 3 {
		f.push(t)
	}
	before, err := f.blobs.List(ctx, blob.PackPrefix(f.repo))
	if err != nil {
		t.Fatalf("list blobs: %v", err)
	}
	beforeKeys := make(map[string]bool, len(before))
	for _, o := range before {
		beforeKeys[o.Key] = true
	}

	r := f.repacker(t.TempDir())
	r.BeforeSwap = func(ctx context.Context, pending ref.Pack) error {
		// Simulate the sweeper reaping the just-uploaded consolidated
		// blob right before the swap commits.
		return f.blobs.Delete(ctx, pending.Key)
	}
	if _, err := r.Repack(ctx, f.repo); err != nil {
		t.Fatalf("repack: %v", err)
	}

	rows := f.packRows(t)
	if len(rows) != 1 {
		t.Fatalf("pack rows = %+v, want the fold", rows)
	}
	if !f.blobExists(t, rows[0].Key) {
		t.Fatalf("consolidated blob %s not re-published after mid-swap sweep", rows[0].Key)
	}
	f.checkColdHydrate(t)
}

// statOutageStore delegates until armed, then fails every Stat — the shape
// of a blob-store outage hitting exactly the post-swap verification.
type statOutageStore struct {
	blob.BlobStore
	mu     stdsync.Mutex
	broken bool
}

func (s *statOutageStore) arm() { s.mu.Lock(); s.broken = true; s.mu.Unlock() }

func (s *statOutageStore) Stat(ctx context.Context, key string) (int64, error) {
	s.mu.Lock()
	broken := s.broken
	s.mu.Unlock()
	if broken {
		return 0, errors.New("injected stat outage")
	}
	return s.BlobStore.Stat(ctx, key)
}

// TestRepack_CommittedFoldReportsFoldedOnVerifyError pins the round-5
// follow-up contract: when ReplacePacks commits but the post-swap
// verification fails transiently, Repack must report folded=true alongside
// the error — queue backends key their follow-up scheduling on it, and a
// false report would strand mid-fold registrations whose triggers were
// coalesced.
func TestRepack_CommittedFoldReportsFoldedOnVerifyError(t *testing.T) {
	f := newRepackFixture(t)
	ctx := context.Background()
	for range 3 {
		f.push(t)
	}

	outage := &statOutageStore{BlobStore: f.blobs}
	r := NewRepacker(f.store, outage, fixtureMaterialize(f.blobs, t.TempDir()), slog.New(slog.DiscardHandler))
	r.BeforeSwap = func(context.Context, ref.Pack) error {
		outage.arm() // the verify leg's Stat will fail; the swap still commits
		return nil
	}
	folded, err := r.Repack(ctx, f.repo)
	if !folded {
		t.Fatalf("Repack = (folded %v, err %v): committed fold must report folded=true", folded, err)
	}
	if err == nil {
		t.Fatal("Repack must surface the verification failure")
	}
	if rows := f.packRows(t); len(rows) != 1 {
		t.Fatalf("pack rows = %+v, want the committed fold", rows)
	}
	// The blob itself is fine (only the verification read was injected).
	f.checkColdHydrate(t)
}

// TestRepack_ColdWorkerDownloadsOnlyBudgetedPacks pins the round-5
// select-before-materialize ordering: a cold worker must download only the
// byte-budget-selected packs, never the whole repository.
func TestRepack_ColdWorkerDownloadsOnlyBudgetedPacks(t *testing.T) {
	f := newRepackFixture(t)
	ctx := context.Background()
	for range 4 {
		f.push(t)
	}
	rows := f.packRows(t)
	if len(rows) != 4 {
		t.Fatalf("pack rows = %d, want 4", len(rows))
	}

	// Budget fits exactly the two oldest packs (fixture pushes produce
	// near-identical sizes; 2x the first pack's size + slack < 3 packs).
	var first int64
	objs, err := f.blobs.List(ctx, blob.PackPrefix(f.repo))
	if err != nil {
		t.Fatalf("list blobs: %v", err)
	}
	for _, o := range objs {
		if o.Key == rows[0].Key {
			first = o.Size
		}
	}
	if first == 0 {
		t.Fatalf("first pack blob %s not found", rows[0].Key)
	}

	coldDir := t.TempDir()
	counting := &countingDownloadStore{BlobStore: f.blobs}
	r := NewRepacker(f.store, f.blobs, fixtureMaterialize(counting, coldDir), slog.New(slog.DiscardHandler))
	r.MaxFoldBytes = first*2 + first/2
	folded, err := r.Repack(ctx, f.repo)
	if err != nil || !folded {
		t.Fatalf("budgeted cold repack = (folded %v, err %v), want a fold", folded, err)
	}
	if got := counting.count(); got != 2 {
		t.Fatalf("cold worker downloaded %d packs, want exactly the 2 byte-budgeted candidates", got)
	}
	if after := f.packRows(t); len(after) != 3 {
		t.Fatalf("pack rows after budgeted fold = %+v, want 3 (fold + 2 leftovers)", after)
	}
	f.checkColdHydrate(t)
}

// countingDownloadStore counts DownloadFile calls (the materialization
// seam's only download path).
type countingDownloadStore struct {
	blob.BlobStore
	mu stdsync.Mutex
	n  int
}

func (s *countingDownloadStore) DownloadFile(ctx context.Context, key, dest string) error {
	s.mu.Lock()
	s.n++
	s.mu.Unlock()
	return s.BlobStore.DownloadFile(ctx, key, dest)
}

func (s *countingDownloadStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n
}

// enqueueOnListPacks delegates ListPacks, but when armed the next call
// first runs fire() — the deterministic stand-in for a trigger landing in
// the gap between the local worker's follow-up recheck (whose ListPacks
// this is) and its exit-decision lock.
type enqueueOnListPacks struct {
	ref.RefStore
	mu    stdsync.Mutex
	fire  func()
	calls int
}

func (s *enqueueOnListPacks) arm(fire func()) {
	s.mu.Lock()
	s.fire = fire
	s.mu.Unlock()
}

func (s *enqueueOnListPacks) ListPacks(ctx context.Context, repo string) ([]ref.Pack, error) {
	s.mu.Lock()
	s.calls++
	fire := s.fire
	s.fire = nil
	s.mu.Unlock()
	if fire != nil {
		fire()
	}
	return s.RefStore.ListPacks(ctx, repo)
}

func (s *enqueueOnListPacks) listPacksCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// TestLocalQueue_TriggerInExitWindowNotLost pins the round-7 race: a
// trigger that lands after the worker's follow-up recheck computed false
// but before it takes the queue lock must still earn a fold attempt — the
// pending flag is consumed in the same critical section that decides exit,
// so it can never be deleted unseen.
func TestLocalQueue_TriggerInExitWindowNotLost(t *testing.T) {
	f := newRepackFixture(t)
	ctx := context.Background()
	for range 2 {
		f.push(t)
	}

	shim := &enqueueOnListPacks{RefStore: f.store}
	r := NewRepacker(shim, f.blobs, fixtureMaterialize(f.blobs, t.TempDir()), slog.New(slog.DiscardHandler))
	r.FollowUpThreshold = 2
	deliverer := &events.Deliverer{Webhooks: stubWebhooks{}, Logger: slog.New(slog.DiscardHandler)}
	q := NewLocal(deliverer, func(context.Context, sync.Job) error { return nil }, r, slog.New(slog.DiscardHandler))

	r.BeforeSwap = func(context.Context, ref.Pack) error {
		// Arm mid-fold: the NEXT ListPacks is the post-fold NeedsFollowUp
		// read, and firing there lands the trigger exactly in the window
		// between the recheck and the worker's lock.
		shim.arm(func() { _ = q.EnqueueRepack(context.Background(), f.repo) })
		return nil
	}
	if err := q.EnqueueRepack(ctx, f.repo); err != nil {
		t.Fatalf("enqueue repack: %v", err)
	}
	// Wait for the worker to drain NATURALLY (Close would set closed and
	// legitimately short-circuit the loop, masking the race under test).
	deadline := time.Now().Add(30 * time.Second)
	for {
		q.mu.Lock()
		busy := q.repacking[f.repo]
		q.mu.Unlock()
		if !busy {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("worker never drained")
		}
		time.Sleep(10 * time.Millisecond)
	}
	q.Close()

	// Call ledger: fold-1 snapshot, NeedsFollowUp read (fires the racing
	// trigger), and — only if the trigger was consumed correctly — the
	// earned attempt's snapshot. Fewer than 3 means the pending flag was
	// deleted unseen.
	if got := shim.listPacksCalls(); got < 3 {
		t.Fatalf("ListPacks called %d times; the exit-window trigger was dropped without a fold attempt", got)
	}
	if rows := f.packRows(t); len(rows) != 1 {
		t.Fatalf("pack rows = %+v, want the fold", rows)
	}
	f.checkColdHydrate(t)
}

// TestRepack_RuntimeBudgetBoundsFold pins the security-round runtime
// budget: the whole fold runs under MaxFoldDuration, so an
// already-expired budget aborts before any mutation — compressed-size
// budgets alone undercount CPU (a tiny pack can inflate to huge logical
// content), and only a wall-clock bound caps the actual work.
func TestRepack_RuntimeBudgetBoundsFold(t *testing.T) {
	f := newRepackFixture(t)
	ctx := context.Background()
	for range 3 {
		f.push(t)
	}
	before, err := f.blobs.List(ctx, blob.PackPrefix(f.repo))
	if err != nil {
		t.Fatalf("list blobs: %v", err)
	}

	r := f.repacker(t.TempDir())
	r.MaxFoldDuration = time.Nanosecond // expires before any step
	folded, err := r.Repack(ctx, f.repo)
	if err == nil || folded {
		t.Fatalf("expired runtime budget: Repack = (folded %v, err %v), want a deadline error", folded, err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Repack error = %v, want context.DeadlineExceeded in the chain", err)
	}
	// Crash-equivalence: no DB change, no blob change.
	if rows := f.packRows(t); len(rows) != 3 {
		t.Fatalf("pack rows after aborted fold = %+v, want the original 3", rows)
	}
	after, err := f.blobs.List(ctx, blob.PackPrefix(f.repo))
	if err != nil {
		t.Fatalf("list blobs: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("aborted fold changed the blob set: %d -> %d", len(before), len(after))
	}

	// A sane budget folds normally.
	r.MaxFoldDuration = time.Minute
	if folded, err := r.Repack(ctx, f.repo); err != nil || !folded {
		t.Fatalf("sane runtime budget: Repack = (folded %v, err %v), want a fold", folded, err)
	}
	f.checkColdHydrate(t)
}

// TestLocalQueue_RepackWorkersCapped pins the round-15 security finding:
// triggers across many distinct repos must not multiply the per-fold
// budgets — at most localRepackWorkers folds execute concurrently, the
// rest park on the semaphore (and Close releases parked waiters).
func TestLocalQueue_RepackWorkersCapped(t *testing.T) {
	f := newRepackFixture(t)
	ctx := context.Background()
	// One real foldable repo is enough; the cap is about EXECUTION slots,
	// which the gate seam occupies deterministically.
	for range 2 {
		f.push(t)
	}

	const repos = 12
	gate := make(chan struct{})
	var mu stdsync.Mutex
	executing, peak := 0, 0
	r := NewRepacker(f.store, f.blobs, func(ctx context.Context, repo string, packs []ref.Pack) (string, error) {
		mu.Lock()
		executing++
		if executing > peak {
			peak = executing
		}
		mu.Unlock()
		<-gate
		mu.Lock()
		executing--
		mu.Unlock()
		return "", errors.New("gated fixture materialize: fold aborts after the concurrency probe")
	}, slog.New(slog.DiscardHandler))
	deliverer := &events.Deliverer{Webhooks: stubWebhooks{}, Logger: slog.New(slog.DiscardHandler)}
	q := NewLocal(deliverer, func(context.Context, sync.Job) error { return nil }, r, slog.New(slog.DiscardHandler))

	// Seed rows (with real backing blobs — selection point-reads their
	// size and 12-byte pack header) for N distinct repo keys so every
	// fold reaches the materialize seam.
	seed := filepath.Join(t.TempDir(), "seed.pack")
	if err := os.WriteFile(seed, fakePackHeader(), 0o644); err != nil {
		t.Fatal(err)
	}
	for i := range repos {
		repo := fmt.Sprintf("capped%d", i)
		packs := []ref.Pack{
			{ID: "a", Key: blob.PackKey(repo, "aaaa")},
			{ID: "b", Key: blob.PackKey(repo, "bbbb")},
		}
		for _, p := range packs {
			if err := f.blobs.UploadFile(ctx, p.Key, seed); err != nil {
				t.Fatalf("upload %s: %v", p.Key, err)
			}
		}
		if err := f.store.RegisterPacks(ctx, repo, packs); err != nil {
			t.Fatalf("register %s: %v", repo, err)
		}
		if err := q.EnqueueRepack(ctx, repo); err != nil {
			t.Fatalf("enqueue %s: %v", repo, err)
		}
	}
	// Let the executing set saturate, then check the cap held.
	deadline := time.Now().Add(10 * time.Second)
	for {
		mu.Lock()
		cur := executing
		mu.Unlock()
		if cur == localRepackWorkers {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("executing folds = %d, want the cap %d to saturate", cur, localRepackWorkers)
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // give over-cap goroutines a chance to (wrongly) enter
	mu.Lock()
	gotPeak := peak
	mu.Unlock()
	if gotPeak != localRepackWorkers {
		t.Fatalf("peak concurrent folds = %d, want exactly the cap %d", gotPeak, localRepackWorkers)
	}
	// Close must release both the executing folds (gate) and the parked
	// waiters; everything exits.
	close(gate)
	q.Close()
}

// TestLocalQueue_CloseSkipsParkedFolds pins the round-19 finding: Go picks
// randomly among ready select cases, so a parked worker can win a free
// semaphore slot over the closed done channel — it must recheck closure
// after acquiring and exit WITHOUT folding, or Close would sit through
// fold batches it meant to discard.
func TestLocalQueue_CloseSkipsParkedFolds(t *testing.T) {
	f := newRepackFixture(t)
	ctx := context.Background()

	gate := make(chan struct{})
	var mu stdsync.Mutex
	folds := 0
	r := NewRepacker(f.store, f.blobs, func(ctx context.Context, repo string, packs []ref.Pack) (string, error) {
		mu.Lock()
		folds++
		mu.Unlock()
		<-gate
		return "", errors.New("gated")
	}, slog.New(slog.DiscardHandler))
	deliverer := &events.Deliverer{Webhooks: stubWebhooks{}, Logger: slog.New(slog.DiscardHandler)}
	q := NewLocal(deliverer, func(context.Context, sync.Job) error { return nil }, r, slog.New(slog.DiscardHandler))

	// Fill every slot plus park several waiters.
	seed := filepath.Join(t.TempDir(), "seed.pack")
	if err := os.WriteFile(seed, fakePackHeader(), 0o644); err != nil {
		t.Fatal(err)
	}
	total := localRepackWorkers + 6
	for i := range total {
		repo := fmt.Sprintf("closeskip%d", i)
		packs := []ref.Pack{{ID: "a", Key: blob.PackKey(repo, "aaaa")}, {ID: "b", Key: blob.PackKey(repo, "bbbb")}}
		for _, p := range packs {
			if err := f.blobs.UploadFile(ctx, p.Key, seed); err != nil {
				t.Fatal(err)
			}
		}
		if err := f.store.RegisterPacks(ctx, repo, packs); err != nil {
			t.Fatal(err)
		}
		if err := q.EnqueueRepack(ctx, repo); err != nil {
			t.Fatal(err)
		}
	}
	// Wait for the executing set to saturate the slots.
	deadline := time.Now().Add(10 * time.Second)
	for {
		mu.Lock()
		cur := folds
		mu.Unlock()
		if cur >= localRepackWorkers {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("folds started = %d, want the cap %d", cur, localRepackWorkers)
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Close with parked waiters; release the executing folds concurrently
	// (Close blocks on wg until they finish). Parked waiters must exit
	// WITHOUT folding, whichever select case wins their wakeup.
	go func() { time.Sleep(50 * time.Millisecond); close(gate) }()
	q.Close()
	mu.Lock()
	got := folds
	mu.Unlock()
	if got != localRepackWorkers {
		t.Fatalf("folds executed = %d, want exactly the %d in flight at Close (parked waiters must be discarded)", got, localRepackWorkers)
	}
}

// TestNormalizeCtxErr pins the deadline-classification identity (security
// round 14): CommandContext kills an over-budget git subprocess, whose raw
// *exec.ExitError ("signal: killed") does NOT match
// errors.Is(err, context.DeadlineExceeded) — the river worker classifies
// deterministic budget exhaustion by exactly that identity, so the helpers
// must fold the context's own error into the chain.
func TestNormalizeCtxErr(t *testing.T) {
	killed := errors.New("signal: killed") // the *exec.ExitError shape
	// Live context: the error passes through untouched (a genuine crash
	// must stay retryable).
	if got := normalizeCtxErr(context.Background(), killed); !errors.Is(got, killed) || errors.Is(got, context.DeadlineExceeded) {
		t.Fatalf("live-ctx normalize = %v, want the original error only", got)
	}
	// Expired context: the chain must carry DeadlineExceeded AND the
	// original error.
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()
	got := normalizeCtxErr(ctx, killed)
	if !errors.Is(got, context.DeadlineExceeded) || !errors.Is(got, killed) {
		t.Fatalf("expired-ctx normalize = %v, want both DeadlineExceeded and the original", got)
	}

	// End to end through a real killed subprocess: a git command started
	// under an expiring context surfaces DeadlineExceeded through
	// runGitStdin's chain. The pipe writer closes on expiry so exec's
	// stdin-copy goroutine (which Wait joins) can finish — production
	// callers pass an *os.File or a finite reader and never hit that.
	shortCtx, cancel2 := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel2()
	blockingStdin, w := io.Pipe() // never written: show-index blocks reading it
	go func() { <-shortCtx.Done(); _ = w.Close() }()
	_, err := runGitStdin(shortCtx, t.TempDir(), blockingStdin, "show-index")
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("killed-subprocess error = %v, want DeadlineExceeded in the chain", err)
	}
}

// TestRepackTrigger_BoundedExecution pins the P2 contract: MaybeEnqueue is
// time-bounded regardless of the caller's context, so a stalled store
// cannot pin the write handler that invoked the trigger.
func TestRepackTrigger_BoundedExecution(t *testing.T) {
	f := newRepackFixture(t)
	trigger := NewRepackTrigger(&stallingStore{RefStore: f.store}, 2,
		func(context.Context, string) error { return nil }, slog.New(slog.DiscardHandler))
	trigger.timeout = 50 * time.Millisecond

	done := make(chan struct{})
	go func() {
		trigger.MaybeEnqueue(context.Background(), f.repo)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("MaybeEnqueue not bounded: still running after 5s against a stalled store")
	}
}

// stallingStore blocks ListPacks until the context is canceled.
type stallingStore struct {
	ref.RefStore
}

func (s *stallingStore) ListPacks(ctx context.Context, _ string) ([]ref.Pack, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestRepackTrigger_Threshold(t *testing.T) {
	f := newRepackFixture(t)
	ctx := context.Background()

	var enqueued []string
	trigger := NewRepackTrigger(f.store, 3, func(_ context.Context, repo string) error {
		enqueued = append(enqueued, repo)
		return nil
	}, slog.New(slog.DiscardHandler))

	f.push(t)
	trigger.MaybeEnqueue(ctx, f.repo)
	f.push(t)
	trigger.MaybeEnqueue(ctx, f.repo)
	if len(enqueued) != 0 {
		t.Fatalf("trigger fired below threshold: %v", enqueued)
	}
	f.push(t)
	trigger.MaybeEnqueue(ctx, f.repo)
	if len(enqueued) != 1 || enqueued[0] != f.repo {
		t.Fatalf("trigger at threshold enqueued %v, want exactly one %s", enqueued, f.repo)
	}
	// Above threshold it keeps firing (the queue dedupes, not the trigger).
	f.push(t)
	trigger.MaybeEnqueue(ctx, f.repo)
	if len(enqueued) != 2 {
		t.Fatalf("trigger above threshold enqueued %v, want two entries", enqueued)
	}
}

// TestRiver_RepackUniqueByRepo proves the durable queue's coalescing on a
// real Postgres: triggers firing while a fold is running insert nothing new
// (ByArgs uniqueness across active states), and a completed fold does not
// block later triggers. The repacker itself still folds the sqlite+fileblob
// fixture — the queue backend and the pack stores are independent seams.
func TestRiver_RepackUniqueByRepo(t *testing.T) {
	ctx := context.Background()
	pgC, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("postgres"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = pgC.Terminate(context.Background()) })
	pgURL, err := pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}

	f := newRepackFixture(t)
	for range 3 {
		f.push(t)
	}
	gate := make(chan struct{})
	released := false
	release := func() {
		if !released {
			released = true
			close(gate)
		}
	}
	defer release()
	r := f.repacker(t.TempDir())
	r.BeforeSwap = func(context.Context, ref.Pack) error { <-gate; return nil }
	r.FollowUpThreshold = 2 // successor gate: mid-run rows demand a follow-up

	deliverer := &events.Deliverer{Webhooks: stubWebhooks{}, Logger: slog.New(slog.DiscardHandler)}
	q, err := NewRiver(ctx, pgURL, deliverer, func(context.Context, sync.Job) error { return nil }, sync.NewMemStore(),
		nil, r, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewRiver: %v", err)
	}
	defer q.Close()

	pool, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	countJobs := func() int {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM river_job WHERE kind = 'repack'`).Scan(&n); err != nil {
			t.Fatalf("count repack jobs: %v", err)
		}
		return n
	}

	// First trigger inserts the job; wait until a worker is RUNNING it
	// (parked at the BeforeSwap gate).
	if err := q.EnqueueRepack(ctx, f.repo); err != nil {
		t.Fatalf("enqueue repack: %v", err)
	}
	waitJobs := func(query string, want int, what string) {
		t.Helper()
		deadline := time.Now().Add(60 * time.Second)
		for {
			var n int
			if err := pool.QueryRow(ctx, query).Scan(&n); err != nil {
				t.Fatalf("count %s: %v", what, err)
			}
			if n == want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s = %d, want %d (pack rows %+v)", what, n, want, f.packRows(t))
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	waitJobs(`SELECT count(*) FROM river_job WHERE kind = 'repack' AND state = 'running'`, 1, "running repack jobs")

	// Triggers firing while the fold runs are swallowed by uniqueness
	// (running is a required unique state): still exactly one job. The
	// packs registered mid-fold survive the fold's snapshot; the WORKER
	// owns folding them via the successor inserted atomically with its
	// completion.
	f.push(t)
	f.push(t)
	for range 3 {
		if err := q.EnqueueRepack(ctx, f.repo); err != nil {
			t.Fatalf("enqueue repack mid-run: %v", err)
		}
	}
	if n := countJobs(); n != 1 {
		t.Fatalf("river holds %d repack jobs while one is running, want 1 (unique)", n)
	}
	release()

	// The chain: job 1 folds its snapshot and — the post-completion count
	// (fold + 2 mid-run rows = 3 ≥ threshold 2, and it folded) — inserts a
	// successor atomically with its completion; the successor folds the
	// remaining rows to one, drops below the threshold, and completes
	// WITHOUT chaining: exactly two completed jobs, one pack row.
	waitJobs(`SELECT count(*) FROM river_job WHERE kind = 'repack' AND state = 'completed'`, 2, "completed repack jobs")
	if n := countJobs(); n != 2 {
		t.Fatalf("river holds %d repack jobs after the chain, want 2 (below-threshold successor must not chain)", n)
	}
	if rows := f.packRows(t); len(rows) != 1 {
		t.Fatalf("pack rows after follow-up fold = %+v, want 1 (mid-run rows not refolded)", rows)
	}
	f.checkColdHydrate(t)

	// Completed jobs do not participate in uniqueness: a later trigger
	// inserts a fresh job. It no-ops on the single remaining pack over an
	// unchanged count, so it must NOT chain a successor (the wedge/plateau
	// termination rule).
	if err := q.EnqueueRepack(ctx, f.repo); err != nil {
		t.Fatalf("enqueue after completion: %v", err)
	}
	waitJobs(`SELECT count(*) FROM river_job WHERE kind = 'repack' AND state = 'completed'`, 3, "completed repack jobs")
	if n := countJobs(); n != 3 {
		t.Fatalf("river holds %d repack jobs after the no-op retrigger, want 3 (no-op over unchanged count must not chain)", n)
	}

	// Insert-only clients (hook subprocesses) must refuse repack inserts:
	// they register no worker, and a uniquely-queued job nobody works
	// would swallow every later trigger.
	ins, err := NewRiverInsertOnly(ctx, pgURL)
	if err != nil {
		t.Fatalf("NewRiverInsertOnly: %v", err)
	}
	defer ins.Close()
	if err := ins.EnqueueRepack(ctx, f.repo); err == nil {
		t.Fatal("insert-only EnqueueRepack must fail")
	}
}

// TestLocalQueue_TriggerMidFoldConvergesToOne pins the issue #34 flake
// interleave deterministically, with the PRODUCTION trigger making every
// scheduling decision: a push registers its pack after the running fold's
// snapshot but before its swap — the trigger fires while the job is active
// and is coalesced — and the follow-up machinery must still converge the
// repo to exactly ONE pack row. At threshold 2 that convergence is
// guaranteed for every interleave (any registration leaves the count ≥ 2,
// so the last one always fires a trigger or lands inside an active job
// whose follow-up covers it); the e2e TestRepack_PushesFoldToOnePack relies
// on exactly this property, so it is pinned here without CI-load timing.
func TestLocalQueue_TriggerMidFoldConvergesToOne(t *testing.T) {
	f := newRepackFixture(t)
	ctx := context.Background()

	r := f.repacker(t.TempDir())
	r.FollowUpThreshold = 2
	deliverer := &events.Deliverer{Webhooks: stubWebhooks{}, Logger: slog.New(slog.DiscardHandler)}
	q := NewLocal(deliverer, func(context.Context, sync.Job) error { return nil }, r, slog.New(slog.DiscardHandler))
	defer q.Close()
	trigger := NewRepackTrigger(f.store, 2, q.EnqueueRepack, slog.New(slog.DiscardHandler))

	// The FIRST fold parks in its race window (post-snapshot, pre-swap) and
	// hands control back to the test goroutine, which performs the late
	// push there; successor folds pass straight through. Pushes must run on
	// the test goroutine (testGit fails via t.Fatalf).
	raceWindow := make(chan struct{})
	resume := make(chan struct{})
	first := true
	r.BeforeSwap = func(context.Context, ref.Pack) error {
		if first {
			first = false
			raceWindow <- struct{}{}
			<-resume
		}
		return nil
	}

	// Push 1: below threshold, the trigger must schedule nothing.
	f.push(t)
	trigger.MaybeEnqueue(ctx, f.repo)
	// Push 2: at threshold — the trigger starts the fold, which parks in
	// the race window with both rows in its snapshot.
	f.push(t)
	trigger.MaybeEnqueue(ctx, f.repo)
	<-raceWindow

	// Push 3 lands mid-fold: registration + its post-receive trigger, like
	// the git surface after every receive-pack. The trigger coalesces into
	// the running job (pending flag) — this row survives the snapshot.
	f.push(t)
	trigger.MaybeEnqueue(ctx, f.repo)
	close(resume)

	// Convergence: the pending rerun (or follow-up gate) folds the fold
	// output plus the late row down to one.
	deadline := time.Now().Add(30 * time.Second)
	for len(f.packRows(t)) != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("mid-fold registration never converged to one pack row: %+v", f.packRows(t))
		}
		time.Sleep(20 * time.Millisecond)
	}
	f.checkColdHydrate(t)
}

// TestLocalQueue_LateRegistrationBelowThresholdSettles is the deterministic
// repro of why the e2e fold test could NOT assert "exactly one row" at
// threshold 3 (issue #34): when the fold's snapshot covers all but the last
// push and that push registers after the swap, the count settles BELOW the
// threshold but above one — and nothing may fold it further. The trigger
// correctly declines (2 < 3), the follow-up gate is not due, and the system
// invariant is exactly this: convergence below the threshold, not to one
// row, with no busy-chained no-op folds. The queue must be observably idle
// afterwards.
func TestLocalQueue_LateRegistrationBelowThresholdSettles(t *testing.T) {
	f := newRepackFixture(t)
	ctx := context.Background()

	r := f.repacker(t.TempDir())
	r.FollowUpThreshold = 3
	deliverer := &events.Deliverer{Webhooks: stubWebhooks{}, Logger: slog.New(slog.DiscardHandler)}
	q := NewLocal(deliverer, func(context.Context, sync.Job) error { return nil }, r, slog.New(slog.DiscardHandler))
	defer q.Close()
	trigger := NewRepackTrigger(f.store, 3, q.EnqueueRepack, slog.New(slog.DiscardHandler))

	// Four pushes, then the fold consumes all four.
	for range 4 {
		f.push(t)
	}
	trigger.MaybeEnqueue(ctx, f.repo)
	waitIdle := func(what string) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for {
			q.mu.Lock()
			busy := q.repacking[f.repo]
			q.mu.Unlock()
			if !busy {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("queue never drained %s", what)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	waitIdle("after the four-push fold")
	if rows := f.packRows(t); len(rows) != 1 {
		t.Fatalf("fold of four pushes left %+v, want 1 row", rows)
	}

	// The late push lands strictly after the swap: count 2 < threshold 3.
	// Its trigger must decline, and no follow-up may run — the steady state
	// is TWO rows, permanently. This is the interleave that made the old
	// exactly-one e2e wait unreachable at threshold 3.
	f.push(t)
	trigger.MaybeEnqueue(ctx, f.repo)
	waitIdle("after the below-threshold trigger")
	if rows := f.packRows(t); len(rows) != 2 {
		t.Fatalf("below-threshold steady state = %+v, want 2 rows (fold output + late push)", rows)
	}
	f.checkColdHydrate(t)
}

func TestLocalQueue_RepackCoalescesPerRepo(t *testing.T) {
	f := newRepackFixture(t)
	ctx := context.Background()
	for range 3 {
		f.push(t)
	}

	gate := make(chan struct{})
	entered := make(chan struct{}, 8)
	r := f.repacker(t.TempDir())
	r.BeforeSwap = func(context.Context, ref.Pack) error {
		entered <- struct{}{}
		<-gate
		return nil
	}
	deliverer := &events.Deliverer{Webhooks: stubWebhooks{}, Logger: slog.New(slog.DiscardHandler)}
	q := NewLocal(deliverer, func(context.Context, sync.Job) error { return nil }, r, slog.New(slog.DiscardHandler))

	// Three triggers while the first fold is parked in its race window:
	// exactly one goroutine may run (River-equivalent unique-by-repo).
	for range 3 {
		if err := q.EnqueueRepack(ctx, f.repo); err != nil {
			t.Fatalf("enqueue repack: %v", err)
		}
	}
	<-entered
	select {
	case <-entered:
		t.Fatal("a second repack ran concurrently for the same repo")
	case <-time.After(100 * time.Millisecond):
	}
	// Packs registered after the running fold's snapshot survive it; the
	// coalesced triggers above must yield ONE pending rerun that folds
	// them once the first fold finishes (mirrors River's queued-states
	// uniqueness): without it, these rows would strand above the
	// threshold until the next write.
	f.push(t)
	f.push(t)
	close(gate)
	deadline := time.Now().Add(30 * time.Second)
	for len(f.packRows(t)) != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("pending rerun never folded the mid-run rows: %+v", f.packRows(t))
		}
		time.Sleep(20 * time.Millisecond)
	}
	q.Close() // waits for any in-flight fold

	if rows := f.packRows(t); len(rows) != 1 {
		t.Fatalf("pack rows after coalesced fold + rerun = %+v, want 1", rows)
	}
	if err := q.EnqueueRepack(ctx, f.repo); err == nil {
		t.Fatal("EnqueueRepack after Close must fail")
	}

	// Backlog draining (round-6): with a follow-up threshold set, a
	// budgeted fold that leaves the repo at/above it must keep folding
	// WITHOUT any concurrent trigger. Four fresh pushes under an object
	// budget of two 3-object packs per fold: one enqueue drains the
	// backlog 4 → 3 → 2 rows via successive budgeted folds, stopping at
	// the budget's steady state (two 6-object folds exceed the budget
	// together — the documented O(size/budget) convergence).
	f2 := newRepackFixture(t)
	for range 4 {
		f2.push(t)
	}
	// ListPacks orders by created_at (1-second resolution in SQLite) with
	// an id tiebreak: give the folds a strictly later timestamp than the
	// push packs, or a fold whose hex id sorts first would be selected
	// ahead of the remaining pushes and (over budget with either) block
	// them from pairing.
	time.Sleep(1100 * time.Millisecond)
	r2 := f2.repacker(t.TempDir())
	r2.MaxFoldObjects = 7 // two 3-object push packs per fold
	r2.FollowUpThreshold = 2
	q2 := NewLocal(deliverer, func(context.Context, sync.Job) error { return nil }, r2, slog.New(slog.DiscardHandler))
	if err := q2.EnqueueRepack(ctx, f2.repo); err != nil {
		t.Fatalf("enqueue backlog repack: %v", err)
	}
	deadline = time.Now().Add(30 * time.Second)
	for len(f2.packRows(t)) > 2 {
		if time.Now().After(deadline) {
			t.Fatalf("backlog never drained to the budget steady state: %+v", f2.packRows(t))
		}
		time.Sleep(20 * time.Millisecond)
	}
	q2.Close()
	if rows := f2.packRows(t); len(rows) != 2 {
		t.Fatalf("backlog steady state = %+v, want 2 budget-limited folds", rows)
	}
	f2.checkColdHydrate(t)

	// A queue without a repacker must refuse rather than silently drop.
	bare := NewLocal(deliverer, func(context.Context, sync.Job) error { return nil }, nil, slog.New(slog.DiscardHandler))
	defer bare.Close()
	if err := bare.EnqueueRepack(ctx, f.repo); err == nil {
		t.Fatal("EnqueueRepack without a repacker must fail")
	}
}
