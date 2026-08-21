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
	"errors"
	"fmt"
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

	"codestorage/internal/blob"
	"codestorage/internal/events"
	"codestorage/internal/githttp"
	"codestorage/internal/ref"
	"codestorage/internal/sync"
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
	if err := f.store.RegisterPacks(ctx, f.repo, []ref.Pack{{ID: id, Key: key}}); err != nil {
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
	return NewRepacker(f.store, f.blobs, func(ctx context.Context, repo string) (string, error, error) {
		return githttp.Hydrate(ctx, f.store, f.blobs, cacheDir, repo)
	}, slog.New(slog.DiscardHandler))
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
	repoPath, refsErr, err := githttp.Hydrate(ctx, f.store, f.blobs, coldDir, f.repo)
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

	if err := f.repacker(t.TempDir()).Repack(ctx, f.repo); err != nil {
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
	if err := f.repacker(t.TempDir()).Repack(ctx, f.repo); err != nil {
		t.Fatalf("second repack: %v", err)
	}
	if again := f.packRows(t); len(again) != 1 || again[0].ID != rows[0].ID {
		t.Fatalf("second fold changed the pack set: %+v", again)
	}
}

func TestRepack_LostRaceDeletesOwnOrphan(t *testing.T) {
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
	loser.BeforeSwap = func(ctx context.Context) error {
		// The loser's consolidated blob is uploaded by now: it is the only
		// key not present in the pre-race listing.
		mid, err := f.blobs.List(ctx, blob.PackPrefix(f.repo))
		if err != nil {
			return err
		}
		for _, o := range mid {
			if !beforeKeys[o.Key] {
				loserKey = o.Key
			}
		}
		if loserKey == "" {
			return errors.New("loser blob not uploaded before swap")
		}
		// Land a DIFFERENT competing fold: one more push changes the
		// winner's input set, so the two consolidated packs differ and the
		// loser's own-orphan cleanup must actually delete bytes.
		f.push(t)
		return f.repacker(winnerDir).Repack(ctx, f.repo)
	}
	if err := loser.Repack(ctx, f.repo); err != nil {
		t.Fatalf("loser repack: %v", err)
	}

	rows := f.packRows(t)
	if len(rows) != 1 {
		t.Fatalf("pack rows after race = %+v, want the winner's single fold", rows)
	}
	if rows[0].Key == loserKey {
		t.Fatalf("winner and loser folds coincided (%s); the race was not exercised", loserKey)
	}
	// Lost race: zero DB changes beyond the winner's fold, and the loser
	// deleted exactly its own orphan.
	if f.blobExists(t, loserKey) {
		t.Fatalf("loser's orphan blob %s survived its lost race", loserKey)
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

	r := NewRepacker(ambiguousReplaceStore{f.store}, f.blobs, func(ctx context.Context, repo string) (string, error, error) {
		return githttp.Hydrate(ctx, f.store, f.blobs, f.cacheDir, repo)
	}, slog.New(slog.DiscardHandler))
	err := r.Repack(ctx, f.repo)
	if !errors.Is(err, errAmbiguousReplace) {
		t.Fatalf("repack error = %v, want the injected ambiguous drop", err)
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

func TestRepack_SweeperCollectsSupersededBlobs(t *testing.T) {
	f := newRepackFixture(t)
	ctx := context.Background()
	var oldIDs []string
	for range 4 {
		oldIDs = append(oldIDs, f.push(t))
	}
	if err := f.repacker(t.TempDir()).Repack(ctx, f.repo); err != nil {
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
	if err := f.repacker(t.TempDir()).Repack(ctx, f.repo); err != nil {
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
	repoPath, refsErr, err := githttp.Hydrate(ctx, stale, f.blobs, coldDir, f.repo)
	if err != nil || refsErr != nil {
		t.Fatalf("stale-listing hydrate: err=%v refsErr=%v", err, refsErr)
	}
	testGit(t, repoPath, "", "fsck", "--connectivity-only", "--no-progress", "--no-dangling")
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
	r.BeforeSwap = func(context.Context) error { <-gate; return nil }

	deliverer := &events.Deliverer{Webhooks: stubWebhooks{}, Logger: slog.New(slog.DiscardHandler)}
	q, err := NewRiver(ctx, pgURL, deliverer, func(context.Context, sync.Job) error { return nil },
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

	// Concurrent triggers while the fold is parked: exactly one durable job.
	for range 3 {
		if err := q.EnqueueRepack(ctx, f.repo); err != nil {
			t.Fatalf("enqueue repack: %v", err)
		}
	}
	if n := countJobs(); n != 1 {
		t.Fatalf("river holds %d repack jobs while one is active, want 1", n)
	}
	release()

	// Wait for the JOB to complete (not merely the fold to land: the state
	// flips after the worker returns, and the uniqueness check below must
	// not race it).
	deadline := time.Now().Add(30 * time.Second)
	for {
		var done int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM river_job WHERE kind = 'repack' AND state = 'completed'`).Scan(&done); err != nil {
			t.Fatalf("count completed repack jobs: %v", err)
		}
		if done == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("repack job did not complete; pack rows = %+v", f.packRows(t))
		}
		time.Sleep(50 * time.Millisecond)
	}
	if rows := f.packRows(t); len(rows) != 1 {
		t.Fatalf("pack rows after fold = %+v, want 1", rows)
	}
	f.checkColdHydrate(t)

	// Completed jobs do not participate in uniqueness: a later trigger
	// inserts a fresh job (which no-ops on the single remaining pack).
	if err := q.EnqueueRepack(ctx, f.repo); err != nil {
		t.Fatalf("enqueue after completion: %v", err)
	}
	if n := countJobs(); n != 2 {
		t.Fatalf("river holds %d repack jobs after completion + retrigger, want 2", n)
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

func TestLocalQueue_RepackCoalescesPerRepo(t *testing.T) {
	f := newRepackFixture(t)
	ctx := context.Background()
	for range 3 {
		f.push(t)
	}

	gate := make(chan struct{})
	entered := make(chan struct{}, 8)
	r := f.repacker(t.TempDir())
	r.BeforeSwap = func(context.Context) error {
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
	close(gate)
	q.Close() // waits for the in-flight fold

	if rows := f.packRows(t); len(rows) != 1 {
		t.Fatalf("pack rows after coalesced fold = %+v, want 1", rows)
	}
	if err := q.EnqueueRepack(ctx, f.repo); err == nil {
		t.Fatal("EnqueueRepack after Close must fail")
	}

	// A queue without a repacker must refuse rather than silently drop.
	bare := NewLocal(deliverer, func(context.Context, sync.Job) error { return nil }, nil, slog.New(slog.DiscardHandler))
	defer bare.Close()
	if err := bare.EnqueueRepack(ctx, f.repo); err == nil {
		t.Fatal("EnqueueRepack without a repacker must fail")
	}
}
