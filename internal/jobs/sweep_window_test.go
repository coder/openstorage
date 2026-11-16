// Sweeper-vs-writer window tests (claim B): a writer re-registers an orphan
// pack key between the sweeper's classify and its delete. The sweeper and the
// writer use real stores (sqlite registry, file blob store, real git packs).
// Decorators over blob.BlobStore and PackRegistry park the sweeper at chosen
// calls, so every interleaving here is deterministic.
package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/githttp"
	"github.com/coder/openstorage/internal/ref"
)

// windowPack is one pack the writer publishes: its key, local file and the
// commit its ref move points at.
type windowPack struct {
	id, key, path, commit string
	size                  int64
}

// mintOrphanPack builds a real pack for a new commit, uploads it, ages it
// past the sweep threshold and leaves it unregistered: an orphan the sweeper
// will classify for deletion. The local pack file stays for the writer.
func mintOrphanPack(t *testing.T, f *repackFixture) windowPack {
	t.Helper()
	ctx := context.Background()
	f.seq++
	blobSHA := strings.TrimSpace(testGit(t, f.scratch, fmt.Sprintf("window %d\n", f.seq), "hash-object", "-w", "--stdin"))
	treeSHA := strings.TrimSpace(testGit(t, f.scratch, fmt.Sprintf("100644 blob %s\tdata.txt\n", blobSHA), "mktree"))
	commitSHA := strings.TrimSpace(testGit(t, f.scratch, "", "commit-tree", treeSHA, "-m", fmt.Sprintf("w%d", f.seq)))
	packDir := t.TempDir()
	objs := blobSHA + "\n" + treeSHA + "\n" + commitSHA + "\n"
	id := strings.TrimSpace(testGit(t, f.scratch, objs, "pack-objects", "--threads=1", "-q", filepath.Join(packDir, "w")))
	p := windowPack{id: id, key: blob.PackKey(f.repo, id), path: filepath.Join(packDir, "w-"+id+".pack"), commit: commitSHA}
	fi, err := os.Stat(p.path)
	if err != nil {
		t.Fatalf("stat pack: %v", err)
	}
	p.size = fi.Size()
	if err := f.blobs.UploadFile(ctx, p.key, p.path); err != nil {
		t.Fatalf("upload orphan: %v", err)
	}
	past := time.Now().Add(-25 * time.Hour)
	if err := os.Chtimes(filepath.Join(f.blobDir, filepath.FromSlash(p.key)), past, past); err != nil {
		t.Fatalf("backdate orphan: %v", err)
	}
	return p
}

// The writer below runs the sweeper-relevant calls of api.publishPack with
// quota off, one step per helper so a test can interleave them: the Stat at
// publish.go:58, RegisterPacks at :175, blob.EnsurePublished at :200, then
// the caller's ref move.

// writerStat is publish.go:58. A present key is "published": no upload.
func writerStat(t *testing.T, f *repackFixture, p windowPack) (published bool) {
	t.Helper()
	_, err := f.blobs.Stat(context.Background(), p.key)
	if err != nil && !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("writer stat: %v", err)
	}
	return err == nil
}

// writerRegister is publish.go:175 for a published (adopted) pack.
func writerRegister(t *testing.T, f *repackFixture, p windowPack) {
	t.Helper()
	pack := ref.Pack{ID: p.id, Key: p.key, Size: p.size, Adopted: true}
	if err := f.store.RegisterPacks(context.Background(), f.repo, []ref.Pack{pack}); err != nil {
		t.Fatalf("writer register: %v", err)
	}
}

// writerVerify is publish.go:200: re-upload when the key is missing.
func writerVerify(t *testing.T, f *repackFixture, p windowPack) (restored bool) {
	t.Helper()
	restored, err := blob.EnsurePublished(context.Background(), f.blobs, p.key, p.path, nil, nil)
	if err != nil {
		t.Fatalf("writer verify: %v", err)
	}
	return restored
}

// writerMoveRef is the ref move the caller makes once publishPack returns.
func writerMoveRef(t *testing.T, f *repackFixture, p windowPack) {
	t.Helper()
	won, err := f.store.CASRef(context.Background(), f.repo, "refs/heads/main", f.tip, p.commit)
	if err != nil || !won {
		t.Fatalf("writer ref move: won=%v err=%v", won, err)
	}
	f.tip = p.commit
}

// windowBlobs is the sweeper's blob store. It calls stop before the
// sweeper deletes key ("delete") and before it copies a backup back onto key
// ("restore"), and records when each inner call returned.
type windowBlobs struct {
	blob.BlobStore
	key                   string
	stop                  func(step string) error
	deletedAt, restoredAt time.Time
}

func (b *windowBlobs) Delete(ctx context.Context, key string) error {
	if key != b.key {
		return b.BlobStore.Delete(ctx, key)
	}
	if err := b.stop("delete"); err != nil {
		return err
	}
	err := b.BlobStore.Delete(ctx, key)
	b.deletedAt = time.Now()
	return err
}

func (b *windowBlobs) Copy(ctx context.Context, dst, src string) error {
	if dst != b.key { // the quarantine copy writes the backup key
		return b.BlobStore.Copy(ctx, dst, src)
	}
	if err := b.stop("restore"); err != nil {
		return err
	}
	err := b.BlobStore.Copy(ctx, dst, src)
	b.restoredAt = time.Now()
	return err
}

// windowPacks is the sweeper's registry. Its third HasPack of key is
// safeDelete's post-delete recheck (1 = classify, 2 = revalidateOrphan), and
// it calls stop before that read.
type windowPacks struct {
	PackRegistry
	key   string
	calls int
	stop  func(step string) error
}

func (p *windowPacks) HasPack(ctx context.Context, repo, key string) (bool, error) {
	if key == p.key {
		p.calls++
		if p.calls == 3 {
			if err := p.stop("recheck"); err != nil {
				return false, err
			}
		}
	}
	return p.PackRegistry.HasPack(ctx, repo, key)
}

// parker parks the sweeper goroutine at a step until the test resumes it.
type parker struct {
	at     chan string
	resume chan error
}

func newParker() *parker { return &parker{at: make(chan string), resume: make(chan error)} }

func (p *parker) stop(step string) error {
	p.at <- step
	return <-p.resume
}

func (p *parker) expect(t *testing.T, step string) {
	t.Helper()
	select {
	case got := <-p.at:
		if got != step {
			t.Fatalf("sweeper parked at %q, want %q", got, step)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("sweeper never reached %q", step)
	}
}

func windowSweeper(f *repackFixture, key string, stop func(string) error) (*Sweeper, *windowBlobs) {
	bl := &windowBlobs{BlobStore: f.blobs, key: key, stop: stop}
	pk := &windowPacks{PackRegistry: f.store, key: key, stop: stop}
	return NewSweeper(bl, pk, SweepConfig{Threshold: 24 * time.Hour, Delete: true}, slog.New(slog.DiscardHandler)), bl
}

// Sweeper.Sweep's doc comment says that a reader racing the restore is
// transient. The hydration retry must wait for the restore: a cold hydrate that
// starts inside the window, with the restore 200 ms away, used to fail with
// ErrPackMissing within milliseconds, because the retry listed again 8 times
// with no wait.
func TestSweepWindow_ColdHydrateInWindowWaitsForRestore(t *testing.T) {
	f := newRepackFixture(t)
	f.push(t)
	p := mintOrphanPack(t, f)
	park := newParker()
	s, _ := windowSweeper(f, p.key, park.stop)
	done := make(chan struct{})
	go func() { defer close(done); _, _ = s.Sweep(context.Background()) }()

	park.expect(t, "delete")
	writerStat(t, f, p)
	writerRegister(t, f, p)
	writerVerify(t, f, p)
	writerMoveRef(t, f, p)
	park.resume <- nil
	park.expect(t, "recheck")
	park.resume <- nil
	park.expect(t, "restore")

	repoPath, err := githttp.EnsureCacheRepo(t.TempDir(), f.repo)
	if err != nil {
		t.Fatalf("EnsureCacheRepo: %v", err)
	}
	// The restore waits until the hydration has read the missing pack, so that
	// the test always enters the window, whatever the scheduler does.
	seen := &notFoundSignal{BlobStore: f.blobs, seen: make(chan struct{})}
	hydrated := make(chan error, 1)
	go func() { hydrated <- githttp.HydratePacks(context.Background(), f.store, seen, repoPath, f.repo) }()
	select {
	case <-seen.seen:
	case <-time.After(30 * time.Second):
		t.Fatal("the hydration never read the missing pack")
	}
	time.Sleep(200 * time.Millisecond) // the restore is 200 ms away from the first miss
	park.resume <- nil                 // the restore copy runs
	<-done
	if err := <-hydrated; err != nil {
		t.Fatalf("cold hydrate that started 200ms before the sweeper's restore: %v", err)
	}
}

// notFoundSignal closes seen at the first download that finds no blob.
type notFoundSignal struct {
	blob.BlobStore
	once sync.Once
	seen chan struct{}
}

func (n *notFoundSignal) DownloadFile(ctx context.Context, key, path string) error {
	err := n.BlobStore.DownloadFile(ctx, key, path)
	if errors.Is(err, blob.ErrNotFound) {
		n.once.Do(func() { close(n.seen) })
	}
	return err
}
