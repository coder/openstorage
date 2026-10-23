package blob

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// openFile opens a File store rooted at a fresh temp dir. Zero containers.
func openFile(t *testing.T) (*File, string) {
	t.Helper()
	dir := t.TempDir()
	b, err := OpenFile("file://" + dir)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return b, dir
}

// seededFile builds a File store containing key→content (seeded through
// UploadFile, exercising the write path).
func seededFile(t *testing.T, key, content string) BlobStore {
	t.Helper()
	b, _ := openFile(t)
	src := filepath.Join(t.TempDir(), "src")
	if err := os.WriteFile(src, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := b.UploadFile(context.Background(), key, src); err != nil {
		t.Fatalf("UploadFile: %v", err)
	}
	return b
}

// TestDownloadFileCapped (Codex round 117 on #104): a mutable legacy key
// can grow between a caller's Stat and the GET; the capped download must
// abort with ErrCapExceeded instead of consuming unreserved disk, while
// an exactly-at-cap object still downloads whole.
func TestDownloadFileCapped(t *testing.T) {
	ctx := context.Background()
	b := seededFile(t, "k", "0123456789") // 10 bytes
	dest := filepath.Join(t.TempDir(), "snap")

	if err := b.DownloadFileCapped(ctx, "k", dest, 10); err != nil {
		t.Fatalf("cap == size: %v, want success", err)
	}
	if got, err := os.ReadFile(dest); err != nil || string(got) != "0123456789" {
		t.Fatalf("capped download content = (%q, %v)", got, err)
	}
	err := b.DownloadFileCapped(ctx, "k", filepath.Join(t.TempDir(), "snap2"), 9)
	if !errors.Is(err, ErrCapExceeded) {
		t.Fatalf("cap < size: %v, want ErrCapExceeded — the stream must not outgrow the reservation", err)
	}
	if err := b.DownloadFileCapped(ctx, "missing", dest, 10); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing key: %v, want ErrNotFound", err)
	}
}

func TestOpenFile_URLValidation(t *testing.T) {
	cases := map[string]struct {
		url     string
		wantErr string
	}{
		"wrong scheme":   {"s3://bucket", `scheme "s3" unsupported`},
		"non-empty host": {"file://host/dir", "empty host"},
		"no path":        {"file://", "no directory path"},
		"unparseable":    {":no-scheme", "parse blob URL"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := OpenFile(tc.url)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("OpenFile(%q) = %v, want error containing %q", tc.url, err, tc.wantErr)
			}
		})
	}
}

// OpenFile must create the root directory up front: hooks reconstruct stores
// from env URLs without ever calling EnsureBucket.
func TestOpenFile_CreatesDirUpFront(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "a", "b", "blobs")
	b, err := OpenFile("file://" + dir)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer b.Close()
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		t.Fatalf("blob dir not created: info=%v err=%v", info, err)
	}
}

func TestFile_EnsureBucketIdempotent(t *testing.T) {
	ctx := context.Background()
	b, dir := openFile(t)

	// Twice on an existing dir (idempotence), then again after the dir
	// vanished (recreation, mirroring EnsureBucket on a fresh backend).
	for i := range 2 {
		if err := b.EnsureBucket(ctx); err != nil {
			t.Fatalf("EnsureBucket #%d: %v", i+1, err)
		}
	}
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := b.EnsureBucket(ctx); err != nil {
		t.Fatalf("EnsureBucket after removal: %v", err)
	}
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		t.Fatalf("blob dir not recreated: info=%v err=%v", info, err)
	}
}

// Round-trip through the PackKey/PackPrefix helpers: upload packs for two
// repos, list one repo's prefix, download a pack back, and check the on-disk
// layout is clean (real pack files, no .attrs metadata sidecars).
func TestFile_PackKeyRoundTrip(t *testing.T) {
	ctx := context.Background()
	b, dir := openFile(t)

	packs := map[string]string{
		PackKey("r1", "abc"): "pack-abc-bytes",
		PackKey("r1", "def"): "pack-def-bytes-longer",
		PackKey("r2", "zzz"): "other-repo",
	}
	srcDir := t.TempDir()
	for key, content := range packs {
		src := filepath.Join(srcDir, filepath.Base(key))
		if err := os.WriteFile(src, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := b.UploadFile(ctx, key, src); err != nil {
			t.Fatalf("UploadFile(%q): %v", key, err)
		}
	}

	listed, err := b.List(ctx, PackPrefix("r1"))
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	want := map[string]int64{
		PackKey("r1", "abc"): int64(len(packs[PackKey("r1", "abc")])),
		PackKey("r1", "def"): int64(len(packs[PackKey("r1", "def")])),
	}
	if len(listed) != len(want) {
		t.Fatalf("List returned %d objects (%v), want %d", len(listed), listed, len(want))
	}
	for _, obj := range listed {
		size, ok := want[obj.Key]
		if !ok || obj.Size != size {
			t.Fatalf("List returned %+v, want keys/sizes %v", obj, want)
		}
	}

	dest := filepath.Join(t.TempDir(), "back.pack")
	if err := b.DownloadFile(ctx, PackKey("r1", "def"), dest); err != nil {
		t.Fatalf("DownloadFile: %v", err)
	}
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != packs[PackKey("r1", "def")] {
		t.Fatalf("downloaded content = %q, want %q", got, packs[PackKey("r1", "def")])
	}

	// On-disk layout: keys map to plain files, and MetadataDontWrite means no
	// sidecars pollute pack listings.
	entries, err := filepath.Glob(filepath.Join(dir, "packs", "r1", "*"))
	if err != nil {
		t.Fatal(err)
	}
	wantFiles := []string{
		filepath.Join(dir, "packs", "r1", "pack-abc.pack"),
		filepath.Join(dir, "packs", "r1", "pack-def.pack"),
	}
	if len(entries) != len(wantFiles) {
		t.Fatalf("on-disk entries = %v, want exactly %v (no sidecars)", entries, wantFiles)
	}
	for i, want := range wantFiles {
		if entries[i] != want {
			t.Fatalf("on-disk entries = %v, want %v", entries, wantFiles)
		}
	}
}

// TestFile_StatAndSignedURL pins the phase-6 LFS foundation contract on the
// filesystem backend: Stat reports the exact size, missing keys match
// ErrNotFound via errors.Is, and SignedURL yields ErrSignedURLUnsupported
// (the proxy-fallback trigger).
func TestFile_StatAndSignedURL(t *testing.T) {
	ctx := context.Background()
	key := LFSKey("r1", strings.Repeat("a", 64))
	b := seededFile(t, key, "hello lfs")

	size, err := b.Stat(ctx, key)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if want := int64(len("hello lfs")); size != want {
		t.Fatalf("Stat size = %d, want %d", size, want)
	}

	if _, err := b.Stat(ctx, LFSKey("r1", strings.Repeat("b", 64))); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Stat of missing key = %v, want ErrNotFound", err)
	}

	if _, err := b.SignedURL(ctx, key, http.MethodGet, time.Hour); !errors.Is(err, ErrSignedURLUnsupported) {
		t.Fatalf("SignedURL on fileblob = %v, want ErrSignedURLUnsupported", err)
	}
}

// A failed download must never leave anything at destPath (atomic contract),
// and a missing key must match ErrNotFound via errors.Is — the hydration
// missing-pack retry keys on it.
func TestFile_DownloadMissingKey(t *testing.T) {
	b, _ := openFile(t)
	dest := filepath.Join(t.TempDir(), "missing.pack")
	if err := b.DownloadFile(context.Background(), PackKey("r1", "nope"), dest); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DownloadFile of missing key = %v, want ErrNotFound", err)
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatalf("destPath must not exist after failed download, stat err = %v", err)
	}
}

// TestFile_UploadNeverUsesOSTempDir: fileblob must stage upload temp files
// next to their destination (NoTempDir), not in os.TempDir. When the bucket
// dir and os.TempDir are different mounts (e.g. --data-dir on /home, tmpfs
// /tmp), the publish rename otherwise fails with EXDEV "invalid cross-device
// link" on every pack upload. Cross-device mounts can't be fabricated in a
// unit test, so it pins the invariant portably: with TMPDIR pointing at an
// unwritable directory, any writer that staged in os.TempDir would fail.
func TestFile_UploadNeverUsesOSTempDir(t *testing.T) {
	b, _ := openFile(t)
	src := filepath.Join(t.TempDir(), "src")
	if err := os.WriteFile(src, []byte("pack bytes"), 0o644); err != nil {
		t.Fatal(err)
	}

	unwritable := filepath.Join(t.TempDir(), "no-write")
	if err := os.Mkdir(unwritable, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(unwritable, 0o700) })
	t.Setenv("TMPDIR", unwritable)
	if f, err := os.CreateTemp("", "probe-*"); err == nil {
		f.Close()
		t.Skip("os.TempDir still writable (running as root?); cannot pin the invariant")
	}

	if err := b.UploadFile(context.Background(), PackKey("r1", "exdev"), src); err != nil {
		t.Fatalf("UploadFile must not depend on os.TempDir: %v", err)
	}
}

// ModTime must be populated by List (the orphan-pack sweeper's age source)
// and track the underlying object's last-modified time.
func TestFile_ListModTime(t *testing.T) {
	ctx := context.Background()
	b, dir := openFile(t)
	src := filepath.Join(t.TempDir(), "src")
	if err := os.WriteFile(src, []byte("pack"), 0o644); err != nil {
		t.Fatal(err)
	}
	key := PackKey("r1", "aged")
	if err := b.UploadFile(ctx, key, src); err != nil {
		t.Fatalf("UploadFile: %v", err)
	}

	listed, err := b.List(ctx, PackPrefix("r1"))
	if err != nil || len(listed) != 1 {
		t.Fatalf("List = %v, %v; want one object", listed, err)
	}
	if age := time.Since(listed[0].ModTime); listed[0].ModTime.IsZero() || age < 0 || age > time.Minute {
		t.Fatalf("fresh upload ModTime = %v (age %v), want ~now", listed[0].ModTime, age)
	}

	// Backdating the underlying file must surface through List: that is
	// exactly how sweeper tests age orphans.
	past := time.Now().Add(-25 * time.Hour)
	if err := os.Chtimes(filepath.Join(dir, "packs", "r1", "pack-aged.pack"), past, past); err != nil {
		t.Fatal(err)
	}
	listed, err = b.List(ctx, PackPrefix("r1"))
	if err != nil || len(listed) != 1 {
		t.Fatalf("List after backdate = %v, %v", listed, err)
	}
	if got := listed[0].ModTime; got.After(past.Add(time.Second)) || got.Before(past.Add(-time.Second)) {
		t.Fatalf("backdated ModTime = %v, want ~%v", got, past)
	}
}

// Delete removes exactly the addressed object; a missing key yields
// ErrNotFound (the sweeper tolerates concurrent deletion races via that
// sentinel).
func TestFile_Delete(t *testing.T) {
	ctx := context.Background()
	keep := PackKey("r1", "keep")
	victim := PackKey("r1", "victim")
	b := seededFile(t, keep, "keep-bytes")
	src := filepath.Join(t.TempDir(), "src")
	if err := os.WriteFile(src, []byte("victim-bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := b.UploadFile(ctx, victim, src); err != nil {
		t.Fatalf("UploadFile: %v", err)
	}

	if err := b.Delete(ctx, victim); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := b.Stat(ctx, victim); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Stat after delete = %v, want ErrNotFound", err)
	}
	if _, err := b.Stat(ctx, keep); err != nil {
		t.Fatalf("neighbor object must survive: %v", err)
	}
	if err := b.Delete(ctx, victim); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Delete of missing key = %v, want ErrNotFound", err)
	}
}

// Copy duplicates objects inside the store without local staging; missing
// sources map to ErrNotFound (the sweeper's gone-already signal).
func TestFile_Copy(t *testing.T) {
	ctx := context.Background()
	src := PackKey("r1", "src")
	b := seededFile(t, src, "copy-me")

	dst := "sweep/quarantine/" + src
	if err := b.Copy(ctx, dst, src); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	for _, key := range []string{src, dst} {
		size, err := b.Stat(ctx, key)
		if err != nil || size != int64(len("copy-me")) {
			t.Fatalf("Stat(%q) = %d, %v; want %d", key, size, err, len("copy-me"))
		}
	}
	if err := b.Copy(ctx, "x", PackKey("r1", "missing")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Copy of missing source = %v, want ErrNotFound", err)
	}
}

// EnsurePublished is the writer half of the orphan-sweep coordination:
// no-op when the key exists, re-upload when a sweeper deleted it between
// upload and registration commit.
func TestEnsurePublished(t *testing.T) {
	ctx := context.Background()
	key := PackKey("r1", "verify")
	b := seededFile(t, key, "pack-bytes")
	src := filepath.Join(t.TempDir(), "src")
	if err := os.WriteFile(src, []byte("pack-bytes"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Present: no restore, and onRestore must NOT run (it accounts quota
	// bytes — a false invocation would reserve for bytes never absent).
	restored, err := EnsurePublished(ctx, b, key, src, func(context.Context, int64) error {
		t.Fatal("onRestore ran for a present blob")
		return nil
	}, nil)
	if err != nil || restored {
		t.Fatalf("EnsurePublished(present) = (%v, %v), want (false, nil)", restored, err)
	}
	if err := b.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	// Missing: onRestore runs BEFORE the upload (the cover must exist by
	// the time the bytes become physical — Codex round 58) with the local
	// file's size, the restore is reported, and a post-publication
	// refresh follows (Codex round 59) once the bytes are back.
	coverCalls := 0
	restored, err = EnsurePublished(ctx, b, key, src, func(cctx context.Context, size int64) error {
		coverCalls++
		if size != int64(len("pack-bytes")) {
			t.Fatalf("onRestore size = %d, want %d", size, len("pack-bytes"))
		}
		if coverCalls == 1 {
			// The initial cover must precede publication.
			if _, serr := b.Stat(cctx, key); !errors.Is(serr, ErrNotFound) {
				t.Fatalf("bytes already physical when the initial cover ran (stat: %v)", serr)
			}
		}
		return nil
	}, nil)
	if err != nil || !restored || coverCalls != 2 {
		t.Fatalf("EnsurePublished(missing) = (%v, %v), onRestore calls %d; want (true, nil), 2 (initial cover + post-publication refresh)", restored, err, coverCalls)
	}
	size, err := b.Stat(ctx, key)
	if err != nil || size != int64(len("pack-bytes")) {
		t.Fatalf("Stat after re-publish = %d, %v", size, err)
	}
	// An onRestore failure ABORTS the restore: uncovered bytes must never
	// become physical (the retry converges once the cover succeeds).
	if err := b.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	restored, err = EnsurePublished(ctx, b, key, src, func(context.Context, int64) error {
		return errors.New("cover denied")
	}, nil)
	if err == nil || restored {
		t.Fatalf("EnsurePublished(cover denied) = (%v, %v), want (false, error)", restored, err)
	}
	if _, serr := b.Stat(ctx, key); !errors.Is(serr, ErrNotFound) {
		t.Fatalf("uncovered restore published bytes anyway (stat: %v)", serr)
	}
	if _, err := EnsurePublished(ctx, b, key, src, nil, nil); err != nil {
		t.Fatal(err) // nil onRestore: re-seed for the next case
	}

	// A canceled request context must not skip the verification: the
	// registration it covers already committed (Codex round 14).
	if err := b.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := EnsurePublished(canceled, b, key, src, nil, nil); err != nil {
		t.Fatalf("EnsurePublished(canceled ctx): %v", err)
	}
	if _, err := b.Stat(ctx, key); err != nil {
		t.Fatalf("Stat after canceled-ctx re-publish: %v", err)
	}
}

// slowUploadStore delays UploadFile, simulating a restore that outlives a
// near-minimum reservation TTL (Codex round 59).
type slowUploadStore struct {
	BlobStore
	delay time.Duration
}

func (s slowUploadStore) UploadFile(ctx context.Context, key, path string) error {
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.BlobStore.UploadFile(ctx, key, path)
}

// TestEnsurePublishedRenewsCoverThroughPublication (Codex round 59): the
// restore allowance deliberately exceeds the minimum reservation TTL, so
// one cover reservation cannot span a slow repair upload — the cover
// could expire mid-restore, letting a stale-floor admission commit before
// the bytes become physical. The seam must renew the cover through the
// upload and refresh it once after publication (the restarted TTL clock
// outlives the last stale-floor commit); a failed post-publication
// refresh is surfaced WITH the restore reported, so callers still bump
// the floor generation.
//
// The test runs in a synctest bubble with the production cadence (#545):
// with millisecond knobs on the wall clock, a host stall of a few tens of
// milliseconds aged the cover past its budget and the seam correctly
// reported a gap. Fake time advances only while every goroutine in the
// bubble is blocked, so the test asserts the exact deadlines instead.
func TestEnsurePublishedRenewsCoverThroughPublication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		// The upload outlives the continuity budget (so renewals must carry
		// it) and ends between two ticks (so the call count has no tie).
		const uploadDelay = 110 * time.Second
		if uploadDelay <= coverLapseBudget || uploadDelay%coverRenewInterval == 0 {
			t.Fatalf("uploadDelay %v must exceed coverLapseBudget %v and fall between ticks of %v", uploadDelay, coverLapseBudget, coverRenewInterval)
		}

		key := PackKey("r1", "renewed")
		b := seededFile(t, key, "pack-bytes")
		src := filepath.Join(t.TempDir(), "src")
		if err := os.WriteFile(src, []byte("pack-bytes"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := b.Delete(ctx, key); err != nil {
			t.Fatal(err)
		}

		slow := slowUploadStore{BlobStore: b, delay: uploadDelay}
		var calls atomic.Int32
		start := time.Now()
		restored, err := EnsurePublished(ctx, slow, key, src, func(context.Context, int64) error {
			calls.Add(1)
			return nil
		}, nil)
		if err != nil || !restored {
			t.Fatalf("EnsurePublished(slow restore) = (%v, %v), want (true, nil)", restored, err)
		}
		if got := time.Since(start); got != uploadDelay {
			t.Fatalf("slow restore took %v, want exactly the upload delay %v", got, uploadDelay)
		}
		// 1 initial + one renewal per tick during the upload + 1
		// post-publication refresh.
		if got, want := calls.Load(), int32(1+uploadDelay/coverRenewInterval+1); got != want {
			t.Fatalf("cover calls = %d, want %d (initial + renewals through the upload + post-publication refresh)", got, want)
		}

		// Once every renewal fails, continuity runs out MID-UPLOAD and the
		// restore aborts (Codex rounds 62+63): uncovered bytes must never
		// become physical. The abort lands exactly at the budget deadline,
		// measured from the initial cover.
		if err := b.Delete(ctx, key); err != nil {
			t.Fatal(err)
		}
		calls.Store(0)
		start = time.Now()
		restored, err = EnsurePublished(ctx, slow, key, src, func(context.Context, int64) error {
			if calls.Add(1) == 1 {
				return nil // the initial cover succeeds
			}
			return errors.New("ledger unreachable") // every renewal fails
		}, nil)
		if restored || err == nil || !strings.Contains(err.Error(), "restore cover continuity lost") {
			t.Fatalf("EnsurePublished(dead ledger) = (%v, %v), want an aborted, unpublished restore", restored, err)
		}
		if got := time.Since(start); got != coverLapseBudget {
			t.Fatalf("dead-ledger restore aborted after %v, want exactly the continuity budget %v", got, coverLapseBudget)
		}
		if got, want := calls.Load(), int32(1+coverLapseBudget/coverRenewInterval); got != want {
			t.Fatalf("dead-ledger cover calls = %d, want %d (initial + failed renewals before the deadline)", got, want)
		}
		if _, serr := b.Stat(ctx, key); !errors.Is(serr, ErrNotFound) {
			t.Fatalf("aborted restore published anyway (stat: %v)", serr)
		}
	})
}

// TestWithRenewedCoverAbortsOnLapse (Codex round 62): a renewal stalling
// past the cover's TTL used to block the renewal loop while the restore
// upload continued on its own healthy connection — the cover row expired
// mid-restore and the uncovered bytes still became physical. Each renewal
// attempt is now deadline-bounded (a stall costs one attempt, not the
// loop), and once coverLapseBudget passes without a successful renewal
// the op's context is canceled: an uncovered restore must never publish.
//
// The WithRenewedCover tests run in synctest bubbles with the production
// cadence (#549): fake time advances only while every goroutine in the
// bubble is blocked, so a host stall cannot move a deadline and each test
// asserts exact times instead of wall-clock slack.
func TestWithRenewedCoverAbortsOnLapse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// The first stalled attempt gets its full timeout; the second
		// starts with less than that left in the budget.
		if coverRenewInterval+coverRenewTimeout > coverLapseBudget || 2*coverRenewInterval >= coverLapseBudget || 2*coverRenewInterval+coverRenewTimeout <= coverLapseBudget {
			t.Fatalf("cadence %v/%v/%v no longer caps the second stalled attempt at the budget", coverRenewInterval, coverRenewTimeout, coverLapseBudget)
		}
		// The initial cover succeeds; every later renewal stalls until its
		// per-attempt deadline: the loop must keep making (failing) attempts
		// and abort the op at the budget.
		start := time.Now()
		var deadlines []time.Duration // per renewal, relative to start
		renew := func(rctx context.Context) error {
			d, ok := rctx.Deadline()
			if !ok {
				return errors.New("unbounded renewal attempt")
			}
			deadlines = append(deadlines, d.Sub(start))
			if len(deadlines) == 1 {
				return nil
			}
			<-rctx.Done()
			return rctx.Err()
		}
		var abortedAt time.Duration
		opErr, renewErr := WithRenewedCover(context.Background(), renew, func(opCtx context.Context) error {
			select {
			case <-opCtx.Done():
				abortedAt = time.Since(start)
				return context.Cause(opCtx)
			case <-time.After(time.Hour):
				return nil // the abort never came: uncovered bytes would publish
			}
		}, nil)
		if opErr == nil || !strings.Contains(opErr.Error(), "restore cover continuity lost") {
			t.Fatalf("opErr = %v, want a cover-lapse abort", opErr)
		}
		if renewErr != nil {
			t.Fatalf("renewErr = %v, want nil (op failed, no final refresh)", renewErr)
		}
		if abortedAt != coverLapseBudget {
			t.Fatalf("op aborted after %v, want exactly the continuity budget %v", abortedAt, coverLapseBudget)
		}
		// Initial attempt, then one stalled attempt per tick: the first
		// gets the full per-attempt timeout, the last is capped at the
		// remaining budget (Codex round 66).
		want := []time.Duration{
			coverRenewTimeout,
			coverRenewInterval + coverRenewTimeout,
			coverLapseBudget,
		}
		if !slices.Equal(deadlines, want) {
			t.Fatalf("renewal deadlines = %v, want %v", deadlines, want)
		}
	})
}

// TestWithRenewedCoverRejectsLateSuccess (Codex round 63): a renewal that
// SUCCEEDS but completes after the prior cover's budget window cannot
// prove continuity — the prior row may have expired mid-attempt, leaving
// an uncovered gap a stale-floor admission can commit in — so it must
// abort exactly like a lapse instead of resetting the coverage anchor.
func TestWithRenewedCoverRejectsLateSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// The initial cover is prompt. Every renewal SUCCEEDS, but it
		// ignores its deadline and completes only after the budget window.
		const latency = 30 * time.Second
		if coverRenewInterval+latency <= coverLapseBudget {
			t.Fatalf("latency %v must end the first renewal past the budget %v", latency, coverLapseBudget)
		}
		calls := 0
		renew := func(context.Context) error {
			calls++
			if calls > 1 {
				time.Sleep(latency)
			}
			return nil
		}
		start := time.Now()
		var abortedAt time.Duration
		opErr, _ := WithRenewedCover(context.Background(), renew, func(opCtx context.Context) error {
			select {
			case <-opCtx.Done():
				abortedAt = time.Since(start)
				return context.Cause(opCtx)
			case <-time.After(time.Hour):
				return nil // late successes were accepted: the gap went unnoticed
			}
		}, nil)
		// The abort comes from the late success itself (its error is nil),
		// when it completes, not from the budget timer.
		if opErr == nil || !strings.Contains(opErr.Error(), "restore cover continuity lost (last renewal: <nil>)") {
			t.Fatalf("opErr = %v, want a continuity abort on the late-completing renewal", opErr)
		}
		if want := coverRenewInterval + latency; abortedAt != want {
			t.Fatalf("op aborted after %v, want %v (when the first late renewal completed)", abortedAt, want)
		}
		if calls != 2 {
			t.Fatalf("renew calls = %d, want 2 (initial + the rejected late renewal)", calls)
		}
	})
}

// TestWithRenewedCoverBudgetExpiryAbortsBetweenTicks (Codex round 66):
// the continuity abort was TICK-driven — quick failures early left the
// next tick scheduled past covered+budget, and an attempt starting near
// the budget edge could run its full per-attempt timeout — in both cases
// op kept running while the prior cover aged out, and a publication in
// that gap landed uncovered. The budget deadline must be enforced by its
// own timer: op is canceled AT covered+coverLapseBudget, not at whatever
// later tick happens to notice.
func TestWithRenewedCoverBudgetExpiryAbortsBetweenTicks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// The budget ends between two ticks: only a budget-deadline timer
		// can abort on time.
		if coverLapseBudget%coverRenewInterval == 0 {
			t.Fatalf("budget %v falls on a tick of %v", coverLapseBudget, coverRenewInterval)
		}
		calls := 0
		renew := func(context.Context) error {
			calls++
			if calls == 1 {
				return nil // the initial cover succeeds; nothing renews it
			}
			return errors.New("ledger down")
		}
		start := time.Now()
		var abortedAt time.Duration
		opErr, _ := WithRenewedCover(context.Background(), renew, func(opCtx context.Context) error {
			select {
			case <-opCtx.Done():
				abortedAt = time.Since(start)
				return context.Cause(opCtx)
			case <-time.After(time.Hour):
				return nil // never aborted: uncovered bytes would publish
			}
		}, nil)
		if opErr == nil || !strings.Contains(opErr.Error(), "budget expired without a renewed cover") {
			t.Fatalf("opErr = %v, want a budget-expiry abort", opErr)
		}
		if abortedAt != coverLapseBudget {
			t.Fatalf("op aborted after %v, want exactly the continuity budget %v (the budget timer, not the next tick)", abortedAt, coverLapseBudget)
		}
		if want := 1 + int(coverLapseBudget/coverRenewInterval); calls != want {
			t.Fatalf("renew calls = %d, want %d (initial + one failure per tick before the deadline)", calls, want)
		}
	})
}

// TestWithRenewedCoverTakesInitialCoverItself (Codex round 64): a
// caller-side initial reservation with the anchor captured at helper
// entry let a process suspension between the caller's commit and entry
// fabricate a fresh anchor over an already-aging row — publication could
// then run past the real cover's expiry. The helper must take the
// initial cover ITSELF, anchored at the attempt's own start, before any
// op byte can become physical; an initial-cover failure prevents op
// entirely.
func TestWithRenewedCoverTakesInitialCoverItself(t *testing.T) {
	var renews, opStarts atomic.Int32
	opErr, renewErr := WithRenewedCover(context.Background(), func(context.Context) error {
		renews.Add(1)
		return nil
	}, func(context.Context) error {
		if renews.Load() == 0 {
			t.Fatal("op started before the initial cover committed")
		}
		opStarts.Add(1)
		return nil
	}, nil)
	if opErr != nil || renewErr != nil || opStarts.Load() != 1 {
		t.Fatalf("WithRenewedCover = (%v, %v), op starts %d; want (nil, nil), 1", opErr, renewErr, opStarts.Load())
	}

	// An initial-cover failure aborts before op runs.
	opStarts.Store(0)
	opErr, _ = WithRenewedCover(context.Background(), func(context.Context) error {
		return errors.New("ledger down")
	}, func(context.Context) error {
		opStarts.Add(1)
		return nil
	}, nil)
	if opErr == nil || !strings.Contains(opErr.Error(), "initial restore cover") || opStarts.Load() != 0 {
		t.Fatalf("WithRenewedCover(initial failure) = %v, op starts %d; want an initial-cover abort with op never run", opErr, opStarts.Load())
	}
}

// TestWithRenewedCoverHandoffRetriesUnderLiveCover (Codex round 70): the
// caller's invalidation used to be one best-effort attempt after the seam
// returned — if it failed, the durable restored bytes were left with
// neither cover nor invalidation, permanently (a retry of the original
// write sees the blob present and never restores again). The handoff now
// runs INSIDE the seam: each attempt bounded and executed under a freshly
// proven cover, retried — alternating with renewals — until it commits;
// only lost continuity gives up (and surfaces renewErr).
func TestWithRenewedCoverHandoffRetriesUnderLiveCover(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		var renews int32
		renew := func(context.Context) error {
			renews++
			return nil // the cover stays renewable throughout
		}
		var handoffAt []time.Duration
		lastRenewsSeen := int32(0)
		handoff := func(hctx context.Context) error {
			handoffAt = append(handoffAt, time.Since(start))
			if d, ok := hctx.Deadline(); !ok || time.Until(d) != CoverHandoffTimeout {
				t.Fatalf("handoff attempt %d deadline (%v, %v), want exactly %v ahead", len(handoffAt), d, ok, CoverHandoffTimeout)
			}
			if renews <= lastRenewsSeen {
				t.Fatalf("handoff attempt %d ran without a fresh cover renewal since the last attempt", len(handoffAt))
			}
			lastRenewsSeen = renews
			if len(handoffAt) < 3 {
				return errors.New("ledger transiently down") // the invalidation must be RETRIED
			}
			return nil
		}
		opErr, renewErr := WithRenewedCover(context.Background(), renew, func(context.Context) error { return nil }, handoff)
		if opErr != nil || renewErr != nil {
			t.Fatalf("WithRenewedCover = (%v, %v), want (nil, nil) once the handoff lands", opErr, renewErr)
		}
		// Two transient failures, each retried one renewal interval later
		// under a fresh cover.
		if want := []time.Duration{0, coverRenewInterval, 2 * coverRenewInterval}; !slices.Equal(handoffAt, want) {
			t.Fatalf("handoff attempts at %v, want %v", handoffAt, want)
		}
		if renews != 4 {
			t.Fatalf("renew calls = %d, want 4 (initial + one fresh cover before each handoff attempt)", renews)
		}
	})
}

// TestWithRenewedCoverTailPersistsThroughContinuityLoss (Codex round 72,
// security): continuity loss after publication used to abandon BOTH
// repairs immediately, while absence-window floors minted before
// publication stay admissible for the full stale-floor tail — the last
// cover could expire with neither invalidation nor re-cover. After a
// loss the seam must keep attempting both through the tail: a handoff
// success still invalidates every stale floor.
func TestWithRenewedCoverTailPersistsThroughContinuityLoss(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// A tail long enough for three post-loss attempts on the renewal
		// cadence, so no attempt lands on the tail's edge.
		oldTail := staleFloorTail
		staleFloorTail = coverLapseBudget + 3*coverRenewInterval
		t.Cleanup(func() { staleFloorTail = oldTail })

		calls := 0
		renew := func(context.Context) error {
			calls++
			if calls == 1 {
				return nil // the initial cover; every later renewal fails
			}
			return errors.New("ledger down")
		}
		start := time.Now()
		var handoffAt []time.Duration
		handoff := func(context.Context) error {
			handoffAt = append(handoffAt, time.Since(start))
			if len(handoffAt) < 3 {
				return errors.New("ledger still down") // recovery on the 3rd try
			}
			return nil
		}
		opErr, renewErr := WithRenewedCover(context.Background(), renew, func(context.Context) error { return nil }, handoff)
		if opErr != nil {
			t.Fatalf("opErr = %v, want nil", opErr)
		}
		// The repairs landed — but a post-gap success must REPORT the gap
		// (Codex round 73): a write settled in the uncovered window cannot
		// be revoked, so a gapped restore is never clean.
		if renewErr == nil || !strings.Contains(renewErr.Error(), "uncovered cover gap") {
			t.Fatalf("renewErr = %v, want the gap report on a post-loss repair", renewErr)
		}
		// The loss is noticed at the budget deadline, and the handoff is
		// retried on the renewal cadence inside the tail.
		want := []time.Duration{coverLapseBudget, coverLapseBudget + coverRenewInterval, coverLapseBudget + 2*coverRenewInterval}
		if !slices.Equal(handoffAt, want) {
			t.Fatalf("handoff attempts at %v, want %v (post-loss retries through the stale-floor tail)", handoffAt, want)
		}
	})
}

// TestWithRenewedCoverAbandonsAfterStaleFloorTail (Codex rounds 67+72):
// when the ledger never recovers, the post-publication loop gives up only
// once the stale-floor tail has passed — every floor that could hide the
// restored bytes has expired by then, so abandoning is safe — and the
// abandonment is reported. It must not give up earlier (round 72: floors
// outlive the continuity budget) nor persist forever.
func TestWithRenewedCoverAbandonsAfterStaleFloorTail(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Renewals fail at once; each handoff is wedged until its own
		// deadline. A wedged attempt is also what moves fake time past the
		// tail's edge: instant failures would retry at the same instant.
		calls := 0
		renew := func(context.Context) error {
			calls++
			if calls == 1 {
				return nil // the initial cover succeeds
			}
			return errors.New("ledger down") // and never recovers
		}
		handoffs := 0
		handoff := func(hctx context.Context) error {
			handoffs++
			if handoffs > 100 {
				t.Fatalf("still retrying after %d handoff attempts: the seam never gives up", handoffs)
			}
			<-hctx.Done()
			return hctx.Err()
		}
		// The loss lands at the budget; wedged handoffs run at the loss and
		// at the tail's edge, and the check after the second one gives up.
		if coverLapseBudget+CoverHandoffTimeout > staleFloorTail || staleFloorTail-coverLapseBudget-CoverHandoffTimeout >= coverRenewInterval {
			t.Fatalf("cadence %v/%v/%v no longer puts the second handoff on the tail's edge", coverLapseBudget, CoverHandoffTimeout, staleFloorTail)
		}
		start := time.Now()
		opErr, renewErr := WithRenewedCover(context.Background(), renew, func(context.Context) error { return nil }, handoff)
		elapsed := time.Since(start)
		if opErr != nil {
			t.Fatalf("opErr = %v, want nil (op is instant)", opErr)
		}
		if renewErr == nil || !strings.Contains(renewErr.Error(), "abandoned after the stale-floor tail") {
			t.Fatalf("renewErr = %v, want the tail-abandonment report", renewErr)
		}
		if want := staleFloorTail + CoverHandoffTimeout; elapsed != want {
			t.Fatalf("abandonment took %v, want %v (the first check after the tail)", elapsed, want)
		}
		if handoffs != 2 {
			t.Fatalf("handoff attempts = %d, want 2 (at the loss and at the tail's edge)", handoffs)
		}
	})
}

// TestWithRenewedCoverRefreshSurvivesOpDeadline (Codex round 60): the
// post-publication refresh is MANDATORY tail coverage — its restarted TTL
// must span the stale-floor tail (floor lifetime + admission handoff
// margin) — but it used to inherit the operation context. A restore
// succeeding at its deadline's edge then lost the refresh to that same
// deadline, leaving the cover's remaining life (TTL minus up to one
// renewal interval) shorter than the tail. The refresh must run on a
// fresh bounded context detached from the op's.
func TestWithRenewedCoverRefreshSurvivesOpDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var refreshCtxErr error
	opErr, renewErr := WithRenewedCover(ctx, func(rctx context.Context) error {
		refreshCtxErr = rctx.Err()
		return rctx.Err()
	}, func(context.Context) error {
		cancel() // the op consumed its whole deadline as it finished
		return nil
	}, nil)
	if opErr != nil {
		t.Fatalf("opErr = %v, want nil", opErr)
	}
	if renewErr != nil {
		t.Fatalf("post-publication refresh failed on the exhausted op context: %v", renewErr)
	}
	if refreshCtxErr != nil {
		t.Fatalf("refresh ran on the dead op context (ctx err %v), want a fresh bounded context", refreshCtxErr)
	}
}

// The verification deadline scales with pack size so a repair upload of a
// supported large pack (S3 multipart copy handles >5 GiB) can finish
// within it (Codex round 15).
func TestEnsurePublishedTimeout(t *testing.T) {
	if got := ensurePublishedTimeout(0); got != 5*time.Minute {
		t.Fatalf("timeout(0) = %v, want the 5m floor", got)
	}
	const fiveGiB = 5 << 30
	got := ensurePublishedTimeout(fiveGiB)
	if want := 5*time.Minute + 5120*time.Second; got != want {
		t.Fatalf("timeout(5GiB) = %v, want %v", got, want)
	}
	if got <= 6*time.Minute {
		t.Fatalf("timeout(5GiB) = %v: cannot cover a 5 GiB re-upload", got)
	}
}

// ListPages streams in key order with bounded pages; the final short page
// is delivered and fn errors abort the scan.
func TestFile_ListPages(t *testing.T) {
	ctx := context.Background()
	b, _ := openFile(t)
	src := filepath.Join(t.TempDir(), "src")
	if err := os.WriteFile(src, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	var want []string
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		key := PackKey("r1", id)
		if err := b.UploadFile(ctx, key, src); err != nil {
			t.Fatal(err)
		}
		want = append(want, key)
	}

	var got []string
	var pages []int
	err := b.ListPages(ctx, PacksRoot, 2, func(page []ListedObject) error {
		pages = append(pages, len(page))
		for _, o := range page {
			got = append(got, o.Key)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ListPages: %v", err)
	}
	if len(pages) != 3 || pages[0] != 2 || pages[1] != 2 || pages[2] != 1 {
		t.Fatalf("page sizes = %v, want [2 2 1]", pages)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("keys = %v, want lexicographic %v", got, want)
		}
	}

	sentinel := errors.New("stop")
	calls := 0
	err = b.ListPages(ctx, PacksRoot, 2, func([]ListedObject) error {
		calls++
		return sentinel
	})
	if !errors.Is(err, sentinel) || calls != 1 {
		t.Fatalf("fn error must abort the scan: err=%v calls=%d", err, calls)
	}
}
