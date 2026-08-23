package blob

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
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
	for i := 0; i < 2; i++ {
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
func TestEnsurePublishedRenewsCoverThroughPublication(t *testing.T) {
	ctx := context.Background()
	oldInterval, oldBudget := coverRenewInterval, coverLapseBudget
	coverRenewInterval, coverLapseBudget = 20*time.Millisecond, 80*time.Millisecond
	t.Cleanup(func() { coverRenewInterval, coverLapseBudget = oldInterval, oldBudget })

	key := PackKey("r1", "renewed")
	b := seededFile(t, key, "pack-bytes")
	src := filepath.Join(t.TempDir(), "src")
	if err := os.WriteFile(src, []byte("pack-bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}

	slow := slowUploadStore{BlobStore: b, delay: 110 * time.Millisecond}
	var calls atomic.Int32
	restored, err := EnsurePublished(ctx, slow, key, src, func(context.Context, int64) error {
		calls.Add(1)
		return nil
	}, nil)
	if err != nil || !restored {
		t.Fatalf("EnsurePublished(slow restore) = (%v, %v), want (true, nil)", restored, err)
	}
	// 1 initial + ≥2 mid-upload renewals (110ms across 20ms ticks, half
	// slack for scheduler noise) + 1 post-publication refresh.
	if n := calls.Load(); n < 4 {
		t.Fatalf("cover calls = %d, want ≥ 4 (initial + renewals through the upload + post-publication refresh)", n)
	}

	// Once every renewal fails, continuity runs out MID-UPLOAD and the
	// restore aborts (Codex rounds 62+63): uncovered bytes must never
	// become physical.
	if err := b.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	first := true
	restored, err = EnsurePublished(ctx, slow, key, src, func(context.Context, int64) error {
		if first {
			first = false
			return nil // the initial cover succeeds
		}
		return errors.New("ledger unreachable") // every renewal fails
	}, nil)
	if restored || err == nil || !strings.Contains(err.Error(), "restore cover continuity lost") {
		t.Fatalf("EnsurePublished(dead ledger) = (%v, %v), want an aborted, unpublished restore", restored, err)
	}
	if _, serr := b.Stat(ctx, key); !errors.Is(serr, ErrNotFound) {
		t.Fatalf("aborted restore published anyway (stat: %v)", serr)
	}
}

// TestWithRenewedCoverAbortsOnLapse (Codex round 62): a renewal stalling
// past the cover's TTL used to block the renewal loop while the restore
// upload continued on its own healthy connection — the cover row expired
// mid-restore and the uncovered bytes still became physical. Each renewal
// attempt is now deadline-bounded (a stall costs one attempt, not the
// loop), and once coverLapseBudget passes without a successful renewal
// the op's context is canceled: an uncovered restore must never publish.
func TestWithRenewedCoverAbortsOnLapse(t *testing.T) {
	oldI, oldT, oldB := coverRenewInterval, coverRenewTimeout, coverLapseBudget
	coverRenewInterval, coverRenewTimeout, coverLapseBudget = 10*time.Millisecond, 5*time.Millisecond, 40*time.Millisecond
	t.Cleanup(func() { coverRenewInterval, coverRenewTimeout, coverLapseBudget = oldI, oldT, oldB })

	// The initial cover succeeds; every later renewal stalls until its
	// per-attempt deadline: the loop must keep making (failing) attempts
	// and abort the op at the budget.
	initial := true
	renew := func(rctx context.Context) error {
		if initial {
			initial = false
			return nil
		}
		<-rctx.Done()
		return rctx.Err()
	}
	opErr, renewErr := WithRenewedCover(context.Background(), renew, func(opCtx context.Context) error {
		select {
		case <-opCtx.Done():
			return context.Cause(opCtx)
		case <-time.After(2 * time.Second):
			return nil // the abort never came: uncovered bytes would publish
		}
	}, nil)
	if opErr == nil || !strings.Contains(opErr.Error(), "restore cover continuity lost") {
		t.Fatalf("opErr = %v, want a cover-lapse abort", opErr)
	}
	if renewErr != nil {
		t.Fatalf("renewErr = %v, want nil (op failed, no final refresh)", renewErr)
	}
}

// TestWithRenewedCoverRejectsLateSuccess (Codex round 63): a renewal that
// SUCCEEDS but completes after the prior cover's budget window cannot
// prove continuity — the prior row may have expired mid-attempt, leaving
// an uncovered gap a stale-floor admission can commit in — so it must
// abort exactly like a lapse instead of resetting the coverage anchor.
func TestWithRenewedCoverRejectsLateSuccess(t *testing.T) {
	oldI, oldT, oldB := coverRenewInterval, coverRenewTimeout, coverLapseBudget
	coverRenewInterval, coverRenewTimeout, coverLapseBudget = 10*time.Millisecond, 100*time.Millisecond, 25*time.Millisecond
	t.Cleanup(func() { coverRenewInterval, coverRenewTimeout, coverLapseBudget = oldI, oldT, oldB })

	// Every renewal SUCCEEDS — but only after the budget window passed.
	renew := func(context.Context) error {
		time.Sleep(40 * time.Millisecond)
		return nil
	}
	opErr, _ := WithRenewedCover(context.Background(), renew, func(opCtx context.Context) error {
		select {
		case <-opCtx.Done():
			return context.Cause(opCtx)
		case <-time.After(2 * time.Second):
			return nil // late successes were accepted: the gap went unnoticed
		}
	}, nil)
	if opErr == nil || !strings.Contains(opErr.Error(), "restore cover continuity lost") {
		t.Fatalf("opErr = %v, want a continuity abort on late-completing renewals", opErr)
	}
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
	oldI, oldT, oldB := coverRenewInterval, coverRenewTimeout, coverLapseBudget
	// Ticks land far past the budget: only a budget-deadline timer can
	// abort in time.
	coverRenewInterval, coverRenewTimeout, coverLapseBudget = 300*time.Millisecond, 10*time.Millisecond, 50*time.Millisecond
	t.Cleanup(func() { coverRenewInterval, coverRenewTimeout, coverLapseBudget = oldI, oldT, oldB })

	initial := true
	renew := func(context.Context) error {
		if initial {
			initial = false
			return nil // the initial cover succeeds; nothing renews it
		}
		return errors.New("ledger down")
	}
	start := time.Now()
	opErr, _ := WithRenewedCover(context.Background(), renew, func(opCtx context.Context) error {
		select {
		case <-opCtx.Done():
			return context.Cause(opCtx)
		case <-time.After(2 * time.Second):
			return nil // never aborted: uncovered bytes would publish
		}
	}, nil)
	elapsed := time.Since(start)
	if opErr == nil || !strings.Contains(opErr.Error(), "restore cover continuity lost") {
		t.Fatalf("opErr = %v, want a budget-expiry abort", opErr)
	}
	// The abort must come from the budget deadline (~50ms), not the
	// first tick (~300ms); generous slack for scheduler noise.
	if elapsed > 200*time.Millisecond {
		t.Fatalf("abort took %v, want ≈coverLapseBudget (the budget timer, not the next tick)", elapsed)
	}
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
	oldI, oldT, oldB := coverRenewInterval, coverRenewTimeout, coverLapseBudget
	coverRenewInterval, coverRenewTimeout, coverLapseBudget = 10*time.Millisecond, 50*time.Millisecond, 200*time.Millisecond
	t.Cleanup(func() { coverRenewInterval, coverRenewTimeout, coverLapseBudget = oldI, oldT, oldB })

	var renews atomic.Int32
	renew := func(context.Context) error {
		renews.Add(1)
		return nil // the cover stays renewable throughout
	}
	handoffs := 0
	lastRenewsSeen := int32(0)
	handoff := func(hctx context.Context) error {
		handoffs++
		if d, ok := hctx.Deadline(); !ok || time.Until(d) > CoverHandoffTimeout+time.Second {
			t.Fatalf("handoff attempt %d unbounded or over the handoff window", handoffs)
		}
		if r := renews.Load(); r <= lastRenewsSeen {
			t.Fatalf("handoff attempt %d ran without a fresh cover renewal since the last attempt", handoffs)
		}
		lastRenewsSeen = renews.Load()
		if handoffs < 3 {
			return errors.New("ledger transiently down") // the invalidation must be RETRIED
		}
		return nil
	}
	opErr, renewErr := WithRenewedCover(context.Background(), renew, func(context.Context) error { return nil }, handoff)
	if opErr != nil || renewErr != nil {
		t.Fatalf("WithRenewedCover = (%v, %v), want (nil, nil) once the handoff lands", opErr, renewErr)
	}
	if handoffs != 3 {
		t.Fatalf("handoff attempts = %d, want 3 (two transient failures retried under live covers)", handoffs)
	}
}

// TestWithRenewedCoverTailPersistsThroughContinuityLoss (Codex round 72,
// security): continuity loss after publication used to abandon BOTH
// repairs immediately, while absence-window floors minted before
// publication stay admissible for the full stale-floor tail — the last
// cover could expire with neither invalidation nor re-cover. After a
// loss the seam must keep attempting both through the tail: a handoff
// success still invalidates every stale floor.
func TestWithRenewedCoverTailPersistsThroughContinuityLoss(t *testing.T) {
	oldI, oldT, oldB, oldTail := coverRenewInterval, coverRenewTimeout, coverLapseBudget, staleFloorTail
	coverRenewInterval, coverRenewTimeout, coverLapseBudget, staleFloorTail =
		10*time.Millisecond, 5*time.Millisecond, 25*time.Millisecond, 2*time.Second
	t.Cleanup(func() {
		coverRenewInterval, coverRenewTimeout, coverLapseBudget, staleFloorTail = oldI, oldT, oldB, oldTail
	})

	initial := true
	renew := func(rctx context.Context) error {
		if initial {
			initial = false
			return nil // the initial cover; every later renewal fails
		}
		return errors.New("ledger down")
	}
	handoffs := 0
	handoff := func(context.Context) error {
		handoffs++
		if handoffs < 3 {
			return errors.New("ledger still down") // recovery on the 3rd try
		}
		return nil
	}
	opErr, renewErr := WithRenewedCover(context.Background(), renew, func(context.Context) error { return nil }, handoff)
	if opErr != nil || renewErr != nil {
		t.Fatalf("WithRenewedCover = (%v, %v), want (nil, nil): the tail must persist through continuity loss until the handoff lands", opErr, renewErr)
	}
	if handoffs < 3 {
		t.Fatalf("handoff attempts = %d, want ≥3 (post-loss retries through the stale-floor tail)", handoffs)
	}
}

// TestWithRenewedCoverAbandonsAfterStaleFloorTail (Codex rounds 67+72):
// when the ledger never recovers, the post-publication loop gives up only
// once the stale-floor tail has passed — every floor that could hide the
// restored bytes has expired by then, so abandoning is safe — and the
// abandonment is reported. It must not give up earlier (round 72: floors
// outlive the continuity budget) nor persist forever.
func TestWithRenewedCoverAbandonsAfterStaleFloorTail(t *testing.T) {
	oldI, oldT, oldB, oldTail := coverRenewInterval, coverRenewTimeout, coverLapseBudget, staleFloorTail
	coverRenewInterval, coverRenewTimeout, coverLapseBudget, staleFloorTail =
		10*time.Millisecond, 5*time.Millisecond, 25*time.Millisecond, 150*time.Millisecond
	t.Cleanup(func() {
		coverRenewInterval, coverRenewTimeout, coverLapseBudget, staleFloorTail = oldI, oldT, oldB, oldTail
	})

	initial := true
	renew := func(context.Context) error {
		if initial {
			initial = false
			return nil // the initial cover succeeds
		}
		return errors.New("ledger down") // and never recovers
	}
	start := time.Now()
	opErr, renewErr := WithRenewedCover(context.Background(), renew, func(context.Context) error { return nil },
		func(context.Context) error { return errors.New("ledger down") })
	elapsed := time.Since(start)
	if opErr != nil {
		t.Fatalf("opErr = %v, want nil (op is instant)", opErr)
	}
	if renewErr == nil || !strings.Contains(renewErr.Error(), "abandoned after the stale-floor tail") {
		t.Fatalf("renewErr = %v, want the tail-abandonment report", renewErr)
	}
	// Give-up lands ≈ the tail's end (150ms) — after the continuity
	// budget (25ms: too early) and well before forever.
	if elapsed < 100*time.Millisecond || elapsed > time.Second {
		t.Fatalf("abandonment took %v, want ≈staleFloorTail", elapsed)
	}
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
