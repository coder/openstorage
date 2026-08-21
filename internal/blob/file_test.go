package blob

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
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

	if err := EnsurePublished(ctx, b, key, src); err != nil {
		t.Fatalf("EnsurePublished(present): %v", err)
	}
	if err := b.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if err := EnsurePublished(ctx, b, key, src); err != nil {
		t.Fatalf("EnsurePublished(missing): %v", err)
	}
	size, err := b.Stat(ctx, key)
	if err != nil || size != int64(len("pack-bytes")) {
		t.Fatalf("Stat after re-publish = %d, %v", size, err)
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
