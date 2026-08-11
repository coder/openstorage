package blob

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
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

// A failed download must never leave anything at destPath (atomic contract).
func TestFile_DownloadMissingKey(t *testing.T) {
	b, _ := openFile(t)
	dest := filepath.Join(t.TempDir(), "missing.pack")
	if err := b.DownloadFile(context.Background(), PackKey("r1", "nope"), dest); err == nil {
		t.Fatal("DownloadFile of missing key must fail")
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatalf("destPath must not exist after failed download, stat err = %v", err)
	}
}
