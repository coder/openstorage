package gitread

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"testing"
)

// readTarGz decodes a tar.gz stream into header-by-name and content-by-name
// maps.
func readTarGz(t *testing.T, data []byte) (map[string]*tar.Header, map[string]string) {
	t.Helper()
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("gzip: %v", err)
	}
	headers := map[string]*tar.Header{}
	contents := map[string]string{}
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar: %v", err)
		}
		body, err := io.ReadAll(tr)
		if err != nil {
			t.Fatalf("tar body %s: %v", hdr.Name, err)
		}
		headers[hdr.Name] = hdr
		contents[hdr.Name] = string(body)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return headers, contents
}

func TestArchiveFullTree(t *testing.T) {
	f := newFixture(t)
	var buf bytes.Buffer
	if err := Archive(context.Background(), f.dir, f.c1, ArchiveOptions{Prefix: "repo-main/"}, &buf); err != nil {
		t.Fatal(err)
	}
	headers, contents := readTarGz(t, buf.Bytes())
	if len(headers) != 5 {
		t.Fatalf("archive has %d entries, want 5: %v", len(headers), headers)
	}

	readme := headers["repo-main/README.md"]
	if readme == nil || readme.Typeflag != tar.TypeReg || readme.Mode != 0o644 {
		t.Fatalf("README entry = %+v", readme)
	}
	if contents["repo-main/README.md"] != "# Title\nline two\nline three\n" {
		t.Errorf("README content = %q", contents["repo-main/README.md"])
	}
	if readme.ModTime.IsZero() {
		t.Error("entry mtime is zero, want commit time")
	}

	script := headers["repo-main/script.sh"]
	if script == nil || script.Mode != 0o755 {
		t.Errorf("script entry = %+v, want mode 0755", script)
	}

	link := headers["repo-main/link"]
	if link == nil || link.Typeflag != tar.TypeSymlink || link.Linkname != "README.md" {
		t.Errorf("symlink entry = %+v", link)
	}

	if _, ok := headers["repo-main/dir/data.txt"]; !ok {
		t.Error("nested file missing from archive")
	}
}

func TestArchiveFilters(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	// Include only .txt files.
	var buf bytes.Buffer
	if err := Archive(ctx, f.dir, f.c2, ArchiveOptions{IncludeGlobs: []string{"*.txt"}}, &buf); err != nil {
		t.Fatal(err)
	}
	headers, _ := readTarGz(t, buf.Bytes())
	if len(headers) != 2 {
		t.Fatalf("include filter kept %v, want extra.txt + dir/renamed.txt", headers)
	}
	if _, ok := headers["extra.txt"]; !ok {
		t.Errorf("extra.txt missing: %v", headers)
	}
	if _, ok := headers["dir/renamed.txt"]; !ok {
		t.Errorf("dir/renamed.txt missing: %v", headers)
	}

	// Exclude wins over include.
	buf.Reset()
	if err := Archive(ctx, f.dir, f.c2, ArchiveOptions{
		IncludeGlobs: []string{"*.txt"},
		ExcludeGlobs: []string{"dir/**"},
	}, &buf); err != nil {
		t.Fatal(err)
	}
	headers, _ = readTarGz(t, buf.Bytes())
	if len(headers) != 1 {
		t.Fatalf("exclude filter kept %v", headers)
	}

	// Size filter drops bin.dat (10 bytes) with a 5-byte cap but keeps the
	// symlink (targets are not size-filtered).
	buf.Reset()
	if err := Archive(ctx, f.dir, f.c1, ArchiveOptions{MaxBlobSize: 5}, &buf); err != nil {
		t.Fatal(err)
	}
	headers, _ = readTarGz(t, buf.Bytes())
	if _, ok := headers["bin.dat"]; ok {
		t.Errorf("size filter kept bin.dat: %v", headers)
	}
	if _, ok := headers["link"]; !ok {
		t.Errorf("size filter dropped symlink: %v", headers)
	}
}
