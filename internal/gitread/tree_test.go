package gitread

import (
	"context"
	"errors"
	"testing"
)

func entriesByPath(entries []TreeEntry) map[string]TreeEntry {
	m := make(map[string]TreeEntry, len(entries))
	for _, e := range entries {
		m[e.Path] = e
	}
	return m
}

func TestLsTreeRoot(t *testing.T) {
	f := newFixture(t)
	entries, err := LsTree(context.Background(), f.dir, f.c1, "", false)
	if err != nil {
		t.Fatal(err)
	}
	m := entriesByPath(entries)
	if len(m) != 5 {
		t.Fatalf("root entries = %v, want 5", entries)
	}
	if e := m["README.md"]; e.Type != EntryBlob || e.Mode != "100644" || e.Size != 28 {
		t.Errorf("README.md = %+v", e)
	}
	if e := m["dir"]; e.Type != EntryTree || e.Size != -1 {
		t.Errorf("dir = %+v", e)
	}
	if e := m["script.sh"]; e.Type != EntryBlob || e.Mode != "100755" {
		t.Errorf("script.sh = %+v", e)
	}
	if e := m["link"]; e.Type != EntrySymlink || e.Mode != "120000" {
		t.Errorf("link = %+v", e)
	}
	if e := m["bin.dat"]; e.Type != EntryBlob {
		t.Errorf("bin.dat = %+v", e)
	}
	for _, e := range entries {
		if e.SHA == "" {
			t.Errorf("%s has empty sha", e.Path)
		}
	}
}

func TestLsTreeRecursiveAndPath(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	entries, err := LsTree(ctx, f.dir, f.c1, "", true)
	if err != nil {
		t.Fatal(err)
	}
	m := entriesByPath(entries)
	if _, ok := m["dir/data.txt"]; !ok {
		t.Errorf("recursive listing misses dir/data.txt: %v", entries)
	}
	if _, ok := m["dir"]; ok {
		t.Errorf("recursive listing must not include tree entries: %v", entries)
	}

	// Directory contents via trailing slash; entry itself without.
	sub, err := LsTree(ctx, f.dir, f.c1, "dir/", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(sub) != 1 || sub[0].Path != "dir/data.txt" {
		t.Errorf(`LsTree("dir/") = %v`, sub)
	}

	// Missing path: empty result, no error (handler owns the 404).
	none, err := LsTree(ctx, f.dir, f.c1, "no/such/dir/", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(none) != 0 {
		t.Errorf("missing path listed %v", none)
	}
}

func TestStatBlobAndReadBlob(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	info, err := StatBlob(ctx, f.dir, f.c2, "README.md")
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != 38 || info.Mode != "100644" || info.Symlink {
		t.Errorf("README.md info = %+v", info)
	}
	// README.md was modified in c2, so its last commit is c2 itself.
	if info.LastCommit.SHA != f.c2 {
		t.Errorf("last commit = %s, want %s", info.LastCommit.SHA, f.c2)
	}
	content, err := ReadBlob(ctx, f.dir, info.SHA)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "# Title\nline two\nline three\nline four\n" {
		t.Errorf("blob content %q", content)
	}

	// extra.txt was untouched after c2; script.sh untouched after c1.
	info, err = StatBlob(ctx, f.dir, f.c2, "script.sh")
	if err != nil {
		t.Fatal(err)
	}
	if info.LastCommit.SHA != f.c1 {
		t.Errorf("script.sh last commit = %s, want %s", info.LastCommit.SHA, f.c1)
	}

	// Symlink stat.
	info, err = StatBlob(ctx, f.dir, f.c1, "link")
	if err != nil {
		t.Fatal(err)
	}
	if !info.Symlink {
		t.Errorf("link info = %+v, want symlink", info)
	}
	target, err := ReadBlob(ctx, f.dir, info.SHA)
	if err != nil {
		t.Fatal(err)
	}
	if string(target) != "README.md" {
		t.Errorf("symlink target %q", target)
	}

	// Missing path and directory path both wrap ErrNotFound.
	if _, err := StatBlob(ctx, f.dir, f.c1, "nope.txt"); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing path err = %v, want ErrNotFound", err)
	}
	if _, err := StatBlob(ctx, f.dir, f.c1, "dir"); !errors.Is(err, ErrNotFound) {
		t.Errorf("directory path err = %v, want ErrNotFound", err)
	}
	if _, err := StatBlob(ctx, f.dir, f.c1, ""); !errors.Is(err, ErrNotFound) {
		t.Errorf("empty path err = %v, want ErrNotFound", err)
	}
}
