package ref

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestOpenSchemeDispatch covers the factory's container-free paths: unknown
// schemes fail fast listing the supported set, and postgres:// dispatches
// into OpenPG (proved by a connect error against a dead endpoint, never an
// unsupported-scheme error).
func TestOpenSchemeDispatch(t *testing.T) {
	ctx := context.Background()

	_, err := Open(ctx, "mysql://h/db")
	if err == nil || !strings.Contains(err.Error(), `scheme "mysql" unsupported`) {
		t.Fatalf("want unsupported-scheme error, got %v", err)
	}
	for _, scheme := range []string{"postgres", "postgresql", "sqlite"} {
		if !strings.Contains(err.Error(), scheme) {
			t.Fatalf("unsupported-scheme error must list %q, got %v", scheme, err)
		}
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err = Open(ctx, "postgres://127.0.0.1:1/db?connect_timeout=1")
	if err == nil || strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("postgres:// must dispatch to OpenPG, got %v", err)
	}

	st, err := Open(ctx, "sqlite://"+filepath.Join(t.TempDir(), "refs.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := st.(*SQLite); !ok {
		t.Fatalf("sqlite:// must open the SQLite backend, got %T", st)
	}
	st.Close()
}

// ListRefsAfter must page in ascending BYTE order with an exclusive cursor
// (issue #119): hydration merge-joins pages against git's own strcmp-ordered
// ref iteration, so a locale-collated page order would silently mis-sync.
// The fixture mixes cases and punctuation whose byte order ('B' < '_' < 'a')
// disagrees with common locale collations.
func TestListRefsAfter_PagesInByteOrder(t *testing.T) {
	runRefBackends(t, testListRefsAfterPagesInByteOrder)
}

func testListRefsAfterPagesInByteOrder(t *testing.T, s Store) {
	ctx := context.Background()
	repo := t.Name()
	// Deliberately inserted out of byte order.
	names := []string{
		"refs/heads/a",
		"refs/heads/B",
		"refs/heads/_underscore",
		"refs/tags/x",
		"refs/heads/a/b",
	}
	shas := map[string]string{}
	for i, name := range names {
		sha := testSHA(byte(0x21 + i))
		shas[name] = sha
		if won, err := s.CASRef(ctx, repo, name, ZeroSHA, sha); err != nil || !won {
			t.Fatalf("CASRef %s = (%v, %v), want win", name, won, err)
		}
	}
	wantOrder := []string{
		"refs/heads/B",
		"refs/heads/_underscore",
		"refs/heads/a",
		"refs/heads/a/b",
		"refs/tags/x",
	}

	// Page with limit 2 from the empty cursor: pages stitch to exactly the
	// byte-ordered sequence, and a page shorter than the limit ends the scan.
	var got []RefEntry
	after := ""
	for {
		page, err := s.ListRefsAfter(ctx, repo, after, 2)
		if err != nil {
			t.Fatalf("ListRefsAfter(after=%q): %v", after, err)
		}
		got = append(got, page...)
		if len(page) < 2 {
			break
		}
		after = page[len(page)-1].Name
	}
	if len(got) != len(wantOrder) {
		t.Fatalf("stitched %d rows, want %d: %+v", len(got), len(wantOrder), got)
	}
	for i, e := range got {
		if e.Name != wantOrder[i] || e.SHA != shas[e.Name] {
			t.Fatalf("row %d = %+v, want {%s %s}", i, e, wantOrder[i], shas[wantOrder[i]])
		}
	}

	// The cursor is exclusive, and a cursor at (or past) the last name
	// yields the empty page.
	page, err := s.ListRefsAfter(ctx, repo, "refs/heads/a", 10)
	if err != nil {
		t.Fatalf("ListRefsAfter(after=refs/heads/a): %v", err)
	}
	if len(page) != 2 || page[0].Name != "refs/heads/a/b" || page[1].Name != "refs/tags/x" {
		t.Fatalf("exclusive cursor page = %+v, want [refs/heads/a/b refs/tags/x]", page)
	}
	if page, err := s.ListRefsAfter(ctx, repo, "refs/tags/x", 10); err != nil || len(page) != 0 {
		t.Fatalf("past-end page = (%+v, %v), want empty", page, err)
	}
}
