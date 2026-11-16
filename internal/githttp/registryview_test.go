package githttp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/ref"
)

func TestParseObjectKinds(t *testing.T) {
	a, b, c := strings.Repeat("a", 40), strings.Repeat("b", 40), strings.Repeat("c", 40)
	got, err := ParseObjectKinds([]string{a, b, c}, "commit\n"+b+" missing\ntag\n")
	if err != nil || got[a] != "commit" || got[b] != "missing" || got[c] != "tag" {
		t.Fatalf("ParseObjectKinds = %v, %v", got, err)
	}
	for _, bad := range []string{"commit\n", "commit\nbogus\ntag\n", "commit\n" + c + " missing\ntag\n"} {
		if _, err := ParseObjectKinds([]string{a, b, c}, bad); err == nil {
			t.Fatalf("ParseObjectKinds(%q) = nil error, want a refusal", bad)
		}
	}
}

// The view points git at the cache repository and at the view directory, and at
// no alternate or quarantine that the caller's environment names.
func TestRegisteredPackViewEnvReplacesTheObjectVariables(t *testing.T) {
	v := &RegisteredPackView{dir: "/view", repoPath: "/cache/r.git"}
	env := v.Env([]string{"PATH=/bin", "GIT_DIR=.", "GIT_OBJECT_DIRECTORY=/q", "GIT_ALTERNATE_OBJECT_DIRECTORIES=/main", "GIT_QUARANTINE_PATH=/q", "GIT_COMMON_DIR=/c", "HOME=/h"})
	want := []string{"PATH=/bin", "HOME=/h", "GIT_DIR=/cache/r.git", "GIT_OBJECT_DIRECTORY=/view"}
	if !slices.Equal(env, want) {
		t.Fatalf("Env = %v, want %v", env, want)
	}
}

func TestRegisteredPackViewEnvForPush(t *testing.T) {
	v := &RegisteredPackView{dir: "/view", repoPath: "/cache/r.git"}
	base := []string{"PATH=/bin", "GIT_DIR=.", "GIT_OBJECT_DIRECTORY=/q", "GIT_ALTERNATE_OBJECT_DIRECTORIES=/main", "GIT_QUARANTINE_PATH=/q"}
	// With a quarantine, git keeps it as the object directory and the view
	// replaces the main store as the alternate.
	got := v.EnvForPush(base, "/q")
	want := []string{"PATH=/bin", "GIT_DIR=.", "GIT_OBJECT_DIRECTORY=/q", "GIT_QUARANTINE_PATH=/q", "GIT_ALTERNATE_OBJECT_DIRECTORIES=/view"}
	if !slices.Equal(got, want) {
		t.Fatalf("EnvForPush with a quarantine = %v, want %v", got, want)
	}
	// Without one, the view is the object directory.
	if got := v.EnvForPush(base, ""); !slices.Contains(got, "GIT_OBJECT_DIRECTORY=/view") || slices.Contains(got, "GIT_ALTERNATE_OBJECT_DIRECTORIES=/main") {
		t.Fatalf("EnvForPush with no quarantine = %v", got)
	}
}

// listsPacks lists the packs that the test sets.
type listsPacks struct {
	ref.RefStore
	packs func(call int) []ref.Pack
	calls int
}

func (s *listsPacks) ListPacks(context.Context, string) ([]ref.Pack, error) {
	s.calls++
	return s.packs(s.calls), nil
}

// A listing that names a pack that the cache lacks heals and lists again, a
// bounded number of times: another node can register a pack in between.
func TestNewRegisteredPackViewHealsAgainWhenTheListingGrows(t *testing.T) {
	repoPath := t.TempDir()
	packDir := filepath.Join(repoPath, "objects", "pack")
	if err := os.MkdirAll(packDir, 0o755); err != nil {
		t.Fatal(err)
	}
	put := func(id string) {
		for _, ext := range []string{".pack", ".idx"} {
			if err := os.WriteFile(filepath.Join(packDir, "pack-"+id+ext), nil, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Each listing names one more pack than the cache has after the last heal.
	st := &listsPacks{packs: func(call int) []ref.Pack {
		var packs []ref.Pack
		for i := 1; i <= call && i <= 3; i++ {
			packs = append(packs, ref.Pack{ID: strings.Repeat(string(rune('a'+i)), 40)})
		}
		return packs
	}}
	heals := 0
	view, err := NewRegisteredPackView(t.Context(), st, repoPath, "r", func(context.Context) error {
		heals++
		for i := 1; i <= st.calls && i <= 3; i++ {
			put(strings.Repeat(string(rune('a'+i)), 40))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("NewRegisteredPackView = %v after %d heals", err, heals)
	}
	defer view.Close()
	if heals != 3 || st.calls != 4 {
		t.Fatalf("heals = %d, listings = %d, want 3 and 4", heals, st.calls)
	}

	// A pack that stays missing ends the loop at the bound. The view is built
	// without it: an answer that holds each object stands, and a "missing"
	// answer is an error that wraps ErrViewIncomplete.
	st2 := &listsPacks{packs: func(int) []ref.Pack {
		return []ref.Pack{{ID: strings.Repeat("b", 40)}, {ID: strings.Repeat("f", 40)}}
	}}
	heals = 0
	view2, err := NewRegisteredPackView(t.Context(), st2, repoPath, "r", func(context.Context) error { heals++; return nil })
	if err != nil {
		t.Fatalf("a pack that stays missing = %v, want a view without it", err)
	}
	defer view2.Close()
	if heals != registryViewHealRounds {
		t.Fatalf("heals = %d, want %d", heals, registryViewHealRounds)
	}
	for _, name := range []string{"pack-" + strings.Repeat("b", 40) + ".idx", "pack-" + strings.Repeat("f", 40) + ".idx"} {
		_, err := os.Lstat(filepath.Join(view2.dir, "pack", name))
		if present := err == nil; present != strings.Contains(name, "bbbb") {
			t.Fatalf("%s present = %v in the view, want only the pack that the cache has", name, present)
		}
	}
	if err := view2.CheckAnswer(map[string]string{"x": "commit", "y": "tree"}); err != nil {
		t.Fatalf("CheckAnswer(all held) = %v, want nil", err)
	}
	if err := view2.CheckAnswer(map[string]string{"x": "commit", "y": "missing"}); !errors.Is(err, ErrViewIncomplete) {
		t.Fatalf("CheckAnswer(one missing) = %v, want ErrViewIncomplete", err)
	}

	// A complete view never fails a "missing" answer: the object is not in the registry.
	st3 := &listsPacks{packs: func(int) []ref.Pack { return []ref.Pack{{ID: strings.Repeat("b", 40)}} }}
	view3, err := NewRegisteredPackView(t.Context(), st3, repoPath, "r", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer view3.Close()
	if err := view3.CheckAnswer(map[string]string{"y": "missing"}); err != nil {
		t.Fatalf("CheckAnswer on a complete view = %v, want nil", err)
	}
}

// A "missing" answer stands only when no pack was registered since the view was
// built: another node can register a pack that holds the object right after
// the listing, so the answer is asked again over a new view, a bounded number
// of times.
func TestAskRegisteredPacksAsksAgainWhenAPackWasRegisteredSinceTheView(t *testing.T) {
	repoPath := t.TempDir()
	packDir := filepath.Join(repoPath, "objects", "pack")
	if err := os.MkdirAll(packDir, 0o755); err != nil {
		t.Fatal(err)
	}
	id := func(i int) string { return strings.Repeat(string(rune('a'+i)), 40) }
	for i := range 8 {
		for _, ext := range []string{".pack", ".idx"} {
			if err := os.WriteFile(filepath.Join(packDir, "pack-"+id(i)+ext), nil, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	ctx := t.Context()
	listing := func(n int) func(int) []ref.Pack {
		return func(int) []ref.Pack {
			var packs []ref.Pack
			for i := range n {
				packs = append(packs, ref.Pack{ID: id(i)})
			}
			return packs
		}
	}

	t.Run("a pack registered after the view holds the object", func(t *testing.T) {
		// The first listing names one pack, the later ones two.
		st := &listsPacks{packs: func(call int) []ref.Pack { return listing(min(call, 2))(call) }}
		asks := 0
		kinds, err := AskRegisteredPacks(ctx, st, repoPath, "r", nil, func(*RegisteredPackView) (map[string]string, error) {
			asks++
			if asks == 1 {
				return map[string]string{"x": "missing"}, nil
			}
			return map[string]string{"x": "commit"}, nil
		})
		if err != nil || kinds["x"] != "commit" || asks != 2 {
			t.Fatalf("AskRegisteredPacks = %v, %v after %d asks, want commit after 2", kinds, err, asks)
		}
	})

	t.Run("a listing that did not change keeps the missing answer", func(t *testing.T) {
		st := &listsPacks{packs: listing(2)}
		asks := 0
		kinds, err := AskRegisteredPacks(ctx, st, repoPath, "r", nil, func(*RegisteredPackView) (map[string]string, error) {
			asks++
			return map[string]string{"x": "missing"}, nil
		})
		if err != nil || kinds["x"] != "missing" || asks != 1 || st.calls != 2 {
			t.Fatalf("AskRegisteredPacks = %v, %v after %d asks and %d listings, want missing after 1 ask and 2 listings", kinds, err, asks, st.calls)
		}
	})

	t.Run("a found answer never lists again", func(t *testing.T) {
		st := &listsPacks{packs: listing(2)}
		kinds, err := AskRegisteredPacks(ctx, st, repoPath, "r", nil, func(*RegisteredPackView) (map[string]string, error) {
			return map[string]string{"x": "commit"}, nil
		})
		if err != nil || kinds["x"] != "commit" || st.calls != 1 {
			t.Fatalf("AskRegisteredPacks = %v, %v after %d listings, want commit after 1", kinds, err, st.calls)
		}
	})

	t.Run("a registry that keeps growing ends at the bound", func(t *testing.T) {
		// Each listing names one more pack than the last, and the cache has them all.
		st := &listsPacks{packs: func(call int) []ref.Pack { return listing(min(call, 8))(call) }}
		asks := 0
		kinds, err := AskRegisteredPacks(ctx, st, repoPath, "r", nil, func(*RegisteredPackView) (map[string]string, error) {
			asks++
			return map[string]string{"x": "missing"}, nil
		})
		if err != nil || kinds["x"] != "missing" || asks != registryViewHealRounds+1 {
			t.Fatalf("AskRegisteredPacks = %v, %v after %d asks, want missing after %d", kinds, err, asks, registryViewHealRounds+1)
		}
	})

	t.Run("an incomplete view fails the missing answer", func(t *testing.T) {
		st := &listsPacks{packs: func(int) []ref.Pack { return []ref.Pack{{ID: id(0)}, {ID: strings.Repeat("z", 40)}} }}
		_, err := AskRegisteredPacks(ctx, st, repoPath, "r", nil, func(*RegisteredPackView) (map[string]string, error) {
			return map[string]string{"x": "missing"}, nil
		})
		if !errors.Is(err, ErrViewIncomplete) {
			t.Fatalf("AskRegisteredPacks on an incomplete view = %v, want ErrViewIncomplete", err)
		}
	})
}
