package gitread

// LsTreePage with a cursor against the stream it replaces (#615), on random
// git trees: folders of every size from none to dozens of files, nested up to
// five deep, with names on both sides of '/' in byte order. Whichever engine
// answers (the walk, or the stream after the walk declines), the page, the
// more flag, found and the error are the stream's.

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func randomGitFiles(r *rand.Rand) []fiFile {
	var files []fiFile
	sizes := []int{0, 1, 1, 2, 3, 3, 5, 12, 40}
	var gen func(prefix string, depth int)
	gen = func(prefix string, depth int) {
		used := map[string]bool{}
		name := func() string {
			for {
				n := genName(r)
				if n != "." && n != ".." && !used[n] && !strings.EqualFold(n, ".git") {
					used[n] = true
					return n
				}
			}
		}
		for range sizes[r.IntN(len(sizes))] {
			mode, data := "100644", fmt.Sprint(r.IntN(20))
			switch r.IntN(12) {
			case 0:
				mode = "100755"
			case 1:
				mode, data = "120000", "target"
			case 2:
				mode, data = "160000", strings.Repeat("1", 40)
			}
			files = append(files, fiFile{mode, prefix + name(), data})
		}
		if depth < 5 {
			for range r.IntN(4) {
				gen(prefix+name()+"/", depth+1)
			}
		}
	}
	gen("", 0)
	return files
}

// OPENSTORAGE_TEST_TREE_SEEDS sets how many trees the test reads (4 by
// default; a run for a change to the walk uses hundreds).
func TestLsTreePageCursorMatchesTheStreamOnRandomTrees(t *testing.T) {
	seeds := uint64(4)
	if v := os.Getenv("OPENSTORAGE_TEST_TREE_SEEDS"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		seeds = n
	}
	var pages, walked int
	for seed := range seeds {
		r := rand.New(rand.NewPCG(seed, 715))
		files := randomGitFiles(r)
		if len(files) == 0 {
			continue
		}
		dir, commit := fiRepo(t, files)
		sorted := sortedListing(t, dir, commit, "")
		folders := []string{""}
		for _, e := range sorted {
			for i := range len(e.Path) {
				if e.Path[i] == '/' && !slices.Contains(folders, e.Path[:i+1]) {
					folders = append(folders, e.Path[:i+1])
				}
			}
		}
		for range 4 {
			path := folders[r.IntN(len(folders))]
			listing := sortedListing(t, dir, commit, path)
			if len(listing) == 0 {
				continue
			}
			afters := []string{"\x00", listing[r.IntN(len(listing))].Path, listing[r.IntN(len(listing))].Path + "\x00", path + "zzz", path + "\xff"}
			if p := listing[r.IntN(len(listing))].Path; len(p) > len(path) {
				afters = append(afters, p[:len(path)+(len(p)-len(path))/2])
			}
			for _, after := range afters {
				for _, limit := range []int{1, 7} {
					sameAsStream(t, context.Background(), dir, commit, path, after, limit)
					pages++
					if _, _, _, err := resumeTreePage(context.Background(), dir, commit, path, after, limit); err == nil {
						walked++
					}
				}
			}
		}
	}
	if walked < pages/4 {
		t.Fatalf("the walk answered %d of %d pages: the trees do not exercise it", walked, pages)
	}
	t.Logf("%d pages, %d answered by the walk", pages, walked)
}
