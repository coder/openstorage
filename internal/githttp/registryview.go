package githttp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/coder/openstorage/internal/ref"
)

// A node's cache repository can hold an object that no registered pack holds:
// a cache that was copied, restored or edited out of band, or a registry that
// was rolled back. git reads such an object without complaint, so "git can
// read it" proves nothing about the repository's durable state. Every other
// node, and this node after its cache is gone, hydrates from the registered
// packs alone and cannot write a ref at that object.
//
// RegisteredPackView is the repository as those nodes see it: an object
// directory that holds exactly the packs that the registry lists. A git child
// that runs with GIT_OBJECT_DIRECTORY set to it, and with no alternates, reads
// an object only when a registered pack holds it.

// registryViewHealRounds bounds how often NewRegisteredPackView lists the packs
// again after it fetched the ones that the cache lacked.
const registryViewHealRounds = 3

// RegisteredPackView is a temporary object directory that links the pack files
// of the repository's cache that the registry lists, and nothing else.
type RegisteredPackView struct {
	dir      string
	repoPath string
	repo     string
	// absent lists the registered packs that the cache still lacked after the
	// heal rounds. The view does not link them.
	absent []string
	// listed holds the IDs of every pack that the registry listed when the view
	// was built.
	listed map[string]bool
}

// NewRegisteredPackView lists the packs that st has for repo and links their
// files from repoPath's object directory. A registered pack that is not in the
// cache (a fold replaced the packs since the cache was last hydrated, or
// another node registered it a moment ago) is fetched by heal and the listing
// is read again, at most registryViewHealRounds times, because packs keep
// arriving while pushes run. A pack that the cache still lacks after that is
// left out of the view: an object that the view
// holds is held by a registered pack, but an object that it does not hold may
// be in a pack that it lacks. A caller that reads "missing" from an incomplete
// view must call CheckAnswer.
func NewRegisteredPackView(ctx context.Context, st ref.RefStore, repoPath, repo string, heal func(context.Context) error) (*RegisteredPackView, error) {
	packDir := filepath.Join(repoPath, "objects", "pack")
	var packs, present []ref.Pack
	var absent []string
	for attempt := 0; ; attempt++ {
		var err error
		if packs, err = st.ListPacks(ctx, repo); err != nil {
			return nil, fmt.Errorf("list the packs of %s: %w", repo, err)
		}
		present, absent = present[:0], absent[:0]
		for _, p := range packs {
			if packFilesExist(packDir, p.ID) {
				present = append(present, p)
			} else {
				absent = append(absent, p.ID)
			}
		}
		if len(absent) == 0 || heal == nil || attempt >= registryViewHealRounds {
			break
		}
		if err := heal(ctx); err != nil {
			return nil, fmt.Errorf("fetch the registered packs of %s: %w", repo, err)
		}
	}
	dir, err := os.MkdirTemp("", "openstorage-registry-view-")
	if err != nil {
		return nil, err
	}
	listed := make(map[string]bool, len(packs))
	for _, p := range packs {
		listed[p.ID] = true
	}
	v := &RegisteredPackView{dir: dir, repoPath: repoPath, repo: repo, absent: absent, listed: listed}
	viewPacks := filepath.Join(dir, "pack")
	if err := os.Mkdir(viewPacks, 0o700); err != nil {
		v.Close()
		return nil, err
	}
	for _, p := range present {
		for _, ext := range []string{".pack", ".idx"} {
			name := "pack-" + p.ID + ext
			if err := os.Symlink(filepath.Join(packDir, name), filepath.Join(viewPacks, name)); err != nil {
				v.Close()
				return nil, err
			}
		}
	}
	return v, nil
}

// listingChanged reports whether the registry now lists a pack that the view was
// not built from. Another node can register a pack that holds an object after
// the view was built, so a "missing" answer is final only when this is false.
func (v *RegisteredPackView) listingChanged(ctx context.Context, st ref.RefStore) (bool, error) {
	packs, err := st.ListPacks(ctx, v.repo)
	if err != nil {
		return false, fmt.Errorf("list the packs of %s: %w", v.repo, err)
	}
	for _, p := range packs {
		if !v.listed[p.ID] {
			return true, nil
		}
	}
	return false, nil
}

// AskRegisteredPacks builds a view, calls ask with it and returns the answer of
// ask, the type of each id as the registered packs show it ("missing" for an
// object that no registered pack holds). A "missing" answer stands only when the
// registry lists no pack that the view was built without, so it builds a new
// view and asks again, at most registryViewHealRounds more times, when another
// node registered a pack since the view was built. An incomplete view fails a
// "missing" answer (CheckAnswer).
func AskRegisteredPacks(ctx context.Context, st ref.RefStore, repoPath, repo string, heal func(context.Context) error, ask func(*RegisteredPackView) (map[string]string, error)) (map[string]string, error) {
	for round := 0; ; round++ {
		view, err := NewRegisteredPackView(ctx, st, repoPath, repo, heal)
		if err != nil {
			return nil, err
		}
		kinds, err := ask(view)
		if err == nil && hasMissing(kinds) && round < registryViewHealRounds {
			var changed bool
			if changed, err = view.listingChanged(ctx, st); err == nil && changed {
				view.Close()
				continue
			}
		}
		if err == nil {
			err = view.CheckAnswer(kinds)
		}
		view.Close()
		if err != nil {
			return nil, err
		}
		return kinds, nil
	}
}

func hasMissing(kinds map[string]string) bool {
	for _, k := range kinds {
		if k == "missing" {
			return true
		}
	}
	return false
}

// CheckAnswer returns an error that wraps ErrPackMissing when kinds, an answer
// that git gave over this view, reads an object as "missing" and the view lacks
// a registered pack: the object may be in that pack, so refusing it would be
// refusing a ref that is healthy. The caller must fail the request and let the
// client retry. Every other answer stands: an object that the view holds is
// held by a registered pack.
func (v *RegisteredPackView) CheckAnswer(kinds map[string]string) error {
	if len(v.absent) == 0 {
		return nil
	}
	for _, k := range kinds {
		if k == "missing" {
			return fmt.Errorf("%w: the cache of %s lacks the registered pack %s", ErrPackMissing, v.repo, v.absent[0])
		}
	}
	return nil
}

func packFilesExist(packDir, id string) bool {
	for _, ext := range []string{".pack", ".idx"} {
		if _, err := os.Stat(filepath.Join(packDir, "pack-"+id+ext)); err != nil {
			return false
		}
	}
	return true
}

// Close removes the view. The pack files that it linked stay.
func (v *RegisteredPackView) Close() {
	if v != nil && v.dir != "" {
		_ = os.RemoveAll(v.dir)
	}
}

// Env returns base with the variables that point git at the cache repository
// and at the view, and none of git's other repository variables.
func (v *RegisteredPackView) Env(base []string) []string {
	out := make([]string, 0, len(base)+2)
	for _, kv := range base {
		name, _, _ := strings.Cut(kv, "=")
		switch name {
		case "GIT_DIR", "GIT_COMMON_DIR", "GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES", "GIT_QUARANTINE_PATH":
		default:
			out = append(out, kv)
		}
	}
	return append(out, "GIT_DIR="+v.repoPath, "GIT_OBJECT_DIRECTORY="+v.dir)
}

// EnvForPush returns base for a git child that reads the registered packs and
// the quarantine of a push in flight, and nothing else of the node's cache. With
// a quarantine path, git keeps the quarantine as its object directory and the
// view replaces the main store as the alternate. Without one, the view is the
// object directory.
func (v *RegisteredPackView) EnvForPush(base []string, quarantine string) []string {
	if quarantine == "" {
		return v.Env(base)
	}
	out := make([]string, 0, len(base)+2)
	for _, kv := range base {
		name, _, _ := strings.Cut(kv, "=")
		if name == "GIT_ALTERNATE_OBJECT_DIRECTORIES" {
			continue
		}
		out = append(out, kv)
	}
	return append(out, "GIT_ALTERNATE_OBJECT_DIRECTORIES="+v.dir)
}

// ParseObjectKinds reads the output of `git cat-file
// --batch-check=%(objecttype)` for ids: one line for each id, the object's
// type or "<id> missing".
func ParseObjectKinds(ids []string, out string) (map[string]string, error) {
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	if len(lines) != len(ids) {
		return nil, fmt.Errorf("cat-file --batch-check answered %d lines for %d ids", len(lines), len(ids))
	}
	kinds := make(map[string]string, len(ids))
	for i, l := range lines {
		switch l {
		case "commit", "tree", "blob", "tag":
			kinds[ids[i]] = l
		case ids[i] + " missing":
			kinds[ids[i]] = "missing"
		default:
			return nil, errors.New("cat-file --batch-check answered an unknown line for " + ids[i])
		}
	}
	return kinds, nil
}
