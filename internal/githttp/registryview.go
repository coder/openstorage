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
}

// NewRegisteredPackView lists the packs that st has for repo and links their
// files from repoPath's object directory. A registered pack that is not in the
// cache (a fold replaced the packs since the cache was last hydrated) is
// fetched by heal, once, and the listing is read again. A nil heal, or a pack
// that is still missing after it, is an error that wraps ErrPackMissing: the
// view would lack objects that the registry holds, and a caller that read
// "missing" from it would refuse a ref that is healthy.
func NewRegisteredPackView(ctx context.Context, st ref.RefStore, repoPath, repo string, heal func(context.Context) error) (*RegisteredPackView, error) {
	packDir := filepath.Join(repoPath, "objects", "pack")
	var packs []ref.Pack
	// Another node can register a pack between the listing and the heal, so a
	// listing that still names a missing pack heals again, within a bound, like
	// HydratePacks.
	for attempt := 0; ; attempt++ {
		var err error
		if packs, err = st.ListPacks(ctx, repo); err != nil {
			return nil, fmt.Errorf("list the packs of %s: %w", repo, err)
		}
		missing := ""
		for _, p := range packs {
			if !packFilesExist(packDir, p.ID) {
				missing = p.ID
				break
			}
		}
		if missing == "" {
			break
		}
		if heal == nil || attempt >= registryViewHealRounds {
			return nil, fmt.Errorf("%w: the cache of %s lacks the registered pack %s", ErrPackMissing, repo, missing)
		}
		if err := heal(ctx); err != nil {
			return nil, fmt.Errorf("fetch the registered packs of %s: %w", repo, err)
		}
	}
	dir, err := os.MkdirTemp("", "openstorage-registry-view-")
	if err != nil {
		return nil, err
	}
	v := &RegisteredPackView{dir: dir, repoPath: repoPath}
	viewPacks := filepath.Join(dir, "pack")
	if err := os.Mkdir(viewPacks, 0o700); err != nil {
		v.Close()
		return nil, err
	}
	for _, p := range packs {
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
