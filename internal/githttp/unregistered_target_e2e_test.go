package githttp

// A node's cache repository can hold an object that no registered pack holds (a
// cache that was copied or edited out of band, or a registry that was rolled
// back). git reads such an object, so its connectivity check lets a push update
// a ref to it, with an empty pack that carries nothing. Every other node
// hydrates from the registered packs alone and cannot write a ref at that
// object, so the ref would wedge the repository. Pre-receive refuses the push.

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/ref"
)

// unregisteredFixture is a node with one pushed commit, and a second commit
// that only the node's cache holds.
type unregisteredFixture struct {
	node      *testNode
	repo      string
	work      string
	token     string
	c1        string
	cacheOnly string
	emptyPack []byte
}

func newUnregisteredFixture(t *testing.T, name string) *unregisteredFixture {
	t.Helper()
	cache := t.TempDir()
	node, err := startNode(cache)
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(node.stop)
	f := &unregisteredFixture{node: node, repo: registerRepo(t, name), work: initWorkRepo(t)}
	f.token = mintTestToken(t, f.repo, "git:read,git:write", "")
	mustGit(t, f.work, "push", remoteURL(node, f.repo, f.token), "HEAD:refs/heads/main")
	f.c1 = revParse(t, f.work, "HEAD")
	tree := revParse(t, f.work, f.c1+"^{tree}")
	// The commit's tree and parent are in a registered pack, so git's
	// connectivity check passes. The same commit exists in the work repository.
	f.cacheOnly = plantCommit(t, RepoPath(cache, f.repo), tree, f.c1)
	if again := plantCommit(t, f.work, tree, f.c1); again != f.cacheOnly {
		t.Fatalf("planted commits differ: %s and %s", f.cacheOnly, again)
	}
	cmd := exec.Command("git", "pack-objects", "--stdout", "-q")
	cmd.Dir = f.work
	if f.emptyPack, err = cmd.Output(); err != nil { // no object ids on stdin: a pack with no objects
		t.Fatalf("git pack-objects of nothing: %v", err)
	}
	return f
}

const unregisteredRefusal = "no pack of this repository's registry holds"

func TestPreReceiveRefusesATargetThatNoRegisteredPackHolds(t *testing.T) {
	f := newUnregisteredFixture(t, "t-unregistered-target")
	for _, name := range []string{"refs/heads/planted", "refs/tags/planted", "refs/notes/planted"} {
		t.Run(name, func(t *testing.T) {
			report, remote := rawReceivePack(t, f.node, f.repo, f.token, false, f.emptyPack, rawPushCommand{ref.ZeroSHA, f.cacheOnly, name})
			assertNoCrashText(t, remote)
			if !strings.HasPrefix(report[name], "ng") || !strings.Contains(remote, unregisteredRefusal) {
				t.Fatalf("%s at a commit that only the cache holds was not refused with %q: report %v\nremote:\n%s", name, unregisteredRefusal, report, remote)
			}
			if got, ok := pgRefs(t, f.repo)[name]; ok {
				t.Fatalf("%s = %s after a refused push, want no ref", name, got)
			}
		})
	}

	// A commit that a registered pack holds still works, with the same empty pack.
	t.Run("a registered commit", func(t *testing.T) {
		const name = "refs/heads/copy"
		report, remote := rawReceivePack(t, f.node, f.repo, f.token, false, f.emptyPack, rawPushCommand{ref.ZeroSHA, f.c1, name})
		if report[name] != "ok" || pgRefs(t, f.repo)[name] != f.c1 {
			t.Fatalf("a branch at a registered commit: report %v, ref %s\nremote:\n%s", report, pgRefs(t, f.repo)[name], remote)
		}
	})

	// A push that carries the commit in its pack registers it, so the object is
	// no longer cache-only: the push heals the node.
	t.Run("the pack of the push carries the commit", func(t *testing.T) {
		const name = "refs/heads/healed"
		pack := packObjectList(t, f.work, f.cacheOnly)
		report, remote := rawReceivePack(t, f.node, f.repo, f.token, false, pack, rawPushCommand{ref.ZeroSHA, f.cacheOnly, name})
		if report[name] != "ok" || pgRefs(t, f.repo)[name] != f.cacheOnly {
			t.Fatalf("a push that carries the cache-only commit: report %v, ref %s\nremote:\n%s", report, pgRefs(t, f.repo)[name], remote)
		}
	})
}

// Under --atomic, an update hook's compare-and-swap for an earlier command
// commits before git refuses a later one. The check runs in pre-receive, so no
// ref of the push moves, atomic or not.
func TestPreReceiveRefusesTheWholePushForATargetThatNoRegisteredPackHolds(t *testing.T) {
	for _, atomic := range []bool{true, false} {
		name := "t-unregistered-whole"
		if atomic {
			name += "-atomic"
		}
		t.Run(name, func(t *testing.T) {
			f := newUnregisteredFixture(t, name)
			report, remote := rawReceivePack(t, f.node, f.repo, f.token, atomic, f.emptyPack,
				rawPushCommand{ref.ZeroSHA, f.c1, "refs/heads/fine"}, rawPushCommand{ref.ZeroSHA, f.cacheOnly, "refs/heads/planted"})
			assertNoCrashText(t, remote)
			if !strings.Contains(remote, unregisteredRefusal) {
				t.Fatalf("the push was not refused with %q: report %v\nremote:\n%s", unregisteredRefusal, report, remote)
			}
			refs := pgRefs(t, f.repo)
			if _, ok := refs["refs/heads/fine"]; ok {
				t.Fatalf("refs/heads/fine moved although the push was refused: %v", refs)
			}
			if _, ok := refs["refs/heads/planted"]; ok {
				t.Fatalf("refs/heads/planted exists: %v", refs)
			}
		})
	}
}
