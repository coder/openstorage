package githttp

// A node's cache repository can hold an object that no registered pack holds (a
// cache that was copied or edited out of band, or a registry that was rolled
// back). git reads such an object, so its connectivity check lets a push update
// a ref to it, with an empty pack that carries nothing. Every other node
// hydrates from the registered packs alone and cannot write a ref at that
// object, so the ref would wedge the repository. The update hook refuses it.

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/ref"
)

func TestUpdateHookRefusesATargetThatNoRegisteredPackHolds(t *testing.T) {
	cache := t.TempDir()
	node, err := startNode(cache)
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(node.stop)
	repo := registerRepo(t, "t-unregistered-target")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/main")
	c1 := revParse(t, work, "HEAD")
	tree := revParse(t, work, c1+"^{tree}")

	// The node's cache holds a commit that no registered pack holds. Its tree
	// and parent are in a registered pack, so git's connectivity check passes.
	cacheOnly := plantCommit(t, RepoPath(cache, repo), tree, c1)
	cmd := exec.Command("git", "pack-objects", "--stdout", "-q")
	cmd.Dir = work
	emptyPack, err := cmd.Output() // no object ids on stdin: a pack with no objects
	if err != nil {
		t.Fatalf("git pack-objects of nothing: %v", err)
	}

	const refused = "no pack of this repository's registry holds"
	for _, name := range []string{"refs/heads/planted", "refs/tags/planted", "refs/notes/planted"} {
		t.Run(name, func(t *testing.T) {
			report, remote := rawReceivePack(t, node, repo, token, false, emptyPack, rawPushCommand{ref.ZeroSHA, cacheOnly, name})
			assertNoCrashText(t, remote)
			if !strings.HasPrefix(report[name], "ng") || !strings.Contains(remote, refused) {
				t.Fatalf("%s at a commit that only the cache holds was not refused with %q: report %v\nremote:\n%s", name, refused, report, remote)
			}
			if got, ok := pgRefs(t, repo)[name]; ok {
				t.Fatalf("%s = %s after a refused push, want no ref", name, got)
			}
		})
	}

	// A commit that a registered pack holds still works, with the same empty pack.
	t.Run("a registered commit", func(t *testing.T) {
		const name = "refs/heads/copy"
		report, remote := rawReceivePack(t, node, repo, token, false, emptyPack, rawPushCommand{ref.ZeroSHA, c1, name})
		if report[name] != "ok" || pgRefs(t, repo)[name] != c1 {
			t.Fatalf("a branch at a registered commit: report %v, ref %s\nremote:\n%s", report, pgRefs(t, repo)[name], remote)
		}
	})
}
