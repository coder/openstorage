// Streaming ref-sync tests (issue #119): full syncs merge-join bounded
// store pages against for-each-ref's sorted stream — deletions flush in
// batches strictly before any update, updates spool one batch and fall back
// to a second pass on overflow, and a torn paged scan converges once a
// follow-up sync sees consistent state (the generation marker forces that
// follow-up in production). All tests drive syncRefsStreamed directly with
// tiny batch sizes so batching and overflow are exercised with small
// fixtures.
package githttp

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
)

// syncFromMap runs the streaming sync from an in-memory want set with the
// given batch size.
func syncFromMap(t *testing.T, repoPath string, want map[string]string, headTarget string, batch int) {
	t.Helper()
	names := make([]string, 0, len(want))
	for name := range want {
		names = append(names, name)
	}
	sort.Strings(names)
	newStore := func() refSeq { return &sliceRefSeq{names: names, shas: want} }
	if _, err := syncRefsStreamed(context.Background(), repoPath, headTarget, newStore, batch); err != nil {
		t.Fatalf("syncRefsStreamed(batch=%d): %v", batch, err)
	}
}

// localRefs snapshots the repo's refs as name -> sha.
func localRefs(t *testing.T, repoPath string) map[string]string {
	t.Helper()
	out := mustGit(t, repoPath, "for-each-ref", "--format=%(refname) %(objectname)")
	refs := map[string]string{}
	for line := range strings.SplitSeq(strings.TrimSpace(out), "\n") {
		if line == "" {
			continue
		}
		name, sha, ok := strings.Cut(line, " ")
		if !ok {
			t.Fatalf("for-each-ref emitted %q", line)
		}
		refs[name] = sha
	}
	return refs
}

// A D/F rename (delete refs/heads/foo/bar, create refs/heads/foo) must
// apply cleanly even when every batch is a separate update-ref transaction:
// all deletions land strictly before any update, or git's files backend
// rejects the create while the doomed loose ref still exists.
func TestSyncRefsStreamed_DFRenameDeletesFirst(t *testing.T) {
	dir := initWorkRepo(t)
	head := revParse(t, dir, "HEAD")
	mustGit(t, dir, "update-ref", "refs/heads/foo/bar", head)

	syncFromMap(t, dir, map[string]string{
		"refs/heads/main": head,
		"refs/heads/foo":  head,
	}, "refs/heads/main", 1)

	got := localRefs(t, dir)
	if _, ok := got["refs/heads/foo/bar"]; ok {
		t.Fatalf("refs/heads/foo/bar survived the rename sync: %v", got)
	}
	if got["refs/heads/foo"] != head {
		t.Fatalf("refs/heads/foo = %q, want %q", got["refs/heads/foo"], head)
	}
}

// Mass change with a tiny batch: creating from a cold cache overflows the
// update spool (second pass), deleting everything flushes multiple delete
// batches. State must converge exactly, and the update-ref transaction
// count must reflect batching (bounded transactions, not one giant one).
func TestSyncRefsStreamed_BatchesAndOverflow(t *testing.T) {
	dir := initWorkRepo(t)
	head := revParse(t, dir, "HEAD")

	want := map[string]string{"refs/heads/main": head}
	for i := range 7 {
		want[fmt.Sprintf("refs/heads/load-%02d", i)] = head
	}

	var (
		mu     sync.Mutex
		spawns []string
	)
	obs := func(spawnDir string, args []string) {
		if spawnDir != dir {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		spawns = append(spawns, args[0])
	}
	gitSpawnObserver.Store(&obs)
	defer gitSpawnObserver.Store(nil)

	// Cold-cache mass create: 7 new refs (main already exists), batch 2 →
	// the spool overflows and pass two applies updates in 4 transactions.
	syncFromMap(t, dir, want, "refs/heads/main", 2)
	got := localRefs(t, dir)
	for name, sha := range want {
		if got[name] != sha {
			t.Fatalf("after create sync, %s = %q, want %q", name, got[name], sha)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("after create sync, %d local refs, want %d: %v", len(got), len(want), got)
	}
	mu.Lock()
	counts := map[string]int{}
	for _, s := range spawns {
		counts[s]++
	}
	spawns = nil
	mu.Unlock()
	// Overflow: two for-each-ref (pass one + pass two), ceil(7/2)=4
	// update-ref transactions, one symbolic-ref.
	if counts["for-each-ref"] != 2 || counts["update-ref"] != 4 || counts["symbolic-ref"] != 1 {
		t.Fatalf("create sync spawned %v, want for-each-ref=2 update-ref=4 symbolic-ref=1", counts)
	}

	// Mass delete: back to main only, batch 2 → ceil(7/2)=4 delete
	// transactions in ONE pass (deletions never spool).
	syncFromMap(t, dir, map[string]string{"refs/heads/main": head}, "refs/heads/main", 2)
	got = localRefs(t, dir)
	if len(got) != 1 || got["refs/heads/main"] != head {
		t.Fatalf("after delete sync, refs = %v, want only main", got)
	}
	mu.Lock()
	counts = map[string]int{}
	for _, s := range spawns {
		counts[s]++
	}
	mu.Unlock()
	if counts["for-each-ref"] != 1 || counts["update-ref"] != 4 || counts["symbolic-ref"] != 1 {
		t.Fatalf("delete sync spawned %v, want for-each-ref=1 update-ref=4 symbolic-ref=1", counts)
	}
}

// A no-change full sync must not fork more than the pre-#119 code did:
// exactly one for-each-ref and one symbolic-ref (no update-ref, no second
// pass) — the TTL-expiry resync is the most common full-sync flavor.
func TestSyncRefsStreamed_NoChangeForkParity(t *testing.T) {
	dir := initWorkRepo(t)
	head := revParse(t, dir, "HEAD")
	want := map[string]string{"refs/heads/main": head}
	syncFromMap(t, dir, want, "refs/heads/main", 2)

	var (
		mu     sync.Mutex
		spawns []string
	)
	obs := func(spawnDir string, args []string) {
		if spawnDir != dir {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		spawns = append(spawns, args[0])
	}
	gitSpawnObserver.Store(&obs)
	defer gitSpawnObserver.Store(nil)

	syncFromMap(t, dir, want, "refs/heads/main", 2)

	mu.Lock()
	defer mu.Unlock()
	if len(spawns) != 2 || spawns[0] != "for-each-ref" || spawns[1] != "symbolic-ref" {
		t.Fatalf("no-change sync spawned %v, want [for-each-ref symbolic-ref]", spawns)
	}
}

// A paged scan torn by concurrent mutation can miss a ref that was deleted
// ahead of the cursor and recreated behind it — the sync then wrongly
// deletes the local copy. The sync must still complete, and a follow-up
// sync over consistent state must converge exactly: in production the
// mutation bumped the generation past this sync's marker, forcing exactly
// that follow-up on the next request.
func TestSyncRefsStreamed_TornScanHealsOnNextSync(t *testing.T) {
	dir := initWorkRepo(t)
	head := revParse(t, dir, "HEAD")
	mustGit(t, dir, "update-ref", "refs/heads/racy", head)

	// Torn view: the scan misses refs/heads/racy although the final store
	// state contains it → this sync wrongly deletes the local copy.
	syncFromMap(t, dir, map[string]string{"refs/heads/main": head}, "refs/heads/main", 2)
	if got := localRefs(t, dir); got["refs/heads/racy"] != "" {
		t.Fatalf("torn sync should have deleted refs/heads/racy (it was absent from the scan): %v", got)
	}

	// The healing follow-up sync sees consistent state and restores it.
	syncFromMap(t, dir, map[string]string{
		"refs/heads/main": head,
		"refs/heads/racy": head,
	}, "refs/heads/main", 2)
	got := localRefs(t, dir)
	if got["refs/heads/racy"] != head || got["refs/heads/main"] != head {
		t.Fatalf("healing sync did not converge: %v", got)
	}
}
