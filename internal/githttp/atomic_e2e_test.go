package githttp

// End-to-end tests for issue #28: `git push --atomic` with mixed policy
// outcomes must not register unreachable packs. Registered packs are not
// sweeper orphans and repack folds carry every registered object forward,
// so before the wire-capability gate the leak below was permanent.

import (
	"strings"
	"testing"
)

// TestAtomicPush_MixedPolicyStoresNothing reproduces the issue #28 leak
// vector: an atomic push pairing a policy-DENIED update (named to sort
// first — the denied-first hook ordering is the one that leaked) with an
// allowed update that needs the pack. The whole push must be denied at
// pre-receive with nothing published or registered. The same refspecs
// without --atomic keep their per-ref semantics (allowed ref lands, pack
// publishes), and a wholly-allowed atomic push is unaffected.
func TestAtomicPush_MixedPolicyStoresNothing(t *testing.T) {
	repo := registerRepo(t, "atomic-mixed")
	work := initWorkRepo(t)
	mixed := mintTestToken(t, repo, "git:read,git:write",
		`[["refs/heads/aa-denied",["no-push"]],["*",[]]]`)

	// Atomic mixed push: denied wholesale BEFORE publication.
	out, err := tryGit(work, "push", "--atomic", remoteURL(nodeA, repo, mixed),
		"HEAD:refs/heads/aa-denied", "HEAD:refs/heads/zz-allowed")
	if err == nil {
		t.Fatalf("atomic mixed push succeeded:\n%s", out)
	}
	if !strings.Contains(out, "atomic push carries a policy-denied update") {
		t.Fatalf("denial does not name the atomic gate:\n%s", out)
	}
	t.Logf("atomic mixed push output:\n%s", out)
	if objs := s3Packs(t, repo); len(objs) != 0 {
		t.Fatalf("atomic mixed push left pack blobs: %v", objs)
	}
	if packs := pgPacks(t, repo); len(packs) != 0 {
		t.Fatalf("atomic mixed push left registered packs: %v", packs)
	}
	if refs := pgRefs(t, repo); len(refs) != 0 {
		t.Fatalf("atomic mixed push landed refs: %v", refs)
	}

	// Same refspecs without --atomic: unchanged per-ref semantics.
	out, err = tryGit(work, "push", remoteURL(nodeA, repo, mixed),
		"HEAD:refs/heads/aa-denied", "HEAD:refs/heads/zz-allowed")
	if err == nil {
		t.Fatalf("non-atomic mixed push should exit non-zero (aa-denied is rejected):\n%s", out)
	}
	t.Logf("non-atomic mixed push output:\n%s", out)
	head := revParse(t, work, "HEAD")
	refs := pgRefs(t, repo)
	if refs["refs/heads/zz-allowed"] != head {
		t.Fatalf("zz-allowed = %q, want %q (allowed ref must land without --atomic)", refs["refs/heads/zz-allowed"], head)
	}
	if _, exists := refs["refs/heads/aa-denied"]; exists {
		t.Fatal("aa-denied landed; the no-push policy did not fire")
	}
	if packs := pgPacks(t, repo); len(packs) == 0 {
		t.Fatal("non-atomic mixed push registered no packs; the surviving ref's pack is missing")
	}

	// Wholly-allowed atomic push (new bytes, so a pack is required): the
	// gate must not regress legitimate --atomic use.
	head2 := commitFile(t, work, "b.txt", "two\n", "two", nil)
	out = mustGit(t, work, "push", "--atomic", remoteURL(nodeA, repo, mixed),
		"HEAD:refs/heads/zz-allowed", "HEAD:refs/heads/zz-second")
	t.Logf("wholly-allowed atomic push output:\n%s", out)
	refs = pgRefs(t, repo)
	if refs["refs/heads/zz-allowed"] != head2 || refs["refs/heads/zz-second"] != head2 {
		t.Fatalf("wholly-allowed atomic push did not land both refs: %v", refs)
	}
}

// TestAtomicPush_DeleteOnlyMixedDeniedAsUnit covers the no-pack pre-receive
// path (delete-only pushes have no quarantine): an atomic push mixing an
// allowed deletion with a denied one must deny BOTH — before the gate, the
// allowed deletion's update hook could commit its PG CAS durably and then
// survive a push git reports as wholly failed (the durable-CAS twist). The
// allowed deletion is named to sort first so its update hook would have run
// before the denial. Without --atomic the per-ref semantics stay unchanged.
func TestAtomicPush_DeleteOnlyMixedDeniedAsUnit(t *testing.T) {
	repo := registerRepo(t, "atomic-delete-only")
	work := initWorkRepo(t)
	full := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, full),
		"HEAD:refs/heads/main", "HEAD:refs/heads/aa-allowed-del", "HEAD:refs/heads/zz-denied-del")

	mixed := mintTestToken(t, repo, "git:read,git:write",
		`[["refs/heads/zz-denied-del",["no-push"]],["*",[]]]`)
	out, err := tryGit(work, "push", "--atomic", remoteURL(nodeA, repo, mixed),
		":refs/heads/aa-allowed-del", ":refs/heads/zz-denied-del")
	if err == nil {
		t.Fatalf("atomic mixed deletion succeeded:\n%s", out)
	}
	if !strings.Contains(out, "atomic push carries a policy-denied update") {
		t.Fatalf("denial does not name the atomic gate:\n%s", out)
	}
	t.Logf("atomic mixed deletion output:\n%s", out)
	refs := pgRefs(t, repo)
	if _, ok := refs["refs/heads/aa-allowed-del"]; !ok {
		t.Fatal("aa-allowed-del was deleted despite the failed atomic push (durable-CAS twist)")
	}
	if _, ok := refs["refs/heads/zz-denied-del"]; !ok {
		t.Fatal("zz-denied-del was deleted; the no-push policy did not fire")
	}

	// Without --atomic: allowed deletion lands, denied one survives.
	out, err = tryGit(work, "push", remoteURL(nodeA, repo, mixed),
		":refs/heads/aa-allowed-del", ":refs/heads/zz-denied-del")
	if err == nil {
		t.Fatalf("non-atomic mixed deletion should exit non-zero (zz-denied-del is rejected):\n%s", out)
	}
	t.Logf("non-atomic mixed deletion output:\n%s", out)
	refs = pgRefs(t, repo)
	if _, ok := refs["refs/heads/aa-allowed-del"]; ok {
		t.Fatal("aa-allowed-del survived a non-atomic push that should delete it")
	}
	if _, ok := refs["refs/heads/zz-denied-del"]; !ok {
		t.Fatal("zz-denied-del was deleted by the non-atomic push")
	}
}

// TestAtomicPush_ServeEnvNotInherited pins the round-2 fix: an operator
// exporting CODESTORAGE_GIT_ATOMIC=1 in the serve process environment must
// not make the hooks treat every push as atomic — the variable is a
// per-request bit owned by the wire sniff, and duplicate env entries
// resolve first-match in Go children, so it must be stripped, not merely
// overridden by appending.
func TestAtomicPush_ServeEnvNotInherited(t *testing.T) {
	node := startNodeT(t, EnvAtomic+"=1")
	repo := registerRepo(t, "atomic-env-not-inherited")
	work := initWorkRepo(t)
	mixed := mintTestToken(t, repo, "git:read,git:write",
		`[["refs/heads/aa-denied",["no-push"]],["*",[]]]`)

	// NON-atomic mixed push: per-ref semantics must survive the polluted
	// serve environment (the allowed ref lands).
	out, err := tryGit(work, "push", remoteURL(node, repo, mixed),
		"HEAD:refs/heads/aa-denied", "HEAD:refs/heads/zz-allowed")
	if err == nil {
		t.Fatalf("non-atomic mixed push should exit non-zero (aa-denied is rejected):\n%s", out)
	}
	if strings.Contains(out, "atomic push carries a policy-denied update") {
		t.Fatalf("non-atomic push hit the atomic gate; serve env leaked into the hook:\n%s", out)
	}
	head := revParse(t, work, "HEAD")
	if refs := pgRefs(t, repo); refs["refs/heads/zz-allowed"] != head {
		t.Fatalf("zz-allowed = %q, want %q (allowed ref must land)", refs["refs/heads/zz-allowed"], head)
	}
}
