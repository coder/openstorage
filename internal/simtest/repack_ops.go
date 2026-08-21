// repack_ops.go adds the issue-#1 background repack to the simulation
// (the "pack rows only grow, modulo repack" oracle the harness deferred
// until repack existed):
//
//   - opRepack folds through the node's FAULT-INJECTED stores, exercising
//     clean/ambiguous faults on the fold pipeline (list, upload, replace);
//   - opRepackRace interleaves two folds deterministically on the CLEAN
//     oracle stores — a mid-race push changes the winner's input set — and
//     asserts the loser installed nothing and deleted exactly its own blob;
//   - the pack-row oracle (checkPackRows) asserts after every step that a
//     repo's row set only ever GROWS except across a repack, whose exact
//     before→after transition is pinned in-op (atomicity: the observable
//     state is the pre-set or the folded set, never anything between).
package simtest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"

	"codestorage/internal/blob"
	"codestorage/internal/githttp"
	"codestorage/internal/jobs"
	"codestorage/internal/ref"
)

// discardLogger silences the Repacker inside simulations (its slog output
// is wall-clock-stamped and must not reach traces).
func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// packSet renders a pack row set as a deterministic map id->key.
func packSet(packs []ref.Pack) map[string]string {
	m := make(map[string]string, len(packs))
	for _, p := range packs {
		m[p.ID] = p.Key
	}
	return m
}

// packSetToken renders a row set for trace lines: sorted ids, no paths.
func packSetToken(set map[string]string) string {
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return strings.Join(ids, ",")
}

func setsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// oraclePackSet lists repo's pack rows through the clean oracle handle.
func (s *sim) oraclePackSet(repoKey string) (map[string]string, error) {
	packs, err := s.cl.oracle.ListPacks(context.Background(), repoKey)
	if err != nil {
		return nil, fmt.Errorf("oracle ListPacks %s: %w", repoKey, err)
	}
	return packSet(packs), nil
}

// foldOutcome computes the row set a won fold of old into newPack yields.
func foldOutcome(pre map[string]string, old []ref.Pack, newPack ref.Pack) map[string]string {
	out := make(map[string]string, len(pre))
	for id, key := range pre {
		out[id] = key
	}
	for _, p := range old {
		delete(out, p.ID)
	}
	out[newPack.ID] = newPack.Key
	return out
}

// doRepack runs one background fold on the node's fault-injected stores and
// pins the replacement-atomicity invariant: the post-op row set is exactly
// the pre-set (fold aborted or lost) or exactly the folded set — never a
// partial delete, never an insert without the deletes.
func (s *sim) doRepack(name string, node *simNode, repo *simRepo) error {
	ctx := context.Background()
	pre, err := s.oraclePackSet(repo.key)
	if err != nil {
		return err
	}
	if len(pre) < 2 {
		s.tracef("step=%04d op=%s node=%d repo=%s out=skip-nothing-to-fold", s.step, name, node.id, repo.key)
		return nil
	}

	// The fold input is what the (possibly faulted) repacker itself lists;
	// capture the swap arguments through the seam so the oracle can compute
	// the exact expected outcome even under faults.
	var swapOld []ref.Pack
	var swapNew ref.Pack
	inner := &captureReplace{RefStore: node.simRef, old: &swapOld, new: &swapNew}
	r := jobs.NewRepacker(inner, node.simBlob, func(ctx context.Context, repoKey string) (string, error, error) {
		return githttp.Hydrate(ctx, node.simRef, node.simBlob, node.cacheDir, repoKey)
	}, discardLogger())

	mark := s.inj.Seq()
	rerr := r.Repack(ctx, repo.key)
	evs, _ := s.inj.FiredSince(mark)

	post, err := s.oraclePackSet(repo.key)
	if err != nil {
		return err
	}
	allowed := []map[string]string{pre}
	if swapNew.ID != "" {
		allowed = append(allowed, foldOutcome(pre, swapOld, swapNew))
	}
	ok := false
	for _, a := range allowed {
		if setsEqual(post, a) {
			ok = true
			break
		}
	}
	if !ok {
		s.violate("step %d: repack atomicity: %s rows [%s] not the pre-set [%s] nor the folded set (old=%d new=%s)",
			s.step, repo.key, packSetToken(post), packSetToken(pre), len(swapOld), swapNew.ID)
		return nil
	}
	// The fold never shrinks the registered OBJECT set, so invariant (c)
	// plus cold hydrate (checkFull) remain the byte-level correctness
	// oracles; update the row baseline to the observed post-state.
	s.prevPacks[repo.key] = post

	outcome := "folded"
	switch {
	case rerr != nil:
		outcome = "err-" + errToken(rerr)
	case setsEqual(post, pre):
		outcome = "noop"
	}
	s.tracef("step=%04d op=%s node=%d repo=%s pre=[%s] post=[%s] out=%s faults=[%s]",
		s.step, name, node.id, repo.key, packSetToken(pre), packSetToken(post), outcome, faultToken(evs))
	return nil
}

// captureReplace records the ReplacePacks arguments flowing to the inner
// store so the scheduler's atomicity oracle knows the exact intended fold.
type captureReplace struct {
	ref.RefStore
	old *[]ref.Pack
	new *ref.Pack
}

func (c *captureReplace) ReplacePacks(ctx context.Context, repo string, old []ref.Pack, newPack ref.Pack) (bool, error) {
	*c.old = append([]ref.Pack(nil), old...)
	*c.new = newPack
	return c.RefStore.ReplacePacks(ctx, repo, old, newPack)
}

// doRepackRace interleaves two competing folds deterministically on the
// CLEAN oracle stores (fault interplay is opRepack's job; this op pins the
// race semantics exactly):
//
//	loser lists + packs + uploads → [mid-race push lands; winner folds] →
//	loser's ReplacePacks loses → loser deletes its own uploaded blob.
//
// Oracle: exactly the winner's fold installs; the loser leaves no DB change
// and no orphan blob; the mid-race push's row survives the winner's fold
// input snapshot semantics (it IS part of the winner's input here because
// the winner lists after it lands).
func (s *sim) doRepackRace(name string, node *simNode, repo *simRepo, branch string) error {
	ctx := context.Background()
	pre, err := s.oraclePackSet(repo.key)
	if err != nil {
		return err
	}
	if len(pre) < 2 {
		s.tracef("step=%04d op=%s node=%d repo=%s out=skip-nothing-to-fold", s.step, name, node.id, repo.key)
		return nil
	}

	// Snapshot the repo's blob KEY set before the race: the only upload
	// between here and the loser's swap window is the loser's own
	// consolidated pack, so a listing diff identifies it exactly (crashed
	// pushes may have left unregistered orphans, but those predate this
	// snapshot).
	preObjs, err := s.cl.oracleBl.List(ctx, blob.PackPrefix(repo.key))
	if err != nil {
		return fmt.Errorf("step %d: list pre-race blobs: %w", s.step, err)
	}
	preKeys := make(map[string]bool, len(preObjs))
	for _, o := range preObjs {
		preKeys[o.Key] = true
	}

	raceDir := s.raceCacheDir(node)
	winner := jobs.NewRepacker(s.cl.oracle, s.cl.oracleBl, func(ctx context.Context, repoKey string) (string, error, error) {
		return githttp.Hydrate(ctx, s.cl.oracle, s.cl.oracleBl, raceDir+"-winner", repoKey)
	}, discardLogger())
	loser := jobs.NewRepacker(s.cl.oracle, s.cl.oracleBl, func(ctx context.Context, repoKey string) (string, error, error) {
		return githttp.Hydrate(ctx, s.cl.oracle, s.cl.oracleBl, raceDir+"-loser", repoKey)
	}, discardLogger())

	var loserKey string
	var winnerErr error
	loser.BeforeSwap = func(ctx context.Context) error {
		// The loser's consolidated blob is uploaded by now: it is the only
		// key missing from the pre-race snapshot.
		objs, err := s.cl.oracleBl.List(ctx, blob.PackPrefix(repo.key))
		if err != nil {
			return err
		}
		for _, o := range objs {
			if !preKeys[o.Key] {
				loserKey = o.Key
			}
		}
		// A mid-race push changes the winner's fold input, so the two
		// consolidated packs differ and the loser's cleanup must delete
		// real bytes (identity folds would mask a cleanup bug).
		if err := s.oraclePush(ctx, repo, branch); err != nil {
			return err
		}
		winnerErr = winner.Repack(ctx, repo.key)
		return nil
	}
	if err := loser.Repack(ctx, repo.key); err != nil {
		return fmt.Errorf("step %d: loser repack: %w", s.step, err)
	}
	if winnerErr != nil {
		return fmt.Errorf("step %d: winner repack: %w", s.step, winnerErr)
	}
	if loserKey == "" {
		return fmt.Errorf("step %d: race op could not identify the loser's uploaded blob", s.step)
	}

	post, err := s.oraclePackSet(repo.key)
	if err != nil {
		return err
	}
	// Exactly one fold installed: the winner folded its whole input (the
	// pre-set plus the mid-race push's pack) into one row.
	if len(post) != 1 {
		s.violate("step %d: repack race: %s rows [%s] after the race, want exactly the winner's fold",
			s.step, repo.key, packSetToken(post))
		return nil
	}
	for id := range pre {
		if _, still := post[id]; still {
			s.violate("step %d: repack race: pre-race row %s survived the winner's fold", s.step, id)
			return nil
		}
	}
	// Lost race leaves no leaked blob: the loser's upload must be gone...
	if _, err := s.cl.oracleBl.Stat(ctx, loserKey); err == nil {
		s.violate("step %d: repack race: loser's blob %s survived its lost race", s.step, loserKey)
		return nil
	} else if !errors.Is(err, blob.ErrNotFound) {
		return fmt.Errorf("step %d: stat loser blob: %w", s.step, err)
	}
	// ...while the winner's install and every superseded blob stay (the
	// sweeper owns superseded blobs; per-step invariant (c) covers the
	// winner's row).
	for id, key := range pre {
		if _, err := s.cl.oracleBl.Stat(ctx, key); err != nil {
			s.violate("step %d: repack race: superseded blob %s (row %s) vanished synchronously: %v",
				s.step, key, id, err)
			return nil
		}
	}
	s.prevPacks[repo.key] = post

	s.tracef("step=%04d op=%s node=%d repo=%s pre=[%s] post=[%s] out=raced",
		s.step, name, node.id, repo.key, packSetToken(pre), packSetToken(post))
	return nil
}

// raceCacheDir names the private hydration caches the race op's two
// repackers use. Deterministic (node id only): reusing them across race
// ops is fine — hydration is idempotent over a rebuildable cache.
func (s *sim) raceCacheDir(node *simNode) string {
	return node.cacheDir + "-race"
}

// oraclePush lands one mid-race commit through the clean oracle stores:
// mint, pack, upload, register, CAS. The committed transition is recorded
// in observed (delivery soundness needs no entry — no webhook flows emit
// here — but the CAS moves ground truth, and later warm checks must not
// call it foul play). Node beliefs are left stale on purpose: stale beliefs
// are the harness's normal CAS-conflict fuel.
func (s *sim) oraclePush(ctx context.Context, repo *simRepo, branch string) error {
	old, err := s.groundTip(repo.key, branch)
	if err != nil {
		return err
	}
	parent := ""
	if old != ref.ZeroSHA {
		parent = old
	}
	mc, err := mintCommit(repo.scratch, parent, fmt.Sprintf("%s %s mid-race step %d", repo.key, branch, s.step), s.step)
	if err != nil {
		return fmt.Errorf("mint mid-race commit: %w", err)
	}
	packPath, err := packObjects(repo.scratch, s.tmpDir, []string{mc.blobSHA, mc.treeSHA, mc.commitSHA})
	if err != nil {
		return fmt.Errorf("pack mid-race objects: %w", err)
	}
	s.packSeq++
	packID := fmt.Sprintf("%06d", s.packSeq)
	key := blob.PackKey(repo.key, packID)
	if err := s.cl.oracleBl.UploadFile(ctx, key, packPath); err != nil {
		return fmt.Errorf("upload mid-race pack: %w", err)
	}
	if err := s.cl.oracle.RegisterPacks(ctx, repo.key, []ref.Pack{{ID: packID, Key: key}}); err != nil {
		return fmt.Errorf("register mid-race pack: %w", err)
	}
	won, err := s.cl.oracle.CASRef(ctx, repo.key, branch, old, mc.commitSHA)
	if err != nil {
		return fmt.Errorf("mid-race CAS: %w", err)
	}
	if !won {
		return fmt.Errorf("mid-race CAS lost under the serialized scheduler (old=%s)", old)
	}
	s.observed[pushTransition{Repo: repo.key, Ref: branch, Before: old, After: mc.commitSHA}] = true
	return nil
}

// checkPackRows is the deferred "pack rows only grow, modulo repack"
// oracle, run after every step: outside a repack (whose exact transition
// the repack ops pin and bake into prevPacks themselves), a registered row
// must never disappear or change its key.
func (s *sim) checkPackRows() error {
	for _, r := range s.repos {
		current, err := s.oraclePackSet(r.key)
		if err != nil {
			return err
		}
		for id, key := range s.prevPacks[r.key] {
			got, ok := current[id]
			if !ok {
				s.violate("step %d: pack rows only grow: %s row %s disappeared outside a repack", s.step, r.key, id)
				return nil
			}
			if got != key {
				s.violate("step %d: pack rows only grow: %s row %s changed key %s -> %s", s.step, r.key, id, key, got)
				return nil
			}
		}
		s.prevPacks[r.key] = current
	}
	return nil
}
