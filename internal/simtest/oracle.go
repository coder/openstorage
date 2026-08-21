package simtest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"codestorage/internal/githttp"
)

// countMissingPacks reports how many registered packs the node has not yet
// hydrated (no .idx in its cache repo), via the clean oracle handles.
func (s *sim) countMissingPacks(node *simNode, repo *simRepo) (int, error) {
	packs, err := s.cl.oracle.ListPacks(context.Background(), repo.key)
	if err != nil {
		return 0, fmt.Errorf("oracle ListPacks %s: %w", repo.key, err)
	}
	packDir := filepath.Join(githttp.RepoPath(node.cacheDir, repo.key), "objects", "pack")
	missing := 0
	for _, p := range packs {
		if _, err := os.Stat(filepath.Join(packDir, "pack-"+p.ID+".idx")); err != nil {
			missing++
		}
	}
	return missing, nil
}

// doHydrate runs the production hydration path on a node through its
// fault-injected stores. withNotFound arms a targeted transient
// FaultNotFound on the next pack download (downgraded to a plain hydrate
// when the node has nothing to download), which issue #6's bounded
// re-list retry must absorb: a hydrate that fails without any transport
// fault in its window is an invariant violation.
func (s *sim) doHydrate(name string, node *simNode, repo *simRepo, withNotFound bool) error {
	ctx := context.Background()
	if withNotFound {
		missing, err := s.countMissingPacks(node, repo)
		if err != nil {
			return err
		}
		if missing == 0 {
			name = "hydrate" // downgraded: nothing to download
		} else {
			s.inj.FailNext(OpBlobDownloadFile, FaultNotFound)
		}
	}

	mark := s.inj.Seq()
	repoPath, refsErr, err := githttp.Hydrate(ctx, node.simRef, node.simBlob, node.cacheDir, repo.key)
	evs, _ := s.inj.FiredSince(mark)

	outcome := "ok"
	if err != nil || refsErr != nil {
		outcome = "failed"
		transport := false
		for _, ev := range evs {
			if ev.Class == FaultClean || ev.Class == FaultAmbiguous {
				transport = true
				break
			}
		}
		if !transport {
			s.violate("step %d: hydrate %s on node %d failed without a transport fault (transient not-found must be absorbed, issue #6 class): err=%v refsErr=%v",
				s.step, repo.key, node.id, err, refsErr)
		}
	} else {
		// Invariant (b), warm form: the hydrated cache repo must mirror the
		// ref authority exactly.
		if err := s.checkRefsMatch(repoPath, repo.key, fmt.Sprintf("node %d hydrate", node.id)); err != nil {
			return err
		}
	}
	s.tracef("step=%04d op=%s node=%d repo=%s out=%s faults=[%s]",
		s.step, name, node.id, repo.key, outcome, faultToken(evs))
	return nil
}

// checkRefsMatch compares a hydrated repo's refs against the authority.
func (s *sim) checkRefsMatch(repoPath, repoKey, who string) error {
	have, err := forEachRef(repoPath)
	if err != nil {
		return fmt.Errorf("for-each-ref: %w", err)
	}
	want, err := s.cl.oracle.ListRefs(context.Background(), repoKey)
	if err != nil {
		return fmt.Errorf("oracle ListRefs %s: %w", repoKey, err)
	}
	if len(have) != len(want) {
		s.violate("step %d: %s: %s has %d refs, authority has %d (%s)",
			s.step, who, repoKey, len(have), len(want), refsDiff(have, want))
		return nil
	}
	for name, sha := range want {
		if have[name] != sha {
			s.violate("step %d: %s: %s ref %s = %s locally, authority says %s",
				s.step, who, repoKey, name, have[name], sha)
			return nil
		}
	}
	return nil
}

// refsDiff renders a deterministic (sorted) summary of two ref maps.
func refsDiff(have, want map[string]string) string {
	names := make(map[string]bool)
	for n := range have {
		names[n] = true
	}
	for n := range want {
		names[n] = true
	}
	sorted := make([]string, 0, len(names))
	for n := range names {
		sorted = append(sorted, n)
	}
	sort.Strings(sorted)
	out := ""
	for _, n := range sorted {
		if have[n] != want[n] {
			out += fmt.Sprintf(" %s: have=%.8s want=%.8s", n, have[n], want[n])
		}
	}
	return "diff:" + out
}

// checkPackBlobs enforces invariant (c) after every step: no packs row may
// reference a blob that is not durably present.
func (s *sim) checkPackBlobs() error {
	ctx := context.Background()
	for _, r := range s.repos {
		packs, err := s.cl.oracle.ListPacks(ctx, r.key)
		if err != nil {
			return fmt.Errorf("oracle ListPacks %s: %w", r.key, err)
		}
		for _, p := range packs {
			if _, err := s.cl.oracleBl.Stat(ctx, p.Key); err != nil {
				s.violate("step %d: invariant (c): packs row %s/%s references missing blob %s: %v",
					s.step, r.key, p.ID, p.Key, err)
				return nil
			}
		}
	}
	return nil
}

// checkFull is the expensive oracle: for every repo, a brand-new node
// (fresh cache dir, clean un-faulted stores) hydrates from scratch; the
// result must (a) succeed with every ref's object closure fully present
// and (b) mirror the ref authority exactly. Invariant (c) is re-checked at
// blob level first so a missing blob is reported as (c), not as a hydrate
// failure.
func (s *sim) checkFull() error {
	ctx := context.Background()
	if err := s.checkPackBlobs(); err != nil || s.stopped {
		return err
	}
	coldDir := filepath.Join(s.dir, fmt.Sprintf("oracle-cold-%04d", s.step))
	defer os.RemoveAll(coldDir)
	for _, r := range s.repos {
		repoPath, refsErr, err := githttp.Hydrate(ctx, s.cl.oracle, s.cl.oracleBl, coldDir, r.key)
		if err != nil || refsErr != nil {
			s.violate("step %d: invariant (a): cold hydrate of %s failed on clean stores: err=%v refsErr=%v",
				s.step, r.key, err, refsErr)
			return nil
		}
		if err := s.checkRefsMatch(repoPath, r.key, "cold node"); err != nil {
			return err
		}
		if s.stopped {
			return nil
		}
		// Connectivity: every object reachable from any ref must exist.
		if out, err := simGit(repoPath, nil, nil, "fsck", "--connectivity-only", "--no-progress", "--no-dangling"); err != nil {
			s.violate("step %d: invariant (a): fsck of cold-hydrated %s failed: %v (%s)",
				s.step, r.key, err, out)
			return nil
		}
	}
	s.tracef("step=%04d op=full-oracle out=ok", s.step)
	return nil
}

// checkDeliveries runs the end-of-run webhook oracles:
//
//   - soundness: every recorded delivery corresponds to a transition that
//     actually committed (no fabricated push events);
//   - completeness: every CAS the writer saw won must have emitted at
//     least one delivery (recovered ambiguous wins included — the webhook
//     half of issue #2). Duplicates are fine: delivery is at-least-once.
func (s *sim) checkDeliveries() {
	sent := s.cl.rec.all()
	have := make(map[pushTransition]bool, len(sent))
	for _, d := range sent {
		have[d] = true
		if !s.observed[d] {
			s.violate("delivery soundness: push event %+v for a transition that never committed", d)
			return
		}
	}
	for _, w := range s.reportedWins {
		if !have[w] {
			s.violate("delivery completeness: reported win %+v emitted no push event (lost webhook, issue #2 class)", w)
			return
		}
	}
	s.tracef("end deliveries=%d wins=%d transitions=%d", len(sent), len(s.reportedWins), len(s.observed))
}
