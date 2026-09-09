// Quota-ledger scenario ops and oracle (issues #29/#31): leaked admission
// reservations (reserve → crash) with sweep-side reclamation, and a
// per-step consistency check tying the maintained usage counter to ground
// truth — registered pack blobs plus recorded retained bytes.
package simtest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/ref"
)

// simQuotaBytes is the generous per-repo quota the reserve op admits
// against: admission itself is exercised (including fault injection on the
// reserve statement), while the sim's pushes stay unthrottled.
const simQuotaBytes = int64(1) << 40

// simReservationTTL keeps every reservation "active" for the admission
// predicate for the whole run; only the explicit sweep op reclaims them
// (olderThan=0 — the deterministic recovery form).
const simReservationTTL = time.Hour

// packFileSize returns the pack file's byte size for the registration
// ledger. The file was just written by pack-objects; a vanished file is a
// harness bug.
func packFileSize(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		panic(fmt.Sprintf("simtest: stat pack file %s: %v", path, err))
	}
	return fi.Size()
}

// doReserveCrash mints a pack exactly like a push would and commits ONLY
// its admission reservation, then abandons it — the issue-#29 crash window
// between reserve and upload. The leaked row must gate nothing in the sim
// (quota is generous) and must be reclaimable by doReserveSweep. The
// reserve call runs through the fault-injected store, so clean and
// ambiguous reservation failures are exercised too.
func (s *sim) doReserveCrash(name string, node *simNode, repo *simRepo) error {
	ctx := context.Background()
	content := fmt.Sprintf("%s reserve step %d", repo.key, s.step)
	mc, err := mintCommit(repo.scratch, "", content, s.step)
	if err != nil {
		return fmt.Errorf("step %d: mint commit: %w", s.step, err)
	}
	packPath, err := packObjects(repo.scratch, s.tmpDir, []string{mc.blobSHA, mc.treeSHA, mc.commitSHA})
	if err != nil {
		return fmt.Errorf("step %d: pack objects: %w", s.step, err)
	}
	defer os.Remove(packPath)
	s.packSeq++
	packID := fmt.Sprintf("%06d", s.packSeq)
	pack := ref.Pack{ID: packID, Key: blob.PackKey(repo.key, packID), Size: packFileSize(packPath)}

	mark := s.inj.Seq()
	dec, rerr := node.simRef.ReservePackBytes(ctx, blob.QuotaRepoGroup(repo.key),
		[]ref.Pack{pack}, ref.NewReservationOwner(), 0, ref.ReserveNoFloorGen, simQuotaBytes, simReservationTTL)
	evs, _ := s.inj.FiredSince(mark)
	outcome := "crashed-after-reserve"
	switch {
	case rerr != nil:
		outcome = "reserve-" + errToken(rerr)
	case !dec.OK:
		// A generous quota can only deny if the ledger overcounts —
		// leaked reservations are bounded by run length, so this is a
		// soundness violation, not scheduling noise.
		s.violate("step %d: reservation denied under generous quota: %+v", s.step, dec)
		outcome = "reserve-denied"
	}
	s.tracef("step=%04d op=%s node=%d repo=%s pack=%s out=%s faults=[%s]",
		s.step, name, node.id, repo.key, packID, outcome, faultToken(evs))
	return nil
}

// doReserveSweep runs the sweep-side reclamation on the clean oracle store
// (olderThan=0 prunes every reservation, exactly the recovery the periodic
// sweeper performs after TTL expiry) and asserts the reservation ledger
// drains to zero.
func (s *sim) doReserveSweep(name string, node *simNode) error {
	ctx := context.Background()
	// Through the NODE's stores, not the oracle: on the s3wal lane the
	// sweep's fleet listing and per-base CAS then run over the faulted
	// SimCAS transport — the only scheduled consumer of cas.List (Codex
	// round 2 on PR #159). A fault-classed failure is a legal outcome
	// (rows survive to a later sweep), so the zero-reserved check
	// applies only to a completed sweep.
	mark := s.inj.Seq()
	n, err := node.simRef.SweepExpiredPackReservations(ctx, 0)
	evs, _ := s.inj.FiredSince(mark)
	if err != nil {
		if !errors.Is(err, ErrInjectedClean) && !errors.Is(err, ErrInjectedAmbiguous) {
			return fmt.Errorf("step %d: sweep reservations: %w", s.step, err)
		}
		s.tracef("step=%04d op=%s node=%d out=%s faults=[%s]", s.step, name, node.id, errToken(err), faultToken(evs))
		return nil
	}
	for _, repo := range s.repos {
		st, err := s.cl.oracle.QuotaLedgerState(ctx, blob.QuotaRepoGroup(repo.key), simReservationTTL)
		if err != nil {
			return fmt.Errorf("step %d: ledger state %s: %w", s.step, repo.key, err)
		}
		if st.ReservedBytes != 0 {
			s.violate("step %d: %s holds %d reserved bytes after a full reservation sweep",
				s.step, repo.key, st.ReservedBytes)
		}
	}
	s.tracef("step=%04d op=%s node=%d swept=%d faults=[%s]", s.step, name, node.id, n, faultToken(evs))
	return nil
}

// checkQuotaLedger is the retained-byte soundness oracle (issue #31), run
// after every step: for each repo, the maintained usage counter must equal
// the ground-truth bytes of its REGISTERED pack blobs (registered blobs
// always exist — invariant (c)) plus its recorded retained bytes. Every
// ledger writer preserves this equation transactionally: registration
// credits new non-retained rows, folds move input bytes from "registered"
// to "retained" while crediting the consolidated pack, re-registrations
// consume retained rows without re-crediting, and aborted/faulted writes
// change nothing. A divergence is double-counting or byte leakage.
func (s *sim) checkQuotaLedger() error {
	ctx := context.Background()
	for _, repo := range s.repos {
		packs, err := s.cl.oracle.ListPacks(ctx, repo.key)
		if err != nil {
			return fmt.Errorf("step %d: list packs %s: %w", s.step, repo.key, err)
		}
		var registered int64
		for _, p := range packs {
			size, err := s.cl.oracleBl.Stat(ctx, p.Key)
			if err != nil {
				return fmt.Errorf("step %d: stat registered %s: %w", s.step, p.Key, err)
			}
			registered += size
		}
		st, err := s.cl.oracle.QuotaLedgerState(ctx, blob.QuotaRepoGroup(repo.key), simReservationTTL)
		if err != nil {
			return fmt.Errorf("step %d: ledger state %s: %w", s.step, repo.key, err)
		}
		if st.UsageBytes != registered+st.RetainedBytes {
			s.violate("step %d: %s usage counter %d != registered %d + retained %d",
				s.step, repo.key, st.UsageBytes, registered, st.RetainedBytes)
		}
	}
	return nil
}
