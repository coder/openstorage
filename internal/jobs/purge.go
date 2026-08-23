// purge.go implements repository reclamation for soft-deleted repositories
// (issue #52): soft-delete burns the name and keeps every registry row and
// blob (docs/explanation/domain-model.md), so without a follow-up story a
// deleted repository's storage lives forever. The RepoPurger permanently
// reclaims it — on the sweep cadence once CODESTORAGE_REPO_RETENTION
// expires, or synchronously via `codestorage purge-repo`.
//
// The purge is TOMBSTONE-THEN-DELETE, the inverse of publish-before-CAS:
// registry rows are dropped (packs rows becoming pack_retained tombstones)
// strictly BEFORE their blobs are deleted, and ref rows die first of all,
// so no crash point can leave a committed ref pointing at deleted bytes —
// only reapable tombstones and orphan-class blobs. The soft-deleted repos
// row is the durable marker that drives resumption: every phase is
// idempotent, a crash anywhere resumes on the next pass, and stamping the
// row's purged_at tombstone is the completion commit. The row itself is
// kept forever — the burned (org, url) name is never released, because
// URL-scoped JWTs can outlive any retention window (see meta/purge.go) —
// and restore after purge is impossible.
//
// Blob disposal reuses the sweeper's quarantine handshake verbatim
// (safeDelete: quarantine-copy → delete → registry recheck, GuardedLFSDelete
// for lfs/ keys), so purge deletions inherit every crash/race repair layer,
// and the aged quarantine purge collects the backups afterwards.
package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/coder/code-storage/internal/blob"
	"github.com/coder/code-storage/internal/meta"
	"github.com/coder/code-storage/internal/ref"
)

// RepoPurgeRegistry is the slice of the concrete ref stores the purge
// needs beyond PackRegistry: row tombstoning and final ledger cleanup.
// Deliberately NOT part of ref.RefStore — that contract has many
// test/simulation mirrors that must not grow purge-only surface — so the
// wiring type-asserts the concrete backend (see the compile-time
// assertions below).
type RepoPurgeRegistry interface {
	// FenceRepoPurge durably write-fences the base repo (phase 0, before
	// any tombstoning): pack/LFS registrations for the group fail with
	// ref.ErrRepoPurged from then on, so the later verification and
	// completion cannot be raced by an in-flight pre-delete writer.
	// Permanent, like the name burn; idempotent.
	FenceRepoPurge(ctx context.Context, base string) error
	// IsRepoPurged reports whether the base repo is purge-fenced — the
	// sweeper's straggler-blob probe (SweepConfig.PurgedRepoProbe).
	IsRepoPurged(ctx context.Context, base string) (bool, error)
	// DropRepoRefs deletes the group's ref rows (purge phase 1 — refs die
	// first, so no later phase can strand a committed ref).
	DropRepoRefs(ctx context.Context, group []string) (int64, error)
	// RetireRepoPacks tombstones the group's packs rows into pack_retained
	// (phase 2): a crash after this leaves the blobs in the sweeper's
	// provably-DB-absent retained class (reaped under DeleteSuperseded,
	// on by default) even if retention is later disabled.
	RetireRepoPacks(ctx context.Context, group []string) (int64, error)
	// DropRepoLFSObjects deletes the group's lfs_objects/count rows
	// (phase 3).
	DropRepoLFSObjects(ctx context.Context, group []string) (int64, error)
	// CountRepoRows counts the group's refs+packs+lfs_objects rows — the
	// pre-completion verification read (phase 5).
	CountRepoRows(ctx context.Context, group []string) (int64, error)
	// PurgeRepoLedger removes the group's remaining accounting/state rows
	// (phase 6).
	PurgeRepoLedger(ctx context.Context, group []string) (int64, error)
}

// RepoPurgeMeta is the slice of the concrete meta stores the purge needs:
// eligibility reads and the final metadata deletion. Same concrete-backend
// discipline as RepoPurgeRegistry.
type RepoPurgeMeta interface {
	// PurgeableRepos lists up to limit repo IDs soft-deleted at or before
	// deletedBefore, oldest first.
	PurgeableRepos(ctx context.Context, deletedBefore time.Time, limit int) ([]string, error)
	// RepoPurgeState returns the repo row plus its purge tombstone
	// (found=false when absent; Repo.DeletedAt==nil when live; PurgedAt
	// non-nil once a purge completed).
	RepoPurgeState(ctx context.Context, repoID string) (state meta.PurgeState, found bool, err error)
	// PurgeRepoMeta deletes credentials and the upstream row and stamps
	// the repos row's purged_at tombstone in one guarded transaction
	// (phase 7, the completion commit; the row is kept as the permanent
	// name burn).
	PurgeRepoMeta(ctx context.Context, repoID string) error
}

// Both concrete backends implement the purge seams.
var (
	_ RepoPurgeRegistry = (*ref.PG)(nil)
	_ RepoPurgeRegistry = (*ref.SQLite)(nil)
	_ RepoPurgeMeta     = (*meta.PG)(nil)
	_ RepoPurgeMeta     = (*meta.SQLite)(nil)
)

// MinRepoPurgeAge is the minimum time a repository must have been
// soft-deleted before any purge path (background or CLI) will touch it: a
// push or import that resolved the repo just before its soft-delete can
// still be in flight, and its post-tombstone registrations would make the
// purge churn (spare, defer, retry). One hour comfortably exceeds bounded
// request lifetimes; the spare-and-defer verification (see purgeRepo)
// remains the correctness backstop for anything longer.
const MinRepoPurgeAge = time.Hour

// purgeBatchLimit bounds how many repositories one PurgeExpired pass
// processes; the eligible set drains across passes. A var so tests can
// exercise batching cheaply.
var purgeBatchLimit = 100

// Purge outcome errors, matched by the CLI and tests via errors.Is.
var (
	// ErrRepoRecentlyDeleted rejects purging a repo soft-deleted less than
	// MinRepoPurgeAge ago.
	ErrRepoRecentlyDeleted = errors.New("repository was soft-deleted too recently to purge")
	// ErrPurgeDeferred reports that registry rows reappeared after
	// tombstoning (an in-flight pre-delete writer): the run made progress
	// but left the repos row unstamped so the next pass re-tombstones and
	// retries.
	ErrPurgeDeferred = errors.New("purge deferred: registry rows were re-registered mid-purge")
)

// RepoPurgeReport summarizes one repository's purge.
type RepoPurgeReport struct {
	RepoID       string
	RefsDropped  int64 // ref rows deleted (phase 1)
	PacksRetired int64 // packs rows tombstoned into pack_retained (phase 2)
	LFSDropped   int64 // lfs_objects rows deleted (phase 3)
	BlobsReaped  int   // blobs quarantined-then-deleted (phase 4)
	BytesReaped  int64 // their sizes (bytes move to quarantine, freed by the aged purge)
	BlobsSpared  int   // still-registered blobs spared (in-flight writer; forces deferral)
	RowsPurged   int64 // ledger/state rows removed (phase 6)
}

// PurgeRunReport summarizes one PurgeExpired pass.
type PurgeRunReport struct {
	Eligible int // repos past retention this pass picked up
	Purged   int // completed purges (repos row deleted)
	Failed   int // deferred or errored repos (retried next pass)
}

// RepoPurger reclaims soft-deleted repositories. Construct with
// NewRepoPurger; PurgeExpired runs on the sweep cadence, PurgeRepo backs
// the operator CLI. Both funnel through the same purge primitive.
type RepoPurger struct {
	sweeper  *Sweeper
	registry RepoPurgeRegistry
	meta     RepoPurgeMeta
	// retention is how long after soft-deletion PurgeExpired reclaims a
	// repo; zero means the background pass is disabled (PurgeRepo still
	// works — the operator command is its own consent).
	retention time.Duration
	logger    *slog.Logger
	now       func() time.Time // injectable clock for tests
}

// NewRepoPurger builds a RepoPurger over the sweeper (blob disposal
// machinery) and the concrete stores' purge seams.
func NewRepoPurger(sweeper *Sweeper, registry RepoPurgeRegistry, metaStore RepoPurgeMeta, retention time.Duration, logger *slog.Logger) *RepoPurger {
	assertf(sweeper != nil, "NewRepoPurger: nil sweeper")
	assertf(registry != nil, "NewRepoPurger: nil purge registry")
	assertf(metaStore != nil, "NewRepoPurger: nil purge meta store")
	assertf(retention >= 0, "NewRepoPurger: negative retention %v", retention)
	assertf(logger != nil, "NewRepoPurger: nil logger")
	return &RepoPurger{
		sweeper:   sweeper,
		registry:  registry,
		meta:      metaStore,
		retention: retention,
		logger:    logger,
		now:       time.Now,
	}
}

// PurgeExpired reclaims one batch of repositories soft-deleted longer than
// the retention. Per-repo failures defer that repo to the next pass without
// stopping the batch; they are joined into the returned error so the job
// backend records them.
func (p *RepoPurger) PurgeExpired(ctx context.Context) (PurgeRunReport, error) {
	assertf(p.retention > 0, "PurgeExpired: retention disabled")
	var report PurgeRunReport
	// Fail closed before any destructive step, mirroring Sweep: the
	// bucket-versioning precondition can drift while the process runs.
	if guard := p.sweeper.cfg.DeleteGuard; guard != nil {
		if err := guard(ctx); err != nil {
			return report, fmt.Errorf("repo purge: delete guard: %w", err)
		}
	}
	cutoff := p.now().Add(-max(p.retention, MinRepoPurgeAge))
	ids, err := p.meta.PurgeableRepos(ctx, cutoff, purgeBatchLimit)
	if err != nil {
		return report, fmt.Errorf("repo purge: list purgeable repos: %w", err)
	}
	report.Eligible = len(ids)
	var errs boundedErrs
	for _, id := range ids {
		if ctx.Err() != nil {
			errs.add(ctx.Err())
			break
		}
		rep, err := p.purgeRepo(ctx, id)
		if err != nil {
			report.Failed++
			errs.add(fmt.Errorf("repo purge %s: %w", id, err))
			continue
		}
		report.Purged++
		p.logger.Info("repo purge: reclaimed soft-deleted repository",
			"repo", id, "refs_dropped", rep.RefsDropped,
			"packs_retired", rep.PacksRetired, "lfs_dropped", rep.LFSDropped,
			"blobs_reaped", rep.BlobsReaped, "bytes_reaped", rep.BytesReaped,
			"rows_purged", rep.RowsPurged)
	}
	if report.Eligible > 0 {
		p.logger.Info("repo purge pass finished",
			"eligible", report.Eligible, "purged", report.Purged,
			"failed", report.Failed, "retention", p.retention)
	}
	return report, errs.join()
}

// PurgeRepo purges one soft-deleted repository synchronously (the
// `codestorage purge-repo` primitive). The repo must be a BASE repo ID and
// must have been soft-deleted at least MinRepoPurgeAge ago; the retention
// window does NOT apply — invoking the command is the operator's consent.
func (p *RepoPurger) PurgeRepo(ctx context.Context, repoID string) (RepoPurgeReport, error) {
	if guard := p.sweeper.cfg.DeleteGuard; guard != nil {
		if err := guard(ctx); err != nil {
			return RepoPurgeReport{RepoID: repoID}, fmt.Errorf("repo purge: delete guard: %w", err)
		}
	}
	return p.purgeRepo(ctx, repoID)
}

// purgeRepo runs the purge phases for one repository. Callers checked the
// DeleteGuard. Every phase is idempotent; any error leaves the soft-deleted
// repos row in place, so the next pass (or CLI re-run) resumes.
func (p *RepoPurger) purgeRepo(ctx context.Context, repoID string) (RepoPurgeReport, error) {
	rep := RepoPurgeReport{RepoID: repoID}
	if repoID == "" || strings.ContainsAny(repoID, "+/") {
		// Namespace keys (<base>+ephemeral/<base>+import) purge with their
		// base; slashes would corrupt the blob-prefix walk below.
		return rep, fmt.Errorf("invalid repo ID %q: purge takes the base repository ID", repoID)
	}
	state, found, err := p.meta.RepoPurgeState(ctx, repoID)
	if err != nil {
		return rep, fmt.Errorf("read repo state: %w", err)
	}
	if !found {
		return rep, fmt.Errorf("repo %s: %w", repoID, meta.ErrNotFound)
	}
	if state.PurgedAt != nil {
		// Completed purge: the row phases are done and the fence rejects
		// every new registration, but a pre-fence writer may have
		// PUBLISHED a blob after the completed pass's prefix scan
		// (uploads precede registration and cannot be fenced). Re-running
		// is a straggler rescan: reap the prefixes again and leave the
		// stamped tombstone alone. The background analogue is the
		// sweeper's PurgedRepoProbe classification.
		return rep, p.rescanPurged(ctx, repoID, &rep)
	}
	if state.Repo.DeletedAt == nil {
		return rep, fmt.Errorf("repo %s is live — delete it first: %w", repoID, meta.ErrRepoNotDeleted)
	}
	if age := p.now().Sub(*state.Repo.DeletedAt); age < MinRepoPurgeAge {
		return rep, fmt.Errorf("repo %s was soft-deleted %v ago (minimum %v — an in-flight pre-delete push could still be writing): %w",
			repoID, age.Round(time.Second), MinRepoPurgeAge, ErrRepoRecentlyDeleted)
	}

	group := blob.QuotaRepoGroup(repoID)
	// Phase 0: durable write fence, BEFORE any tombstoning. Pack/LFS
	// registrations serialize against it in the ref store, so every
	// straggler write either committed before this fence (its rows are
	// tombstoned by phases 1–3 / the next resume) or fails with
	// ref.ErrRepoPurged — the phase-5 verification below can then never
	// be invalidated by a write settling after it (PR #105 security
	// round). Ref-only CAS writes are not fenced: pushes register packs
	// before any ref CAS, so a fenced push dies before touching refs, and
	// a hypothetical straggler ref row is unreachable metadata (the repo
	// no longer resolves) that carries no bytes.
	if err := p.registry.FenceRepoPurge(ctx, repoID); err != nil {
		return rep, fmt.Errorf("fence repo purge: %w", err)
	}
	// Phases 1–3: tombstone registry rows strictly before touching blobs.
	if rep.RefsDropped, err = p.registry.DropRepoRefs(ctx, group); err != nil {
		return rep, fmt.Errorf("drop refs: %w", err)
	}
	if rep.PacksRetired, err = p.registry.RetireRepoPacks(ctx, group); err != nil {
		return rep, fmt.Errorf("retire packs: %w", err)
	}
	if rep.LFSDropped, err = p.registry.DropRepoLFSObjects(ctx, group); err != nil {
		return rep, fmt.Errorf("drop lfs objects: %w", err)
	}
	// Phase 4: reap the repo's blob prefixes — the same trio
	// blob.StorageUsageBytes measures (base packs, namespace packs, LFS).
	for _, prefix := range []string{
		blob.PackPrefix(repoID),
		blob.PacksRoot + repoID + "+",
		blob.LFSPrefix(repoID),
	} {
		if err := p.reapPrefix(ctx, prefix, &rep); err != nil {
			return rep, err
		}
	}
	// Phase 5: verification — a writer that resolved the repo before its
	// soft-delete may have registered rows after the tombstoning phases.
	// Defer rather than delete the repo's meta row over live registry
	// rows; the next pass re-tombstones and retries.
	rows, err := p.registry.CountRepoRows(ctx, group)
	if err != nil {
		return rep, fmt.Errorf("verify registry rows: %w", err)
	}
	if rows > 0 || rep.BlobsSpared > 0 {
		return rep, fmt.Errorf("repo %s: %d registry rows / %d spared blobs after tombstoning: %w",
			repoID, rows, rep.BlobsSpared, ErrPurgeDeferred)
	}
	// Phase 6: accounting/state cleanup (the whole unit dies — no per-row
	// debits; see RepoPurgeRegistry.PurgeRepoLedger).
	if rep.RowsPurged, err = p.registry.PurgeRepoLedger(ctx, group); err != nil {
		return rep, fmt.Errorf("purge ledger rows: %w", err)
	}
	// Phase 7: completion commit — deletes credentials/upstream and stamps
	// the repos row's purged_at tombstone, guarded on deleted_at. The row
	// stays: the burned name is never released (see meta/purge.go).
	if err := p.meta.PurgeRepoMeta(ctx, repoID); err != nil {
		return rep, err
	}
	return rep, nil
}

// rescanPurged reaps straggler blobs of an already-completed purge — the
// same prefix walk as phase 4, with the row phases (provably done, fenced)
// skipped.
func (p *RepoPurger) rescanPurged(ctx context.Context, repoID string, rep *RepoPurgeReport) error {
	for _, prefix := range []string{
		blob.PackPrefix(repoID),
		blob.PacksRoot + repoID + "+",
		blob.LFSPrefix(repoID),
	} {
		if err := p.reapPrefix(ctx, prefix, rep); err != nil {
			return err
		}
	}
	return nil
}

// reapPrefix disposes of every blob under prefix through the sweeper's
// quarantine handshake. Registered keys are spared without churn (the
// verification phase then defers the purge); per-key failures are joined so
// one bad object does not strand the rest.
func (p *RepoPurger) reapPrefix(ctx context.Context, prefix string, rep *RepoPurgeReport) error {
	var errs boundedErrs
	err := p.sweeper.blobs.ListPages(ctx, prefix, sweepListPageSize, func(page []blob.ListedObject) error {
		for _, obj := range page {
			if err := ctx.Err(); err != nil {
				return err
			}
			repoKey, ok := splitSweepKey(obj.Key)
			if !ok {
				p.logger.Warn("repo purge: skipping foreign key", "key", obj.Key)
				continue
			}
			registered, err := p.sweeper.registered(ctx, repoKey, obj.Key)
			if err != nil {
				errs.add(fmt.Errorf("classify %s: %w", obj.Key, err))
				continue
			}
			if registered {
				// An in-flight pre-delete writer re-registered this key
				// after the tombstoning phases. Its rows win this round.
				rep.BlobsSpared++
				p.logger.Warn("repo purge: sparing re-registered blob", "key", obj.Key)
				continue
			}
			reaped, err := p.sweeper.safeDelete(ctx, repoKey, obj.Key)
			if err != nil {
				errs.add(err)
				continue
			}
			if !reaped {
				continue // gone already, or restored for a racing registration
			}
			rep.BlobsReaped++
			rep.BytesReaped += obj.Size
			if !strings.HasPrefix(obj.Key, blob.LFSRoot) {
				// Consume the pack's retained tombstone now that the bytes
				// left the pack prefix: debits real repack-retained bytes;
				// purge's own zero-byte tombstones debit nothing (the usage
				// row dies wholesale in phase 6). Idempotent.
				if _, err := p.sweeper.packs.ReclaimRetainedPack(ctx, repoKey, obj.Key); err != nil {
					errs.add(fmt.Errorf("reclaim retained %s: %w", obj.Key, err))
				}
			}
		}
		return nil
	})
	if err != nil {
		errs.add(fmt.Errorf("repo purge: list %s: %w", prefix, err))
	}
	return errs.join()
}
