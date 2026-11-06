package meta

import (
	"errors"
	"log/slog"

	"github.com/coder/openstorage/internal/metrics"
)

// The meta.json refusal model (issue #516). Every refusal of a fetched or
// adopted document goes through refuse, and every end of a latch goes
// through heal, so one rule per class holds everywhere. Only refuse, heal
// and adoptOrgKeys write the refusal state (rolledBack, quarantined,
// keysRefused, orgKeys); TestS3WALMetaRefuseIsTheOnlyWriter enforces it.
//
// Two tiers:
//   - Before adoption (decode, stale, vanished): the process refused the
//     document before installing anything, so its cache and key snapshot
//     still hold the last document it trusted. It latches: it keeps that
//     state, refuses every other document, and heals when the store serves
//     the cached bytes again or answers 304.
//   - After adoption (foreign nonce, vanished mark): the process found the
//     problem after it installed the edited body. It cannot tell which
//     earlier state to trust, so it quarantines: it drops the cache, the
//     shared view and the key snapshot, and every operation and token
//     check fails until a restart. Key lookups fail from memory, so
//     unauthenticated floods still cost no store reads.

// refuseClass is a refusal class: the metric label and the tier.
type refuseClass = metrics.MetaRefusal

const (
	refuseDecode       = metrics.MetaRefusedDecode       // undecodable or wrong-version bytes replaced the cached object
	refuseStale        = metrics.MetaRefusedStale        // an older seq, or other bytes at the same seq
	refuseVanished     = metrics.MetaRefusedVanished     // the cached object vanished
	refuseForeignNonce = metrics.MetaRefusedForeignNonce // our writer nonce at a foreign seq after an ambiguous write
	refuseVanishedMark = metrics.MetaRefusedVanishedMark // a detaching mark vanished mid-detach
)

func quarantines(c refuseClass) bool { return c == refuseForeignNonce || c == refuseVanishedMark }

// QuarantinedError answers every metadata operation and key lookup of a
// process that quarantined its view of meta.json (issue #516). It wraps
// the refusal that caused it.
type QuarantinedError struct {
	Err error
}

// cause names why the process stopped trusting meta.json, without the
// restore hint that Error and the log line each carry on their own.
func (e *QuarantinedError) cause() string {
	// A corrupt document's own text carries a "keep serve stopped" hint that
	// would contradict the restore hint (#525).
	if cd, ok := errors.AsType[*CorruptDocumentError](e.Err); ok {
		return s3walMetaKey + " was edited out of band: " + cd.Err.Error()
	}
	return e.Err.Error()
}

func (e *QuarantinedError) Error() string {
	return "s3walmeta: this process stopped trusting " + s3walMetaKey + " (" + e.cause() + "). Restore the bucket from a verified stopped-node backup first, then restart serve: a restart adopts whatever the bucket holds (docs/how-to/backup-restore-s3wal.md#restore-and-validate-before-cutover)"
}

func (e *QuarantinedError) Unwrap() error { return e.Err }

// refuse applies class's rule and returns the error the caller must
// return. Caller holds mu. Entering either state logs once and counts
// once; repeated refusals in the same state do neither.
func (s *S3WAL) refuse(class refuseClass, err error) error {
	// Every read returns a quarantine before it can refuse again.
	assertf(err != nil && s.quarantined == nil, "refuse(%v, %v): nil error or already quarantined", class, err)
	if quarantines(class) {
		q := &QuarantinedError{Err: err}
		s.quarantined = q
		s.cacheBody, s.cacheETag, s.cacheView = nil, "", nil
		s.orgKeysMu.Lock()
		s.orgKeys, s.keysRefused = nil, q
		s.orgKeysMu.Unlock()
		metrics.S3WALMetaRefused(class)
		slog.Error("s3walmeta: quarantined _wal/meta.json until restart; every metadata operation and token check fails",
			"class", class.String(), "err", q.cause(), "restore_first", "docs/how-to/backup-restore-s3wal.md#restore-and-validate-before-cutover")
		return q
	}
	if !s.rolledBack {
		s.rolledBack = true
		metrics.S3WALMetaRefused(class)
		slog.Error("s3walmeta: refusing _wal/meta.json until the store serves the cached document again; keeping the last trusted state",
			"class", class.String(), "seen_seq", s.lastSeq, "err", err)
	}
	return err
}

// heal ends a latch: the store serves the cached document again (its
// bytes, or a 304 on its ETag). A quarantine never heals: no read reaches
// heal under it. Caller holds mu.
func (s *S3WAL) heal() {
	if s.rolledBack {
		s.rolledBack = false
		slog.Info("s3walmeta: the store serves the cached _wal/meta.json again; refusal ended", "seq", s.lastSeq)
	}
}
