package meta

import (
	"log/slog"

	"github.com/coder/openstorage/internal/metrics"
)

// The meta.json refusal model (issue #516). Every refusal of a fetched
// document goes through refuse, and every end of a latch goes through
// heal, so one rule per class holds everywhere. Only refuse and heal write
// rolledBack; TestS3WALMetaRefuseIsTheOnlyWriter enforces it.
//
// Before-adoption refusals (decode, stale, vanished): the process refused
// the document before installing anything, so its cache and key snapshot
// still hold the last document it trusted. It latches: it keeps that
// state, refuses every other document, and heals when the store serves the
// cached bytes again or answers 304.

// refuseClass is a refusal class: the metric label and the tier.
type refuseClass = metrics.MetaRefusal

const (
	refuseDecode   = metrics.MetaRefusedDecode   // undecodable or wrong-version bytes replaced the cached object
	refuseStale    = metrics.MetaRefusedStale    // an older seq, or other bytes at the same seq
	refuseVanished = metrics.MetaRefusedVanished // the cached object vanished
)

// refuse applies class's rule and returns the error the caller must
// return. Caller holds mu. Entering the latch logs once and counts once;
// repeated refusals while latched do neither.
func (s *S3WAL) refuse(class refuseClass, err error) error {
	assertf(err != nil, "refuse: nil error")
	if !s.rolledBack {
		s.rolledBack = true
		metrics.S3WALMetaRefused(class)
		slog.Error("s3walmeta: refusing _wal/meta.json until the store serves the cached document again; keeping the last trusted state",
			"class", class.String(), "seen_seq", s.lastSeq, "err", err)
	}
	return err
}

// heal ends a latch: the store serves the cached document again (its
// bytes, or a 304 on its ETag). Caller holds mu.
func (s *S3WAL) heal() {
	if s.rolledBack {
		s.rolledBack = false
		slog.Info("s3walmeta: the store serves the cached _wal/meta.json again; refusal ended", "seq", s.lastSeq)
	}
}
