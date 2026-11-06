package githttp

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

// ErrPushPublicationUnverified marks a WaitForPushPublication that could
// not prove every live push hold released: a hold or its tombstones were
// unreadable, or checking them would exceed the wait's resource bounds
// (issue #331). Publication must then be treated as still pending.
var ErrPushPublicationUnverified = errors.New("push publication wait: live push holds could not be checked; publication stays held")

// errPublicationScanLimit is the cause wrapped when a wait's byte, record,
// or record-length bound is reached.
var errPublicationScanLimit = errors.New("push hold scan limit reached")

// publicationLimits bounds one WaitForPushPublication. Per poll: bytes
// read from hold files and tombstone sidecars, and records examined
// (directory entries, hold lines, tombstone lines — blank, duplicate, and
// released lines included, because each costs work). Per wait: the same
// content bytes and hold/tombstone records summed over every poll, which
// bounds rereads when tombstone churn keeps invalidating the verdict
// cache. Directory entries count only per poll: an unchanged hold costs a
// listing and stats each poll, bounded by the caller's context instead.
type publicationLimits struct {
	passBytes, passRecords int64
	waitBytes, waitRecords int64
}

// defaultPublicationLimits: a poll may retain at most ~8 MiB / 131,072
// tombstones, enough for a ~65k-ref mirror push to finish releasing; a
// wait may reread 64 such polls. Larger holds fail closed and the caller
// retries after the push exits (holds are transient).
var defaultPublicationLimits = publicationLimits{
	passBytes:   8 << 20,
	passRecords: 1 << 17,
	waitBytes:   512 << 20,
	waitRecords: 1 << 23,
}

// publicationPollInterval matches the existing push-hold polling cadence
// (reservePushHolds).
const publicationPollInterval = 25 * time.Millisecond

// WaitForPushPublication blocks until no live push hold in repo's cache
// still names an unreleased ref: every accepted authority update that a
// hold protected has published its cache ref (or its push has exited).
// A caller that has already claimed its durable intent (issue #266) uses
// it to avoid acting on a cache ref an in-flight push has not yet
// published. Nothing on main calls it yet.
//
// Result contract:
//   - nil: no live, unreleased held name remained at the last poll.
//   - ctx.Err() (unwrapped) on cancellation or deadline.
//   - otherwise an error wrapping ErrPushPublicationUnverified: a hold
//     was unreadable or exceeded the resource bounds. Publication was NOT
//     proven. The caller must not start the protected work, and must keep
//     its durable intent, force flag, and retry scheduling exactly as for
//     any failed wait; a later retry re-checks from scratch. The case is
//     also logged here.
//
// Bounds (defaultPublicationLimits): holds are streamed, never collected
// or sorted. Each poll reads a hold's tombstone sidecar into a bounded
// set, then its lines until the first live name. Bytes are charged as
// they are read, so file growth after the listing cannot exceed the
// budget (at most one probe byte past it), and each record is charged
// before it is retained. A record longer than pushRefPathMax fails
// closed. Verdicts are cached per hold by (size, mtime) of the hold and
// its sidecar, so only changed holds are reread.
func WaitForPushPublication(ctx context.Context, cacheDir, repo string) error {
	assertf(ctx != nil && cacheDir != "" && repo != "", "WaitForPushPublication: missing ctx/cache/repo")
	return newPublicationWait(filepath.Join(RepoPath(cacheDir, repo), pushHoldDir), defaultPublicationLimits).wait(ctx)
}

// HoldRef takes a push hold on one ref of repo's cache for a write that
// changes the ref outside receive-pack: a REST write on the local schedule
// (ADR-50) holds the ref from before it records its forward intent until
// its authority CAS returns. While the hold is live, WaitForPushPublication
// blocks and a strict sync fetch refuses the ref, so a push run that
// claims the intent cannot snapshot the ref before the CAS. A conflicting
// hold is waited out as for a push (pushHoldWaitMax), then HoldRef fails.
// release stops the refresh and removes the hold; the TTL machinery covers
// a process that dies first.
func HoldRef(ctx context.Context, cacheDir, repo, refName string) (release func(), err error) {
	assertf(ctx != nil && cacheDir != "" && repo != "" && refName != "", "HoldRef: missing ctx/cache/repo/ref")
	repoPath, err := EnsureCacheRepoCtx(ctx, cacheDir, repo)
	if err != nil {
		return nil, err
	}
	holdPath, err := mintPushHold(repoPath)
	if err != nil {
		return nil, err
	}
	stop := startPushHoldRefresh(holdPath)
	release = func() {
		stop()
		removePushHoldFiles(holdPath)
	}
	if err := reservePushHolds(ctx, holdPath, []string{refName}); err != nil {
		release()
		return nil, err
	}
	return release, nil
}

// publicationWait is one WaitForPushPublication's state. Not
// goroutine-safe; one wait owns it.
type publicationWait struct {
	dir string
	lim publicationLimits

	verdicts map[string]holdVerdict // by hold path, from the last poll
	br       *bufio.Reader          // one fixed record buffer, reused

	passes, reads  int   // polls run; holds actually reread
	bytes, records int64 // content bytes and records charged over the wait

	beforeRead func(path string) // test seam: before opening a file to read
	afterPass  func(held bool)   // test seam: after each successful poll
}

// holdVerdict caches whether a hold had a live name, keyed by the stats
// it was read under. Holds and sidecars only grow by append (a changed
// size) or are rewritten through rename (a new mtime), so an unchanged
// key means unchanged content.
type holdVerdict struct {
	size, mtime, relSize, relMtime int64 // relSize -1: no sidecar
	held                           bool
}

func newPublicationWait(dir string, lim publicationLimits) *publicationWait {
	assertf(dir != "", "newPublicationWait: empty dir")
	assertf(lim.passBytes > 0 && lim.passRecords > 0 && lim.waitBytes >= lim.passBytes && lim.waitRecords >= lim.passRecords,
		"newPublicationWait: invalid limits %+v", lim)
	return &publicationWait{
		dir: dir,
		lim: lim,
		// One record plus its newline; longer records fail closed.
		br: bufio.NewReaderSize(nil, pushRefPathMax+1),
	}
}

func (w *publicationWait) wait(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		held, err := w.pass()
		if err != nil {
			log.Printf("push publication wait %s: %v (publication stays held; the caller retries)", w.dir, err)
			return fmt.Errorf("%w: %w", ErrPushPublicationUnverified, err)
		}
		if w.afterPass != nil {
			w.afterPass(held)
		}
		if !held {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(publicationPollInterval):
		}
	}
}

// scanBudget is one poll's remaining allowance.
type scanBudget struct {
	w              *publicationWait
	bytes, records int64
}

func (b *scanBudget) limitErr(what string, pass, wait int64) error {
	return fmt.Errorf("%w: more %s than allowed (%d per poll, %d per wait)", errPublicationScanLimit, what, pass, wait)
}

// entry charges one directory entry (per poll only).
func (b *scanBudget) entry() error {
	if b.records == 0 {
		return b.limitErr("records", b.w.lim.passRecords, b.w.lim.waitRecords)
	}
	b.records--
	return nil
}

// record charges one hold or tombstone line, before it is used.
func (b *scanBudget) record() error {
	if err := b.entry(); err != nil {
		return err
	}
	b.w.records++
	return nil
}

// budgetReader charges bytes as they are read and never pulls more than
// one probe byte past the budget: a file that grows after its stat, or
// one huge record, cannot be read beyond the limit.
type budgetReader struct {
	r io.Reader
	b *scanBudget
}

func (r *budgetReader) Read(p []byte) (int, error) {
	if int64(len(p)) > r.b.bytes+1 {
		p = p[:r.b.bytes+1]
	}
	n, err := r.r.Read(p)
	r.b.bytes -= int64(n)
	r.b.w.bytes += int64(n)
	if r.b.bytes < 0 {
		// Deliver nothing from an over-budget read: the scan fails.
		return 0, r.b.limitErr("bytes", r.b.w.lim.passBytes, r.b.w.lim.waitBytes)
	}
	return n, err
}

// pass runs one poll and reports whether any live hold still names an
// unreleased ref.
func (w *publicationWait) pass() (bool, error) {
	w.passes++
	b := &scanBudget{
		w:       w,
		bytes:   min(w.lim.passBytes, w.lim.waitBytes-w.bytes),
		records: min(w.lim.passRecords, w.lim.waitRecords-w.records),
	}
	// A failed poll ends the wait, so every poll starts within budget.
	assertf(b.bytes >= 0 && b.records >= 0, "publication wait: poll started over budget (%d bytes, %d records)", b.bytes, b.records)

	d, err := os.Open(w.dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			w.verdicts = nil
			return false, nil
		}
		return false, err
	}
	defer func() { _ = d.Close() }()

	// Keep listing after the verdict is known so the cache holds exactly
	// the holds present now (bounded by the entry budget), but read no
	// further files.
	next := make(map[string]holdVerdict, len(w.verdicts))
	held := false
	for {
		entries, rerr := d.ReadDir(64)
		for _, e := range entries {
			if err := b.entry(); err != nil {
				return false, err
			}
			// Sidecars are never holds (see readPushHolds).
			if isPushHoldSidecar(e.Name()) || e.IsDir() {
				continue
			}
			path := filepath.Join(w.dir, e.Name())
			info, err := e.Info()
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					continue // released between the listing and the stat
				}
				return false, err
			}
			if !pushHoldLive(path, info.ModTime()) {
				continue
			}
			v := holdVerdict{size: info.Size(), mtime: info.ModTime().UnixNano(), relSize: -1, relMtime: -1}
			rel := holdSidecarOwner(path) + pushHoldReleasedSuffix
			relOK := true
			switch ri, err := os.Lstat(rel); {
			case err == nil:
				v.relSize, v.relMtime = ri.Size(), ri.ModTime().UnixNano()
			case !errors.Is(err, os.ErrNotExist):
				relOK = false // unreadable: subtract nothing, cache nothing
			}
			if old, ok := w.verdicts[path]; ok && relOK &&
				old.size == v.size && old.mtime == v.mtime && old.relSize == v.relSize && old.relMtime == v.relMtime {
				next[path] = old
				held = held || old.held
				continue
			}
			if held {
				continue
			}
			live, cacheable, err := w.holdLive(path, rel, relOK && v.relSize >= 0, b)
			if err != nil {
				return false, err
			}
			if cacheable && relOK {
				v.held = live
				next[path] = v
			}
			held = live
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			return false, rerr
		}
	}
	w.verdicts = next
	return held, nil
}

// holdLive reports whether the hold at path names a ref its tombstone
// sidecar rel has not released. Tombstones are read first: they only
// subtract, and an earlier read of the append-only sidecar is a subset, so
// a release landing mid-check over-holds (the safe direction). Only
// newline-terminated tombstones count (a torn append's prefix may equal a
// different live name); an unterminated hold tail counts as held. An
// unreadable sidecar subtracts nothing, like readPushHolds, and the
// verdict is not cached so the next poll re-probes.
func (w *publicationWait) holdLive(path, rel string, hasRel bool, b *scanBudget) (live, cacheable bool, err error) {
	w.reads++
	cacheable = true
	var released map[string]struct{}
	if hasRel {
		released, err = w.readReleased(rel, b)
		if errors.Is(err, errPublicationScanLimit) {
			return false, false, err
		}
		if err != nil {
			released, cacheable = nil, false
		}
	}
	if w.beforeRead != nil {
		w.beforeRead(path)
	}
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, false, nil // released since the listing
		}
		return false, false, err
	}
	defer func() { _ = f.Close() }()
	err = w.scan(f, b, false, func(rec []byte) bool {
		if len(rec) == 0 {
			return false
		}
		_, gone := released[string(rec)]
		live = !gone
		return live
	})
	if err != nil {
		return false, false, err
	}
	return live, cacheable, nil
}

func (w *publicationWait) readReleased(rel string, b *scanBudget) (map[string]struct{}, error) {
	if w.beforeRead != nil {
		w.beforeRead(rel)
	}
	f, err := os.Open(rel)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	released := make(map[string]struct{})
	err = w.scan(f, b, true, func(rec []byte) bool {
		if len(rec) > 0 {
			released[string(rec)] = struct{}{}
		}
		return false
	})
	return released, err
}

// scan feeds f's newline-separated records to fn until fn returns true or
// the file ends, charging every byte and record to b first. An
// unterminated final record is passed to fn unless terminatedOnly.
func (w *publicationWait) scan(f io.Reader, b *scanBudget, terminatedOnly bool, fn func(rec []byte) (stop bool)) error {
	w.br.Reset(&budgetReader{r: f, b: b})
	defer w.br.Reset(nil)
	for {
		rec, err := w.br.ReadSlice('\n')
		switch {
		case err == nil:
			rec = rec[:len(rec)-1]
		case errors.Is(err, bufio.ErrBufferFull):
			return fmt.Errorf("%w: a record exceeds %d bytes", errPublicationScanLimit, pushRefPathMax)
		case errors.Is(err, io.EOF):
			if len(rec) == 0 || terminatedOnly {
				return nil
			}
		default:
			return err
		}
		if cerr := b.record(); cerr != nil {
			return cerr
		}
		if fn(rec) || err != nil {
			return nil
		}
	}
}
