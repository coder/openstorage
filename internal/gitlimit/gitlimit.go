// Package gitlimit is the shared chokepoint for git subprocesses that
// untrusted input can make expensive (#313, #322): the process-wide
// admission pool, the per-child heap cap, and their error values. The
// read API (internal/gitread, ADR-049) and cache maintenance
// (internal/githttp's commit-graph refresh, ADR-47) both use it, so they
// share one admission pool instead of each bounding itself. It is a leaf
// package: gitread imports githttp, so the shared pieces cannot live in
// gitread.
package gitlimit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	// HeavyHeapBytes caps the heap (RLIMIT_DATA, Linux) of a child whose
	// work grows with history length, tree size, blob size, or output
	// size. See gitread.HeavyHeapBytes for the measurements.
	HeavyHeapBytes int64 = 256 << 20

	// ReadSlotCount is the size of ReadSlots: requests with JSON
	// responses that run git, plus admitted maintenance, at once in this
	// process.
	ReadSlotCount = 8
	// RepoSlotCount is how many slots of a pool one repository may hold.
	RepoSlotCount = 2
	// ReadSlotWait is how long a read request waits for a ReadSlots slot.
	ReadSlotWait = time.Second
	// MaintenanceShare is how many ReadSlots slots optional maintenance
	// (commit-graph refreshes) may hold at once, so it can never take the
	// whole pool from reads.
	MaintenanceShare = 2
)

var (
	// ErrBusy: no admission slot freed up in time; git was not started.
	ErrBusy = errors.New("git read capacity exhausted")
	// ErrMemoryLimit: git needed more heap than its cap and was stopped by
	// its own allocation failure (Linux only).
	ErrMemoryLimit = errors.New("git exceeded its memory limit")
)

// assertf is a fail-fast invariant check for programming errors only.
func assertf(cond bool, format string, args ...any) {
	if !cond {
		panic(fmt.Sprintf("gitlimit assertion failed: "+format, args...))
	}
}

// Slots is a pool of admission slots. One key (a repository, RepoKey) may
// hold at most perKey of them, so slow work on one repository cannot take
// the whole pool from the others. A holder releases exactly the slots it
// acquired, under the same key.
type Slots struct {
	n, perKey int
	wait      time.Duration

	mu    sync.Mutex
	used  int
	byKey map[string]int
	freed chan struct{} // closed and replaced on every release
}

// NewSlots returns a pool of n slots, at most perKey of them per key.
// Acquire waits at most wait for a free slot before failing with ErrBusy.
func NewSlots(n, perKey int, wait time.Duration) *Slots {
	assertf(n > 0, "NewSlots: n %d", n)
	assertf(perKey > 0 && perKey <= n, "NewSlots: perKey %d of %d", perKey, n)
	assertf(wait >= 0, "NewSlots: wait %v", wait)
	return &Slots{n: n, perKey: perKey, wait: wait, byKey: map[string]int{}, freed: make(chan struct{})}
}

// tryAcquire takes a slot for key if the pool and the key's share allow
// it; otherwise it returns a channel that is closed on the next release.
func (s *Slots) tryAcquire(key string) (bool, <-chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.used < s.n && s.byKey[key] < s.perKey {
		s.used++
		s.byKey[key]++
		return true, nil
	}
	return false, s.freed
}

// TryAcquire takes a slot for key if one is free right now.
func (s *Slots) TryAcquire(key string) bool {
	ok, _ := s.tryAcquire(key)
	return ok
}

// Acquire takes a slot for key, waiting at most the pool's wait (ErrBusy)
// or until ctx ends (its error).
func (s *Slots) Acquire(ctx context.Context, key string) error {
	var expired <-chan time.Time
	if s.wait > 0 {
		t := time.NewTimer(s.wait)
		defer t.Stop()
		expired = t.C
	}
	for {
		ok, freed := s.tryAcquire(key)
		if ok {
			return nil
		}
		if s.wait == 0 {
			return ErrBusy
		}
		select {
		case <-freed: // try again
		case <-expired:
			return ErrBusy
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Release returns one slot taken for key.
func (s *Slots) Release(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	assertf(s.used > 0 && s.byKey[key] > 0, "Slots.Release: no slot held for %q", key)
	s.used--
	if s.byKey[key]--; s.byKey[key] == 0 {
		delete(s.byKey, key)
	}
	close(s.freed)
	s.freed = make(chan struct{})
}

// InUse reports how many slots are held (tests and diagnostics).
func (s *Slots) InUse() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.used
}

// RepoKey is the Slots key of a repository: its canonical RepoID, so every
// spelling of one repository shares one per-repository quota.
func RepoKey(repoID string) string {
	assertf(repoID != "", "RepoKey: empty RepoID")
	return "repo\x00" + repoID
}

// ReadSlots is the process-wide pool shared by JSON git reads (the read
// API's budget middleware) and admitted maintenance.
var ReadSlots = NewSlots(ReadSlotCount, RepoSlotCount, ReadSlotWait)

// maintenanceShare bounds maintenance's share of ReadSlots.
var maintenanceShare = NewSlots(MaintenanceShare, MaintenanceShare, 0)

// maintenanceKey is the single key of maintenanceShare.
const maintenanceKey = "maintenance"

// TryAdmitMaintenance admits one optional maintenance job for the
// repository key (RepoKey) into ReadSlots without waiting: it needs one of
// the MaintenanceShare maintenance permits AND one ReadSlots slot within
// that repository's share, both taken with non-blocking tries (no
// check-then-act on counts). So at most MaintenanceShare jobs run, reads
// always keep the rest of the pool, and a repository's maintenance and
// reads share its per-repository quota. ok=false means skip the job; a
// caller holding other locks never blocks here. release must be called
// exactly once when ok.
func TryAdmitMaintenance(key string) (release func(), ok bool) {
	assertf(key != "", "TryAdmitMaintenance: empty key")
	if !maintenanceShare.TryAcquire(maintenanceKey) {
		return nil, false
	}
	if !ReadSlots.TryAcquire(key) {
		maintenanceShare.Release(maintenanceKey)
		return nil, false
	}
	released := false
	return func() {
		assertf(!released, "TryAdmitMaintenance: release called twice")
		released = true
		ReadSlots.Release(key)
		maintenanceShare.Release(maintenanceKey)
	}, true
}
