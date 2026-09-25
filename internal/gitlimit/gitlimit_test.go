package gitlimit

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// One key (repository) holds at most perKey slots; other keys still get
// the rest, a waiter wakes as soon as a slot is released, and a cancelled
// waiter gives up.
func TestSlotsTryAcquireAndPerKeyShare(t *testing.T) {
	s := NewSlots(3, 2, 0)
	if !s.TryAcquire("a") || s.Acquire(context.Background(), "a") != nil {
		t.Fatal("could not take key a's two slots")
	}
	if s.TryAcquire("a") {
		t.Fatal("key a exceeded its per-key share")
	}
	if !s.TryAcquire("b") {
		t.Fatal("key b refused while the pool has room")
	}
	if err := s.Acquire(context.Background(), "c"); !errors.Is(err, ErrBusy) {
		t.Fatalf("Acquire on a full pool without wait = %v, want ErrBusy", err)
	}
	s.Release("a")
	if s.InUse() != 2 || !s.TryAcquire("c") {
		t.Fatalf("after one release: InUse %d, or key c refused", s.InUse())
	}
	w := NewSlots(1, 1, time.Minute)
	w.TryAcquire("a")
	got := make(chan error, 1)
	start := time.Now()
	go func() { got <- w.Acquire(context.Background(), "b") }()
	time.Sleep(50 * time.Millisecond)
	w.Release("a")
	if err := <-got; err != nil {
		t.Fatalf("waiter: %v", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("waiter woke after %v, want right after the release", d)
	}
	// The pool is full again (b holds its only slot): a cancelled waiter
	// must give up instead of waiting out the minute.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := w.Acquire(ctx, "a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Acquire with a cancelled context = %v, want context.Canceled", err)
	}
	w.Release("b")
	if w.InUse() != 0 {
		t.Fatalf("in use = %d after releasing everything", w.InUse())
	}
}

// Maintenance takes at most MaintenanceShare slots of ReadSlots, within
// each repository's share, fails without taking anything when any limit
// is reached, and returns both permits.
func TestTryAdmitMaintenance(t *testing.T) {
	var releases []func()
	for i := range MaintenanceShare {
		r, ok := TryAdmitMaintenance(RepoKey(fmt.Sprintf("repo_m%d", i)))
		if !ok {
			t.Fatal("maintenance refused below its share")
		}
		releases = append(releases, r)
	}
	if _, ok := TryAdmitMaintenance(RepoKey("repo_other")); ok {
		t.Fatal("maintenance admitted beyond its share")
	}
	if got := ReadSlots.InUse(); got != MaintenanceShare {
		t.Fatalf("ReadSlots in use = %d, want %d", got, MaintenanceShare)
	}
	for _, r := range releases {
		r()
	}

	// A repository whose reads hold its whole share refuses maintenance
	// for that repository only.
	busy := RepoKey("repo_busy")
	for range RepoSlotCount {
		if !ReadSlots.TryAcquire(busy) {
			t.Fatal("read slot unavailable")
		}
	}
	if _, ok := TryAdmitMaintenance(busy); ok {
		t.Fatal("maintenance admitted past the repository's share")
	}
	r, ok := TryAdmitMaintenance(RepoKey("repo_idle"))
	if !ok {
		t.Fatal("maintenance for another repository refused")
	}
	r()
	for range RepoSlotCount {
		ReadSlots.Release(busy)
	}

	// A full read pool refuses maintenance.
	for i := range ReadSlotCount {
		if !ReadSlots.TryAcquire(RepoKey(fmt.Sprintf("repo_r%d", i))) {
			t.Fatal("read slot unavailable")
		}
	}
	if _, ok := TryAdmitMaintenance(RepoKey("repo_x")); ok {
		t.Fatal("maintenance admitted with the read pool full")
	}
	for i := range ReadSlotCount {
		ReadSlots.Release(RepoKey(fmt.Sprintf("repo_r%d", i)))
	}
	if ReadSlots.InUse() != 0 || maintenanceShare.InUse() != 0 {
		t.Fatalf("leaked permits: reads %d, maintenance %d", ReadSlots.InUse(), maintenanceShare.InUse())
	}
}

func TestTryAdmitMaintenanceDoubleReleasePanics(t *testing.T) {
	r, ok := TryAdmitMaintenance(RepoKey("repo_d"))
	if !ok {
		t.Fatal("maintenance refused")
	}
	r()
	defer func() {
		if recover() == nil {
			t.Fatal("second release did not panic")
		}
	}()
	r()
}
