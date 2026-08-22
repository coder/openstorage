package simtest

import (
	"context"
	"fmt"
	"time"

	"codestorage/internal/ref"
)

// SimRefStore decorates a ref.RefStore with seeded clean/ambiguous fault
// injection. It sits BELOW ref.IdempotentCAS in the decorator chain (wrap
// SimRefStore in IdempotentCAS, or hand it to events.NewRefStore, which
// applies IdempotentCAS itself), so injected ambiguous CAS errors exercise
// the issue-#2 recovery path exactly where real transport errors surface.
type SimRefStore struct {
	inner ref.RefStore
	inj   *Injector
}

var _ ref.RefStore = (*SimRefStore)(nil)

// NewSimRefStore wraps inner with fault injection driven by inj.
func NewSimRefStore(inner ref.RefStore, inj *Injector) *SimRefStore {
	assertf(inner != nil, "NewSimRefStore: nil inner store")
	assertf(inj != nil, "NewSimRefStore: nil injector")
	return &SimRefStore{inner: inner, inj: inj}
}

// refReadFault turns an injected class into the error a faulted read
// returns. Reads never mutate, so clean and ambiguous faults behave
// identically (the op is skipped); the error identity still records which
// class fired. NotFound is a blob-only class.
func refReadFault(op string, class FaultClass) error {
	switch class {
	case FaultClean:
		return cleanFaultError{op: op}
	case FaultAmbiguous:
		return ambiguousFaultError{op: op}
	}
	panic(fmt.Sprintf("simtest: fault class %v invalid for %s", class, op))
}

// ListRefs delegates to the inner store unless a fault is injected.
func (s *SimRefStore) ListRefs(ctx context.Context, repo string) (map[string]string, error) {
	if class := s.inj.begin(OpRefListRefs); class != FaultNone {
		return nil, refReadFault(OpRefListRefs, class)
	}
	return s.inner.ListRefs(ctx, repo)
}

// ListRefsWithMeta delegates to the inner store unless a fault is injected.
func (s *SimRefStore) ListRefsWithMeta(ctx context.Context, repo string) ([]ref.RefInfo, error) {
	if class := s.inj.begin(OpRefListRefsWithMeta); class != FaultNone {
		return nil, refReadFault(OpRefListRefsWithMeta, class)
	}
	return s.inner.ListRefsWithMeta(ctx, repo)
}

// GetRef delegates to the inner store unless a fault is injected. Faulting
// it starves ref.IdempotentCAS's recovery recheck, which must then fall
// back to the original ambiguous outcome.
func (s *SimRefStore) GetRef(ctx context.Context, repo, name string) (string, error) {
	if class := s.inj.begin(OpRefGetRef); class != FaultNone {
		return "", refReadFault(OpRefGetRef, class)
	}
	return s.inner.GetRef(ctx, repo, name)
}

// CASRef injects the fault classes that matter for issue #2:
//
//   - clean: the statement never executes; the returned error satisfies
//     SafeToRetry, so IdempotentCAS must propagate it un-recovered.
//   - ambiguous: the statement executes (the ref may really move) but the
//     caller sees a transport-shaped error; the real outcome is recorded on
//     the FaultEvent for oracle bookkeeping.
func (s *SimRefStore) CASRef(ctx context.Context, repo, name, old, new string) (bool, error) {
	switch class := s.inj.begin(OpRefCASRef); class {
	case FaultClean:
		return false, cleanFaultError{op: OpRefCASRef}
	case FaultAmbiguous:
		won, err := s.inner.CASRef(ctx, repo, name, old, new)
		s.inj.noteInnerOutcome(OpRefCASRef, fmt.Sprintf("won=%v err=%v", won, err))
		return false, ambiguousFaultError{op: OpRefCASRef}
	case FaultNone:
		return s.inner.CASRef(ctx, repo, name, old, new)
	default:
		panic(fmt.Sprintf("simtest: fault class %v invalid for %s", class, OpRefCASRef))
	}
}

// RegisterPacks supports the same clean/ambiguous split as CASRef: an
// ambiguous fault registers the packs (idempotently) and then loses the
// result.
func (s *SimRefStore) RegisterPacks(ctx context.Context, repo string, packs []ref.Pack) error {
	switch class := s.inj.begin(OpRefRegisterPacks); class {
	case FaultClean:
		return cleanFaultError{op: OpRefRegisterPacks}
	case FaultAmbiguous:
		err := s.inner.RegisterPacks(ctx, repo, packs)
		s.inj.noteInnerOutcome(OpRefRegisterPacks, fmt.Sprintf("err=%v", err))
		return ambiguousFaultError{op: OpRefRegisterPacks}
	case FaultNone:
		return s.inner.RegisterPacks(ctx, repo, packs)
	default:
		panic(fmt.Sprintf("simtest: fault class %v invalid for %s", class, OpRefRegisterPacks))
	}
}

// ReplacePacks supports the same clean/ambiguous split as CASRef: an
// ambiguous fault executes the swap (which may really commit) and then
// loses the outcome, exercising the repack job's leave-the-blob handling.
func (s *SimRefStore) ReplacePacks(ctx context.Context, repo string, old []ref.Pack, newPack ref.Pack) (bool, error) {
	switch class := s.inj.begin(OpRefReplacePacks); class {
	case FaultClean:
		return false, cleanFaultError{op: OpRefReplacePacks}
	case FaultAmbiguous:
		won, err := s.inner.ReplacePacks(ctx, repo, old, newPack)
		s.inj.noteInnerOutcome(OpRefReplacePacks, fmt.Sprintf("won=%v err=%v", won, err))
		return false, ambiguousFaultError{op: OpRefReplacePacks}
	case FaultNone:
		return s.inner.ReplacePacks(ctx, repo, old, newPack)
	default:
		panic(fmt.Sprintf("simtest: fault class %v invalid for %s", class, OpRefReplacePacks))
	}
}

// ListPacks delegates to the inner store unless a fault is injected.
func (s *SimRefStore) ListPacks(ctx context.Context, repo string) ([]ref.Pack, error) {
	if class := s.inj.begin(OpRefListPacks); class != FaultNone {
		return nil, refReadFault(OpRefListPacks, class)
	}
	return s.inner.ListPacks(ctx, repo)
}

// HasPack delegates to the inner store unless a fault is injected; it
// shares OpRefListPacks with ListPacks (one registry-read fault point).
func (s *SimRefStore) HasPack(ctx context.Context, repo, key string) (bool, error) {
	if class := s.inj.begin(OpRefListPacks); class != FaultNone {
		return false, refReadFault(OpRefListPacks, class)
	}
	return s.inner.HasPack(ctx, repo, key)
}

// ReservePackBytes supports the same clean/ambiguous split as CASRef: an
// ambiguous fault runs the admission (a reservation row may really be
// inserted — the leaked-reservation class TTL expiry and the sweeper
// reclaim) and then loses the outcome.
func (s *SimRefStore) ReservePackBytes(ctx context.Context, group []string, packs []ref.Pack, owner string, usageFloor, quotaBytes int64, ttl time.Duration) (ref.ReserveDecision, error) {
	switch class := s.inj.begin(OpRefReservePacks); class {
	case FaultClean:
		return ref.ReserveDecision{}, cleanFaultError{op: OpRefReservePacks}
	case FaultAmbiguous:
		dec, err := s.inner.ReservePackBytes(ctx, group, packs, owner, usageFloor, quotaBytes, ttl)
		s.inj.noteInnerOutcome(OpRefReservePacks, fmt.Sprintf("ok=%v err=%v", dec.OK, err))
		return ref.ReserveDecision{}, ambiguousFaultError{op: OpRefReservePacks}
	case FaultNone:
		return s.inner.ReservePackBytes(ctx, group, packs, owner, usageFloor, quotaBytes, ttl)
	default:
		panic(fmt.Sprintf("simtest: fault class %v invalid for %s", class, OpRefReservePacks))
	}
}

// SweepExpiredPackReservations delegates to the inner store.
func (s *SimRefStore) SweepExpiredPackReservations(ctx context.Context, olderThan time.Duration) (int64, error) {
	return s.inner.SweepExpiredPackReservations(ctx, olderThan)
}

// IsRetainedPack delegates to the inner store.
func (s *SimRefStore) IsRetainedPack(ctx context.Context, repo, key string) (bool, error) {
	return s.inner.IsRetainedPack(ctx, repo, key)
}

// ReclaimRetainedPack delegates to the inner store.
func (s *SimRefStore) ReclaimRetainedPack(ctx context.Context, repo, key string) (int64, error) {
	return s.inner.ReclaimRetainedPack(ctx, repo, key)
}

// RegisterLFSObject delegates to the inner store (the DST does not model
// LFS transfers yet; delegation keeps the interface honest without adding
// an unexercised fault point).
func (s *SimRefStore) RegisterLFSObject(ctx context.Context, repo, key string, bytes int64) error {
	return s.inner.RegisterLFSObject(ctx, repo, key, bytes)
}

// HasLFSObject delegates to the inner store.
func (s *SimRefStore) HasLFSObject(ctx context.Context, repo, key string) (bool, error) {
	return s.inner.HasLFSObject(ctx, repo, key)
}

// QuotaLedgerState delegates to the inner store (oracle reads).
func (s *SimRefStore) QuotaLedgerState(ctx context.Context, group []string, ttl time.Duration) (ref.PackQuotaLedgerState, error) {
	return s.inner.QuotaLedgerState(ctx, group, ttl)
}

// GuardedLFSDelete delegates to the inner store.
func (s *SimRefStore) GuardedLFSDelete(ctx context.Context, repo, key string, del func(context.Context) error) (bool, error) {
	return s.inner.GuardedLFSDelete(ctx, repo, key, del)
}

// CountLFSObjects delegates to the inner store.
func (s *SimRefStore) CountLFSObjects(ctx context.Context, repo string) (int64, error) {
	return s.inner.CountLFSObjects(ctx, repo)
}

// GetGeneration delegates to the inner store unless a fault is injected.
// Faulting it starves the issue-#4 hydrate short-circuit, which must then
// surface the error rather than serve from a stale marker.
func (s *SimRefStore) GetGeneration(ctx context.Context, repo string) (int64, error) {
	if class := s.inj.begin(OpRefGetGeneration); class != FaultNone {
		return 0, refReadFault(OpRefGetGeneration, class)
	}
	return s.inner.GetGeneration(ctx, repo)
}
