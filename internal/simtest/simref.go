package simtest

import (
	"context"
	"fmt"

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

// ListPacks delegates to the inner store unless a fault is injected.
func (s *SimRefStore) ListPacks(ctx context.Context, repo string) ([]ref.Pack, error) {
	if class := s.inj.begin(OpRefListPacks); class != FaultNone {
		return nil, refReadFault(OpRefListPacks, class)
	}
	return s.inner.ListPacks(ctx, repo)
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
