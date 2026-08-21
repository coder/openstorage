// IdempotentCAS resolves ambiguous CASRef outcomes (issue #2): if the DB
// connection dies after the CAS statement commits but before the result is
// read, the store reports an error for an update that actually landed. The
// events.RefStore decorator then skips the push webhook, and the client's
// retry matches zero rows and surfaces a spurious "moved concurrently"
// conflict. Moving a ref to the sha we were asked to move it to is
// idempotent, so both cases recover by re-reading the ref: if it already
// equals the target, the operation succeeded — attribution does not matter.

package ref

import (
	"context"
	"time"
)

// recheckTimeout bounds the recovery point-read. The recheck runs on a
// cancellation-detached context — the original ctx may already be dead (a
// client hang-up mid-CAS is one way the ambiguous outcome happens) — so it
// needs its own deadline.
const recheckTimeout = 5 * time.Second

// IdempotentCAS decorates a RefStore so a CASRef that errored or lost is
// recovered as a win when the ref already holds the target sha. It is
// applied at the two write seams (events.NewRefStore wraps its inner store;
// the update hook's eventless fallback wraps directly), so every CAS caller
// — git hook path, API branch/tag/notes handlers, commit publish, merge
// fast-forward, fork — sees reconciled outcomes.
type IdempotentCAS struct {
	RefStore
}

var _ RefStore = IdempotentCAS{}

// CASRef delegates to the inner store and reconciles non-win outcomes for
// non-delete updates: on an ambiguous error OR a lost CAS, re-read the ref
// and treat current == new as a win. Callers observing won=true may emit
// duplicate webhooks for one committed update (first attempt + recovered
// retry); delivery is at-least-once, so duplicates are acceptable.
//
// Deletes (new == ZeroSHA) are never recovered. A lost delete finds no row,
// and absence cannot distinguish "our DELETE committed" from "the ref never
// existed" or "someone else deleted it" — unlike current == new, it is not
// positive evidence. The same reasoning covers the ambiguous-error delete:
// the original outcome is returned rather than fabricating a success (and a
// deletion webhook) for an operation this call may not have performed.
// Accepted cost: a client retrying a delete whose first attempt committed
// but failed to report sees a spurious CAS conflict; the end state (ref
// absent) is the one it asked for, and the next fetch clarifies.
func (s IdempotentCAS) CASRef(ctx context.Context, repo, name, old, new string) (bool, error) {
	won, err := s.RefStore.CASRef(ctx, repo, name, old, new)
	assertf(!won || err == nil, "CASRef returned won=true with error: %v", err)
	if won {
		return true, nil
	}
	if new == ZeroSHA {
		return false, err
	}
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), recheckTimeout)
	defer cancel()
	cur, gerr := s.GetRef(rctx, repo, name)
	if gerr != nil || cur != new {
		// The recheck failed or showed anything but the target: fall back
		// to the original outcome (genuine conflicts stay conflicts, and a
		// dead connection stays an error for the client to retry).
		return false, err
	}
	return true, nil
}
