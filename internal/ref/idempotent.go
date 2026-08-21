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
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"modernc.org/sqlite"
)

// recheckTimeout bounds the recovery point-read. The recheck runs on a
// cancellation-detached context — the original ctx may already be dead (a
// client hang-up mid-CAS is one way the ambiguous outcome happens) — so it
// needs its own deadline.
const recheckTimeout = 5 * time.Second

// IdempotentCAS decorates a RefStore so a CASRef whose outcome is
// ambiguous (or, for updates, cleanly lost) is recovered as a win when the
// ref already holds the target sha — see CASRef for the exact matrix. It is
// applied at the two write seams (events.NewRefStore wraps its inner store;
// the update hook's eventless fallback wraps directly), so every CAS caller
// — git hook path, API branch/tag/notes handlers, commit publish, merge
// fast-forward, fork — sees reconciled outcomes.
type IdempotentCAS struct {
	RefStore
}

var _ RefStore = IdempotentCAS{}

// CASRef delegates to the inner store and reconciles non-win outcomes by
// operation shape (the same create/update/delete split CASRef itself
// switches on):
//
//   - ambiguous error (see ambiguousError), create or update: recheck;
//     current == new is a win — our statement may well have committed, and
//     reporting the win emits the push webhook that would otherwise be
//     dropped.
//   - definitive error (the backend rejected the statement, or the request
//     provably never reached it): NEVER recovered — the statement did not
//     commit, so current == new only means someone else put the target
//     there; recovering would fabricate a success (and a push event) for a
//     transition this call never committed. The error propagates; a
//     client's retry then converges through the clean-loss rules below.
//   - clean loss (won=false, err=nil), update (old and new both non-zero):
//     recheck; current == new is a win. This heals the client that retries
//     an update whose first attempt committed but failed to report and
//     would otherwise see a spurious conflict against its own sha.
//   - clean loss, create (old == ZeroSHA): NEVER recovered. A clean create
//     loss is authoritative — our INSERT affirmatively wrote nothing and
//     the ref exists. "Already exists" is the correct answer even when the
//     existing sha equals the target: recovering would turn every same-sha
//     duplicate create into a fabricated success and emit a fabricated
//     zero->sha push event for a transition this call never committed.
//   - delete (new == ZeroSHA): NEVER recovered, clean or ambiguous. The
//     row is absent after a committed delete, and absence cannot
//     distinguish "our DELETE committed" from "the ref never existed" or
//     "someone else deleted it" — unlike current == new, it is not
//     positive evidence. Accepted cost: a client retrying a delete whose
//     first attempt committed but failed to report sees a spurious CAS
//     conflict; the end state (ref absent) is the one it asked for.
//
// Callers observing a recovered win may emit duplicate webhooks for one
// committed update (first attempt + recovered retry); delivery is
// at-least-once, so duplicates are acceptable.
//
// Generation interaction (issue #4): a recovered win needs no generation
// bump. The bump commits atomically with the swap itself (schema-level
// AFTER-row triggers fire inside the swap statement's own transaction), so
// an ambiguous-but-actually-committed CAS already moved the generation, and
// the recheck below is a pure point-read. Recovering here can therefore
// never leave a won CAS without its bump, nor bump twice.
func (s IdempotentCAS) CASRef(ctx context.Context, repo, name, old, new string) (bool, error) {
	won, err := s.RefStore.CASRef(ctx, repo, name, old, new)
	assertf(!won || err == nil, "CASRef returned won=true with error: %v", err)
	if won {
		return true, nil
	}
	if new == ZeroSHA {
		return false, err
	}
	if err != nil && !ambiguousError(err) {
		return false, err // definitive failure: the statement did not commit
	}
	if err == nil && old == ZeroSHA {
		return false, nil // clean create conflict is authoritative
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

// ambiguousError reports whether a CASRef error leaves the statement's
// commit status unknown. Definitive failures prove the statement did NOT
// commit:
//
//   - *pgconn.PgError / *sqlite.Error: the backend itself rejected the
//     statement (serialization abort, permission, constraint, busy) — it
//     responded, so it did not silently commit first. Exception: SQLSTATEs
//     that explicitly encode an unknown outcome (40003
//     statement_completion_unknown, 08007 transaction_resolution_unknown,
//     as reported by servers or compatible proxies) stay ambiguous.
//   - pgconn.SafeToRetry: the request never reached the server. Both
//     stores mark their acquire stage this way (see notExecuted): a
//     pool/connection acquisition failure — e.g. a deadline expiring while
//     the pool is exhausted — happens strictly before the statement is
//     sent, so it must not be recovered even though it often surfaces as a
//     bare context error.
//
// Everything else — connection drops, unexpected EOFs, context
// cancellation mid-flight after the request was sent — is ambiguous: the
// statement may have committed with the result lost on the way back.
func ambiguousError(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// These SQLSTATEs are the backend SAYING the outcome is unknown:
		// the statement/transaction may have committed.
		return pgErr.Code == "40003" || pgErr.Code == "08007"
	}
	var sqErr *sqlite.Error
	if errors.As(err, &sqErr) {
		return false
	}
	return !pgconn.SafeToRetry(err)
}

// notExecutedError marks an error raised before the CAS statement was sent
// to the backend (pool/connection acquisition): the statement provably did
// not execute. It satisfies the SafeToRetry contract pgconn uses for
// never-sent requests, so ambiguousError classifies it as definitive.
type notExecutedError struct{ err error }

// notExecuted wraps an acquire-stage error with the not-executed marker.
func notExecuted(err error) error {
	assertf(err != nil, "notExecuted: nil error")
	return notExecutedError{err: err}
}

func (e notExecutedError) Error() string   { return e.err.Error() }
func (e notExecutedError) Unwrap() error   { return e.err }
func (notExecutedError) SafeToRetry() bool { return true }
