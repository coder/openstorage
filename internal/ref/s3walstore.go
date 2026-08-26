// S3WAL implements Store over the s3wal manifest-CAS substrate (issue
// #133, docs/plans/s3wal-plan.md): the ENTIRE authority state of one base
// repo group {base, +ephemeral, +import} lives in one JSON manifest object
// advanced by conditional PUT, so every multi-entity transaction the SQL
// backends run is a single-object CAS here.
//
// The implementation lives in package ref (not package s3wal) on purpose:
// it needs ref's unexported machinery — casFence, notExecuted, the ledger
// knobs (maxActiveReservations, reconcileLegacyLFSBatch), baseRepoOf,
// lfsClaimTwin — and the internal conformance tests must inspect it, while
// ref.Open must dispatch to it. A store.go inside package s3wal would be
// an import cycle in both directions; s3wal stays a leaf substrate
// (manifest codec, conditional clients, fakes).
//
// Concurrency: every manifest operation — reads included — runs under a
// per-base in-process mutex. This mirrors the serialization the SQL
// backends already impose (PG's lockBaseLedger advisory lock, SQLite's
// single-writer BEGIN IMMEDIATE) under the single-node decree, and it is
// what keeps the casobj session-scoped seq watermark sound: casobj.go
// documents that goroutines sharing one Client can process responses out
// of store-serialization order on a hot key, so unserialized same-key
// reads could trip the stale-read assert on benign reordering.
// Cross-PROCESS writers remain serialized by the manifest CAS itself.
package ref

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	"github.com/coder/code-storage/internal/ref/s3wal"
)

// s3wal manifest size guards (plan §2.2): a documented boundary, not a
// silent failure. Enforced only on mutations that ADD entries
// (registrations, reservations, ref creates) — shrinking mutations must
// always pass so an oversized manifest can be cleaned up.
const (
	s3walSoftWarnBytes = 4 << 20
	s3walHardCapBytes  = 16 << 20
)

// W6 delete-lease protocol constants (plan §2.5): the lock TTL must
// provably cover the delete I/O — TTL >= deleteIOTimeout + safety margin +
// skew margin (min(15m, TTL/4)) — so even a lock that missed its final
// renewal still fences the worst-case in-flight DELETE. Vars so the
// recovery unit tests can shrink the protocol to milliseconds.
var (
	s3walDeleteLockTTL    = 2 * time.Minute
	s3walDeleteIOTimeout  = 30 * time.Second
	s3walDeleteLockSafety = 15 * time.Second
	s3walDeleteLockPoll   = 25 * time.Millisecond
)

// S3WAL implements Store (plus the purge/reclaim/absence job seams) over a
// conditional object store.
type S3WAL struct {
	cas s3wal.ConditionalStore
	// probe runs InitSchema's conditional-write fidelity check (§2.6);
	// nil (test fakes) skips it.
	probe   func(ctx context.Context) error
	closeFn func()
	// now is the local clock source, overridable by clock tests. Every
	// stamp WRITTEN into a manifest uses the manifest logical clock
	// derived from it (s3wal.ClkNext), never a bare now().
	now func() time.Time
	// wallDrift reports how far the process wall clock has diverged from
	// its monotonic clock since the store was built (positive = the wall
	// clock jumped forward) — the §2.4 drift arm of s3wal.SkewOK.
	// Overridable by clock tests: fake now() funcs carry no monotonic
	// reading, so the production default stays ~0 under them and jump
	// tests inject the drift explicitly.
	wallDrift func() time.Duration

	// Lost-race retry budget (plan §2.3 risk note: raise attempts on the
	// STORE if contention flakes appear, never in tests).
	casAttempts int
	backoffBase time.Duration
	backoffCap  time.Duration

	softWarnBytes int
	hardCapBytes  int

	mu    sync.Mutex
	locks map[string]*sync.Mutex

	// Driver-level per-manifest seq watermark: the same stale-read
	// defense casobj.Client arms for the real store, kept here too so it
	// covers ANY ConditionalStore (MemCAS included) and this process's
	// own accepted writes.
	seqMu   sync.Mutex
	lastSeq map[string]int64

	warnMu sync.Mutex
	warned map[string]bool

	// reapEpochMu mirrors PG's FOR SHARE pin on the destructive-sweep
	// epoch row (Codex round 33 on PR #132): retirements hold the read
	// side from their epoch read through the retire CAS and refund park,
	// and BeginLFSReapEpoch's increment takes the write side — so a sweep
	// with a strictly greater epoch provably began its listing after
	// every retirement pinned below it finished. In-process locking is
	// the sound substitute under the single-process decree (§2.10).
	reapEpochMu sync.RWMutex
	// absenceFenceMu mirrors PG's fleet-global admission-fence advisory
	// lock pair (Codex round 35 on PR #132): finite admissions hold the
	// read side across their fence check and admission CAS;
	// ArmAbsenceAdmissionFence takes the write side. In-process fast
	// path ONLY — separate processes (Phase 3 receive hooks) hold
	// separate mutexes, so the cross-process guarantee is the
	// post-commit marker re-read in s3walRevalidateFence (Codex on
	// PR #156, security).
	absenceFenceMu sync.RWMutex
}

var _ Store = (*S3WAL)(nil)

// newS3WALStore wraps a conditional store. probe and closeFn may be nil
// (in-memory fakes).
func newS3WALStore(cs s3wal.ConditionalStore, probe func(ctx context.Context) error, closeFn func()) *S3WAL {
	assertf(cs != nil, "newS3WALStore: nil conditional store")
	return &S3WAL{
		cas:           cs,
		probe:         probe,
		closeFn:       closeFn,
		now:           time.Now,
		wallDrift:     newWallDrift(),
		casAttempts:   16,
		backoffBase:   10 * time.Millisecond,
		backoffCap:    2 * time.Second,
		softWarnBytes: s3walSoftWarnBytes,
		hardCapBytes:  s3walHardCapBytes,
		locks:         make(map[string]*sync.Mutex),
		lastSeq:       make(map[string]int64),
		warned:        make(map[string]bool),
	}
}

// openS3WALStore opens s3wal://bucket?endpoint=…&region=… (ADR-12 factory
// discipline: Open never runs the probe — InitSchema does).
func openS3WALStore(ctx context.Context, rawURL string) (*S3WAL, error) {
	c, err := s3wal.Dial(ctx, rawURL)
	if err != nil {
		return nil, fmt.Errorf("open s3wal store: %w", err)
	}
	return newS3WALStore(c, func(ctx context.Context) error { return s3wal.Probe(ctx, c) }, c.Close), nil
}

// InitSchema proves the store's conditional-write fidelity before serving
// (plan §2.6): bucket reachability and header behavior are both covered by
// the 5-step probe, which refuses to serve on any deviation. Idempotent.
func (s *S3WAL) InitSchema(ctx context.Context) error {
	if s.probe == nil {
		return nil
	}
	if err := s.probe(ctx); err != nil {
		return fmt.Errorf("s3wal init: %w", err)
	}
	return nil
}

func (s *S3WAL) Close() {
	if s.closeFn != nil {
		s.closeFn()
	}
}

// lockBase serializes manifest operations for one base group within this
// process (see the package comment). Returns the unlock func.
func (s *S3WAL) lockBase(base string) func() {
	assertf(base != "", "lockBase: empty base")
	s.mu.Lock()
	l := s.locks[base]
	if l == nil {
		l = &sync.Mutex{}
		s.locks[base] = l
	}
	s.mu.Unlock()
	l.Lock()
	return l.Unlock
}

// checkSeq enforces the driver-level per-manifest seq watermark: the store
// serving an older seq than this process already observed is a stale read
// — a store-fidelity violation, crash-and-burn (plan §2.3).
func (s *S3WAL) checkSeq(key string, seq int64) {
	s.seqMu.Lock()
	defer s.seqMu.Unlock()
	last := s.lastSeq[key]
	assertf(seq >= last, "s3wal stale read on %s: store served seq %d after this process observed %d", key, seq, last)
	if seq > last {
		s.lastSeq[key] = seq
	}
}

// raiseSeq lifts the watermark to an ACCEPTED write's seq (monotonic raise
// only — acknowledgments make no ordering claim against other writers).
func (s *S3WAL) raiseSeq(key string, seq int64) {
	s.seqMu.Lock()
	defer s.seqMu.Unlock()
	if seq > s.lastSeq[key] {
		s.lastSeq[key] = seq
	}
}

// readManifest fetches base's manifest. A missing object is the empty
// (virgin or never-mutated) state — manifests are created lazily by the
// first mutation. etag "" reports the missing case to the CAS layer
// (create path). Caller must hold the base lock.
func (s *S3WAL) readManifest(ctx context.Context, base string) (*s3wal.Manifest, string, error) {
	key := s3wal.ManifestKey(base)
	obj, err := s.cas.Get(ctx, key)
	if errors.Is(err, s3wal.ErrNotExist) {
		return s3wal.NewManifest(), "", nil
	}
	if err != nil {
		return nil, "", fmt.Errorf("s3wal read %s: %w", base, err)
	}
	m, err := s3wal.DecodeManifest(obj.Body)
	if err != nil {
		return nil, "", fmt.Errorf("s3wal read %s: %w", base, err)
	}
	s.checkSeq(key, m.Seq)
	return m, obj.ETag, nil
}

// viewManifest is the read-only entry point: lock, fetch, return.
func (s *S3WAL) viewManifest(ctx context.Context, base string) (*s3wal.Manifest, error) {
	unlock := s.lockBase(base)
	defer unlock()
	m, _, err := s.readManifest(ctx, base)
	return m, err
}

// s3walVerdict is a recovery predicate's answer for an ambiguous PUT whose
// writer nonce is not ours (plan §2.3): the write may still have landed
// and been superseded.
type s3walVerdict int

const (
	// s3walRerun re-evaluates the mutation on the fresh snapshot — only
	// legal where re-applying is provably harmless (ID) or where the
	// mutation function itself re-derives the correct outcome (PRED
	// preconditions, CASRef won/lost).
	s3walRerun s3walVerdict = iota
	// s3walApplied concludes our write landed (semantic effect present):
	// the mutation's outputs stand.
	s3walApplied
	// s3walFailClosed surfaces the ambiguity as a retryable error — a
	// non-idempotent mutation is never blindly re-applied (OPS class).
	s3walFailClosed
)

// s3walMutation drives one read-mutate-CAS operation.
type s3walMutation struct {
	// op names the operation for errors.
	op string
	// fn mutates m in place using clk as the write-time stamp for every
	// in-body timestamp, returning apply=false to finish without a CAS
	// (read-only verdicts, no-op retries) or an error to abort (the
	// mutated snapshot is discarded — nothing was written). fn may run
	// several times (fresh snapshot each time) and must reset its output
	// closure variables at the top of every run.
	fn func(m *s3wal.Manifest, clk time.Time) (apply bool, err error)
	// grows marks mutations that ADD entries: the manifest hard size cap
	// refuses them (soft cap warns); shrinking mutations always pass.
	grows bool
	// recover classifies a fresh post-ambiguity snapshot (nil = s3walRerun,
	// the idempotent-re-apply fallback).
	recover func(m *s3wal.Manifest) s3walVerdict
}

// mutate runs one read-mutate-CAS loop with the §2.3 recovery discipline:
// 412 lost races re-read and re-evaluate under a full-jitter backoff; 409
// contention retries live inside the conditional client; ambiguous PUTs
// re-read and resolve via writer nonce, then the per-method recovery
// class; ErrNotExecuted retries the same attempt.
func (s *S3WAL) mutate(ctx context.Context, base string, mu s3walMutation) error {
	assertf(mu.fn != nil && mu.op != "", "mutate: incomplete mutation %+v", mu)
	unlock := s.lockBase(base)
	defer unlock()
	key := s3wal.ManifestKey(base)
	for attempt := 0; attempt < s.casAttempts; attempt++ {
		m, etag, err := s.readManifest(ctx, base)
		if err != nil {
			return err
		}
		clk := s3wal.ClkNext(s.now(), m.Clk)
		apply, err := mu.fn(m, clk)
		if err != nil {
			return err
		}
		if !apply {
			return nil
		}
		body, err := m.EncodeAdvance(clk)
		if err != nil {
			return fmt.Errorf("s3wal %s %s: %w", mu.op, base, err)
		}
		if err := s.checkManifestSize(base, mu, len(body)); err != nil {
			return err
		}
		ourNonce, ourSeq := m.Writer, m.Seq
		var werr error
		if etag == "" {
			_, werr = s.cas.Create(ctx, key, body)
		} else {
			_, werr = s.cas.Advance(ctx, key, body, etag)
		}
		if werr == nil {
			s.raiseSeq(key, ourSeq)
			return nil
		}
		switch {
		case errors.Is(werr, s3wal.ErrExists), errors.Is(werr, s3wal.ErrLostRace):
			// Definitively not executed: someone else advanced first.
			// Re-read and re-evaluate on the fresh snapshot.
			if err := s.sleepJitter(ctx, attempt); err != nil {
				return fmt.Errorf("s3wal %s %s: %w", mu.op, base, notExecuted(err))
			}
			continue
		case errors.Is(werr, s3wal.ErrNotExecuted):
			// Provably never sent — plain retry.
			continue
		}
		var amb *s3wal.AmbiguousError
		if !errors.As(werr, &amb) {
			// Definitive non-conditional failure (auth, bucket, …) or
			// exhausted 409 contention: surface.
			return fmt.Errorf("s3wal %s %s: %w", mu.op, base, werr)
		}
		// Ambiguous: the write MAY have landed. Re-read and resolve.
		fresh, _, rerr := s.readManifest(ctx, base)
		if rerr != nil {
			// Recovery read failed: stay ambiguous (fail closed).
			return fmt.Errorf("s3wal %s %s: %w (recovery read failed: %v)", mu.op, base, werr, rerr)
		}
		if fresh.Writer == ourNonce {
			assertf(fresh.Seq == ourSeq, "s3wal %s %s: our nonce at foreign seq %d (wrote %d)", mu.op, base, fresh.Seq, ourSeq)
			s.raiseSeq(key, ourSeq)
			return nil // our write landed and is the latest
		}
		verdict := s3walRerun
		if mu.recover != nil {
			verdict = mu.recover(fresh)
		}
		switch verdict {
		case s3walApplied:
			return nil
		case s3walFailClosed:
			return fmt.Errorf("s3wal %s %s: %w", mu.op, base, werr)
		default:
			continue // re-run fn on a fresh snapshot
		}
	}
	return fmt.Errorf("s3wal %s %s: %d attempts: %w", mu.op, base, s.casAttempts, s3wal.ErrCASContention)
}

// checkManifestSize enforces the §2.2 growth thresholds on growing
// mutations and soft-warns once per base past the warn threshold.
func (s *S3WAL) checkManifestSize(base string, mu s3walMutation, size int) error {
	assertf(size > 0, "checkManifestSize: non-positive size %d", size)
	if !mu.grows {
		return nil
	}
	if size > s.hardCapBytes {
		return fmt.Errorf("s3wal %s %s: manifest would grow to %d bytes (cap %d): refusing new registrations", mu.op, base, size, s.hardCapBytes)
	}
	if size > s.softWarnBytes {
		s.warnMu.Lock()
		warnedBefore := s.warned[base]
		s.warned[base] = true
		s.warnMu.Unlock()
		if !warnedBefore {
			slog.Warn("s3wal manifest approaching size cap", "base", base, "bytes", size, "warn", s.softWarnBytes, "cap", s.hardCapBytes)
		}
	}
	return nil
}

// newWallDrift anchors the §2.4 wall-vs-monotonic drift detector: a wall
// clock jump (settimeofday, an NTP step, a VM resume) moves the wall
// reading without moving the monotonic one, so the difference between the
// two elapsed readings since the anchor exposes the jump. The anchor is
// process-lifetime by design — a legitimate wall step (e.g. host
// suspend/resume, where the monotonic clock pauses too) keeps reporting
// as drift until restart, which is over-retention, the only failure
// direction §2.4 permits, and every demoted verdict logs loudly.
func newWallDrift() func() time.Duration {
	anchor := time.Now() // carries both wall and monotonic readings
	return func() time.Duration {
		n := time.Now()
		wall := n.Round(0).Sub(anchor.Round(0)) // wall arithmetic (monotonic stripped)
		return wall - n.Sub(anchor)             // minus monotonic elapsed
	}
}

// sleepJitter blocks for the §2.3 full-jitter lost-race backoff:
// rand(0, min(cap, base·2^attempt)), ctx-bounded.
func (s *S3WAL) sleepJitter(ctx context.Context, attempt int) error {
	assertf(attempt >= 0 && attempt < 63, "sleepJitter: attempt %d out of range", attempt)
	ceiling := min(s.backoffCap, s.backoffBase<<uint(attempt))
	assertf(ceiling > 0, "sleepJitter: non-positive backoff ceiling %v", ceiling)
	timer := time.NewTimer(rand.N(ceiling))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// ---------------------------------------------------------------------------
// Refs.

// ListRefs returns all refs for repo as name -> sha.
func (s *S3WAL) ListRefs(ctx context.Context, repo string) (map[string]string, error) {
	m, err := s.viewManifest(ctx, baseRepoOf(repo))
	if err != nil {
		return nil, err
	}
	refs := make(map[string]string)
	if mem := m.Member(repo); mem != nil {
		for name, r := range mem.Refs {
			assertf(ValidSHA(r.SHA), "manifest holds malformed sha %q for %s/%s", r.SHA, repo, name)
			refs[name] = r.SHA
		}
	}
	return refs, nil
}

// ListRefsWithMeta returns all refs for repo sorted by name ascending.
func (s *S3WAL) ListRefsWithMeta(ctx context.Context, repo string) ([]RefInfo, error) {
	m, err := s.viewManifest(ctx, baseRepoOf(repo))
	if err != nil {
		return nil, err
	}
	var refs []RefInfo
	if mem := m.Member(repo); mem != nil {
		for name, r := range mem.Refs {
			assertf(ValidSHA(r.SHA), "manifest holds malformed sha %q for %s/%s", r.SHA, repo, name)
			refs = append(refs, RefInfo{Name: name, SHA: r.SHA, CreatedAt: r.CreatedAt.UTC()})
		}
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].Name < refs[j].Name })
	return refs, nil
}

// ListRefsAfter pages repo's refs in ascending byte order above the
// exclusive cursor (issue #119). The whole set is already in hand (one
// manifest read), so paging is in-memory — the contract (byte order,
// exclusive cursor, short page ends the scan) is preserved.
func (s *S3WAL) ListRefsAfter(ctx context.Context, repo, after string, limit int) ([]RefEntry, error) {
	assertf(repo != "", "ListRefsAfter: empty repo")
	assertf(limit > 0, "ListRefsAfter: non-positive limit %d", limit)
	m, err := s.viewManifest(ctx, baseRepoOf(repo))
	if err != nil {
		return nil, err
	}
	mem := m.Member(repo)
	if mem == nil {
		return nil, nil
	}
	names := make([]string, 0, len(mem.Refs))
	for name := range mem.Refs {
		if name > after {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	if len(names) > limit {
		names = names[:limit]
	}
	refs := make([]RefEntry, 0, len(names))
	for _, name := range names {
		sha := mem.Refs[name].SHA
		assertf(ValidSHA(sha), "manifest holds malformed sha %q for %s/%s", sha, repo, name)
		refs = append(refs, RefEntry{Name: name, SHA: sha})
	}
	return refs, nil
}

// GetRef returns the current sha of one ref, or ErrNotFound.
func (s *S3WAL) GetRef(ctx context.Context, repo, name string) (string, error) {
	assertf(repo != "" && name != "", "GetRef: empty repo %q or name %q", repo, name)
	m, err := s.viewManifest(ctx, baseRepoOf(repo))
	if err != nil {
		return "", err
	}
	mem := m.Member(repo)
	if mem == nil {
		return "", ErrNotFound
	}
	r, ok := mem.Refs[name]
	if !ok {
		return "", ErrNotFound
	}
	assertf(ValidSHA(r.SHA), "manifest holds malformed sha %q for %s/%s", r.SHA, repo, name)
	return r.SHA, nil
}

// CASRef performs the compare-and-swap ref update (see RefStore). The
// purge fence and the sync-claim fence are fields of the SAME object the
// CAS advances, so both checks are same-atom with the swap — the total
// order PG buys with lockBaseLedger is inherent here (plan §1).
//
// Recovery class CAS (plan Appendix A): a lost race re-reads and
// re-evaluates — a fresh snapshot no longer matching old is won=false,
// never retried. An ambiguous PUT whose writer nonce is not ours and whose
// fresh snapshot no longer satisfies the precondition surfaces the
// ambiguity as an error: exactly like a lost-commit-response on SQL, the
// IdempotentCAS decorator's recheck (current == new) recovers the win.
func (s *S3WAL) CASRef(ctx context.Context, repo, name, old, new string) (won bool, err error) {
	assertf(ValidSHA(old), "CASRef: malformed old sha %q", old)
	assertf(ValidSHA(new), "CASRef: malformed new sha %q", new)
	assertf(old != ZeroSHA || new != ZeroSHA, "CASRef: zero -> zero update for %s/%s", repo, name)
	fence, fenced := casFence(ctx, repo)
	// matches reports whether the precondition holds on m — the shared
	// text of the mutation and its ambiguity recovery predicate.
	matches := func(m *s3wal.Manifest) bool {
		if fenced && (m.SyncClaim == nil || m.SyncClaim.Holder != fence.holder) {
			return false // fence fails CLOSED (issue #86)
		}
		mem := m.Member(repo)
		var cur string
		if mem != nil {
			if r, ok := mem.Refs[name]; ok {
				cur = r.SHA
			}
		}
		if old == ZeroSHA {
			return cur == "" // create: must not exist
		}
		return cur == old
	}
	err = s.mutate(ctx, baseRepoOf(repo), s3walMutation{
		op:    "cas ref",
		grows: old == ZeroSHA,
		fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
			won = false
			if m.PurgedAt != nil {
				// Same taxonomy as SQL (issue #52): the statement
				// provably never executed, so IdempotentCAS must not
				// recover it.
				return false, fmt.Errorf("cas %s/%s: %w", repo, name, notExecuted(ErrRepoPurged))
			}
			if !matches(m) {
				return false, nil // lost cleanly: won=false, no error
			}
			mem := m.EnsureMember(repo)
			switch {
			case old == ZeroSHA: // create
				if mem.Refs == nil {
					mem.Refs = make(map[string]s3wal.RefRecord)
				}
				// created_at is set here once; SHA updates never touch it.
				mem.Refs[name] = s3wal.RefRecord{SHA: new, CreatedAt: clk}
			case new == ZeroSHA: // delete
				delete(mem.Refs, name)
			default: // update preserves created_at
				r := mem.Refs[name]
				r.SHA = new
				mem.Refs[name] = r
			}
			mem.Gen++ // one changed row = one bump (the trigger contract)
			won = true
			return true, nil
		},
		recover: func(fresh *s3wal.Manifest) s3walVerdict {
			if matches(fresh) {
				return s3walRerun // our write did not land: retry the swap
			}
			// The precondition no longer holds and the last writer is not
			// us: our write may have landed and been superseded. Surface
			// the ambiguity (IdempotentCAS rechecks current == new).
			return s3walFailClosed
		},
	})
	if err != nil {
		return false, err
	}
	return won, nil
}

// ---------------------------------------------------------------------------
// Pack registry.

// RegisterPacks records packs atomically (see RefStore): pack-set insert,
// reservation consumption, retained-reregistration credit rule, usage
// credit, adoption-gen bump, and per-row generation bumps are ONE CAS.
// Recovery class PRED/ID: re-applying on a fresh snapshot is the SQL
// upsert contract (existing rows skip, reservations already gone consume
// nothing, no double credit).
func (s *S3WAL) RegisterPacks(ctx context.Context, repo string, packs []Pack) error {
	assertf(len(packs) > 0, "RegisterPacks called with no packs")
	return s.mutate(ctx, baseRepoOf(repo), s3walMutation{
		op:    "register packs",
		grows: true,
		fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
			if m.PurgedAt != nil {
				return false, fmt.Errorf("register packs %s: %w", repo, ErrRepoPurged)
			}
			changed := false
			var credit int64
			adopted := false
			mem := m.Member(repo)
			for _, p := range packs {
				assertf(p.ID != "" && p.Key != "", "RegisterPacks: empty pack id/key %+v", p)
				if mem == nil || !s3walHasPackID(mem, p.ID) {
					if mem == nil {
						mem = m.EnsureMember(repo)
					}
					mem.Packs = append(mem.Packs, s3wal.PackRecord{ID: p.ID, Key: p.Key, Bytes: p.Size, CreatedAt: clk})
					mem.Gen++ // per inserted row, like the SQL triggers
					c := s3walRegisterLedger(mem, p)
					credit += c
					adopted = adopted || (p.Adopted && c > 0)
					changed = true
				}
				// Settlement consumes the key's reservation regardless of
				// insertion: its bytes are counter-accounted now.
				if _, ok := m.Reservations[p.Key]; ok {
					delete(m.Reservations, p.Key)
					changed = true
				}
			}
			if adopted {
				// Atomic with the credit (Codex round 53): cached floors
				// listing these bytes must re-scan.
				m.LFS.AdoptionGen++
			}
			assertf(credit >= 0, "register packs %s: negative credit %d", repo, credit)
			m.UsageBytes += credit
			return changed, nil
		},
	})
}

// s3walHasPackID reports whether mem holds a pack row with id.
func s3walHasPackID(mem *s3wal.Member, id string) bool {
	for _, p := range mem.Packs {
		if p.ID == id {
			return true
		}
	}
	return false
}

// s3walRegisterLedger applies the retained-reregistration credit rule for
// one newly inserted pack row (the registerLedgerPG twin): a retained key
// stops being retained and credits nothing (its bytes never left the
// counter); otherwise the row's size is the credit.
func s3walRegisterLedger(mem *s3wal.Member, p Pack) int64 {
	if _, ok := mem.Retained[p.Key]; ok {
		delete(mem.Retained, p.Key)
		return 0
	}
	return p.Size
}

// ListPacks returns all registered packs for repo, oldest first (byte
// order of id breaks CreatedAt ties, mirroring ORDER BY created_at, id).
func (s *S3WAL) ListPacks(ctx context.Context, repo string) ([]Pack, error) {
	m, err := s.viewManifest(ctx, baseRepoOf(repo))
	if err != nil {
		return nil, err
	}
	mem := m.Member(repo)
	if mem == nil {
		return nil, nil
	}
	recs := append([]s3wal.PackRecord(nil), mem.Packs...)
	sort.Slice(recs, func(i, j int) bool {
		if !recs[i].CreatedAt.Equal(recs[j].CreatedAt) {
			return recs[i].CreatedAt.Before(recs[j].CreatedAt)
		}
		return recs[i].ID < recs[j].ID
	})
	packs := make([]Pack, 0, len(recs))
	for _, r := range recs {
		// Size is accounting input, not a listed column (see Pack).
		packs = append(packs, Pack{ID: r.ID, Key: r.Key})
	}
	return packs, nil
}

// HasPack reports whether repo has a registered pack with blob key.
func (s *S3WAL) HasPack(ctx context.Context, repo, key string) (bool, error) {
	assertf(repo != "" && key != "", "HasPack(%q, %q)", repo, key)
	m, err := s.viewManifest(ctx, baseRepoOf(repo))
	if err != nil {
		return false, err
	}
	mem := m.Member(repo)
	if mem == nil {
		return false, nil
	}
	for _, p := range mem.Packs {
		if p.Key == key {
			return true, nil
		}
	}
	return false, nil
}

// PackRegisteredBytes returns the registered pack's recorded size — see
// RefStore (0 = size unknown).
func (s *S3WAL) PackRegisteredBytes(ctx context.Context, repo, key string) (int64, bool, error) {
	assertf(repo != "" && key != "", "PackRegisteredBytes(%q, %q)", repo, key)
	m, err := s.viewManifest(ctx, baseRepoOf(repo))
	if err != nil {
		return 0, false, err
	}
	mem := m.Member(repo)
	if mem == nil {
		return 0, false, nil
	}
	for _, p := range mem.Packs {
		if p.Key == key {
			return p.Bytes, true, nil
		}
	}
	return 0, false, nil
}

// ReplacePacks installs a repack fold in one CAS (see RefStore): the
// precondition — every old row still present — is evaluated on the read
// snapshot and enforced by the CAS itself. Recovery class PRED: fold
// present on the fresh snapshot ⇒ applied; old set fully intact ⇒ our
// write never landed (re-run); anything else is inconclusive and fails
// closed (a false loss would make the caller delete a REGISTERED fold
// blob).
func (s *S3WAL) ReplacePacks(ctx context.Context, repo string, old []Pack, newPack Pack) (won bool, err error) {
	validateReplacePacks(repo, old, newPack)
	oldByID := make(map[string]bool, len(old))
	for _, p := range old {
		oldByID[p.ID] = true
	}
	allOldPresent := func(m *s3wal.Manifest) bool {
		mem := m.Member(repo)
		if mem == nil {
			return false
		}
		n := 0
		for _, p := range mem.Packs {
			if oldByID[p.ID] {
				n++
			}
		}
		return n == len(old)
	}
	err = s.mutate(ctx, baseRepoOf(repo), s3walMutation{
		op:    "replace packs",
		grows: true,
		fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
			won = false
			if !allOldPresent(m) {
				return false, nil // another fold got here first: clean loss
			}
			mem := m.Member(repo)
			// Delete exactly the old rows (per-row generation bumps).
			kept := mem.Packs[:0]
			for _, p := range mem.Packs {
				if oldByID[p.ID] {
					mem.Gen++
					continue
				}
				kept = append(kept, p)
			}
			mem.Packs = kept
			// Retained tombstones precede the fold insert on purpose: an
			// identity fold consumes its own retained row below, leaving
			// the counter untouched (issue #31).
			if mem.Retained == nil {
				mem.Retained = make(map[string]s3wal.RetainedPack)
			}
			for _, p := range old {
				mem.Retained[p.Key] = s3wal.RetainedPack{Bytes: p.Size, SupersededAt: clk}
			}
			if s3walHasPackID(mem, newPack.ID) {
				won = true
				return true, nil // fold row already registered: bytes counted
			}
			mem.Packs = append(mem.Packs, s3wal.PackRecord{ID: newPack.ID, Key: newPack.Key, Bytes: newPack.Size, CreatedAt: clk})
			mem.Gen++
			m.UsageBytes += s3walRegisterLedger(mem, newPack)
			won = true
			return true, nil
		},
		recover: func(fresh *s3wal.Manifest) s3walVerdict {
			if mem := fresh.Member(repo); mem != nil && s3walHasPackID(mem, newPack.ID) && !allOldPresent(fresh) {
				won = true
				return s3walApplied
			}
			if allOldPresent(fresh) {
				return s3walRerun // our write did not land: the fold retries
			}
			return s3walFailClosed
		},
	})
	if err != nil {
		return false, fmt.Errorf("replace packs %s: %w", repo, err)
	}
	return won, nil
}

// GetGeneration returns repo's mutation generation (0 = never mutated).
// s3wal has no schema triggers; the driver bumps member generations inside
// every CAS that changes the member's refs or packs rows — same
// invalidation-token contract, same per-changed-row granularity.
func (s *S3WAL) GetGeneration(ctx context.Context, repo string) (int64, error) {
	assertf(repo != "", "GetGeneration: empty repo")
	m, err := s.viewManifest(ctx, baseRepoOf(repo))
	if err != nil {
		return 0, err
	}
	mem := m.Member(repo)
	if mem == nil {
		return 0, nil
	}
	assertf(mem.Gen >= 0, "manifest holds negative generation %d for %s", mem.Gen, repo)
	return mem.Gen, nil
}
