package githttp

// Per-repo pack-bytes quota (issue #21). Pre-receive uploads and registers
// packs BEFORE the update hook applies ref policy, so without a quota even
// rejected pushes grow packs/ durably.
//
// Enforcement lives exclusively in the pre-receive hook (hook.go): it alone
// sees the quarantined packs' content-addressed keys, so it alone can
// distinguish a quota-breaching push from a zero-growth one — delete-only
// pushes, refs to already-stored objects, and crash-window retries whose
// identical packs are already published and/or registered (Codex review
// rounds 3–6 each found a zero-growth case that an HTTP-time rejection
// would wedge). Denying in the hook leaves the quarantine for git to
// discard: nothing is published or registered, and the client sees the
// denial via the sideband channel. The HTTP layer contributes the cached
// quota-pressure log line (packUsageCached) — the server-side
// observability signal — and never rejects on quota.
//
// Usage feeding the admission decision has two sources (issue #29,
// ref/ledger.go): the repo's pack-prefix listing (registered packs plus
// not-yet-swept orphans and retained superseded blobs — exactly the packs/
// growth the quota bounds) serves as the FLOOR, and the ref store's
// maintained usage counter plus in-flight reservations close the
// concurrent-push overshoot the listing alone admitted: admission is a
// durable reservation committed before upload and consumed at
// registration, serialized per base repo across nodes and replicas.
// Residual overshoot is bounded and documented in ref/ledger.go (TTL-
// expired in-flight reservations; orphaned uploads seen only by the next
// listing).
// Namespaced repos (`+ephemeral`/`+import`) keep separate pack PREFIXES but
// share the base repository's quota: blob.StorageUsageBytes rolls all of a
// repo's namespaces up, because auth strips the suffix and one token must
// not obtain a fresh quota per namespace (Codex round 15).

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/coder/code-storage/internal/blob"
	"github.com/coder/code-storage/internal/ref"
)

// EnvPackQuota carries the resolved per-repo pack-bytes quota from the serve
// process into the pre-receive hook (set only when the quota is enabled).
const EnvPackQuota = "CODESTORAGE_GIT_PACK_QUOTA"

// EnvPackReservationTTL carries the resolved reservation TTL (issue #29, a
// Go duration string) from the serve process into the pre-receive hook.
// Set alongside EnvPackQuota; the hook falls back to
// defaultReservationTTL when it is absent (a serve binary predating the
// ledger), so quota enforcement never silently weakens on skew.
const EnvPackReservationTTL = "CODESTORAGE_GIT_PACK_RESERVATION_TTL"

// EnvUsageFloor carries the repo's UNCOUNTED-bytes admission floor (the
// packs + LFS listing minus the usage counter at scan start — Codex round
// 44) from the serve process's shared single-flight cache into the
// pre-receive hook (Codex round 21): the hook is a fresh process per
// push, so a hook-side listing cannot be cached across pushes and
// re-scans both prefixes every time — ~100 pages per push at the LFS
// object cap. Set alongside EnvPackQuota as "<bytes>:<unix mint time>"
// (Codex round 30): the value is minted at receive-pack spawn but the
// hook runs only after the client's transfer, whose duration the CLIENT
// controls, so the hook self-lists when the floor has aged past
// hookFloorMaxAge instead of trusting it — beyond the reservation TTL,
// orphaned uploads whose reservations expired are visible only to a
// listing.
const EnvUsageFloor = "CODESTORAGE_GIT_USAGE_FLOOR"

// hookFloorMaxAge bounds how old a server-minted usage floor may be
// before the hook self-lists instead (a var for tests). Five minutes
// passes every legitimate push comfortably while staying far under any
// sane reservation TTL (which must already exceed the longest plausible
// upload); a deliberately slow push exceeding it pays one self-listing —
// no amplification, since the client spends over five minutes per
// listing it triggers. The EFFECTIVE bound also never reaches the
// configured reservation TTL (Codex round 32): an orphan published after
// the mint stops being counted by the ledger once its reservation
// expires, so the floor must be young enough to have seen it — and the
// minted value may itself be one usageCacheTTL stale, so the trusted
// mint-to-hook age is capped at TTL − usageCacheTTL (operators may
// configure TTLs below five minutes).
var hookFloorMaxAge = 5 * time.Minute

// defaultReservationTTL bounds how long a crashed push's reservation keeps
// gating admission (ref/ledger.go). It must comfortably exceed the longest
// plausible server-side pack upload — the reservation is taken BEFORE the
// upload and consumed at registration — because an expiring in-flight
// reservation reopens exactly the overshoot the ledger closes. One hour
// clears a 10 GiB (default-quota-sized) upload at ~3 MB/s.
const defaultReservationTTL = time.Hour

// reservationTTL resolves the reservation TTL the hook charges and stamps.
// Malformed values fail closed (deny the push) like a malformed quota: the
// env comes from our own server, so nonsense means misconfiguration, not
// client input.
func reservationTTL() (time.Duration, error) {
	raw := os.Getenv(EnvPackReservationTTL)
	if raw == "" {
		return defaultReservationTTL, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("malformed %s=%q", EnvPackReservationTTL, raw)
	}
	return d, nil
}

// usageCacheTTL bounds how often the HTTP-layer pressure check lists one
// repo's pack prefix (Codex round 2): without it, every receive-pack POST —
// cheap for the client, before any body is read — would cost an
// O(pack-count) listing against the shared blob backend, an amplification
// vector for a git:write token holder replaying empty POSTs. The check is
// purely observational (pre-receive owns enforcement), so a TTL-stale value
// only delays the pressure log line by at most this long.
const usageCacheTTL = 30 * time.Second

// UsageFloorHandoffMargin is the freshness headroom the reservation TTL
// must leave BEYOND the floor cache lifetime (Codex round 42 on #51): a
// floor can be served at the very end of its usageCacheTTL window and
// then sit while ReservePackBytes waits on the database, so the interval
// from scan start to the admission snapshot is floor age plus handoff
// time. Charging reserve calls are deadline-bounded by this margin, which
// makes the chain airtight: an orphan published after its page was
// scanned (publish p > scan start t0) had its reservation refreshed at
// the pre-publish admission, so it gates until p+TTL, while any admission
// using a floor from t0 snapshots by t0 + usageCacheTTL + margin < p+TTL.
// The reservation stamps and the snapshot share the DATABASE clock, so
// only the duration — not absolute clocks — must agree.
const UsageFloorHandoffMargin = 30 * time.Second

// MinPackReservationTTL is the exclusive lower bound serve's config
// enforces on CODESTORAGE_PACK_RESERVATION_TTL (Codex rounds 36+42 on
// #51): the reservation TTL must STRICTLY exceed the usage-floor cache
// lifetime plus the floor-to-reservation handoff margin. CachedUsageFloor
// feeds LFS and REST admission directly with a listing up to
// usageCacheTTL stale, and the subsequent reserve call takes up to the
// margin; with a TTL at or below their sum, an orphaned upload's
// reservation (published, registration failed) can expire while a floor
// listed BEFORE the publish still drives an admission — the writer then
// sees neither the orphaned bytes nor a live reservation and is admitted
// into headroom that no longer exists. The hook path tolerates such TTLs
// by self-listing (hookUsageFloor's maxAge goes non-positive), but the
// direct cache consumers have no fresher source, so the configuration is
// rejected at startup instead.
const MinPackReservationTTL = usageCacheTTL + UsageFloorHandoffMargin

// usageCachePruneInterval is how often inserts drop expired entries,
// bounding memory on nodes serving many repos. Time-based like the rate
// limiter's sweepLocked (Codex round 8): a size trigger would walk the
// whole map under the mutex on EVERY insert while high-cardinality traffic
// keeps the map above the threshold — quadratic in active repos. One sweep
// per TTL removes everything removable (entries expire after exactly
// usageCacheTTL), so the map is bounded by one TTL window of active repos.
const usageCachePruneInterval = usageCacheTTL

// usageListTimeout bounds one coalesced usage listing. The flight runs on a
// context detached from the leader request (mirroring blob.EnsurePublished):
// its result serves every coalesced waiter, so the leader hanging up must
// not poison the followers with a cancellation.
const usageListTimeout = time.Minute

// usageRefreshSlots bounds concurrent background usage listings node-wide
// (Codex round 10): coalescing is keyed per repo, so high-cardinality
// traffic could otherwise fan out one listing goroutine per repo against
// the shared blob backend. A full pool skips the refresh — the check is
// observational, so the only cost is a delayed pressure line until a later
// request finds a free slot.
var usageRefreshSlots = make(chan struct{}, 4)

// usageCache memoizes per-repo pack usage for the pressure check. The zero
// value is ready to use. Cold and expired lookups coalesce into one
// in-flight listing per repo (Codex round 4): without that, concurrent
// cheap POSTs racing a cold entry would each run a full O(pack-count) scan,
// reopening the amplification the cache exists to close. Failed flights are
// not cached; each new request elects a new leader.
type usageCache struct {
	now func() time.Time // injectable clock for deterministic tests

	mu        sync.Mutex
	entries   map[string]usageEntry
	inflight  map[string]*usageFlight
	lastPrune time.Time
}

type usageEntry struct {
	usage       int64 // raw listing bytes (the pressure log's view)
	counter     int64 // usage counter at scan start (Codex round 44)
	adoptionGen int64 // shared adoption generation at scan start (round 49)
	expires     time.Time
}

// uncounted is the entry's admission floor: physical bytes the usage
// counter did not cover at scan start (pre-registry LFS objects, orphans;
// Codex round 44). ReservePackBytes SUMS it with the live counter, so
// settlements after the scan are counted exactly once.
func (e usageEntry) uncounted() int64 {
	return max(0, e.usage-e.counter)
}

// usageFlight is one in-progress listing; done is closed after
// usage/err/expires are set, releasing every coalesced waiter. expires is
// the scan-start-anchored freshness bound (Codex round 41): waiters must
// not admit against a result whose scan outlived it. waiters/started/
// aborted are guarded by the cache mutex; abort is closed when the last
// admission waiter departs before the flight acquired its slot, so
// abandoned flights exit without ever listing (round 41: slots bound only
// ACTIVE listings — without cancellation, one parked goroutine and
// inflight entry per repository would survive client disconnects and
// eventually run a detached listing nobody consumes).
type usageFlight struct {
	done        chan struct{}
	usage       int64 // raw listing bytes (pressure log)
	counter     int64 // usage counter at scan start (Codex round 44)
	adoptionGen int64 // shared adoption generation at scan start (round 49)
	err         error
	expires     time.Time

	abort   chan struct{}
	waiters int
	started bool
	aborted bool
}

func (c *usageCache) clockNow() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

// putLocked stores repo's usage for usageCacheTTL measured from scanStart
// — the moment its listing BEGAN, not completed (Codex round 40): a scan
// approaching usageListTimeout otherwise yields an entry whose true age
// exceeds the recorded expiry by the whole scan duration, undoing the
// MinPackReservationTTL guarantee for orphans published after their page
// was scanned. A scan that outlives the TTL stores a dead-on-arrival
// entry, and the flight's own waiters reject the result too (round 41 —
// usageFlight.expires). Drops expired entries at most once per
// usageCachePruneInterval. Callers hold c.mu.
func (c *usageCache) putLocked(repo string, usage, counter, adoptionGen int64, scanStart time.Time) {
	assertf(repo != "" && usage >= 0 && counter >= 0 && adoptionGen >= 0 && !scanStart.IsZero(),
		"usageCache.putLocked(%q, %d, %d, %d, %v)", repo, usage, counter, adoptionGen, scanStart)
	now := c.clockNow()
	if c.entries == nil {
		c.entries = make(map[string]usageEntry)
		c.lastPrune = now
	} else if now.Sub(c.lastPrune) >= usageCachePruneInterval {
		c.lastPrune = now
		for k, e := range c.entries {
			if now.After(e.expires) {
				delete(c.entries, k)
			}
		}
	}
	c.entries[repo] = usageEntry{usage: usage, counter: counter, adoptionGen: adoptionGen, expires: scanStart.Add(usageCacheTTL)}
}

// invalidateUsageFloor drops repo's cached floor entry (Codex round 48):
// adopting a pre-registry LFS object moves bytes a cached scan already
// listed into the usage counter, so entries scanned before the adoption
// would double-count them in the floor+counter sum until expiry — the
// false-denial side. Verified NEW uploads need no invalidation: their
// bytes were published after any older scan, so no cached floor contains
// them.
func (s *Server) invalidateUsageFloor(repo string) {
	assertf(repo != "", "invalidateUsageFloor: empty repo")
	c := &s.usage
	c.mu.Lock()
	delete(c.entries, repo)
	c.mu.Unlock()
}

// peekUsage returns repo's cached pack usage without ever blocking the
// request path (Codex round 9: a stalled blob backend must not delay
// receive-pack behind a synchronous listing — the check is observational,
// and pre-receive enforces without it). ok=false means no fresh value; a
// cold or expired entry triggers one asynchronous, coalesced refresh whose
// result serves later peeks.
func (s *Server) peekUsage(repo string) (usage int64, ok bool) {
	assertf(repo != "", "peekUsage: empty repo")
	c := &s.usage
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, entryOK := c.entries[repo]; entryOK && !c.clockNow().After(e.expires) {
		return e.usage, true
	}
	if _, refreshing := c.inflight[repo]; !refreshing {
		select {
		case usageRefreshSlots <- struct{}{}:
			// started: the slot is already held, so a departing
			// admission waiter must not abort this flight.
			f := &usageFlight{done: make(chan struct{}), abort: make(chan struct{}), started: true}
			if c.inflight == nil {
				c.inflight = make(map[string]*usageFlight)
			}
			c.inflight[repo] = f
			slots := usageRefreshSlots // captured now: tests swap the var
			go func() {
				defer func() { <-slots }()
				s.refreshUsage(repo, f)
			}()
		default:
			// Pool exhausted: skip; a later peek retries the refresh.
			// (Observational path only — admission's CachedUsageFloor
			// instead parks its flight on the slot, because it needs a
			// value.)
		}
	}
	return 0, false
}

// refreshUsage runs one usage flight for repo and publishes the result.
// It owns the c.inflight[repo] slot the caller created; errors are logged
// and not cached, so the next peek elects a new flight. The cached measure
// is the FULL storage usage (packs + LFS): the LFS admission floor needs
// the LFS prefix, and the git-surface pressure log is observational either
// way.
func (s *Server) refreshUsage(repo string, f *usageFlight) {
	assertf(repo != "" && f != nil, "refreshUsage: missing repo or flight")
	scanStart := s.usage.clockNow() // entry age counts from HERE (round 40)
	ctx, cancel := context.WithTimeout(context.Background(), usageListTimeout)
	defer cancel()
	// The usage counter is read BEFORE the listing begins (Codex round
	// 44) — the subtraction that yields the uncounted floor is only
	// conservative when the counter snapshot cannot contain bytes the
	// listing missed: a registration committing mid-scan lands in the
	// LIVE counter admission reads, never silently vanishes from both.
	// A missing Store (bare test servers; NewServer asserts it in
	// production) degrades to counter 0 — the floor then counts
	// everything listed, the deny-safe side.
	var counter, adoptionGen int64
	if s.Store != nil {
		// The adoption generation is read FIRST (Codex round 49): an
		// adoption committing after this read is caught by the serve-time
		// generation check, so no ordering can hide one.
		gen, gerr := s.Store.LFSAdoptionGen(ctx, BaseRepo(repo))
		if gerr != nil {
			s.finishUsageFlight(repo, f, 0, 0, 0, scanStart, fmt.Errorf("adoption gen read: %w", gerr))
			return
		}
		adoptionGen = gen
		ls, lerr := s.Store.QuotaLedgerState(ctx, blob.QuotaRepoGroup(repo), time.Hour)
		if lerr != nil {
			s.finishUsageFlight(repo, f, 0, 0, 0, scanStart, fmt.Errorf("usage counter read: %w", lerr))
			return
		}
		counter = ls.UsageBytes
	}
	usage, err := blob.StorageUsageBytes(ctx, s.Blob, repo)
	s.finishUsageFlight(repo, f, usage, counter, adoptionGen, scanStart, err)
}

// finishUsageFlight publishes one flight outcome: cache insert on
// success, inflight removal, and waiter release.
func (s *Server) finishUsageFlight(repo string, f *usageFlight, usage, counter, adoptionGen int64, scanStart time.Time, err error) {
	c := &s.usage
	c.mu.Lock()
	if c.inflight[repo] == f { // an aborted flight may have been replaced
		delete(c.inflight, repo)
	}
	if err == nil {
		c.putLocked(repo, usage, counter, adoptionGen, scanStart)
	}
	c.mu.Unlock()
	if err != nil {
		log.Printf("quota: %s: usage refresh failed: %v", repo, err)
	}
	f.usage, f.counter, f.adoptionGen, f.err, f.expires = usage, counter, adoptionGen, err, scanStart.Add(usageCacheTTL)
	close(f.done)
}

// packUsageBytes sums the sizes of every object under repo's pack AND LFS
// prefixes (blob.StorageUsageBytes, shared with the REST pack writers):
// pre-ledger LFS bytes are invisible to the usage counter until adopted,
// so pack admission must floor on a listing that sees them (issue #51,
// Codex round 18).
func packUsageBytes(ctx context.Context, bl blob.BlobStore, repo string) (int64, error) {
	return blob.StorageUsageBytes(ctx, bl, repo)
}

// hookFloorFreshness bounds one self-listed floor scan's duration — the
// hook-side mirror of the cache's scan-start-anchored expiry (Codex round
// 46): with no cache in front of it, a self-listing that outlives this
// window may miss an orphan whose reservation expires before the hook's
// margin-bounded reserve lands. It is also each attempt's context
// DEADLINE (Codex round 47): a scan that cannot finish inside the window
// is canceled instead of completing uselessly and burning a second full
// scan's backend work on rejection. usageCacheTTL keeps the coverage
// chain identical to the cached path (scanStart + freshness + margin <
// TTL). A var so tests can shrink it.
var hookFloorFreshness = usageCacheTTL

// hookFloorScanSlots bounds concurrent self-listed floor scans PER NODE,
// across processes (Codex round 47): every pre-receive hook is a fresh
// process, so the serve-side usageRefreshSlots pool cannot see it — a
// client slowing pushes past the trusted-floor age on a large repo could
// otherwise fan out one ~100-page scan per throttled push against the
// shared blob backend. flock-based slot files under os.TempDir() (hooks
// inherit the serve process's environment, so all of a node's processes
// agree on the directory) mirror the in-process pool's size; a crashed
// holder releases its slot with its file descriptor. The serve process's
// own scans stay on usageRefreshSlots — two pools of four bound the
// node's total concurrent floor scans at eight.
const hookFloorScanSlots = 4

// acquireHookFloorScanSlot polls the slot files with non-blocking flocks
// until one is taken or ctx expires (the attempt deadline).
func acquireHookFloorScanSlot(ctx context.Context) (unlock func(), err error) {
	for {
		for i := range hookFloorScanSlots {
			path := filepath.Join(os.TempDir(), fmt.Sprintf("codestorage-floor-scan-%d.lock", i))
			f, oerr := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
			if oerr != nil {
				return nil, oerr
			}
			ferr := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
			if ferr == nil {
				return func() { _ = f.Close() }, nil // Close releases the flock
			}
			_ = f.Close()
			if ferr != syscall.EWOULDBLOCK && ferr != syscall.EAGAIN && ferr != syscall.EINTR {
				return nil, ferr
			}
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("floor scan slot: %w", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// hookSelfListFloor is the hook's own uncounted-floor measurement: the
// usage counter is read BEFORE the listing starts (the same conservative
// ordering as refreshUsage — Codex round 44), and the floor is the
// listing minus that counter, clamped at zero. Each attempt is
// slot-gated across processes and deadline-bounded by the freshness
// window (Codex rounds 46+47); one retry covers a transient stall, then
// the push fails closed.
func hookSelfListFloor(ctx context.Context, bl blob.BlobStore, st ref.Store, repo string) (int64, error) {
	assertf(st != nil, "hookSelfListFloor: nil ref store")
	var lastErr error
	for attempt := 0; attempt < 2 && ctx.Err() == nil; attempt++ {
		floor, err := hookFloorScanOnce(ctx, bl, st, repo)
		if err == nil {
			return floor, nil
		}
		lastErr = err
	}
	return 0, fmt.Errorf("usage floor for %s: %w", repo, lastErr)
}

// hookFloorScanOnce runs one slot-gated, deadline-bounded floor scan.
func hookFloorScanOnce(ctx context.Context, bl blob.BlobStore, st ref.Store, repo string) (int64, error) {
	actx, cancel := context.WithTimeout(ctx, hookFloorFreshness)
	defer cancel()
	unlock, err := acquireHookFloorScanSlot(actx)
	if err != nil {
		return 0, err
	}
	defer unlock()
	scanStart := time.Now()
	ls, err := st.QuotaLedgerState(actx, blob.QuotaRepoGroup(repo), time.Hour)
	if err != nil {
		return 0, fmt.Errorf("usage counter read: %w", err)
	}
	listing, err := packUsageBytes(actx, bl, repo)
	if err != nil {
		return 0, err
	}
	// Defensive backstop: the deadline above should preempt any scan
	// this check would reject.
	if time.Since(scanStart) > hookFloorFreshness {
		return 0, fmt.Errorf("listing outlived its %v freshness window", hookFloorFreshness)
	}
	return max(0, listing-ls.UsageBytes), nil
}

// hookUsageFloor resolves the pre-receive admission floor — the
// UNCOUNTED-bytes floor ReservePackBytes sums with the live counter
// (Codex round 44). The serve process mints EnvUsageFloor
// ("<bytes>:<unix mint time>") from its shared single-flight cache (Codex
// round 21 — the hook is a fresh process per push, so hook-side listings
// re-scan the pack AND LFS prefixes on every push). A floor older than
// hookFloorMaxAge is NOT trusted (Codex round 30 — the client controls
// the transfer duration between mint and hook) and the hook self-lists,
// as it does for an absent env (a serve binary predating the minted
// floor) or a bare timestampless value (one predating the mint time):
// skew never weakens enforcement, it only costs that push a listing.
// Malformed values fail closed like a malformed EnvPackQuota: the env
// comes from our own server, so nonsense means misconfiguration, not
// client input.
func hookUsageFloor(ctx context.Context, bl blob.BlobStore, st ref.Store, repo string) (int64, error) {
	raw, ok := os.LookupEnv(EnvUsageFloor)
	if !ok {
		return hookSelfListFloor(ctx, bl, st, repo)
	}
	val, rest, found := strings.Cut(raw, ":")
	if !found {
		if _, err := strconv.ParseInt(raw, 10, 64); err != nil {
			return 0, fmt.Errorf("malformed %s=%q", EnvUsageFloor, raw)
		}
		return hookSelfListFloor(ctx, bl, st, repo) // timestampless legacy floor: no age evidence
	}
	mint, genStr, hasGen := strings.Cut(rest, ":")
	floor, ferr := strconv.ParseInt(val, 10, 64)
	mintUnix, merr := strconv.ParseInt(mint, 10, 64)
	if ferr != nil || merr != nil || floor < 0 {
		return 0, fmt.Errorf("malformed %s=%q", EnvUsageFloor, raw)
	}
	if !hasGen {
		// A serve binary predating the adoption generation (Codex round
		// 49) minted this floor: without generation evidence the hook
		// self-lists, like every other skew case.
		return hookSelfListFloor(ctx, bl, st, repo)
	}
	mintGen, gerr := strconv.ParseInt(genStr, 10, 64)
	if gerr != nil || mintGen < 0 {
		return 0, fmt.Errorf("malformed %s=%q", EnvUsageFloor, raw)
	}
	ttl, err := reservationTTL()
	if err != nil {
		return 0, err // fail closed, matching the reserve path's own use
	}
	// The trusted age leaves the handoff margin too (Codex round 42): the
	// hook's own reserve call runs after this floor resolves, and an
	// orphan's reservation must outlive scan start + trusted age + that
	// call.
	maxAge := min(hookFloorMaxAge, ttl-usageCacheTTL-UsageFloorHandoffMargin)
	if maxAge <= 0 || time.Since(time.Unix(mintUnix, 0)) > maxAge {
		return hookSelfListFloor(ctx, bl, st, repo)
	}
	// A non-zero floor is trusted only while the shared adoption
	// generation still matches its scan (Codex round 49): an adoption on
	// ANY replica during the client's transfer moved listed bytes into
	// the counter, and summing both would double-count them. Zero floors
	// cannot double-count, so they skip the point read. The lookup is
	// bounded by the floor's remaining trusted age and the age check is
	// re-evaluated afterwards (Codex round 50): a database stall here
	// otherwise consumes the window after the check passed.
	if floor > 0 {
		gctx, gcancel := context.WithDeadline(ctx, time.Unix(mintUnix, 0).Add(maxAge))
		live, err := st.LFSAdoptionGen(gctx, BaseRepo(repo))
		gcancel()
		if err != nil {
			// The remaining-age deadline killed the read: the floor's
			// window is consumed — self-list like every other
			// untrustworthy-floor case. Other failures stay closed.
			if !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
				return 0, fmt.Errorf("adoption gen read: %w", err)
			}
			return hookSelfListFloor(ctx, bl, st, repo)
		}
		if live != mintGen || time.Since(time.Unix(mintUnix, 0)) > maxAge {
			return hookSelfListFloor(ctx, bl, st, repo)
		}
	}
	return floor, nil
}

// CachedUsageFloor returns repo's UNCOUNTED-bytes admission floor — the
// listing minus the usage counter at that listing's scan start, clamped
// at zero (Codex round 44: ReservePackBytes SUMS this floor with the live
// counter, so settlements after the scan count exactly once; the old
// max() combination hid them whenever pre-registry bytes coexisted with
// fresh settlements). Serves a ≤usageCacheTTL-stale cached scan and
// coalesces concurrent misses into one flight (issue #51, Codex round 15:
// fresh per-transfer listings amplify — a 100-object batch cost ~201 full
// prefix scans). It feeds every admission surface: LFS transfers here,
// the pre-receive hook via its spawn env (EnvUsageFloor), and REST pack
// writers via api.Config.UsageFloor (Codex round 21 — per-push
// self-listings in those paths re-amplify at the LFS object cap).
// Staleness is safe FOR THE FLOOR: bytes registered since the scan are in
// the live counter, in-flight uploads in their reservations, regardless
// of this value; the floor exists for pre-ledger bytes (fixed) and
// orphaned uploads, whose admission visibility this cache delays by at
// most its TTL — a bounded stretch of the ledger's documented
// orphan-window overshoot. Unlike peekUsage (observational, never
// blocks), admission needs a value: a cold entry lists inline, concurrent
// cold callers share the leader's flight.
func (s *Server) CachedUsageFloor(ctx context.Context, repo string) (int64, error) {
	floor, _, err := s.cachedUsageFloorGen(ctx, repo)
	return floor, err
}

// cachedUsageFloorGen returns the floor plus the shared adoption
// generation its scan snapshotted (the receive-pack spawn forwards the
// generation to the hook — Codex round 49).
func (s *Server) cachedUsageFloorGen(ctx context.Context, repo string) (int64, int64, error) {
	assertf(repo != "", "CachedUsageFloor: empty repo")
	// A flight result whose scan outlived the freshness window is NOT
	// servable (Codex round 41): under the minimum reservation TTL, an
	// orphan published after its page was scanned has already lost
	// reservation cover when such a listing completes. A non-zero floor
	// whose shared adoption generation moved is not servable either
	// (Codex round 49): another replica's adoption moved listed bytes
	// into the counter, so the entry would double-count them. One retry
	// covers either transient; persistent failure is CLOSED (deny-safe)
	// rather than admitting or denying against the stale floor.
	for attempt := 0; ; attempt++ {
		usage, gen, expires, expired, err := s.usageFloorOnce(ctx, repo)
		if err != nil {
			return 0, 0, err
		}
		if !expired {
			// A zero floor cannot double-count: skip the generation
			// point read on the hot steady-state path.
			if usage == 0 {
				return 0, gen, nil
			}
			// The generation read is bounded by the floor's REMAINING
			// freshness (computed on the cache clock, so injected test
			// clocks stay consistent) AND the expiry is re-checked
			// afterwards (Codex round 50): a database stall here
			// otherwise consumes the window after the measurement was
			// declared fresh — an orphan published after the scan could
			// lose reservation cover during the stall. A read the
			// deadline killed degrades to invalidate-and-retry like a
			// stale generation; other failures stay closed.
			gctx, gcancel := context.WithTimeout(ctx, expires.Sub(s.usage.clockNow()))
			fresh, err := s.floorGenFresh(gctx, repo, gen)
			gcancel()
			if err != nil {
				if !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
					return 0, 0, err
				}
				fresh = false // the stall consumed the window
			}
			if fresh && !s.usage.clockNow().After(expires) {
				return usage, gen, nil
			}
			s.invalidateUsageFloor(repo) // adopted or stall-expired: re-scan
		}
		if attempt >= 1 {
			return 0, 0, fmt.Errorf("usage floor for %s: no fresh measurement after retry (slow scan, stall, or concurrent adoption)", repo)
		}
	}
}

// floorGenFresh reports whether the shared adoption generation still
// matches the one a floor's scan snapshotted (Codex round 49).
func (s *Server) floorGenFresh(ctx context.Context, repo string, gen int64) (bool, error) {
	if s.Store == nil {
		return true, nil // bare test servers; NewServer asserts Store
	}
	live, err := s.Store.LFSAdoptionGen(ctx, BaseRepo(repo))
	if err != nil {
		return false, fmt.Errorf("adoption gen read: %w", err)
	}
	return live == gen, nil
}

// usageFloorOnce serves one cache read / coalesced-flight wait for
// CachedUsageFloor. expired=true reports a completed flight whose scan
// outlived the freshness window (the caller retries or fails closed);
// gen is the shared adoption generation the measurement snapshotted and
// expires its scan-start-anchored freshness bound.
func (s *Server) usageFloorOnce(ctx context.Context, repo string) (usage, gen int64, expires time.Time, expired bool, err error) {
	c := &s.usage
	c.mu.Lock()
	if e, ok := c.entries[repo]; ok && !c.clockNow().After(e.expires) {
		c.mu.Unlock()
		return e.uncounted(), e.adoptionGen, e.expires, false, nil
	}
	f := c.inflight[repo]
	if f == nil {
		f = &usageFlight{done: make(chan struct{}), abort: make(chan struct{})}
		if c.inflight == nil {
			c.inflight = make(map[string]*usageFlight)
		}
		c.inflight[repo] = f
		// The flight is ASYNCHRONOUS and slot-gated (Codex round 40):
		// an inline leader listing bypassed usageRefreshSlots, so a
		// writer with tokens for many repositories could disconnect many
		// cold requests and leave one detached backend listing per
		// repository running for up to usageListTimeout. The goroutine
		// parks until a node-wide slot frees — at most one per repo (the
		// inflight map), holding only memory while parked, and reaped
		// entirely when its last waiter departs (the abort arm; round
		// 41) — and every caller, the flight starter included, waits
		// below bounded by its own request context.
		slots := usageRefreshSlots // captured now: tests swap the var
		go func() {
			select {
			case slots <- struct{}{}:
			case <-f.abort:
				return // last waiter left before a slot freed
			}
			c.mu.Lock()
			run := !f.aborted
			if run {
				f.started = true
			}
			c.mu.Unlock()
			if !run { // abort raced the slot: hand it back unused
				<-slots
				return
			}
			defer func() { <-slots }()
			s.refreshUsage(repo, f)
		}()
	}
	f.waiters++
	c.mu.Unlock()
	select {
	case <-f.done:
		c.mu.Lock()
		f.waiters--
		c.mu.Unlock()
		if f.err != nil {
			return 0, 0, time.Time{}, false, f.err
		}
		if c.clockNow().After(f.expires) {
			return 0, 0, time.Time{}, true, nil
		}
		return max(0, f.usage-f.counter), f.adoptionGen, f.expires, false, nil
	case <-ctx.Done():
		c.mu.Lock()
		f.waiters--
		if f.waiters == 0 && !f.started && !f.aborted {
			// Nobody is left to consume this flight and it never
			// started listing: reap it so abandoned cold requests
			// cannot accumulate parked goroutines and inflight
			// entries, or run detached listings with no consumer.
			f.aborted = true
			close(f.abort)
			if c.inflight[repo] == f {
				delete(c.inflight, repo)
			}
		}
		c.mu.Unlock()
		return 0, 0, time.Time{}, false, ctx.Err()
	}
}
