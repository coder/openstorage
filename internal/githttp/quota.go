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
// share the base repository's quota: blob.PackUsageBytes rolls all of a
// repo's namespaces up, because auth strips the suffix and one token must
// not obtain a fresh quota per namespace (Codex round 15).

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/coder/code-storage/internal/blob"
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
	usage   int64
	expires time.Time
}

// usageFlight is one in-progress listing; done is closed after usage/err
// are set, releasing every coalesced waiter.
type usageFlight struct {
	done  chan struct{}
	usage int64
	err   error
}

func (c *usageCache) clockNow() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

// putLocked stores repo's usage for usageCacheTTL, dropping expired entries
// at most once per usageCachePruneInterval. Callers hold c.mu.
func (c *usageCache) putLocked(repo string, usage int64) {
	assertf(repo != "" && usage >= 0, "usageCache.putLocked(%q, %d)", repo, usage)
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
	c.entries[repo] = usageEntry{usage: usage, expires: now.Add(usageCacheTTL)}
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
			f := &usageFlight{done: make(chan struct{})}
			if c.inflight == nil {
				c.inflight = make(map[string]*usageFlight)
			}
			c.inflight[repo] = f
			go func() {
				defer func() { <-usageRefreshSlots }()
				s.refreshUsage(repo, f)
			}()
		default:
			// Pool exhausted: skip; a later peek retries the refresh.
		}
	}
	return 0, false
}

// refreshUsage runs one usage flight for repo and publishes the result.
// It owns the c.inflight[repo] slot the caller created; errors are logged
// and not cached, so the next peek elects a new flight.
func (s *Server) refreshUsage(repo string, f *usageFlight) {
	assertf(repo != "" && f != nil, "refreshUsage: missing repo or flight")
	ctx, cancel := context.WithTimeout(context.Background(), usageListTimeout)
	defer cancel()
	usage, err := packUsageBytes(ctx, s.Blob, repo)

	c := &s.usage
	c.mu.Lock()
	delete(c.inflight, repo)
	if err == nil {
		c.putLocked(repo, usage)
	}
	c.mu.Unlock()
	if err != nil {
		log.Printf("quota: %s: usage refresh failed: %v", repo, err)
	}
	f.usage, f.err = usage, err
	close(f.done)
}

// packUsageBytes sums the sizes of every object under repo's pack prefix
// (blob.PackUsageBytes, shared with the REST pack writers).
func packUsageBytes(ctx context.Context, bl blob.BlobStore, repo string) (int64, error) {
	return blob.PackUsageBytes(ctx, bl, repo)
}
