// Pre-auth rate limiting (docs/03: gateway-level 429). This file holds the
// frozen phase-6 contract: RateLimitConfig on api.Config, a *rateLimiter
// built once in New, and (*rateLimiter).limit(flavor) applied at every REST
// route registration site (v1 + named-repo aliases + repo-urls) BEFORE the
// auth middleware.
//
// Admission is a per-client-IP token bucket (hand-rolled, x/time/rate
// semantics: float tokens, monotonic refill, burst cap) in a lazy map with a
// mutex and a periodic stale-entry sweep. The key is the RemoteAddr host —
// X-Forwarded-For is trusted only from peers inside the operator's explicit
// TrustedProxies allowlist (issue #30); by default (empty allowlist) it is
// deliberately ignored. Limiting is per node, in
// memory: nodes are stateless and per-node limiting is the documented
// "basic" scope. 429-before-auth means unauthenticated clients consume rate
// budget keyed by IP — accepted for the basic scope (plan risk note).
package api

import (
	"context"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RateLimitConfig enables per-client-IP rate limiting when RPS > 0; the zero
// value disables limiting entirely (the middleware becomes the identity).
// Limiting is per node, in memory: nodes are stateless and per-node limiting
// is the documented "basic" scope.
type RateLimitConfig struct {
	// RPS is the sustained refill rate per client IP.
	RPS float64
	// Burst is the token-bucket depth; must be >= 1 when RPS > 0.
	Burst int
	// TrustedProxies is the reverse-proxy allowlist for client identity
	// (issue #30). When the TCP peer is inside one of these prefixes, the
	// bucket key becomes the rightmost X-Forwarded-For hop that is not
	// itself allowlisted — the client address the nearest trusted proxy
	// saw. Empty (the default) keeps RemoteAddr-only keying: a spoofable
	// key would let any client mint fresh buckets at will, so forwarded
	// headers from unlisted peers are never consulted. Operator contract:
	// list ONLY proxies that set/append X-Forwarded-For themselves; a
	// listed proxy that forwards client-supplied values verbatim
	// reintroduces exactly that bucket-minting spoof.
	TrustedProxies []netip.Prefix
}

// routeFlavor selects the 429 body shape, matching the existing envelope
// split of the route's auth middleware.
type routeFlavor int

const (
	// routeRead: routes registered with requireAuth answer 429 with the
	// plain {"error": ...} envelope.
	routeRead routeFlavor = iota
	// routeWrite: routes registered with requireAuthWrite answer 429 with
	// the docs/04 §3 result envelope ({"result": {success: false, status:
	// "unavailable", ...}}, docs/03 status mapping).
	routeWrite
)

// sweepInterval is how often the bucket map drops stale (fully refilled)
// entries. Eviction is behavior-neutral: a dropped bucket is recreated full,
// exactly what refilling it to burst would have produced.
const sweepInterval = time.Minute

// bucket is one client's token bucket. tokens is refreshed lazily from the
// elapsed time since last. blocked is the git surface's hysteresis latch
// (admitReserve); the REST path's reserve of 1 makes it equivalent to the
// plain tokens >= 1 check.
type bucket struct {
	tokens  float64
	last    time.Time
	blocked bool
}

// rateLimiter holds the per-node limiter state. A nil *rateLimiter means
// disabled and is always valid: limit returns the identity middleware.
type rateLimiter struct {
	cfg RateLimitConfig
	now func() time.Time // injectable clock for deterministic tests

	mu        sync.Mutex
	buckets   map[string]*bucket
	lastSweep time.Time
}

// newRateLimiter returns (nil, nil) (disabled) when cfg.RPS <= 0, else a
// limiter. Invalid config is an operator input error, so it returns a clean
// startup error instead of panicking (docs/15 deferred #4).
func newRateLimiter(cfg RateLimitConfig) (*rateLimiter, error) {
	if math.IsNaN(cfg.RPS) || math.IsInf(cfg.RPS, 0) {
		return nil, fmt.Errorf("rate limit config: RPS must be finite (got %v)", cfg.RPS)
	}
	if cfg.RPS <= 0 {
		return nil, nil
	}
	if cfg.Burst < 1 {
		return nil, fmt.Errorf("rate limit config: Burst must be >= 1 when RPS > 0 (got %d)", cfg.Burst)
	}
	for _, p := range cfg.TrustedProxies {
		// The allowlist arrives pre-parsed (resolveTrustedProxies); a zero
		// prefix here is a caller bug, not operator input.
		assertf(p.IsValid(), "rate limit config: invalid trusted-proxy prefix %v", p)
	}
	l := &rateLimiter{cfg: cfg, now: time.Now, buckets: make(map[string]*bucket)}
	l.lastSweep = l.now()
	return l, nil
}

// rlExemptKey marks a request that already paid the rate-limit toll at an
// outer route (the named-repo alias layer re-dispatches through the mux, so
// the v1 twin's limiter must not charge the same request a second token).
type rlExemptKey struct{}

// rateLimitExempt marks ctx so nested limit middlewares pass through.
func rateLimitExempt(ctx context.Context) context.Context {
	return context.WithValue(ctx, rlExemptKey{}, true)
}

// limit returns the pre-auth middleware for one REST route; flavor picks the
// 429 envelope shape and must match the route's auth-failure envelope.
// 429 responses carry a Retry-After header.
func (l *rateLimiter) limit(flavor routeFlavor) middleware {
	assertf(flavor == routeRead || flavor == routeWrite, "limit: unknown flavor %d", flavor)
	if l == nil {
		return func(next http.Handler) http.Handler { return next }
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Context().Value(rlExemptKey{}) != nil {
				next.ServeHTTP(w, r)
				return
			}
			ok, retryAfter := l.admit(l.clientKey(r))
			if !ok {
				w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
				const msg = "rate limit exceeded"
				if flavor == routeRead {
					writeErr(w, http.StatusTooManyRequests, msg)
					return
				}
				writeJSON(w, http.StatusTooManyRequests, map[string]any{"result": map[string]any{
					"success": false,
					"status":  statusForHTTP(http.StatusTooManyRequests),
					"message": msg,
				}})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// admit refills key's bucket and takes one token. When the bucket is empty
// it returns ok=false and the whole seconds until one token is available
// (>= 1, the Retry-After granularity).
func (l *rateLimiter) admit(key string) (ok bool, retryAfterSecs int) {
	return l.admitReserve(key, 1)
}

// admitReserve is admit with hysteresis: a request is charged one token,
// but once a client has been limited its bucket latches blocked and
// reopens only when reserve tokens have refilled. reserve = 1 degenerates
// to the plain per-request bucket (the REST path). The git surface passes
// a whole-exchange reserve (Codex round 9): one git operation is several
// dependent HTTP requests, so with a tiny RPS a client that retries after
// Retry-After would otherwise win exactly one token, spend it on the
// exchange's first request, and 429 on the second — forever. Latching
// until a full exchange fits makes the advertised Retry-After honest: the
// retry completes. Long-run admission stays at RPS; the reserve only
// quantizes it into completable bursts.
func (l *rateLimiter) admitReserve(key string, reserve float64) (ok bool, retryAfterSecs int) {
	assertf(key != "", "admitReserve: empty client key")
	assertf(reserve >= 1 && reserve <= float64(l.cfg.Burst), "admitReserve: reserve %v outside [1, %d]", reserve, l.cfg.Burst)
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	l.sweepLocked(now)

	b := l.buckets[key]
	if b == nil {
		b = &bucket{tokens: float64(l.cfg.Burst), last: now}
		l.buckets[key] = b
	} else {
		b.refill(now, l.cfg)
	}
	assertf(b.tokens >= 0 && b.tokens <= float64(l.cfg.Burst), "admitReserve: bucket tokens %v outside [0, %d]", b.tokens, l.cfg.Burst)

	if b.blocked && b.tokens >= reserve {
		b.blocked = false
	}
	if b.blocked || b.tokens < 1 {
		b.blocked = true
		secs := int(math.Ceil((reserve - b.tokens) / l.cfg.RPS))
		if secs < 1 {
			secs = 1
		}
		return false, secs
	}
	b.tokens--
	return true, 0
}

// refill advances the bucket to now, capping at the configured burst.
// A non-monotonic clock step (now before last) leaves tokens unchanged.
func (b *bucket) refill(now time.Time, cfg RateLimitConfig) {
	elapsed := now.Sub(b.last)
	if elapsed <= 0 {
		return
	}
	b.tokens = math.Min(float64(cfg.Burst), b.tokens+elapsed.Seconds()*cfg.RPS)
	b.last = now
}

// sweepLocked drops fully-refilled (stale) buckets at most once per
// sweepInterval. Callers hold l.mu. Eviction stays behavior-neutral with
// hysteresis: a full bucket satisfies any valid reserve (reserve <= Burst),
// so recreating it unblocked is exactly what unlatching would produce.
func (l *rateLimiter) sweepLocked(now time.Time) {
	if now.Sub(l.lastSweep) < sweepInterval {
		return
	}
	l.lastSweep = now
	for key, b := range l.buckets {
		b.refill(now, l.cfg)
		if b.tokens >= float64(l.cfg.Burst) {
			delete(l.buckets, key)
		}
	}
}

// clientKey extracts the bucket key for a request. The base identity is the
// RemoteAddr host (IPv4/IPv6), falling back to the raw RemoteAddr when it
// carries no port; an empty RemoteAddr (possible for exotic listeners/
// in-process handlers) maps to the shared "unknown" bucket so admit's
// non-empty-key invariant holds (docs/15 deferred #3).
//
// When the peer is inside the TrustedProxies allowlist (issue #30), the key
// becomes the rightmost X-Forwarded-For hop that is not itself allowlisted:
// hops at or right of that boundary were appended by trusted proxies, and
// hops left of it are client-supplied and never examined. Everything else
// fails closed to the peer key (today's behavior): an empty allowlist, an
// unlisted peer (spoofed XFF), a trusted peer with no XFF or an all-trusted
// chain (the proxy's own traffic), and any malformed hop at or right of the
// boundary.
func (l *rateLimiter) clientKey(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil || host == "" {
		host = r.RemoteAddr
	}
	if host == "" {
		return "unknown"
	}
	if len(l.cfg.TrustedProxies) == 0 {
		return host
	}
	peer, err := netip.ParseAddr(host)
	if err != nil || !trustedContains(l.cfg.TrustedProxies, peer) {
		return host
	}
	// Walk the XFF chain right-to-left across all header copies (later
	// headers are later hops).
	values := r.Header.Values("X-Forwarded-For")
	for vi := len(values) - 1; vi >= 0; vi-- {
		hops := strings.Split(values[vi], ",")
		for hi := len(hops) - 1; hi >= 0; hi-- {
			addr, ok := parseForwardedAddr(hops[hi])
			if !ok {
				return host // malformed hop: fall back to the peer key
			}
			if !trustedContains(l.cfg.TrustedProxies, addr) {
				return addr.String()
			}
		}
	}
	return host
}

// trustedContains reports whether addr is inside any allowlisted prefix.
// Addresses are unmapped first so IPv4-mapped IPv6 peers match IPv4
// allowlist entries. Zone-scoped addresses never match (netip.Prefix
// semantics), so link-local peers keep RemoteAddr keying.
func trustedContains(prefixes []netip.Prefix, addr netip.Addr) bool {
	addr = addr.Unmap()
	for _, p := range prefixes {
		if p.Contains(addr) {
			return true
		}
	}
	return false
}

// parseForwardedAddr parses one X-Forwarded-For hop into an address.
// Accepted spellings (surrounding whitespace ignored): bare IPv4/IPv6
// (zone IDs allowed), bracketed IPv6, and either with a port. Anything
// else — empty hops, "unknown", RFC 7239 obfuscated tokens, hostnames —
// is malformed. IPv4-mapped IPv6 forms are unmapped so one client cannot
// key two buckets under both spellings.
func parseForwardedAddr(hop string) (netip.Addr, bool) {
	hop = strings.TrimSpace(hop)
	if addr, err := netip.ParseAddr(hop); err == nil {
		return addr.Unmap(), true
	}
	host := hop
	if h, _, err := net.SplitHostPort(hop); err == nil {
		host = h // v4:port or [v6]:port
	} else if len(hop) >= 2 && hop[0] == '[' && hop[len(hop)-1] == ']' {
		host = hop[1 : len(hop)-1] // bracketed v6 without a port
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return netip.Addr{}, false
	}
	return addr.Unmap(), true
}

// gitMinBurst is the git-surface floor for the token-bucket depth. One git
// operation is several smart-HTTP requests — up to two per endpoint (the
// unauthenticated 401 probe plus the authenticated retry) across info/refs
// and the service RPC — and git clients treat a mid-exchange 429 as a hard
// failure rather than waiting for Retry-After. A burst below one full
// exchange (e.g. the documented-valid BURST=1) would therefore make every
// clone/fetch/push permanently fail, so NewGitRateLimit raises the depth to
// this floor: 8 covers a complete exchange with headroom (e.g. an LFS batch
// call) while keeping the operator's sustained RPS untouched.
const gitMinBurst = 8

// NewGitRateLimit returns the pre-auth rate-limit middleware for the git
// smart-HTTP surface (issue #21): the same admission policy and config knob
// as the REST limiter, applied by the serve wiring around the whole githttp
// handler. Bucket state is intentionally separate from the REST limiter's —
// git transfers are far heavier per request, so one surface exhausting its
// budget must not starve the other. The configured burst is raised to
// gitMinBurst so a single protocol exchange can always complete. 429
// responses carry Retry-After and the git surface's plain {"error"}
// envelope (git clients surface the status line, not the body). The zero
// config yields the identity middleware; invalid config is a startup error,
// mirroring newRateLimiter.
func NewGitRateLimit(cfg RateLimitConfig) (func(http.Handler) http.Handler, error) {
	// Validate the operator's pair as configured (half-set or malformed
	// pairs stay startup errors), then apply the git-surface floor.
	if _, err := newRateLimiter(cfg); err != nil {
		return nil, err
	}
	if cfg.RPS > 0 && cfg.Burst < gitMinBurst {
		cfg.Burst = gitMinBurst
	}
	l, err := newRateLimiter(cfg)
	if err != nil {
		return nil, err
	}
	if l == nil {
		return func(next http.Handler) http.Handler { return next }, nil
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// clientKey maps an empty RemoteAddr to the shared "unknown"
			// bucket (docs/15 deferred #3), so exotic listeners degrade to
			// a collective limit instead of panicking or bypassing, and
			// resolves trusted-proxy identity exactly like the REST
			// limiter (issue #30, shared plumbing). The whole-exchange
			// reserve (== the burst floor) latches limited clients until
			// a complete git operation fits (admitReserve).
			ok, retryAfter := l.admitReserve(l.clientKey(r), gitMinBurst)
			if !ok {
				w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
				writeErr(w, http.StatusTooManyRequests, "rate limit exceeded")
				return
			}
			next.ServeHTTP(w, r)
		})
	}, nil
}
