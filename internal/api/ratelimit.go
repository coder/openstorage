// Pre-auth rate limiting (docs/03: gateway-level 429). This file holds the
// frozen phase-6 contract: RateLimitConfig on api.Config, a *rateLimiter
// built once in New, and (*rateLimiter).limit(flavor) applied at every REST
// route registration site (v1 + named-repo aliases + repo-urls) BEFORE the
// auth middleware.
//
// Admission is a per-client-IP token bucket (hand-rolled, x/time/rate
// semantics: float tokens, monotonic refill, burst cap) in a lazy map with a
// mutex and a periodic stale-entry sweep. The key is the RemoteAddr host —
// X-Forwarded-For is deliberately not trusted. Limiting is per node, in
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
	"strconv"
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
// elapsed time since last.
type bucket struct {
	tokens float64
	last   time.Time
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
			ok, retryAfter := l.admit(clientKey(r))
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
	assertf(key != "", "admit: empty client key")
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
	assertf(b.tokens >= 0 && b.tokens <= float64(l.cfg.Burst), "admit: bucket tokens %v outside [0, %d]", b.tokens, l.cfg.Burst)

	if b.tokens < 1 {
		secs := int(math.Ceil((1 - b.tokens) / l.cfg.RPS))
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
// sweepInterval. Callers hold l.mu.
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

// clientKey extracts the bucket key for a request: the RemoteAddr host
// (IPv4/IPv6), falling back to the raw RemoteAddr when it carries no port.
// X-Forwarded-For is deliberately ignored (no proxy trust model here).
// An empty RemoteAddr (possible for exotic listeners/in-process handlers)
// maps to the shared "unknown" bucket so admit's non-empty-key invariant
// holds (docs/15 deferred #3).
func clientKey(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil || host == "" {
		host = r.RemoteAddr
	}
	if host == "" {
		return "unknown"
	}
	return host
}
