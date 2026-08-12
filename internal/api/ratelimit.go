// Pre-auth rate limiting (docs/03: gateway-level 429). This file holds the
// frozen phase-6 contract: RateLimitConfig on api.Config, a *rateLimiter
// built once in New, and (*rateLimiter).limit(flavor) applied at every REST
// route registration site (v1 + named-repo aliases + repo-urls) BEFORE the
// auth middleware. Workstream D implements the token-bucket admission behind
// this seam (per-client-IP golang.org/x/time/rate buckets, lazy map with
// mutex + stale-entry eviction, keyed by the RemoteAddr host — no XFF trust).
package api

import "net/http"

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

// rateLimiter holds the per-node limiter state. A nil *rateLimiter means
// disabled and is always valid: limit returns the identity middleware.
type rateLimiter struct {
	cfg RateLimitConfig
	// Workstream D adds the lazy per-IP bucket map (+ mutex and stale-entry
	// eviction) here.
}

// newRateLimiter returns nil (disabled) when cfg.RPS <= 0, else a limiter
// after fail-fast config validation.
func newRateLimiter(cfg RateLimitConfig) *rateLimiter {
	if cfg.RPS <= 0 {
		return nil
	}
	assertf(cfg.Burst >= 1, "RateLimitConfig: Burst must be >= 1 when RPS > 0 (got %d)", cfg.Burst)
	return &rateLimiter{cfg: cfg}
}

// limit returns the pre-auth middleware for one REST route; flavor picks the
// 429 envelope shape and must match the route's auth-failure envelope.
// 429 responses carry a Retry-After header.
//
// Foundation hook: workstream D implements the admission check. Until it
// lands, enabled limiters pass every request through unchanged.
func (l *rateLimiter) limit(flavor routeFlavor) middleware {
	assertf(flavor == routeRead || flavor == routeWrite, "limit: unknown flavor %d", flavor)
	return func(next http.Handler) http.Handler { return next }
}
