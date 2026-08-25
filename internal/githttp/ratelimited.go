// Package-level helper for throttled (429) responses. Upstream's OpenAPI
// documents TWO headers on every 429: Retry-After and RateLimit (issue
// #150). RateLimit is the current IETF draft shape
// (draft-ietf-httpapi-ratelimit-headers, §4): a structured-field service
// limit Item — a quoted quota-policy name with parameters r (available
// quota) and t (effective window in seconds). On a throttled response r is
// by definition 0, and t mirrors Retry-After's delay-seconds: the draft
// (§6) requires Retry-After to reference a point no earlier than the end of
// the effective window, and equal values are the exact boundary (the
// draft's own throttled-response example, appendix B.1.4, does the same).
//
// The policy name is opaque to clients: upstream deliberately advertises no
// RateLimit-Policy field ("the gateway's capacity is dynamic"), so the
// identifier here is the draft's canonical example name "default".
//
// This helper is the single source of truth for the wire shape across both
// serving surfaces: the LFS busy/backpressure 429s in this package and the
// api package's rate-limiter middleware and busy handlers (the api package
// imports githttp; the reverse edge would cycle).
package githttp

import (
	"fmt"
	"net/http"
	"strconv"
)

// SetRateLimited stamps the two throttling headers carried by every
// HTTP-level 429 the server emits. retryAfterSecs is the delay-seconds
// value (>= 1, the Retry-After granularity).
func SetRateLimited(h http.Header, retryAfterSecs int) {
	assertf(retryAfterSecs >= 1, "SetRateLimited: retry-after %d below the 1s delay-seconds granularity", retryAfterSecs)
	h.Set("Retry-After", strconv.Itoa(retryAfterSecs))
	h.Set("RateLimit", fmt.Sprintf(`"default";r=0;t=%d`, retryAfterSecs))
}
