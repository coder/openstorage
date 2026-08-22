package api

// Unit tests for the pre-auth per-client-IP token bucket (ratelimit.go):
// envelope shapes per route flavor, deterministic refill via the injectable
// clock, per-client isolation, stale-bucket eviction, the client key
// derivation (XFF trusted only behind allowlisted proxies, issue #30), and
// the alias re-dispatch exemption that keeps aliased requests at exactly
// one token.

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"
)

// okHandler is the downstream handler admitted requests must reach.
func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

// rlRequest drives one request with a fixed client address through h.
func rlRequest(t *testing.T, h http.Handler, remoteAddr string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, "/probe", nil)
	r.RemoteAddr = remoteAddr
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestRateLimitDisabledPassthrough(t *testing.T) {
	rl, err := newRateLimiter(RateLimitConfig{})
	if rl != nil || err != nil {
		t.Fatalf("newRateLimiter(zero) = (%v, %v), want (nil, nil) (disabled)", rl, err)
	}
	h := rl.limit(routeRead)(okHandler())
	for i := 0; i < 10; i++ {
		if w := rlRequest(t, h, "10.0.0.1:1234"); w.Code != http.StatusOK {
			t.Fatalf("request %d through disabled limiter = %d, want 200", i, w.Code)
		}
	}
}

// TestRateLimitInvalidConfigError asserts invalid operator config is a clean
// startup error, never a panic (docs/15 deferred #4).
func TestRateLimitInvalidConfigError(t *testing.T) {
	cases := map[string]RateLimitConfig{
		"burst zero": {RPS: 1},
		"burst neg":  {RPS: 1, Burst: -3},
		"nan rps":    {RPS: math.NaN(), Burst: 1},
		"inf rps":    {RPS: math.Inf(1), Burst: 1},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if p := recover(); p != nil {
					t.Fatalf("newRateLimiter(%+v) panicked: %v", cfg, p)
				}
			}()
			rl, err := newRateLimiter(cfg)
			if err == nil || rl != nil {
				t.Fatalf("newRateLimiter(%+v) = (%v, %v), want (nil, error)", cfg, rl, err)
			}
		})
	}
}

// newTestLimiter returns a limiter on a manual clock starting at base.
func newTestLimiter(cfg RateLimitConfig, base time.Time) (*rateLimiter, *time.Time) {
	rl, err := newRateLimiter(cfg)
	if err != nil || rl == nil {
		panic(fmt.Sprintf("newTestLimiter(%+v) = (%v, %v), want a limiter", cfg, rl, err))
	}
	now := base
	rl.now = func() time.Time { return now }
	rl.lastSweep = base
	return rl, &now
}

func TestRateLimit429ReadEnvelope(t *testing.T) {
	rl, _ := newTestLimiter(RateLimitConfig{RPS: 1, Burst: 1}, time.Unix(1000, 0))
	h := rl.limit(routeRead)(okHandler())

	if w := rlRequest(t, h, "10.0.0.1:1234"); w.Code != http.StatusOK {
		t.Fatalf("first request = %d, want 200", w.Code)
	}
	w := rlRequest(t, h, "10.0.0.1:5678") // same IP, different port
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("second request = %d, want 429", w.Code)
	}
	if ra := w.Header().Get("Retry-After"); ra != "1" {
		t.Errorf("Retry-After = %q, want %q", ra, "1")
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var body struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal 429 body %q: %v", w.Body.Bytes(), err)
	}
	if body.Error != "rate limit exceeded" {
		t.Errorf("error = %q, want %q", body.Error, "rate limit exceeded")
	}
}

func TestRateLimit429WriteEnvelope(t *testing.T) {
	rl, _ := newTestLimiter(RateLimitConfig{RPS: 1, Burst: 1}, time.Unix(1000, 0))
	h := rl.limit(routeWrite)(okHandler())

	if w := rlRequest(t, h, "10.0.0.1:1234"); w.Code != http.StatusOK {
		t.Fatalf("first request = %d, want 200", w.Code)
	}
	w := rlRequest(t, h, "10.0.0.1:1234")
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("second request = %d, want 429", w.Code)
	}
	if ra := w.Header().Get("Retry-After"); ra == "" {
		t.Error("Retry-After header missing")
	}
	var body struct {
		Result struct {
			Success bool   `json:"success"`
			Status  string `json:"status"`
			Message string `json:"message"`
		} `json:"result"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal 429 body %q: %v", w.Body.Bytes(), err)
	}
	if body.Result.Success || body.Result.Status != "unavailable" || body.Result.Message == "" {
		t.Errorf("result envelope = %+v, want success=false status=unavailable non-empty message", body.Result)
	}
}

func TestRateLimitDeterministicRefill(t *testing.T) {
	rl, now := newTestLimiter(RateLimitConfig{RPS: 2, Burst: 2}, time.Unix(1000, 0))
	h := rl.limit(routeRead)(okHandler())
	const addr = "10.0.0.1:1"

	// Drain the burst.
	for i := 0; i < 2; i++ {
		if w := rlRequest(t, h, addr); w.Code != http.StatusOK {
			t.Fatalf("burst request %d = %d, want 200", i, w.Code)
		}
	}
	if w := rlRequest(t, h, addr); w.Code != http.StatusTooManyRequests {
		t.Fatalf("drained bucket admitted a request (code %d)", w.Code)
	}

	// 500ms at 2 RPS refills exactly one token.
	*now = now.Add(500 * time.Millisecond)
	if w := rlRequest(t, h, addr); w.Code != http.StatusOK {
		t.Fatalf("request after refill = %d, want 200", w.Code)
	}
	if w := rlRequest(t, h, addr); w.Code != http.StatusTooManyRequests {
		t.Fatalf("second request after single-token refill = %d, want 429", w.Code)
	}
}

func TestRateLimitPerClientIsolation(t *testing.T) {
	rl, _ := newTestLimiter(RateLimitConfig{RPS: 1, Burst: 1}, time.Unix(1000, 0))
	h := rl.limit(routeRead)(okHandler())

	if w := rlRequest(t, h, "10.0.0.1:1"); w.Code != http.StatusOK {
		t.Fatalf("client A first request = %d, want 200", w.Code)
	}
	if w := rlRequest(t, h, "10.0.0.1:1"); w.Code != http.StatusTooManyRequests {
		t.Fatalf("client A second request = %d, want 429", w.Code)
	}
	// A different IP has its own untouched bucket.
	if w := rlRequest(t, h, "10.0.0.2:1"); w.Code != http.StatusOK {
		t.Fatalf("client B first request = %d, want 200", w.Code)
	}
}

func TestRateLimitStaleEviction(t *testing.T) {
	rl, now := newTestLimiter(RateLimitConfig{RPS: 1, Burst: 1}, time.Unix(1000, 0))
	h := rl.limit(routeRead)(okHandler())

	rlRequest(t, h, "10.0.0.1:1")
	rlRequest(t, h, "10.0.0.2:1")
	if got := len(rl.buckets); got != 2 {
		t.Fatalf("bucket count = %d, want 2", got)
	}

	// Past the sweep interval both buckets are fully refilled; the next
	// admission sweeps them and creates only the new client's bucket.
	*now = now.Add(sweepInterval + time.Second)
	rlRequest(t, h, "10.0.0.3:1")
	if got := len(rl.buckets); got != 1 {
		t.Fatalf("bucket count after sweep = %d, want 1 (stale entries evicted)", got)
	}
	if rl.buckets["10.0.0.3"] == nil {
		t.Error("surviving bucket is not the active client's")
	}
}

func TestClientKey(t *testing.T) {
	rl, _ := newTestLimiter(RateLimitConfig{RPS: 1, Burst: 1}, time.Unix(1000, 0))
	cases := []struct {
		name       string
		remoteAddr string
		xff        string
		want       string
	}{
		{"ipv4", "10.1.2.3:5555", "", "10.1.2.3"},
		{"ipv6", "[2001:db8::1]:443", "", "2001:db8::1"},
		{"no-port", "10.1.2.3", "", "10.1.2.3"},
		{"xff-ignored", "10.1.2.3:5555", "203.0.113.9", "10.1.2.3"},
		// docs/15 deferred #3: an empty RemoteAddr falls back to the
		// shared sentinel bucket instead of tripping admit's assert.
		{"empty", "", "", "unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.RemoteAddr = tc.remoteAddr
			if tc.xff != "" {
				r.Header.Set("X-Forwarded-For", tc.xff)
			}
			if got := rl.clientKey(r); got != tc.want {
				t.Errorf("clientKey(%q) = %q, want %q", tc.remoteAddr, got, tc.want)
			}
		})
	}
}

// TestClientKeyTrustedProxies covers the issue #30 identity resolution: with
// a trusted-proxy allowlist configured, a request whose TCP peer is inside
// the allowlist keys on the rightmost X-Forwarded-For hop that is not
// itself allowlisted; every other shape falls back to the RemoteAddr host.
func TestClientKeyTrustedProxies(t *testing.T) {
	trusted := []netip.Prefix{
		netip.MustParsePrefix("10.0.0.0/8"),
		netip.MustParsePrefix("2001:db8:1::/48"),
	}
	rl, _ := newTestLimiter(RateLimitConfig{RPS: 1, Burst: 1, TrustedProxies: trusted}, time.Unix(1000, 0))

	const proxy = "10.0.0.5:7777" // inside 10.0.0.0/8
	cases := []struct {
		name       string
		remoteAddr string
		xff        []string // one entry per X-Forwarded-For header copy
		want       string
	}{
		// Untrusted peer: XFF is never consulted (spoof attempt).
		{"untrusted-peer-ignores-xff", "192.0.2.1:1", []string{"203.0.113.9"}, "192.0.2.1"},
		// Trusted peer, single client hop.
		{"single-hop", proxy, []string{"203.0.113.9"}, "203.0.113.9"},
		// Rightmost untrusted wins; client-supplied garbage left of the
		// boundary is never examined.
		{"rightmost-untrusted", proxy, []string{"junk, 198.51.100.7"}, "198.51.100.7"},
		// Trusted hops right of the client are skipped.
		{"skips-trusted-hops", proxy, []string{"198.51.100.7, 10.0.0.9"}, "198.51.100.7"},
		{"skips-trusted-v6-hop", proxy, []string{"198.51.100.7, 2001:db8:1::9"}, "198.51.100.7"},
		// All hops trusted / no XFF: the proxy's own traffic.
		{"all-trusted", proxy, []string{"10.0.0.9, 10.0.0.10"}, "10.0.0.5"},
		{"no-xff", proxy, nil, "10.0.0.5"},
		// Multiple header copies: later headers are later hops.
		{"multi-header", proxy, []string{"198.51.100.7", "10.0.0.9"}, "198.51.100.7"},
		// Hop spellings: ports, brackets, whitespace, zones, 4-in-6.
		{"v4-with-port", proxy, []string{"203.0.113.9:4711"}, "203.0.113.9"},
		{"v6-bare", proxy, []string{"2001:db8:2::9"}, "2001:db8:2::9"},
		{"v6-bracketed", proxy, []string{"[2001:db8:2::9]"}, "2001:db8:2::9"},
		{"v6-bracketed-port", proxy, []string{"[2001:db8:2::9]:443"}, "2001:db8:2::9"},
		{"v6-zone", proxy, []string{"fe80::1%eth0"}, "fe80::1%eth0"},
		{"whitespace", proxy, []string{"  203.0.113.9\t"}, "203.0.113.9"},
		{"v4-mapped-unmapped", proxy, []string{"::ffff:203.0.113.9"}, "203.0.113.9"},
		// Malformed rightmost hop: fail closed to the peer key.
		{"malformed-hostname", proxy, []string{"evil.example"}, "10.0.0.5"},
		{"malformed-unknown", proxy, []string{"unknown"}, "10.0.0.5"},
		// An empty hop left of the boundary is never reached: the walk
		// stops at the rightmost untrusted hop.
		{"empty-hop-left-of-boundary", proxy, []string{"203.0.113.9,,198.51.100.7"}, "198.51.100.7"},
		// ...but a malformed hop at the boundary (right of it only
		// trusted hops) fails closed to the peer.
		{"malformed-at-boundary", proxy, []string{"203.0.113.9,,10.0.0.9"}, "10.0.0.5"},
		{"malformed-trailing-comma", proxy, []string{"203.0.113.9,"}, "10.0.0.5"},
		{"malformed-empty-header", proxy, []string{""}, "10.0.0.5"},
		// A 4-in-6 peer matches the IPv4 allowlist entry.
		{"v4-mapped-peer-trusted", "[::ffff:10.0.0.5]:7777", []string{"203.0.113.9"}, "203.0.113.9"},
		// A zone-scoped peer never matches the allowlist (netip semantics).
		{"zoned-peer-untrusted", "[fe80::1%eth0]:7777", []string{"203.0.113.9"}, "fe80::1%eth0"},
		// Sentinel keys stay put even with an allowlist configured.
		{"empty-remoteaddr", "", []string{"203.0.113.9"}, "unknown"},
		{"non-ip-remoteaddr", "pipe", []string{"203.0.113.9"}, "pipe"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.RemoteAddr = tc.remoteAddr
			for _, v := range tc.xff {
				r.Header.Add("X-Forwarded-For", v)
			}
			if got := rl.clientKey(r); got != tc.want {
				t.Errorf("clientKey(peer %q, xff %q) = %q, want %q", tc.remoteAddr, tc.xff, got, tc.want)
			}
		})
	}
}

// TestRateLimitAliasSingleCharge replicates the alias.go re-dispatch shape:
// the alias route charges one token, marks the request exempt, and
// re-dispatches to the v1 twin whose limiter must pass it through. With
// Burst=1 a double charge would turn every aliased request into a 429.
func TestRateLimitAliasSingleCharge(t *testing.T) {
	rl, _ := newTestLimiter(RateLimitConfig{RPS: 0.001, Burst: 1}, time.Unix(1000, 0))
	mux := http.NewServeMux()
	mux.Handle("GET /v1/x", rl.limit(routeRead)(okHandler()))
	mux.Handle("GET /alias/x", rl.limit(routeRead)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r2 := r.Clone(rateLimitExempt(r.Context()))
		r2.URL.Path = "/v1/x"
		r2.RequestURI = r2.URL.RequestURI()
		mux.ServeHTTP(w, r2)
	})))

	r := httptest.NewRequest(http.MethodGet, "/alias/x", nil)
	r.RemoteAddr = "10.0.0.1:1"
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("aliased request = %d, want 200 (double-charged the token bucket?)", w.Code)
	}

	// Exactly one token was spent: a direct v1 request now 429s.
	r = httptest.NewRequest(http.MethodGet, "/v1/x", nil)
	r.RemoteAddr = "10.0.0.1:1"
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("follow-up direct request = %d, want 429", w.Code)
	}
}

// --- git-surface limiter (issue #21) ----------------------------------------

// TestGitRateLimitDisabledPassthrough: the zero config yields the identity
// middleware, exactly like the REST limiter.
func TestGitRateLimitDisabledPassthrough(t *testing.T) {
	mw, err := NewGitRateLimit(RateLimitConfig{})
	if err != nil {
		t.Fatal(err)
	}
	h := mw(okHandler())
	for i := 0; i < 10; i++ {
		if w := rlRequest(t, h, "10.0.0.1:1234"); w.Code != http.StatusOK {
			t.Fatalf("request %d through disabled git limiter = %d, want 200", i, w.Code)
		}
	}
}

// TestGitRateLimitInvalidConfigError: invalid operator config is a clean
// startup error, never a panic (mirrors newRateLimiter).
func TestGitRateLimitInvalidConfigError(t *testing.T) {
	for _, cfg := range []RateLimitConfig{
		{RPS: 5, Burst: 0},
		{RPS: math.NaN(), Burst: 1},
		{RPS: math.Inf(1), Burst: 1},
	} {
		if _, err := NewGitRateLimit(cfg); err == nil {
			t.Fatalf("NewGitRateLimit(%+v) = nil error, want config error", cfg)
		}
	}
}

// TestGitRateLimit429Envelope: exhausting the burst answers 429 with
// Retry-After and the git surface's plain {"error"} envelope.
func TestGitRateLimit429Envelope(t *testing.T) {
	mw, err := NewGitRateLimit(RateLimitConfig{RPS: 0.5, Burst: gitMinBurst})
	if err != nil {
		t.Fatal(err)
	}
	h := mw(okHandler())
	for i := 0; i < gitMinBurst; i++ {
		if w := rlRequest(t, h, "10.0.0.9:1"); w.Code != http.StatusOK {
			t.Fatalf("burst request %d = %d, want 200", i, w.Code)
		}
	}
	w := rlRequest(t, h, "10.0.0.9:1")
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("over-burst request = %d, want 429", w.Code)
	}
	if w.Header().Get("Retry-After") == "" {
		t.Fatal("429 must carry Retry-After")
	}
	var body errorEnvelope
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body.Error == "" {
		t.Fatalf("expected {\"error\"} envelope, got %q (err %v)", w.Body.String(), err)
	}
	// A different client IP still has its own full bucket.
	if w := rlRequest(t, h, "10.0.0.10:1"); w.Code != http.StatusOK {
		t.Fatalf("other client = %d, want 200 (per-client isolation)", w.Code)
	}
}

// TestGitRateLimitEmptyRemoteAddr: an empty RemoteAddr (exotic listeners,
// in-process handlers) degrades to the shared "unknown" bucket instead of
// panicking or bypassing the limiter (docs/15 deferred #3, issue #21).
func TestGitRateLimitEmptyRemoteAddr(t *testing.T) {
	mw, err := NewGitRateLimit(RateLimitConfig{RPS: 0.001, Burst: 1})
	if err != nil {
		t.Fatal(err)
	}
	h := mw(okHandler())
	for i := 0; i < gitMinBurst; i++ {
		if w := rlRequest(t, h, ""); w.Code != http.StatusOK {
			t.Fatalf("empty-RemoteAddr request %d = %d, want 200", i, w.Code)
		}
	}
	// Same shared bucket: the next keyless request is limited.
	if w := rlRequest(t, h, ""); w.Code != http.StatusTooManyRequests {
		t.Fatalf("over-burst empty-RemoteAddr request = %d, want 429", w.Code)
	}
}

// TestGitRateLimitMinimumBurst: the git surface raises sub-exchange bucket
// depths to gitMinBurst — a Burst=1 config (documented-valid, and fine for
// REST) must still admit one complete multi-request git exchange (Codex
// round 1: git clients treat a mid-exchange 429 as a hard failure). The
// REST limiter built from the same config keeps the configured depth.
func TestGitRateLimitMinimumBurst(t *testing.T) {
	cfg := RateLimitConfig{RPS: 0.001, Burst: 1}
	mw, err := NewGitRateLimit(cfg)
	if err != nil {
		t.Fatal(err)
	}
	git := mw(okHandler())
	for i := 0; i < gitMinBurst; i++ {
		if w := rlRequest(t, git, "10.0.0.8:1"); w.Code != http.StatusOK {
			t.Fatalf("git request %d = %d, want 200 (burst floor not applied)", i, w.Code)
		}
	}
	if w := rlRequest(t, git, "10.0.0.8:1"); w.Code != http.StatusTooManyRequests {
		t.Fatalf("request past the floor = %d, want 429", w.Code)
	}

	// The floor is git-only: a REST limiter from the same config still
	// exhausts after the configured single token.
	restRL, err := newRateLimiter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	rest := restRL.limit(routeRead)(okHandler())
	if w := rlRequest(t, rest, "10.0.0.8:1"); w.Code != http.StatusOK {
		t.Fatalf("first REST request = %d, want 200", w.Code)
	}
	if w := rlRequest(t, rest, "10.0.0.8:1"); w.Code != http.StatusTooManyRequests {
		t.Fatalf("second REST request = %d, want 429 (configured burst 1)", w.Code)
	}
}

// TestGitRateLimitSeparateBucketState: the git-surface limiter and a REST
// limiter built from the same config never share buckets — exhausting one
// surface's budget must not starve the other (issue #21).
func TestGitRateLimitSeparateBucketState(t *testing.T) {
	cfg := RateLimitConfig{RPS: 0.001, Burst: 1}
	gitMW, err := NewGitRateLimit(cfg)
	if err != nil {
		t.Fatal(err)
	}
	restRL, err := newRateLimiter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	git := gitMW(okHandler())
	rest := restRL.limit(routeRead)(okHandler())

	// Drain the git bucket (raised to the gitMinBurst floor).
	for i := 0; i < gitMinBurst; i++ {
		if w := rlRequest(t, git, "10.0.0.7:1"); w.Code != http.StatusOK {
			t.Fatalf("git request %d = %d, want 200", i, w.Code)
		}
	}
	if w := rlRequest(t, git, "10.0.0.7:1"); w.Code != http.StatusTooManyRequests {
		t.Fatalf("git surface not exhausted: %d, want 429", w.Code)
	}
	// The REST surface still has its own full bucket for the same client.
	if w := rlRequest(t, rest, "10.0.0.7:1"); w.Code != http.StatusOK {
		t.Fatalf("REST request after git exhaustion = %d, want 200 (shared bucket state?)", w.Code)
	}
}

// TestGitRateLimitExchangeHysteresis (Codex round 9): once a client is
// limited, the git bucket latches and reopens only when a full exchange
// reserve has refilled — one refilled token must NOT admit the first
// request of an exchange whose followers would then 429 forever. The
// advertised Retry-After names the reserve wait, and after it a whole
// multi-request exchange completes.
func TestGitRateLimitExchangeHysteresis(t *testing.T) {
	rl, now := newTestLimiter(RateLimitConfig{RPS: 0.01, Burst: gitMinBurst}, time.Unix(1000, 0))
	const key = "10.0.0.42"

	// Drain the full burst, then latch.
	for i := 0; i < gitMinBurst; i++ {
		if ok, _ := rl.admitReserve(key, gitMinBurst); !ok {
			t.Fatalf("burst request %d denied", i)
		}
	}
	ok, retryAfter := rl.admitReserve(key, gitMinBurst)
	if ok {
		t.Fatal("drained bucket admitted a request")
	}
	if want := int(math.Ceil(gitMinBurst / 0.01)); retryAfter != want {
		t.Fatalf("latched Retry-After = %d, want %d (full reserve wait)", retryAfter, want)
	}

	// One token refilled (the pre-hysteresis trap): still latched.
	*now = now.Add(100 * time.Second) // exactly 1 token at 0.01 RPS
	if ok, _ := rl.admitReserve(key, gitMinBurst); ok {
		t.Fatal("latched bucket admitted on a single refilled token")
	}

	// After the full reserve wait, an entire exchange completes.
	*now = now.Add(time.Duration(gitMinBurst) * 100 * time.Second)
	for i := 0; i < gitMinBurst; i++ {
		if ok, _ := rl.admitReserve(key, gitMinBurst); !ok {
			t.Fatalf("post-reserve exchange request %d denied", i)
		}
	}

	// REST semantics are unchanged by the shared latch field: reserve=1
	// unlatches on the next single token.
	if ok, _ := rl.admitReserve(key, gitMinBurst); ok {
		t.Fatal("bucket should be drained again")
	}
	*now = now.Add(100 * time.Second)
	if ok, _ := rl.admit(key); !ok {
		t.Fatal("reserve=1 admit must unlatch on one refilled token")
	}
}

// --- trusted-proxy identity (issue #30) --------------------------------------

// rlRequestXFF drives one request with a fixed peer address and
// X-Forwarded-For header through h.
func rlRequestXFF(t *testing.T, h http.Handler, remoteAddr, xff string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, "/probe", nil)
	r.RemoteAddr = remoteAddr
	r.Header.Set("X-Forwarded-For", xff)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

// TestRateLimitMappedPrefixNormalization: the limiter constructors — not
// just the CLI env parser — normalize IPv4-mapped allowlist prefixes, so a
// direct api.New/NewGitRateLimit caller passing ::ffff:10.0.0.0/104 still
// matches IPv4 peers (Codex round 2); mapped prefixes below /96 are a
// config error on both constructors.
func TestRateLimitMappedPrefixNormalization(t *testing.T) {
	mapped := []netip.Prefix{netip.MustParsePrefix("::ffff:10.0.0.0/104")} // == 10.0.0.0/8
	cfg := RateLimitConfig{RPS: 0.001, Burst: 1, TrustedProxies: mapped}
	rl, err := newRateLimiter(cfg)
	if err != nil || rl == nil {
		t.Fatalf("newRateLimiter(mapped /104) = (%v, %v), want a limiter", rl, err)
	}
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = "10.0.0.5:1234"
	r.Header.Set("X-Forwarded-For", "203.0.113.9")
	if got := rl.clientKey(r); got != "203.0.113.9" {
		t.Errorf("clientKey via mapped-prefix-trusted peer = %q, want 203.0.113.9", got)
	}
	// The caller's slice is not mutated by normalization.
	if mapped[0].String() != "::ffff:10.0.0.0/104" {
		t.Errorf("caller slice mutated to %v", mapped[0])
	}

	tooShort := RateLimitConfig{RPS: 1, Burst: 1, TrustedProxies: []netip.Prefix{netip.MustParsePrefix("::ffff:10.0.0.0/64")}}
	if _, err := newRateLimiter(tooShort); err == nil {
		t.Error("newRateLimiter(mapped /64) = nil error, want config error")
	}
	if _, err := NewGitRateLimit(tooShort); err == nil {
		t.Error("NewGitRateLimit(mapped /64) = nil error, want config error")
	}
}

// TestRateLimitTrustedProxyBuckets: with the allowlist configured, clients
// arriving through a trusted proxy get their own buckets keyed by the
// forwarded address, while a spoofed XFF from an unlisted peer still shares
// that peer's RemoteAddr bucket.
func TestRateLimitTrustedProxyBuckets(t *testing.T) {
	trusted := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}
	rl, _ := newTestLimiter(RateLimitConfig{RPS: 0.001, Burst: 1, TrustedProxies: trusted}, time.Unix(1000, 0))
	h := rl.limit(routeRead)(okHandler())
	const proxy = "10.0.0.5:1234"

	// Two clients behind the same proxy connection source: separate buckets.
	if w := rlRequestXFF(t, h, proxy, "203.0.113.9"); w.Code != http.StatusOK {
		t.Fatalf("client A via proxy = %d, want 200", w.Code)
	}
	if w := rlRequestXFF(t, h, proxy, "198.51.100.7"); w.Code != http.StatusOK {
		t.Fatalf("client B via proxy = %d, want 200 (collapsed into A's bucket?)", w.Code)
	}
	if w := rlRequestXFF(t, h, proxy, "203.0.113.9"); w.Code != http.StatusTooManyRequests {
		t.Fatalf("client A second request = %d, want 429", w.Code)
	}
	if w := rlRequestXFF(t, h, proxy, "198.51.100.7"); w.Code != http.StatusTooManyRequests {
		t.Fatalf("client B second request = %d, want 429", w.Code)
	}

	// An unlisted peer cannot mint buckets via XFF: both spoofed values
	// share the peer's RemoteAddr bucket.
	if w := rlRequestXFF(t, h, "192.0.2.1:1", "203.0.113.100"); w.Code != http.StatusOK {
		t.Fatalf("untrusted peer first request = %d, want 200", w.Code)
	}
	if w := rlRequestXFF(t, h, "192.0.2.1:1", "203.0.113.101"); w.Code != http.StatusTooManyRequests {
		t.Fatalf("untrusted peer spoofed second request = %d, want 429 (XFF minted a bucket)", w.Code)
	}
}

// TestRateLimitNoTrustBucketCollapse: the default (empty allowlist) keeps
// today's behavior — XFF is ignored and every client behind one proxy
// address shares the proxy's bucket.
func TestRateLimitNoTrustBucketCollapse(t *testing.T) {
	rl, _ := newTestLimiter(RateLimitConfig{RPS: 0.001, Burst: 1}, time.Unix(1000, 0))
	h := rl.limit(routeRead)(okHandler())
	const proxy = "10.0.0.5:1234"

	if w := rlRequestXFF(t, h, proxy, "203.0.113.9"); w.Code != http.StatusOK {
		t.Fatalf("first request = %d, want 200", w.Code)
	}
	if w := rlRequestXFF(t, h, proxy, "198.51.100.7"); w.Code != http.StatusTooManyRequests {
		t.Fatalf("second client via same proxy = %d, want 429 (shared RemoteAddr bucket)", w.Code)
	}
}

// TestGitRateLimitTrustedProxyIdentity: the git-surface limiter resolves
// client identity through the same trusted-proxy plumbing as the REST
// limiter (issue #30) — forwarded clients get separate buckets behind a
// trusted proxy, and unlisted peers cannot mint buckets via XFF.
func TestGitRateLimitTrustedProxyIdentity(t *testing.T) {
	trusted := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}
	mw, err := NewGitRateLimit(RateLimitConfig{RPS: 0.001, Burst: 1, TrustedProxies: trusted})
	if err != nil {
		t.Fatal(err)
	}
	h := mw(okHandler())
	const proxy = "10.0.0.5:1234"

	// Drain client A's bucket (raised to the gitMinBurst floor).
	for i := 0; i < gitMinBurst; i++ {
		if w := rlRequestXFF(t, h, proxy, "203.0.113.9"); w.Code != http.StatusOK {
			t.Fatalf("client A request %d = %d, want 200", i, w.Code)
		}
	}
	if w := rlRequestXFF(t, h, proxy, "203.0.113.9"); w.Code != http.StatusTooManyRequests {
		t.Fatalf("client A over-burst = %d, want 429", w.Code)
	}
	// Client B behind the same proxy still has its own full bucket.
	if w := rlRequestXFF(t, h, proxy, "198.51.100.7"); w.Code != http.StatusOK {
		t.Fatalf("client B via proxy = %d, want 200 (git surface not honoring proxy identity)", w.Code)
	}

	// An unlisted peer's XFF is ignored: all spoofs drain one bucket.
	for i := 0; i < gitMinBurst; i++ {
		w := rlRequestXFF(t, h, "192.0.2.1:1", fmt.Sprintf("203.0.113.%d", 100+i))
		if w.Code != http.StatusOK {
			t.Fatalf("untrusted peer request %d = %d, want 200", i, w.Code)
		}
	}
	if w := rlRequestXFF(t, h, "192.0.2.1:1", "203.0.113.200"); w.Code != http.StatusTooManyRequests {
		t.Fatalf("untrusted peer over-burst = %d, want 429 (spoofed XFF minted buckets)", w.Code)
	}
}
