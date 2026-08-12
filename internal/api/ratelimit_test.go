package api

// Unit tests for the pre-auth per-client-IP token bucket (ratelimit.go):
// envelope shapes per route flavor, deterministic refill via the injectable
// clock, per-client isolation, stale-bucket eviction, the client key
// derivation (no XFF trust), and the alias re-dispatch exemption that keeps
// aliased requests at exactly one token.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
	rl := newRateLimiter(RateLimitConfig{})
	if rl != nil {
		t.Fatalf("newRateLimiter(zero) = %v, want nil (disabled)", rl)
	}
	h := rl.limit(routeRead)(okHandler())
	for i := 0; i < 10; i++ {
		if w := rlRequest(t, h, "10.0.0.1:1234"); w.Code != http.StatusOK {
			t.Fatalf("request %d through disabled limiter = %d, want 200", i, w.Code)
		}
	}
}

func TestRateLimitBurstValidationPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("newRateLimiter(RPS>0, Burst=0) did not panic")
		}
	}()
	newRateLimiter(RateLimitConfig{RPS: 1})
}

// newTestLimiter returns a limiter on a manual clock starting at base.
func newTestLimiter(cfg RateLimitConfig, base time.Time) (*rateLimiter, *time.Time) {
	rl := newRateLimiter(cfg)
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
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.RemoteAddr = tc.remoteAddr
			if tc.xff != "" {
				r.Header.Set("X-Forwarded-For", tc.xff)
			}
			if got := clientKey(r); got != tc.want {
				t.Errorf("clientKey(%q) = %q, want %q", tc.remoteAddr, got, tc.want)
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
