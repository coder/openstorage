// Hot-path benchmarks for pre-auth rate limiting (issue #92): clientKey and
// admitReserve run on every REST and git request before auth ever sees it.
// admitReserve serializes every caller on one central mutex, so the
// RunParallel matrix scales b.SetParallelism to expose that contention —
// the worker count is parallelism × GOMAXPROCS, each worker keying its own
// bucket (distinct clients, the production shape). The limiter config
// refills far faster than the benchmark can drain (RPS 1e9 against a
// mutex-serialized admit path), so the timed path is always the admit
// branch, never the 429 branch.
package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync/atomic"
	"testing"
)

// benchLimiter builds an enabled limiter on the real clock.
func benchLimiter(b *testing.B, cfg RateLimitConfig) *rateLimiter {
	b.Helper()
	rl, err := newRateLimiter(cfg)
	if err != nil || rl == nil {
		b.Fatalf("newRateLimiter(%+v) = (%v, %v), want a limiter", cfg, rl, err)
	}
	return rl
}

func BenchmarkAdmitReserve(b *testing.B) {
	for _, p := range []int{1, 8, 64, 256} {
		b.Run(fmt.Sprintf("parallelism=%d", p), func(b *testing.B) {
			rl := benchLimiter(b, RateLimitConfig{RPS: 1e9, Burst: 1 << 20})
			var nextClient atomic.Int64
			b.ReportAllocs()
			b.SetParallelism(p)
			b.RunParallel(func(pb *testing.PB) {
				id := nextClient.Add(1)
				key := fmt.Sprintf("10.%d.%d.%d", id>>16&0xff, id>>8&0xff, id&0xff)
				for pb.Next() {
					if ok, retry := rl.admitReserve(key, 1); !ok {
						// Errorf, not Fatalf: FailNow must not run on a
						// RunParallel worker goroutine.
						b.Errorf("admitReserve(%q) denied (retry %ds), want the config to keep buckets full", key, retry)
						return
					}
				}
			})
		})
	}
}

func BenchmarkClientKey(b *testing.B) {
	trusted := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}
	// A long all-trusted chain forces the full right-to-left walk before
	// failing closed to the peer key (the proxy's own traffic shape).
	longChain := ""
	for i := range 64 {
		if i > 0 {
			longChain += ", "
		}
		longChain += fmt.Sprintf("10.0.%d.%d", i/256, i%256+1)
	}
	cases := []struct {
		name       string
		trusted    []netip.Prefix
		remoteAddr string
		xff        string
		want       string
	}{
		{"peer", nil, "203.0.113.7:52814", "", "203.0.113.7"},
		{"xff-3hop", trusted, "10.0.0.1:9999", "198.51.100.9, 10.0.0.2, 10.0.0.3", "198.51.100.9"},
		{"xff-all-trusted-64hop", trusted, "10.0.0.1:9999", longChain, "10.0.0.1"},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			rl := benchLimiter(b, RateLimitConfig{RPS: 1, Burst: 1, TrustedProxies: tc.trusted})
			req := httptest.NewRequest(http.MethodGet, "http://api.example/v1/repos", nil)
			req.RemoteAddr = tc.remoteAddr
			if tc.xff != "" {
				req.Header.Set("X-Forwarded-For", tc.xff)
			}
			b.ReportAllocs()
			for b.Loop() {
				if got := rl.clientKey(req); got != tc.want {
					b.Fatalf("clientKey = %q, want %q", got, tc.want)
				}
			}
		})
	}
}
