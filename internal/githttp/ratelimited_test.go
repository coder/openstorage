package githttp

// Unit tests for the throttling header pair (ratelimited.go, issue #150):
// the exact RateLimit wire shape shared by every HTTP-level 429, and
// lfsError's stamping seam — 429s get the pair, other statuses stay bare.

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSetRateLimitedShape(t *testing.T) {
	h := http.Header{}
	SetRateLimited(h, 10)
	if ra := h.Get("Retry-After"); ra != "10" {
		t.Errorf("Retry-After = %q, want %q", ra, "10")
	}
	if rl := h.Get("RateLimit"); rl != `"default";r=0;t=10` {
		t.Errorf("RateLimit = %q, want %q", rl, `"default";r=0;t=10`)
	}
}

// TestLFSError429Headers: every LFS-surface 429 flows through lfsError, so
// the stamping there is what guarantees no busy path can answer 429 without
// the documented header pair — and non-throttled errors must not leak it.
func TestLFSError429Headers(t *testing.T) {
	w := httptest.NewRecorder()
	lfsError(w, http.StatusTooManyRequests, "busy")
	if w.Header().Get("Retry-After") != "10" || w.Header().Get("RateLimit") != `"default";r=0;t=10` {
		t.Errorf("429 headers = (Retry-After %q, RateLimit %q), want (10, %q)",
			w.Header().Get("Retry-After"), w.Header().Get("RateLimit"), `"default";r=0;t=10`)
	}

	w = httptest.NewRecorder()
	lfsError(w, http.StatusInsufficientStorage, "quota")
	if w.Header().Get("Retry-After") != "" || w.Header().Get("RateLimit") != "" {
		t.Errorf("507 must not carry throttling headers, got (Retry-After %q, RateLimit %q)",
			w.Header().Get("Retry-After"), w.Header().Get("RateLimit"))
	}
}
