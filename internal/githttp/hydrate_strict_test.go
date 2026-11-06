package githttp

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/coder/openstorage/internal/auth"
)

// TestHydrateStrictHeld_RefusesStaleHeldRef: a strict hydrate (a local
// push run's loopback fetch) that skips a push-held ref reports
// ErrHeldRefStale, also where a lenient hydrate's held-skip verdict would
// short-cut the sync. A hold on a ref the cache already matches is no
// error, and the strict hydrate converges once the hold is gone.
func TestHydrateStrictHeld_RefusesStaleHeldRef(t *testing.T) {
	ctx := context.Background()
	st, bl, cacheDir := hydrateEnv(t)
	repoPath := mustHydrate(t, st, bl, cacheDir, "r")
	hold := mintHold(t, repoPath, "refs/heads/held")
	st.setGen(8)
	if _, refsErr, err := hydrateRefs(ctx, st, bl, cacheDir, "r", "main", true); refsErr != nil || err != nil {
		t.Fatalf("strict hydrate, hold on a ref the cache matches = (refsErr %v, err %v), want clean", refsErr, err)
	}

	// The authority moves under the hold; the SHA never needs to exist
	// locally because every sync skips the held ref.
	st.mu.Lock()
	st.refs["refs/heads/held"] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	st.mu.Unlock()
	st.setGen(9)
	mustHydrate(t, st, bl, cacheDir, "r")
	if !heldMarkerValid(repoPath, 9, HeadTarget("main")) {
		t.Fatal("lenient held skip left no held-skip verdict: the short-cut case below tests nothing")
	}
	for range 2 {
		if _, refsErr, err := hydrateRefs(ctx, st, bl, cacheDir, "r", "main", true); err != nil || !errors.Is(refsErr, ErrHeldRefStale) {
			t.Fatalf("strict hydrate of a stale held ref = (refsErr %v, err %v), want ErrHeldRefStale", refsErr, err)
		}
	}
	mustHydrate(t, st, bl, cacheDir, "r") // lenient callers keep serving

	removePushHoldFiles(hold)
	st.mu.Lock()
	delete(st.refs, "refs/heads/held")
	st.mu.Unlock()
	st.setGen(10)
	if _, refsErr, err := hydrateRefs(ctx, st, bl, cacheDir, "r", "main", true); refsErr != nil || err != nil {
		t.Fatalf("strict hydrate after the hold went = (refsErr %v, err %v), want clean", refsErr, err)
	}
}

// TestServerHydrate_StrictOnlyForPushRunFetch: only an upload-pack with a
// sync claim (a push run's loopback fetch) hydrates strictly, and it
// answers 423 Locked, which the worker reads as "restore".
func TestServerHydrate_StrictOnlyForPushRunFetch(t *testing.T) {
	st, bl, cacheDir := hydrateEnv(t)
	repoPath := mustHydrate(t, st, bl, cacheDir, "r")
	mintHold(t, repoPath, "refs/heads/held")
	st.mu.Lock()
	st.refs["refs/heads/held"] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	st.mu.Unlock()
	st.setGen(8)
	for _, tc := range []struct {
		service string
		claim   string
		want    bool
	}{
		{"upload-pack", "holder", true},
		{"upload-pack", "", false},
		{"receive-pack", "holder", false},
	} {
		s := &Server{Store: st, Blob: bl, CacheDir: cacheDir}
		_, err := s.hydrate(context.Background(), "r", tc.service, "main", &auth.Claims{SyncClaim: tc.claim})
		if got := errors.Is(err, ErrHeldRefStale); got != tc.want || (!tc.want && err != nil) {
			t.Errorf("%s claim=%q: hydrate err = %v, want stale=%v", tc.service, tc.claim, err, tc.want)
		}
		if tc.want {
			rec := httptest.NewRecorder()
			hydrateFailed(rec, "r", err)
			if rec.Code != http.StatusLocked {
				t.Errorf("stale held ref answered %d, want 423", rec.Code)
			}
		}
	}
}
