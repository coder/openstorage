package events

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/open-storage/internal/meta"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeLookup is an in-memory WebhookLookup counting resolutions.
type fakeLookup struct {
	mu    sync.Mutex
	hooks map[string]Webhook
	calls int
}

func (f *fakeLookup) GetWebhook(_ context.Context, id string) (Webhook, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	wh, ok := f.hooks[id]
	if !ok {
		return Webhook{}, fmt.Errorf("webhook %s: %w", id, meta.ErrNotFound)
	}
	return wh, nil
}

// capturedRequest is one delivery observed by the test receiver.
type capturedRequest struct {
	event       string
	signature   string
	userAgent   string
	contentType string
	body        []byte
}

// receiver is an httptest handler that records deliveries and answers with
// scripted statuses (last status repeats once the script is exhausted).
type receiver struct {
	mu       sync.Mutex
	statuses []int
	got      []capturedRequest
}

func (rc *receiver) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			panic(err)
		}
		rc.mu.Lock()
		rc.got = append(rc.got, capturedRequest{
			event:       r.Header.Get(HeaderEvent),
			signature:   r.Header.Get(HeaderSignature),
			userAgent:   r.Header.Get("User-Agent"),
			contentType: r.Header.Get("Content-Type"),
			body:        body,
		})
		status := rc.statuses[min(len(rc.got), len(rc.statuses))-1]
		rc.mu.Unlock()
		w.WriteHeader(status)
	}
}

func (rc *receiver) requests() []capturedRequest {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return append([]capturedRequest(nil), rc.got...)
}

// steppingClock returns a distinct, strictly increasing timestamp per call
// so per-attempt re-signing is observable.
func steppingClock(start int64, stepSecs int64) func() time.Time {
	var mu sync.Mutex
	n := int64(0)
	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		n++
		return time.Unix(start+(n-1)*stepSecs, 0)
	}
}

// sigTimestamp extracts the t= value from a signature header.
func sigTimestamp(t *testing.T, sig string) int64 {
	t.Helper()
	rest, ok := strings.CutPrefix(sig, "t=")
	if !ok {
		t.Fatalf("signature %q missing t= prefix", sig)
	}
	tsStr, _, ok := strings.Cut(rest, ",")
	if !ok {
		t.Fatalf("signature %q missing comma", sig)
	}
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		t.Fatalf("signature timestamp %q: %v", tsStr, err)
	}
	return ts
}

func TestAttemptSignsAndPosts(t *testing.T) {
	rc := &receiver{statuses: []int{http.StatusNoContent}} // any 2xx counts
	srv := httptest.NewServer(rc.handler())
	defer srv.Close()

	const secret = "whsec_attempt"
	body := []byte(`{"repository":{"id":"repo_ab","url":"acme/w"}}`)
	d := &Deliverer{
		Webhooks: &fakeLookup{hooks: map[string]Webhook{
			"wh_1": {ID: "wh_1", URL: srv.URL, Secret: secret},
		}},
		AllowPrivate: true, // httptest receivers are loopback (issue #53)
		Now:          steppingClock(1737369000, 10),
		Logger:       testLogger(),
	}
	if err := d.Attempt(context.Background(), Delivery{WebhookID: "wh_1", Event: EventPush, Payload: body}); err != nil {
		t.Fatalf("Attempt: %v", err)
	}

	reqs := rc.requests()
	if len(reqs) != 1 {
		t.Fatalf("got %d requests, want 1", len(reqs))
	}
	got := reqs[0]
	if got.event != EventPush {
		t.Errorf("%s = %q, want %q", HeaderEvent, got.event, EventPush)
	}
	if got.userAgent != UserAgent {
		t.Errorf("User-Agent = %q, want %q", got.userAgent, UserAgent)
	}
	if got.contentType != "application/json" {
		t.Errorf("Content-Type = %q", got.contentType)
	}
	if string(got.body) != string(body) {
		t.Errorf("body = %q, want %q", got.body, body)
	}
	// Receiver-side verification: recompute the signature from the header
	// timestamp + raw body, exactly like the vendor SDK helpers.
	ts := sigTimestamp(t, got.signature)
	if want := Sign(secret, ts, got.body); got.signature != want {
		t.Errorf("signature does not verify:\n got %s\nwant %s", got.signature, want)
	}
	if ts != 1737369000 {
		t.Errorf("signing timestamp = %d, want the injected clock's 1737369000", ts)
	}
}

func TestDeliverResignsOnRetry(t *testing.T) {
	rc := &receiver{statuses: []int{http.StatusInternalServerError, http.StatusOK}}
	srv := httptest.NewServer(rc.handler())
	defer srv.Close()

	const secret = "whsec_retry"
	d := &Deliverer{
		Webhooks: &fakeLookup{hooks: map[string]Webhook{
			"wh_1": {ID: "wh_1", URL: srv.URL, Secret: secret},
		}},
		AllowPrivate: true, // httptest receivers are loopback (issue #53)
		Now:          steppingClock(1737369000, 30),
		Backoff:      func(int) time.Duration { return 0 },
		Logger:       testLogger(),
	}
	err := d.Deliver(context.Background(), Delivery{WebhookID: "wh_1", Event: EventPush, Payload: []byte(`{"x":1}`)})
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	reqs := rc.requests()
	if len(reqs) != 2 {
		t.Fatalf("got %d requests, want 2 (fail then success)", len(reqs))
	}
	ts1, ts2 := sigTimestamp(t, reqs[0].signature), sigTimestamp(t, reqs[1].signature)
	if ts2 <= ts1 {
		t.Errorf("retry timestamp %d not after first attempt %d (no re-sign?)", ts2, ts1)
	}
	if reqs[0].signature == reqs[1].signature {
		t.Errorf("retry reused the first attempt's signature %s", reqs[0].signature)
	}
	for i, r := range reqs {
		if want := Sign(secret, sigTimestamp(t, r.signature), r.body); r.signature != want {
			t.Errorf("attempt %d signature does not verify", i+1)
		}
	}
}

func TestDeliverDropsGoneWebhook(t *testing.T) {
	rc := &receiver{statuses: []int{http.StatusOK}}
	srv := httptest.NewServer(rc.handler())
	defer srv.Close()

	lookup := &fakeLookup{hooks: map[string]Webhook{}} // webhook deleted
	d := &Deliverer{Webhooks: lookup, Logger: testLogger(),
		Backoff: func(int) time.Duration { return 0 }}
	err := d.Deliver(context.Background(), Delivery{WebhookID: "wh_gone", Event: EventPush, Payload: []byte("{}")})
	if !errors.Is(err, ErrWebhookGone) {
		t.Fatalf("Deliver = %v, want ErrWebhookGone", err)
	}
	if lookup.calls != 1 {
		t.Errorf("lookup calls = %d, want 1 (terminal, no retries)", lookup.calls)
	}
	if got := rc.requests(); len(got) != 0 {
		t.Errorf("receiver saw %d requests, want 0", len(got))
	}
}

func TestDeliverGivesUpAfterMaxAttempts(t *testing.T) {
	rc := &receiver{statuses: []int{http.StatusBadGateway}}
	srv := httptest.NewServer(rc.handler())
	defer srv.Close()

	d := &Deliverer{
		Webhooks: &fakeLookup{hooks: map[string]Webhook{
			"wh_1": {ID: "wh_1", URL: srv.URL, Secret: "s"},
		}},
		AllowPrivate: true, // httptest receivers are loopback (issue #53)
		MaxAttempts:  3,
		Backoff:      func(int) time.Duration { return 0 },
		Logger:       testLogger(),
	}
	err := d.Deliver(context.Background(), Delivery{WebhookID: "wh_1", Event: EventPush, Payload: []byte("{}")})
	if err == nil || !strings.Contains(err.Error(), "giving up after 3 attempts") {
		t.Fatalf("Deliver = %v, want giving-up error", err)
	}
	if got := len(rc.requests()); got != 3 {
		t.Errorf("receiver saw %d requests, want exactly MaxAttempts=3", got)
	}
}

func TestDeliverStopsOnContextCancel(t *testing.T) {
	rc := &receiver{statuses: []int{http.StatusInternalServerError}}
	srv := httptest.NewServer(rc.handler())
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	d := &Deliverer{
		Webhooks: &fakeLookup{hooks: map[string]Webhook{
			"wh_1": {ID: "wh_1", URL: srv.URL, Secret: "s"},
		}},
		AllowPrivate: true, // httptest receivers are loopback (issue #53)
		Backoff:      func(int) time.Duration { return time.Hour },
		Logger:       testLogger(),
	}
	err := d.Deliver(ctx, Delivery{WebhookID: "wh_1", Event: EventPush, Payload: []byte("{}")})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Deliver = %v, want context deadline error", err)
	}
	if got := len(rc.requests()); got != 1 {
		t.Errorf("receiver saw %d requests, want 1 (canceled during backoff)", got)
	}
}

// TestZeroValueDelivererRefusesLoopback pins the fail-closed default
// (issue #53): a Deliverer constructed without an explicit Client or
// AllowPrivate must NOT reach a loopback receiver — the SSRF guard denies
// the dial before any packet leaves the process.
func TestZeroValueDelivererRefusesLoopback(t *testing.T) {
	rc := &receiver{statuses: []int{http.StatusOK}}
	srv := httptest.NewServer(rc.handler())
	defer srv.Close()

	d := &Deliverer{
		Webhooks: &fakeLookup{hooks: map[string]Webhook{
			"wh_1": {ID: "wh_1", URL: srv.URL, Secret: "s"},
		}},
		Logger: testLogger(),
	}
	err := d.Attempt(context.Background(), Delivery{WebhookID: "wh_1", Event: EventPush, Payload: []byte("{}")})
	if err == nil {
		t.Fatal("Attempt delivered to a loopback target: SSRF dial guard missing")
	}
	if got := len(rc.requests()); got != 0 {
		t.Errorf("loopback receiver saw %d requests, want 0 (denied before connect)", got)
	}
}

// TestZeroValueDelivererRefusesRedirects pins the no-redirect policy
// (issue #53): a delivery answered with a redirect must fail without a
// second request — following it would reopen SSRF via redirect targets.
func TestZeroValueDelivererRefusesRedirects(t *testing.T) {
	target := &receiver{statuses: []int{http.StatusOK}}
	targetSrv := httptest.NewServer(target.handler())
	defer targetSrv.Close()
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, targetSrv.URL, http.StatusMovedPermanently)
	}))
	defer front.Close()

	d := &Deliverer{
		Webhooks: &fakeLookup{hooks: map[string]Webhook{
			"wh_1": {ID: "wh_1", URL: front.URL, Secret: "s"},
		}},
		Logger: testLogger(),
	}
	err := d.Attempt(context.Background(), Delivery{WebhookID: "wh_1", Event: EventPush, Payload: []byte("{}")})
	if err == nil {
		t.Fatal("Attempt succeeded through a redirect: redirects must be delivery failures")
	}
	if got := len(target.requests()); got != 0 {
		t.Errorf("redirect target saw %d requests, want 0 (redirect must not be followed)", got)
	}
}
