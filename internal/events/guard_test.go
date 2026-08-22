package events

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

func TestTargetGuardRejectsDeniedDialTargets(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{
		"127.0.0.1",              // loopback v4
		"::1",                    // loopback v6
		"10.1.2.3",               // RFC 1918 10/8
		"172.16.0.1",             // RFC 1918 172.16/12 lower bound
		"172.31.255.254",         // RFC 1918 172.16/12 upper bound
		"192.168.1.1",            // RFC 1918 192.168/16
		"100.64.0.1",             // RFC 6598 shared lower bound
		"100.100.100.200",        // RFC 6598 shared internal target
		"100.127.255.254",        // RFC 6598 shared upper bound
		"169.254.1.1",            // IPv4 link-local
		"fe80::1",                // IPv6 link-local
		"fc00::1",                // IPv6 ULA lower half
		"fd12:3456::1",           // IPv6 ULA commonly allocated half
		"0.0.0.0",                // unspecified v4
		"::",                     // unspecified v6
		"169.254.169.254",        // cloud metadata v4
		"fd00:ec2::254",          // cloud metadata v6
		"::ffff:127.0.0.1",       // mapped loopback
		"::ffff:10.2.3.4",        // mapped RFC 1918
		"::ffff:169.254.169.254", // mapped cloud metadata
	} {
		raw := raw
		t.Run(strings.ReplaceAll(raw, ":", "_"), func(t *testing.T) {
			t.Parallel()
			var dialed atomic.Int32
			guard := TargetGuard{dialAddrPort: func(context.Context, string, netip.AddrPort) (net.Conn, error) {
				dialed.Add(1)
				return nil, errors.New("unexpected dial")
			}}
			_, err := guard.dialContext(context.Background(), "tcp", net.JoinHostPort(raw, "80"))
			if err == nil {
				t.Fatalf("dial to denied target %s succeeded", raw)
			}
			if got := dialed.Load(); got != 0 {
				t.Fatalf("dialer called %d times for denied target %s, want 0", got, raw)
			}
		})
	}
}

func TestTargetGuardFailsClosedOnResolutionProblems(t *testing.T) {
	t.Parallel()

	public := netip.MustParseAddr("198.51.100.10")
	private := netip.MustParseAddr("10.0.0.1")
	invalid := netip.Addr{}
	cases := map[string]func(context.Context, string) ([]netip.Addr, error){
		"resolution failure": func(context.Context, string) ([]netip.Addr, error) {
			return nil, errors.New("resolver unavailable")
		},
		"empty answer": func(context.Context, string) ([]netip.Addr, error) {
			return nil, nil
		},
		"mixed public and private": func(context.Context, string) ([]netip.Addr, error) {
			return []netip.Addr{public, private}, nil
		},
		"invalid answer": func(context.Context, string) ([]netip.Addr, error) {
			return []netip.Addr{invalid}, nil
		},
	}
	for name, lookup := range cases {
		lookup := lookup
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var dialed atomic.Int32
			guard := TargetGuard{
				LookupNetIP: lookup,
				dialAddrPort: func(context.Context, string, netip.AddrPort) (net.Conn, error) {
					dialed.Add(1)
					return nil, errors.New("unexpected dial")
				},
			}
			_, err := guard.dialContext(context.Background(), "tcp", "webhook.example:443")
			if err == nil {
				t.Fatal("dial succeeded; want fail-closed rejection")
			}
			if got := dialed.Load(); got != 0 {
				t.Fatalf("dialer called %d times, want 0", got)
			}
		})
	}
}

func TestTargetGuardRejectsMalformedDialInputs(t *testing.T) {
	t.Parallel()

	guard := TargetGuard{}
	for name, tc := range map[string]struct {
		network string
		addr    string
	}{
		"non TCP network": {network: "udp", addr: "example.com:80"},
		"missing port":    {network: "tcp", addr: "example.com"},
		"bad port":        {network: "tcp", addr: "example.com:not-a-port"},
		"overflow port":   {network: "tcp", addr: "example.com:65536"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := guard.dialContext(context.Background(), tc.network, tc.addr); err == nil {
				t.Fatalf("dialContext(%q, %q) succeeded", tc.network, tc.addr)
			}
		})
	}
}

func TestTargetGuardPublicLiteralDialsTestServer(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	actual, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, portStr, err := net.SplitHostPort(actual.Host)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		t.Fatal(err)
	}
	public := netip.MustParseAddr("198.51.100.10")
	guard := TargetGuard{dialAddrPort: func(ctx context.Context, network string, got netip.AddrPort) (net.Conn, error) {
		if got != netip.AddrPortFrom(public, uint16(port)) {
			return nil, errors.New("guard dialed an unexpected address")
		}
		var d net.Dialer
		return d.DialContext(ctx, network, actual.Host)
	}}

	resp, err := guard.NewClient().Post("http://"+netip.AddrPortFrom(public, uint16(port)).String(), "application/json", strings.NewReader(`{"ok":true}`))
	if err != nil {
		t.Fatalf("post to admitted public literal: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", resp.StatusCode)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("test server saw %d requests, want 1", got)
	}
}

func TestTargetGuardRejectsDNSRebindingAtDialTime(t *testing.T) {
	t.Parallel()

	var lookups atomic.Int32
	var dials atomic.Int32
	guard := TargetGuard{
		LookupNetIP: func(context.Context, string) ([]netip.Addr, error) {
			if lookups.Add(1) == 1 {
				return []netip.Addr{netip.MustParseAddr("198.51.100.10")}, nil
			}
			return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
		},
		dialAddrPort: func(context.Context, string, netip.AddrPort) (net.Conn, error) {
			dials.Add(1)
			return nil, errors.New("unexpected dial")
		},
	}
	const rawURL = "http://rebinding.example/hook"
	if err := guard.CheckRegistration(context.Background(), rawURL); err != nil {
		t.Fatalf("registration-time public hint rejected: %v", err)
	}
	resp, err := guard.NewClient().Post(rawURL, "application/json", strings.NewReader("{}"))
	if resp != nil {
		_ = resp.Body.Close()
	}
	if err == nil {
		t.Fatal("delivery succeeded after hostname rebound to loopback")
	}
	if got := lookups.Load(); got != 2 {
		t.Fatalf("resolver calls = %d, want 2 (registration + authoritative dial)", got)
	}
	if got := dials.Load(); got != 0 {
		t.Fatalf("socket dialer called %d times, want 0 after private re-resolution", got)
	}
}

func TestTargetGuardRegistrationCourtesyCheck(t *testing.T) {
	t.Parallel()

	guard := TargetGuard{LookupNetIP: func(_ context.Context, host string) ([]netip.Addr, error) {
		switch host {
		case "private.example":
			return []netip.Addr{netip.MustParseAddr("10.0.0.1")}, nil
		case "mixed.example":
			return []netip.Addr{netip.MustParseAddr("198.51.100.10"), netip.MustParseAddr("fc00::1")}, nil
		case "unresolved.example":
			return nil, errors.New("temporary resolver failure")
		default:
			return []netip.Addr{netip.MustParseAddr("198.51.100.10")}, nil
		}
	}}
	for name, tc := range map[string]struct {
		rawURL string
		wantOK bool
	}{
		"private literal denied":     {rawURL: "http://192.168.1.2/hook"},
		"mapped literal denied":      {rawURL: "http://[::ffff:10.0.0.1]/hook"},
		"private resolution denied":  {rawURL: "https://private.example/hook"},
		"mixed resolution denied":    {rawURL: "https://mixed.example/hook"},
		"public literal accepted":    {rawURL: "https://198.51.100.10/hook", wantOK: true},
		"public resolution accepted": {rawURL: "https://public.example/hook", wantOK: true},
		// Registration is courtesy-only: a temporary resolution failure
		// does not promise reachability; dial time later fails closed.
		"resolution failure accepted": {rawURL: "https://unresolved.example/hook", wantOK: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			err := guard.CheckRegistration(context.Background(), tc.rawURL)
			if tc.wantOK && err != nil {
				t.Fatalf("CheckRegistration: %v", err)
			}
			if !tc.wantOK && err == nil {
				t.Fatal("CheckRegistration succeeded, want denial")
			}
		})
	}
}

func TestTargetGuardRefusesRedirectsEvenWhenPrivateAllowed(t *testing.T) {
	for _, status := range []int{
		http.StatusMovedPermanently,
		http.StatusFound,
		http.StatusTemporaryRedirect,
		http.StatusPermanentRedirect,
	} {
		status := status
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			var frontRequests atomic.Int32
			var targetRequests atomic.Int32
			target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				targetRequests.Add(1)
			}))
			defer target.Close()
			front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				frontRequests.Add(1)
				http.Redirect(w, r, target.URL, status)
			}))
			defer front.Close()

			lookup := &fakeLookup{hooks: map[string]Webhook{
				"wh_redirect": {ID: "wh_redirect", URL: front.URL, Secret: "s"},
			}}
			d := &Deliverer{Webhooks: lookup, AllowPrivate: true, Logger: testLogger()}
			err := d.Attempt(context.Background(), Delivery{WebhookID: "wh_redirect", Event: EventPush, Payload: []byte("{}")})
			if err == nil || !strings.Contains(err.Error(), "refusing to follow redirect") {
				t.Fatalf("Attempt error = %v, want explicit redirect refusal", err)
			}
			if got := frontRequests.Load(); got != 1 {
				t.Fatalf("redirecting server saw %d requests, want 1", got)
			}
			if got := targetRequests.Load(); got != 0 {
				t.Fatalf("redirect target saw %d requests, want 0", got)
			}
		})
	}
}

func TestTargetGuardAllowPrivateScope(t *testing.T) {
	t.Parallel()

	admitted := []string{
		"127.0.0.1",
		"10.0.0.1",
		"100.64.0.1",
		"169.254.1.1",
		"fe80::1",
		"fc00::1",
		"::ffff:10.0.0.1",
	}
	for _, raw := range admitted {
		raw := raw
		t.Run("admits_"+strings.ReplaceAll(raw, ":", "_"), func(t *testing.T) {
			t.Parallel()
			wantDial := errors.New("dial observed")
			var gotAddr netip.Addr
			guard := TargetGuard{
				AllowPrivate: true,
				dialAddrPort: func(_ context.Context, _ string, ap netip.AddrPort) (net.Conn, error) {
					gotAddr = ap.Addr()
					return nil, wantDial
				},
			}
			_, err := guard.dialContext(context.Background(), "tcp", net.JoinHostPort(raw, "80"))
			if !errors.Is(err, wantDial) {
				t.Fatalf("dial error = %v, want admitted address to reach dialer", err)
			}
			if gotAddr.Is4In6() {
				t.Fatalf("dial address %s remained IPv4-mapped; want canonical IPv4", gotAddr)
			}
		})
	}

	for _, raw := range []string{
		"169.254.169.254",
		"fd00:ec2::254",
		"::ffff:169.254.169.254",
		"0.0.0.0",
		"::",
		"224.0.0.1",
	} {
		raw := raw
		t.Run("still_denies_"+strings.ReplaceAll(raw, ":", "_"), func(t *testing.T) {
			t.Parallel()
			var dialed atomic.Int32
			guard := TargetGuard{
				AllowPrivate: true,
				dialAddrPort: func(context.Context, string, netip.AddrPort) (net.Conn, error) {
					dialed.Add(1)
					return nil, errors.New("unexpected dial")
				},
			}
			if _, err := guard.dialContext(context.Background(), "tcp", net.JoinHostPort(raw, "80")); err == nil {
				t.Fatalf("AllowPrivate admitted %s", raw)
			}
			if got := dialed.Load(); got != 0 {
				t.Fatalf("dialer called %d times for still-denied target %s", got, raw)
			}
		})
	}
}

func TestTargetGuardTransportCannotUseEnvironmentProxy(t *testing.T) {
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:3128")
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:3128")

	client := (TargetGuard{}).NewClient()
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", client.Transport)
	}
	if transport.Proxy != nil {
		t.Fatal("webhook transport honors environment proxies; target validation could be bypassed")
	}
	if !transport.DisableKeepAlives {
		t.Fatal("webhook transport keeps connections alive; each attempt must get a fresh guarded dial")
	}
}

func TestTargetGuardAllowPrivateAdmitsLoopback(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	resp, err := (TargetGuard{AllowPrivate: true}).NewClient().Post(srv.URL, "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("post with AllowPrivate: %v", err)
	}
	_ = resp.Body.Close()
	if got := requests.Load(); got != 1 {
		t.Fatalf("loopback server saw %d requests, want 1", got)
	}
}
