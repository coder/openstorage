package events

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"syscall"
	"time"
)

// TargetGuard decides which network addresses webhook traffic may reach
// (issue #53). Webhook URLs are attacker-controlled data — any org-level
// repo:write token can register one — so without a guard, deliveries are a
// blind-SSRF path from the replicas into loopback, RFC 1918, link-local,
// and cloud-metadata endpoints.
//
// The zero value is the production default: private-range targets are
// denied and hostnames resolve through net.DefaultResolver. Enforcement is
// layered per the issue-#53 triage:
//
//   - Registration time (CheckRegistration): fast-feedback courtesy only.
//     Acceptance is NOT a delivery guarantee — DNS can change between
//     registration and delivery.
//   - Dial time (NewClient): authoritative. The client's dialer re-resolves
//     and vets EVERY candidate address at connect time, then dials only the
//     vetted literals, closing the DNS-rebinding TOCTOU window that
//     URL-time validation leaves open.
type TargetGuard struct {
	// AllowPrivate admits loopback/private/link-local/ULA delivery targets
	// (CODESTORAGE_WEBHOOK_ALLOW_PRIVATE) for genuinely internal
	// deployments. Cloud metadata and addresses that can never be a
	// legitimate receiver (unspecified, multicast, broadcast) stay denied,
	// and redirects stay refused: the escape hatch relaxes only the private
	// receiver classes, not the rest of the policy.
	AllowPrivate bool

	// LookupNetIP resolves a hostname to candidate addresses; nil uses
	// net.DefaultResolver. Tests inject it to simulate DNS rebinding.
	LookupNetIP func(ctx context.Context, host string) ([]netip.Addr, error)

	// dialAddrPort dials one vetted ip:port literal; nil uses a
	// net.Dialer. Test seam: observing a call proves the guard admitted
	// the address without sending real packets.
	dialAddrPort func(ctx context.Context, network string, addrPort netip.AddrPort) (net.Conn, error)
}

// registrationLookupTimeout bounds the courtesy DNS lookup inside the
// registration handler so a slow resolver cannot wedge the request.
const registrationLookupTimeout = 2 * time.Second

// dialTimeout bounds one connect attempt to one vetted address; the
// client's overall 30s timeout bounds the whole delivery attempt.
const dialTimeout = 10 * time.Second

var (
	// broadcastV4 is the IPv4 limited-broadcast address, never a dial target.
	broadcastV4 = netip.AddrFrom4([4]byte{255, 255, 255, 255})
	// Cloud metadata remains denied even with AllowPrivate: the escape
	// hatch enables internal webhook receivers, not instance credentials.
	metadataV4 = netip.MustParseAddr("169.254.169.254")
	metadataV6 = netip.MustParseAddr("fd00:ec2::254")
	// RFC 6598 shared address space is non-public even though Go's
	// IsGlobalUnicast reports true. Internal deployments may opt into it
	// through AllowPrivate alongside RFC 1918.
	sharedV4 = netip.MustParsePrefix("100.64.0.0/10")
)

// deniedReason classifies ip against the delivery deny-list, returning ""
// when the address is an acceptable target. IPv4-mapped IPv6 is unmapped
// first (::ffff:10.0.0.1 IS 10.0.0.1 — the same normalization discipline
// as the trusted-proxy allowlist, issue #30), so mapped spellings cannot
// smuggle a private IPv4 target past the IPv6 checks.
func (g TargetGuard) deniedReason(ip netip.Addr) string {
	if ip.Zone() != "" {
		return "zone-scoped address"
	}
	ip = ip.Unmap()
	// Never legitimate receivers, AllowPrivate or not.
	switch {
	case !ip.IsValid():
		return "invalid address"
	case ip == metadataV4 || ip == metadataV6:
		return "cloud metadata address"
	case ip.IsUnspecified():
		return "unspecified address"
	case ip.IsMulticast():
		return "multicast address"
	case ip == broadcastV4:
		return "broadcast address"
	case ip.IsLoopback():
		if !g.AllowPrivate {
			return "loopback address"
		}
	case ip.IsPrivate():
		// RFC 1918 (10/8, 172.16/12, 192.168/16) and RFC 4193 ULA
		// (fc00::/7).
		if !g.AllowPrivate {
			return "private address"
		}
	case sharedV4.Contains(ip):
		if !g.AllowPrivate {
			return "shared address"
		}
	case ip.IsLinkLocalUnicast():
		// 169.254/16 and fe80::/10 (except metadata above).
		if !g.AllowPrivate {
			return "link-local address"
		}
	case !ip.IsGlobalUnicast():
		// Defensive catch-all: anything outside the explicitly allowed
		// private classes stays denied even under the escape hatch.
		return "non-global address"
	}
	return ""
}

// lookup resolves host through the injected resolver or net.DefaultResolver.
func (g TargetGuard) lookup(ctx context.Context, host string) ([]netip.Addr, error) {
	if g.LookupNetIP != nil {
		return g.LookupNetIP(ctx, host)
	}
	return net.DefaultResolver.LookupNetIP(ctx, "ip", host)
}

// vetHost resolves host (hostname or IP literal) and validates every
// candidate address, fail closed: a resolution failure, an empty answer,
// or ANY denied address (mixed allowed/denied sets included) rejects the
// whole set — a mixed answer is exactly what a rebinding resolver would
// serve to smuggle one private address past a first-match check.
func (g TargetGuard) vetHost(ctx context.Context, host string) ([]netip.Addr, error) {
	if ip, err := netip.ParseAddr(host); err == nil {
		if reason := g.deniedReason(ip); reason != "" {
			return nil, fmt.Errorf("webhook target %s denied: %s", host, reason)
		}
		return []netip.Addr{ip.Unmap()}, nil
	}
	addrs, err := g.lookup(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("resolve webhook target %s: %w", host, err)
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("resolve webhook target %s: resolver returned no addresses", host)
	}
	vetted := make([]netip.Addr, len(addrs))
	for i, ip := range addrs {
		if reason := g.deniedReason(ip); reason != "" {
			return nil, fmt.Errorf("webhook target %s resolves to denied address %s: %s", host, ip, reason)
		}
		vetted[i] = ip.Unmap()
	}
	return vetted, nil
}

// CheckRegistration is the registration-time courtesy check: it rejects
// URLs whose host is a denied IP literal or currently resolves to a denied
// address, giving operators fast feedback at POST /webhooks instead of a
// dead registration that silently never delivers.
//
// It deliberately fails OPEN on resolution errors: registration acceptance
// is NOT a delivery-reachability promise (the resolver may be partitioned,
// or the record may change later either way). Dial-time enforcement in
// NewClient is authoritative.
func (g TargetGuard) CheckRegistration(ctx context.Context, rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("parse webhook url: %w", err)
	}
	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("webhook url %s has no host", rawURL)
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		if reason := g.deniedReason(ip); reason != "" {
			return fmt.Errorf("webhook target %s denied: %s", host, reason)
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, registrationLookupTimeout)
	defer cancel()
	addrs, err := g.lookup(ctx, host)
	if err != nil {
		return nil // courtesy check only; dial time enforces
	}
	for _, ip := range addrs {
		if reason := g.deniedReason(ip); reason != "" {
			return fmt.Errorf("webhook target %s resolves to denied address %s: %s", host, ip, reason)
		}
	}
	return nil
}

// NewClient builds the delivery client every webhook POST must go through:
// a 30s overall timeout (a receiver that never responds must not wedge a
// delivery worker), NO redirect following, and the guarded dialer.
//
// Redirects are refused unconditionally — legitimate webhook receivers
// answer at their registered URL, and following a redirect would hand the
// receiver a fresh, unvetted target. A redirect response is therefore a
// delivery failure with the standard retry semantics.
//
// The transport intentionally ignores HTTP(S)_PROXY: validating the proxy
// socket would not validate the URL target the proxy later connects to.
// Operators that require proxy egress must enforce the same destination
// policy at that proxy rather than bypass this direct-dial guard.
func (g TargetGuard) NewClient() *http.Client {
	transport := &http.Transport{
		DialContext:           g.dialContext,
		DisableKeepAlives:     true, // re-resolve and re-vet every attempt
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	return &http.Client{
		Timeout:   30 * time.Second,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			assertf(len(via) > 0, "CheckRedirect called with empty via chain")
			return fmt.Errorf("refusing to follow redirect to %s: webhook deliveries never follow redirects (issue #53)", req.URL.Redacted())
		},
	}
}

// dialContext is the authoritative enforcement point: resolve, vet every
// candidate, then dial only the vetted literals — handing the hostname to
// a re-resolving dialer here would reopen the rebinding window vetHost
// just closed.
func (g TargetGuard) dialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	switch network {
	case "tcp", "tcp4", "tcp6":
	default:
		return nil, fmt.Errorf("webhook dial: refusing non-TCP network %q", network)
	}
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("webhook dial: malformed address %q: %w", addr, err)
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return nil, fmt.Errorf("webhook dial: malformed port %q: %w", portStr, err)
	}
	addrs, err := g.vetHost(ctx, host)
	if err != nil {
		return nil, err
	}
	assertf(len(addrs) > 0, "vetHost returned no addresses without error")

	var firstErr error
	for _, ip := range addrs {
		conn, err := g.dialVetted(ctx, network, netip.AddrPortFrom(ip, uint16(port)))
		if err == nil {
			return conn, nil
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	assertf(firstErr != nil, "all vetted dial attempts failed without an error")
	return nil, firstErr
}

// dialVetted connects to one already-vetted address. The Control hook
// re-checks the exact address the socket is about to connect to as an
// independent second line of defense: it can only fire on a guard bug
// (crash-and-burn: surface it as a dial error, never silently connect).
func (g TargetGuard) dialVetted(ctx context.Context, network string, addrPort netip.AddrPort) (net.Conn, error) {
	if g.dialAddrPort != nil {
		return g.dialAddrPort(ctx, network, addrPort)
	}
	dialer := &net.Dialer{
		Timeout: dialTimeout,
		Control: func(_, address string, _ syscall.RawConn) error {
			ap, err := netip.ParseAddrPort(address)
			if err != nil {
				return fmt.Errorf("webhook dial control: unparseable address %q: %w", address, err)
			}
			if reason := g.deniedReason(ap.Addr()); reason != "" {
				return fmt.Errorf("webhook dial control: %s denied: %s (guard bug: vetted set disagrees with connect target)", ap.Addr(), reason)
			}
			return nil
		},
	}
	return dialer.DialContext(ctx, network, addrPort.String())
}
