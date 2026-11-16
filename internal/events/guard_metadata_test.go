package events

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
)

// cloudMetadataAddrs spells out metadataAddrs (guard.go cites a source of
// each), so a test fails when one leaves the guard.
var cloudMetadataAddrs = []string{
	"169.254.169.254",    // AWS, Google Cloud, Azure, Oracle Cloud and others
	"fd00:ec2::254",      // AWS IPv6
	"169.254.170.2",      // AWS ECS task metadata and credentials
	"169.254.170.23",     // AWS EKS Pod Identity Agent
	"fd00:ec2::23",       // AWS EKS Pod Identity Agent IPv6
	"fd20:ce::254",       // Google Cloud IPv6-only instances
	"169.254.169.252",    // GKE metadata server
	"100.100.100.200",    // Alibaba Cloud
	"169.254.10.10",      // Tencent Cloud metadata.tencentyun.com
	"169.254.0.23",       // Tencent Cloud Cloudbase-Init
	"169.254.42.42",      // Scaleway
	"fd00:42::42",        // Scaleway IPv6
	"fd00:a9fe:a9fe::1",  // Akamai (Linode) IPv6
	"fe80::a9fe:a9fe",    // OpenStack, Hetzner and Akamai IPv6
	"fd00:c1::a9fe:a9fe", // Oracle Cloud IPv6
	"168.63.129.16",      // Azure WireServer
	"192.0.0.192",        // Oracle Cloud Infrastructure Classic
}

// TestTargetGuardDeniesCloudMetadataUnderEveryPolicy: every cloud metadata
// endpoint is denied with AllowPrivate off and on, which covers both the
// webhook guard (OPENSTORAGE_WEBHOOK_ALLOW_PRIVATE) and the sync guard
// (OPENSTORAGE_SYNC_ALLOW_PRIVATE): as a literal and as a DNS answer, at
// registration (CheckRegistration, CheckHost) and at dial time, IPv4 also
// in its IPv4-mapped IPv6 form.
func TestTargetGuardDeniesCloudMetadataUnderEveryPolicy(t *testing.T) {
	t.Parallel()

	var raws []string
	for _, raw := range cloudMetadataAddrs {
		raws = append(raws, raw)
		if ip := netip.MustParseAddr(raw); ip.Is4() {
			raws = append(raws, "::ffff:"+raw)
		}
	}
	for _, raw := range raws {
		for _, allow := range []bool{false, true} {
			name := strings.ReplaceAll(raw, ":", "_")
			if allow {
				name += "/allow_private"
			} else {
				name += "/strict"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				ip := netip.MustParseAddr(raw)
				var dialed atomic.Int32
				guard := TargetGuard{
					AllowPrivate: allow,
					LookupNetIP: func(context.Context, string) ([]netip.Addr, error) {
						return []netip.Addr{ip}, nil
					},
					dialAddrPort: func(context.Context, string, netip.AddrPort) (net.Conn, error) {
						dialed.Add(1)
						return nil, errors.New("unexpected dial")
					},
				}
				if got := guard.deniedReason(ip); got != "cloud metadata address" {
					t.Fatalf("deniedReason = %q, want cloud metadata address", got)
				}
				ctx := context.Background()
				for _, addr := range []string{net.JoinHostPort(raw, "443"), "metadata.example:443"} {
					if _, err := guard.DialContext(ctx, "tcp", addr); !errors.Is(err, ErrTargetDenied) {
						t.Fatalf("dial %s: err = %v, want ErrTargetDenied", addr, err)
					}
				}
				if got := dialed.Load(); got != 0 {
					t.Fatalf("dialer called %d times, want 0", got)
				}
				for _, host := range []string{raw, "metadata.example"} {
					if err := guard.CheckHost(ctx, host); err == nil || !strings.Contains(err.Error(), "cloud metadata address") {
						t.Fatalf("CheckHost(%s) = %v, want a cloud metadata denial", host, err)
					}
				}
				for _, rawURL := range []string{"http://" + net.JoinHostPort(raw, "80") + "/hook", "https://metadata.example/hook"} {
					if err := guard.CheckRegistration(ctx, rawURL); err == nil || !strings.Contains(err.Error(), "cloud metadata address") {
						t.Fatalf("CheckRegistration(%s) = %v, want a cloud metadata denial", rawURL, err)
					}
				}
			})
		}
	}
}

// TestTargetGuardMetadataNeighborsKeepTheirClass: the metadata endpoints
// are single addresses, not ranges. Their neighbours stay what their class
// makes them: admitted with AllowPrivate, denied without it.
func TestTargetGuardMetadataNeighborsKeepTheirClass(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{
		"169.254.169.253",
		"169.254.169.251",
		"169.254.170.3",
		"100.100.100.201",
		"169.254.10.11",
		"fd20:ce::255",
		"fd00:ec2::22",
		"fe80::a9fe:a9ff",
		"fd00:c1::a9fe:a9ff",
	} {
		t.Run(strings.ReplaceAll(raw, ":", "_"), func(t *testing.T) {
			t.Parallel()
			ip := netip.MustParseAddr(raw)
			if got := (TargetGuard{AllowPrivate: true}).deniedReason(ip); got != "" {
				t.Fatalf("AllowPrivate: deniedReason = %q, want admitted", got)
			}
			if got := (TargetGuard{}).deniedReason(ip); got == "" || got == "cloud metadata address" {
				t.Fatalf("strict: deniedReason = %q, want the reason of its private class", got)
			}
		})
	}
	// Azure's WireServer is a public address: its neighbours are admitted
	// under every policy.
	for _, raw := range []string{"168.63.129.15", "168.63.129.17"} {
		for _, allow := range []bool{false, true} {
			if got := (TargetGuard{AllowPrivate: allow}).deniedReason(netip.MustParseAddr(raw)); got != "" {
				t.Errorf("%s (AllowPrivate %v): deniedReason = %q, want admitted", raw, allow, got)
			}
		}
	}
}
