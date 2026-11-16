package sync

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
)

// TestEgressProxyDeniesCloudMetadataUnderEveryPolicy: the sync egress
// proxy refuses every cloud metadata endpoint with 403 and records a
// policy denial, for an https upstream (CONNECT) and an http one (absolute
// form), whether OPENSTORAGE_SYNC_ALLOW_PRIVATE admits private upstreams or
// not. The guard's own test (events) covers each endpoint; this one shows
// the sync lane enforces them.
func TestEgressProxyDeniesCloudMetadataUnderEveryPolicy(t *testing.T) {
	for _, allow := range []bool{false, true} {
		for _, ip := range []string{"169.254.169.254", "100.100.100.200", "169.254.170.2", "fd20:ce::254", "fe80::a9fe:a9fe"} {
			t.Run(fmt.Sprintf("allow_private=%v/%s", allow, strings.ReplaceAll(ip, ":", "_")), func(t *testing.T) {
				p, err := startEgressProxy(allow)
				if err != nil {
					t.Fatal(err)
				}
				defer p.Close()
				target := net.JoinHostPort(ip, "443")
				for _, req := range []string{
					fmt.Sprintf("CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target),
					fmt.Sprintf("GET http://%s/o/n.git/info/refs HTTP/1.1\r\nHost: %s\r\n\r\n", target, target),
				} {
					conn, err := net.Dial("tcp", p.ln.Addr().String())
					if err != nil {
						t.Fatal(err)
					}
					fmt.Fprint(conn, req)
					method, _, _ := strings.Cut(req, " ")
					resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: method})
					if err != nil {
						conn.Close()
						t.Fatalf("%s: read response: %v", method, err)
					}
					resp.Body.Close()
					conn.Close()
					if resp.StatusCode != http.StatusForbidden {
						t.Fatalf("%s %s: status %d, want 403", method, target, resp.StatusCode)
					}
				}
				if got := p.deniedReason(); !strings.Contains(got, "cloud metadata address") {
					t.Fatalf("recorded denial %q, want a cloud metadata denial", got)
				}
			})
		}
	}
}
