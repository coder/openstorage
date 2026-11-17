package sync

import (
	"strings"
	"testing"
)

// TestValidUpstreamHost: an upstream host is a bare host[:port] or an
// http(s) base URL whose hostname a sync run can dial: an IP address
// without a zone, or a name that Go's resolver looks up (net.isDomainName),
// with a port from 1 to 65535 and no query or fragment.
func TestValidUpstreamHost(t *testing.T) {
	label63 := strings.Repeat("a", 63)
	// 63+1+63+1+63+1+61 = 253 bytes, the longest name without a final dot.
	name253 := label63 + "." + label63 + "." + label63 + "." + strings.Repeat("b", 61)
	for _, host := range []string{
		"forge.example",
		"forge.example.",
		"FORGE.Example:8443",
		"a_b.example",
		"xn--bcher-kva.example",
		"localhost",
		"1forge.example",
		label63 + ".example",
		"forge." + label63,
		"1-1",
		"1_1",
		"cafe",
		"0xg.example",
		// Names that only look numeric stay names for curl: five labels,
		// a part out of IPv4's range, an empty or a bad number.
		"0x1.0x2.0x3.0x4.0x5",
		"0x0.0x0.0x0.0x0.0x0",
		"0x100.1",
		"0x100000000",
		"0xff.0x1000000",
		"0xff.0xff.0x10000",
		"0x",
		"0x.1",
		"1.0x",
		"09.0x1",
		"0xg",
		"axe",
		"6x6",
		// A 63-byte label of letters, digits and dashes.
		"a" + strings.Repeat("1", 30) + strings.Repeat("-", 30) + "bc",
		name253,
		name253 + ".",
		"10.0.0.1",
		"10.0.0.1:1",
		"[::1]:65535",
		"[2001:db8::1]",
		"https://forge.example",
		"https://forge.example/sub/path/",
		"http://127.0.0.1:3000",
		"https://[2001:db8::1]:443/git",
		"https://forge.example/a%3Fb",
		"forge.example:",
		"https://forge.example:/git",
	} {
		if !ValidUpstreamHost(host) {
			t.Errorf("ValidUpstreamHost(%q) = false, want true", host)
		}
	}
	for _, host := range []string{
		// Names that the resolver refuses.
		"<<<.com",
		"https://<<<.com",
		"bücher.example",
		"https://bücher.example",
		"a..b.example",
		".forge.example",
		"-forge.example",
		"forge-.example",
		"forge.-example",
		"forge.example-",
		label63 + "a.example",
		"a" + strings.Repeat("1", 63) + ".example",
		"a" + strings.Repeat("-", 62) + "a.example",
		"forge." + label63 + "a",
		name253 + "a",
		name253 + "a.",
		".",
		// Only digits and dots, but no IP address: curl would read
		// 2130706433 as 127.0.0.1, Go's resolver refuses it.
		"2130706433",
		"1.2.3",
		"https://1.2.3",
		"01.2.3.4",
		"1.2.3.4.5",
		// Hexadecimal forms that curl reads as IPv4 too.
		"0x7f000001",
		"0x7f.1",
		"0X7F.0.0.1",
		"1.2.3.0x4",
		"0x7f.1.",
		"0xa9fea9fe",
		"0xA9FEA9FE",
		"0xA.0xA.0xA.0xA",
		"0xffffffff",
		"0xff.0xffffff",
		"0xff.0xff.0xffff",
		"0377.0377.0377.0377",
		"http://0x7f000001:3000/git",
		// A zone.
		"https://1.2.3.4%25eth0",
		"https://[fe80::1%25eth0]",
		"[fe80::1%25eth0]",
		// Ports.
		"forge.example:0",
		"forge.example:65536",
		"https://forge.example:99999/git",
		// A query or fragment, even an empty one.
		"https://forge.example?",
		"https://forge.example#",
		"https://forge.example/git?",
		"https://forge.example/git?a=b",
		"https://forge.example/git#x",
		// A space, which curl refuses, and a scheme that is not lower
		// case, which GIT_ALLOW_PROTOCOL refuses.
		"https://forge.example/a b",
		"HTTPS://forge.example",
		"Http://forge.example",
		// Unchanged refusals.
		":80",
		"https://:80",
		"forge.example/git",
		"ssh://forge.example",
		"https://u:p@forge.example",
	} {
		if ValidUpstreamHost(host) {
			t.Errorf("ValidUpstreamHost(%q) = true, want false", host)
		}
	}
}
