package main

import (
	"net"
	"testing"
)

// TestSelfLoopbackURL pins the push-target derivation (issue #45): the sync
// worker must dial THIS process, so unspecified binds map to the matching
// loopback address, concrete binds stay literal, and scoped IPv6 zones
// survive with RFC 6874 %25 encoding.
func TestSelfLoopbackURL(t *testing.T) {
	cases := []struct {
		name string
		addr *net.TCPAddr
		want string
	}{
		{"unspecified ipv4", &net.TCPAddr{IP: net.IPv4zero, Port: 8080}, "http://127.0.0.1:8080"},
		{"unspecified ipv6", &net.TCPAddr{IP: net.IPv6unspecified, Port: 8080}, "http://[::1]:8080"},
		{"nil ip (dual-stack :port)", &net.TCPAddr{IP: nil, Port: 9}, "http://127.0.0.1:9"},
		{"concrete ipv4", &net.TCPAddr{IP: net.ParseIP("10.1.2.3"), Port: 8080}, "http://10.1.2.3:8080"},
		{"concrete ipv6", &net.TCPAddr{IP: net.ParseIP("2001:db8::5"), Port: 8080}, "http://[2001:db8::5]:8080"},
		{"loopback ipv4", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 7103}, "http://127.0.0.1:7103"},
		{"scoped ipv6 keeps its zone", &net.TCPAddr{IP: net.ParseIP("fe80::1"), Zone: "eth0", Port: 8080}, "http://[fe80::1%25eth0]:8080"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := selfLoopbackURL(tc.addr); got != tc.want {
				t.Fatalf("selfLoopbackURL(%v) = %q, want %q", tc.addr, got, tc.want)
			}
		})
	}
}
