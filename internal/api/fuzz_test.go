// Fuzz targets for package api's untrusted-input surfaces (issue #91):
// clientKey derives the rate-limit bucket key pre-auth on every request
// (RemoteAddr + client-controlled X-Forwarded-For), and coreCursorDecode
// parses opaque pagination cursors. Both must stay total and fail closed on
// arbitrary input.
package api

import (
	"encoding/base64"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"
)

// fuzzB64 encodes a raw cursor body the way coreCursorEncode does.
func fuzzB64(raw string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func FuzzClientKey(f *testing.F) {
	trusted, err := newRateLimiter(RateLimitConfig{RPS: 1, Burst: 1, TrustedProxies: []netip.Prefix{
		netip.MustParsePrefix("10.0.0.0/8"),
		netip.MustParsePrefix("192.0.2.0/24"),
		netip.MustParsePrefix("::1/128"),
		netip.MustParsePrefix("fd00::/8"),
	}})
	if err != nil {
		f.Fatal(err)
	}
	bare, err := newRateLimiter(RateLimitConfig{RPS: 1, Burst: 1})
	if err != nil {
		f.Fatal(err)
	}
	for _, seed := range [][3]string{
		{"10.0.0.1:9999", "203.0.113.7", ""},
		{"10.0.0.1:9999", "junk, 203.0.113.7", "10.0.0.2"},
		{"[::1]:443", "2001:db8::1", "::ffff:192.0.2.9"},
		{"badaddr", "1.2.3.4", ""},
		{"", "", ""},
		{"10.0.0.1:1", strings.Repeat("203.0.113.7, ", 400), ""}, // oversized chain
		{"192.0.2.5:2", "[2001:db8::2]:8443", "unknown"},
		{"10.0.0.1:1", "10.0.0.9,fd00::5", "10.0.0.3"}, // all-trusted chain
		{"10.0.0.1:1", "203.0.113.7%eth0", "  "},
	} {
		f.Add(seed[0], seed[1], seed[2])
	}
	f.Fuzz(func(t *testing.T, remoteAddr, xff1, xff2 string) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = remoteAddr
		req.Header.Add("X-Forwarded-For", xff1)
		req.Header.Add("X-Forwarded-For", xff2)

		// The documented peer fallback key, mirrored independently.
		host, _, err := net.SplitHostPort(remoteAddr)
		if err != nil || host == "" {
			host = remoteAddr
		}
		base := host
		if base == "" {
			base = "unknown"
		}

		if got := bare.clientKey(req); got != base {
			t.Fatalf("clientKey without an allowlist = %q, want peer key %q", got, base)
		}
		key := trusted.clientKey(req)
		if key == "" {
			t.Fatal("clientKey returned an empty bucket key")
		}
		if again := trusted.clientKey(req); again != key {
			t.Fatalf("clientKey not deterministic: %q then %q", key, again)
		}
		if len(xff1)+len(xff2) > maxForwardedForBytes && key != base {
			t.Fatalf("oversized XFF chain escaped the fail-closed peer key: %q", key)
		}
		if key == base {
			return // fail-closed outcome; nothing further to check
		}
		// The walk re-keyed the request: only a trusted peer may trigger
		// that, and only to a valid, untrusted, unmapped address that a
		// trusted proxy actually appended to the chain.
		peer, err := netip.ParseAddr(host)
		if err != nil || !trustedContains(trusted.cfg.TrustedProxies, peer) {
			t.Fatalf("untrusted peer %q re-keyed to XFF-derived %q", remoteAddr, key)
		}
		addr, err := netip.ParseAddr(key)
		if err != nil {
			t.Fatalf("derived key %q is not a valid address: %v", key, err)
		}
		if trustedContains(trusted.cfg.TrustedProxies, addr) {
			t.Fatalf("derived key %q is inside the trusted allowlist", key)
		}
		if addr != addr.Unmap() {
			t.Fatalf("derived key %q is not unmapped", key)
		}
		found := false
		for _, v := range []string{xff1, xff2} {
			for hop := range strings.SplitSeq(v, ",") {
				if a, ok := parseForwardedAddr(hop); ok && a.String() == key {
					found = true
				}
			}
		}
		if !found {
			t.Fatalf("derived key %q does not correspond to any XFF hop", key)
		}
	})
}

// FuzzRawPathBase64: a path sibling exists exactly for bytes that are not
// valid UTF-8, and decodes (strictly) back to those bytes.
func FuzzRawPathBase64(f *testing.F) {
	for _, seed := range [][2]string{
		{"", ""}, {"plain.txt", "a\ufffdb"}, {"a\xffb", "a\xfeb"}, {"caf\xe9.txt", "caf\u00e9.txt"},
		{"\xc0\x80", "\xed\xa0\x80"}, {"d\xfe/x.txt", "\xf4\x90\x80\x80"}, {strings.Repeat("\xff", 200), "\x00"},
	} {
		f.Add(seed[0], seed[1])
	}
	f.Fuzz(func(t *testing.T, p, q string) {
		one := rawPathBase64(p)
		if utf8.ValidString(p) != (one == "") {
			t.Fatalf("rawPathBase64(%q) = %q: present exactly when the path is not valid UTF-8", p, one)
		}
		if one != "" {
			if raw, err := base64.StdEncoding.Strict().DecodeString(one); err != nil || string(raw) != p {
				t.Fatalf("rawPathBase64(%q) = %q decodes to %q, %v", p, one, raw, err)
			}
		}
		list := rawPathsBase64([]string{p, q})
		if utf8.ValidString(p) && utf8.ValidString(q) {
			if list != nil {
				t.Fatalf("rawPathsBase64(%q, %q) = %q, want nil", p, q, list)
			}
			return
		}
		if len(list) != 2 {
			t.Fatalf("rawPathsBase64(%q, %q) = %q, want 2 elements", p, q, list)
		}
		for i, want := range []string{p, q} {
			if raw, err := base64.StdEncoding.Strict().DecodeString(list[i]); err != nil || string(raw) != want {
				t.Fatalf("rawPathsBase64[%d] = %q decodes to %q, %v, want %q", i, list[i], raw, err, want)
			}
		}
	})
}

func FuzzCoreCursor(f *testing.F) {
	f.Add(coreCursorEncode(cursorKindCommits, "abc123", "2024-01-01T00:00:00Z"), "main", "src/a|b.txt")
	f.Add("", "", "")
	f.Add("!!!!", "x", "y")
	f.Add(coreCursorEncode(cursorKindBranches, "feature/x"), "n", "")
	f.Add(fuzzB64("commits|a"), "a", "b")     // missing field
	f.Add(fuzzB64("commits|a|b|c"), "a", "b") // separator folds into last field
	f.Add(fuzzB64("tags|a|b"), "a", "b")      // wrong kind
	f.Add(fuzzB64("commits||b"), "a", "b")    // empty field
	f.Fuzz(func(t *testing.T, cursor, f1, f2 string) {
		fields, err := coreCursorDecode(cursorKindCommits, cursor, 2)
		if err == nil {
			if len(fields) != 2 {
				t.Fatalf("decode returned %d fields, want 2", len(fields))
			}
			for _, fd := range fields {
				if fd == "" {
					t.Fatal("decode returned an empty cursor field")
				}
			}
			// Whatever decoded must survive a re-encode round-trip …
			back, err := coreCursorDecode(cursorKindCommits, coreCursorEncode(cursorKindCommits, fields...), 2)
			if err != nil || !slices.Equal(back, fields) {
				t.Fatalf("re-encode round-trip = (%v, %v), want %v", back, err, fields)
			}
			// … and a cursor valid for one kind must fail closed elsewhere.
			if _, err := coreCursorDecode(cursorKindTags, cursor, 2); err == nil {
				t.Fatal("cursor decoded under a foreign kind")
			}
		}
		// Encode→decode: non-empty fields round-trip exactly; only the
		// last field may carry the separator.
		if f1 != "" && !strings.Contains(f1, "|") && f2 != "" {
			got, err := coreCursorDecode(cursorKindFiles, coreCursorEncode(cursorKindFiles, f1, f2), 2)
			if err != nil || len(got) != 2 || got[0] != f1 || got[1] != f2 {
				t.Fatalf("encode/decode lost fields: (%v, %v), want [%q %q]", got, err, f1, f2)
			}
		}
		if f1 != "" {
			got, err := coreCursorDecode(cursorKindBranches, coreCursorEncode(cursorKindBranches, f1), 1)
			if err != nil || len(got) != 1 || got[0] != f1 {
				t.Fatalf("single-field encode/decode lost %q: (%v, %v)", f1, got, err)
			}
		}
	})
}
