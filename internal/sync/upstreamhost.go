package sync

import (
	"net/netip"
	neturl "net/url"
	"strconv"
	"strings"
)

// InvalidUpstreamHostReason is the reason that POST /repos gives for a host
// that ValidUpstreamHost refuses (the invalid_params reason of
// `upstream.host` or `base_repo.upstream_host`). A run that finds such a
// host stored fails with the same reason (upstreamURLFailure).
const InvalidUpstreamHostReason = "is not a valid host"

// ValidUpstreamHost accepts the two documented upstream_host shapes — a
// bare hostname[:port] or an http(s):// base URL — and nothing else. Both
// need a hostname, so a host such as ":80" is refused.
// CloneURL uses the value verbatim as the base of a git fetch URL, so
// file://, ssh://, transport helpers (ext::), embedded credentials, and
// query/fragment noise must be rejected where the attacker-controlled
// value enters, POST /repos (the worker additionally pins
// GIT_ALLOW_PROTOCOL).
//
// It also refuses hosts that a sync run cannot use as given, so the create
// answers 400 instead of storing a link that the registration check could
// not vet: a hostname that is neither an IP address nor a name that the
// run's resolver asks DNS for (dialableHostname), a port outside 1-65535,
// and a base URL with a query or a fragment, even an empty one
// (`https://h?`), which would end the path of every fetch URL, with a
// space, which curl refuses, or with a scheme that is not lower case
// (`HTTPS://`), which GIT_ALLOW_PROTOCOL refuses. A run refuses such a
// host too when an older release stored it (upstreamURLFailure).
func ValidUpstreamHost(host string) bool {
	assertf(host != "", "ValidUpstreamHost: empty host")
	if !strings.Contains(host, "://") {
		// Bare host[:port]: must round-trip as exactly the authority
		// component (no path, userinfo, or control characters). A zone
		// is written %25, which the parse decodes, so it never
		// round-trips.
		u, err := neturl.Parse("https://" + host)
		return err == nil && u.Host == host && u.User == nil &&
			u.Path == "" && u.RawQuery == "" && u.Fragment == "" && dialableAuthority(u)
	}
	if !strings.HasPrefix(host, "http://") && !strings.HasPrefix(host, "https://") {
		return false
	}
	u, err := neturl.Parse(host)
	if err != nil {
		return false
	}
	// A path is allowed (sub-path-hosted forges); credentials belong in
	// the credential store, never in the URL. url.Parse drops an empty
	// query or fragment and keeps a space in a path, so the bytes are
	// checked.
	return u.User == nil && !strings.ContainsAny(host, "?# ") && dialableAuthority(u)
}

// dialableAuthority reports whether the host and port of u name something
// that a sync run can dial: a dialableHostname and, when u has one, a port
// from 1 to 65535. An empty port (`forge.example:`) is the default one.
func dialableAuthority(u *neturl.URL) bool {
	if port := u.Port(); port != "" {
		if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
			return false
		}
	}
	return dialableHostname(u.Hostname())
}

// dialableHostname reports whether name is an IP address without a zone,
// or a name that the resolver of a sync run asks DNS for and that curl
// does not read as an IPv4 address (curlIPv4Name). A zone-scoped address
// (fe80::1%eth0) is refused: the egress guard refuses it under every
// policy, and the egress answer would repeat its zone, which only the
// upstream URL limit bounds.
func dialableHostname(name string) bool {
	if ip, err := netip.ParseAddr(name); err == nil {
		return ip.Zone() == ""
	}
	return resolverName(name) && !curlIPv4Name(name)
}

// resolverName reports whether the Go resolver of a sync run asks DNS for
// name. A run dials through the egress proxy (startEgressProxy),
// whose resolver refuses any other name (net.isDomainName, after RFC 1035
// and RFC 3696): at most 253 bytes, or 254 with a final dot; labels of 1 to
// 63 letters, digits, '_' and '-' that do not start or end with '-'; and
// not only digits and dots, which curl reads as an IPv4 address
// (2130706433 is 127.0.0.1) that the registration check never saw. The
// resolver reads /etc/hosts before it applies the rule, but a name that
// only /etc/hosts holds is no upstream host.
//
// A name with other letters goes in its ASCII form
// (xn--bcher-kva.example): the registration check cannot look bücher.example
// up, and whether git and curl convert it depends on the locale that the
// server's git runs in.
func resolverName(name string) bool {
	if name == "" || len(name) > 254 || len(name) == 254 && name[253] != '.' {
		return false
	}
	prev := byte('.')
	letters := false // a letter, '_' or '-': all digits would be an address
	label := 0
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', c == '_':
			letters = true
			label++
		case '0' <= c && c <= '9':
			label++
		case c == '-':
			if prev == '.' {
				return false
			}
			letters = true
			label++
		case c == '.':
			if prev == '.' || prev == '-' || label > 63 {
				return false
			}
			label = 0
		default:
			return false
		}
		prev = c
	}
	return prev != '-' && label <= 63 && letters
}

// curlIPv4Name reports whether curl reads name as an IPv4 address, as its
// URL parser does (ipv4_normalize): one to four labels, a final dot aside,
// each a decimal, octal (leading 0) or hexadecimal (0x) number of at most
// 32 bits, all but the last at most 255, and the last within the bits the
// others leave, such as 2130706433, 0x7f000001, 0x7f.1 or 1.2.3.0x4. curl
// dials such a name as the address (127.0.0.1, 1.2.3.4), which the
// registration check, which reads it as a name, never saw. A name that only
// looks numeric, such as 0x100.1 or 0x100000000, stays a name for curl.
func curlIPv4Name(name string) bool {
	labels := strings.Split(strings.TrimSuffix(name, "."), ".")
	if len(labels) > 4 {
		return false
	}
	last := len(labels) - 1
	for i, label := range labels {
		v, ok := curlIPv4Part(label)
		if !ok || i < last && v > 0xff || i == last && v > 0xffffffff>>(8*last) {
			return false
		}
	}
	return true
}

// curlIPv4Part parses one label of a numerical IPv4 address as curl does:
// 0x or 0X and hexadecimal digits, 0 and octal digits, or decimal digits,
// at most 32 bits.
func curlIPv4Part(label string) (uint64, bool) {
	base, digits := 10, label
	switch {
	case len(label) >= 2 && label[0] == '0' && (label[1] == 'x' || label[1] == 'X'):
		base, digits = 16, label[2:]
	case len(label) >= 1 && label[0] == '0':
		base = 8
	}
	if digits == "" {
		return 0, false
	}
	v, err := strconv.ParseUint(digits, base, 32)
	return v, err == nil
}
