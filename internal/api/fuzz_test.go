// Fuzz targets for package api's untrusted-input surfaces (issue #91):
// clientKey derives the rate-limit bucket key pre-auth on every request
// (RemoteAddr + client-controlled X-Forwarded-For), and coreCursorDecode
// parses opaque pagination cursors. Both must stay total and fail closed on
// arbitrary input. FuzzApplyRejection covers git apply's stderr and
// FuzzPatchModeError its summary, which both hold client-chosen path names
// (#613).
package api

import (
	"encoding/base64"
	"encoding/json"
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
		both := []string{p, q}
		if anyInvalidUTF8(both) == (utf8.ValidString(p) && utf8.ValidString(q)) {
			t.Fatalf("anyInvalidUTF8(%q, %q) = %v", p, q, anyInvalidUTF8(both))
		}
		// The streamed list holds the base64 of every element, in order.
		rec := httptest.NewRecorder()
		writeJSONLists(rec, http.StatusOK, struct {
			L []string `json:"l"`
		}{L: []string{}}, rawPathsBase64List("l", both))
		var got struct {
			L []string `json:"l"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil || len(got.L) != 2 {
			t.Fatalf("streamed list of %q: %q, %v", both, rec.Body.Bytes(), err)
		}
		for i, want := range both {
			if raw, err := base64.StdEncoding.Strict().DecodeString(got.L[i]); err != nil || string(raw) != want {
				t.Fatalf("streamed list[%d] = %q decodes to %q, %v, want %q", i, got.L[i], raw, err, want)
			}
		}
	})
}

// FuzzDecodeRawPath: a request's path_base64 text decodes only when it is
// the canonical padded standard base64 of the bytes it yields, so no two
// texts name one path.
func FuzzDecodeRawPath(f *testing.F) {
	for _, seed := range []string{"", "YQ==", "YQ=", "YQ", "YR==", "Yf9i", "!!", "Yf9\ni", "Yf9i\r\n", "-_8=", "+/8=", "AA==", "////", "Y W I=", "YQ==YQ=="} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		p, ok := decodeRawPath(s)
		if ok != (encodeRawPath(p) == s) || (!ok && p != "") {
			t.Fatalf("decodeRawPath(%q) = %q, %v", s, p, ok)
		}
		// Every byte string has one text, and it decodes back.
		if q, ok := decodeRawPath(encodeRawPath(s)); !ok || q != s {
			t.Fatalf("decodeRawPath(encodeRawPath(%q)) = %q, %v", s, q, ok)
		}
	})
}

// FuzzDecodeRawPatterns: a decoded list holds one value per element, each
// not blank and with no NUL, and a failure names the first bad element.
func FuzzDecodeRawPatterns(f *testing.F) {
	for _, seed := range [][2]string{
		{"YQ==", "Yf9i"}, {"", "YQ=="}, {"YQ==", ""}, {"AA==", "YQ=="}, {"IA==", "YQ=="},
		{"CQ==", "Yf9i"}, {"!!", "!!"}, {"YR==", "YQ=="}, {"YQ==", "YQ=="},
	} {
		f.Add(seed[0], seed[1])
	}
	f.Fuzz(func(t *testing.T, a, b string) {
		list := []string{a, b}
		decoded, bad := decodeRawPatterns(list)
		valid := func(enc string) bool {
			p, ok := decodeRawPath(enc)
			return ok && strings.TrimSpace(p) != "" && !strings.ContainsRune(p, 0)
		}
		if bad < 0 {
			if len(decoded) != 2 || encodeRawPath(decoded[0]) != a || encodeRawPath(decoded[1]) != b || !valid(a) || !valid(b) {
				t.Fatalf("decodeRawPatterns(%q) = %q, %d", list, decoded, bad)
			}
			return
		}
		if decoded != nil || bad > 1 || valid(list[bad]) || (bad == 1 && !valid(a)) {
			t.Fatalf("decodeRawPatterns(%q) = %q, %d", list, decoded, bad)
		}
	})
}

// FuzzCommitPackFilePath: a file operation names its path with exactly one
// of path and path_base64 (an empty one is absent), and the path it ends with
// is the one it names.
func FuzzCommitPackFilePath(f *testing.F) {
	for _, seed := range [][2]string{
		{"a.txt", ""}, {"", "Yf9i"}, {"a.txt", "Yf9i"}, {"", ""}, {"", "YR=="}, {"", "Li4v"},
		{"", "LmdpdC9jb25maWc="}, {"", "YQBi"}, {"", "AA=="}, {"../a", ""}, {"a\xffb", ""},
	} {
		f.Add(seed[0], seed[1])
	}
	f.Fuzz(func(t *testing.T, path, enc string) {
		files := []wcFileOp{{Path: path, PathBase64: enc, ContentID: "c", Operation: "upsert"}}
		err := wcValidateFiles(files)
		decoded, decodedOK := decodeRawPath(enc)
		want := path
		if enc != "" {
			want = decoded
		}
		switch {
		case enc != "" && (path != "" || !decodedOK):
			if err == nil {
				t.Fatalf("path %q, path_base64 %q: accepted", path, enc)
			}
		case err == nil:
			if files[0].Path != want || wcValidPath(want) != nil {
				t.Fatalf("path %q, path_base64 %q: resolved to %q, want the valid path %q", path, enc, files[0].Path, want)
			}
		default:
			if wcValidPath(want) == nil {
				t.Fatalf("path %q, path_base64 %q: refused with %v though %q is valid", path, enc, err, want)
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

// FuzzApplyRejection: wcApplyStatus reads git apply's stderr,
// which can quote any file name the client sent. It must be total, answer
// 409 for exit 1, 500 for any status but 1 and 128, and for 128 read only
// whole lines that start with a known message, ignoring the lines before.
func FuzzApplyRejection(f *testing.F) {
	for _, seed := range []struct {
		exit int
		msg  string
	}{
		{128, "error: invalid path '.git./x'"},
		{128, "error: invalid path 'git~1/x'"},
		{128, "error: invalid path '.gitmodules'"},
		{128, "error: patch with only garbage at <stdin>:4"},
		{128, "error: patch with only garbage at <stdin>:3"},
		{128, "error: corrupt patch for submodule sub"},
		{128, "error: cache entry has null sha1: sub\nerror: Unable to write new index file"},
		{128, "error: 'd' appears as both a file and as a directory\nerror: unable to add cache entry for d"},
		{128, "fatal: Unable to create '/tmp/openstorage-write-1/index.lock': File exists."},
		{128, "error: unable to read index file"},
		{128, "error: Unable to write new index file"},
		{128, "error: No valid patches in input (allow with \"--allow-empty\")"},
		{1, "error: patch failed: f:1\nerror: f: patch does not apply"},
		{128, "error: invalid path '\nerror: unable to add cache entry for "},
		{128, ""},
		{0, "error: invalid path 'a'"},
		{-1, "error: invalid path 'a'"},
	} {
		f.Add(seed.exit, seed.msg)
	}
	hasLine := func(msg, prefix string) bool {
		return strings.HasPrefix(msg, prefix) || strings.Contains(msg, "\n"+prefix)
	}
	f.Fuzz(func(t *testing.T, exit int, msg string) {
		got := wcApplyStatus(exit, msg)
		want := http.StatusInternalServerError
		switch {
		case exit == 1:
			want = http.StatusConflict
		case exit != 128:
		case slices.ContainsFunc(wcApplyPayloadLines, func(p string) bool { return hasLine(msg, p) }):
			want = http.StatusBadRequest
		case hasLine(msg, wcApplyClashLine):
			want = http.StatusConflict
		}
		if got != want {
			t.Fatalf("wcApplyStatus(%d, %q) = %d, want %d", exit, msg, got, want)
		}
		if noisy := wcApplyStatus(exit, "noise\n"+msg); noisy != got {
			t.Fatalf("a line before %q changed the status of exit %d from %d to %d", msg, exit, got, noisy)
		}
	})
}

// FuzzPatchModeError: wcPatchModeError reads `git apply --summary`, which
// prints file names as they are. It must be total, a line that is not a mode
// line or that holds a valid mode must not change its answer, and a directory
// mode on a line of its own must always fail it.
func FuzzPatchModeError(f *testing.F) {
	for _, seed := range []string{
		"",
		" create mode 100644 a.txt\n",
		" create mode 040000 dd\n",
		" mode change 100644 => 040000 a.txt\n",
		" mode change 100644 => 100755 a.txt\n create mode 160000 sub\n",
		" rename a => b (100%)\n delete mode 040000 c\n",
		" create mode 100644 x\n create mode 040000 y\n",
		" mode change 100644\n",
		" create mode ",
		" create mode  \n",
		"x create mode 040000 y",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, summary string) {
		err := wcPatchModeError(summary)
		for _, extra := range []string{"noise\n", " create mode 100644 x\n", " mode change 100644 => 100755 y\n", " delete mode 040000 z\n"} {
			if got := wcPatchModeError(extra + summary); (got == nil) != (err == nil) {
				t.Fatalf("the line %q changed the answer for %q from %v to %v", extra, summary, err, got)
			}
		}
		if wcPatchModeError(" create mode 040000 dd\n"+summary) == nil {
			t.Fatalf("a directory mode in front of %q passed", summary)
		}
	})
}
