package githttp

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"
)

// pkt frames one pkt-line (gitprotocol-common).
func pkt(s string) string { return fmt.Sprintf("%04x%s", len(s)+4, s) }

const (
	tOld = "1111111111111111111111111111111111111111"
	tNew = "2222222222222222222222222222222222222222"
)

func cmdLine(ref, caps string) string {
	return tOld + " " + tNew + " " + ref + "\x00" + caps
}

// TestSniffAtomicPush pins the wire-capability detection (issue #28): the
// `atomic` token on the first command line of an update-requests stream,
// with shallow-line and push-cert prefixes per the grammar, malformed
// framing failing open, and — in every case — byte-exact replay of the
// whole stream for receive-pack.
func TestSniffAtomicPush(t *testing.T) {
	manyShallow := strings.Repeat(pkt("shallow "+tOld), 3)
	overBudget := strings.Repeat(pkt("shallow "+tOld), sniffPeekBudget/len(pkt("shallow "+tOld))+2)

	cases := []struct {
		name  string
		input string
		want  bool
	}{
		{"atomic requested", pkt(cmdLine("refs/heads/main", "report-status side-band-64k atomic agent=git/2.55")) + "0000PACKdata", true},
		{"atomic absent", pkt(cmdLine("refs/heads/main", "report-status side-band-64k agent=git/2.55")) + "0000PACKdata", false},
		{"atomic last token no trailer", pkt(cmdLine("refs/heads/main", "report-status atomic")), true},
		{"shallow prefix then atomic", manyShallow + pkt(cmdLine("refs/heads/main", "atomic")) + "0000", true},
		{"push-cert with atomic", pkt("push-cert\x00report-status atomic\n") + pkt("certificate version 0.1\n"), true},
		{"atomic-named ref does not count", pkt(cmdLine("refs/heads/atomic", "report-status")), false},
		// git reads the capability list as a NUL-terminated C string
		// (Codex security round): tokens before a second NUL count even
		// with trailing bytes after it; tokens after it are invisible to
		// git and must be invisible to the sniffer too.
		{"second NUL ends caps: atomic before it counts", pkt(cmdLine("refs/heads/main", "report-status atomic\x00ignored junk")), true},
		{"second NUL ends caps: atomic after it does not", pkt(cmdLine("refs/heads/main", "report-status\x00atomic")), false},
		{"agent=atomic does not count", pkt(cmdLine("refs/heads/main", "agent=atomic report-status")), false},
		{"no capabilities (no NUL)", pkt(tOld + " " + tNew + " refs/heads/main"), false},
		{"empty capability list", pkt(cmdLine("refs/heads/main", "")), false},
		{"flush first (nothing to push)", "0000", false},
		{"empty stream", "", false},
		{"empty pkt-line", "0004" + pkt(cmdLine("x", "atomic")), false},
		{"delim pkt invalid here", "0001" + pkt(cmdLine("x", "atomic")), false},
		{"malformed length", "zzzz garbage", false},
		{"oversized length", "ffff" + strings.Repeat("x", 32), false},
		{"truncated payload", "0040short", false},
		// A client can pad the shallow prefix past any fixed budget, so
		// exhaustion must fail CLOSED (atomic) or padding would bypass the
		// gate (Codex round 1) — regardless of what follows the budget.
		{"peek budget exhausted fails closed (atomic follows)", overBudget + pkt(cmdLine("refs/heads/main", "atomic")), true},
		{"peek budget exhausted fails closed (non-atomic follows)", overBudget + pkt(cmdLine("refs/heads/main", "report-status")), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, replay := sniffAtomicPush(strings.NewReader(tc.input))
			if got != tc.want {
				t.Fatalf("sniffAtomicPush = %v, want %v", got, tc.want)
			}
			rest, err := io.ReadAll(replay)
			if err != nil {
				t.Fatalf("replay: %v", err)
			}
			if !bytes.Equal(rest, []byte(tc.input)) {
				t.Fatalf("replay mismatch: got %d bytes %q, want %d bytes", len(rest), rest[:min(len(rest), 64)], len(tc.input))
			}
		})
	}
}
