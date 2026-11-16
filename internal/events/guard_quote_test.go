package events

import (
	"context"
	"net/netip"
	"strings"
	"testing"
)

// TestCheckRegistrationQuotesURLTextBounded: a webhook registration refusal
// quotes the host or the URL it names with errtext.Quote, cut at 256 bytes
// with its length, also for a host that resolves to a denied address and
// for a URL that does not parse.
func TestCheckRegistrationQuotesURLTextBounded(t *testing.T) {
	t.Parallel()

	guard := TargetGuard{LookupNetIP: func(context.Context, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("10.0.0.1")}, nil
	}}
	longName := strings.Repeat("a", 300) + ".example"
	longPort := strings.Repeat("<", 1000)
	for _, tc := range []struct{ name, rawURL, want string }{
		{"a host that resolves to a denied address", "https://private.example/hook",
			`webhook target "private.example" resolves to denied address 10.0.0.1: private address`},
		{"a long host that resolves to a denied address", "https://" + longName + "/hook",
			`webhook target "` + strings.Repeat("a", 256) + `…" (308 bytes) resolves to denied address 10.0.0.1: private address`},
		// url.Parse names the bad port in its error too, so that is cut.
		{"a URL that does not parse", "https://h:" + longPort,
			`parse webhook url "https://h:` + strings.Repeat("<", 256-10) + `…" (1010 bytes): invalid port ":` + strings.Repeat("<", 256-15) + `…`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := guard.CheckRegistration(context.Background(), tc.rawURL)
			if err == nil || err.Error() != tc.want {
				t.Fatalf("CheckRegistration = %.400v\nwant %.400s", err, tc.want)
			}
		})
	}
}
