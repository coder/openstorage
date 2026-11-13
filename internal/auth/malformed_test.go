package auth

import (
	"bytes"
	"context"
	"errors"
	"log"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// goDecoderText is what encoding/json and jwx say about input that does not
// decode. It names Go structs and types, which a client must never see
// (issue #841).
var goDecoderText = []string{"Go struct", "Go value", "json:", "cannot unmarshal", "of type", "unexpected end of JSON", "invalid character", "jws.Parse", "failed to parse"}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// captureSlog replaces slog's default logger until the test ends. No test
// that calls it may run in parallel.
func captureSlog(t *testing.T) *lockedBuffer {
	t.Helper()
	out := &lockedBuffer{}
	prev := slog.Default()
	// slog.SetDefault also redirects the log package, and restoring slog's
	// own default handler does not undo that.
	prevOut, prevFlags := log.Writer(), log.Flags()
	slog.SetDefault(slog.New(slog.NewTextHandler(out, nil)))
	t.Cleanup(func() {
		slog.SetDefault(prev)
		log.SetOutput(prevOut)
		log.SetFlags(prevFlags)
	})
	return out
}

// requireMalformed fails unless err is a *MalformedError with the fixed text
// want, no text of a decoder, and the decoder's error behind Unwrap.
func requireMalformed(t *testing.T, err error, want string) {
	t.Helper()
	var bad *MalformedError
	if !errors.As(err, &bad) {
		t.Fatalf("error %T %q, want *MalformedError", err, err)
	}
	if err.Error() != want {
		t.Fatalf("error text %q, want %q", err, want)
	}
	for _, text := range goDecoderText {
		if strings.Contains(err.Error(), text) {
			t.Fatalf("error text %q holds %q", err, text)
		}
	}
	if errors.Unwrap(err) == nil || bad.Err == nil {
		t.Fatalf("error %q has no decoder error behind it", err)
	}
}

// TestVerify_MalformedTokenAnswersFixedText: a token or a claim that does not
// decode gets a fixed text, whatever the decoder said. The git and LFS
// transports show that text to the client (issue #841). Claims decoded
// before the signature check are not logged (the client chose them), and
// claims that a registered key signed are, with the decoder's text.
func TestVerify_MalformedTokenAnswersFixedText(t *testing.T) {
	key := mustGenerateKey(t, "ES256")
	v := NewVerifier(fakeKeySource{"acme": {mustPublicPEM(t, key)}})
	exp := time.Now().Add(time.Hour).Unix()
	signed := func(payload map[string]any) string { return mustMintPayload(t, key, "ES256", payload) }
	unsigned := func(header, payload string) string {
		return b64seg([]byte(header)) + "." + b64seg([]byte(payload)) + "." + b64seg([]byte("sig"))
	}
	const es256 = `{"alg":"ES256"}`

	for _, tc := range []struct {
		name, token, want string
		logged            bool // a registered key signed the token
	}{
		{"a header that is not JSON", unsigned("x", `{"iss":"acme"}`), "malformed jwt", false},
		{"a header member of the wrong type", unsigned(`{"alg":5}`, `{"iss":"acme"}`), "malformed jwt", false},
		{"a header that is not an object", unsigned(`[1]`, `{"iss":"acme"}`), "malformed jwt", false},
		{"a payload that is not JSON", unsigned(es256, `{`), "malformed jwt claims", false},
		{"a payload that is not an object", unsigned(es256, `[1]`), "malformed jwt claims", false},
		{"an iss of the wrong type", unsigned(es256, `{"iss":5}`), "malformed jwt claims", false},
		{"a signed exp of the wrong type", signed(map[string]any{"iss": "acme", "sub": "s", "exp": "x"}), "malformed jwt claims", true},
		{"a signed sub of the wrong type", signed(map[string]any{"iss": "acme", "sub": 5, "exp": exp}), "malformed jwt claims", true},
		{"signed scopes of the wrong type", signed(map[string]any{"iss": "acme", "sub": "s", "exp": exp, "scopes": 5}), "malformed jwt claims", true},
		{"a signed repo of the wrong type", signed(map[string]any{"iss": "acme", "sub": "s", "exp": exp, "repo": 5}), "malformed jwt claims", true},
		{"signed ops of the wrong type", signed(map[string]any{"iss": "acme", "sub": "s", "exp": exp, "ops": "x"}), "malformed jwt claims", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureSlog(t)
			_, err := v.Verify(context.Background(), tc.token)
			requireMalformed(t, err, tc.want)
			if tc.logged {
				if got := logs.String(); !strings.Contains(got, "do not decode") || !strings.Contains(got, "cannot unmarshal") {
					t.Fatalf("a signed claim that does not decode left this log: %q", got)
				}
			} else if got := logs.String(); got != "" {
				t.Fatalf("a token that no key signed left this log: %q", got)
			}
		})
	}
}
