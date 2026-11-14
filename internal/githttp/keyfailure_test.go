package githttp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/meta"
)

// kfKeys is the key source of the org "acme": it answers err, or else pems.
type kfKeys struct {
	pems []string
	err  error
}

func (k kfKeys) PublicKeysForOrg(context.Context, string) ([]string, error) { return k.pems, k.err }

const (
	kfStoreDown   = "dial tcp canary-db.internal:5432: connection refused"
	kfCorruptPEM  = "-----BEGIN PUBLIC KEY-----\nAAAA\n-----END PUBLIC KEY-----\n"
	kfStoreAnswer = `issuer "acme": keys unavailable`
	kfStoredKey   = `issuer "acme": a registered key cannot be used`
)

// kfToken returns a token for the issuer iss. The key store of kfServer fails
// before any signature is checked, so any key signs it.
func kfToken(t *testing.T, iss string) string {
	t.Helper()
	key, err := auth.GenerateKey("ES256")
	if err != nil {
		t.Fatal(err)
	}
	claims := testClaims("r", "git:read,git:write")
	claims.Iss = iss
	tok, err := auth.MintToken(key, "ES256", claims)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// kfServer returns a server whose only org, "acme", has the key source keys,
// and a token that the org's key signed.
func kfServer(t *testing.T, keys kfKeys) (*Server, string) {
	t.Helper()
	key, err := auth.GenerateKey("ES256")
	if err != nil {
		t.Fatal(err)
	}
	claims := testClaims("r", "git:read,git:write")
	claims.Iss = "acme"
	tok, err := auth.MintToken(key, "ES256", claims)
	if err != nil {
		t.Fatal(err)
	}
	return &Server{Verifier: auth.NewVerifier(keys), Org: "acme"}, tok
}

// TestKeyFailureAnswersHoldNoStoreText: a key store that fails, and a key of
// the org that does not parse, answer 401 with fixed text on every route
// that verifies a token. The store's error names its database host, and the
// parser's error names the key's encoding: a client must read neither.
func TestKeyFailureAnswersHoldNoStoreText(t *testing.T) {
	captureLog(t)
	for _, tc := range []struct {
		name string
		keys kfKeys
		want string
		// leaks is text of the failure that no answer may hold.
		leaks []string
	}{
		{"the key store fails", kfKeys{err: errors.New(kfStoreDown)}, kfStoreAnswer,
			[]string{"canary-db", "dial tcp", "5432", "connection refused"}},
		{"a registered key does not parse", kfKeys{pems: []string{kfCorruptPEM}}, kfStoredKey,
			[]string{"asn1", "x509", "PEM", "structure error", "parse public key"}},
		{"the issuer is not registered", kfKeys{err: fmt.Errorf("org %q: %w", "acme", meta.ErrNotFound)}, `issuer "acme" is not registered`,
			[]string{"not found"}},
	} {
		srv, tok := kfServer(t, tc.keys)
		for _, rt := range tcdRoutes {
			t.Run(tc.name+", "+rt.name, func(t *testing.T) {
				rec := tcdServe(srv, rt, tok)
				if rec.Code != http.StatusUnauthorized {
					t.Fatalf("status %d, want 401 (body %s)", rec.Code, rec.Body)
				}
				if rec.Header().Get("WWW-Authenticate") == "" {
					t.Fatal("a 401 without a WWW-Authenticate challenge")
				}
				// A git route answers {"error": text}, an LFS route {"message": text}.
				var answer map[string]string
				if err := json.Unmarshal(rec.Body.Bytes(), &answer); err != nil || len(answer) != 1 {
					t.Fatalf("body %s is not one JSON text: %v", rec.Body, err)
				}
				for _, text := range answer {
					if text != tc.want {
						t.Fatalf("the answer is %q, want %q", text, tc.want)
					}
				}
				for _, leak := range tc.leaks {
					if strings.Contains(rec.Body.String(), leak) {
						t.Fatalf("the answer holds %q: %s", leak, rec.Body)
					}
				}
			})
		}
	}
}

// countKeyFailureLines returns how many lines of out report a key failure.
func countKeyFailureLines(out string) int {
	return strings.Count(out, "verify token: ")
}

// TestKeyFailureLogIsRateLimited: a key store that is down logs its error
// once a minute, with the count of the failures that were not logged. A
// client that names an unknown issuer, or hangs up while the keys are read,
// logs nothing.
func TestKeyFailureLogIsRateLimited(t *testing.T) {
	out := captureLog(t)
	srv, tok := kfServer(t, kfKeys{err: errors.New(kfStoreDown)})
	clock := time.Unix(1_000_000, 0)
	srv.keyFailures.now = func() time.Time { return clock }
	rt := tcdRoutes[0]

	for range 5 {
		tcdServe(srv, rt, tok)
	}
	if n := countKeyFailureLines(out.String()); n != 1 || !strings.Contains(out.String(), "canary-db.internal") {
		t.Fatalf("5 failures in one second logged %d lines: %q", n, out)
	}
	clock = clock.Add(keyFailureLogEvery - time.Second)
	tcdServe(srv, rt, tok)
	if n := countKeyFailureLines(out.String()); n != 1 {
		t.Fatalf("a failure inside the interval logged: %q", out)
	}
	clock = clock.Add(2 * time.Second)
	tcdServe(srv, rt, tok)
	if n := countKeyFailureLines(out.String()); n != 2 || !strings.Contains(out.String(), "(5 earlier failures of this kind not logged)") {
		t.Fatalf("the failure after the interval did not log the held count: %q", out)
	}
	// The count starts again after a line, and a failure of the same kind
	// for another issuer is held back like any other: a client names the
	// issuer.
	for _, iss := range []string{"acme2", "acme3"} {
		tcdServe(srv, rt, kfToken(t, iss))
	}
	if n := countKeyFailureLines(out.String()); n != 2 {
		t.Fatalf("a failure for another issuer logged inside the interval: %q", out)
	}
	clock = clock.Add(keyFailureLogEvery)
	tcdServe(srv, rt, tok)
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if last := lines[len(lines)-1]; len(lines) != 3 || !strings.Contains(last, "(2 earlier failures of this kind not logged)") {
		t.Fatalf("the third line does not hold the count of its own interval: %q", out)
	}

	t.Run("an unknown issuer", func(t *testing.T) {
		out := captureLog(t)
		srv, tok := kfServer(t, kfKeys{err: fmt.Errorf("org %q: %w", "acme", meta.ErrNotFound)})
		for range 3 {
			tcdServe(srv, rt, tok)
		}
		if out.Len() != 0 {
			t.Fatalf("an unknown issuer logged: %q", out)
		}
	})
	t.Run("a request that ended", func(t *testing.T) {
		out := captureLog(t)
		srv, _ := kfServer(t, kfKeys{})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := srv.verifyFailure(ctx, &auth.KeySourceError{Org: "acme", Err: context.Canceled})
		if err.Error() != kfStoreAnswer {
			t.Fatalf("error %q, want %q", err, kfStoreAnswer)
		}
		if out.Len() != 0 {
			t.Fatalf("a request that ended logged: %q", out)
		}
	})
	t.Run("a key that does not parse", func(t *testing.T) {
		out := captureLog(t)
		srv, tok := kfServer(t, kfKeys{pems: []string{kfCorruptPEM}})
		for range 3 {
			tcdServe(srv, rt, tok)
		}
		if n := countKeyFailureLines(out.String()); n != 1 || !strings.Contains(out.String(), "asn1") {
			t.Fatalf("3 failures logged %d lines, want 1 with the parser's error: %q", n, out)
		}
	})
}

// TestKeyFailureLogKindsAreIndependent: a key store failure does not hold back
// the log line of a key that does not parse, nor the reverse.
func TestKeyFailureLogKindsAreIndependent(t *testing.T) {
	out := captureLog(t)
	var l keyFailureLog
	clock := time.Unix(1_000_000, 0)
	l.now = func() time.Time { return clock }
	l.report("one", "acme", errors.New("a"))
	l.report("two", "acme", errors.New("b"))
	l.report("one", "acme", errors.New("c"))
	if got := out.String(); strings.Count(got, "verify token: ") != 2 || strings.Contains(got, ": c") {
		t.Fatalf("log: %q", got)
	}
}

// TestKeyFailureLogLineIsBounded: a store repeats the issuer in its own
// error, so a line holds a bounded part of the cause, and one line.
func TestKeyFailureLogLineIsBounded(t *testing.T) {
	out := captureLog(t)
	var l keyFailureLog
	huge := strings.Repeat("a", 100_000)
	l.report("kind", huge, fmt.Errorf("lookup org %q: connection refused\nsecond line", huge))
	line := out.String()
	if len(line) > 1000 || strings.Count(line, "\n") != 1 {
		t.Fatalf("the line is %d bytes with %d newlines: %.200q", len(line), strings.Count(line, "\n"), line)
	}
}
