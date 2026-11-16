package githttp

import (
	"context"
	"crypto"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jws"

	"github.com/coder/openstorage/internal/auth"
)

// tcdDecoderText is what encoding/json and the JWT library say about input
// that does not decode. It names Go structs and types, which a client must
// never see (issue #841).
var tcdDecoderText = []string{"Go struct", "Go value", "json:", "cannot unmarshal", "of type", "unexpected end of JSON", "invalid character", "jws.Parse", "failed to parse"}

type tcdKeys map[string][]string

func (k tcdKeys) PublicKeysForOrg(_ context.Context, name string) ([]string, error) {
	pems, ok := k[name]
	if !ok {
		return nil, errors.New("org not found")
	}
	return pems, nil
}

const tcdOID = "0000000000000000000000000000000000000000000000000000000000000000"

// tcdRoute is a route that verifies a token, with a request that reaches the
// verification.
type tcdRoute struct {
	name, method, path, body string
	bearer                   bool
}

// tcdRoutes lists every git and LFS route that verifies a token.
var tcdRoutes = []tcdRoute{
	{"info/refs, fetch", http.MethodGet, "/r.git/info/refs?service=git-upload-pack", "", false},
	{"info/refs, push", http.MethodGet, "/r.git/info/refs?service=git-receive-pack", "", false},
	{"upload-pack", http.MethodPost, "/r.git/git-upload-pack", "", false},
	{"receive-pack", http.MethodPost, "/r.git/git-receive-pack", "", false},
	{"LFS batch, basic", http.MethodPost, "/r.git/info/lfs/objects/batch", `{"operation":"download"}`, false},
	{"LFS batch, bearer", http.MethodPost, "/r.git/info/lfs/objects/batch", `{"operation":"upload"}`, true},
	{"LFS object download", http.MethodGet, "/r.git/info/lfs/objects/" + tcdOID, "", true},
	{"LFS object upload", http.MethodPut, "/r.git/info/lfs/objects/" + tcdOID, "x", false},
}

// tcdOrg is the org that tcdServer's verifier knows.
const tcdOrg = "acme"

// tcdServer returns a server with a real verifier and the ES256 key that
// signs for tcdOrg. It has no stores: a request that passes authentication
// would fail later.
func tcdServer(t *testing.T) (*Server, crypto.Signer) {
	t.Helper()
	key, err := auth.GenerateKey("ES256")
	if err != nil {
		t.Fatal(err)
	}
	pemBytes, err := auth.MarshalPublicKeyPEM(key.Public())
	if err != nil {
		t.Fatal(err)
	}
	return &Server{Verifier: auth.NewVerifier(tcdKeys{tcdOrg: {string(pemBytes)}}), Org: tcdOrg}, key
}

// tcdServe sends rt with token as its credential to srv.
func tcdServe(srv *Server, rt tcdRoute, token string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(rt.method, rt.path, strings.NewReader(rt.body))
	if rt.bearer {
		req.Header.Set("Authorization", "Bearer "+token)
	} else {
		req.SetBasicAuth("t", token)
	}
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	return rec
}

// TestTokenClaimDecodeAnswersHoldNoGoTypeNames covers issue #841 on the git
// and LFS transports: they answer 401 with the text of the verifier's error,
// so a token or a claim that does not decode must answer a fixed text. It
// holds for every route that verifies a token.
func TestTokenClaimDecodeAnswersHoldNoGoTypeNames(t *testing.T) {
	captureLog(t) // the verifier logs a signed claim that does not decode
	srv, key := tcdServer(t)

	seg := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
	unsigned := func(header, payload string) string { return seg(header) + "." + seg(payload) + "." + seg("sig") }
	exp := time.Now().Add(time.Hour).Unix()
	signed := func(payload string) string {
		tok, err := jws.Sign([]byte(payload), jws.WithKey(jwa.ES256(), key))
		if err != nil {
			t.Fatal(err)
		}
		return string(tok)
	}
	const es256 = `{"alg":"ES256"}`
	tokens := []struct{ name, token, want string }{
		{"a header that is not JSON", unsigned("x", `{"iss":"acme"}`), "malformed jwt"},
		{"a header member of the wrong type", unsigned(`{"alg":5}`, `{"iss":"acme"}`), "malformed jwt"},
		{"a payload that is not JSON", unsigned(es256, `{`), "malformed jwt claims"},
		{"an iss of the wrong type", unsigned(es256, `{"iss":5}`), "malformed jwt claims"},
		{"a signed exp of the wrong type", signed(`{"iss":"acme","sub":"s","repo":"r","exp":"x"}`), "malformed jwt claims"},
		{"signed scopes of the wrong type", signed(`{"iss":"acme","sub":"s","repo":"r","exp":` + strconv.FormatInt(exp, 10) + `,"scopes":5}`), "malformed jwt claims"},
		{"a signed repo of the wrong type", signed(`{"iss":"acme","sub":"s","repo":5,"exp":` + strconv.FormatInt(exp, 10) + `}`), "malformed jwt claims"},
	}
	for _, tok := range tokens {
		for _, rt := range tcdRoutes {
			t.Run(tok.name+", "+rt.name, func(t *testing.T) {
				rec := tcdServe(srv, rt, tok.token)
				if rec.Code != http.StatusUnauthorized {
					t.Fatalf("status %d, want 401 (body %s)", rec.Code, rec.Body)
				}
				if !strings.Contains(rec.Body.String(), tok.want) {
					t.Fatalf("the answer does not hold %q: %s", tok.want, rec.Body)
				}
				for _, bad := range tcdDecoderText {
					if strings.Contains(rec.Body.String(), bad) {
						t.Fatalf("the answer holds %q: %s", bad, rec.Body)
					}
				}
			})
		}
	}
}

// tcdSign returns a token for tcdOrg that key signed, with extra protected
// header members.
func tcdSign(t *testing.T, key crypto.Signer, header map[string]any) string {
	t.Helper()
	payload, err := json.Marshal(auth.Claims{
		Iss: tcdOrg, Sub: auth.SubjectLiteral, Repo: "r", Scopes: []string{auth.ScopeGitRead},
		Exp: time.Now().Add(time.Hour).Unix(),
	})
	if err != nil {
		t.Fatal(err)
	}
	hdrs := jws.NewHeaders()
	for name, v := range header {
		if err := hdrs.Set(name, v); err != nil {
			t.Fatal(err)
		}
	}
	tok, err := jws.Sign(payload, jws.WithKey(jwa.ES256(), key, jws.WithProtectedHeaders(hdrs)))
	if err != nil {
		t.Fatal(err)
	}
	return string(tok)
}

// TestCritHeaderIsRefusedOnEveryRoute: a token that a registered key signed
// is refused when its header has `crit` (RFC 7515 §4.1.11), on every git and
// LFS route that verifies a token, with the fixed text. The same claims
// without `crit` verify.
func TestCritHeaderIsRefusedOnEveryRoute(t *testing.T) {
	srv, key := tcdServer(t)
	if _, err := srv.Verifier.Verify(context.Background(), tcdSign(t, key, nil)); err != nil {
		t.Fatalf("a token without crit: %v", err)
	}
	withCrit := tcdSign(t, key, map[string]any{"crit": []string{"x-ext"}, "x-ext": true})
	for _, rt := range tcdRoutes {
		t.Run(rt.name, func(t *testing.T) {
			rec := tcdServe(srv, rt, withCrit)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status %d, want 401 (body %s)", rec.Code, rec.Body)
			}
			if want := "jwt crit header is not supported"; !strings.Contains(rec.Body.String(), want) {
				t.Fatalf("the answer does not hold %q: %s", want, rec.Body)
			}
		})
	}
}

// tcdHeaderTails are bytes that can follow the object of a protected header:
// other values, bytes that start no value, and a NUL.
var tcdHeaderTails = []string{"x", "}", "]", "{}", "\x00", " x", `"`, "0", `{"crit":null}`}

// tcdHeaderPads are the bytes that the tests put before and after a header
// object: every tail, and each JSON whitespace byte before, after and on both
// sides of it.
func tcdHeaderPads() [][2]string {
	var pads [][2]string
	for _, tail := range tcdHeaderTails {
		pads = append(pads, [2]string{"", tail})
	}
	for _, ws := range []string{" ", "\t", "\r", "\n"} {
		pads = append(pads, [2]string{ws, ""}, [2]string{"", ws}, [2]string{ws, ws})
	}
	return pads
}

// TestHeaderPadIsRefusedOnEveryRoute: a token that a registered key signed is
// refused when its header segment holds any byte outside the header object,
// whitespace included, with or without `crit`, on every git and LFS route that
// verifies a token. The signature still covers the object alone, and jwx
// verifies it against the object alone. The answer is the fixed `malformed jwt`.
func TestHeaderPadIsRefusedOnEveryRoute(t *testing.T) {
	srv, key := tcdServer(t)
	for name, header := range map[string]map[string]any{
		"without crit": nil,
		"with crit":    {"crit": []string{"x-ext"}, "x-ext": true},
	} {
		token := tcdSign(t, key, header)
		seg, rest, _ := strings.Cut(token, ".")
		obj, err := base64.RawURLEncoding.DecodeString(seg)
		if err != nil {
			t.Fatal(err)
		}
		for _, pad := range tcdHeaderPads() {
			padded := base64.RawURLEncoding.EncodeToString([]byte(pad[0]+string(obj)+pad[1])) + "." + rest
			for _, rt := range tcdRoutes {
				t.Run(fmt.Sprintf("%s %q %q, %s", name, pad[0], pad[1], rt.name), func(t *testing.T) {
					rec := tcdServe(srv, rt, padded)
					if rec.Code != http.StatusUnauthorized {
						t.Fatalf("status %d, want 401 (body %s)", rec.Code, rec.Body)
					}
					if want := "malformed jwt"; !strings.Contains(rec.Body.String(), want) {
						t.Fatalf("the answer does not hold %q: %s", want, rec.Body)
					}
					for _, bad := range tcdDecoderText {
						if strings.Contains(rec.Body.String(), bad) {
							t.Fatalf("the answer holds %q: %s", bad, rec.Body)
						}
					}
				})
			}
		}
	}
}

// TestMalformedRefsClaimPushAnswersFixedText covers issue #841 for a push:
// the hook refuses a token whose `refs` claim does not parse, and git
// relays the hook's text to the pusher. The text is fixed.
func TestMalformedRefsClaimPushAnswersFixedText(t *testing.T) {
	repo := registerRepo(t, "t-refs-claim-text")
	work := initWorkRepo(t)
	tok := mintTestToken(t, repo, "git:read,git:write", `[[5,[]]]`)
	out, err := tryGit(work, "push", remoteURL(nodeA, repo, tok), "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("push with a malformed refs claim succeeded:\n%s", out)
	}
	if !strings.Contains(out, "malformed refs claim") {
		t.Fatalf("the pusher does not read the fixed text:\n%s", out)
	}
	for _, bad := range tcdDecoderText {
		if strings.Contains(out, bad) {
			t.Fatalf("the pusher reads %q:\n%s", bad, out)
		}
	}
}

// TestRefPolicyHookSplitsPusherAndOperatorText: the hook refuses a refs claim
// that does not parse with a fixed text that the pusher may read, and writes
// the decoder's text to the hook-errors file, which serve logs for the
// operator.
func TestRefPolicyHookSplitsPusherAndOperatorText(t *testing.T) {
	records, err := createHookErrors(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvHookErrors, records)
	// loadRefPolicies memoizes its last input, errors included.
	refPolicyCache.Store(nil)
	t.Cleanup(func() { refPolicyCache.Store(nil) })

	_, err = loadRefPolicies(`[[5,[]]]`, "")
	if err == nil || err.Error() != "update: malformed refs claim" || !ShownToPusher(err) {
		t.Fatalf("error %v, want the fixed text that the pusher may read", err)
	}
	var bad *auth.MalformedError
	if !errors.As(err, &bad) {
		t.Fatalf("error %T does not wrap *auth.MalformedError", err)
	}
	b, err := os.ReadFile(records)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(b); !strings.Contains(got, "refs claim does not parse") || !strings.Contains(got, "cannot unmarshal") {
		t.Fatalf("the hook-errors file does not hold the decoder's text: %s", got)
	}
}
