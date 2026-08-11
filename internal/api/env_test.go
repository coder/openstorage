package api

// Shared integration-test environment: one Postgres testcontainer (started
// lazily so container-free unit tests run without Docker), one in-process
// API server on 127.0.0.1:0, and runtime-generated signing keys for every
// allowed JWT algorithm (never committed).
//
// INTEGRATION NOTE: newTestDeps is the single seam wiring the frozen
// meta.Store/auth.Verifier contracts into these tests; swap in the sibling
// slice's production constructors here when they land.

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jws"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"codestorage/internal/auth"
	"codestorage/internal/meta"
)

const testHTTPBaseURL = "https://selfhost.code.storage"

type testKeys struct {
	es256 *ecdsa.PrivateKey
	es384 *ecdsa.PrivateKey
	es512 *ecdsa.PrivateKey
	rsa   *rsa.PrivateKey // backs both RS256 and PS256
	ed    ed25519.PrivateKey

	pubPEMs []string // registered for every test org
}

type testEnv struct {
	store meta.Store
	srv   *httptest.Server
	keys  *testKeys
}

var (
	envOnce sync.Once
	envErr  error
	env     *testEnv

	envCleanup []func()
)

// newTestDeps builds the meta.Store and auth.Verifier driving the matrix.
// Currently backed by the test-scoped implementations in this package; see
// the INTEGRATION NOTE above.
func newTestDeps(ctx context.Context, pgURL string) (meta.Store, auth.Verifier, error) {
	store, err := openPGMetaStore(ctx, pgURL)
	if err != nil {
		return nil, nil, err
	}
	envCleanup = append(envCleanup, store.Close)
	return store, &storeVerifier{store: store}, nil
}

func generateTestKeys() (*testKeys, error) {
	keys := &testKeys{}
	var err error
	if keys.es256, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader); err != nil {
		return nil, err
	}
	if keys.es384, err = ecdsa.GenerateKey(elliptic.P384(), rand.Reader); err != nil {
		return nil, err
	}
	if keys.es512, err = ecdsa.GenerateKey(elliptic.P521(), rand.Reader); err != nil {
		return nil, err
	}
	if keys.rsa, err = rsa.GenerateKey(rand.Reader, 2048); err != nil {
		return nil, err
	}
	var edPub ed25519.PublicKey
	if edPub, keys.ed, err = ed25519.GenerateKey(rand.Reader); err != nil {
		return nil, err
	}
	for _, pub := range []any{
		&keys.es256.PublicKey, &keys.es384.PublicKey, &keys.es512.PublicKey,
		&keys.rsa.PublicKey, edPub,
	} {
		der, err := x509.MarshalPKIXPublicKey(pub)
		if err != nil {
			return nil, err
		}
		keys.pubPEMs = append(keys.pubPEMs,
			string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})))
	}
	return keys, nil
}

// testEnvironment lazily boots the PG container + in-process server.
func testEnvironment(t *testing.T) *testEnv {
	t.Helper()
	envOnce.Do(func() {
		ctx := context.Background()

		pgC, err := tcpostgres.Run(ctx, "postgres:17-alpine",
			tcpostgres.WithDatabase("postgres"),
			tcpostgres.WithUsername("postgres"),
			tcpostgres.WithPassword("postgres"),
			tcpostgres.BasicWaitStrategies(),
		)
		if err != nil {
			envErr = fmt.Errorf("start postgres container: %w", err)
			return
		}
		envCleanup = append(envCleanup, func() { _ = pgC.Terminate(ctx) })

		pgURL, err := pgC.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			envErr = err
			return
		}

		store, verifier, err := newTestDeps(ctx, pgURL)
		if err != nil {
			envErr = err
			return
		}

		keys, err := generateTestKeys()
		if err != nil {
			envErr = err
			return
		}

		srv := httptest.NewServer(New(Config{
			Store:       store,
			Verifier:    verifier,
			Logger:      discardLogger(),
			HTTPBaseURL: testHTTPBaseURL,
		}))
		envCleanup = append(envCleanup, srv.Close)

		env = &testEnv{store: store, srv: srv, keys: keys}
	})
	if envErr != nil {
		t.Fatalf("test environment: %v", envErr)
	}
	return env
}

func TestMain(m *testing.M) {
	code := m.Run()
	for i := len(envCleanup) - 1; i >= 0; i-- {
		envCleanup[i]()
	}
	os.Exit(code)
}

func randomHex(t *testing.T, n int) string {
	t.Helper()
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(buf)
}

// newUnregisteredES256Key generates a fresh P-256 key that is NOT registered
// with any org (for the bad-signature matrix case).
func newUnregisteredES256Key(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

// newOrg registers a fresh org (unique name, all test public keys) so each
// test gets an isolated repo namespace.
func (e *testEnv) newOrg(t *testing.T) string {
	t.Helper()
	name := "org-" + randomHex(t, 6)
	if _, err := e.store.UpsertOrgWithKeys(context.Background(), name, e.keys.pubPEMs); err != nil {
		t.Fatalf("register org %s: %v", name, err)
	}
	return name
}

// tokenSpec describes one JWT to mint. Zero values pick the happy-path
// defaults (ES256, registered key, fixed sub literal, exp in one hour).
type tokenSpec struct {
	alg    string // default "ES256"
	iss    string
	sub    string // default auth.SubjectLiteral
	repo   string
	scopes []string
	expIn  time.Duration // default 1h; negative mints an expired token
	key    any           // signing key override (default: env key for alg)
}

// signingKey returns the environment's private key for alg.
func (e *testEnv) signingKey(alg string) any {
	switch alg {
	case "ES256":
		return e.keys.es256
	case "ES384":
		return e.keys.es384
	case "ES512":
		return e.keys.es512
	case "RS256", "PS256":
		return e.keys.rsa
	case "EdDSA":
		return e.keys.ed
	default:
		panic("no test key for algorithm " + alg)
	}
}

// mint signs a JWT per spec with jws (compact serialization, typ JWT).
func (e *testEnv) mint(t *testing.T, spec tokenSpec) string {
	t.Helper()
	if spec.alg == "" {
		spec.alg = "ES256"
	}
	if spec.sub == "" {
		spec.sub = auth.SubjectLiteral
	}
	if spec.expIn == 0 {
		spec.expIn = time.Hour
	}
	if spec.key == nil {
		if spec.alg == "HS256" {
			spec.key = []byte("test-hmac-secret")
		} else {
			spec.key = e.signingKey(spec.alg)
		}
	}
	now := time.Now()
	payload, err := json.Marshal(map[string]any{
		"iss":    spec.iss,
		"sub":    spec.sub,
		"repo":   spec.repo,
		"scopes": spec.scopes,
		"iat":    now.Unix(),
		"exp":    now.Add(spec.expIn).Unix(),
	})
	if err != nil {
		t.Fatal(err)
	}
	algs := map[string]jwa.SignatureAlgorithm{
		"ES256": jwa.ES256(), "ES384": jwa.ES384(), "ES512": jwa.ES512(),
		"RS256": jwa.RS256(), "PS256": jwa.PS256(), "EdDSA": jwa.EdDSA(),
		"HS256": jwa.HS256(),
	}
	alg, ok := algs[spec.alg]
	if !ok {
		t.Fatalf("mint: unsupported alg %q", spec.alg)
	}
	hdrs := jws.NewHeaders()
	if err := hdrs.Set("typ", "JWT"); err != nil {
		t.Fatal(err)
	}
	signed, err := jws.Sign(payload, jws.WithKey(alg, spec.key, jws.WithProtectedHeaders(hdrs)))
	if err != nil {
		t.Fatalf("mint %s token: %v", spec.alg, err)
	}
	return string(signed)
}

// doReq performs one HTTP request against the in-process server. token ""
// omits the Authorization header; authHeader (optional) overrides it
// verbatim. Returns the response and the fully read body.
func (e *testEnv) doReq(t *testing.T, method, path, token string, body []byte, authHeader string) (*http.Response, []byte) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, e.srv.URL+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := e.srv.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return resp, respBody
}

// requireEnvelope asserts an error response carries the {"error"} envelope
// (and WWW-Authenticate on 401), returning the message.
func requireEnvelope(t *testing.T, resp *http.Response, body []byte) string {
	t.Helper()
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("error Content-Type = %q, want application/json (body %q)", ct, body)
	}
	var envlp errorEnvelope
	if err := json.Unmarshal(body, &envlp); err != nil || envlp.Error == "" {
		t.Fatalf("body %q is not a {\"error\"} envelope (err %v)", body, err)
	}
	if resp.StatusCode == http.StatusUnauthorized {
		if got := resp.Header.Get("WWW-Authenticate"); got != "Bearer" {
			t.Fatalf("401 WWW-Authenticate = %q, want Bearer", got)
		}
	}
	return envlp.Error
}
