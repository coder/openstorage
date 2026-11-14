package api

// Shared integration-test environments, one per storage backend lane:
// the pg lane boots one Postgres testcontainer (started lazily so
// container-free lanes run without Docker), the sqlite lane opens a
// temp-dir DB file (zero containers). Each lane gets an in-process API
// server on 127.0.0.1:0 and runtime-generated signing keys for every
// allowed JWT algorithm (never committed); integration tests fan out over
// both lanes via runBackends.
//
// newTestDeps wires the PRODUCTION implementations (meta.Open scheme
// dispatch, auth.NewVerifier) so the integration matrix covers the code
// that ships.

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
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jws"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref"
	upsync "github.com/coder/openstorage/internal/sync"
	"github.com/coder/openstorage/internal/testutil/testimages"
)

const testHTTPBaseURL = "https://selfhost.open.storage"

// envSyncQueue satisfies the required Config.SyncQueue by dropping enqueued
// syncs: shared-env tests never run sync workers (the sync lifecycle is
// exercised by sync_test.go with its own recorders and queue).
type envSyncQueue struct{}

func (envSyncQueue) EnqueueSync(context.Context, upsync.Job) error { return nil }

func (envSyncQueue) EnqueuePush(context.Context, upsync.Job) error { return nil }

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

	// Read-endpoint dependencies (same backend DB as store, file:// blobs,
	// temp cache dir) so git:read integration tests can seed repos by real
	// git push through a githttp fallback and read them back over HTTP.
	refStore  ref.Store
	blobStore blob.BlobStore
	cacheDir  string
}

// backendState lazily boots one backend lane exactly once; the boot error
// is sticky so every subtest of a broken lane fails with the same cause.
type backendState struct {
	once sync.Once
	env  *testEnv
	err  error
}

// backendStates holds the per-lane singletons keyed by backend name.
var backendStates = map[string]*backendState{
	"pg":     {},
	"sqlite": {},
}

var envCleanup []func()

// newTestDeps builds the production meta backend (URL-scheme dispatch via
// meta.Open) and auth.Verifier driving the integration matrix.
func newTestDeps(ctx context.Context, metaURL string) (meta.Store, auth.Verifier, error) {
	store, err := meta.Open(ctx, metaURL)
	if err != nil {
		return nil, nil, err
	}
	if err := store.InitSchema(ctx); err != nil {
		store.Close()
		return nil, nil, err
	}
	envCleanup = append(envCleanup, store.Close)
	return store, auth.NewVerifier(store), nil
}

// backendMetaURL resolves the metadata store URL for one backend lane:
// pg boots a Postgres testcontainer exactly as before, sqlite derives a
// temp-dir DB file URL (zero containers).
func backendMetaURL(ctx context.Context, backend string) (string, error) {
	switch backend {
	case "pg":
		pgC, err := tcpostgres.Run(ctx, testimages.Postgres,
			tcpostgres.WithDatabase("postgres"),
			tcpostgres.WithUsername("postgres"),
			tcpostgres.WithPassword("postgres"),
			tcpostgres.BasicWaitStrategies(),
		)
		if err != nil {
			return "", fmt.Errorf("start postgres container: %w", err)
		}
		envCleanup = append(envCleanup, func() { _ = pgC.Terminate(ctx) })
		return pgC.ConnectionString(ctx, "sslmode=disable")
	case "sqlite":
		dir, err := os.MkdirTemp("", "openstorage-api-sqlite-*")
		if err != nil {
			return "", err
		}
		envCleanup = append(envCleanup, func() { _ = os.RemoveAll(dir) })
		return "sqlite://" + filepath.Join(dir, "meta.db"), nil
	default:
		panic("backendMetaURL: unknown backend " + backend)
	}
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

// testEnvironment lazily boots the named backend lane (PG container or
// temp-dir sqlite DB) + in-process server, once per lane.
func testEnvironment(t *testing.T, backend string) *testEnv {
	t.Helper()
	state, ok := backendStates[backend]
	if !ok {
		t.Fatalf("unknown backend %q", backend)
	}
	state.once.Do(func() {
		ctx := context.Background()

		metaURL, err := backendMetaURL(ctx, backend)
		if err != nil {
			state.err = err
			return
		}

		store, verifier, err := newTestDeps(ctx, metaURL)
		if err != nil {
			state.err = err
			return
		}

		keys, err := generateTestKeys()
		if err != nil {
			state.err = err
			return
		}

		// Read-endpoint dependencies: the ref store shares the lane's DB
		// (both schemas coexist, exactly like production), blobs live in a
		// temp file:// bucket, and the cache dir is a temp dir.
		refStore, err := ref.Open(ctx, metaURL)
		if err != nil {
			state.err = err
			return
		}
		envCleanup = append(envCleanup, refStore.Close)
		if err := refStore.InitSchema(ctx); err != nil {
			state.err = err
			return
		}
		blobDir, err := os.MkdirTemp("", "openstorage-api-blobs-*")
		if err != nil {
			state.err = err
			return
		}
		envCleanup = append(envCleanup, func() { _ = os.RemoveAll(blobDir) })
		blobStore, err := blob.Open(ctx, "file://"+blobDir)
		if err != nil {
			state.err = err
			return
		}
		envCleanup = append(envCleanup, func() { _ = blobStore.Close() })
		if err := blobStore.EnsureBucket(ctx); err != nil {
			state.err = err
			return
		}
		cacheDir, err := os.MkdirTemp("", "openstorage-api-cache-*")
		if err != nil {
			state.err = err
			return
		}
		envCleanup = append(envCleanup, func() { _ = os.RemoveAll(cacheDir) })

		handler, err := New(Config{
			Store:       store,
			Verifier:    verifier,
			Logger:      discardLogger(),
			HTTPBaseURL: testHTTPBaseURL,
			RefStore:    refStore,
			BlobStore:   blobStore,
			CacheDir:    cacheDir,
			SyncQueue:   envSyncQueue{},
		})
		if err != nil {
			state.err = err
			return
		}
		srv := httptest.NewServer(handler)
		envCleanup = append(envCleanup, srv.Close)

		state.env = &testEnv{
			store:     store,
			srv:       srv,
			keys:      keys,
			refStore:  refStore,
			blobStore: blobStore,
			cacheDir:  cacheDir,
		}
	})
	if state.err != nil {
		t.Fatalf("%s test environment: %v", backend, state.err)
	}
	return state.env
}

// runBackends fans an integration test out over every storage backend lane:
// t.Run("pg") (lazy testcontainer) and t.Run("sqlite") (temp-dir DB, zero
// containers). Each lane environment boots on first use.
func runBackends(t *testing.T, fn func(t *testing.T, e *testEnv)) {
	for _, backend := range []string{"pg", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			fn(t, testEnvironment(t, backend))
		})
	}
}

func TestMain(m *testing.M) {
	code := m.Run()
	for _, e := range slices.Backward(envCleanup) {
		e()
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
// defaults (ES256, registered key, auth.SubjectLiteral, exp in one hour).
type tokenSpec struct {
	alg    string // default "ES256"
	iss    string
	sub    string // default auth.SubjectLiteral
	repo   string
	scopes []string
	refs   string         // raw `refs` claim JSON (docs/reference/authentication.md policy tuples); "" omits
	ops    []string       // deprecated repo-wide `ops` claim; nil omits
	expIn  time.Duration  // default 1h; negative mints an expired token
	key    any            // signing key override (default: env key for alg)
	claims map[string]any // set last over the claims above; a nil value omits the claim
	header map[string]any // extra protected header members
}

// critHeader is a `crit` header that lists an extension no verifier here
// understands (RFC 7515 §4.1.11).
var critHeader = map[string]any{"crit": []string{"x-ext"}, "x-ext": true}

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
	claims := map[string]any{
		"iss":    spec.iss,
		"sub":    spec.sub,
		"repo":   spec.repo,
		"scopes": spec.scopes,
		"iat":    now.Unix(),
		"exp":    now.Add(spec.expIn).Unix(),
	}
	if spec.refs != "" {
		claims["refs"] = json.RawMessage(spec.refs)
	}
	if spec.ops != nil {
		claims["ops"] = spec.ops
	}
	for name, v := range spec.claims {
		if v == nil {
			delete(claims, name)
			continue
		}
		claims[name] = v
	}
	payload, err := json.Marshal(claims)
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
	for name, v := range spec.header {
		if err := hdrs.Set(name, v); err != nil {
			t.Fatal(err)
		}
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
	if ct := resp.Header.Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("error Content-Type = %q, want application/problem+json (body %q)", ct, body)
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
	assertProblemFields(t, resp.StatusCode, body)
	// `instance` is the escaped path the client sent, without the query
	// (issue #352): for an alias route, not the v1 path it dispatches to.
	if want := resp.Request.URL.EscapedPath(); envlp.Instance != want {
		t.Fatalf("instance = %q, want the request path %q (body %s)", envlp.Instance, want, body)
	}
	return envlp.Error
}
