package githttp

// End-to-end acceptance tests for the git smart HTTP surface, over two
// backend lanes selected by CODESTORAGE_TEST_BACKEND:
//
//   - "" / "pg" (default): real containers only (no fakes) — testcontainers-go
//     boots Postgres + SeaweedFS, exactly like production would. The nightly
//     Floci lane (floci-nightly-githttp.yml) reuses this lane with
//     CODESTORAGE_TEST_S3_ENDPOINT pointing the blob store at an externally
//     booted auth-enforcing S3 endpoint instead of the managed SeaweedFS.
//   - "local": zero containers — sqlite:// + file:// URLs derived from a
//     temp data dir, the way `serve --data-dir` derives them.
//
// Either way TestMain builds the codestorage binary once and runs nodes as
// separate OS processes, so the hook re-exec path crosses a real process
// boundary on both lanes. All keys (JWT org keys, SSH and PGP signing keys)
// are generated at runtime and never committed.

import (
	"bufio"
	"context"
	"crypto"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/coder/code-storage/internal/auth"
	"github.com/coder/code-storage/internal/blob"
	"github.com/coder/code-storage/internal/jobs"
	"github.com/coder/code-storage/internal/meta"
	"github.com/coder/code-storage/internal/ref"
	"github.com/coder/code-storage/internal/ref/s3wal/s3waltest"
)

// testOrg is the org the serve nodes are configured for; secondOrg is
// registered directly in meta so its (valid) tokens can prove the 403
// cross-org mapping.
const (
	testOrg   = "org1"
	secondOrg = "org2"
)

// Backend lanes for CODESTORAGE_TEST_BACKEND.
const (
	backendPG    = "pg"
	backendLocal = "local"
	// backendS3WAL is the zero-infra lane (issue #133 Phase 3): ONE
	// SeaweedFS bucket carries the s3wal ref+meta authority (_wal/) AND
	// the packs/lfs blobs — the expected production topology.
	backendS3WAL = "s3wal"
)

// Static dummy credentials for the SeaweedFS S3 gateway lanes (the
// container is throwaway and unreachable beyond the mapped test port).
const (
	s3AccessKey = "codestorage-e2e"
	s3SecretKey = "codestorage-e2e-secret"
)

var (
	binPath      string
	testBackend  string // resolved CODESTORAGE_TEST_BACKEND (backendPG or backendLocal)
	localDataDir string // local lane only: the --data-dir-style directory holding codestorage.db + blobs/
	dbURL        string
	blobURL      string
	orgKeysPath  string        // PEM file with testOrg's public keys (ES256 + Ed25519)
	orgES256Key  crypto.Signer // default token-signing key for testOrg
	orgEdKey     crypto.Signer // second registered key (EdDSA)
	org2Key      crypto.Signer // secondOrg's registered key
	testStore    ref.Store
	testBlob     blob.BlobStore
	testMeta     meta.Backend
	nodeA        *testNode
)

func TestMain(m *testing.M) {
	// Hook re-exec dispatch: an in-process httptest server (see
	// TestServiceRPC_ReleasesPushHoldBeforeRepackTrigger) hydrates cache
	// repos whose hook scripts embed THIS test binary's path — route
	// those invocations to the real hook entrypoint instead of the test
	// runner (mirrors cmd/codestorage's "hook" dispatch).
	if len(os.Args) > 1 && os.Args[1] == "hook" {
		if err := RunHook(context.Background(), os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	if fuzzSkipEnvBoot(os.Args[1:]) {
		os.Exit(m.Run())
	}
	os.Exit(testMainInner(m))
}

// fuzzSkipEnvBoot reports whether this process belongs to a `go test -fuzz`
// campaign that cannot reach the ordinary e2e tests, in which case TestMain
// skips the environment boot (issue #91): without it, -parallel=N would run
// N binary builds + backend stacks per campaign, because `go test -fuzz`
// runs TestMain in the coordinator AND in every worker process. True for
// fuzz workers (they only ever execute the fuzz target) and for a
// coordinator whose -run selection is the match-nothing idiom `^$` (the
// documented campaign form). A coordinator with any other -run value still
// runs the matching ordinary tests before fuzzing (`go help testflag`), so
// it keeps the full boot (Codex review). None of this package's fuzz
// targets touch the e2e environment; plain `go test` — including the
// seed-corpus replay of the fuzz targets — never sees these flags.
func fuzzSkipEnvBoot(args []string) bool {
	flagValue := func(name string) (string, bool) {
		for i, arg := range args {
			a := strings.TrimPrefix(strings.TrimPrefix(arg, "-"), "-")
			if a == name {
				// Bare-flag spelling; a non-bool flag's value is the next arg.
				if i+1 < len(args) {
					return args[i+1], true
				}
				return "", true
			}
			if v, ok := strings.CutPrefix(a, name+"="); ok {
				return v, true
			}
		}
		return "", false
	}
	if _, worker := flagValue("test.fuzzworker"); worker {
		return true
	}
	if _, fuzzing := flagValue("test.fuzz"); !fuzzing {
		return false
	}
	runPat, ok := flagValue("test.run")
	return ok && runPat == "^$"
}

func testMainInner(m *testing.M) int {
	ctx := context.Background()

	scratch, err := os.MkdirTemp("", "githttp-accept-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(scratch)

	// Build the binary once.
	binPath = filepath.Join(scratch, "codestorage")
	build := exec.Command("go", "build", "-o", binPath, "./cmd/codestorage")
	build.Dir = "../.."
	if out, err := build.CombinedOutput(); err != nil {
		panic(fmt.Sprintf("go build: %v: %s", err, out))
	}

	// CODESTORAGE_TEST_S3_ENDPOINT (the nightly Floci lane,
	// floci-nightly-githttp.yml) swaps the pg lane's managed SeaweedFS
	// container for an externally booted S3 endpoint. It is honored ONLY
	// on the pg lane; on any other lane it would be silently ignored, and
	// a green run against the wrong store is exactly the failure mode the
	// nightly lane exists to catch — so refuse the combination loudly.
	extS3Endpoint := os.Getenv("CODESTORAGE_TEST_S3_ENDPOINT")
	if extS3Endpoint != "" {
		if v := os.Getenv("CODESTORAGE_TEST_BACKEND"); v != "" && v != backendPG {
			panic(fmt.Sprintf("CODESTORAGE_TEST_S3_ENDPOINT is a pg-lane-only override; unset it or drop CODESTORAGE_TEST_BACKEND=%q", v))
		}
		// Credentials stay in the environment — the workflow mints them via
		// IAM CreateAccessKey → STS AssumeRole — so the serve children and
		// the SDK pick them up the production way (env → LoadDefaultConfig,
		// including AWS_SESSION_TOKEN). Require them before any container
		// boots instead of failing later with opaque signing errors.
		if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
			panic("CODESTORAGE_TEST_S3_ENDPOINT requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in the environment")
		}
	}

	// Backend lane: pg (default) boots real containers; local derives
	// sqlite:// + file:// URLs from a temp data dir (zero containers).
	switch v := os.Getenv("CODESTORAGE_TEST_BACKEND"); v {
	case "", backendPG:
		testBackend = backendPG

		pgC, err := tcpostgres.Run(ctx, "postgres:17-alpine",
			tcpostgres.WithDatabase("postgres"),
			tcpostgres.WithUsername("postgres"),
			tcpostgres.WithPassword("postgres"),
			tcpostgres.BasicWaitStrategies(),
		)
		if err != nil {
			panic("start postgres container: " + err.Error())
		}
		defer func() { _ = pgC.Terminate(ctx) }()
		dbURL, err = pgC.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			panic(err)
		}

		if extS3Endpoint != "" {
			// Nightly Floci lane (issue #203): same pg lane, but the blob
			// store is an external, auth-enforcing S3 endpoint, with the
			// env-guaranteed credentials from the pre-switch guard above.
			// The readiness probe signs with the session token when one is
			// present — SigV4 with temporary keys requires the
			// X-Amz-Security-Token to accompany the signature.
			blobURL = fmt.Sprintf("s3://githttp-test?endpoint=%s&s3ForcePathStyle=true&region=us-east-1", extS3Endpoint)
			raw := s3waltest.RawS3ClientSession(extS3Endpoint, "us-east-1",
				os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), os.Getenv("AWS_SESSION_TOKEN"))
			if err := s3waltest.EnsureBucket(ctx, raw, "githttp-test"); err != nil {
				panic("external S3 endpoint never became write-ready: " + err.Error())
			}
		} else {
			s3Addr, terminate := startSeaweedFS(ctx)
			defer terminate()
			blobURL = fmt.Sprintf("s3://githttp-test?endpoint=http://%s&s3ForcePathStyle=true&region=us-east-1", s3Addr)
			os.Setenv("AWS_ACCESS_KEY_ID", s3AccessKey)
			os.Setenv("AWS_SECRET_ACCESS_KEY", s3SecretKey)

			// SeaweedFS keeps 500ing object PUTs for a while after CreateBucket
			// succeeds — bucket creation is filer-only, object writes need a
			// volume assignment (issues #165/#177) — so gate write readiness
			// now instead of letting the suite's first pack upload draw a
			// boot-window 500 through the SDK's bounded retries. Doubles as
			// the lane's bucket bootstrap (the shared EnsureBucket retry below
			// then succeeds immediately).
			raw := s3waltest.RawS3Client("http://"+s3Addr, "us-east-1", s3AccessKey, s3SecretKey)
			if err := s3waltest.EnsureBucket(ctx, raw, "githttp-test"); err != nil {
				panic("SeaweedFS never became write-ready: " + err.Error())
			}
		}
	case backendLocal:
		testBackend = backendLocal
		localDataDir = filepath.Join(scratch, "data")
		dbURL = "sqlite://" + filepath.Join(localDataDir, "codestorage.db")
		blobURL = "file://" + filepath.Join(localDataDir, "blobs")
	case backendS3WAL:
		testBackend = backendS3WAL

		// Zero-infra topology (issue #133 Phase 3): ONE bucket serves the
		// s3wal ref+meta authority (_wal/ prefix) AND the packs/lfs
		// blobs; the child serve processes get one s3wal:// URL for both
		// stores plus the paired s3:// blob URL.
		s3Addr, terminate := startSeaweedFS(ctx)
		defer terminate()
		const bucket = "githttp-s3wal"
		dbURL = fmt.Sprintf("s3wal://%s?endpoint=http://%s&region=us-east-1", bucket, s3Addr)
		blobURL = fmt.Sprintf("s3://%s?endpoint=http://%s&s3ForcePathStyle=true&region=us-east-1", bucket, s3Addr)
		os.Setenv("AWS_ACCESS_KEY_ID", s3AccessKey)
		os.Setenv("AWS_SECRET_ACCESS_KEY", s3SecretKey)

		// The bucket must exist AND be writable BEFORE testStore.InitSchema
		// below: the s3wal boot probe writes _wal/probe on a no-retry
		// client, and SeaweedFS keeps 500ing object PUTs for a while after
		// CreateBucket succeeds — bucket creation is filer-only, object
		// writes need a volume assignment (issue #165). The shared harness
		// gate retries bucket creation while the gateway chain boots and
		// returns only once a scratch write roundtrip succeeds.
		raw := s3waltest.RawS3Client("http://"+s3Addr, "us-east-1", s3AccessKey, s3SecretKey)
		if err := s3waltest.EnsureBucket(ctx, raw, bucket); err != nil {
			panic("SeaweedFS never became write-ready: " + err.Error())
		}
	default:
		panic(fmt.Sprintf("CODESTORAGE_TEST_BACKEND=%q: unknown backend (want %q, %q, %q, or unset)", v, backendPG, backendLocal, backendS3WAL))
	}

	// Runtime-generated org keys (never committed). testOrg registers two
	// keys with different algorithms to prove multi-alg verification on the
	// git path; secondOrg exists to prove the cross-org 403.
	orgES256Key = mustGenKey("ES256")
	orgEdKey = mustGenKey("EdDSA")
	org2Key = mustGenKey("ES256")
	orgKeysPath = filepath.Join(scratch, "org1-keys.pub.pem")
	if err := os.WriteFile(orgKeysPath, append(mustPubPEM(orgES256Key), mustPubPEM(orgEdKey)...), 0o644); err != nil {
		panic(err)
	}

	// Direct store/blob/meta handles for assertions, opened through the
	// URL-scheme factories so the lane picks the backend; also prepares
	// schema + bucket.
	testStore, err = ref.Open(ctx, dbURL)
	if err != nil {
		panic(err)
	}
	defer testStore.Close()
	if err := testStore.InitSchema(ctx); err != nil {
		panic(err)
	}
	testBlob, err = blob.Open(ctx, blobURL)
	if err != nil {
		panic(err)
	}
	defer testBlob.Close()
	// Doubles as the pg lane's S3 readiness poll: SeaweedFS accepts TCP on
	// the gateway port before the filer chain can serve, so retry the
	// bucket create until it succeeds (the local lane's fileblob mkdir
	// succeeds on the first try).
	for deadline := time.Now().Add(60 * time.Second); ; {
		err := testBlob.EnsureBucket(ctx)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			panic("EnsureBucket never succeeded (blob store not ready?): " + err.Error())
		}
		time.Sleep(250 * time.Millisecond)
	}
	testMeta, err = meta.Open(ctx, dbURL)
	if err != nil {
		panic(err)
	}
	defer testMeta.Close()
	if err := testMeta.InitSchema(ctx); err != nil {
		panic(err)
	}
	if _, err := testMeta.UpsertOrgWithKeys(ctx, secondOrg, []string{string(mustPubPEM(org2Key))}); err != nil {
		panic(err)
	}

	// Shared push node A. Nodes with special needs (fresh cache, delay knob)
	// are started per-test.
	nodeA, err = startNode(filepath.Join(scratch, "nodeA"))
	if err != nil {
		panic(err)
	}
	defer nodeA.stop()

	return m.Run()
}

// startSeaweedFS boots the qualified SeaweedFS recipe (Gate 1a-A, PR
// #142): pinned image, one-process `weed server -s3`, S3 gateway on 8333.
// A static identity (s3.config) turns on SigV4 validation so the LFS
// round-trip's presigned download keeps failing on a bad or missing
// signature, exactly like the MinIO container it replaced — an anonymous
// gateway would serve unsigned requests too. The wait strategy only proves
// the gateway port accepts TCP — weed starts master/volume/filer/s3
// asynchronously — so the callers' EnsureBucket retry is the actual
// readiness signal (the s3wal lane additionally gates on a scratch write
// roundtrip before its no-retry boot probe runs — issue #165). Shared by
// the pg and s3wal lanes.
func startSeaweedFS(ctx context.Context) (s3Addr string, terminate func()) {
	s3Config := fmt.Sprintf(`{"identities":[{"name":"e2e","credentials":[{"accessKey":%q,"secretKey":%q}],"actions":["Admin","Read","List","Tagging","Write"]}]}`,
		s3AccessKey, s3SecretKey)
	seaweedC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		Image:        "chrislusf/seaweedfs:4.42",
		ExposedPorts: []string{"8333/tcp"},
		Cmd:          []string{"server", "-s3", "-dir=/data", "-ip.bind=0.0.0.0", "-s3.config=/etc/seaweedfs/s3.json"},
		Files: []testcontainers.ContainerFile{{
			Reader:            strings.NewReader(s3Config),
			ContainerFilePath: "/etc/seaweedfs/s3.json",
			FileMode:          0o444,
		}},
		WaitingFor: wait.ForListeningPort("8333/tcp").WithStartupTimeout(2 * time.Minute),
		Started:    true,
	})
	if err != nil {
		panic("start seaweedfs container: " + err.Error())
	}
	// PortEndpoint brackets IPv6 hosts (net.JoinHostPort), so the
	// address stays a valid URL host under IPv6 Docker daemons.
	s3Addr, err = seaweedC.PortEndpoint(ctx, "8333/tcp", "")
	if err != nil {
		panic(err)
	}
	return s3Addr, func() { _ = seaweedC.Terminate(ctx) }
}

func mustGenKey(alg string) crypto.Signer {
	key, err := auth.GenerateKey(alg)
	if err != nil {
		panic(err)
	}
	return key
}

func mustPubPEM(key crypto.Signer) []byte {
	pemBytes, err := auth.MarshalPublicKeyPEM(key.Public())
	if err != nil {
		panic(err)
	}
	return pemBytes
}

type testNode struct {
	cmd  *exec.Cmd
	addr string
}

func (n *testNode) stop() {
	if n.cmd.Process != nil {
		_ = n.cmd.Process.Kill()
		_ = n.cmd.Wait()
	}
}

// startNode runs `codestorage serve` in single-org mode. Later duplicate
// env entries win (os/exec dedups), so tests can override the defaults —
// e.g. `CODESTORAGE_TEST_MODE=` to prove knobs are refused in production.
func startNode(cacheDir string, extraEnv ...string) (*testNode, error) {
	cmd := exec.Command(binPath, "serve",
		"--listen", "127.0.0.1:0",
		"--cache-dir", cacheDir,
	)
	cmd.Env = append(os.Environ(),
		"CODESTORAGE_PG_URL="+dbURL,
		"CODESTORAGE_S3_URL="+blobURL,
		"CODESTORAGE_ORG="+testOrg,
		"CODESTORAGE_ORG_PUBKEYS="+orgKeysPath,
		"CODESTORAGE_TEST_MODE=1",
	)
	if testBackend == backendS3WAL {
		// The suite legitimately runs several serve processes against
		// the one lane bucket (nodeA plus per-test nodes), which is
		// exactly what the §2.11 instance lease exists to refuse —
		// disable it by default; the dual-serve drill re-enables it via
		// extraEnv (later duplicate wins).
		cmd.Env = append(cmd.Env, "CODESTORAGE_S3WAL_LEASE_TTL=0")
	}
	cmd.Env = append(cmd.Env, extraEnv...)
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	sc := bufio.NewScanner(stdout)
	lineCh := make(chan string, 1)
	go func() {
		if sc.Scan() {
			lineCh <- sc.Text()
		}
		close(lineCh)
		_, _ = io.Copy(io.Discard, stdout)
	}()
	select {
	case line, ok := <-lineCh:
		if !ok || !strings.HasPrefix(line, "listening on ") {
			_ = cmd.Process.Kill()
			return nil, fmt.Errorf("node did not announce address (got %q)", line)
		}
		return &testNode{cmd: cmd, addr: strings.TrimPrefix(line, "listening on ")}, nil
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Kill()
		return nil, fmt.Errorf("node startup timed out")
	}
}

func startNodeT(t *testing.T, extraEnv ...string) *testNode {
	t.Helper()
	n, err := startNode(t.TempDir(), extraEnv...)
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(n.stop)
	return n
}

// --- git helpers -----------------------------------------------------------

var gitIdentity = []string{
	"-c", "user.email=githttp@test.invalid",
	"-c", "user.name=githttp",
	"-c", "init.defaultBranch=main",
	"-c", "advice.detachedHead=false",
}

func tryGit(dir string, args ...string) (string, error) {
	return tryGitEnv(dir, nil, args...)
}

// tryGitEnv runs git with extra environment entries (e.g. GNUPGHOME for
// OpenPGP signing).
func tryGitEnv(dir string, env []string, args ...string) (string, error) {
	full := append(append([]string{}, gitIdentity...), args...)
	cmd := exec.Command("git", full...)
	cmd.Dir = dir
	if len(env) > 0 {
		cmd.Env = append(os.Environ(), env...)
	}
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func mustGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	out, err := tryGit(dir, args...)
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

func mustGitEnv(t *testing.T, dir string, env []string, args ...string) string {
	t.Helper()
	out, err := tryGitEnv(dir, env, args...)
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

func revParse(t *testing.T, dir, rev string) string {
	t.Helper()
	return strings.TrimSpace(mustGit(t, dir, "rev-parse", rev))
}

// initWorkRepo creates a client repo with one commit on main.
func initWorkRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	mustGit(t, dir, "init", "-b", "main", ".")
	writeFile(t, dir, "a.txt", "one\n")
	mustGit(t, dir, "add", ".")
	mustGit(t, dir, "commit", "-m", "one")
	return dir
}

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// commitFile stages a new file and commits it; extra global git -c flags
// (e.g. SSH/PGP signing config) come from gitFlags.
func commitFile(t *testing.T, dir, name, content, msg string, env []string, gitFlags ...string) string {
	t.Helper()
	writeFile(t, dir, name, content)
	mustGit(t, dir, "add", ".")
	args := append(append([]string{}, gitFlags...), "commit", "-m", msg)
	mustGitEnv(t, dir, env, args...)
	return revParse(t, dir, "HEAD")
}

// --- token helpers ---------------------------------------------------------

// testClaims returns a fully populated docs/reference/authentication.md claim set for testOrg.
func testClaims(repo, scopes string) auth.Claims {
	now := time.Now()
	return auth.Claims{
		Iss:    testOrg,
		Sub:    auth.SubjectLiteral,
		Repo:   repo,
		Scopes: strings.Split(scopes, ","),
		Iat:    now.Unix(),
		Exp:    now.Add(time.Hour).Unix(),
	}
}

func mintWith(t *testing.T, key crypto.Signer, alg string, c auth.Claims) string {
	t.Helper()
	token, err := auth.MintToken(key, alg, c)
	if err != nil {
		t.Fatalf("mint token: %v", err)
	}
	return token
}

// mintTestToken mints a testOrg ES256 token; refsJSON is the raw docs/reference/authentication.md
// tuple encoding ("" for no ref policies).
func mintTestToken(t *testing.T, repo, scopes, refsJSON string) string {
	t.Helper()
	c := testClaims(repo, scopes)
	if refsJSON != "" {
		c.Refs = json.RawMessage(refsJSON)
	}
	return mintWith(t, orgES256Key, "ES256", c)
}

func remoteURL(n *testNode, repo, token string) string {
	return fmt.Sprintf("http://t:%s@%s/%s.git", token, n.addr, repo)
}

// registerRepo creates the repo record in meta and returns its RepoID — the
// canonical storage key (ADR-21: unregistered path elements are 404 on the
// git surface, and pg/blob state is keyed by RepoID only). Tests push via
// the returned RepoID so their storage assertions read the same key;
// name-style URL resolution is pinned by TestRepoKeySplit_CrossSurface.
func registerRepo(t *testing.T, url string) string {
	t.Helper()
	repo, err := testMeta.CreateRepo(context.Background(), testOrg, url, "main")
	if err != nil {
		t.Fatalf("register repo %q: %v", url, err)
	}
	return repo.RepoID
}

// apiGet hits the node's HTTP API with a Bearer token, decodes a 200 JSON
// body into out (unless nil), and returns the status code (cross-surface
// assertions against the read API).
func apiGet(t *testing.T, n *testNode, token, path string, out any) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://"+n.addr+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if out != nil && resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(body, out); err != nil {
			t.Fatalf("decode %s: %v\n%s", path, err, body)
		}
	}
	return resp.StatusCode
}

func pgRefs(t *testing.T, repo string) map[string]string {
	t.Helper()
	refs, err := testStore.ListRefs(context.Background(), repo)
	if err != nil {
		t.Fatalf("list refs: %v", err)
	}
	return refs
}

func pgPacks(t *testing.T, repo string) []ref.Pack {
	t.Helper()
	packs, err := testStore.ListPacks(context.Background(), repo)
	if err != nil {
		t.Fatalf("list packs: %v", err)
	}
	return packs
}

func s3Packs(t *testing.T, repo string) []blob.ListedObject {
	t.Helper()
	objs, err := testBlob.List(context.Background(), blob.PackPrefix(repo))
	if err != nil {
		t.Fatalf("list blob-store objects: %v", err)
	}
	return objs
}

// --- acceptance criterion 1 ------------------------------------------------

func TestPush_RefsInPG_PacksInS3(t *testing.T) {
	repo := registerRepo(t, "t1-push")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")

	out := mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")
	t.Logf("push output:\n%s", out)
	head := revParse(t, work, "HEAD")

	refs := pgRefs(t, repo)
	if got := refs["refs/heads/main"]; got != head {
		t.Fatalf("PG refs/heads/main = %q, want client HEAD %q (refs: %v)", got, head, refs)
	}
	if objs := s3Packs(t, repo); len(objs) < 1 {
		t.Fatalf("expected >=1 pack object in the blob store under %s, got %v", blob.PackPrefix(repo), objs)
	}
	if packs := pgPacks(t, repo); len(packs) < 1 {
		t.Fatalf("expected >=1 pack row in PG, got %v", packs)
	}
}

// --- acceptance criterion 2 ------------------------------------------------

func TestCloneFromNodeB_FreshCache(t *testing.T) {
	repo := registerRepo(t, "t2-clone")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")

	// Two pushes => two independent packs; rehydration must index both and
	// fsck must prove per-pack self-containment (--fix-thin assumption).
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")
	writeFile(t, work, "b.txt", "two\n")
	mustGit(t, work, "add", ".")
	mustGit(t, work, "commit", "-m", "two")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")
	head := revParse(t, work, "HEAD")

	nodeB := startNodeT(t) // empty cache dir: stateless-node proof
	cloneDir := filepath.Join(t.TempDir(), "clone")
	mustGit(t, "", "clone", remoteURL(nodeB, repo, token), cloneDir)

	if got := revParse(t, cloneDir, "HEAD"); got != head {
		t.Fatalf("clone HEAD = %q, want %q", got, head)
	}
	if out, err := tryGit(cloneDir, "fsck", "--strict"); err != nil {
		t.Fatalf("git fsck: %v\n%s", err, out)
	}
}

// --- acceptance criterion 3 ------------------------------------------------

func TestConcurrentPush_OneWinsCAS(t *testing.T) {
	repo := registerRepo(t, "t3-race")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")

	// Dedicated node with the delay knob so both update hooks overlap and the
	// loser is deterministically rejected at the PG CAS.
	raceNode := startNodeT(t, EnvHookDelay+"=1500")

	clones := make([]string, 2)
	heads := make([]string, 2)
	for i := range clones {
		dir := filepath.Join(t.TempDir(), fmt.Sprintf("clone%d", i))
		mustGit(t, "", "clone", remoteURL(raceNode, repo, token), dir)
		writeFile(t, dir, fmt.Sprintf("racer%d.txt", i), fmt.Sprintf("racer %d\n", i))
		mustGit(t, dir, "add", ".")
		mustGit(t, dir, "commit", "-m", fmt.Sprintf("racer %d", i))
		clones[i] = dir
		heads[i] = revParse(t, dir, "HEAD")
	}
	if heads[0] == heads[1] {
		t.Fatal("racer commits must differ")
	}

	outs := make([]string, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range clones {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			outs[i], errs[i] = tryGit(clones[i], "push", remoteURL(raceNode, repo, token), "HEAD:refs/heads/main")
		}(i)
	}
	wg.Wait()

	winners := 0
	winnerIdx := -1
	for i := range errs {
		t.Logf("push %d (err=%v):\n%s", i, errs[i], outs[i])
		if errs[i] == nil {
			winners++
			winnerIdx = i
		}
	}
	if winners != 1 {
		t.Fatalf("expected exactly one winning push, got %d", winners)
	}
	loserIdx := 1 - winnerIdx
	rejected := regexp.MustCompile(`\[(remote )?rejected\]`)
	if !rejected.MatchString(outs[loserIdx]) {
		t.Fatalf("loser output does not show a git-native rejection:\n%s", outs[loserIdx])
	}
	if got := pgRefs(t, repo)["refs/heads/main"]; got != heads[winnerIdx] {
		t.Fatalf("PG main = %q, want winner %q", got, heads[winnerIdx])
	}
}

// --- acceptance criterion 4 ------------------------------------------------

func TestCrash_AfterS3_BeforePGCommit(t *testing.T) {
	repo := registerRepo(t, "t4-crash")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")
	oldHead := revParse(t, work, "HEAD")

	refsBefore := pgRefs(t, repo)
	packsBefore := pgPacks(t, repo)
	objsBefore := s3Packs(t, repo)

	writeFile(t, work, "b.txt", "two\n")
	mustGit(t, work, "add", ".")
	mustGit(t, work, "commit", "-m", "two")
	newHead := revParse(t, work, "HEAD")

	// Crash the pre-receive hook between S3 upload and PG commit.
	out, err := tryGit(work,
		"-c", "http.extraHeader="+CrashHeader+": after-s3",
		"push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("push should have failed under crash knob:\n%s", out)
	}
	t.Logf("crashed push output:\n%s", out)

	// Orphaned S3 pack only; PG refs + packs untouched.
	objsAfter := s3Packs(t, repo)
	if len(objsAfter) != len(objsBefore)+1 {
		t.Fatalf("blob-store objects = %d, want %d (exactly one new orphan)", len(objsAfter), len(objsBefore)+1)
	}
	if got := pgRefs(t, repo); !mapsEqual(got, refsBefore) {
		t.Fatalf("PG refs changed across crash: %v -> %v", refsBefore, got)
	}
	if got := pgPacks(t, repo); len(got) != len(packsBefore) {
		t.Fatalf("PG packs changed across crash: %v -> %v", packsBefore, got)
	}

	// The orphan-pack sweeper (issue #5) sees exactly this crash artifact.
	// Object stores cannot be backdated, so age the orphan for real past a
	// short threshold instead of injecting a clock.
	orphanKey := ""
	before := make(map[string]bool, len(objsBefore))
	for _, o := range objsBefore {
		before[o.Key] = true
	}
	for _, o := range objsAfter {
		if !before[o.Key] {
			orphanKey = o.Key
		}
	}
	if orphanKey == "" {
		t.Fatal("could not identify the orphan key")
	}
	sweepLogger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	threshold := time.Second
	time.Sleep(threshold + 500*time.Millisecond)

	// List-only (the default): the orphan is reported, nothing is deleted.
	report, err := jobs.NewSweeper(testBlob, testStore, jobs.SweepConfig{Threshold: threshold}, sweepLogger).Sweep(t.Context())
	if err != nil {
		t.Fatalf("list-only sweep: %v", err)
	}
	foundOrphan := false
	for _, o := range report.Orphans {
		if o.Key == orphanKey {
			foundOrphan = true
		}
	}
	if !foundOrphan {
		t.Fatalf("list-only sweep did not report the crash orphan %q: %+v", orphanKey, report.Orphans)
	}
	if got := s3Packs(t, repo); len(got) != len(objsAfter) {
		t.Fatalf("list-only sweep deleted objects: %d -> %d", len(objsAfter), len(got))
	}

	// Delete enabled: the orphan is reaped; every registered pack survives.
	if _, err := jobs.NewSweeper(testBlob, testStore, jobs.SweepConfig{Threshold: threshold, Delete: true}, sweepLogger).Sweep(t.Context()); err != nil {
		t.Fatalf("delete sweep: %v", err)
	}
	objsSwept := s3Packs(t, repo)
	if len(objsSwept) != len(objsBefore) {
		t.Fatalf("objects after delete sweep = %d, want %d (orphan reaped)", len(objsSwept), len(objsBefore))
	}
	for _, o := range objsSwept {
		if o.Key == orphanKey {
			t.Fatalf("orphan %q survived the delete sweep", orphanKey)
		}
	}
	for _, p := range pgPacks(t, repo) {
		if _, err := testBlob.Stat(t.Context(), p.Key); err != nil {
			t.Fatalf("registered pack %q lost after sweep: %v", p.Key, err)
		}
	}

	// A fresh node still serves a healthy clone at the old HEAD.
	nodeB := startNodeT(t)
	cloneDir := filepath.Join(t.TempDir(), "clone")
	mustGit(t, "", "clone", remoteURL(nodeB, repo, token), cloneDir)
	if got := revParse(t, cloneDir, "HEAD"); got != oldHead {
		t.Fatalf("clone HEAD after crash = %q, want old HEAD %q", got, oldHead)
	}
	if out, err := tryGit(cloneDir, "fsck", "--strict"); err != nil {
		t.Fatalf("git fsck after crash: %v\n%s", err, out)
	}

	// Retry without the knob succeeds and lands the new head.
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")
	if got := pgRefs(t, repo)["refs/heads/main"]; got != newHead {
		t.Fatalf("PG main after retry = %q, want %q", got, newHead)
	}
}

// The crash knob is a test-only fault injection: a node NOT running in test
// mode must refuse the header outright (403) without touching PG or S3.
func TestCrashHeader_RefusedOutsideTestMode(t *testing.T) {
	repo := registerRepo(t, "t4b-crash-refused")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	prodNode := startNodeT(t, "CODESTORAGE_TEST_MODE=") // later duplicate wins
	mustGit(t, work, "push", remoteURL(prodNode, repo, token), "HEAD:refs/heads/main")

	objsBefore := s3Packs(t, repo)
	refsBefore := pgRefs(t, repo)
	writeFile(t, work, "b.txt", "two\n")
	mustGit(t, work, "add", ".")
	mustGit(t, work, "commit", "-m", "two")

	out, err := tryGit(work,
		"-c", "http.extraHeader="+CrashHeader+": after-s3",
		"push", remoteURL(prodNode, repo, token), "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("push with crash header should be refused outside test mode:\n%s", out)
	}
	if !strings.Contains(out, "403") {
		t.Fatalf("expected a 403 refusal, got:\n%s", out)
	}
	if got := s3Packs(t, repo); len(got) != len(objsBefore) {
		t.Fatalf("refused push still wrote to S3: %d -> %d objects", len(objsBefore), len(got))
	}
	if got := pgRefs(t, repo); !mapsEqual(got, refsBefore) {
		t.Fatalf("refused push changed PG refs: %v -> %v", refsBefore, got)
	}
}

// --- docs/reference/authentication.md auth matrix (e2e) ----------------------------------------------

// TestAuth_Matrix drives the failure mapping end-to-end over real HTTP:
// 401 for anything the Verifier rejects, 403 for valid-but-wrong tokens.
func TestAuth_Matrix(t *testing.T) {
	repo := registerRepo(t, "t-auth-matrix")
	work := initWorkRepo(t)
	setup := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, setup), "HEAD:refs/heads/main")

	infoRefs := func(user, token, service string) int {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet,
			"http://"+nodeA.addr+"/"+repo+".git/info/refs?service=git-"+service, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.SetBasicAuth(user, token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.StatusCode
	}

	expired := testClaims(repo, "git:read")
	expired.Exp = time.Now().Add(-time.Minute).Unix()
	unknownIss := testClaims(repo, "git:read")
	unknownIss.Iss = "ghost-org"
	org2Claims := testClaims(repo, "git:read")
	org2Claims.Iss = secondOrg

	cases := []struct {
		name    string
		user    string
		token   string
		service string
		want    int
	}{
		{"ES256 org key read", "t", mintWith(t, orgES256Key, "ES256", testClaims(repo, "git:read")), "upload-pack", 200},
		{"EdDSA org key read", "t", mintWith(t, orgEdKey, "EdDSA", testClaims(repo, "git:read")), "upload-pack", 200},
		{"expired token", "t", mintWith(t, orgES256Key, "ES256", expired), "upload-pack", 401},
		{"unknown issuer", "t", mintWith(t, orgES256Key, "ES256", unknownIss), "upload-pack", 401},
		{"second org's valid token", "t", mintWith(t, org2Key, "ES256", org2Claims), "upload-pack", 403},
		{"wrong repo claim", "t", mintTestToken(t, "some-other-repo", "git:read", ""), "upload-pack", 403},
		{"read scope on receive-pack", "t", mintWith(t, orgES256Key, "ES256", testClaims(repo, "git:read")), "receive-pack", 403},
		{"bad basic user", "x", setup, "upload-pack", 401},
		{"garbage token", "t", "eyJhbGciOiJub25lIn0.e30.", "upload-pack", 401},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := infoRefs(c.user, c.token, c.service); got != c.want {
				t.Fatalf("info/refs status = %d, want %d", got, c.want)
			}
		})
	}

	// A read-only token must not be able to push (whole-push 403).
	writeFile(t, work, "b.txt", "two\n")
	mustGit(t, work, "add", ".")
	mustGit(t, work, "commit", "-m", "two")
	readOnly := mintTestToken(t, repo, "git:read", "")
	out, err := tryGit(work, "push", remoteURL(nodeA, repo, readOnly), "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("push with git:read-only token should fail:\n%s", out)
	}
	if !strings.Contains(out, "403") {
		t.Fatalf("read-only push should be a 403, got:\n%s", out)
	}
}

// --- unit: path parsing ------------------------------------------------------

func TestSplitRepoPath_Namespaces(t *testing.T) {
	cases := []struct {
		path, repo, rest string
		ok               bool
	}{
		{"/r1.git/info/refs", "r1", "info/refs", true},
		{"/r1+ephemeral.git/info/refs", "r1+ephemeral", "info/refs", true},
		{"/r1+import.git/git-receive-pack", "r1+import", "git-receive-pack", true},
		{"/r1+bogus.git/info/refs", "", "", false},
		{"/+ephemeral.git/info/refs", "", "", false},
		{"/r1+ephemeral+import.git/info/refs", "", "", false},
	}
	for _, c := range cases {
		repo, rest, ok := splitRepoPath(c.path)
		if repo != c.repo || rest != c.rest || ok != c.ok {
			t.Errorf("splitRepoPath(%q) = (%q, %q, %v), want (%q, %q, %v)",
				c.path, repo, rest, ok, c.repo, c.rest, c.ok)
		}
	}
}

// --- push-path regressions ---------------------------------------------------

// A push that transfers no new objects (branch created from an already-pushed
// commit) must succeed: depending on git version/client it carries an empty
// pack or no pack at all, and neither may be rejected by pre-receive.
func TestBranchCreate_NoNewObjects(t *testing.T) {
	repo := registerRepo(t, "t6-branch-create")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")
	head := revParse(t, work, "HEAD")

	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/copy")
	if got := pgRefs(t, repo)["refs/heads/copy"]; got != head {
		t.Fatalf("PG refs/heads/copy = %q, want %q", got, head)
	}

	// A fresh node must rehydrate cleanly (an empty pack, if registered,
	// must not break index-pack) and serve both branches.
	nodeB := startNodeT(t)
	cloneDir := filepath.Join(t.TempDir(), "clone")
	mustGit(t, "", "clone", remoteURL(nodeB, repo, token), cloneDir)
	if got := revParse(t, cloneDir, "origin/copy"); got != head {
		t.Fatalf("clone origin/copy = %q, want %q", got, head)
	}
}

func TestRefPolicy_NoPushWildcard(t *testing.T) {
	repo := registerRepo(t, "t7-no-push")
	work := initWorkRepo(t)
	setupToken := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, setupToken), "HEAD:refs/heads/main")
	mainHead := revParse(t, work, "HEAD")

	writeFile(t, work, "b.txt", "two\n")
	mustGit(t, work, "add", ".")
	mustGit(t, work, "commit", "-m", "two")

	// Standalone `*` catch-all combined with the no-push op.
	noPushToken := mintTestToken(t, repo, "git:read,git:write", `[["*",["no-push"]]]`)
	out, err := tryGit(work, "push", remoteURL(nodeA, repo, noPushToken), "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("push should be rejected by no-push policy:\n%s", out)
	}
	t.Logf("no-push output:\n%s", out)
	if !regexp.MustCompile(`\[remote rejected\].*(no-push|hook declined)`).MatchString(out) {
		t.Fatalf("push was not remote-rejected with a no-push message:\n%s", out)
	}
	if got := pgRefs(t, repo)["refs/heads/main"]; got != mainHead {
		t.Fatalf("PG main = %q, want unchanged %q", got, mainHead)
	}
}

// docs/explanation/git-protocol-and-namespaces.md: `+ephemeral` remotes see/update only the ephemeral ref space and
// persistent remotes must not see ephemeral refs (separate ref stores).
// Extended with promotion prep: fetching from `+ephemeral` and pushing the
// same objects to a persistent branch is the client-side promotion flow.
func TestNamespace_EphemeralIsolated(t *testing.T) {
	repo := registerRepo(t, "t9-ns")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "") // minted for the base repo id
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")

	writeFile(t, work, "eph.txt", "scratch\n")
	mustGit(t, work, "add", ".")
	mustGit(t, work, "commit", "-m", "scratch")
	ephHead := revParse(t, work, "HEAD")
	mustGit(t, work, "push", remoteURL(nodeA, repo+"+ephemeral", token), "HEAD:refs/heads/scratch")

	if got := pgRefs(t, repo+"+ephemeral")["refs/heads/scratch"]; got != ephHead {
		t.Fatalf("PG ephemeral scratch = %q, want %q", got, ephHead)
	}
	if refs := pgRefs(t, repo); refs["refs/heads/scratch"] != "" {
		t.Fatalf("persistent ref store leaked ephemeral ref: %v", refs)
	}
	if refs := pgRefs(t, repo+"+ephemeral"); refs["refs/heads/main"] != "" {
		t.Fatalf("ephemeral ref store leaked persistent ref: %v", refs)
	}

	// The smart-HTTP advertisement shows the same isolation.
	if out := mustGit(t, "", "ls-remote", remoteURL(nodeA, repo, token)); strings.Contains(out, "scratch") {
		t.Fatalf("persistent advertisement leaked ephemeral ref:\n%s", out)
	}
	out := mustGit(t, "", "ls-remote", remoteURL(nodeA, repo+"+ephemeral", token))
	if !strings.Contains(out, "refs/heads/scratch") || strings.Contains(out, "refs/heads/main") {
		t.Fatalf("ephemeral advertisement wrong:\n%s", out)
	}

	// Promotion prep, from a second replica with a cold cache: fetch the
	// scratch branch back out of `+ephemeral`, push those same objects to a
	// persistent branch, and prove the persistent remote gains ONLY the
	// promoted ref while `+ephemeral` keeps isolating.
	nodeB := startNodeT(t)
	promoDir := filepath.Join(t.TempDir(), "promo")
	mustGit(t, "", "clone", remoteURL(nodeB, repo, token), promoDir)
	mustGit(t, promoDir, "fetch", remoteURL(nodeB, repo+"+ephemeral", token), "refs/heads/scratch")
	if got := revParse(t, promoDir, "FETCH_HEAD"); got != ephHead {
		t.Fatalf("fetched ephemeral scratch = %q, want %q", got, ephHead)
	}
	mustGit(t, promoDir, "push", remoteURL(nodeB, repo, token), "FETCH_HEAD:refs/heads/promoted")

	persistent := mustGit(t, "", "ls-remote", remoteURL(nodeA, repo, token))
	if !strings.Contains(persistent, ephHead+"\trefs/heads/promoted") {
		t.Fatalf("persistent remote missing promoted branch:\n%s", persistent)
	}
	if strings.Contains(persistent, "refs/heads/scratch") {
		t.Fatalf("persistent remote leaked the ephemeral scratch ref:\n%s", persistent)
	}
	ephOut := mustGit(t, "", "ls-remote", remoteURL(nodeA, repo+"+ephemeral", token))
	if !strings.Contains(ephOut, "refs/heads/scratch") || strings.Contains(ephOut, "refs/heads/promoted") {
		t.Fatalf("ephemeral namespace lost isolation after promotion:\n%s", ephOut)
	}
}

// docs/explanation/git-protocol-and-namespaces.md `+import` namespace: bulk history round-trips through its own
// isolated ref space (clone/push/fetch), invisible from the persistent
// remote.
func TestNamespace_ImportRoundTrip(t *testing.T) {
	repo := registerRepo(t, "t10-import")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")

	mustGit(t, work, "tag", "v1")
	mustGit(t, work, "push", remoteURL(nodeA, repo+"+import", token), "HEAD:refs/heads/main", "refs/tags/v1")
	head := revParse(t, work, "HEAD")

	importOut := mustGit(t, "", "ls-remote", remoteURL(nodeA, repo+"+import", token))
	if !strings.Contains(importOut, "refs/heads/main") || !strings.Contains(importOut, "refs/tags/v1") {
		t.Fatalf("import advertisement missing pushed refs:\n%s", importOut)
	}
	if persistent := mustGit(t, "", "ls-remote", remoteURL(nodeA, repo, token)); strings.Contains(persistent, "refs/heads/main") {
		t.Fatalf("persistent remote sees import refs:\n%s", persistent)
	}

	// Clone the import namespace (fresh replica, cold cache), verify
	// integrity, extend it, and fetch the extension back in the original
	// working repo.
	nodeB := startNodeT(t)
	cloneDir := filepath.Join(t.TempDir(), "clone")
	mustGit(t, "", "clone", remoteURL(nodeB, repo+"+import", token), cloneDir)
	if got := revParse(t, cloneDir, "HEAD"); got != head {
		t.Fatalf("import clone HEAD = %q, want %q", got, head)
	}
	if out, err := tryGit(cloneDir, "fsck", "--strict"); err != nil {
		t.Fatalf("git fsck: %v\n%s", err, out)
	}
	writeFile(t, cloneDir, "bulk.txt", "imported\n")
	mustGit(t, cloneDir, "add", ".")
	mustGit(t, cloneDir, "commit", "-m", "bulk")
	newHead := revParse(t, cloneDir, "HEAD")
	mustGit(t, cloneDir, "push", remoteURL(nodeB, repo+"+import", token), "HEAD:refs/heads/main")

	mustGit(t, work, "fetch", remoteURL(nodeA, repo+"+import", token), "refs/heads/main")
	if got := revParse(t, work, "FETCH_HEAD"); got != newHead {
		t.Fatalf("fetched import main = %q, want %q", got, newHead)
	}
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// --- acceptance criterion 5 ------------------------------------------------

func TestForcePush_RejectedPerRef(t *testing.T) {
	repo := registerRepo(t, "t5-policy")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")
	mainHead := revParse(t, work, "HEAD")

	// Rewrite history: non-fast-forward relative to the pushed main.
	mustGit(t, work, "commit", "--amend", "--allow-empty", "-m", "rewrite")
	forcedHead := revParse(t, work, "HEAD")
	if forcedHead == mainHead {
		t.Fatal("amend must change the commit")
	}

	nfToken := mintTestToken(t, repo, "git:read,git:write",
		`[["refs/heads/main",["no-force-push"]],["*",[]]]`)

	// ONE push carrying a forced main update and a new branch: per-ref
	// enforcement must reject main and accept the branch.
	out, err := tryGit(work, "push", remoteURL(nodeA, repo, nfToken),
		"+HEAD:refs/heads/main", "HEAD:refs/heads/feature")
	if err == nil {
		t.Fatalf("push should exit non-zero when a ref is rejected:\n%s", out)
	}
	t.Logf("policy push output:\n%s", out)

	if !regexp.MustCompile(`\[remote rejected\].*(no-force-push|hook declined)`).MatchString(out) {
		t.Fatalf("main was not remote-rejected with a policy message:\n%s", out)
	}
	if !strings.Contains(out, "[new branch]") {
		t.Fatalf("feature branch was not accepted:\n%s", out)
	}

	refs := pgRefs(t, repo)
	if got := refs["refs/heads/main"]; got != mainHead {
		t.Fatalf("PG main = %q, want unchanged %q", got, mainHead)
	}
	if got := refs["refs/heads/feature"]; got != forcedHead {
		t.Fatalf("PG feature = %q, want %q", got, forcedHead)
	}
}

// TestRefPolicy_NoForcePushDeniesDeletion: no-force-push must also reject
// branch DELETION — ref policies are the only branch-protection mechanism
// (docs/reference/authentication.md), so allowing deletion would make the policy trivially
// bypassable (delete + re-push = force push). Unpoliced refs stay deletable.
// The policed ref is deliberately NOT the repo HEAD: deleting HEAD is
// rejected by git's own receive.denyDeleteCurrent before hooks run.
func TestRefPolicy_NoForcePushDeniesDeletion(t *testing.T) {
	repo := registerRepo(t, "t8-delete-policy")
	work := initWorkRepo(t)
	setupToken := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, setupToken),
		"HEAD:refs/heads/main", "HEAD:refs/heads/protected", "HEAD:refs/heads/scratch")
	head := revParse(t, work, "HEAD")

	nfToken := mintTestToken(t, repo, "git:read,git:write",
		`[["refs/heads/protected",["no-force-push"]],["*",[]]]`)

	// Deleting the policed branch must be rejected by the policy hook.
	out, err := tryGit(work, "push", remoteURL(nodeA, repo, nfToken), ":refs/heads/protected")
	if err == nil {
		t.Fatalf("deletion of policed branch should be rejected:\n%s", out)
	}
	if !regexp.MustCompile(`\[remote rejected\].*(no-force-push|hook declined)`).MatchString(out) {
		t.Fatalf("protected deletion was not remote-rejected with a policy message:\n%s", out)
	}

	// Deleting an unpoliced branch with the SAME token must succeed.
	mustGit(t, work, "push", remoteURL(nodeA, repo, nfToken), ":refs/heads/scratch")

	refs := pgRefs(t, repo)
	if got := refs["refs/heads/protected"]; got != head {
		t.Fatalf("PG protected = %q, want unchanged %q", got, head)
	}
	if _, exists := refs["refs/heads/scratch"]; exists {
		t.Fatal("PG still holds refs/heads/scratch after allowed deletion")
	}
}

// --- local mode (zero containers) --------------------------------------------

// TestLocalMode_PacksOnDiskAndStatelessCache proves the local lane's on-disk
// contract: pushed packs are plain files under <data-dir>/blobs/packs/<repo>/,
// the ref/meta authority is one <data-dir>/codestorage.db file, and the cache
// dir stays a rebuildable cache — wiping it and restarting the node loses
// nothing. Skips on the pg lane, where durable state lives in containers.
func TestLocalMode_PacksOnDiskAndStatelessCache(t *testing.T) {
	if testBackend != backendLocal {
		t.Skipf("local-mode-only test (running the %s lane; set CODESTORAGE_TEST_BACKEND=local)", testBackend)
	}

	repo := registerRepo(t, "t-local-mode")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")

	cacheDir := filepath.Join(t.TempDir(), "cache")
	node, err := startNode(cacheDir)
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	defer node.stop()
	mustGit(t, work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/main")
	head := revParse(t, work, "HEAD")

	// Pack bytes are plain files under the derived blob dir.
	pattern := filepath.Join(localDataDir, "blobs", "packs", repo, "*.pack")
	packFiles, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatal(err)
	}
	if len(packFiles) < 1 {
		t.Fatalf("expected >=1 pack file matching %s", pattern)
	}
	// The ref/meta authority is one SQLite file in the data dir.
	if _, err := os.Stat(filepath.Join(localDataDir, "codestorage.db")); err != nil {
		t.Fatalf("codestorage.db missing from the data dir: %v", err)
	}
	// The ref flip landed in that file (factory-opened store handle).
	if got := pgRefs(t, repo)["refs/heads/main"]; got != head {
		t.Fatalf("sqlite refs/heads/main = %q, want client HEAD %q", got, head)
	}

	// Stateless cache: wipe it, restart the node, clone — identical history.
	node.stop()
	if err := os.RemoveAll(cacheDir); err != nil {
		t.Fatal(err)
	}
	restarted, err := startNode(cacheDir)
	if err != nil {
		t.Fatalf("restart node after cache wipe: %v", err)
	}
	defer restarted.stop()
	cloneDir := filepath.Join(t.TempDir(), "clone")
	mustGit(t, "", "clone", remoteURL(restarted, repo, token), cloneDir)
	if got := revParse(t, cloneDir, "HEAD"); got != head {
		t.Fatalf("clone HEAD after cache wipe = %q, want %q", got, head)
	}
	if out, err := tryGit(cloneDir, "fsck", "--strict"); err != nil {
		t.Fatalf("git fsck: %v\n%s", err, out)
	}
}

// --- issue #1: insert-triggered background repack ----------------------------

// TestRepack_PushesFoldToOnePack drives the full trigger→job→fold pipeline
// through the real binary on both backend lanes: pushes past the threshold
// make the background repack consolidate the registered rows down to ONE
// pack, superseded blobs linger for the sweeper, and a cold node clones the
// byte-identical repo from the consolidated pack alone.
//
// Threshold 2 — the ONLY threshold where "exactly one row" is a guaranteed
// eventual state (issue #34). The system invariant is convergence BELOW the
// threshold, not to one row: at threshold 3, a fold that snapshots pushes
// 1–4 with push 5 registering after the swap legitimately settles at 2 rows
// forever (2 < 3: the trigger correctly declines, the follow-up gate is not
// due). At threshold 2, every registration leaves the count at ≥ 2, so the
// last push always fires a trigger (or lands inside an active job whose
// gated successor covers it) and the chain terminates only at 1 row.
// TestLocalQueue_TriggerMidFoldConvergesToOne pins the mid-fold interleave
// of that argument deterministically.
func TestRepack_PushesFoldToOnePack(t *testing.T) {
	node := startNodeT(t, "CODESTORAGE_REPACK_THRESHOLD=2")
	repo := registerRepo(t, "t-repack")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")

	mustGit(t, work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/main")
	for i := range 4 {
		writeFile(t, work, "f.txt", fmt.Sprintf("rev %d\n", i))
		mustGit(t, work, "add", ".")
		mustGit(t, work, "commit", "-m", fmt.Sprintf("rev %d", i))
		mustGit(t, work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/main")
	}
	head := revParse(t, work, "HEAD")

	// Event-driven convergence wait. Generous budget: the river lane chains
	// gated successors scheduled repackFollowUpDelay (5s) in the future,
	// each surfaced by river's scheduler/fetch ticks, and an unlucky
	// interleave needs several links — under -race on a loaded shared
	// runner the old 60s budget expired mid-chain (issue #34).
	deadline := time.Now().Add(3 * time.Minute)
	for len(pgPacks(t, repo)) != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("pack rows never folded to 1: %+v", pgPacks(t, repo))
		}
		time.Sleep(100 * time.Millisecond)
	}
	packs := pgPacks(t, repo)
	objs := s3Packs(t, repo)
	if len(objs) <= 1 {
		t.Fatalf("superseded pack blobs were deleted synchronously: %v", objs)
	}
	foundFold := false
	for _, o := range objs {
		if o.Key == packs[0].Key {
			foundFold = true
		}
	}
	if !foundFold {
		t.Fatalf("consolidated pack row %s has no blob among %v", packs[0].Key, objs)
	}

	// Cold node: clone must reproduce the exact history from the fold.
	nodeB := startNodeT(t)
	cloneDir := filepath.Join(t.TempDir(), "clone")
	mustGit(t, "", "clone", remoteURL(nodeB, repo, token), cloneDir)
	if got := revParse(t, cloneDir, "HEAD"); got != head {
		t.Fatalf("clone HEAD after fold = %q, want %q", got, head)
	}
	if out, err := tryGit(cloneDir, "fsck", "--strict"); err != nil {
		t.Fatalf("git fsck: %v\n%s", err, out)
	}
}

// --- repo-key-split regression (ADR-21) --------------------------------------

// TestRepoKeySplit_CrossSurface pins the p3 gate blocker fix end-to-end:
// a push to the name-style /{url}.git URL lands under the RepoID storage
// key (never the raw path element) and is immediately visible to the read
// API; the repo_id-style URL addresses the same repo; unregistered names
// are 404 on the git surface even for authenticated callers.
func TestRepoKeySplit_CrossSurface(t *testing.T) {
	const url = "t-keysplit"
	repoID := registerRepo(t, url)
	work := initWorkRepo(t)
	token := mintTestToken(t, url, "git:read,git:write", "") // claim carries the url

	// (a) Push via the name-style URL (README §6).
	mustGit(t, work, "push", remoteURL(nodeA, url, token), "HEAD:refs/heads/main")
	head := revParse(t, work, "HEAD")

	// Storage is keyed by RepoID only.
	if got := pgRefs(t, repoID)["refs/heads/main"]; got != head {
		t.Fatalf("refs under RepoID key = %v, want main=%s", pgRefs(t, repoID), head)
	}
	if refs := pgRefs(t, url); len(refs) != 0 {
		t.Fatalf("raw url path element must not key ref storage, found: %v", refs)
	}
	if objs := s3Packs(t, url); len(objs) != 0 {
		t.Fatalf("raw url path element must not key pack storage, found: %v", objs)
	}
	if objs := s3Packs(t, repoID); len(objs) < 1 {
		t.Fatal("no pack objects under the RepoID key")
	}

	// Cross-surface: the read API (keyed by RepoID since p3) sees the push.
	var branches struct {
		Branches []struct {
			Name    string `json:"name"`
			HeadSHA string `json:"head_sha"`
		} `json:"branches"`
	}
	if status := apiGet(t, nodeA, token, "/api/v1/repos/branches", &branches); status != http.StatusOK {
		t.Fatalf("GET /repos/branches = %d", status)
	}
	foundMain := false
	for _, b := range branches.Branches {
		if b.Name == "main" && b.HeadSHA == head {
			foundMain = true
		}
	}
	if !foundMain {
		t.Fatalf("read API branches missing main@%s: %+v", head, branches.Branches)
	}
	var commits struct {
		Commits []struct {
			SHA string `json:"sha"`
		} `json:"commits"`
	}
	if status := apiGet(t, nodeA, token, "/api/v1/repos/commits?branch=main", &commits); status != http.StatusOK {
		t.Fatalf("GET /repos/commits = %d", status)
	}
	if len(commits.Commits) == 0 || commits.Commits[0].SHA != head {
		t.Fatalf("read API commits missing pushed head %s: %+v", head, commits.Commits)
	}

	// (c) The repo_id-style URL addresses the same repo.
	cloneDir := filepath.Join(t.TempDir(), "clone")
	mustGit(t, "", "clone", remoteURL(nodeA, repoID, token), cloneDir)
	if got := revParse(t, cloneDir, "HEAD"); got != head {
		t.Fatalf("repo_id-style clone HEAD = %q, want %q", got, head)
	}

	// (b) Unregistered names are 404 for authenticated callers (repos must
	// be created via POST /api/v1/repos first). git surfaces the 404 as a
	// "repository ... not found" fatal.
	ghost := mintTestToken(t, "t-keysplit-ghost", "git:read,git:write", "")
	out, err := tryGit(work, "push", remoteURL(nodeA, "t-keysplit-ghost", ghost), "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("push to unregistered repo should fail:\n%s", out)
	}
	if !strings.Contains(out, "not found") && !strings.Contains(out, "404") {
		t.Fatalf("expected a 404/not-found rejection, got:\n%s", out)
	}
	if refs := pgRefs(t, "t-keysplit-ghost"); len(refs) != 0 {
		t.Fatalf("unregistered push must not create refs: %v", refs)
	}
}

// --- issue #18: PATCH default branch ----------------------------------------

// apiJSON hits the node's HTTP API with a Bearer token and a JSON body,
// decodes a JSON response into out (unless nil), and returns the status.
func apiJSON(t *testing.T, n *testNode, method, token, path, body string, out any) int {
	t.Helper()
	req, err := http.NewRequest(method, "http://"+n.addr+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if out != nil && resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(respBody, out); err != nil {
			t.Fatalf("decode %s %s: %v\n%s", method, path, err, respBody)
		}
	}
	return resp.StatusCode
}

// PATCH /repo end to end (issue #18): after the default branch changes,
// GET /repo reflects it and a fresh clone's HEAD lands on the new branch.
// The second clone runs against the SAME node that just served the first
// one — a warm cache with a fresh generation marker and an unchanged
// ref-store generation — so it also proves the default-branch change
// defeats the issue-#4 hydrate short-circuit.
func TestPatchDefaultBranch_CloneHEADFollows(t *testing.T) {
	const repoURL = "t18-patch-default"
	repoID := registerRepo(t, repoURL)
	// One URL-claim token works on both surfaces (docs/reference/authentication.md): the git path
	// resolves the claim against the repo record, and the API accepts
	// url-style claims directly.
	token := mintTestToken(t, repoURL, "git:read,git:write,repo:write", "")

	// Two branches: main at c1, dev one commit ahead at c2.
	work := initWorkRepo(t)
	mustGit(t, work, "push", remoteURL(nodeA, repoID, token), "HEAD:refs/heads/main")
	mainTip := revParse(t, work, "HEAD")
	writeFile(t, work, "d.txt", "dev\n")
	mustGit(t, work, "add", ".")
	mustGit(t, work, "commit", "-m", "dev tip")
	devTip := revParse(t, work, "HEAD")
	mustGit(t, work, "push", remoteURL(nodeA, repoID, token), "HEAD:refs/heads/dev")

	// Warm the cache: the pre-PATCH clone advertises HEAD -> main.
	clone1 := filepath.Join(t.TempDir(), "clone1")
	mustGit(t, "", "clone", remoteURL(nodeA, repoID, token), clone1)
	if got := revParse(t, clone1, "HEAD"); got != mainTip {
		t.Fatalf("pre-PATCH clone HEAD = %q, want main tip %q", got, mainTip)
	}

	// PATCH via the v1 shape; the response carries the live-spec fields.
	var patched struct {
		RepoID        string `json:"repo_id"`
		ID            string `json:"id"`
		RepoName      string `json:"repo_name"`
		URL           string `json:"url"`
		DefaultBranch string `json:"default_branch"`
	}
	if status := apiJSON(t, nodeA, http.MethodPatch, token, "/api/v1/repo",
		`{"default_branch":"dev"}`, &patched); status != http.StatusOK {
		t.Fatalf("PATCH /api/v1/repo = %d", status)
	}
	if patched.RepoID != repoID || patched.ID != repoID ||
		patched.RepoName != repoURL || patched.URL != repoURL ||
		patched.DefaultBranch != "dev" {
		t.Fatalf("PATCH response = %+v, want %s/%s default_branch dev", patched, repoID, repoURL)
	}

	// GET /repo reflects the change.
	var got struct {
		DefaultBranch string `json:"default_branch"`
	}
	if status := apiGet(t, nodeA, token, "/api/v1/repo", &got); status != http.StatusOK {
		t.Fatalf("GET /api/v1/repo = %d", status)
	}
	if got.DefaultBranch != "dev" {
		t.Fatalf("GET default_branch = %q after PATCH, want dev", got.DefaultBranch)
	}

	// Immediate re-clone from the SAME warm node: HEAD must follow dev.
	clone2 := filepath.Join(t.TempDir(), "clone2")
	mustGit(t, "", "clone", remoteURL(nodeA, repoID, token), clone2)
	if got := revParse(t, clone2, "HEAD"); got != devTip {
		t.Fatalf("post-PATCH clone HEAD = %q, want dev tip %q (hydrate short-circuit served stale HEAD?)", got, devTip)
	}
	if out := strings.TrimSpace(mustGit(t, clone2, "symbolic-ref", "--short", "HEAD")); out != "dev" {
		t.Fatalf("post-PATCH clone branch = %q, want dev", out)
	}

	// The alias shape switches back; the next clone follows again.
	if status := apiJSON(t, nodeA, http.MethodPatch, token, "/api/repos/"+repoURL,
		`{"default_branch":"refs/heads/main"}`, nil); status != http.StatusOK {
		t.Fatalf("PATCH /api/repos/%s = %d", repoURL, status)
	}
	clone3 := filepath.Join(t.TempDir(), "clone3")
	mustGit(t, "", "clone", remoteURL(nodeA, repoID, token), clone3)
	if got := revParse(t, clone3, "HEAD"); got != mainTip {
		t.Fatalf("post-alias-PATCH clone HEAD = %q, want main tip %q", got, mainTip)
	}
}
