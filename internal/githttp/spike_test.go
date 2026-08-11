package githttp

// Acceptance tests for the de-risk spike. Real containers only (no fakes):
// testcontainers-go boots Postgres + MinIO, TestMain builds the codestorage
// binary once and runs nodes as separate OS processes, exactly like
// production would. ES256 keys are generated at runtime and never committed.

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"codestorage/internal/blob"
	"codestorage/internal/ref"
)

var (
	binPath    string
	pgURL      string
	blobURL    string
	pubKeyPath string
	privKey    *ecdsa.PrivateKey
	testStore  *ref.PG
	testBlob   *blob.S3
	nodeA      *testNode
)

func TestMain(m *testing.M) {
	os.Exit(testMainInner(m))
}

func testMainInner(m *testing.M) int {
	ctx := context.Background()

	scratch, err := os.MkdirTemp("", "spike-accept-*")
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

	// Real containers.
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
	pgURL, err = pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		panic(err)
	}

	minioC, err := tcminio.Run(ctx, "minio/minio:RELEASE.2024-01-16T16-07-38Z")
	if err != nil {
		panic("start minio container: " + err.Error())
	}
	defer func() { _ = minioC.Terminate(ctx) }()
	minioAddr, err := minioC.ConnectionString(ctx)
	if err != nil {
		panic(err)
	}
	blobURL = fmt.Sprintf("s3://spike-test?endpoint=http://%s&s3ForcePathStyle=true&region=us-east-1", minioAddr)
	os.Setenv("AWS_ACCESS_KEY_ID", minioC.Username)
	os.Setenv("AWS_SECRET_ACCESS_KEY", minioC.Password)

	// Runtime-generated ES256 keypair (never committed).
	privKey, err = GenerateKeyPair()
	if err != nil {
		panic(err)
	}
	if _, pubKeyPath, err = WriteKeyPairPEM(privKey, filepath.Join(scratch, "key")); err != nil {
		panic(err)
	}

	// Direct PG/S3 handles for assertions; also prepares schema + bucket.
	testStore, err = ref.OpenPG(ctx, pgURL)
	if err != nil {
		panic(err)
	}
	defer testStore.Close()
	if err := testStore.InitSchema(ctx); err != nil {
		panic(err)
	}
	testBlob, err = blob.OpenS3(ctx, blobURL)
	if err != nil {
		panic(err)
	}
	defer testBlob.Close()
	if err := testBlob.EnsureBucket(ctx); err != nil {
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

func startNode(cacheDir string, extraEnv ...string) (*testNode, error) {
	cmd := exec.Command(binPath, "serve",
		"--addr", "127.0.0.1:0",
		"--cache-dir", cacheDir,
		"--jwt-pubkey", pubKeyPath,
	)
	cmd.Env = append(os.Environ(),
		"PG_URL="+pgURL,
		"BLOB_URL="+blobURL,
		"SPIKE_TEST_MODE=1",
	)
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
	"-c", "user.email=spike@test.invalid",
	"-c", "user.name=spike",
	"-c", "init.defaultBranch=main",
	"-c", "advice.detachedHead=false",
}

func tryGit(dir string, args ...string) (string, error) {
	full := append(append([]string{}, gitIdentity...), args...)
	cmd := exec.Command("git", full...)
	cmd.Dir = dir
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

func mintTestToken(t *testing.T, repo, scopes, refsJSON string) string {
	t.Helper()
	token, err := MintToken(privKey, "spike", repo, strings.Split(scopes, ","), refsJSON, time.Hour)
	if err != nil {
		t.Fatalf("mint token: %v", err)
	}
	return token
}

func remoteURL(n *testNode, repo, token string) string {
	return fmt.Sprintf("http://t:%s@%s/%s.git", token, n.addr, repo)
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

func minioPacks(t *testing.T, repo string) []blob.ListedObject {
	t.Helper()
	objs, err := testBlob.List(context.Background(), blob.PackPrefix(repo))
	if err != nil {
		t.Fatalf("list minio objects: %v", err)
	}
	return objs
}

// --- acceptance criterion 1 ------------------------------------------------

func TestPush_RefsInPG_PacksInMinIO(t *testing.T) {
	repo := "t1-push"
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")

	out := mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")
	t.Logf("push output:\n%s", out)
	head := revParse(t, work, "HEAD")

	refs := pgRefs(t, repo)
	if got := refs["refs/heads/main"]; got != head {
		t.Fatalf("PG refs/heads/main = %q, want client HEAD %q (refs: %v)", got, head, refs)
	}
	if objs := minioPacks(t, repo); len(objs) < 1 {
		t.Fatalf("expected >=1 pack object in MinIO under %s, got %v", blob.PackPrefix(repo), objs)
	}
	if packs := pgPacks(t, repo); len(packs) < 1 {
		t.Fatalf("expected >=1 pack row in PG, got %v", packs)
	}
}

// --- acceptance criterion 2 ------------------------------------------------

func TestCloneFromNodeB_FreshCache(t *testing.T) {
	repo := "t2-clone"
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
	repo := "t3-race"
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")

	// Dedicated node with the delay knob so both update hooks overlap and the
	// loser is deterministically rejected at the PG CAS.
	raceNode := startNodeT(t, "SPIKE_HOOK_DELAY_MS=1500")

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
	repo := "t4-crash"
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")
	oldHead := revParse(t, work, "HEAD")

	refsBefore := pgRefs(t, repo)
	packsBefore := pgPacks(t, repo)
	objsBefore := minioPacks(t, repo)

	writeFile(t, work, "b.txt", "two\n")
	mustGit(t, work, "add", ".")
	mustGit(t, work, "commit", "-m", "two")
	newHead := revParse(t, work, "HEAD")

	// Crash the pre-receive hook between S3 upload and PG commit.
	out, err := tryGit(work,
		"-c", "http.extraHeader=X-Spike-Crash: after-s3",
		"push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("push should have failed under crash knob:\n%s", out)
	}
	t.Logf("crashed push output:\n%s", out)

	// Orphaned S3 pack only; PG refs + packs untouched.
	objsAfter := minioPacks(t, repo)
	if len(objsAfter) != len(objsBefore)+1 {
		t.Fatalf("MinIO objects = %d, want %d (exactly one new orphan)", len(objsAfter), len(objsBefore)+1)
	}
	if got := pgRefs(t, repo); !mapsEqual(got, refsBefore) {
		t.Fatalf("PG refs changed across crash: %v -> %v", refsBefore, got)
	}
	if got := pgPacks(t, repo); len(got) != len(packsBefore) {
		t.Fatalf("PG packs changed across crash: %v -> %v", packsBefore, got)
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

// --- review-finding regressions: unit --------------------------------------

func TestGlobMatch(t *testing.T) {
	cases := []struct {
		pattern, s string
		want       bool
	}{
		{"*", "refs/heads/main", true}, // leading `*` (regression: never matched)
		{"*", "", true},
		{"*-wip", "refs/heads/foo-wip", true},
		{"refs/heads/*", "refs/heads/a/b", true}, // `*` crosses `/` (spike-local)
		{"refs/heads/*", "refs/tags/v1", false},
		{"refs/heads/main", "refs/heads/main", true},
		{"refs/heads/ma.n", "refs/heads/maXn", false}, // regexp metachars quoted
	}
	for _, c := range cases {
		if got := globMatch(c.pattern, c.s); got != c.want {
			t.Errorf("globMatch(%q, %q) = %v, want %v", c.pattern, c.s, got, c.want)
		}
	}
}

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

// --- review-finding regressions: acceptance ---------------------------------

// A push that transfers no new objects (branch created from an already-pushed
// commit) must succeed: depending on git version/client it carries an empty
// pack or no pack at all, and neither may be rejected by pre-receive.
func TestBranchCreate_NoNewObjects(t *testing.T) {
	repo := "t6-branch-create"
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
	repo := "t7-no-push"
	work := initWorkRepo(t)
	setupToken := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, setupToken), "HEAD:refs/heads/main")
	mainHead := revParse(t, work, "HEAD")

	writeFile(t, work, "b.txt", "two\n")
	mustGit(t, work, "add", ".")
	mustGit(t, work, "commit", "-m", "two")

	// Leading-`*` wildcard (regression) combined with the no-push op.
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

func TestRefPolicy_VerifySigFailsClosed(t *testing.T) {
	repo := "t8-verify-sig"
	work := initWorkRepo(t)
	setupToken := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, setupToken), "HEAD:refs/heads/main")
	mainHead := revParse(t, work, "HEAD")

	writeFile(t, work, "b.txt", "two\n")
	mustGit(t, work, "add", ".")
	mustGit(t, work, "commit", "-m", "two")

	// verify-sig is not implemented by the spike; it must fail closed.
	token := mintTestToken(t, repo, "git:read,git:write", `[["refs/heads/main",["verify-sig"]]]`)
	out, err := tryGit(work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("push should be rejected by verify-sig policy:\n%s", out)
	}
	t.Logf("verify-sig output:\n%s", out)
	if !regexp.MustCompile(`\[remote rejected\].*(verify-sig|hook declined)`).MatchString(out) {
		t.Fatalf("push was not remote-rejected with a verify-sig message:\n%s", out)
	}
	if got := pgRefs(t, repo)["refs/heads/main"]; got != mainHead {
		t.Fatalf("PG main = %q, want unchanged %q", got, mainHead)
	}
}

// docs/05: `+ephemeral` remotes see/update only the ephemeral ref space and
// persistent remotes must not see ephemeral refs (separate ref stores).
func TestNamespace_EphemeralIsolated(t *testing.T) {
	repo := "t9-ns"
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
	repo := "t5-policy"
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
// (docs/02), so allowing deletion would make the policy trivially
// bypassable (delete + re-push = force push). Unpoliced refs stay deletable.
// The policed ref is deliberately NOT the repo HEAD: deleting HEAD is
// rejected by git's own receive.denyDeleteCurrent before hooks run.
func TestRefPolicy_NoForcePushDeniesDeletion(t *testing.T) {
	repo := "t8-delete-policy"
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
