package githttp

// End-to-end acceptance for the push-work bounds: the decoded-byte ingress
// cap (issue #55, CODESTORAGE_MAX_PUSH_BYTES) and the verify-sig work
// bounds (issue #59: bounded creation walks, per-push verdict cache,
// CODESTORAGE_VERIFY_SIG_BUDGET). Real serve nodes, real git clients, and
// raw smart-HTTP requests where the wire shape matters.

import (
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testPushCap = 64 << 10 // 64 KiB: tiny but roomy for the setup pushes

// postReceivePack sends a raw git-receive-pack POST and returns the status
// code and body. body streams as chunked when it is not a *bytes.Reader.
func postReceivePack(t *testing.T, n *testNode, repo, token string, body io.Reader, gzipEncoded bool) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://"+n.addr+"/"+repo+".git/git-receive-pack", body)
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth("t", token)
	req.Header.Set("Content-Type", "application/x-git-receive-pack-request")
	if gzipEncoded {
		req.Header.Set("Content-Encoding", "gzip")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST git-receive-pack: %v", err)
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return resp.StatusCode, string(got)
}

// assertCapEnvelope asserts the issue #55 rejection contract: 413 plus the
// structured JSON envelope naming the operator knob.
func assertCapEnvelope(t *testing.T, status int, body string) {
	t.Helper()
	if status != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413\nbody: %s", status, body)
	}
	var envelope struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal([]byte(body), &envelope); err != nil {
		t.Fatalf("413 body is not the structured envelope: %v\n%s", err, body)
	}
	if !strings.Contains(envelope.Error, "decoded push-size cap") || !strings.Contains(envelope.Error, "CODESTORAGE_MAX_PUSH_BYTES") {
		t.Fatalf("413 envelope does not name the cap and knob: %q", envelope.Error)
	}
}

// shallowFlood builds n bytes of protocol-valid `shallow <oid>` pkt-lines —
// a client-controlled prefix that legitimately precedes the first command
// (gitprotocol-pack update-requests), so the sniffer keeps reading through
// it and the decoded-byte counter does the rejecting.
func shallowFlood(n int) []byte {
	line := pktLine("shallow 1234567812345678123456781234567812345678\n")
	var b bytes.Buffer
	for b.Len() < n {
		b.WriteString(line)
	}
	return b.Bytes()
}

// TestPushCap_OversizedGitPush413 is the real-client acceptance: a push
// whose pack exceeds the cap is rejected with an HTTP 413 before any pack
// bytes reach storage, and the server state stays pristine.
func TestPushCap_OversizedGitPush413(t *testing.T) {
	cacheDir := t.TempDir()
	node, err := startNode(cacheDir, fmt.Sprintf("CODESTORAGE_MAX_PUSH_BYTES=%d", testPushCap))
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(node.stop)

	repo := registerRepo(t, "t-pushcap-git")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/main")
	mainHead := revParse(t, work, "HEAD")

	// ~300 KiB of incompressible bytes: the pack stays far above the 64 KiB
	// cap but under git's http.postBuffer, so the client declares a
	// Content-Length and the fast path answers 413 without reading.
	big := make([]byte, 300<<10)
	if _, err := rand.Read(big); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(work, "big.bin"), big, 0o644); err != nil {
		t.Fatal(err)
	}
	mustGit(t, work, "add", ".")
	mustGit(t, work, "commit", "-m", "oversized")

	out, err := tryGit(work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("oversized push should fail:\n%s", out)
	}
	t.Logf("oversized push transcript:\n%s", out)
	if !strings.Contains(out, "413") {
		t.Fatalf("push failure does not surface the 413:\n%s", out)
	}

	// Server state is untouched: ref unchanged, no new packs registered,
	// and no quarantine leftovers below the node's cache repo.
	if got := pgRefs(t, repo)["refs/heads/main"]; got != mainHead {
		t.Fatalf("PG main = %q, want unchanged %q", got, mainHead)
	}
	if packs := pgPacks(t, repo); len(packs) != 1 {
		t.Fatalf("registered packs = %d, want 1 (setup push only)", len(packs))
	}
	leftovers, err := filepath.Glob(filepath.Join(cacheDir, "*", "objects", "incoming-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(leftovers) != 0 {
		t.Fatalf("quarantine leftovers after 413: %v", leftovers)
	}
}

// TestPushCap_MidStreamOverflowCleanState drives the streaming path: a
// chunked request (no Content-Length fast path) whose pack overflows the
// cap WHILE git receive-pack is consuming it. The push must fail without
// registering anything or leaving quarantine debris.
func TestPushCap_MidStreamOverflowCleanState(t *testing.T) {
	cacheDir := t.TempDir()
	node, err := startNode(cacheDir, fmt.Sprintf("CODESTORAGE_MAX_PUSH_BYTES=%d", testPushCap))
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(node.stop)

	repo := registerRepo(t, "t-pushcap-stream")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/main")
	mainHead := revParse(t, work, "HEAD")

	// ~4 MiB forces git past http.postBuffer into a chunked upload, so the
	// server must catch the overflow mid-stream (pushGate path).
	big := make([]byte, 4<<20)
	if _, err := rand.Read(big); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(work, "big.bin"), big, 0o644); err != nil {
		t.Fatal(err)
	}
	mustGit(t, work, "add", ".")
	mustGit(t, work, "commit", "-m", "oversized chunked")

	out, err := tryGit(work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("oversized chunked push should fail:\n%s", out)
	}
	t.Logf("oversized chunked push transcript:\n%s", out)

	if got := pgRefs(t, repo)["refs/heads/main"]; got != mainHead {
		t.Fatalf("PG main = %q, want unchanged %q", got, mainHead)
	}
	if packs := pgPacks(t, repo); len(packs) != 1 {
		t.Fatalf("registered packs = %d, want 1 (setup push only)", len(packs))
	}
	leftovers, err := filepath.Glob(filepath.Join(cacheDir, "*", "objects", "incoming-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(leftovers) != 0 {
		t.Fatalf("quarantine leftovers after mid-stream overflow: %v", leftovers)
	}
}

// TestPushCap_RawRequests pins the wire-level rejection shapes: declared
// oversize (Content-Length fast path), chunked overflow caught during the
// capability sniff, and a gzip bomb whose small compressed body decodes
// past the cap.
func TestPushCap_RawRequests(t *testing.T) {
	node, err := startNode(t.TempDir(), fmt.Sprintf("CODESTORAGE_MAX_PUSH_BYTES=%d", testPushCap))
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(node.stop)

	repo := registerRepo(t, "t-pushcap-raw")
	token := mintTestToken(t, repo, "git:read,git:write", "")

	t.Run("content-length-fast-path", func(t *testing.T) {
		// *bytes.Reader ⇒ Content-Length set ⇒ rejected before any read.
		status, body := postReceivePack(t, node, repo, token,
			bytes.NewReader(make([]byte, testPushCap+1)), false)
		assertCapEnvelope(t, status, body)
	})

	t.Run("chunked-overflow-during-sniff", func(t *testing.T) {
		// Wrapping the reader hides the length ⇒ chunked ⇒ the decoded
		// counter (not the absent Content-Length) must reject, before git
		// is ever spawned.
		flood := shallowFlood(2 * testPushCap)
		status, body := postReceivePack(t, node, repo, token,
			io.MultiReader(bytes.NewReader(flood)), false)
		assertCapEnvelope(t, status, body)
	})

	t.Run("chunked-overflow-with-git-running", func(t *testing.T) {
		// A cap ABOVE the sniffer's 1 MiB peek budget: the sniff gives up
		// inside the shallow flood (fail-closed atomic), git receive-pack
		// starts and keeps consuming shallow lines until the counter trips
		// mid-stream — the pushGate path. receive-pack's own failure report
		// must not commit the response: the client still gets the 413.
		bigCap := int64(2 << 20)
		capNode, err := startNode(t.TempDir(), fmt.Sprintf("CODESTORAGE_MAX_PUSH_BYTES=%d", bigCap))
		if err != nil {
			t.Fatalf("start node: %v", err)
		}
		t.Cleanup(capNode.stop)
		capRepo := registerRepo(t, "t-pushcap-midstream")
		capToken := mintTestToken(t, capRepo, "git:read,git:write", "")
		flood := shallowFlood(int(2 * bigCap))
		status, body := postReceivePack(t, capNode, capRepo, capToken,
			io.MultiReader(bytes.NewReader(flood)), false)
		assertCapEnvelope(t, status, body)
	})

	t.Run("gzip-empty-member-flood", func(t *testing.T) {
		// Concatenated EMPTY gzip members decode to nothing, so the
		// decoded counter never advances — only the compressed-side wire
		// counter can stop the stream (Codex round 6). Chunked, so no
		// Content-Length fast path either.
		var one bytes.Buffer
		zw := gzip.NewWriter(&one)
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
		member := one.Bytes()
		flood := bytes.Repeat(member, 3*testPushCap/len(member))
		status, body := postReceivePack(t, node, repo, token,
			io.MultiReader(bytes.NewReader(flood)), true)
		assertCapEnvelope(t, status, body)
	})

	t.Run("gzip-bomb", func(t *testing.T) {
		// ~16 MiB decoded, a few KiB compressed: the compressed size passes
		// any transport-level check, so only the post-gzip decoded counter
		// can catch it.
		var compressed bytes.Buffer
		zw := gzip.NewWriter(&compressed)
		if _, err := zw.Write(shallowFlood(16 << 20)); err != nil {
			t.Fatal(err)
		}
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
		t.Logf("gzip bomb: %d bytes compressed, %d decoded", compressed.Len(), 16<<20)
		// The compressed body must fit UNDER the cap: a server counting
		// compressed (wire) bytes would accept it, so a 413 here proves
		// the counter runs on the decoded stream.
		if compressed.Len() >= testPushCap {
			t.Fatalf("bomb did not compress below the cap (%d bytes); the test would not prove decoded counting", compressed.Len())
		}
		status, body := postReceivePack(t, node, repo, token, bytes.NewReader(compressed.Bytes()), true)
		assertCapEnvelope(t, status, body)
	})
}

// TestPushCap_AtomicSniffStillWorks proves the #28 atomic gate keeps
// working through the capped reader: a mixed-policy `git push --atomic`
// under the cap is denied whole by the atomic gate — not mangled into a
// 413, not partially applied.
func TestPushCap_AtomicSniffStillWorks(t *testing.T) {
	node, err := startNode(t.TempDir(), fmt.Sprintf("CODESTORAGE_MAX_PUSH_BYTES=%d", 1<<20))
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(node.stop)

	repo := registerRepo(t, "t-pushcap-atomic")
	work := initWorkRepo(t)
	plain := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(node, repo, plain), "HEAD:refs/heads/main")

	commitFile(t, work, "b.txt", "two\n", "two", nil)
	mixed := mintTestToken(t, repo, "git:read,git:write",
		`[["refs/heads/protected",["no-push"]],["*",[]]]`)
	out, err := tryGit(work, "push", "--atomic", remoteURL(node, repo, mixed),
		"HEAD:refs/heads/ok", "HEAD:refs/heads/protected")
	if err == nil {
		t.Fatalf("mixed-policy atomic push should be denied whole:\n%s", out)
	}
	if !strings.Contains(out, "atomic push carries a policy-denied update") {
		t.Fatalf("atomic denial missing through the capped path:\n%s", out)
	}
	refs := pgRefs(t, repo)
	if _, exists := refs["refs/heads/ok"]; exists {
		t.Fatal("atomic semantics broken on the capped path: allowed ref landed despite the denial")
	}
	if _, exists := refs["refs/heads/protected"]; exists {
		t.Fatal("policy-denied ref landed")
	}
}

// readSpawnLog returns the verifier-spawn shas recorded by the test-mode
// knob (one line per actual `git verify-commit` subprocess).
func readSpawnLog(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return strings.Fields(string(data))
}

// TestVerifySig_CreationWalkDedupAcrossRefs pins the two issue #59
// properties of creation walks together: per the upstream contract, a
// creation verifies EVERYTHING reachable from the tip (never bounded by
// other refs), but within one push the verdict cache makes N creations at
// the same deep tip cost each commit exactly one verifier spawn instead of
// N full re-verifications.
func TestVerifySig_CreationWalkDedupAcrossRefs(t *testing.T) {
	spawnLog := filepath.Join(t.TempDir(), "spawns.log")
	node, err := startNode(t.TempDir(), EnvVerifySpawnLog+"="+spawnLog)
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(node.stop)

	repo := registerRepo(t, "t-vs-creation")
	privPath, pubLine := genSSHKey(t)
	key := registerSigningKey(t, pubLine)
	t.Cleanup(func() { removeSigningKey(t, key.ID) })

	// A fully signed history (the unsigned initWorkRepo root would deny the
	// creation walk — that path is pinned by TestVerifySig_SSHMatrix (5)).
	work := t.TempDir()
	mustGit(t, work, "init", "-b", "main", ".")
	commits := 20
	want := make(map[string]bool)
	for i := 0; i < commits; i++ {
		sha := commitFile(t, work, "f.txt", fmt.Sprintf("v%d\n", i), fmt.Sprintf("signed %d", i), nil, sshSigningArgs(privPath)...)
		want[sha] = true
	}
	plain := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(node, repo, plain), "HEAD:refs/heads/main")
	tip := revParse(t, work, "HEAD")

	vsAll := mintTestToken(t, repo, "git:read,git:write", `[["*",["verify-sig"]]]`)
	refspecs := []string{"push", remoteURL(node, repo, vsAll)}
	for i := 0; i < 10; i++ {
		refspecs = append(refspecs, fmt.Sprintf("HEAD:refs/heads/copy%d", i))
	}
	mustGit(t, work, refspecs...)

	refs := pgRefs(t, repo)
	for i := 0; i < 10; i++ {
		if got := refs[fmt.Sprintf("refs/heads/copy%d", i)]; got != tip {
			t.Fatalf("copy%d = %q, want %q", i, got, tip)
		}
	}
	spawns := readSpawnLog(t, spawnLog)
	if len(spawns) != commits {
		t.Fatalf("10 creations at one tip spawned %d verifiers, want %d (full reachable walk, each commit once)", len(spawns), commits)
	}
	for _, sha := range spawns {
		if !want[sha] {
			t.Fatalf("verifier spawned for unexpected sha %s", sha)
		}
		delete(want, sha)
	}
}

// TestVerifySig_UnpolicedRefLaunderingDenied pins the branch-protection
// no-bypass rule end-to-end (Codex round 1 on issue #59's original
// direction): pushing an unsigned commit to an unpoliced ref and then
// creating a verify-sig-covered ref at that same SHA must still verify —
// and reject — that commit. The comparison is never against other refs.
func TestVerifySig_UnpolicedRefLaunderingDenied(t *testing.T) {
	repo := registerRepo(t, "t-vs-launder")
	privPath, pubLine := genSSHKey(t)
	key := registerSigningKey(t, pubLine)
	t.Cleanup(func() { removeSigningKey(t, key.ID) })

	// One token: protected/* requires signatures, everything else is open.
	tok := mintTestToken(t, repo, "git:read,git:write",
		`[["refs/heads/protected/*",["verify-sig"]],["*",[]]]`)

	// Signed-from-the-root history lands on protected/main (a creation
	// walks everything reachable, so the root must be signed too).
	work := t.TempDir()
	mustGit(t, work, "init", "-b", "main", ".")
	signedBase := commitFile(t, work, "a2.txt", "signed\n", "signed base", nil, sshSigningArgs(privPath)...)
	mustGit(t, work, "push", remoteURL(nodeA, repo, tok), "HEAD:refs/heads/protected/main")

	// Unsigned commit lands on the unpoliced scratch ref — allowed.
	unsigned := commitFile(t, work, "b2.txt", "unsigned\n", "unsigned laundered", nil)
	mustGit(t, work, "push", remoteURL(nodeA, repo, tok), "HEAD:refs/heads/scratch")

	// Creating a protected ref at that same SHA must re-verify and deny it.
	out, err := tryGit(work, "push", remoteURL(nodeA, repo, tok), "HEAD:refs/heads/protected/laundered")
	if err == nil {
		t.Fatalf("laundered creation should be rejected:\n%s", out)
	}
	if !strings.Contains(out, "verify-sig denied refs/heads/protected/laundered at "+unsigned) {
		t.Fatalf("laundering rejection missing verify-sig log line for the unsigned commit:\n%s", out)
	}
	refs := pgRefs(t, repo)
	if _, exists := refs["refs/heads/protected/laundered"]; exists {
		t.Fatal("laundered protected ref landed")
	}
	if got := refs["refs/heads/protected/main"]; got != signedBase {
		t.Fatalf("protected/main = %q, want %q", got, signedBase)
	}
}

// TestVerifySig_VerdictCacheAcrossRefs is the issue #59b acceptance: one
// push advancing many refs over the same (old,new) range verifies each
// introduced commit exactly once, across the pre-receive policy gate AND
// every update hook (separate processes sharing the per-push state file).
// The unsigned pre-policy base stays acceptable because updates compare
// against each ref's own prior tip.
func TestVerifySig_VerdictCacheAcrossRefs(t *testing.T) {
	spawnLog := filepath.Join(t.TempDir(), "spawns.log")
	node, err := startNode(t.TempDir(), EnvVerifySpawnLog+"="+spawnLog)
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(node.stop)

	repo := registerRepo(t, "t-vs-cache")
	work := initWorkRepo(t)
	plain := mintTestToken(t, repo, "git:read,git:write", "")
	// Seed all 10 refs at the unsigned base with the unpoliced token, so
	// the verify-sig push below is 10 UPDATES sharing one (old,new) range.
	refspecs := []string{"push", remoteURL(node, repo, plain), "HEAD:refs/heads/main"}
	for i := 0; i < 10; i++ {
		refspecs = append(refspecs, fmt.Sprintf("HEAD:refs/heads/r%d", i))
	}
	mustGit(t, work, refspecs...)

	privPath, pubLine := genSSHKey(t)
	key := registerSigningKey(t, pubLine)
	t.Cleanup(func() { removeSigningKey(t, key.ID) })

	want := make(map[string]bool)
	for i := 0; i < 3; i++ {
		sha := commitFile(t, work, "s.txt", fmt.Sprintf("s%d\n", i), fmt.Sprintf("signed %d", i), nil, sshSigningArgs(privPath)...)
		want[sha] = true
	}

	vsAll := mintTestToken(t, repo, "git:read,git:write", `[["*",["verify-sig"]]]`)
	refspecs = []string{"push", remoteURL(node, repo, vsAll)}
	for i := 0; i < 10; i++ {
		refspecs = append(refspecs, fmt.Sprintf("HEAD:refs/heads/r%d", i))
	}
	mustGit(t, work, refspecs...)

	spawns := readSpawnLog(t, spawnLog)
	if len(spawns) != len(want) {
		t.Fatalf("10 refs over one range spawned %d verifiers (%v), want %d (one per introduced commit)", len(spawns), spawns, len(want))
	}
	for _, sha := range spawns {
		if !want[sha] {
			t.Fatalf("verifier spawned for unexpected sha %s", sha)
		}
		delete(want, sha)
	}
}

// TestVerifySig_BudgetFailsClosed is the issue #59c acceptance: a push
// introducing more commits than CODESTORAGE_VERIFY_SIG_BUDGET allows is
// rejected whole BEFORE any verifier runs, with a message naming the knob.
func TestVerifySig_BudgetFailsClosed(t *testing.T) {
	spawnLog := filepath.Join(t.TempDir(), "spawns.log")
	node, err := startNode(t.TempDir(),
		"CODESTORAGE_VERIFY_SIG_BUDGET=2",
		EnvVerifySpawnLog+"="+spawnLog,
	)
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(node.stop)

	repo := registerRepo(t, "t-vs-budget")
	work := initWorkRepo(t)
	plain := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(node, repo, plain), "HEAD:refs/heads/main")

	privPath, pubLine := genSSHKey(t)
	key := registerSigningKey(t, pubLine)
	t.Cleanup(func() { removeSigningKey(t, key.ID) })
	for i := 0; i < 3; i++ {
		commitFile(t, work, "s.txt", fmt.Sprintf("s%d\n", i), fmt.Sprintf("signed %d", i), nil, sshSigningArgs(privPath)...)
	}

	vsAll := mintTestToken(t, repo, "git:read,git:write", `[["*",["verify-sig"]]]`)
	out, err := tryGit(work, "push", remoteURL(node, repo, vsAll), "HEAD:refs/heads/feature")
	if err == nil {
		t.Fatalf("push over the verification budget should be rejected:\n%s", out)
	}
	if !strings.Contains(out, "per-push verification budget") || !strings.Contains(out, "CODESTORAGE_VERIFY_SIG_BUDGET") {
		t.Fatalf("budget rejection missing the documented message:\n%s", out)
	}
	if _, exists := pgRefs(t, repo)["refs/heads/feature"]; exists {
		t.Fatal("budget-rejected ref landed")
	}
	// Fail closed means BEFORE the work: the pre-charge rejected the push
	// without a single verifier spawn.
	if spawns := readSpawnLog(t, spawnLog); len(spawns) != 0 {
		t.Fatalf("budget rejection spawned %d verifiers (%v), want 0", len(spawns), spawns)
	}
}
