package githttp

// E2E matrix for the docs/reference/authentication.md ref-policy engine on the receive path:
// verify-sig (SSH + OpenPGP, delegated to `git verify-commit`), the
// deprecated repo-wide `ops` fallback, the trailing-glob pattern contract,
// and the ephemeral-namespace policy quirk (branch-protection.md).

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/meta"
)

const verifySigMainRefs = `[["refs/heads/main",["verify-sig"]],["*",[]]]`

var policyRejected = regexp.MustCompile(`\[remote rejected\]`)

// sshSigningArgs returns the git -c flags that make a commit SSH-signed
// with the given private key file (commit.gpgsign instead of `commit -S`,
// so the flags stay position-independent).
func sshSigningArgs(privKeyPath string) []string {
	return []string{
		"-c", "gpg.format=ssh",
		"-c", "user.signingkey=" + privKeyPath,
		"-c", "commit.gpgsign=true",
	}
}

// genSSHKey generates a fresh ed25519 SSH keypair and returns the private
// key path and the public key line (authorized-keys format).
func genSSHKey(t *testing.T) (privPath, pubLine string) {
	t.Helper()
	dir := t.TempDir()
	privPath = filepath.Join(dir, "id_ed25519")
	out, err := exec.Command("ssh-keygen", "-t", "ed25519", "-N", "", "-C", "signer@test.invalid", "-f", privPath).CombinedOutput()
	if err != nil {
		t.Fatalf("ssh-keygen: %v\n%s", err, out)
	}
	pub, err := os.ReadFile(privPath + ".pub")
	if err != nil {
		t.Fatal(err)
	}
	return privPath, strings.TrimSpace(string(pub))
}

func registerSigningKey(t *testing.T, keyText string) meta.SigningKey {
	t.Helper()
	k, err := testMeta.AddSigningKey(context.Background(), testOrg, keyText)
	if err != nil {
		t.Fatalf("register signing key: %v", err)
	}
	return k
}

func removeSigningKey(t *testing.T, id int64) {
	t.Helper()
	if err := testMeta.RemoveSigningKey(context.Background(), testOrg, id); err != nil {
		t.Fatalf("remove signing key %d: %v", id, err)
	}
}

// TestVerifySig_SSHMatrix walks the whole verify-sig decision table with a
// real SSH-signed history (branch-protection.md "What gets rejected").
func TestVerifySig_SSHMatrix(t *testing.T) {
	repo := registerRepo(t, "t-verify-sig-ssh")
	work := initWorkRepo(t)
	plain := mintTestToken(t, repo, "git:read,git:write", "")
	vsToken := mintTestToken(t, repo, "git:read,git:write", verifySigMainRefs)

	// Unsigned pre-policy history lands with an unpoliced token.
	mustGit(t, work, "push", remoteURL(nodeA, repo, plain), "HEAD:refs/heads/main")
	baseHead := revParse(t, work, "HEAD")

	// (1) Empty keyring: everything is rejected, even before signing.
	commitFile(t, work, "b.txt", "two\n", "unsigned two", nil)
	out, err := tryGit(work, "push", remoteURL(nodeA, repo, vsToken), "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("push under empty keyring should be rejected:\n%s", out)
	}
	if !strings.Contains(out, "verify-sig denied") || !strings.Contains(out, "no signing keys registered") {
		t.Fatalf("empty-keyring rejection missing documented reason:\n%s", out)
	}

	// Register a runtime-generated SSH signing key.
	privPath, pubLine := genSSHKey(t)
	key := registerSigningKey(t, pubLine)

	// (2) Unsigned commit: rejected, with the documented log line shape.
	out, err = tryGit(work, "push", remoteURL(nodeA, repo, vsToken), "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("unsigned push should be rejected:\n%s", out)
	}
	if !policyRejected.MatchString(out) || !strings.Contains(out, "verify-sig denied refs/heads/main at "+revParse(t, work, "HEAD")) {
		t.Fatalf("unsigned rejection missing verify-sig log line:\n%s", out)
	}
	if got := pgRefs(t, repo)["refs/heads/main"]; got != baseHead {
		t.Fatalf("PG main moved despite rejection: %q", got)
	}

	// (3) Signed with the registered key: accepted. Only the introduced
	// commit is checked — the unsigned pre-policy history stays acceptable.
	mustGit(t, work, "reset", "--hard", "HEAD~1")
	signedHead := commitFile(t, work, "b.txt", "two signed\n", "signed two", nil, sshSigningArgs(privPath)...)
	mustGit(t, work, "push", remoteURL(nodeA, repo, vsToken), "HEAD:refs/heads/main")
	if got := pgRefs(t, repo)["refs/heads/main"]; got != signedHead {
		t.Fatalf("PG main = %q, want signed head %q", got, signedHead)
	}

	// (4) Signed with an UNREGISTERED key: rejected.
	strangerPriv, _ := genSSHKey(t)
	strangerHead := commitFile(t, work, "c.txt", "three\n", "stranger three", nil, sshSigningArgs(strangerPriv)...)
	out, err = tryGit(work, "push", remoteURL(nodeA, repo, vsToken), "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("push signed by unregistered key should be rejected:\n%s", out)
	}
	if !strings.Contains(out, "verify-sig denied refs/heads/main at "+strangerHead) {
		t.Fatalf("unregistered-key rejection missing verify-sig log line:\n%s", out)
	}

	// (5) Branch creation checks EVERYTHING reachable (no pre-push tip), so
	// the unsigned base history rejects it — the documented no-bypass rule
	// (branch-protection.md: the comparison is never against other refs;
	// re-affirmed against issue #59's original creation-walk direction).
	allRefs := mintTestToken(t, repo, "git:read,git:write", `[["*",["verify-sig"]]]`)
	mustGit(t, work, "reset", "--hard", signedHead)
	out, err = tryGit(work, "push", remoteURL(nodeA, repo, allRefs), "HEAD:refs/heads/frombase")
	if err == nil {
		t.Fatalf("branch creation with unsigned history should be rejected:\n%s", out)
	}
	if !strings.Contains(out, "verify-sig denied refs/heads/frombase") {
		t.Fatalf("creation rejection missing verify-sig log line:\n%s", out)
	}

	// (6) Branch deletion under verify-sig introduces nothing: allowed.
	mustGit(t, work, "push", remoteURL(nodeA, repo, plain), "HEAD:refs/heads/todelete")
	mustGit(t, work, "push", remoteURL(nodeA, repo, allRefs), ":refs/heads/todelete")
	if _, exists := pgRefs(t, repo)["refs/heads/todelete"]; exists {
		t.Fatal("verify-sig blocked a deletion (introduces no commits)")
	}

	// (7) Keyring is read fresh per push: after removing the key, even a
	// properly signed push is rejected (empty keyring again).
	removeSigningKey(t, key.ID)
	freshHead := commitFile(t, work, "d.txt", "four\n", "signed four", nil, sshSigningArgs(privPath)...)
	out, err = tryGit(work, "push", remoteURL(nodeA, repo, vsToken), "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("push after key removal should be rejected:\n%s", out)
	}
	if !strings.Contains(out, "verify-sig denied refs/heads/main at "+freshHead) || !strings.Contains(out, "no signing keys registered") {
		t.Fatalf("post-removal rejection missing documented reason:\n%s", out)
	}
}

// TestVerifySig_GPG proves the OpenPGP arm of the git verify-commit
// delegation: a commit signed by a registered armored PGP key passes, one
// signed by an unregistered PGP key fails.
func TestVerifySig_GPG(t *testing.T) {
	if _, err := exec.LookPath("gpg"); err != nil {
		t.Skip("gpg binary not available")
	}
	gnupg := t.TempDir()
	env := []string{"GNUPGHOME=" + gnupg}
	gpg := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("gpg", args...)
		cmd.Env = append(os.Environ(), env...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("gpg %s: %v\n%s", strings.Join(args, " "), err, out)
		}
		return string(out)
	}
	gpg("--batch", "--passphrase", "", "--quick-gen-key", "CS Test <cs@test.invalid>", "ed25519", "sign", "never")
	gpg("--batch", "--passphrase", "", "--quick-gen-key", "CS Stranger <stranger@test.invalid>", "ed25519", "sign", "never")
	armored := gpg("--armor", "--export", "cs@test.invalid")
	if !strings.Contains(armored, "BEGIN PGP PUBLIC KEY BLOCK") {
		t.Fatalf("gpg --export did not produce an armored block:\n%s", armored)
	}
	key := registerSigningKey(t, armored)
	t.Cleanup(func() { removeSigningKey(t, key.ID) })

	repo := registerRepo(t, "t-verify-sig-gpg")
	work := initWorkRepo(t)
	plain := mintTestToken(t, repo, "git:read,git:write", "")
	vsToken := mintTestToken(t, repo, "git:read,git:write", verifySigMainRefs)
	mustGit(t, work, "push", remoteURL(nodeA, repo, plain), "HEAD:refs/heads/main")

	pgpSign := func(who string) []string {
		return []string{"-c", "gpg.format=openpgp", "-c", "user.signingkey=" + who, "-c", "commit.gpgsign=true"}
	}
	signedHead := commitFile(t, work, "b.txt", "signed\n", "gpg signed", env, pgpSign("cs@test.invalid")...)
	mustGit(t, work, "push", remoteURL(nodeA, repo, vsToken), "HEAD:refs/heads/main")
	if got := pgRefs(t, repo)["refs/heads/main"]; got != signedHead {
		t.Fatalf("PG main = %q, want gpg-signed head %q", got, signedHead)
	}

	strangerHead := commitFile(t, work, "c.txt", "stranger\n", "gpg stranger", env, pgpSign("stranger@test.invalid")...)
	out, err := tryGit(work, "push", remoteURL(nodeA, repo, vsToken), "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("push signed by unregistered PGP key should be rejected:\n%s", out)
	}
	if !strings.Contains(out, "verify-sig denied refs/heads/main at "+strangerHead) {
		t.Fatalf("unregistered PGP rejection missing verify-sig log line:\n%s", out)
	}
}

// TestOpsFallback enforces the deprecated repo-wide `ops` claim per docs/reference/authentication.md:
// standalone it acts as a catch-all rule; combined with `refs` it only fills
// the gap left by refs (fold, not additive), preserving first-match-wins.
func TestOpsFallback(t *testing.T) {
	repo := registerRepo(t, "t-ops-fallback")
	work := initWorkRepo(t)
	plain := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, plain), "HEAD:refs/heads/main")
	mainHead := revParse(t, work, "HEAD")

	commitFile(t, work, "b.txt", "two\n", "two", nil)
	newHead := revParse(t, work, "HEAD")

	// ops-only token: acts as a catch-all no-push.
	opsOnly := testClaims(repo, "git:read,git:write")
	opsOnly.Ops = []string{"no-push"}
	out, err := tryGit(work, "push", remoteURL(nodeA, repo, mintWith(t, orgES256Key, "ES256", opsOnly)), "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("ops:[no-push] push should be rejected:\n%s", out)
	}
	if !regexp.MustCompile(`\[remote rejected\].*(no-push|hook declined)`).MatchString(out) {
		t.Fatalf("ops fallback did not reject with a no-push message:\n%s", out)
	}
	if got := pgRefs(t, repo)["refs/heads/main"]; got != mainHead {
		t.Fatalf("PG main = %q, want unchanged %q", got, mainHead)
	}

	// refs + ops: the explicit main passthrough matches first, so main is
	// allowed while every other ref falls through to the folded no-push.
	both := testClaims(repo, "git:read,git:write")
	both.Refs = []byte(`[["refs/heads/main",[]]]`)
	both.Ops = []string{"no-push"}
	bothToken := mintWith(t, orgES256Key, "ES256", both)
	out, err = tryGit(work, "push", remoteURL(nodeA, repo, bothToken), "HEAD:refs/heads/main", "HEAD:refs/heads/other")
	if err == nil {
		t.Fatalf("push should exit non-zero when the other ref is rejected:\n%s", out)
	}
	t.Logf("refs+ops push output:\n%s", out)
	refs := pgRefs(t, repo)
	if got := refs["refs/heads/main"]; got != newHead {
		t.Fatalf("PG main = %q, want %q (explicit refs entry must win)", got, newHead)
	}
	if _, exists := refs["refs/heads/other"]; exists {
		t.Fatal("PG refs/heads/other exists; folded ops fallback did not fire")
	}
}

// TestRefPolicy_TrailingGlob pins the branch-protection.md pattern contract
// end-to-end: a trailing `*` after `/` is a prefix match (covers nested
// segments), and it does not bleed past the prefix boundary.
func TestRefPolicy_TrailingGlob(t *testing.T) {
	repo := registerRepo(t, "t-trailing-glob")
	work := initWorkRepo(t)
	plain := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, plain), "HEAD:refs/heads/main")
	head := revParse(t, work, "HEAD")

	globToken := mintTestToken(t, repo, "git:read,git:write",
		`[["refs/heads/feature/*",["no-push"]],["*",[]]]`)

	// Nested path under the prefix: denied.
	out, err := tryGit(work, "push", remoteURL(nodeA, repo, globToken), "HEAD:refs/heads/feature/a/b")
	if err == nil {
		t.Fatalf("push to feature/a/b should be rejected by prefix glob:\n%s", out)
	}
	// Sibling that only shares the string prefix (no `/` boundary): allowed.
	mustGit(t, work, "push", remoteURL(nodeA, repo, globToken), "HEAD:refs/heads/featurex")

	refs := pgRefs(t, repo)
	if _, exists := refs["refs/heads/feature/a/b"]; exists {
		t.Fatal("prefix glob failed to police nested ref")
	}
	if got := refs["refs/heads/featurex"]; got != head {
		t.Fatalf("PG featurex = %q, want %q", got, head)
	}
}

// TestEphemeralPolicyQuirk pins the branch-protection.md quirk: pushes
// through `+ephemeral` match policies against the REWRITTEN name
// `refs/namespaces/ephemeral/<refname>`, so persistent-branch rules do not
// police the ephemeral copy — only `*` or explicit namespaced patterns do.
func TestEphemeralPolicyQuirk(t *testing.T) {
	repo := registerRepo(t, "t-eph-quirk")
	work := initWorkRepo(t)
	plain := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, plain), "HEAD:refs/heads/main")
	mainHead := revParse(t, work, "HEAD")

	commitFile(t, work, "b.txt", "two\n", "two", nil)
	newHead := revParse(t, work, "HEAD")

	// Persistent-main lockdown: fires on the persistent push...
	guard := mintTestToken(t, repo, "git:read,git:write",
		`[["refs/heads/main",["no-push"]],["*",[]]]`)
	out, err := tryGit(work, "push", remoteURL(nodeA, repo, guard), "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("persistent main push should be rejected:\n%s", out)
	}
	if got := pgRefs(t, repo)["refs/heads/main"]; got != mainHead {
		t.Fatalf("PG main = %q, want unchanged %q", got, mainHead)
	}
	// ...but NOT on the ephemeral copy of the same branch (quirk).
	mustGit(t, work, "push", remoteURL(nodeA, repo+"+ephemeral", guard), "HEAD:refs/heads/main")
	if got := pgRefs(t, repo+"+ephemeral")["refs/heads/main"]; got != newHead {
		t.Fatalf("ephemeral main = %q, want %q", got, newHead)
	}

	// Explicit namespaced prefix pattern DOES police ephemeral pushes.
	ephGuard := mintTestToken(t, repo, "git:read,git:write",
		`[["refs/namespaces/ephemeral/*",["no-push"]],["*",[]]]`)
	commitFile(t, work, "c.txt", "three\n", "three", nil)
	out, err = tryGit(work, "push", remoteURL(nodeA, repo+"+ephemeral", ephGuard), "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("namespaced pattern should police the ephemeral push:\n%s", out)
	}
	if got := pgRefs(t, repo+"+ephemeral")["refs/heads/main"]; got != newHead {
		t.Fatalf("ephemeral main = %q, want unchanged %q", got, newHead)
	}
	// The same token still allows the persistent push (namespaced pattern
	// does not match persistent refs; catch-all passthrough applies).
	mustGit(t, work, "push", remoteURL(nodeA, repo, ephGuard), "HEAD:refs/heads/scratch")
}
