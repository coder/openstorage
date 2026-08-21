package githttp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"codestorage/internal/auth"
	"codestorage/internal/blob"
	"codestorage/internal/meta"
	"codestorage/internal/ref"
)

// Env names used to route request context from the HTTP handler into the
// git hooks (which re-exec this binary). CODESTORAGE_TEST_* fault-injection
// knobs are honored only when CODESTORAGE_TEST_MODE=1.
const (
	EnvRepo       = "CODESTORAGE_GIT_REPO"
	EnvRefsPolicy = "CODESTORAGE_GIT_REFS_POLICY"
	EnvOps        = "CODESTORAGE_GIT_OPS"
	// EnvOrg names the org whose signing keys verify-sig loads. Shared with
	// the serve config on purpose: the server sets it explicitly for hooks.
	EnvOrg       = "CODESTORAGE_ORG"
	EnvTestMode  = "CODESTORAGE_TEST_MODE"
	EnvCrash     = "CODESTORAGE_TEST_CRASH"
	EnvHookDelay = "CODESTORAGE_TEST_HOOK_DELAY_MS"
	// EnvCASDrop/EnvCASDropHard hold a marker-file path; the first update
	// hook to create the marker lets its CAS statement commit, then reports
	// a synthetic connection-drop error (issue #2's ambiguous outcome).
	// The hard variant also fails the recovery recheck read, as a truly
	// dead connection would. Later invocations pass through untouched.
	EnvCASDrop     = "CODESTORAGE_TEST_CAS_DROP"
	EnvCASDropHard = "CODESTORAGE_TEST_CAS_DROP_HARD"
	// CrashHeader lets tests request an EnvCrash injection per push; the
	// server refuses it outside test mode.
	CrashHeader = "X-Codestorage-Test-Crash"
)

// PG/S3 connection env for hooks, inherited from the serve process.
// Canonical CODESTORAGE_* names first; the bare legacy names are still read
// so a serve process exporting only those keeps working hooks.
var (
	envHookPGURL   = []string{"CODESTORAGE_PG_URL", "PG_URL"}
	envHookBlobURL = []string{"CODESTORAGE_S3_URL", "BLOB_URL"}
)

func testMode() bool { return os.Getenv(EnvTestMode) == "1" }

// WrapHookRefStore decorates the ref store the update hook opens for its
// CAS. Hooks run in a fresh `codestorage hook` subprocess (git execs the
// installed hook script), so the serve process's wired store cannot reach
// them; instead the cmd wiring seam — which runs in the hook subprocess
// too, it is the same binary — replaces this variable at startup with the
// events.RefStore decorator so git pushes emit push events (docs/06)
// exactly like API writes (that decorator wraps ref.IdempotentCAS itself).
// The eventless default keeps hooks working before/without eventing wiring,
// wrapping just the CAS-recovery decorator so ambiguous outcomes still
// reconcile.
var WrapHookRefStore = func(ctx context.Context, st ref.RefStore) ref.RefStore {
	return ref.IdempotentCAS{RefStore: st}
}

func mustEnv(names ...string) string {
	for _, n := range names {
		if v := os.Getenv(n); v != "" {
			return v
		}
	}
	panic(fmt.Sprintf("githttp assertion failed: hook: required env %s is empty", names[0]))
}

// RunHook dispatches `codestorage hook <pre-receive|update> [args...]`,
// invoked by git via the scripts EnsureCacheRepo installs. Arguments and
// stdin come from outside this process, so malformed input must produce an
// error (stderr message + non-zero exit denies the push), never a panic.
func RunHook(ctx context.Context, args []string) error {
	if len(args) < 1 {
		return errors.New("hook: missing hook name argument")
	}
	switch args[0] {
	case "pre-receive":
		return preReceive(ctx)
	case "update":
		return updateHook(ctx, args[1:])
	default:
		return fmt.Errorf("unknown hook %q", args[0])
	}
}

type refCommand struct{ old, new, ref string }

// preReceive uploads the quarantined pack(s) to S3 FIRST, then registers
// them in PG in one transaction. Crash windows (validated by tests):
//   - die after S3 upload, before PG commit  => orphaned S3 object only
//   - die after PG commit, before ref CAS    => registered-but-unreferenced pack
//
// Refs are never touched here, so a broken ref is impossible.
func preReceive(ctx context.Context) error {
	repo := mustEnv(EnvRepo)
	// git sets GIT_QUARANTINE_PATH only when the client transfers objects;
	// delete-only pushes invoke pre-receive without it.
	quarantine := os.Getenv("GIT_QUARANTINE_PATH")

	// Stdin is external input (git relays it from the client connection):
	// malformed lines deny the push with an error, never a panic.
	var commands []refCommand
	sc := bufio.NewScanner(os.Stdin)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) != 3 {
			return fmt.Errorf("pre-receive: malformed stdin line %q", line)
		}
		cmd := refCommand{old: parts[0], new: parts[1], ref: parts[2]}
		if !ref.ValidSHA(cmd.old) || !ref.ValidSHA(cmd.new) {
			return fmt.Errorf("pre-receive: malformed sha in stdin line %q", line)
		}
		commands = append(commands, cmd)
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("pre-receive: reading stdin: %w", err)
	}
	if len(commands) == 0 {
		return errors.New("pre-receive: git invoked hook with no commands")
	}

	var packFiles []string
	if quarantine != "" {
		var err error
		packFiles, err = filepath.Glob(filepath.Join(quarantine, "pack", "pack-*.pack"))
		if err != nil {
			return err
		}
	}
	if len(packFiles) == 0 {
		// No pack means git received no new objects: a delete-only push or
		// updates targeting objects the server already has (e.g. a branch
		// created from an existing commit). Whether git sends an empty pack
		// in the latter case is version/client dependent, so both paths must
		// work. receive-pack verified connectivity before invoking us, but
		// the SHAs come from client stdin: a violation denies the push with
		// an error instead of crashing receive-pack.
		for _, c := range commands {
			if c.new != ref.ZeroSHA && !objectExists(c.new) {
				return fmt.Errorf("pre-receive: no quarantined pack and unknown object %s for %s", c.new, c.ref)
			}
		}
		return nil
	}

	bl, err := blob.Open(ctx, mustEnv(envHookBlobURL...))
	if err != nil {
		return err
	}
	defer bl.Close()

	var packs []ref.Pack
	for _, pf := range packFiles {
		base := filepath.Base(pf) // pack-<hash>.pack
		id := strings.TrimSuffix(strings.TrimPrefix(base, "pack-"), ".pack")
		if id == "" || id == base {
			// Quarantine contents are produced by git, but fail closed on a
			// surprising name instead of crashing receive-pack.
			return fmt.Errorf("pre-receive: unexpected pack file name %q", base)
		}
		key := blob.PackKey(repo, id)
		if err := bl.UploadFile(ctx, key, pf); err != nil {
			return fmt.Errorf("pre-receive: upload %s: %w", key, err)
		}
		packs = append(packs, ref.Pack{ID: id, Key: key})
	}

	// Test knob: crash between S3 upload and PG commit.
	if testMode() && os.Getenv(EnvCrash) == "after-s3" {
		fmt.Fprintln(os.Stderr, "codestorage: crashing after S3 upload, before PG commit (test knob)")
		os.Exit(42)
	}

	st, err := ref.Open(ctx, mustEnv(envHookPGURL...))
	if err != nil {
		return err
	}
	defer st.Close()
	// Writer half of the orphan-sweep coordination (issue #5): a sweeper
	// may have deleted a just-uploaded key right before the registration
	// committed; re-upload from the local pack file if so. Verification
	// runs even when RegisterPacks fails — an ambiguous error (commit
	// executed, response lost) may have registered the packs, and a
	// committed row must never point at missing bytes. Extra verify work
	// on a genuinely failed registration is harmless: the keys were just
	// uploaded, so each check is one Stat.
	regErr := st.RegisterPacks(ctx, repo, packs)
	if regErr != nil {
		regErr = fmt.Errorf("pre-receive: register packs: %w", regErr)
	}
	for i, p := range packs {
		if err := blob.EnsurePublished(ctx, bl, p.Key, packFiles[i]); err != nil {
			return errors.Join(regErr, fmt.Errorf("pre-receive: verify %s: %w", p.Key, err))
		}
	}
	return regErr
}

// updateHook enforces the JWT ref policy (first-match-wins), then performs
// the PG CAS. Exit != 0 makes git report a per-ref `ng` with our stderr
// message; git only updates the local cache ref for winners.
func updateHook(ctx context.Context, args []string) error {
	// Hook args are handed over by git per ref update; validate instead of
	// asserting so a malformed invocation denies the update, never panics
	// (CASRef below asserts well-formed SHAs).
	if len(args) != 3 {
		return fmt.Errorf("update: want 3 args (ref old new), got %d", len(args))
	}
	refName, old, new := args[0], args[1], args[2]
	if refName == "" {
		return errors.New("update: empty ref name")
	}
	if !ref.ValidSHA(old) || !ref.ValidSHA(new) {
		return fmt.Errorf("update: malformed sha for %s", refName)
	}
	repo := mustEnv(EnvRepo)

	if err := enforceRefPolicy(ctx, repo, refName, old, new); err != nil {
		return err
	}

	// Test knob: widen the CAS race window deterministically.
	if testMode() {
		if ms := os.Getenv(EnvHookDelay); ms != "" {
			n, err := strconv.Atoi(ms)
			assertf(err == nil && n >= 0, "bad %s=%q", EnvHookDelay, ms)
			time.Sleep(time.Duration(n) * time.Millisecond)
		}
	}

	st, err := ref.Open(ctx, mustEnv(envHookPGURL...))
	if err != nil {
		return err
	}
	defer st.Close()
	var inner ref.RefStore = st
	if testMode() {
		inner = injectCASDrop(inner)
	}
	store := WrapHookRefStore(ctx, inner)
	assertf(store != nil, "WrapHookRefStore returned nil")
	won, err := store.CASRef(ctx, repo, refName, old, new)
	if err != nil {
		return err
	}
	if !won {
		return fmt.Errorf("CAS conflict: %s moved concurrently (expected %.8s)", refName, old)
	}
	return nil
}

// errCASDrop is the synthetic post-commit connection-drop error the
// EnvCASDrop knobs inject.
var errCASDrop = errors.New("injected post-commit connection drop")

// casDropStore simulates a DB connection dying after the CAS statement
// commits: CASRef delegates (the write lands), then reports errCASDrop.
// hard additionally fails GetRef, so the ref.IdempotentCAS recovery recheck
// cannot run either — as with a truly dead connection.
type casDropStore struct {
	ref.RefStore
	hard bool
}

func (s casDropStore) CASRef(ctx context.Context, repo, name, old, new string) (bool, error) {
	if _, err := s.RefStore.CASRef(ctx, repo, name, old, new); err != nil {
		return false, err
	}
	return false, errCASDrop
}

func (s casDropStore) GetRef(ctx context.Context, repo, name string) (string, error) {
	if s.hard {
		return "", errCASDrop
	}
	return s.RefStore.GetRef(ctx, repo, name)
}

// injectCASDrop consumes the EnvCASDrop/EnvCASDropHard marker knobs (test
// mode only): the invocation that creates the marker file gets the faulty
// store; every other invocation passes st through untouched.
func injectCASDrop(st ref.RefStore) ref.RefStore {
	marker, hard := os.Getenv(EnvCASDrop), false
	if marker == "" {
		marker, hard = os.Getenv(EnvCASDropHard), true
	}
	if marker == "" {
		return st
	}
	f, err := os.OpenFile(marker, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return st // marker exists: knob already consumed
	}
	_ = f.Close()
	return casDropStore{RefStore: st, hard: hard}
}

// enforceRefPolicy evaluates the token's ref policies (docs/02, docs/upstream
// branch-protection.md) for one ref update. The deprecated repo-wide `ops`
// claim is folded in as a fallback exactly once, here at parse time.
func enforceRefPolicy(ctx context.Context, repo, refName, old, new string) error {
	policyJSON := os.Getenv(EnvRefsPolicy)
	opsJSON := os.Getenv(EnvOps)
	if policyJSON == "" && opsJSON == "" {
		return nil
	}
	var policies []auth.RefPolicy
	if policyJSON != "" {
		var err error
		policies, err = auth.ParseRefPolicies(policyJSON)
		if err != nil {
			return fmt.Errorf("update: %w", err)
		}
	}
	if opsJSON != "" {
		var ops []string
		if err := json.Unmarshal([]byte(opsJSON), &ops); err != nil {
			return fmt.Errorf("update: parse ops claim %q: %w", opsJSON, err)
		}
		policies = auth.FoldOps(policies, ops)
	}

	// Ephemeral namespace quirk (branch-protection.md): a push through the
	// `+ephemeral` namespace matches policies against the rewritten name
	// `refs/namespaces/ephemeral/<refname>`, so persistent-branch rules do
	// not police ephemeral copies unless the token uses `*` or an explicit
	// namespaced pattern. `+import` and persistent pushes match verbatim.
	matchRef := refName
	if strings.HasSuffix(repo, "+ephemeral") {
		matchRef = auth.EphemeralRefName(refName)
	}

	rule, ok := auth.MatchPolicy(policies, matchRef)
	if !ok {
		return nil // unpoliced ref: allowed
	}
	for _, op := range rule.Ops {
		switch op {
		case auth.OpNoPush:
			return fmt.Errorf("policy no-push denies update to %s", refName)
		case auth.OpNoForcePush:
			// Rejects any rewind of an existing ref: non-fast-forward
			// updates AND deletions. docs/02: ref policies are the
			// only branch-protection mechanism, so allowing deletion
			// would make the policy trivially bypassable
			// (delete + re-push = force push). Safe to check here:
			// quarantined objects were migrated into the main store
			// after pre-receive, before update hooks run.
			if old != ref.ZeroSHA {
				if new == ref.ZeroSHA {
					return fmt.Errorf("policy no-force-push denies deletion of %s", refName)
				}
				if !isAncestor(old, new) {
					return fmt.Errorf("policy no-force-push denies non-fast-forward update to %s", refName)
				}
			}
		case auth.OpVerifySig:
			if err := verifySig(ctx, refName, old, new); err != nil {
				return err
			}
		default:
			// Unknown policy ops fail closed.
			return fmt.Errorf("unknown policy op %q for %s", op, refName)
		}
	}
	return nil
}

// verifySig rejects the update unless every commit it introduces carries a
// valid OpenPGP or SSH signature from one of the org's registered signing
// keys (docs/02 verify-sig, branch-protection.md "Commit signing
// verifications"). "Introduced" = reachable from new but not from this
// ref's own pre-push tip; branch creation therefore checks everything and
// deletion checks nothing. The keyring is read fresh from PG on every push.
func verifySig(ctx context.Context, refName, old, new string) error {
	if new == ref.ZeroSHA {
		return nil // deletion introduces no commits
	}
	org := mustEnv(EnvOrg)

	ms, err := meta.Open(ctx, mustEnv(envHookPGURL...))
	if err != nil {
		return fmt.Errorf("verify-sig: open metadata store: %w", err)
	}
	defer ms.Close()
	keys, err := ms.SigningKeysForOrg(ctx, org)
	if err != nil {
		return fmt.Errorf("verify-sig: load signing keys for org %q: %w", org, err)
	}
	if len(keys) == 0 {
		return verifySigDenied(refName, new, fmt.Sprintf("no signing keys registered for org %q", org))
	}

	v, cleanup, err := newSigVerifier(keys)
	if err != nil {
		return fmt.Errorf("verify-sig: %w", err)
	}
	defer cleanup()

	introduced, err := introducedCommits(ctx, old, new)
	if err != nil {
		return fmt.Errorf("verify-sig: %w", err)
	}
	for _, sha := range introduced {
		if reason := v.verifyCommit(ctx, sha); reason != "" {
			return verifySigDenied(refName, sha, reason)
		}
	}
	return nil
}

// verifySigDenied builds the canonical denial error. Its message is the
// documented log line; the hook runner prints it to stderr, which
// receive-pack relays to both the client and the server log.
func verifySigDenied(refName, sha, reason string) error {
	return fmt.Errorf("verify-sig denied %s at %s: %s", refName, sha, reason)
}

// introducedCommits lists the commits reachable from new but not from old
// (branch creation: old is ZeroSHA, so everything reachable from new).
// Runs in the bare repo git put us in (cwd, GIT_DIR set by receive-pack).
func introducedCommits(ctx context.Context, old, new string) ([]string, error) {
	if !ref.ValidSHA(new) || new == ref.ZeroSHA {
		return nil, fmt.Errorf("introducedCommits: bad new sha %q", new)
	}
	args := []string{"rev-list", new}
	if old != ref.ZeroSHA {
		args = append(args, "^"+old)
	}
	out, err := exec.CommandContext(ctx, "git", args...).Output()
	if err != nil {
		return nil, fmt.Errorf("git %s: %w", strings.Join(args, " "), err)
	}
	var shas []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line == "" {
			continue
		}
		// Subprocess output: fail closed on surprises, never panic mid-push.
		if !ref.ValidSHA(line) {
			return nil, fmt.Errorf("rev-list emitted %q", line)
		}
		shas = append(shas, line)
	}
	return shas, nil
}

// sigVerifier delegates signature checking to git's own machinery
// (`git verify-commit`), which dispatches SSH vs OpenPGP by signature type:
// SSH keys go into a temporary allowed-signers file, PGP keys into a
// temporary GNUPGHOME keyring. Battle-tested verification, no bespoke
// crypto (phase-2 ADR).
type sigVerifier struct {
	allowedSigners string // path to the temp allowed_signers file
	gnupgHome      string // temp GNUPGHOME with imported PGP keys; "" when unused
}

// newSigVerifier materializes the org keyring for git. Registered keys are
// SSH public key lines or ASCII-armored PGP public key blocks (meta.Store
// contract); anything else fails closed. A missing gpg binary downgrades
// PGP support only: SSH verification still works and PGP-signed commits
// then fail with a clear reason.
func newSigVerifier(keys []meta.SigningKey) (*sigVerifier, func(), error) {
	dir, err := os.MkdirTemp("", "codestorage-verify-sig-*")
	if err != nil {
		return nil, func() {}, err
	}
	cleanup := func() { _ = os.RemoveAll(dir) }

	var signers strings.Builder
	var pgpKeys []string
	for _, k := range keys {
		text := strings.TrimSpace(k.KeyText)
		switch {
		case strings.Contains(text, "BEGIN PGP PUBLIC KEY BLOCK"):
			pgpKeys = append(pgpKeys, text)
		case looksLikeSSHPublicKey(text):
			// Any signer principal, git namespace only (git signs with
			// namespace "git"; tag/push signatures use it too).
			fmt.Fprintf(&signers, "* namespaces=\"git\" %s\n", text)
		default:
			cleanup()
			return nil, func() {}, fmt.Errorf("registered signing key %d is neither an SSH public key line nor an armored PGP block", k.ID)
		}
	}

	v := &sigVerifier{allowedSigners: filepath.Join(dir, "allowed_signers")}
	if err := os.WriteFile(v.allowedSigners, []byte(signers.String()), 0o600); err != nil {
		cleanup()
		return nil, func() {}, err
	}

	if len(pgpKeys) > 0 {
		home := filepath.Join(dir, "gnupg")
		if err := os.Mkdir(home, 0o700); err != nil {
			cleanup()
			return nil, func() {}, err
		}
		for _, k := range pgpKeys {
			cmd := exec.Command("gpg", "--batch", "--quiet", "--import")
			cmd.Env = append(os.Environ(), "GNUPGHOME="+home)
			cmd.Stdin = strings.NewReader(k)
			if out, err := cmd.CombinedOutput(); err != nil {
				if errors.Is(err, exec.ErrNotFound) {
					// Documented risk: hosts without gpg keep SSH
					// verification; PGP-signed commits fail closed below
					// with git's own missing-gpg reason.
					fmt.Fprintln(os.Stderr, "verify-sig: gpg binary not found; registered PGP keys are unusable on this host")
					break
				}
				cleanup()
				return nil, func() {}, fmt.Errorf("import registered PGP key: %v: %s", err, strings.TrimSpace(string(out)))
			}
		}
		v.gnupgHome = home
	}
	return v, cleanup, nil
}

// looksLikeSSHPublicKey is a defensive shape check for one authorized-keys
// style line ("<type> <base64> [comment]"). Embedded CR/LF would corrupt the
// allowed_signers file, so any multi-line text fails the check.
func looksLikeSSHPublicKey(line string) bool {
	if strings.ContainsAny(line, "\r\n") {
		return false
	}
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return false
	}
	for _, prefix := range []string{"ssh-", "sk-ssh-", "ecdsa-sha2-", "sk-ecdsa-"} {
		if strings.HasPrefix(fields[0], prefix) {
			return true
		}
	}
	return false
}

// verifyCommit returns "" when sha carries a valid signature from the
// materialized keyring, else a short human-readable reason. Delegates to
// `git verify-commit`, which picks gpg or ssh-keygen by signature type.
func (v *sigVerifier) verifyCommit(ctx context.Context, sha string) (reason string) {
	if !ref.ValidSHA(sha) {
		// Callers feed rev-list output; a malformed sha fails closed as a
		// denial reason instead of crashing receive-pack.
		return fmt.Sprintf("malformed commit sha %q", sha)
	}
	cmd := exec.CommandContext(ctx, "git",
		"-c", "gpg.ssh.allowedSignersFile="+v.allowedSigners,
		"verify-commit", sha)
	cmd.Env = os.Environ()
	if v.gnupgHome != "" {
		cmd.Env = append(cmd.Env, "GNUPGHOME="+v.gnupgHome)
	}
	out, err := cmd.CombinedOutput()
	if err == nil {
		return ""
	}
	msg := condenseGitReason(out)
	if msg == "" {
		// git verify-commit is silent for unsigned commits.
		msg = "commit is not signed"
	}
	return msg
}

// condenseGitReason squeezes git/gpg/ssh-keygen diagnostics into one short
// line for the per-ref rejection message.
func condenseGitReason(out []byte) string {
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	var kept []string
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l == "" {
			continue
		}
		kept = append(kept, l)
	}
	msg := strings.Join(kept, "; ")
	const max = 300
	if len(msg) > max {
		msg = msg[:max] + "..."
	}
	return msg
}

// objectExists reports whether sha names an object in the repo git put us
// in (during pre-receive the quarantine is visible via git's alternates).
func objectExists(sha string) bool {
	err := exec.Command("git", "cat-file", "-e", sha).Run()
	if err == nil {
		return true
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return false
	}
	// Not an exit status (git missing, spawn failure): a bug, fail fast.
	panic(fmt.Sprintf("githttp assertion failed: cat-file -e %s: %v", sha, err))
}

// isAncestor reports whether old is an ancestor of new in the repo git put
// us in (hooks run with cwd = bare repo root, GIT_DIR set by git).
func isAncestor(old, new string) bool {
	cmd := exec.Command("git", "merge-base", "--is-ancestor", old, new)
	err := cmd.Run()
	if err == nil {
		return true
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
		return false
	}
	// Anything else (missing objects, bad SHA) is a bug: fail fast.
	panic(fmt.Sprintf("githttp assertion failed: merge-base --is-ancestor %s %s: %v", old, new, err))
}
