package spike

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"codestorage/internal/blob"
	"codestorage/internal/ref"
)

// Env names used to route request context from the HTTP handler into the
// git hooks (which re-exec this binary).
const (
	EnvRepo       = "SPIKE_REPO"
	EnvRefsPolicy = "SPIKE_REFS_POLICY"
	EnvCrash      = "SPIKE_CRASH"
	EnvTestMode   = "SPIKE_TEST_MODE"
	EnvHookDelay  = "SPIKE_HOOK_DELAY_MS"
)

func testMode() bool { return os.Getenv(EnvTestMode) == "1" }

func mustEnv(name string) string {
	v := os.Getenv(name)
	assertf(v != "", "hook: required env %s is empty", name)
	return v
}

// RunHook dispatches `codestorage hook <pre-receive|update> [args...]`,
// invoked by git via the scripts EnsureCacheRepo installs.
func RunHook(ctx context.Context, args []string) error {
	assertf(len(args) >= 1, "hook: missing hook name argument")
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

	var commands []refCommand
	sc := bufio.NewScanner(os.Stdin)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		assertf(len(parts) == 3, "pre-receive: malformed stdin line %q", line)
		commands = append(commands, refCommand{old: parts[0], new: parts[1], ref: parts[2]})
	}
	assertf(sc.Err() == nil, "pre-receive: reading stdin: %v", sc.Err())
	assertf(len(commands) > 0, "pre-receive: git invoked hook with no commands")

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
		// work. receive-pack verified connectivity before invoking us;
		// assert that invariant rather than rejecting the push.
		for _, c := range commands {
			assertf(c.new == ref.ZeroSHA || objectExists(c.new),
				"pre-receive: no quarantined pack and unknown object %s for %s", c.new, c.ref)
		}
		return nil
	}

	bl, err := blob.OpenS3(ctx, mustEnv("BLOB_URL"))
	if err != nil {
		return err
	}
	defer bl.Close()

	var packs []ref.Pack
	for _, pf := range packFiles {
		base := filepath.Base(pf) // pack-<hash>.pack
		id := strings.TrimSuffix(strings.TrimPrefix(base, "pack-"), ".pack")
		assertf(id != "" && id != base, "pre-receive: unexpected pack file name %q", base)
		key := blob.PackKey(repo, id)
		if err := bl.UploadFile(ctx, key, pf); err != nil {
			return fmt.Errorf("pre-receive: upload %s: %w", key, err)
		}
		packs = append(packs, ref.Pack{ID: id, S3Key: key})
	}

	// Test knob: crash between S3 upload and PG commit.
	if testMode() && os.Getenv(EnvCrash) == "after-s3" {
		fmt.Fprintln(os.Stderr, "spike: crashing after S3 upload, before PG commit (test knob)")
		os.Exit(42)
	}

	st, err := ref.OpenPG(ctx, mustEnv("PG_URL"))
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.RegisterPacks(ctx, repo, packs); err != nil {
		return fmt.Errorf("pre-receive: register packs: %w", err)
	}
	return nil
}

// updateHook enforces the JWT ref policy (first-match-wins), then performs
// the PG CAS. Exit != 0 makes git report a per-ref `ng` with our stderr
// message; git only updates the local cache ref for winners.
func updateHook(ctx context.Context, args []string) error {
	assertf(len(args) == 3, "update: want 3 args (ref old new), got %v", args)
	refName, old, new := args[0], args[1], args[2]
	repo := mustEnv(EnvRepo)

	if policyJSON := os.Getenv(EnvRefsPolicy); policyJSON != "" {
		policies, err := ParseRefPolicies(policyJSON)
		if err != nil {
			return fmt.Errorf("update: %w", err)
		}
		if rule, ok := MatchPolicy(policies, refName); ok {
			for _, op := range rule.Ops {
				switch op {
				case "no-push":
					return fmt.Errorf("policy no-push denies update to %s", refName)
				case "no-force-push":
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
				case "verify-sig":
					return fmt.Errorf("policy verify-sig is not supported by the spike (ref %s)", refName)
				default:
					return fmt.Errorf("unknown policy op %q for %s", op, refName)
				}
			}
		}
	}

	// Test knob: widen the CAS race window deterministically.
	if testMode() {
		if ms := os.Getenv(EnvHookDelay); ms != "" {
			n, err := strconv.Atoi(ms)
			assertf(err == nil && n >= 0, "bad %s=%q", EnvHookDelay, ms)
			time.Sleep(time.Duration(n) * time.Millisecond)
		}
	}

	st, err := ref.OpenPG(ctx, mustEnv("PG_URL"))
	if err != nil {
		return err
	}
	defer st.Close()
	won, err := st.CASRef(ctx, repo, refName, old, new)
	if err != nil {
		return err
	}
	if !won {
		return fmt.Errorf("CAS conflict: %s moved concurrently (expected %.8s)", refName, old)
	}
	return nil
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
	panic(fmt.Sprintf("spike assertion failed: cat-file -e %s: %v", sha, err))
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
	panic(fmt.Sprintf("spike assertion failed: merge-base --is-ancestor %s %s: %v", old, new, err))
}
