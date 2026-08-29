package simtest

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Deterministic git identity for every object the simulation writes. Commit
// SHAs depend only on content, parents, and these identity/date fields, so
// pinning them (dates come from the logical clock) makes the whole object
// graph a pure function of the seed.
const (
	simGitName  = "dst-sim"
	simGitEmail = "dst-sim@codestorage.invalid"
	// simEpoch is the logical time base: step N happens at simEpoch+N.
	simEpoch = 1700000000
)

// gitBaseEnv returns a hermetic environment for simulation git calls: host
// global/system config (hooks, templates, trailers) must not leak into
// object hashing or repo behavior.
func gitBaseEnv() []string {
	return []string{
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_CONFIG_SYSTEM=/dev/null",
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_AUTHOR_NAME=" + simGitName,
		"GIT_AUTHOR_EMAIL=" + simGitEmail,
		"GIT_COMMITTER_NAME=" + simGitName,
		"GIT_COMMITTER_EMAIL=" + simGitEmail,
		"HOME=/dev/null", // belt and braces: nothing should read $HOME
		"PATH=" + os.Getenv("PATH"),
	}
}

// simGit runs git with a hermetic environment plus extraEnv, returning
// stdout. stdin may be nil.
func simGit(dir string, stdin []byte, extraEnv []string, args ...string) (string, error) {
	assertf(len(args) > 0, "simGit: no args")
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(gitBaseEnv(), extraEnv...)
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return string(out), fmt.Errorf("git %s: %w: %s",
			strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return string(out), nil
}

// initScratchRepo creates the bare object-factory repo the simulation mints
// commits in. gc is disabled so minted objects stay loose and addressable.
func initScratchRepo(path string) error {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return err
	}
	if _, err := simGit("", nil, nil, "init", "--bare", "--initial-branch=main", path); err != nil {
		return err
	}
	_, err := simGit(path, nil, nil, "config", "gc.auto", "0")
	return err
}

// mintedCommit is one deterministic commit plus the exact objects a push of
// it must carry (the pack is self-contained modulo already-pushed parents).
type mintedCommit struct {
	blobSHA   string
	treeSHA   string
	commitSHA string
}

// mintCommit writes blob+tree+commit into the scratch repo. parent may be
// empty (root commit). step drives both content and timestamps, so the
// resulting SHA is a pure function of (content, parent, step).
func mintCommit(scratch, parent, content string, step int) (mintedCommit, error) {
	assertf(content != "", "mintCommit: empty content")
	assertf(step >= 0, "mintCommit: negative step %d", step)
	var mc mintedCommit

	out, err := simGit(scratch, []byte(content), nil, "hash-object", "-w", "--stdin")
	if err != nil {
		return mc, err
	}
	mc.blobSHA = strings.TrimSpace(out)

	treeSpec := fmt.Sprintf("100644 blob %s\tdata.txt\n", mc.blobSHA)
	out, err = simGit(scratch, []byte(treeSpec), nil, "mktree")
	if err != nil {
		return mc, err
	}
	mc.treeSHA = strings.TrimSpace(out)

	date := fmt.Sprintf("@%d +0000", simEpoch+step)
	env := []string{"GIT_AUTHOR_DATE=" + date, "GIT_COMMITTER_DATE=" + date}
	args := []string{"commit-tree", mc.treeSHA, "-m", fmt.Sprintf("dst step %d", step)}
	if parent != "" {
		args = append(args, "-p", parent)
	}
	out, err = simGit(scratch, nil, env, args...)
	if err != nil {
		return mc, err
	}
	mc.commitSHA = strings.TrimSpace(out)
	return mc, nil
}

// packObjects writes a self-contained pack holding exactly objs into dir and
// returns its path. Single-threaded so the pack bytes are a deterministic
// function of the objects.
func packObjects(scratch, dir string, objs []string) (string, error) {
	assertf(len(objs) > 0, "packObjects: no objects")
	stdin := strings.Join(objs, "\n") + "\n"
	prefix := filepath.Join(dir, "sim")
	out, err := simGit(scratch, []byte(stdin), nil,
		"pack-objects", "--threads=1", "-q", prefix)
	if err != nil {
		return "", err
	}
	hash := strings.TrimSpace(out)
	assertf(hash != "", "pack-objects emitted no pack hash")
	packPath := prefix + "-" + hash + ".pack"
	if _, err := os.Stat(packPath); err != nil {
		return "", fmt.Errorf("pack-objects output missing: %w", err)
	}
	return packPath, nil
}

// forEachRef returns repoPath's full ref map (name -> sha) using git
// for-each-ref, for comparison against the ref authority.
func forEachRef(repoPath string) (map[string]string, error) {
	out, err := simGit(repoPath, nil, nil, "for-each-ref", "--format=%(refname) %(objectname)")
	if err != nil {
		return nil, err
	}
	refs := make(map[string]string)
	for line := range strings.SplitSeq(strings.TrimSpace(out), "\n") {
		if line == "" {
			continue
		}
		name, sha, ok := strings.Cut(line, " ")
		assertf(ok, "for-each-ref emitted %q", line)
		refs[name] = sha
	}
	return refs, nil
}
