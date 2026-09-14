package githttp

// Test-only input identity helpers for the release-candidate upgrade lane
// (issue #267). prepareUpgradeInputs freezes the official v0.15.0
// linux/amd64 release archive, its checksum file, and the locally built
// candidate binary into an exclusively owned attempt directory, verifies
// every identity against the pinned digests, the embedded VCS build info,
// and the clean source checkout, then records a receipt (inputs.json).
// It never downloads anything and never executes either binary; the
// dedicated upgrade TestMain owns downloads and process lifecycle, and on
// failure the attempt directory is left exactly as it was for inspection.

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
)

// Official v0.15.0 release identities, verified against the primary GitHub
// release assets: checksums.txt is GNU sha256sum output, the archive holds
// exactly one entry (openstorage), and that binary is stamped with the tag
// commit under the pre-rename module path.
const (
	upgradeOldTag            = "v0.15.0"
	upgradeOldCommit         = "d65da6e53ee62145a0cb78a0f89bf6839c8f9171"
	upgradeOldMainPath       = "github.com/coder/open-storage/cmd/openstorage"
	upgradeArchiveName       = "openstorage_0.15.0_linux_amd64.tar.gz"
	upgradeArchiveSHA256     = "b9465088ebf371f2dcd9bed2c3012ea81fd687d39611aff3c1dfdd6ace842611"
	upgradeChecksumsSHA256   = "3aea91fdc66009fbf6e67ea00405cfa88869cd8a69fe320aab52f953937c3175"
	upgradeCandidateMainPath = "github.com/coder/openstorage/cmd/openstorage"
)

// upgradeReleasePin is the expected identity of the old release; the
// official pin is the only one used outside the helper's own tests.
type upgradeReleasePin struct {
	OldTag, OldCommit, OldMainPath, ArchiveName, ArchiveSHA256, ChecksumsSHA256 string
}

var officialUpgradeRelease = upgradeReleasePin{
	OldTag: upgradeOldTag, OldCommit: upgradeOldCommit, OldMainPath: upgradeOldMainPath,
	ArchiveName: upgradeArchiveName, ArchiveSHA256: upgradeArchiveSHA256, ChecksumsSHA256: upgradeChecksumsSHA256,
}

// upgradeInputConfig names the caller-provided inputs: the downloaded
// archive and checksum file, the candidate built from SourceDir, its
// expected sha256, and the commit/tree SourceDir must currently be at.
type upgradeInputConfig struct {
	Archive, Checksums, Candidate, CandidateSHA256, Commit, Tree, SourceDir string
}

// upgradeInputs is the verified-identity receipt, also written to
// dest/inputs.json. Both binaries are frozen 0500 copies inside dest.
type upgradeInputs struct {
	OldBinary       string `json:"old_binary"`
	CandidateBinary string `json:"candidate_binary"`
	OldTag          string `json:"old_tag"`
	OldCommit       string `json:"old_commit"`
	OldBinarySHA256 string `json:"old_binary_sha256"`
	ArchiveSHA256   string `json:"archive_sha256"`
	ChecksumsSHA256 string `json:"checksums_sha256"`
	CandidateSHA256 string `json:"candidate_sha256"`
	Commit          string `json:"commit"`
	Tree            string `json:"tree"`
}

func prepareUpgradeInputs(cfg upgradeInputConfig, dest string) (upgradeInputs, error) {
	return prepareUpgradeInputsPinned(officialUpgradeRelease, cfg, dest)
}

// prepareUpgradeInputsPinned freezes and verifies cfg against pin. dest
// must be an existing private directory owned by this attempt; every
// output is created exclusively, so a reused dest fails instead of
// overwriting or retrying, and nothing is removed on failure.
func prepareUpgradeInputsPinned(pin upgradeReleasePin, cfg upgradeInputConfig, dest string) (upgradeInputs, error) {
	var out upgradeInputs
	if err := cfg.validate(); err != nil {
		return out, err
	}
	if st, err := os.Stat(dest); err != nil || !st.IsDir() || st.Mode().Perm()&0o077 != 0 {
		return out, fmt.Errorf("upgrade inputs: dest %q must be an existing private (0700) directory: %v", dest, err)
	}
	if err := verifyUpgradeSource(cfg.SourceDir, cfg.Commit, cfg.Tree); err != nil {
		return out, err
	}
	// Freeze first and verify only the frozen bytes: the caller's paths are
	// not ours and may change underneath us.
	candidate := filepath.Join(dest, "candidate")
	checksums := filepath.Join(dest, "checksums.txt")
	archive := filepath.Join(dest, pin.ArchiveName)
	for _, c := range []struct {
		src, dst string
		mode     os.FileMode
	}{{cfg.Candidate, candidate, 0o500}, {cfg.Checksums, checksums, 0o400}, {cfg.Archive, archive, 0o400}} {
		if err := freezeUpgradeFile(c.src, c.dst, c.mode); err != nil {
			return out, err
		}
	}
	candidateSum, err := upgradeSHA256(candidate)
	if err != nil {
		return out, err
	}
	if candidateSum != cfg.CandidateSHA256 {
		return out, fmt.Errorf("upgrade inputs: candidate sha256 %s, want %s", candidateSum, cfg.CandidateSHA256)
	}
	if err := verifyUpgradeBinary(candidate, upgradeCandidateMainPath, cfg.Commit); err != nil {
		return out, fmt.Errorf("upgrade inputs: candidate: %w", err)
	}
	checksumsSum, err := upgradeSHA256(checksums)
	if err != nil {
		return out, err
	}
	if checksumsSum != pin.ChecksumsSHA256 {
		return out, fmt.Errorf("upgrade inputs: checksums.txt sha256 %s, want %s", checksumsSum, pin.ChecksumsSHA256)
	}
	listed, err := upgradeListedDigest(checksums, pin.ArchiveName)
	if err != nil {
		return out, err
	}
	if listed != pin.ArchiveSHA256 {
		return out, fmt.Errorf("upgrade inputs: checksums.txt lists %s for %s, want pinned %s", listed, pin.ArchiveName, pin.ArchiveSHA256)
	}
	archiveSum, err := upgradeSHA256(archive)
	if err != nil {
		return out, err
	}
	if archiveSum != pin.ArchiveSHA256 {
		return out, fmt.Errorf("upgrade inputs: archive sha256 %s, want %s", archiveSum, pin.ArchiveSHA256)
	}
	oldBinary := filepath.Join(dest, "openstorage-"+pin.OldTag)
	if err := extractUpgradeBinary(archive, oldBinary); err != nil {
		return out, err
	}
	oldSum, err := upgradeSHA256(oldBinary)
	if err != nil {
		return out, err
	}
	if err := verifyUpgradeBinary(oldBinary, pin.OldMainPath, pin.OldCommit); err != nil {
		return out, fmt.Errorf("upgrade inputs: old binary: %w", err)
	}
	out = upgradeInputs{
		OldBinary: oldBinary, CandidateBinary: candidate, OldTag: pin.OldTag, OldCommit: pin.OldCommit,
		OldBinarySHA256: oldSum, ArchiveSHA256: archiveSum, ChecksumsSHA256: checksumsSum,
		CandidateSHA256: candidateSum, Commit: cfg.Commit, Tree: cfg.Tree,
	}
	receipt, err := json.Marshal(out)
	if err != nil {
		return upgradeInputs{}, fmt.Errorf("upgrade inputs: receipt: %w", err)
	}
	if err := writeUpgradeFile(filepath.Join(dest, "inputs.json"), 0o400, bytes.NewReader(append(receipt, '\n'))); err != nil {
		return upgradeInputs{}, err
	}
	return out, nil
}

func (cfg upgradeInputConfig) validate() error {
	for _, f := range []struct {
		name, path string
		dir        bool
	}{{"Archive", cfg.Archive, false}, {"Checksums", cfg.Checksums, false}, {"Candidate", cfg.Candidate, false}, {"SourceDir", cfg.SourceDir, true}} {
		kind := "regular file"
		if f.dir {
			kind = "directory"
		}
		st, err := os.Stat(f.path)
		if f.path == "" || err != nil || (f.dir && !st.IsDir()) || (!f.dir && !st.Mode().IsRegular()) {
			return fmt.Errorf("upgrade inputs: %s %q must be an existing %s: %v", f.name, f.path, kind, err)
		}
	}
	for _, h := range []struct {
		name, value string
		n           int
	}{{"CandidateSHA256", cfg.CandidateSHA256, 64}, {"Commit", cfg.Commit, 40}, {"Tree", cfg.Tree, 40}} {
		if !isLowerHex(h.value, h.n) {
			return fmt.Errorf("upgrade inputs: %s %q must be %d lowercase hex digits", h.name, h.value, h.n)
		}
	}
	return nil
}

func isLowerHex(s string, n int) bool {
	return len(s) == n && strings.Trim(s, "0123456789abcdef") == ""
}

// verifyUpgradeSource requires dir to be a clean checkout (untracked files
// included) of exactly commit/tree. Git runs hermetically: inherited GIT_*
// variables and user/system config cannot redirect or relax the check.
func verifyUpgradeSource(dir, commit, tree string) error {
	git := func(args ...string) (string, error) {
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		cmd.Env = append(slices.DeleteFunc(os.Environ(), func(kv string) bool { return strings.HasPrefix(kv, "GIT_") }),
			"GIT_CONFIG_GLOBAL="+os.DevNull, "GIT_CONFIG_NOSYSTEM=1")
		raw, err := cmd.CombinedOutput()
		if err != nil {
			return "", fmt.Errorf("upgrade inputs: git %v in %s: %w\n%s", args, dir, err, raw)
		}
		return strings.TrimSpace(string(raw)), nil
	}
	head, err := git("rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return err
	}
	if head != commit {
		return fmt.Errorf("upgrade inputs: %s HEAD is %s, want %s", dir, head, commit)
	}
	headTree, err := git("rev-parse", "--verify", "HEAD^{tree}")
	if err != nil {
		return err
	}
	if headTree != tree {
		return fmt.Errorf("upgrade inputs: %s HEAD tree is %s, want %s", dir, headTree, tree)
	}
	status, err := git("status", "--porcelain", "--untracked-files=all")
	if err != nil {
		return err
	}
	if status != "" {
		return fmt.Errorf("upgrade inputs: %s is not clean:\n%s", dir, status)
	}
	return nil
}

func freezeUpgradeFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("upgrade inputs: %w", err)
	}
	defer in.Close()
	return writeUpgradeFile(dst, mode, in)
}

// writeUpgradeFile creates dst exclusively (an earlier attempt's output is
// never overwritten) with exactly mode, umask notwithstanding.
func writeUpgradeFile(dst string, mode os.FileMode, r io.Reader) error {
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return fmt.Errorf("upgrade inputs: %w", err)
	}
	_, copyErr := io.Copy(out, r)
	if err := errors.Join(copyErr, out.Chmod(mode), out.Sync(), out.Close()); err != nil {
		return fmt.Errorf("upgrade inputs: write %s: %w", dst, err)
	}
	return nil
}

func upgradeSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("upgrade inputs: %w", err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("upgrade inputs: hash %s: %w", path, err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// upgradeListedDigest returns the digest a GNU sha256sum file lists for
// name, rejecting malformed lines and duplicate entries.
func upgradeListedDigest(checksums, name string) (string, error) {
	raw, err := os.ReadFile(checksums)
	if err != nil {
		return "", fmt.Errorf("upgrade inputs: %w", err)
	}
	found := ""
	for line := range strings.Lines(string(raw)) {
		digest, file, ok := strings.Cut(strings.TrimRight(line, "\r\n"), "  ")
		if !ok || !isLowerHex(digest, 64) || file == "" {
			return "", fmt.Errorf("upgrade inputs: malformed checksums.txt line %q", line)
		}
		if file == name {
			if found != "" {
				return "", fmt.Errorf("upgrade inputs: checksums.txt lists %s twice", name)
			}
			found = digest
		}
	}
	if found == "" {
		return "", fmt.Errorf("upgrade inputs: checksums.txt has no entry for %s", name)
	}
	return found, nil
}

// extractUpgradeBinary writes the archive's single regular entry, named
// exactly openstorage, to dst (0500). Paths, links, and extra entries fail.
func extractUpgradeBinary(archive, dst string) error {
	f, err := os.Open(archive)
	if err != nil {
		return fmt.Errorf("upgrade inputs: %w", err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("upgrade inputs: archive %s: %w", archive, err)
	}
	tr := tar.NewReader(gz)
	hdr, err := tr.Next()
	if errors.Is(err, io.EOF) {
		return fmt.Errorf("upgrade inputs: archive %s has no openstorage entry", archive)
	}
	if err != nil {
		return fmt.Errorf("upgrade inputs: archive %s: %w", archive, err)
	}
	if hdr.Name != "openstorage" {
		return fmt.Errorf("upgrade inputs: archive entry name %q, want openstorage", hdr.Name)
	}
	if hdr.Typeflag != tar.TypeReg {
		return fmt.Errorf("upgrade inputs: archive entry openstorage is not a regular file (type %q)", hdr.Typeflag)
	}
	if err := writeUpgradeFile(dst, 0o500, tr); err != nil {
		return err
	}
	if next, err := tr.Next(); err == nil {
		return fmt.Errorf("upgrade inputs: archive has an extra entry %q after openstorage", next.Name)
	} else if !errors.Is(err, io.EOF) {
		return fmt.Errorf("upgrade inputs: archive %s: %w", archive, err)
	}
	return nil
}

func verifyUpgradeBinary(path, mainPath, commit string) error {
	bi, err := buildinfo.ReadFile(path)
	if err != nil {
		return fmt.Errorf("%s has no Go build info: %w", path, err)
	}
	return verifyUpgradeBuildInfo(bi, mainPath, commit)
}

// verifyUpgradeBuildInfo binds a binary to a clean linux/amd64 git build of
// mainPath at commit through its embedded build settings.
func verifyUpgradeBuildInfo(bi *debug.BuildInfo, mainPath, commit string) error {
	if bi.Path != mainPath {
		return fmt.Errorf("main path %q, want %q", bi.Path, mainPath)
	}
	settings := make(map[string]string, len(bi.Settings))
	for _, s := range bi.Settings {
		settings[s.Key] = s.Value
	}
	for _, want := range [...]debug.BuildSetting{
		{Key: "GOOS", Value: "linux"}, {Key: "GOARCH", Value: "amd64"}, {Key: "vcs", Value: "git"},
		{Key: "vcs.revision", Value: commit}, {Key: "vcs.modified", Value: "false"},
	} {
		if got := settings[want.Key]; got != want.Value {
			return fmt.Errorf("build setting %s=%q, want %q", want.Key, got, want.Value)
		}
	}
	return nil
}
