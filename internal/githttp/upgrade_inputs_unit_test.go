package githttp

import (
	"archive/tar"
	"bytes"
	"cmp"
	"compress/gzip"
	"context"
	"encoding/json/v2"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
	"testing"
	"time"
)

// upgradeFixture is a self-built stand-in for the real upgrade inputs: a
// clean git checkout of a tiny github.com/coder/openstorage module, a
// candidate built from it with -buildvcs=true (never executed), and a fake
// archive + checksum file whose digests are pinned through a fake release
// pin. Every test drives the helper API; nothing is downloaded.
type upgradeFixture struct {
	pin           upgradeReleasePin
	cfg           upgradeInputConfig
	oldBinaryBody []byte
}

func newUpgradeFixture(t *testing.T) *upgradeFixture {
	t.Helper()
	src := t.TempDir()
	upgWrite(t, filepath.Join(src, "go.mod"), "module github.com/coder/openstorage\n\ngo 1.21\n")
	upgWrite(t, filepath.Join(src, "cmd/openstorage/main.go"), "package main\n\nfunc main() {}\n")
	upgGit(t, src, "init", "-q", "-b", "main")
	upgGit(t, src, "add", ".")
	upgGit(t, src, "commit", "-q", "-m", "fixture")
	artifacts := t.TempDir()
	f := &upgradeFixture{cfg: upgradeInputConfig{
		Archive:   filepath.Join(artifacts, "archive.tar.gz"),
		Checksums: filepath.Join(artifacts, "checksums.txt"),
		Candidate: buildUpgradeCandidate(t, src),
		Commit:    upgGit(t, src, "rev-parse", "HEAD"),
		Tree:      upgGit(t, src, "rev-parse", "HEAD^{tree}"),
		SourceDir: src,
	}}
	f.cfg.CandidateSHA256 = upgSHA256(t, f.cfg.Candidate)
	// The fixture "old" release binary is the same VCS-stamped build, so the
	// old-binary identity checks see a real build info record.
	body, err := os.ReadFile(f.cfg.Candidate)
	if err != nil {
		t.Fatal(err)
	}
	f.oldBinaryBody = body
	f.pin = upgradeReleasePin{OldTag: "v0.0.0-fixture", OldCommit: f.cfg.Commit, OldMainPath: upgradeCandidateMainPath, ArchiveName: upgradeArchiveName}
	// The released layout: the binary, then LICENSE (skipped).
	f.setArchive(t, tarEntry{Name: "openstorage", Body: f.oldBinaryBody}, tarEntry{Name: "LICENSE", Body: []byte("license")})
	return f
}

type tarEntry struct {
	Name, Link string
	Body       []byte
	Type       byte
}

// setArchive writes a tar.gz with the given entries and a matching GNU
// sha256sum checksum file, then re-pins both digests.
func (f *upgradeFixture) setArchive(t *testing.T, entries ...tarEntry) {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, e := range entries {
		hdr := &tar.Header{Name: e.Name, Typeflag: cmp.Or(e.Type, byte(tar.TypeReg)), Mode: 0o755, Size: int64(len(e.Body)), Linkname: e.Link}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(e.Body); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	upgWrite(t, f.cfg.Archive, buf.String())
	f.pin.ArchiveSHA256 = upgSHA256(t, f.cfg.Archive)
	f.setChecksums(t, f.pin.ArchiveSHA256+"  "+upgradeArchiveName+"\n")
}

func (f *upgradeFixture) setChecksums(t *testing.T, body string) {
	t.Helper()
	upgWrite(t, f.cfg.Checksums, body)
	f.pin.ChecksumsSHA256 = upgSHA256(t, f.cfg.Checksums)
}

func (f *upgradeFixture) attempt(t *testing.T) string {
	t.Helper()
	dest := filepath.Join(t.TempDir(), "attempt")
	if err := os.Mkdir(dest, 0o700); err != nil {
		t.Fatal(err)
	}
	return dest
}

// mustReject asserts a failed preparation left dest intact (only the named
// frozen inputs may exist) and never produced a receipt or old binary.
func (f *upgradeFixture) mustReject(t *testing.T, cfg upgradeInputConfig, want string, frozen ...string) {
	t.Helper()
	dest := f.attempt(t)
	_, err := prepareUpgradeInputsPinned(f.pin, cfg, dest)
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("error %v, want substring %q", err, want)
	}
	entries, readErr := os.ReadDir(dest)
	if readErr != nil {
		t.Fatal(readErr)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if !slices.Equal(names, frozen) {
		t.Fatalf("dest contents after failure %v, want %v", names, frozen)
	}
}

func TestPrepareUpgradeInputs_RejectsInvalidConfig(t *testing.T) {
	f := newUpgradeFixture(t)
	missing := filepath.Join(t.TempDir(), "missing")
	for name, tc := range map[string]struct {
		mutate func(*upgradeInputConfig)
		want   string
	}{
		"empty archive":      {func(c *upgradeInputConfig) { c.Archive = "" }, "Archive"},
		"missing checksums":  {func(c *upgradeInputConfig) { c.Checksums = missing }, "Checksums"},
		"candidate is a dir": {func(c *upgradeInputConfig) { c.Candidate = t.TempDir() }, "Candidate"},
		"empty hash":         {func(c *upgradeInputConfig) { c.CandidateSHA256 = "" }, "CandidateSHA256"},
		"short hash":         {func(c *upgradeInputConfig) { c.CandidateSHA256 = c.CandidateSHA256[1:] }, "CandidateSHA256"},
		"upper hash":         {func(c *upgradeInputConfig) { c.CandidateSHA256 = strings.ToUpper(c.CandidateSHA256) }, "CandidateSHA256"},
		"39-hex commit":      {func(c *upgradeInputConfig) { c.Commit = c.Commit[:39] }, "Commit"},
		"non-hex tree":       {func(c *upgradeInputConfig) { c.Tree = strings.Repeat("g", 40) }, "Tree"},
		"source is a file":   {func(c *upgradeInputConfig) { c.SourceDir = c.Archive }, "SourceDir"},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := f.cfg
			tc.mutate(&cfg)
			f.mustReject(t, cfg, tc.want)
		})
	}
	t.Run("dest not private", func(t *testing.T) {
		dest := filepath.Join(t.TempDir(), "shared")
		if err := os.Mkdir(dest, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(dest, 0o755); err != nil { // Mkdir alone is subject to the umask.
			t.Fatal(err)
		}
		if _, err := prepareUpgradeInputsPinned(f.pin, f.cfg, dest); err == nil || !strings.Contains(err.Error(), "private") {
			t.Fatalf("error %v, want private-dir rejection", err)
		}
	})
}

func TestPrepareUpgradeInputs_RejectsSourceDrift(t *testing.T) {
	f := newUpgradeFixture(t)
	other := strings.Repeat("ab", 20)
	t.Run("wrong commit", func(t *testing.T) {
		cfg := f.cfg
		cfg.Commit = other
		f.mustReject(t, cfg, "HEAD")
	})
	t.Run("wrong tree", func(t *testing.T) {
		cfg := f.cfg
		cfg.Tree = other
		f.mustReject(t, cfg, "tree")
	})
	t.Run("untracked file", func(t *testing.T) {
		scratch := filepath.Join(f.cfg.SourceDir, "scratch.txt")
		upgWrite(t, scratch, "dirty\n")
		f.mustReject(t, f.cfg, "status")
		if err := os.Remove(scratch); err != nil {
			t.Fatal(err)
		}
	})
}

func TestPrepareUpgradeInputs_RejectsCandidate(t *testing.T) {
	f := newUpgradeFixture(t)
	frozen := []string{"candidate", "checksums.txt", upgradeArchiveName}
	t.Run("hash mismatch", func(t *testing.T) {
		cfg := f.cfg
		cfg.CandidateSHA256 = strings.Repeat("0", 64)
		f.mustReject(t, cfg, "candidate sha256", frozen...)
	})
	t.Run("no build info", func(t *testing.T) {
		cfg := f.cfg
		cfg.Candidate = f.cfg.Checksums // a real file that is not a Go binary
		cfg.CandidateSHA256 = f.pin.ChecksumsSHA256
		f.mustReject(t, cfg, "build info", frozen...)
	})
	t.Run("dirty build", func(t *testing.T) {
		scratch := filepath.Join(f.cfg.SourceDir, "scratch.txt")
		upgWrite(t, scratch, "dirty\n")
		cfg := f.cfg
		cfg.Candidate = buildUpgradeCandidate(t, f.cfg.SourceDir)
		if err := os.Remove(scratch); err != nil {
			t.Fatal(err)
		}
		cfg.CandidateSHA256 = upgSHA256(t, cfg.Candidate)
		f.mustReject(t, cfg, "vcs.modified", frozen...)
	})
	t.Run("stale build", func(t *testing.T) {
		// HEAD moves after the candidate was built: the source checkout is
		// clean and matches cfg, but the embedded revision is the old one.
		upgWrite(t, filepath.Join(f.cfg.SourceDir, "cmd/openstorage/doc.go"), "package main\n")
		upgGit(t, f.cfg.SourceDir, "add", ".")
		upgGit(t, f.cfg.SourceDir, "commit", "-q", "-m", "second")
		cfg := f.cfg
		cfg.Commit = upgGit(t, f.cfg.SourceDir, "rev-parse", "HEAD")
		cfg.Tree = upgGit(t, f.cfg.SourceDir, "rev-parse", "HEAD^{tree}")
		f.mustReject(t, cfg, "vcs.revision", frozen...)
	})
}

func TestVerifyUpgradeBuildInfo(t *testing.T) {
	commit := strings.Repeat("ab", 20)
	// Settings are key/value pairs; a later duplicate key overrides (the
	// verifier reads the last value, as debug.BuildInfo lookups do).
	build := func(path string, kv ...string) *debug.BuildInfo {
		bi := &debug.BuildInfo{Path: path}
		for i := 0; i < len(kv); i += 2 {
			bi.Settings = append(bi.Settings, debug.BuildSetting{Key: kv[i], Value: kv[i+1]})
		}
		return bi
	}
	base := []string{"GOOS", "linux", "GOARCH", "amd64", "vcs", "git", "vcs.revision", commit, "vcs.modified", "false"}
	if err := verifyUpgradeBuildInfo(build(upgradeCandidateMainPath, base...), upgradeCandidateMainPath, commit); err != nil {
		t.Fatalf("valid build info rejected: %v", err)
	}
	for name, tc := range map[string]struct {
		bi   *debug.BuildInfo
		want string
	}{
		"wrong main path":    {build("github.com/coder/openstorage/cmd/other", base...), "main path"},
		"darwin":             {build(upgradeCandidateMainPath, append(base, "GOOS", "darwin")...), "GOOS"},
		"arm64":              {build(upgradeCandidateMainPath, append(base, "GOARCH", "arm64")...), "GOARCH"},
		"not git":            {build(upgradeCandidateMainPath, append(base, "vcs", "hg")...), "vcs="},
		"malformed revision": {build(upgradeCandidateMainPath, append(base, "vcs.revision", commit[:39])...), "vcs.revision"},
		"wrong revision":     {build(upgradeCandidateMainPath, append(base, "vcs.revision", strings.Repeat("cd", 20))...), "vcs.revision"},
		"modified":           {build(upgradeCandidateMainPath, append(base, "vcs.modified", "true")...), "vcs.modified"},
		"no revision":        {build(upgradeCandidateMainPath, base[:6]...), "vcs.revision"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := verifyUpgradeBuildInfo(tc.bi, upgradeCandidateMainPath, commit); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestPrepareUpgradeInputs_RejectsOfficialArtifacts(t *testing.T) {
	f := newUpgradeFixture(t)
	frozen := []string{"candidate", "checksums.txt", upgradeArchiveName}
	goodLine := f.pin.ArchiveSHA256 + "  " + upgradeArchiveName + "\n"
	zeros := strings.Repeat("0", 64)
	for name, tc := range map[string]struct{ body, want string }{
		"checksums lacks archive line":      {f.pin.ArchiveSHA256 + "  openstorage_0.18.0_linux_arm64.tar.gz\n", "no entry"},
		"checksums line disagrees with pin": {zeros + "  " + upgradeArchiveName + "\n", "lists"},
		"checksums line malformed":          {"not a digest  " + upgradeArchiveName + "\n", "malformed"},
		"checksums lists archive twice":     {goodLine + goodLine, "twice"},
	} {
		t.Run(name, func(t *testing.T) {
			f.setChecksums(t, tc.body)
			defer f.setChecksums(t, goodLine)
			f.mustReject(t, f.cfg, tc.want, frozen...)
		})
	}
	t.Run("checksums bytes drift", func(t *testing.T) {
		upgWrite(t, f.cfg.Checksums, goodLine+"# tampered\n") // pin unchanged
		defer f.setChecksums(t, goodLine)
		f.mustReject(t, f.cfg, "checksums.txt sha256", frozen...)
	})
	t.Run("archive digest", func(t *testing.T) {
		pin := f.pin
		defer func() { f.pin = pin }()
		f.pin.ArchiveSHA256 = zeros
		f.setChecksums(t, zeros+"  "+upgradeArchiveName+"\n")
		f.mustReject(t, f.cfg, "archive sha256", frozen...)
	})
	for name, tc := range map[string]struct {
		entries []tarEntry
		want    string
	}{
		"symlink":     {[]tarEntry{{Name: "openstorage", Link: "/bin/sh", Type: tar.TypeSymlink}}, "regular file"},
		"nested path": {[]tarEntry{{Name: "bin/openstorage", Body: f.oldBinaryBody}}, "entry name"},
		"directory":   {[]tarEntry{{Name: "openstorage", Type: tar.TypeDir}}, "regular file"},
		"empty":       {nil, "no openstorage entry"},
	} {
		t.Run(name, func(t *testing.T) {
			f.setArchive(t, tc.entries...)
			defer f.setArchive(t, tarEntry{Name: "openstorage", Body: f.oldBinaryBody})
			f.mustReject(t, f.cfg, tc.want, frozen...)
		})
	}
	// The first entry is extracted before the archive is known to hold a
	// second one or a foreign binary, so the frozen old binary is preserved.
	withOld := append(frozen[:2:2], "openstorage-v0.0.0-fixture", upgradeArchiveName)
	t.Run("extra entry", func(t *testing.T) {
		for _, extra := range [][]tarEntry{
			{{Name: "README", Body: []byte("x")}},
			{{Name: "LICENSE", Link: "/etc/passwd", Type: tar.TypeSymlink}},
			{{Name: "LICENSE", Body: []byte("x")}, {Name: "LICENSE", Body: []byte("x")}},
		} {
			f.setArchive(t, append([]tarEntry{{Name: "openstorage", Body: f.oldBinaryBody}}, extra...)...)
			f.mustReject(t, f.cfg, "extra entry", withOld...)
		}
		f.setArchive(t, tarEntry{Name: "openstorage", Body: f.oldBinaryBody})
	})
	t.Run("old binary is not the pinned build", func(t *testing.T) {
		f.setArchive(t, tarEntry{Name: "openstorage", Body: []byte("#!/bin/false\n")})
		defer f.setArchive(t, tarEntry{Name: "openstorage", Body: f.oldBinaryBody})
		f.mustReject(t, f.cfg, "old binary", withOld...)
	})
	t.Run("official pin never matches a fixture", func(t *testing.T) {
		dest := f.attempt(t)
		if _, err := prepareUpgradeInputs(f.cfg, dest); err == nil || !strings.Contains(err.Error(), upgradeChecksumsSHA256) {
			t.Fatalf("error %v, want rejection against the pinned checksums digest", err)
		}
	})
}

func TestPrepareUpgradeInputs_FreezesAndRecords(t *testing.T) {
	f := newUpgradeFixture(t)
	dest := f.attempt(t)
	got, err := prepareUpgradeInputsPinned(f.pin, f.cfg, dest)
	if err != nil {
		t.Fatal(err)
	}
	// The fixture archive entry is the candidate's bytes, so both digests agree.
	want := upgradeInputs{
		OldBinary: filepath.Join(dest, "openstorage-v0.0.0-fixture"), CandidateBinary: filepath.Join(dest, "candidate"),
		OldTag: f.pin.OldTag, OldCommit: f.pin.OldCommit, OldBinarySHA256: f.cfg.CandidateSHA256,
		ArchiveSHA256: f.pin.ArchiveSHA256, ChecksumsSHA256: f.pin.ChecksumsSHA256,
		CandidateSHA256: f.cfg.CandidateSHA256, Commit: f.cfg.Commit, Tree: f.cfg.Tree,
	}
	if got != want {
		t.Fatalf("identities\n got %+v\nwant %+v", got, want)
	}
	for _, p := range []string{got.OldBinary, got.CandidateBinary} {
		st, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if st.Mode().Perm() != 0o500 || upgSHA256(t, p) != f.cfg.CandidateSHA256 {
			t.Fatalf("%s: mode %o, want a frozen 0500 copy of the fixture binary", p, st.Mode().Perm())
		}
	}
	raw, err := os.ReadFile(filepath.Join(dest, "inputs.json"))
	if err != nil {
		t.Fatal(err)
	}
	var receipt upgradeInputs
	if err := json.Unmarshal(raw, &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt != got {
		t.Fatalf("receipt %+v != returned %+v", receipt, got)
	}
	// No overwrites and no silent retries: the frozen inputs already exist.
	if _, err := prepareUpgradeInputsPinned(f.pin, f.cfg, dest); err == nil || !strings.Contains(err.Error(), "exist") {
		t.Fatalf("second attempt in the same dest: %v, want exclusive-create failure", err)
	}
	// Pre-invocation recheck: passes on frozen bytes, fails once they change.
	if err := got.recheck(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(got.CandidateBinary, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(got.CandidateBinary, []byte("tampered"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := got.recheck(); err == nil || !strings.Contains(err.Error(), "candidate sha256") {
		t.Fatalf("recheck after tampering: %v, want candidate digest mismatch", err)
	}
}

// buildUpgradeCandidate builds the fixture module's ./cmd/openstorage with
// VCS stamping. The result is inspected through debug/buildinfo only.
func buildUpgradeCandidate(t *testing.T, src string) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "candidate")
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "build", "-buildvcs=true", "-trimpath", "-o", out, "./cmd/openstorage")
	cmd.Dir = src
	cmd.Env = append(upgGitEnv(), "GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=0", "GOFLAGS=", "GOWORK=off", "GOTOOLCHAIN=local")
	if raw, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build fixture: %v\n%s", err, raw)
	}
	return out
}

// upgGitEnv is a hermetic environment for fixture git (and go build -buildvcs):
// no inherited GIT_*, no user/system config, fixed identity and dates.
func upgGitEnv() []string {
	return append(slices.DeleteFunc(os.Environ(), func(kv string) bool { return strings.HasPrefix(kv, "GIT_") }),
		"GIT_CONFIG_GLOBAL="+os.DevNull, "GIT_CONFIG_SYSTEM="+os.DevNull, "GIT_CONFIG_NOSYSTEM=1",
		"GIT_AUTHOR_NAME=githttp", "GIT_AUTHOR_EMAIL=githttp@test.invalid", "GIT_AUTHOR_DATE=2000-01-01T00:00:00Z",
		"GIT_COMMITTER_NAME=githttp", "GIT_COMMITTER_EMAIL=githttp@test.invalid", "GIT_COMMITTER_DATE=2000-01-01T00:00:00Z")
}

func upgGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "git", args...)
	cmd.Dir, cmd.Env = dir, upgGitEnv()
	raw, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, raw)
	}
	return strings.TrimSpace(string(raw))
}

func upgWrite(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func upgSHA256(t *testing.T, path string) string {
	t.Helper()
	sum, err := upgradeSHA256(path)
	if err != nil {
		t.Fatal(err)
	}
	return sum
}
