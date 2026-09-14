package githttp

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
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
	isolateUpgradeGit(t)
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
	f.setArchive(t, tarEntry{Name: "openstorage", Body: f.oldBinaryBody})
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
		typ := e.Type
		if typ == 0 {
			typ = tar.TypeReg
		}
		hdr := &tar.Header{Name: e.Name, Typeflag: typ, Mode: 0o755, Size: int64(len(e.Body)), Linkname: e.Link}
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
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if strings.Join(names, ",") != strings.Join(frozen, ",") {
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
		f.mustReject(t, f.cfg, "not clean")
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
	valid := func() *debug.BuildInfo {
		return &debug.BuildInfo{Path: upgradeCandidateMainPath, Settings: []debug.BuildSetting{
			{Key: "GOOS", Value: "linux"}, {Key: "GOARCH", Value: "amd64"}, {Key: "vcs", Value: "git"},
			{Key: "vcs.revision", Value: commit}, {Key: "vcs.modified", Value: "false"},
		}}
	}
	if err := verifyUpgradeBuildInfo(valid(), upgradeCandidateMainPath, commit); err != nil {
		t.Fatalf("valid build info rejected: %v", err)
	}
	set := func(key, value string) func(*debug.BuildInfo) {
		return func(bi *debug.BuildInfo) {
			for i := range bi.Settings {
				if bi.Settings[i].Key == key {
					bi.Settings[i].Value = value
				}
			}
		}
	}
	for name, tc := range map[string]struct {
		mutate func(*debug.BuildInfo)
		want   string
	}{
		"wrong main path":    {func(bi *debug.BuildInfo) { bi.Path = "github.com/coder/openstorage/cmd/other" }, "main path"},
		"darwin":             {set("GOOS", "darwin"), "GOOS"},
		"arm64":              {set("GOARCH", "arm64"), "GOARCH"},
		"not git":            {set("vcs", "hg"), "vcs="},
		"malformed revision": {set("vcs.revision", commit[:39]), "vcs.revision"},
		"wrong revision":     {set("vcs.revision", strings.Repeat("cd", 20)), "vcs.revision"},
		"modified":           {set("vcs.modified", "true"), "vcs.modified"},
		"no revision":        {func(bi *debug.BuildInfo) { bi.Settings = bi.Settings[:3] }, "vcs.revision"},
	} {
		t.Run(name, func(t *testing.T) {
			bi := valid()
			tc.mutate(bi)
			if err := verifyUpgradeBuildInfo(bi, upgradeCandidateMainPath, commit); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestPrepareUpgradeInputs_RejectsOfficialArtifacts(t *testing.T) {
	f := newUpgradeFixture(t)
	frozen := []string{"candidate", "checksums.txt", upgradeArchiveName}
	good := f.pin
	t.Run("checksums bytes drift", func(t *testing.T) {
		upgWrite(t, f.cfg.Checksums, good.ArchiveSHA256+"  "+upgradeArchiveName+"\n# tampered\n")
		defer f.setChecksums(t, good.ArchiveSHA256+"  "+upgradeArchiveName+"\n")
		f.mustReject(t, f.cfg, "checksums.txt sha256", frozen...)
	})
	t.Run("checksums lacks archive line", func(t *testing.T) {
		f.setChecksums(t, good.ArchiveSHA256+"  openstorage_0.15.0_linux_arm64.tar.gz\n")
		defer f.setChecksums(t, good.ArchiveSHA256+"  "+upgradeArchiveName+"\n")
		f.mustReject(t, f.cfg, "no entry", frozen...)
	})
	t.Run("checksums line disagrees with pin", func(t *testing.T) {
		f.setChecksums(t, strings.Repeat("0", 64)+"  "+upgradeArchiveName+"\n")
		defer f.setChecksums(t, good.ArchiveSHA256+"  "+upgradeArchiveName+"\n")
		f.mustReject(t, f.cfg, "lists", frozen...)
	})
	t.Run("archive digest", func(t *testing.T) {
		pin := f.pin
		defer func() { f.pin = pin }()
		f.pin.ArchiveSHA256 = strings.Repeat("0", 64)
		f.setChecksums(t, f.pin.ArchiveSHA256+"  "+upgradeArchiveName+"\n")
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
		f.setArchive(t, tarEntry{Name: "openstorage", Body: f.oldBinaryBody}, tarEntry{Name: "LICENSE", Body: []byte("x")})
		defer f.setArchive(t, tarEntry{Name: "openstorage", Body: f.oldBinaryBody})
		f.mustReject(t, f.cfg, "extra entry", withOld...)
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
	for _, p := range []string{got.OldBinary, got.CandidateBinary} {
		st, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if filepath.Dir(p) != dest || st.Mode().Perm() != 0o500 {
			t.Fatalf("%s: dir %s mode %o, want frozen 0500 binary in %s", p, filepath.Dir(p), st.Mode().Perm(), dest)
		}
	}
	if body, err := os.ReadFile(got.OldBinary); err != nil || !bytes.Equal(body, f.oldBinaryBody) {
		t.Fatalf("old binary bytes %q (%v), want archive entry", body, err)
	}
	if got.CandidateSHA256 != f.cfg.CandidateSHA256 || got.Commit != f.cfg.Commit || got.Tree != f.cfg.Tree ||
		got.ArchiveSHA256 != f.pin.ArchiveSHA256 || got.OldBinarySHA256 != upgSHA256(t, got.OldBinary) ||
		got.OldTag != f.pin.OldTag || got.OldCommit != f.pin.OldCommit {
		t.Fatalf("identities %+v do not match fixture %+v", got, f)
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
	cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=0", "GOFLAGS=", "GOWORK=off", "GOTOOLCHAIN=local")
	if raw, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build fixture: %v\n%s", err, raw)
	}
	return out
}

func isolateUpgradeGit(t *testing.T) {
	t.Helper()
	for _, entry := range os.Environ() {
		if key, value, _ := strings.Cut(entry, "="); strings.HasPrefix(key, "GIT_") {
			t.Setenv(key, value)
			if err := os.Unsetenv(key); err != nil {
				t.Fatal(err)
			}
		}
	}
	for key, value := range map[string]string{
		"GIT_CONFIG_GLOBAL": os.DevNull, "GIT_CONFIG_SYSTEM": os.DevNull, "GIT_CONFIG_NOSYSTEM": "1",
		"GIT_AUTHOR_NAME": "githttp", "GIT_AUTHOR_EMAIL": "githttp@test.invalid", "GIT_AUTHOR_DATE": "2000-01-01T00:00:00Z",
		"GIT_COMMITTER_NAME": "githttp", "GIT_COMMITTER_EMAIL": "githttp@test.invalid", "GIT_COMMITTER_DATE": "2000-01-01T00:00:00Z",
	} {
		t.Setenv(key, value)
	}
}

func upgGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "git", args...)
	cmd.Dir = dir
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
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
