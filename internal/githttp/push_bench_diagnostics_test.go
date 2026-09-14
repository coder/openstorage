package githttp

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func newBenchChainTestRepo(t *testing.T) string {
	t.Helper()
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	dir := t.TempDir()
	mustGit(t, dir, "init", "-b", "main", ".")
	writeFile(t, dir, "data.txt", "seed\n")
	mustGit(t, dir, "add", "data.txt")
	mustGit(t, dir, "commit", "-m", "seed")
	return dir
}

func TestBenchGenerateChainDiagnostics_SyntheticGitFailure(t *testing.T) {
	dir := newBenchChainTestRepo(t)
	// Synthetic diagnostics validation, not a reproduction of issue #219's ENOENT.
	writeFile(t, dir, ".git/refs/heads/main", strings.Repeat("0", 39)+"1\n")
	wantOut, wantErr := tryGit(dir, "commit", "-a", "-q", "-m", "w0 op 1")
	if exit, ok := errors.AsType[*exec.ExitError](wantErr); !ok || exit.ExitCode() != 128 {
		t.Fatalf("synthetic Git failure: %v\n%s", wantErr, wantOut)
	}

	chain, err := benchGenerateChain(dir, 0, 1)
	if exit, ok := errors.AsType[*exec.ExitError](err); !ok || exit.ExitCode() != 128 {
		t.Fatalf("original Git exit status lost: %v", err)
	}
	if chain != nil || !strings.HasPrefix(err.Error(), fmt.Sprintf("commit 1: %v\n%s", wantErr, wantOut)) {
		t.Fatalf("original chain failure/output lost: chain=%v err=%v", chain, err)
	}
	for _, want := range []string{
		"benchGenerateChain diagnostics:",
		"captured_at=",
		`command=git commit -a -q -m "w0 op 1"`,
		fmt.Sprintf("repo=%q", dir),
		fmt.Sprintf("objects=%q", filepath.Join(dir, ".git", "objects")),
		"TMPDIR=", "GOTMPDIR=",
		"device=", "inode=", "mode=", "mtime=",
		"statfs_path=", "fsid=", "blocks_free=", "blocks_available=", "inodes_free=",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("missing %q in failure:\n%v", want, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, ".git", "objects")); err != nil {
		t.Fatalf("fixture was cleaned before diagnostics returned: %v", err)
	}
	t.Logf("SYNTHETIC diagnostic capture before cleanup:\n%v", err)
}

func TestBenchGenerateChainSuccess(t *testing.T) {
	dir := newBenchChainTestRepo(t)
	parent := revParse(t, dir, "HEAD")
	chain, err := benchGenerateChain(dir, 7, 3)
	if err != nil || len(chain) != 3 {
		t.Fatalf("chain=%v err=%v, want three commits", chain, err)
	}
	for i, want := range []string{"writer 7 op 1\n", "writer 7 op 2\n", "writer 7 op 3\n"} {
		if got := mustGit(t, dir, "show", chain[i]+":data.txt"); got != want {
			t.Errorf("commit %d content=%q, want %q", i, got, want)
		}
		if got := revParse(t, dir, chain[i]+"^"); got != parent {
			t.Errorf("commit %d parent=%q, want %q", i, got, parent)
		}
		parent = chain[i]
	}
	if got := revParse(t, dir, "HEAD"); got != chain[2] {
		t.Errorf("HEAD=%q, want final chain commit %q", got, chain[2])
	}
	if got := strings.TrimSpace(mustGit(t, dir, "rev-list", "--count", "HEAD")); got != "4" {
		t.Errorf("history count=%q, want seed plus three commits", got)
	}
	mustGit(t, dir, "fsck", "--full", "--no-dangling")
}

func TestBenchGenerateChainDiagnostics_SyntheticResolverFailures(t *testing.T) {
	for _, name := range []string{"ExitFailure", "OutputLimit", "Deadline"} {
		t.Run(name, func(t *testing.T) {
			dir := newBenchChainTestRepo(t)
			writeFile(t, dir, ".git/refs/heads/main", strings.Repeat("0", 39)+"1\n")
			wantOut, wantErr := tryGit(dir, "commit", "-a", "-q", "-m", "w0 op 1")
			wantExit, ok := errors.AsType[*exec.ExitError](wantErr)
			if !ok {
				t.Fatalf("synthetic Git failure did not occur: %v", wantErr)
			}
			realGit, err := exec.LookPath("git")
			if err != nil {
				t.Fatal(err)
			}
			bin := t.TempDir()
			probe, want := "exit 23", "resolve_error=exit status 23"
			switch name {
			case "OutputLimit":
				probe, want = "printf '/%05000d\\n' 0; exit 0", "resolve_error=object path exceeds 4096 bytes"
			case "Deadline":
				fifo := filepath.Join(bin, "blocked")
				if err := syscall.Mkfifo(fifo, 0o600); err != nil {
					t.Fatal(err)
				}
				probe = fmt.Sprintf("read -r ignored < '%s'; exit 0", strings.ReplaceAll(fifo, "'", "'\\''"))
				want = "resolve_error=context deadline exceeded"
			}
			// Only the diagnostic rev-parse is substituted; commits still run real Git.
			script := fmt.Sprintf("#!/bin/sh\nif [ \"$1\" = rev-parse ]; then\n  echo PRIVATE_CONFIG_CANARY >&2\n  %s\nfi\nexec '%s' \"$@\"\n", probe, strings.ReplaceAll(realGit, "'", "'\\''"))
			if err := os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0o755); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
			_, err = benchGenerateChain(dir, 0, 1)
			if exit, ok := errors.AsType[*exec.ExitError](err); !ok || exit.ExitCode() != wantExit.ExitCode() {
				t.Fatalf("diagnostic failure replaced original Git failure: %v", err)
			}
			for _, text := range []string{
				fmt.Sprintf("commit 1: %v\n%s", wantErr, wantOut),
				"objects_resolution=fallback", want,
				fmt.Sprintf("statfs_path=%q", filepath.Join(dir, ".git", "objects")),
			} {
				if !strings.Contains(err.Error(), text) {
					t.Errorf("missing %q in failure:\n%v", text, err)
				}
			}
			if strings.Contains(err.Error(), "PRIVATE_CONFIG_CANARY") {
				t.Errorf("diagnostic stderr leaked: %v", err)
			}
		})
	}
}

func TestBenchGenerateChainDiagnostics_SyntheticObjectPaths(t *testing.T) {
	for _, name := range []string{"MissingObjects", "NonDirectoryParent"} {
		t.Run(name, func(t *testing.T) {
			dir := newBenchChainTestRepo(t)
			parent := filepath.Join(t.TempDir(), "object-parent")
			if name == "NonDirectoryParent" {
				writeFile(t, filepath.Dir(parent), filepath.Base(parent), "not a directory\n")
			} else if err := os.Mkdir(parent, 0o755); err != nil {
				t.Fatal(err)
			}
			objects := filepath.Join(parent, "objects")
			t.Setenv("GIT_OBJECT_DIRECTORY", objects)
			wantOut, wantErr := tryGit(dir, "commit", "-a", "-q", "-m", "w0 op 1")
			wantExit, ok := errors.AsType[*exec.ExitError](wantErr)
			if !ok {
				t.Fatalf("synthetic object-path failure did not reach Git: %v", wantErr)
			}
			_, err := benchGenerateChain(dir, 0, 1)
			if exit, ok := errors.AsType[*exec.ExitError](err); !ok || exit.ExitCode() != wantExit.ExitCode() {
				t.Fatalf("original Git failure lost: %v", err)
			}
			for _, want := range []string{
				fmt.Sprintf("commit 1: %v\n%s", wantErr, wantOut),
				fmt.Sprintf("objects=%q", objects),
				fmt.Sprintf("stat path=%q error=", objects),
				fmt.Sprintf("statfs_path=%q", parent),
			} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("missing %q in failure:\n%v", want, err)
				}
			}
		})
	}
}
