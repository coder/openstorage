//go:build linux

package api

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/coder/openstorage/internal/auth"
)

// TestForkPackAcrossFilesystems (issue #360): the fork pack must not depend
// on TMPDIR sharing a filesystem with the cache dir. A container with the
// data dir on a volume and /tmp on the overlay root has exactly this
// layout; before the fix git wrote its temporary pack inside the source
// cache repo's object dir and rename(2)d it into the TMPDIR pack dir, which
// failed with EXDEV and answered 500.
func TestForkPackAcrossFilesystems(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := newForkServer(t, e)
		src := wrefSeedFixture(t, e)
		srcReadTok := e.mint(t, tokenSpec{iss: src.org, repo: src.url, scopes: []string{auth.ScopeGitRead}})
		caller := newForkCaller(t, e, "")

		// Point TMPDIR at a filesystem other than the cache dir's. The lane
		// environment (and its cache dir) already exists, so only the
		// request's temp dirs move.
		cacheDev := forkDevice(t, e.cacheDir)
		var tmpRoot string
		for _, root := range []string{"/dev/shm", "/tmp", "/var/tmp"} {
			if fi, err := os.Stat(root); err == nil && fi.IsDir() && forkDevice(t, root) != cacheDev {
				tmpRoot = root
				break
			}
		}
		if tmpRoot == "" {
			t.Skipf("no candidate temp root on a filesystem other than the cache dir %s; cannot reproduce the cross-device layout", e.cacheDir)
		}
		tmpDir, err := os.MkdirTemp(tmpRoot, "openstorage-exdev-*")
		if err != nil {
			t.Skipf("cannot create a temp dir under %s: %v", tmpRoot, err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(tmpDir) })
		t.Setenv("TMPDIR", tmpDir)
		if got := os.TempDir(); got != tmpDir {
			t.Fatalf("os.TempDir() = %q, want %q", got, tmpDir)
		}

		resp, body := forkDo(t, srv, caller.token, forkBody(srcReadTok, nil))
		forkID := forkRequireCreated(t, resp, body)
		if refs := wrefRefs(t, e, forkID); refs["refs/heads/main"] != src.c2 {
			t.Fatalf("fork refs = %v, want refs/heads/main=%s", refs, src.c2)
		}
		// The request's scratch is gone once the handler returns.
		left, err := filepath.Glob(filepath.Join(tmpDir, "*"))
		if err != nil {
			t.Fatal(err)
		}
		if len(left) != 0 {
			t.Fatalf("fork left temp entries behind: %v", left)
		}
	})
}

// forkDevice returns the device ID of the filesystem holding path.
func forkDevice(t *testing.T, path string) uint64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("stat %s: no syscall.Stat_t", path)
	}
	return st.Dev
}
