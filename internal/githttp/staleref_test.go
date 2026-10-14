package githttp

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestReportOldRefLocks: hydration reports an old lock that blocks a ref
// update and never removes one (issue #464).
func TestReportOldRefLocks(t *testing.T) {
	old := time.Now().Add(-2 * staleRefLockAge)
	for _, tc := range []struct {
		name     string
		lock     string    // relative to the repo
		mtime    time.Time // zero: now
		reported int
	}{
		{name: "old loose lock", lock: "refs/heads/x.lock", mtime: old, reported: 1},
		{name: "old packed-refs lock", lock: "packed-refs.lock", mtime: old, reported: 1},
		{name: "old lock below the ref", lock: "refs/heads/x/y.lock", mtime: old, reported: 1},
		{name: "old lock of another ref", lock: "refs/heads/z.lock", mtime: old},
		{name: "young lock", lock: "refs/heads/x.lock"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := t.TempDir()
			lock := filepath.Join(repo, filepath.FromSlash(tc.lock))
			if err := os.MkdirAll(filepath.Dir(lock), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(lock, []byte("0000000000000000000000000000000000000000\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if !tc.mtime.IsZero() {
				if err := os.Chtimes(lock, tc.mtime, tc.mtime); err != nil {
					t.Fatal(err)
				}
			}
			if got := reportOldRefLocks(repo, []string{"refs/heads/x", "../escape"}); got != tc.reported {
				t.Fatalf("reported %d locks, want %d", got, tc.reported)
			}
			if _, err := os.Lstat(lock); err != nil {
				t.Fatalf("lock removed: %v", err)
			}
		})
	}
}
