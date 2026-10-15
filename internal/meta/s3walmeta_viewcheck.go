//go:build openstorage_viewcheck

package meta

import (
	"fmt"
	"os"

	"github.com/coder/openstorage/internal/envconfig"
)

// The openstorage_viewcheck build tag turns on the shared-view check
// (checkViewIntact) in any binary, so tests outside this package catch a
// write into the shared metadata view too (issue #497): the e2e serve
// binary that internal/githttp builds, and CI's in-process test runs.
// A failed check is also appended to the file ViewCheckLogEnv names, so
// the e2e harness sees it even when serve recovers or loses the panic
// (issue #503). Release builds never set the tag.
func init() {
	checkSharedView = true
	reportViewCheck = func(msg string) {
		path := envconfig.Getenv(ViewCheckLogEnv)
		if path == "" {
			return
		}
		// Best effort: the panic that follows still fails the caller.
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return
		}
		_, _ = fmt.Fprintf(f, "pid %d: %s\n", os.Getpid(), msg)
		_ = f.Close()
	}
}
