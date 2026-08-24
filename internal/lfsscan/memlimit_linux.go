package lfsscan

import "golang.org/x/sys/unix"

// lfsScanGitHeapLimit caps each walk subprocess's heap at 2 GiB (Codex
// security round 20): a `git fsck`-valid, highly compressible wide tree
// inflates inside rev-list BEFORE its children stream to the metered
// stages, so the work-unit budget cannot cancel the allocation spike it
// never sees — repeated scheduled passes could OOM the worker or node.
// The cap is RLIMIT_DATA — brk plus private anonymous mappings since
// Linux 4.7 — deliberately NOT RLIMIT_AS, which would also count the
// file-backed pack windows git legitimately mmaps on large healthy
// repositories. Any healthy walk's heap is far below this ceiling; an
// attacker tree that crosses it kills only its own subprocess, failing
// the walk (repository deferred, never marked).
const lfsScanGitHeapLimit = 2 << 30

// limitGitHeap applies the heap rlimit to a just-started walk subprocess.
func limitGitHeap(pid int) error {
	rl := &unix.Rlimit{Cur: lfsScanGitHeapLimit, Max: lfsScanGitHeapLimit}
	return unix.Prlimit(pid, unix.RLIMIT_DATA, rl, nil)
}
