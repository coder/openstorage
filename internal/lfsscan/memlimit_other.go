//go:build !linux

package lfsscan

// limitGitHeap is the non-Linux no-op: prlimit-after-start is a Linux
// facility, production deploys on Linux, and the heap cap is
// defense-in-depth behind the work-unit budget and the walk/pass
// wall-clock deadlines.
func limitGitHeap(int) error { return nil }
