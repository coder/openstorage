//go:build !unix

package sync

import "os/exec"

// setProcGroup is the non-unix no-op: process groups are a unix facility,
// production deploys on Linux, and WaitDelay still unblocks Wait if an
// orphaned descendant holds the output pipes after the parent kill.
func setProcGroup(cmd *exec.Cmd) {}
