//go:build !linux

package githttp

import (
	"syscall"
	"testing"
)

// nodeSysProcAttr starts serve as a session leader, so testNode.stop can
// kill serve together with its git children (issue #439). Only Linux has
// Pdeathsig (see testnode_linux_test.go): here a test binary that dies
// before its cleanups run leaves serve running.
func nodeSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setsid: true}
}

// sessionMembers needs /proc: without it testNode.stop reaches serve's
// own process group only.
func sessionMembers(int) []int { return nil }

// Node tracking (the node guard in testnode_linux_test.go) needs /proc: a no-op here.
func trackNodeSession(int) error { return nil }
func untrackNodeSession(int)     {}

func killSessionMembers(int) []int { return nil }

func nodeHelperMain(*testing.M) (int, bool) { return 0, false }
