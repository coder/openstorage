//go:build !linux

package heldport

import (
	"net"
	"syscall"
	"testing"
)

// listenControl is nil off Linux: listeners are plain, as before heldport.
var listenControl func(network, address string, c syscall.RawConn) error

// hold holds nothing off Linux (see the package comment) and returns -1.
func hold(testing.TB, *net.TCPAddr) int { return -1 }
