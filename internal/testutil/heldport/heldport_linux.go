//go:build linux

package heldport

import (
	"net"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

// listenControl sets SO_REUSEPORT on a listener, so it can share the port
// with hold's socket.
func listenControl(_, _ string, c syscall.RawConn) error {
	var serr error
	if err := c.Control(func(fd uintptr) {
		serr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
	}); err != nil {
		return err
	}
	return serr
}

// hold binds a socket to addr with SO_REUSEPORT and never listens on it,
// until the test ends, and returns its descriptor. The kernel answers a
// dial to a port with no listener with a reset (connection refused), and
// gives the port to no socket that lacks SO_REUSEPORT.
func hold(t testing.TB, addr *net.TCPAddr) int {
	t.Helper()
	ip := addr.IP.To4()
	if ip == nil {
		t.Fatalf("heldport: %v is not IPv4", addr)
	}
	// Hold ForkLock from socket creation to CloseOnExec, as the standard
	// library does where SOCK_CLOEXEC is missing, so no child process
	// started in between inherits the socket.
	syscall.ForkLock.RLock()
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err == nil {
		unix.CloseOnExec(fd)
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		t.Fatalf("heldport: socket: %v", err)
	}
	t.Cleanup(func() { _ = unix.Close(fd) })
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEPORT, 1); err != nil {
		t.Fatalf("heldport: SO_REUSEPORT: %v", err)
	}
	sa := &unix.SockaddrInet4{Port: addr.Port}
	copy(sa.Addr[:], ip)
	if err := unix.Bind(fd, sa); err != nil {
		t.Fatalf("heldport: bind %v: %v", addr, err)
	}
	return fd
}
