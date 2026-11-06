// Package heldport keeps a test server's TCP port reserved while the
// server is down, so a test can stop a server and start it again on the
// same address. In between, only a socket that also sets SO_REUSEPORT and
// binds that exact port (on Linux, only from a process of the same user)
// can take the port.
package heldport

import (
	"net"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

// Listen listens on a fresh IPv4 loopback port and holds the port until
// the test ends. After the listener closes, a dial to the port is refused,
// no socket without SO_REUSEPORT can bind it, and Relisten serves on it
// again.
func Listen(t testing.TB) net.Listener {
	t.Helper()
	ln := listen(t, "127.0.0.1:0")
	hold(t, ln.Addr().(*net.TCPAddr))
	return ln
}

// Relisten listens again on addr, a port that Listen holds.
func Relisten(t testing.TB, addr string) net.Listener {
	t.Helper()
	return listen(t, addr)
}

// listen listens on addr with SO_REUSEPORT, so it can share the port with
// hold's socket.
func listen(t testing.TB, addr string) net.Listener {
	t.Helper()
	lc := net.ListenConfig{Control: func(_, _ string, c syscall.RawConn) error {
		var serr error
		if err := c.Control(func(fd uintptr) {
			serr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
		}); err != nil {
			return err
		}
		return serr
	}}
	ln, err := lc.Listen(t.Context(), "tcp4", addr)
	if err != nil {
		t.Fatalf("heldport: listen %s: %v", addr, err)
	}
	return ln
}

// hold binds a socket to addr with SO_REUSEPORT and never listens on it,
// until the test ends. The kernel answers a dial to a port with no
// listener with a reset (connection refused), and gives the port to no
// socket that lacks SO_REUSEPORT.
func hold(t testing.TB, addr *net.TCPAddr) {
	t.Helper()
	ip := addr.IP.To4()
	if ip == nil {
		t.Fatalf("heldport: %v is not IPv4", addr)
	}
	// SOCK_CLOEXEC is Linux-only. As the standard library does where it
	// is missing, hold ForkLock from socket creation to CloseOnExec, so
	// no child process started in between inherits the socket.
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
}
