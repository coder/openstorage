// Package heldport keeps a test server's TCP port reserved while the
// server is down, on Linux only, so a test can stop a server and start it
// again on the same address. In between, only a socket that also sets
// SO_REUSEPORT and binds that exact port, from a process of the same user,
// can take the port. Elsewhere Listen and Relisten are plain listeners and
// hold nothing: on macOS a bound socket that does not listen can receive
// the port's connections (XNU hands a SYN to the first bound socket that
// matches), so a held port would stop the server.
package heldport

import (
	"net"
	"testing"
)

// Listen listens on a fresh IPv4 loopback port. On Linux it also holds the
// port until the test ends: after the listener closes, a dial to the port
// is refused, no socket without SO_REUSEPORT can bind it, and Relisten
// serves on it again.
func Listen(t testing.TB) net.Listener {
	t.Helper()
	ln := listen(t, "127.0.0.1:0")
	hold(t, ln.Addr().(*net.TCPAddr))
	return ln
}

// Relisten listens again on addr, a port that Listen returned.
func Relisten(t testing.TB, addr string) net.Listener {
	t.Helper()
	return listen(t, addr)
}

// listen listens on addr. On Linux, listenControl sets SO_REUSEPORT, so the
// listener can share the port with hold's socket.
func listen(t testing.TB, addr string) net.Listener {
	t.Helper()
	lc := net.ListenConfig{Control: listenControl}
	ln, err := lc.Listen(t.Context(), "tcp4", addr)
	if err != nil {
		t.Fatalf("heldport: listen %s: %v", addr, err)
	}
	return ln
}
