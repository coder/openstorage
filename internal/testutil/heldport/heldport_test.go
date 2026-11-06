package heldport

import (
	"errors"
	"net"
	"testing"

	"golang.org/x/sys/unix"
)

// TestRelistenAfterClose: after Listen's listener closes, a dial is
// refused and a listener without SO_REUSEPORT cannot bind the address;
// Relisten accepts connections on it again.
func TestRelistenAfterClose(t *testing.T) {
	ln := Listen(t)
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	if c, err := net.Dial("tcp4", addr); err == nil || !errors.Is(err, unix.ECONNREFUSED) {
		if c != nil {
			_ = c.Close()
		}
		t.Fatalf("dial while closed: err = %v, want connection refused", err)
	}
	if other, err := net.Listen("tcp4", addr); err == nil {
		_ = other.Close()
		t.Fatalf("another listener could bind %s while the port was held", addr)
	}
	ln = Relisten(t, addr)
	defer ln.Close()
	accepted := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err == nil {
			_ = c.Close()
		}
		accepted <- err
	}()
	c, err := net.Dial("tcp4", addr)
	if err != nil {
		t.Fatalf("dial after Relisten: %v", err)
	}
	_ = c.Close()
	if err := <-accepted; err != nil {
		t.Fatalf("accept after Relisten: %v", err)
	}
}
