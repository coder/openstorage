// Package pgfault relays PostgreSQL connections through a local TCP proxy
// and injects one fault into the next COMMIT, for tests of a COMMIT whose
// outcome the client does not learn.
package pgfault

import (
	"bytes"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Fault is what the proxy does to the next COMMIT.
type Fault int

const (
	// DropReply relays the COMMIT, swallows the server's reply, and closes
	// both sides: the transaction commits, and the client never learns it.
	DropReply Fault = iota + 1
	// DropCommit closes both sides instead of relaying the COMMIT: the
	// server aborts the transaction.
	DropCommit
	// HoldCommit closes the client side and keeps the server side open
	// without relaying the COMMIT: the transaction stays in progress until
	// the test ends.
	HoldCommit
	// CutAll drops the COMMIT like DropCommit, then closes every relayed
	// connection and the listener: no later connection reaches the server.
	CutAll
	// DelayCommit closes the client side, then relays the COMMIT after
	// CommitDelay: the transaction stays in progress for CommitDelay, then
	// commits, and the client never learns it.
	DelayCommit
)

// CommitDelay is how long DelayCommit holds the COMMIT.
const CommitDelay = 300 * time.Millisecond

// commitMsg is COMMIT as pgx sends it: one simple-protocol Query message.
var commitMsg = append([]byte{'Q', 0, 0, 0, 11}, "commit\x00"...)

// Proxy relays TCP connections to one PostgreSQL server.
type Proxy struct {
	ln     net.Listener
	target string
	fired  atomic.Bool

	mu      sync.Mutex
	fault   Fault
	onFault func()
	conns   []net.Conn
}

// Start relays to the host of dbURL and returns the proxy and dbURL
// rewritten to connect through it. t's cleanup closes the listener and
// every relayed connection.
func Start(t testing.TB, dbURL string) (*Proxy, string) {
	t.Helper()
	u, err := url.Parse(dbURL)
	if err != nil || u.Host == "" {
		t.Fatalf("pgfault: bad database URL: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &Proxy{ln: ln, target: u.Host}
	t.Cleanup(p.closeAll)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go p.relay(c)
		}
	}()
	u.Host = ln.Addr().String()
	return p, u.String()
}

// Arm makes the next COMMIT that any relayed connection sends suffer f.
// onFault, when not nil, runs once before the proxy closes any connection,
// so Fired is true by the time the client sees the fault. For DropReply it
// runs after the server's reply arrived: the transaction committed. For the
// other faults it runs when the proxy holds the COMMIT.
func (p *Proxy) Arm(f Fault, onFault func()) {
	if f < DropReply || f > DelayCommit {
		panic("pgfault: unknown fault")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fault, p.onFault = f, onFault
}

// Fired reports whether the armed fault took effect.
func (p *Proxy) Fired() bool { return p.fired.Load() }

// take returns the armed fault, if any, and disarms the proxy.
func (p *Proxy) take() (Fault, func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	f, fn := p.fault, p.onFault
	p.fault, p.onFault = 0, nil
	return f, fn
}

func (p *Proxy) track(c ...net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.conns = append(p.conns, c...)
}

func (p *Proxy) closeAll() {
	_ = p.ln.Close()
	p.mu.Lock()
	conns := p.conns
	p.conns = nil
	p.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

func (p *Proxy) fire(onFault func()) {
	if onFault != nil {
		onFault()
	}
	p.fired.Store(true)
}

func (p *Proxy) relay(client net.Conn) {
	server, err := net.Dial("tcp", p.target)
	if err != nil {
		_ = client.Close()
		return
	}
	p.track(client, server)
	var swallow atomic.Bool
	var onReply func()
	go func() {
		buf := make([]byte, 64<<10)
		for {
			n, err := client.Read(buf)
			if n > 0 && bytes.Equal(buf[:n], commitMsg) {
				switch f, fn := p.take(); f {
				case DropReply:
					onReply = fn
					swallow.Store(true)
				case DropCommit:
					p.fire(fn)
					_ = server.Close()
					_ = client.Close()
					return
				case HoldCommit:
					p.fire(fn)
					_ = client.Close()
					return // the server side stays open until cleanup
				case CutAll:
					p.fire(fn)
					p.closeAll()
					return
				case DelayCommit:
					p.fire(fn)
					_ = client.Close()
					time.Sleep(CommitDelay)
					_, _ = server.Write(buf[:n])
					return // the server side stays open until cleanup
				}
			}
			if n > 0 {
				if _, werr := server.Write(buf[:n]); werr != nil {
					break
				}
			}
			if err != nil {
				break
			}
		}
		_ = server.Close()
	}()
	buf := make([]byte, 64<<10)
	for {
		n, err := server.Read(buf)
		if n > 0 && swallow.Load() {
			p.fire(onReply)
			_ = server.Close()
			break
		}
		if n > 0 {
			if _, werr := client.Write(buf[:n]); werr != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	_ = client.Close()
}
