package githttp

// Unit tests for the issue #55 ingress-cap building blocks: cappedReader
// semantics (inclusive cap, overflow detection), the critical interaction
// with the atomic-capability sniffer (the teed prefix must be counted
// exactly once), and pushGate's buffered-then-streamed response handling.

import (
	"bytes"
	"errors"
	"io"
	"math"
	"strings"
	"testing"
)

func TestCappedReader_UnderCapPassesThrough(t *testing.T) {
	c := newCappedReader(strings.NewReader("hello"), 10)
	got, err := io.ReadAll(c)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "hello" || c.exceeded {
		t.Fatalf("got %q exceeded=%v, want full passthrough", got, c.exceeded)
	}
}

func TestCappedReader_ExactlyAtCapIsClean(t *testing.T) {
	c := newCappedReader(strings.NewReader("hello"), 5)
	got, err := io.ReadAll(c)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "hello" || c.exceeded {
		t.Fatalf("got %q exceeded=%v, want clean at-cap read", got, c.exceeded)
	}
}

func TestCappedReader_OverflowFailsAndSticks(t *testing.T) {
	c := newCappedReader(strings.NewReader("hello!"), 5)
	got, err := io.ReadAll(c)
	if !errors.Is(err, errPushTooLarge) {
		t.Fatalf("ReadAll err = %v, want errPushTooLarge", err)
	}
	if string(got) != "hello" || !c.exceeded {
		t.Fatalf("got %q exceeded=%v, want capped prefix + sticky overflow", got, c.exceeded)
	}
	if _, err := c.Read(make([]byte, 1)); !errors.Is(err, errPushTooLarge) {
		t.Fatalf("post-overflow Read err = %v, want errPushTooLarge", err)
	}
}

// TestCappedReader_SniffCountsPrefixOnce is the #55/#28 interaction
// invariant: sniffAtomicPush tees its consumed prefix and replays it, so a
// counter UNDER the sniffer must charge each wire byte exactly once. A
// double-count would spuriously 413 any push whose body fits the cap but
// whose sniffed prefix pushes the doubled total past it.
func TestCappedReader_SniffCountsPrefixOnce(t *testing.T) {
	line := "0000000000000000000000000000000000000000 1111111111111111111111111111111111111111 refs/heads/main\x00atomic"
	body := pktLine(line) + "0000" + "PACKDATA"
	// Cap exactly at the body length: any double-counting of the sniffed
	// prefix would overflow.
	c := newCappedReader(strings.NewReader(body), int64(len(body)))
	atomic, replay := sniffAtomicPush(c)
	if !atomic {
		t.Fatal("sniffer missed the atomic capability through the capped reader")
	}
	if c.exceeded {
		t.Fatal("cap exceeded during sniff of an exactly-at-cap body")
	}
	got, err := io.ReadAll(replay)
	if err != nil {
		t.Fatalf("drain replay: %v", err)
	}
	if string(got) != body || c.exceeded {
		t.Fatalf("replay mismatch or overflow (exceeded=%v): the teed prefix was double-counted", c.exceeded)
	}
}

// TestCappedReader_OverflowDuringSniff proves a cap below the command-list
// prefix surfaces as exceeded (the handler 413s before git ever spawns) and
// that the sniffer degrades to a non-atomic verdict instead of panicking.
func TestCappedReader_OverflowDuringSniff(t *testing.T) {
	line := "0000000000000000000000000000000000000000 1111111111111111111111111111111111111111 refs/heads/main\x00atomic"
	body := pktLine(line) + "0000"
	c := newCappedReader(strings.NewReader(body), 10)
	_, _ = sniffAtomicPush(c)
	if !c.exceeded {
		t.Fatal("sniffing past the cap did not set exceeded")
	}
}

func TestWirePushLimit(t *testing.T) {
	// The slack must admit any legitimate gzip encoding of a cap-sized
	// body (stored-mode deflate ≈ +0.008% + constant framing) without
	// materially extending the wire bound.
	if got := wirePushLimit(1 << 20); got != (1<<20)+(1<<20)/64+1024 {
		t.Fatalf("wirePushLimit(1MiB) = %d", got)
	}
	if got := wirePushLimit(math.MaxInt64 - 5); got != math.MaxInt64 {
		t.Fatalf("huge cap = %d, want MaxInt64 saturation", got)
	}
}

func TestPushGate_BuffersUntilSettleThenStreams(t *testing.T) {
	var dst bytes.Buffer
	g := newPushGate(&dst)
	if _, err := g.Write([]byte("early")); err != nil {
		t.Fatal(err)
	}
	if dst.Len() != 0 {
		t.Fatalf("gate leaked %q before settle", dst.String())
	}
	if g.responseStarted() {
		t.Fatal("responseStarted before any byte reached dst")
	}
	if err := g.settle(false); err != nil {
		t.Fatal(err)
	}
	if dst.String() != "early" {
		t.Fatalf("settle(false) flushed %q, want %q", dst.String(), "early")
	}
	if _, err := g.Write([]byte("+late")); err != nil {
		t.Fatal(err)
	}
	if dst.String() != "early+late" || !g.responseStarted() {
		t.Fatalf("post-settle write got %q started=%v", dst.String(), g.responseStarted())
	}
}

func TestPushGate_SettleOverflowDiscardsPastAndFuture(t *testing.T) {
	var dst bytes.Buffer
	g := newPushGate(&dst)
	if _, err := g.Write([]byte("unpack failed report")); err != nil {
		t.Fatal(err)
	}
	if err := g.settle(true); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Write([]byte("straggler")); err != nil {
		t.Fatal(err)
	}
	if dst.Len() != 0 || g.responseStarted() {
		t.Fatalf("overflow gate leaked %q (started=%v), want nothing", dst.String(), g.responseStarted())
	}
}

func TestPushGate_ForcesOpenPastBufferBound(t *testing.T) {
	var dst bytes.Buffer
	g := newPushGate(&dst)
	big := bytes.Repeat([]byte("x"), gateBufferMax+1)
	if _, err := g.Write(big); err != nil {
		t.Fatal(err)
	}
	if dst.Len() != len(big) {
		t.Fatalf("oversized early output was buffered (%d of %d bytes flushed), want forced stream", dst.Len(), len(big))
	}
	if !g.responseStarted() {
		t.Fatal("forced-open gate must report a started response (413 no longer possible)")
	}
	// settle(false) after the forced open stays a no-op.
	if err := g.settle(false); err != nil {
		t.Fatal(err)
	}
	if dst.Len() != len(big) {
		t.Fatalf("settle after forced open changed output to %d bytes", dst.Len())
	}
}
