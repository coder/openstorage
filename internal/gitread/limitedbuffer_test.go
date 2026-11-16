package gitread

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// output is a listing-like text of n bytes: every line differs, so a join in
// the wrong order or a lost byte shows.
func output(n int) []byte {
	var b bytes.Buffer
	for i := 0; b.Len() < n; i++ {
		b.WriteString("100644 blob " + strconv.Itoa(i*7919+12345) + "\tdir/file" + strconv.Itoa(i) + ".txt\n")
	}
	return b.Bytes()[:n]
}

// allocated returns the bytes that f allocates, GC or not. The tests of the
// package run one at a time, so nothing else allocates meanwhile.
func allocated(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// maxChunks bounds the chunks of an output of up to 64 MiB, the default
// output allowance. A chunk is a quarter of what is held and at least 64
// bytes: 4 chunks of 64 bytes, then growth by 1.25 times from 256 bytes to
// 32 MiB (about 53 chunks), then chunks of 8 MiB. The most is 61 chunks, for
// writes of 1 or 64 bytes up to 64 MiB. The tests below reach 56 (one byte at
// a time, 24 MiB) and 31 (32 KiB at a time, 48 MiB). A buffer that starts a
// chunk for every write, or one that does not count what it holds, or one
// that grows by an eighth, is far over 80 at once, and the checks fail on the
// write that crosses it, not at the test's timeout.
const maxChunks = 80

// The chunks join into what was written, whatever the sizes of the writes
// and of the chunks: empty writes, writes that end on a chunk's end, and
// writes of several chunks.
func TestLimitedBufferKeepsWhatIsWritten(t *testing.T) {
	data := output(3*(8<<20) + 12345)
	rng := rand.New(rand.NewPCG(1, 2))
	for name, sizes := range map[string][]int{
		"one byte":          {1},
		"the first chunk":   {64, 64, 65, 0, 63},
		"the pipe's 32 KiB": {32 << 10},
		"a write of 3 MiB":  {3 << 20},
		"over a chunk":      {8<<20 - 1, 1, 8<<20 + 1, 2*(8<<20) + 1},
		"random":            nil,
	} {
		t.Run(name, func(t *testing.T) {
			var l limitedBuffer
			rest := data
			written := 0
			for i := 0; len(rest) > 0; i++ {
				n := 0
				if sizes == nil {
					n = rng.IntN(70 << 10)
				} else {
					n = sizes[i%len(sizes)]
				}
				n = min(n, len(rest))
				if w, err := l.Write(rest[:n]); w != n || err != nil {
					t.Fatalf("Write(%d bytes) = %d, %v", n, w, err)
				}
				rest = rest[n:]
				written += n
				if l.n != written || len(l.chunks) > maxChunks {
					t.Fatalf("after %d bytes written the buffer holds %d bytes in %d chunks, want the same bytes in at most %d", written, l.n, len(l.chunks), maxChunks)
				}
			}
			if got := l.String(); got != string(data) {
				t.Fatalf("held %d bytes, want %d, or the bytes differ", len(got), len(data))
			}
		})
	}
	if got := new(limitedBuffer).String(); got != "" {
		t.Fatalf("an empty buffer holds %q", got)
	}
}

// A write past the allowance is refused, git is killed, and the buffer keeps
// the bytes of the writes before it.
func TestLimitedBufferRefusesWhatPassesTheAllowance(t *testing.T) {
	_, b := budgeted(t, 100)
	killed := 0
	l := &limitedBuffer{allow: b, kill: func() { killed++ }}
	if _, err := l.Write(make([]byte, 60)); err != nil {
		t.Fatal(err)
	}
	if n, err := l.Write(make([]byte, 60)); n != 0 || !errors.Is(err, errOutputOverflow) || killed != 1 {
		t.Fatalf("write past the allowance = %d, %v, killed %d times", n, err, killed)
	}
	if n, err := l.Write([]byte("x")); n != 0 || err == nil || killed != 2 {
		t.Fatalf("write after the overflow = %d, %v, killed %d times", n, err, killed)
	}
	if got := len(l.String()); got != 60 || !l.overflow {
		t.Fatalf("held %d bytes, want 60; overflow %v", got, l.overflow)
	}
}

// What the last chunk leaves unused stays within a quarter of the output, and
// no chunk is larger than 8 MiB (the sizes are numbers, not the constants).
// The checks run after every write of 32 KiB up to 48 MiB.
func TestLimitedBufferChunksStayWithinAQuarter(t *testing.T) {
	piece := make([]byte, 32<<10)
	var l limitedBuffer
	for written := len(piece); written <= 48<<20; written += len(piece) {
		if _, err := l.Write(piece); err != nil {
			t.Fatal(err)
		}
		if l.n != written || len(l.chunks) > maxChunks {
			t.Fatalf("after %d bytes written the buffer holds %d bytes in %d chunks, want the same bytes in at most %d", written, l.n, len(l.chunks), maxChunks)
		}
		last := l.chunks[len(l.chunks)-1]
		if unused := cap(last) - len(last); unused > l.n/4 {
			t.Fatalf("after %d bytes the last chunk leaves %d unused, more than a quarter of the output", l.n, unused)
		}
		if cap(last) > 8<<20 {
			t.Fatalf("after %d bytes a chunk holds %d bytes, more than 8 MiB", l.n, cap(last))
		}
	}
}

// The output is copied once, by String: writing 16 MiB in 32 KiB pieces and
// joining it allocates the chunks and the string, twice the output and what
// the last chunk leaves unused. A bytes.Buffer allocated 3 to 4 times as
// much: it doubles by copying, and its String copied the output again.
func TestLimitedBufferAllocatesTwiceItsOutput(t *testing.T) {
	const size = 16 << 20
	data := output(size)
	var got string
	n := allocated(func() {
		var l limitedBuffer
		for rest := data; len(rest) > 0; rest = rest[min(32<<10, len(rest)):] {
			if _, err := l.Write(rest[:min(32<<10, len(rest))]); err != nil {
				t.Fatal(err)
			}
		}
		got = l.String()
	})
	if len(got) != size {
		t.Fatalf("held %d bytes, want %d", len(got), size)
	}
	if limit := uint64(size * 5 / 2); n > limit {
		t.Fatalf("allocated %d MiB for %d MiB of output (%.2f times), want at most 2.5 times", n>>20, size>>20, float64(n)/size)
	}
}

// A small output, as most git calls print, costs a few allocations: the
// chunk, the list of chunks and the string. It costs the same when it comes in
// pieces, because the first chunk holds 64 bytes.
func TestLimitedBufferSmallOutputAllocations(t *testing.T) {
	line := []byte("0123456789012345678901234567890123456789\n")
	var s string
	call := func() {
		var l limitedBuffer
		_, _ = l.Write(line)
		s = l.String()
	}
	if n := testing.AllocsPerRun(100, call); n > 3 {
		t.Fatalf("%v allocations for a 41-byte output, want at most 3", n)
	}
	inPieces := func() {
		var l limitedBuffer
		for i := range line {
			_, _ = l.Write(line[i : i+1])
		}
		s = l.String()
	}
	if n := testing.AllocsPerRun(100, inPieces); n > 3 {
		t.Fatalf("%v allocations for a 41-byte output written a byte at a time, want at most 3", n)
	}
	if n := allocated(func() {
		for range 100 {
			call()
		}
	}) / 100; n > 256 {
		t.Fatalf("%d bytes allocated for a 41-byte output, want at most 256", n)
	}
	if s != string(line) {
		t.Fatalf("held %q", s)
	}
}

// A large output costs one chunk per quarter of what is held, and the list of
// them: 20 allocations for 1 MiB and 33 for 16 MiB, written in 32 KiB pieces
// as the pipe delivers it. A chunk smaller than its write splits the write,
// and the same outputs then cost 50 and 62.
func TestLimitedBufferAllocationsAtSize(t *testing.T) {
	for _, c := range []struct {
		size  int
		limit float64
	}{{1 << 20, 30}, {16 << 20, 45}} {
		t.Run(strconv.Itoa(c.size), func(t *testing.T) {
			data := output(c.size)
			var s string
			n := testing.AllocsPerRun(3, func() {
				var l limitedBuffer
				for rest := data; len(rest) > 0; rest = rest[min(32<<10, len(rest)):] {
					_, _ = l.Write(rest[:min(32<<10, len(rest))])
				}
				s = l.String()
			})
			if n > c.limit {
				t.Fatalf("%v allocations for %d bytes written in 32 KiB pieces, want at most %v", n, c.size, c.limit)
			}
			if len(s) != c.size {
				t.Fatalf("held %d bytes, want %d", len(s), c.size)
			}
		})
	}
}

// Run, with a budget, holds the output of one git call once more than the
// string it returns: a blob of 16 MiB costs about 2 times its size, where it
// cost 3 to 4 times (the peak of the process, 3.2 times on a real listing).
func TestRunAllocatesTwiceItsOutput(t *testing.T) {
	const size = 16 << 20
	data := output(size)
	dir := t.TempDir()
	mustGit(t, dir, "init", "-q", "--bare", ".")
	file := filepath.Join(t.TempDir(), "blob")
	if err := os.WriteFile(file, data, 0o600); err != nil {
		t.Fatal(err)
	}
	sha := strings.TrimSpace(mustGit(t, dir, "hash-object", "-w", file))
	ctx, b := budgeted(t, 2*size)
	var out string
	var err error
	n := allocated(func() {
		out, err = Run(ctx, Cmd{Class: Heavy, Dir: dir, Args: []string{"cat-file", "blob", sha}})
	})
	if err != nil || out != string(data) {
		t.Fatalf("Run: %d bytes, err %v; want %d bytes", len(out), err, size)
	}
	if left := b.remaining(); left != size {
		t.Fatalf("allowance left %d, want %d: the output counts once", left, size)
	}
	if limit := uint64(size * 5 / 2); n > limit {
		t.Fatalf("Run allocated %d MiB for %d MiB of output (%.2f times), want at most 2.5 times", n>>20, size>>20, float64(n)/size)
	}
}

// BenchmarkLimitedBuffer is the buffering of one git call's output, as the
// pipe delivers it (32 KiB pieces) and Run returns it: 41 bytes, as most
// calls print, and 1 MiB and 16 MiB, as a listing of a large tree does. The
// bytes/op and allocs/op show the copies of the output.
func BenchmarkLimitedBuffer(b *testing.B) {
	for _, size := range []int{41, 1 << 20, 16 << 20} {
		data := output(size)
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(size))
			for b.Loop() {
				var l limitedBuffer
				for rest := data; len(rest) > 0; rest = rest[min(32<<10, len(rest)):] {
					_, _ = l.Write(rest[:min(32<<10, len(rest))])
				}
				if s := l.String(); len(s) != size {
					b.Fatalf("held %d bytes, want %d", len(s), size)
				}
			}
		})
	}
}
