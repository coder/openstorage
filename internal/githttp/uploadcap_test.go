package githttp

// Unit tests for the upload-pack negotiation spool (issue #70,
// uploadcap.go): memory-tier round-trips, spill-tier round-trips, and
// error propagation. The cap wiring itself is pinned end-to-end in
// uploadcap_e2e_test.go.

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestBufferUploadPackBody_MemoryTierRoundTrip(t *testing.T) {
	want := []byte("0032want 1234567812345678123456781234567812345678\n00000009done\n")
	buffered, err := bufferUploadPackBody(bytes.NewReader(want))
	if err != nil {
		t.Fatalf("buffer: %v", err)
	}
	defer buffered.Close()
	if buffered.spill != nil {
		t.Fatal("small body spilled to disk")
	}
	got, err := io.ReadAll(buffered)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("round-trip mismatch: got %d bytes, want %d", len(got), len(want))
	}
}

func TestBufferUploadPackBody_SpillTierRoundTrip(t *testing.T) {
	// Incompressible bytes across the memory/spill boundary: the read-back
	// must preserve content and order exactly.
	want := make([]byte, uploadPackMemBufferMax+512<<10)
	if _, err := rand.Read(want); err != nil {
		t.Fatal(err)
	}
	buffered, err := bufferUploadPackBody(bytes.NewReader(want))
	if err != nil {
		t.Fatalf("buffer: %v", err)
	}
	defer buffered.Close()
	if buffered.spill == nil {
		t.Fatal("oversized body did not spill")
	}
	got, err := io.ReadAll(buffered)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("round-trip mismatch: got %d bytes, want %d", len(got), len(want))
	}
}

func TestBufferUploadPackBody_ExactMemoryBoundary(t *testing.T) {
	want := bytes.Repeat([]byte{0xab}, uploadPackMemBufferMax)
	buffered, err := bufferUploadPackBody(bytes.NewReader(want))
	if err != nil {
		t.Fatalf("buffer: %v", err)
	}
	defer buffered.Close()
	got, err := io.ReadAll(buffered)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("round-trip mismatch: got %d bytes, want %d", len(got), len(want))
	}
}

func TestBufferUploadPackBody_CapOverflowPropagates(t *testing.T) {
	// A capped stream that overflows mid-spool: the error must surface so
	// the handler can consult the counters, in both tiers.
	for name, size := range map[string]int64{"memory": 16, "spill": uploadPackMemBufferMax + 16} {
		t.Run(name, func(t *testing.T) {
			capped := newCappedReader(strings.NewReader(strings.Repeat("x", int(size)+1)), size)
			buffered, err := bufferUploadPackBody(capped)
			if !errors.Is(err, errPushTooLarge) {
				t.Fatalf("err = %v, want errPushTooLarge", err)
			}
			if buffered != nil {
				t.Fatal("overflowing spool returned a body")
			}
			if !capped.exceeded {
				t.Fatal("cap counter did not trip")
			}
		})
	}
}
