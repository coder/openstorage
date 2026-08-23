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
	"os"
	"path/filepath"
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

func TestBufferUploadPackBody_SpoolCreateFailureTagged(t *testing.T) {
	previous := createUploadPackTemp
	createUploadPackTemp = func() (*os.File, error) {
		return nil, os.ErrPermission
	}
	t.Cleanup(func() { createUploadPackTemp = previous })

	buffered, err := bufferUploadPackBody(bytes.NewReader(make([]byte, uploadPackMemBufferMax+1)))
	if !errors.Is(err, errUploadPackSpool) {
		t.Fatalf("err = %v, want errUploadPackSpool", err)
	}
	if buffered != nil {
		t.Fatal("failed spool returned a body")
	}
}

func TestBufferUploadPackBody_SpoolWriteFailureTagged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "read-only-spill")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	previous := createUploadPackTemp
	createUploadPackTemp = func() (*os.File, error) {
		return os.Open(path) // read-only: the first spill write fails
	}
	t.Cleanup(func() { createUploadPackTemp = previous })

	buffered, err := bufferUploadPackBody(bytes.NewReader(make([]byte, uploadPackMemBufferMax+1)))
	if !errors.Is(err, errUploadPackSpool) {
		t.Fatalf("err = %v, want errUploadPackSpool", err)
	}
	if buffered != nil {
		t.Fatal("failed spool returned a body")
	}
}

func TestBufferUploadPackBody_SourceFailureNotTaggedAsSpool(t *testing.T) {
	sourceErr := errors.New("corrupt gzip stream")
	r := io.MultiReader(
		bytes.NewReader(make([]byte, uploadPackMemBufferMax+1)),
		errorReader{err: sourceErr},
	)
	buffered, err := bufferUploadPackBody(r)
	if !errors.Is(err, sourceErr) {
		t.Fatalf("err = %v, want source error", err)
	}
	if errors.Is(err, errUploadPackSpool) {
		t.Fatalf("source error was mislabeled as spool failure: %v", err)
	}
	if buffered != nil {
		t.Fatal("failed source returned a body")
	}
}

type errorReader struct {
	err error
}

func (r errorReader) Read([]byte) (int, error) {
	return 0, r.err
}
