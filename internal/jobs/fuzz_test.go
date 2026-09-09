// Fuzz targets for the pack-metadata decoders in package jobs (issue #91):
// packHeaderObjectCount and packObjectCount steer repack fold selection from
// bytes read out of blob storage. Registered packs are validated at write
// time, so corrupt bytes here mean blob corruption — the decoders must fail
// loudly (an error), never panic, and never accept a malformed header.
//
// Oracle note: both targets assert the exact shape checks the functions
// promise (magic, version, fixed-width count field). That is deliberately
// weaker than git index-pack, which additionally verifies the header count
// against the pack's actual object stream and trailer checksum; the repack
// path closes that gap by cross-checking header counts against idx fanouts
// in materializeFold.
package jobs

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/coder/openstorage/internal/blob"
)

// fuzzHeaderStore serves one in-memory blob to packHeaderObjectCount's
// single ReadRange call, mirroring bucketReadRange's exact-range contract
// (a short blob errors instead of returning truncated bytes). The embedded
// nil BlobStore panics on any other method — the target must never need one.
type fuzzHeaderStore struct {
	blob.BlobStore
	data []byte
}

func (s fuzzHeaderStore) ReadRange(_ context.Context, _ string, offset, length int64) ([]byte, error) {
	if offset < 0 || length < 0 || offset+length > int64(len(s.data)) {
		return nil, fmt.Errorf("read [%d,%d) out of range (blob is %d bytes)", offset, offset+length, len(s.data))
	}
	return s.data[offset : offset+length], nil
}

func FuzzPackHeaderObjectCount(f *testing.F) {
	hdr := func(magic string, version, count uint32) []byte {
		b := make([]byte, packHeaderLen)
		copy(b, magic)
		binary.BigEndian.PutUint32(b[4:], version)
		binary.BigEndian.PutUint32(b[8:], count)
		return b
	}
	for _, seed := range [][]byte{
		nil,
		[]byte("PACK"),
		hdr("PACK", 2, 0),
		hdr("PACK", 2, 1234),
		hdr("PACK", 3, math.MaxUint32),
		hdr("PACK", 4, 1),
		hdr("KCAP", 2, 1),
		append(hdr("PACK", 2, 7), "trailing pack data"...),
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		count, err := packHeaderObjectCount(context.Background(), fuzzHeaderStore{data: data}, "packs/fuzz/key")
		wellFormed := len(data) >= packHeaderLen &&
			bytes.Equal(data[:4], []byte("PACK")) &&
			(binary.BigEndian.Uint32(data[4:8]) == 2 || binary.BigEndian.Uint32(data[4:8]) == 3)
		if !wellFormed {
			if err == nil {
				t.Fatalf("accepted a corrupt pack header (%d bytes) with count %d", len(data), count)
			}
			return
		}
		if err != nil {
			t.Fatalf("rejected a well-formed pack header: %v", err)
		}
		if want := int64(binary.BigEndian.Uint32(data[8:packHeaderLen])); count != want {
			t.Fatalf("count = %d, want header field %d", count, want)
		}
		if count < 0 || count > math.MaxUint32 {
			t.Fatalf("count %d outside the uint32 header range", count)
		}
	})
}

func FuzzPackObjectCount(f *testing.F) {
	const fanoutEnd = 8 + 256*4 // idx header + full fanout table
	idxMagic := []byte{0xff, 0x74, 0x4f, 0x63}
	idx := func(version, count uint32, pad int) []byte {
		b := make([]byte, fanoutEnd+pad)
		copy(b, idxMagic)
		binary.BigEndian.PutUint32(b[4:], version)
		binary.BigEndian.PutUint32(b[8+255*4:], count)
		return b
	}
	for _, seed := range [][]byte{
		nil,
		[]byte("not an idx"),
		idx(2, 0, 0),
		idx(2, 42, 128),
		idx(1, 1, 0),
		idx(3, 1, 0),
		append(append([]byte{}, idxMagic...), 0, 0, 0, 2), // header only, fanout missing
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		path := filepath.Join(t.TempDir(), "pack-fuzz.idx")
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		count, err := packObjectCount(path)
		wellFormed := len(data) >= fanoutEnd &&
			bytes.Equal(data[:4], idxMagic) &&
			binary.BigEndian.Uint32(data[4:8]) == 2
		if !wellFormed {
			if err == nil {
				t.Fatalf("accepted a corrupt idx (%d bytes) with count %d", len(data), count)
			}
			return
		}
		if err != nil {
			t.Fatalf("rejected a well-formed v2 idx: %v", err)
		}
		if want := int(binary.BigEndian.Uint32(data[8+255*4:])); count != want || count < 0 {
			t.Fatalf("count = %d, want fanout[255] = %d (non-negative)", count, want)
		}
	})
}
