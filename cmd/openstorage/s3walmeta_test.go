package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/ref/s3wal"
)

const s3walMetaTestKey = "_wal/meta.json"

// newV1Bucket stores a minimal v1 metadata document, as a v1 release
// writes it.
func newV1Bucket(t *testing.T) *s3wal.MemCAS {
	t.Helper()
	cas := s3wal.NewMemCAS()
	body := `{"v":1,"seq":1,"writer":"0123456789abcdef0123456789abcdef","next_org_id":0,"next_key_id":0}`
	if _, err := cas.Create(context.Background(), s3walMetaTestKey, []byte(body)); err != nil {
		t.Fatal(err)
	}
	return cas
}

func storedVersion(t *testing.T, cas s3wal.ConditionalStore) string {
	t.Helper()
	obj, err := cas.Get(context.Background(), s3walMetaTestKey)
	if err != nil {
		t.Fatal(err)
	}
	return string(obj.Body[:6])
}

func TestS3WALMetaConvertCommand(t *testing.T) {
	ctx := context.Background()
	cas := newV1Bucket(t)
	var out bytes.Buffer
	if err := runS3WALMetaConvert(ctx, cas, 2, s3wal.LeaseConfig{}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "converted version 1 -> 2 (seq 1 -> 2), verified") || storedVersion(t, cas) != `{"v":2` {
		t.Fatalf("output %q, stored %s", out.String(), storedVersion(t, cas))
	}
	if _, err := cas.Get(ctx, s3wal.LeaseKey); !errors.Is(err, s3wal.ErrNotExist) {
		t.Fatalf("lease after convert = %v, want released", err)
	}
	out.Reset()
	if err := runS3WALMetaConvert(ctx, cas, 2, s3wal.LeaseConfig{}, &out); err != nil || !strings.Contains(out.String(), "already at version 2") {
		t.Fatalf("re-run = %q, %v", out.String(), err)
	}
	out.Reset()
	if err := runS3WALMetaConvert(ctx, cas, 1, s3wal.LeaseConfig{}, &out); err != nil || storedVersion(t, cas) != `{"v":1` {
		t.Fatalf("downgrade = %q, %v", out.String(), err)
	}
}

// A running serve holds a fresh lease: the command refuses and writes
// nothing.
func TestS3WALMetaConvertRefusesHeldLease(t *testing.T) {
	ctx := context.Background()
	cas := newV1Bucket(t)
	serve, err := s3wal.AcquireInstanceLease(ctx, cas, s3wal.LeaseConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer serve.Release(ctx)
	err = runS3WALMetaConvert(ctx, cas, 2, s3wal.LeaseConfig{}, &bytes.Buffer{})
	if _, ok := errors.AsType[*s3wal.LeaseHeldError](err); !ok || !strings.Contains(err.Error(), "stop serve first") {
		t.Fatalf("convert under a held lease = %v", err)
	}
	if v := storedVersion(t, cas); v != `{"v":1` {
		t.Fatalf("stored %s after refusal", v)
	}
}

// slowGetCAS delays reads of the metadata document.
type slowGetCAS struct {
	*s3wal.MemCAS
	delay time.Duration
}

func (s *slowGetCAS) Get(ctx context.Context, key string) (s3wal.Object, error) {
	if key == s3walMetaTestKey {
		time.Sleep(s.delay)
	}
	return s.MemCAS.Get(ctx, key)
}

// A renewal that fails while the conversion reads cancels it before its
// write.
func TestS3WALMetaConvertStopsOnLeaseLoss(t *testing.T) {
	cas := newV1Bucket(t)
	cas.Hooks.PutFault = func(op, key string) s3wal.PutFault {
		if key == s3wal.LeaseKey && op == "advance" {
			return s3wal.FaultContention // every renewal fails
		}
		return s3wal.FaultNone
	}
	slow := &slowGetCAS{MemCAS: cas, delay: 300 * time.Millisecond}
	err := runS3WALMetaConvert(context.Background(), slow, 2, s3wal.LeaseConfig{TTL: 40 * time.Millisecond, Cadence: 10 * time.Millisecond}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "instance lease lost") {
		t.Fatalf("convert = %v, want a lease loss", err)
	}
	if v := storedVersion(t, cas); v != `{"v":1` {
		t.Fatalf("stored %s after lease loss", v)
	}
}
