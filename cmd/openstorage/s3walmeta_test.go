package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/meta"
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
	if err := runS3WALMetaConvert(ctx, cas, 2, meta.S3WALConvertOptions{}, s3wal.LeaseConfig{}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "converted version 1 -> 2 (seq 1 -> 2), verified") || storedVersion(t, cas) != `{"v":2` {
		t.Fatalf("output %q, stored %s", out.String(), storedVersion(t, cas))
	}
	if _, err := cas.Get(ctx, s3wal.LeaseKey); !errors.Is(err, s3wal.ErrNotExist) {
		t.Fatalf("lease after convert = %v, want released", err)
	}
	out.Reset()
	if err := runS3WALMetaConvert(ctx, cas, 2, meta.S3WALConvertOptions{}, s3wal.LeaseConfig{}, &out); err != nil || !strings.Contains(out.String(), "already at version 2") {
		t.Fatalf("re-run = %q, %v", out.String(), err)
	}
	out.Reset()
	if err := runS3WALMetaConvert(ctx, cas, 1, meta.S3WALConvertOptions{}, s3wal.LeaseConfig{}, &out); err != nil || storedVersion(t, cas) != `{"v":1` {
		t.Fatalf("downgrade = %q, %v", out.String(), err)
	}
}

// --to 1 with an unfinished fork refuses, names the repo and the way out,
// and releases the lease. --force converts and reports both kinds of
// unfinished creates.
func TestS3WALMetaConvertPendingCreatesCommand(t *testing.T) {
	ctx := context.Background()
	cas := s3wal.NewMemCAS()
	st := meta.NewS3WAL(cas)
	defer st.Close()
	if _, err := st.UpsertOrgWithKeys(ctx, "org", []string{"pem"}); err != nil {
		t.Fatal(err)
	}
	fork, err := st.CreateRepoWithState(ctx, "org", "fork-pending", "main", meta.CreateStatePending)
	if err != nil {
		t.Fatal(err)
	}
	sync, err := st.CreateLinkedRepo(ctx, "org", "sync-pending", "main",
		meta.RepoUpstream{Provider: "github", Owner: "acme", Name: "demo", UpstreamHost: "github.com"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	err = runS3WALMetaConvert(ctx, cas, 1, meta.S3WALConvertOptions{}, s3wal.LeaseConfig{}, &out)
	if err == nil || !strings.Contains(err.Error(), fork.RepoID+" (fork-pending)") || !strings.Contains(err.Error(), "DELETE /repos/delete") || storedVersion(t, cas) != `{"v":2` {
		t.Fatalf("refusal = %v, stored %s; want the fork named and v2 kept", err, storedVersion(t, cas))
	}
	if _, err := cas.Get(ctx, s3wal.LeaseKey); !errors.Is(err, s3wal.ErrNotExist) {
		t.Fatalf("lease after refusal = %v, want released", err)
	}
	if err := runS3WALMetaConvert(ctx, cas, 1, meta.S3WALConvertOptions{Force: true}, s3wal.LeaseConfig{}, &out); err != nil || storedVersion(t, cas) != `{"v":1` {
		t.Fatalf("forced = %q, %v", out.String(), err)
	}
	for _, want := range []string{"first sync requested (the v1 release runs it at boot): " + sync.RepoID, "--force: 1 unfinished fork or copy creates", fork.RepoID + " (fork-pending)"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("forced output %q lacks %q", out.String(), want)
		}
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
	err = runS3WALMetaConvert(ctx, cas, 2, meta.S3WALConvertOptions{}, s3wal.LeaseConfig{}, &bytes.Buffer{})
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
	err := runS3WALMetaConvert(context.Background(), slow, 2, meta.S3WALConvertOptions{}, s3wal.LeaseConfig{TTL: 40 * time.Millisecond, Cadence: 10 * time.Millisecond}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "instance lease lost") {
		t.Fatalf("convert = %v, want a lease loss", err)
	}
	if v := storedVersion(t, cas); v != `{"v":1` {
		t.Fatalf("stored %s after lease loss", v)
	}
}

// cancelGetCAS cancels the command's context during the metadata read, as
// SIGINT does through signal.NotifyContext.
type cancelGetCAS struct {
	*s3wal.MemCAS
	cancel context.CancelFunc
}

func (c *cancelGetCAS) Get(ctx context.Context, key string) (s3wal.Object, error) {
	if key == s3walMetaTestKey {
		c.cancel()
	}
	return c.MemCAS.Get(ctx, key)
}

// An interrupted conversion writes nothing and releases its lease, so serve
// can start at once.
func TestS3WALMetaConvertInterruptReleasesLease(t *testing.T) {
	cas := newV1Bucket(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := runS3WALMetaConvert(ctx, &cancelGetCAS{MemCAS: cas, cancel: cancel}, 2, meta.S3WALConvertOptions{}, s3wal.LeaseConfig{}, &bytes.Buffer{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted convert = %v, want context.Canceled", err)
	}
	if v := storedVersion(t, cas); v != `{"v":1` {
		t.Fatalf("stored %s after interrupt", v)
	}
	if _, err := cas.Get(context.Background(), s3wal.LeaseKey); !errors.Is(err, s3wal.ErrNotExist) {
		t.Fatalf("lease after interrupt = %v, want released", err)
	}
}
