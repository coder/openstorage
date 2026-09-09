// Org public-key snapshot coverage (PR #168 security P2):
// PublicKeysForOrg is the verifier's PRE-signature key selection, driven
// by the unverified iss claim of every bearer token, so once populated it
// must be served from memory — never a synchronous store read an
// unauthenticated JWT flood could amplify into S3 round-trips. These
// tests prove the populated lookup path survives a fully faulted read
// substrate, that local registrations are visible to the very next
// lookup, that foreign writes arrive through the background refresher
// within its interval, and that Close joins the refresher goroutine.

package meta

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/ref/s3wal"
)

// errInjectedRead marks a read the store must never have depended on.
var errInjectedRead = errors.New("injected read fault")

// failingReadCAS fails every Get/GetIfChanged while armed — the fault
// oracle proving a code path performs no synchronous store read.
type failingReadCAS struct {
	s3wal.ConditionalStore
	fail atomic.Bool
}

func (c *failingReadCAS) Get(ctx context.Context, key string) (s3wal.Object, error) {
	if c.fail.Load() {
		return s3wal.Object{}, errInjectedRead
	}
	return c.ConditionalStore.Get(ctx, key)
}

func (c *failingReadCAS) GetIfChanged(ctx context.Context, key, etag string) (s3wal.Object, error) {
	if c.fail.Load() {
		return s3wal.Object{}, errInjectedRead
	}
	return c.ConditionalStore.GetIfChanged(ctx, key, etag)
}

func TestS3WALPublicKeysNoSynchronousRead(t *testing.T) {
	ctx := t.Context()
	cas := &failingReadCAS{ConditionalStore: s3wal.NewMemCAS()}
	s := NewS3WAL(cas)
	t.Cleanup(s.Close)

	if _, err := s.UpsertOrgWithKeys(ctx, "org", []string{"pem-a"}); err != nil {
		t.Fatal(err)
	}
	cas.fail.Store(true)

	keys, err := s.PublicKeysForOrg(ctx, "org")
	if err != nil || !slices.Equal(keys, []string{"pem-a"}) {
		t.Fatalf("PublicKeysForOrg with reads faulted = (%q, %v), want ([pem-a], nil) — key lookups must not touch the store", keys, err)
	}
	if _, err := s.PublicKeysForOrg(ctx, "no-such-org"); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown org with reads faulted: err = %v, want ErrNotFound from the snapshot", err)
	}
	// Sanity: revalidating read paths DO depend on the store, so the
	// fault is real and the lookups above genuinely bypassed it.
	if _, err := s.OrgNames(ctx); !errors.Is(err, errInjectedRead) {
		t.Fatalf("OrgNames with reads faulted: err = %v, want the injected fault", err)
	}

	// A local key replacement is visible to the very NEXT lookup — the
	// write path installs the snapshot synchronously, still no store read.
	cas.fail.Store(false)
	if _, err := s.UpsertOrgWithKeys(ctx, "org", []string{"pem-b"}); err != nil {
		t.Fatal(err)
	}
	cas.fail.Store(true)
	keys, err = s.PublicKeysForOrg(ctx, "org")
	if err != nil || !slices.Equal(keys, []string{"pem-b"}) {
		t.Fatalf("PublicKeysForOrg after local replace = (%q, %v), want ([pem-b], nil)", keys, err)
	}
}

func TestS3WALPublicKeysForeignWriteRefresh(t *testing.T) {
	ctx := t.Context()
	inner := s3wal.NewMemCAS()
	a := NewS3WAL(inner, withKeyRefreshInterval(5*time.Millisecond))
	b := NewS3WAL(inner) // a foreign writer bypassing a's snapshot
	t.Cleanup(func() { a.Close(); b.Close() })

	if _, err := b.UpsertOrgWithKeys(ctx, "org", []string{"pem-a"}); err != nil {
		t.Fatal(err)
	}
	// Populate a's snapshot, then prove lookups really serve it: the
	// not-yet-written org misses without consulting the store.
	if _, err := a.PublicKeysForOrg(ctx, "org"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.PublicKeysForOrg(ctx, "org-late"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("pre-write lookup: err = %v, want ErrNotFound", err)
	}

	if _, err := b.UpsertOrgWithKeys(ctx, "org-late", []string{"pem-late"}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		keys, err := a.PublicKeysForOrg(ctx, "org-late")
		if err == nil {
			if !slices.Equal(keys, []string{"pem-late"}) {
				t.Fatalf("keys = %q, want [pem-late]", keys)
			}
			return
		}
		if !errors.Is(err, ErrNotFound) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("foreign key registration never became visible through the background refresher")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestS3WALCloseJoinsKeyRefresher(t *testing.T) {
	s := NewS3WAL(s3wal.NewMemCAS())
	s.Close()
	select {
	case <-s.refreshDone: // Close returned ⇒ the goroutine already exited
	default:
		t.Fatal("org-key refresher still running after Close")
	}
	s.Close() // idempotent
}
