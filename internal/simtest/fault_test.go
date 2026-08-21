package simtest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"codestorage/internal/blob"
	"codestorage/internal/ref"
)

const (
	shaA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	shaB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// openRefStore opens a fresh SQLite ref store under t.TempDir.
func openRefStore(t *testing.T) ref.Store {
	t.Helper()
	ctx := context.Background()
	st, err := ref.OpenSQLite(ctx, "sqlite://"+filepath.Join(t.TempDir(), "refs.sqlite"))
	if err != nil {
		t.Fatalf("open sqlite ref store: %v", err)
	}
	t.Cleanup(st.Close)
	if err := st.InitSchema(ctx); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	return st
}

// openBlobStore opens a fresh fileblob store under t.TempDir.
func openBlobStore(t *testing.T) blob.BlobStore {
	t.Helper()
	bl, err := blob.OpenFile("file://" + t.TempDir())
	if err != nil {
		t.Fatalf("open fileblob store: %v", err)
	}
	t.Cleanup(func() { _ = bl.Close() })
	return bl
}

func TestSimRefCleanFaultDoesNotExecute(t *testing.T) {
	ctx := context.Background()
	inner := openRefStore(t)
	inj := NewInjector(1)
	st := NewSimRefStore(inner, inj)

	inj.FailNext(OpRefCASRef, FaultClean)
	won, err := st.CASRef(ctx, "r", "refs/heads/main", ref.ZeroSHA, shaA)
	if won || !errors.Is(err, ErrInjectedClean) {
		t.Fatalf("CASRef = (%v, %v), want clean-fault error", won, err)
	}
	// The injected clean error models "statement never sent": it must
	// satisfy pgconn's SafeToRetry so ref.IdempotentCAS never recovers it.
	if !pgconn.SafeToRetry(err) {
		t.Errorf("clean fault error not SafeToRetry: %v", err)
	}
	// Ground truth: the ref must NOT exist.
	if _, err := inner.GetRef(ctx, "r", "refs/heads/main"); !errors.Is(err, ref.ErrNotFound) {
		t.Errorf("GetRef after clean fault = %v, want ErrNotFound", err)
	}
}

func TestSimRefAmbiguousFaultExecutes(t *testing.T) {
	ctx := context.Background()
	inner := openRefStore(t)
	inj := NewInjector(1)
	st := NewSimRefStore(inner, inj)

	inj.FailNext(OpRefCASRef, FaultAmbiguous)
	won, err := st.CASRef(ctx, "r", "refs/heads/main", ref.ZeroSHA, shaA)
	if won || !errors.Is(err, ErrInjectedAmbiguous) {
		t.Fatalf("CASRef = (%v, %v), want ambiguous-fault error", won, err)
	}
	if pgconn.SafeToRetry(err) {
		t.Errorf("ambiguous fault error must not be SafeToRetry: %v", err)
	}
	// Ground truth: the create COMMITTED even though the caller saw an error.
	got, err := inner.GetRef(ctx, "r", "refs/heads/main")
	if err != nil || got != shaA {
		t.Errorf("GetRef after ambiguous fault = (%q, %v), want %q", got, err, shaA)
	}
	fired := inj.Fired()
	if len(fired) != 1 || fired[0].InnerOutcome != "won=true err=<nil>" {
		t.Errorf("fired = %+v, want one event with inner outcome won=true", fired)
	}
}

// TestIdempotentCASOverSimStore ties phase 1 to issue #2's contract: the
// production recovery decorator must recover an injected ambiguous fault
// whose statement committed, and must NOT recover a clean fault.
func TestIdempotentCASOverSimStore(t *testing.T) {
	ctx := context.Background()
	inner := openRefStore(t)
	inj := NewInjector(1)
	st := ref.IdempotentCAS{RefStore: NewSimRefStore(inner, inj)}

	if won, err := st.CASRef(ctx, "r", "refs/heads/main", ref.ZeroSHA, shaA); !won || err != nil {
		t.Fatalf("setup create = (%v, %v)", won, err)
	}

	// Ambiguous fault on an update that really commits: recovered to a win.
	inj.FailNext(OpRefCASRef, FaultAmbiguous)
	won, err := st.CASRef(ctx, "r", "refs/heads/main", shaA, shaB)
	if !won || err != nil {
		t.Errorf("ambiguous committed update = (%v, %v), want recovered win", won, err)
	}

	// Clean fault: definitive non-execution, must propagate un-recovered
	// even though a recheck would show... nothing useful (ref is at shaB).
	inj.FailNext(OpRefCASRef, FaultClean)
	won, err = st.CASRef(ctx, "r", "refs/heads/main", shaB, shaA)
	if won || !errors.Is(err, ErrInjectedClean) {
		t.Errorf("clean fault = (%v, %v), want propagated clean error", won, err)
	}
	if got, _ := inner.GetRef(ctx, "r", "refs/heads/main"); got != shaB {
		t.Errorf("ref moved on clean fault: %q", got)
	}

	// Ambiguous fault on an update whose recheck is ALSO faulted: recovery
	// is starved, the ambiguous error stands.
	inj.FailNext(OpRefCASRef, FaultAmbiguous)
	inj.FailNext(OpRefGetRef, FaultClean)
	won, err = st.CASRef(ctx, "r", "refs/heads/main", shaB, shaA)
	if won || !errors.Is(err, ErrInjectedAmbiguous) {
		t.Errorf("starved recovery = (%v, %v), want ambiguous error", won, err)
	}
	// ...but ground truth shows the update committed.
	if got, _ := inner.GetRef(ctx, "r", "refs/heads/main"); got != shaA {
		t.Errorf("ground truth after starved recovery = %q, want %q", got, shaA)
	}
}

func TestFailNthCountsFromNow(t *testing.T) {
	ctx := context.Background()
	inner := openRefStore(t)
	inj := NewInjector(1)
	st := NewSimRefStore(inner, inj)

	inj.FailNth(OpRefGetRef, 3, FaultClean)
	for i := 1; i <= 2; i++ {
		if _, err := st.GetRef(ctx, "r", "refs/heads/main"); !errors.Is(err, ref.ErrNotFound) {
			t.Fatalf("call %d: unexpected fault: %v", i, err)
		}
	}
	if _, err := st.GetRef(ctx, "r", "refs/heads/main"); !errors.Is(err, ErrInjectedClean) {
		t.Fatalf("3rd call = %v, want injected clean fault", err)
	}
	if _, err := st.GetRef(ctx, "r", "refs/heads/main"); !errors.Is(err, ref.ErrNotFound) {
		t.Fatalf("4th call: fault not one-shot: %v", err)
	}
	if got := inj.Count(OpRefGetRef); got != 4 {
		t.Errorf("Count(GetRef) = %d, want 4", got)
	}
}

func TestProbabilisticFaultsAreSeedDeterministic(t *testing.T) {
	ctx := context.Background()
	run := func(seed uint64) []FaultEvent {
		inner := openRefStore(t)
		inj := NewInjector(seed)
		inj.SetProbs(OpRefCASRef, Probs{Clean: 0.2, Ambiguous: 0.2})
		st := NewSimRefStore(inner, inj)
		sha := shaA
		for i := 0; i < 40; i++ {
			_, _ = st.CASRef(ctx, "r", fmt.Sprintf("refs/heads/b%02d", i), ref.ZeroSHA, sha)
		}
		return inj.Fired()
	}
	a, b := run(42), run(42)
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("same seed diverged:\n%+v\n%+v", a, b)
	}
	if len(a) == 0 {
		t.Fatal("probability profile injected no faults in 40 ops (want some)")
	}
	c := run(43)
	if reflect.DeepEqual(a, c) {
		t.Fatal("different seeds produced identical fault sequences (suspicious)")
	}
}

func TestSimBlobAmbiguousUploadPublishes(t *testing.T) {
	ctx := context.Background()
	inner := openBlobStore(t)
	inj := NewInjector(1)
	bl := NewSimBlobStore(inner, inj)

	src := filepath.Join(t.TempDir(), "obj")
	if err := os.WriteFile(src, []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}

	inj.FailNext(OpBlobUploadFile, FaultAmbiguous)
	if err := bl.UploadFile(ctx, "packs/r/pack-1.pack", src); !errors.Is(err, ErrInjectedAmbiguous) {
		t.Fatalf("UploadFile = %v, want ambiguous fault", err)
	}
	// Ground truth: the object IS published.
	if size, err := inner.Stat(ctx, "packs/r/pack-1.pack"); err != nil || size != int64(len("payload")) {
		t.Errorf("Stat after ambiguous upload = (%d, %v), want published object", size, err)
	}

	inj.FailNext(OpBlobUploadFile, FaultClean)
	if err := bl.UploadFile(ctx, "packs/r/pack-2.pack", src); !errors.Is(err, ErrInjectedClean) {
		t.Fatalf("UploadFile = %v, want clean fault", err)
	}
	if _, err := inner.Stat(ctx, "packs/r/pack-2.pack"); !errors.Is(err, blob.ErrNotFound) {
		t.Errorf("clean-faulted upload published an object: %v", err)
	}
}

func TestSimBlobNotFoundFault(t *testing.T) {
	ctx := context.Background()
	inner := openBlobStore(t)
	inj := NewInjector(1)
	bl := NewSimBlobStore(inner, inj)

	src := filepath.Join(t.TempDir(), "obj")
	if err := os.WriteFile(src, []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := bl.UploadFile(ctx, "packs/r/pack-1.pack", src); err != nil {
		t.Fatal(err)
	}

	dest := filepath.Join(t.TempDir(), "out")
	inj.FailNext(OpBlobDownloadFile, FaultNotFound)
	err := bl.DownloadFile(ctx, "packs/r/pack-1.pack", dest)
	if !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("DownloadFile = %v, want blob.ErrNotFound", err)
	}
	if !strings.Contains(err.Error(), "injected") {
		t.Errorf("injected not-found error should identify itself: %v", err)
	}
	if _, err := os.Stat(dest); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("not-found fault materialized the file: %v", err)
	}
	// One-shot: the retry succeeds and downloads real bytes.
	if err := bl.DownloadFile(ctx, "packs/r/pack-1.pack", dest); err != nil {
		t.Fatalf("retry DownloadFile = %v, want success", err)
	}
	got, err := os.ReadFile(dest)
	if err != nil || string(got) != "payload" {
		t.Errorf("downloaded bytes = (%q, %v)", got, err)
	}
}

func TestSimBlobAmbiguousDownloadMaterializes(t *testing.T) {
	ctx := context.Background()
	inner := openBlobStore(t)
	inj := NewInjector(1)
	bl := NewSimBlobStore(inner, inj)

	src := filepath.Join(t.TempDir(), "obj")
	if err := os.WriteFile(src, []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := bl.UploadFile(ctx, "k", src); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "out")
	inj.FailNext(OpBlobDownloadFile, FaultAmbiguous)
	if err := bl.DownloadFile(ctx, "k", dest); !errors.Is(err, ErrInjectedAmbiguous) {
		t.Fatalf("DownloadFile = %v, want ambiguous fault", err)
	}
	// Ambiguous download: the file landed even though an error was reported.
	if got, err := os.ReadFile(dest); err != nil || string(got) != "payload" {
		t.Errorf("ambiguous download did not materialize: (%q, %v)", got, err)
	}
}

func TestFaultPlansQueueFIFO(t *testing.T) {
	ctx := context.Background()
	inner := openRefStore(t)
	inj := NewInjector(1)
	st := NewSimRefStore(inner, inj)

	inj.FailNext(OpRefGetRef, FaultClean)
	inj.FailNth(OpRefGetRef, 2, FaultAmbiguous) // countdown starts after the first plan fires

	if _, err := st.GetRef(ctx, "r", "n"); !errors.Is(err, ErrInjectedClean) {
		t.Fatalf("call 1 = %v, want clean", err)
	}
	if _, err := st.GetRef(ctx, "r", "n"); !errors.Is(err, ref.ErrNotFound) {
		t.Fatalf("call 2 = %v, want passthrough", err)
	}
	if _, err := st.GetRef(ctx, "r", "n"); !errors.Is(err, ErrInjectedAmbiguous) {
		t.Fatalf("call 3 = %v, want ambiguous", err)
	}
}
