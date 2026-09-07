// Phase 2 dogfooding drill (plan §4): a REAL kill -9 mid-push against a
// REAL SeaweedFS store, then clean recovery by a fresh process. The push
// runs the driver-level pipeline (reserve → pack blob PUT → RegisterPacks
// → CASRef) in a re-executed child of this test binary; the parent
// SIGKILLs it at the announced window and then proves the §2.5 recovery
// story over the surviving bucket. The full `openstorage serve` + real
// `git push` + restart-from-bucket-only drill belongs to Phase 3, when
// serve-mode s3wal wiring exists.
//
// Gated behind CODESTORAGE_S3WAL_DRILL=1: this is recorded evidence for
// the PR, not a CI lane — the deterministic equivalents run in
// s3walstore_crashwindow_test.go on every build.
package ref

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/coder/open-storage/internal/ref/s3wal"
	"github.com/coder/open-storage/internal/ref/s3wal/s3waltest"
)

const (
	drillBucket = "s3wal-drill"
	drillBase   = "drillrepo"
	drillBranch = "refs/heads/main"
	drillSHA    = "d411d411d411d411d411d411d411d411d411d411"
	drillKey    = "packs/drillrepo/000001"
)

// drillMarker prints one streamed, unbuffered progress line the parent
// keys the SIGKILL on.
func drillMarker(stage string) {
	fmt.Fprintf(os.Stdout, "DRILL: %s\n", stage)
}

// drillStore dials the drill bucket exactly like the serve path (probe at
// InitSchema).
func drillStore(t *testing.T, endpoint string) *S3WAL {
	t.Helper()
	raw := s3waltest.RawS3Client(endpoint, "us-east-1", s3waltest.SeaweedFSAccessKey, s3waltest.SeaweedFSSecretKey)
	c := s3wal.New(raw, drillBucket)
	c.DecodeSeq = s3wal.DecodeSeq
	store := newS3WALStore(c, func(ctx context.Context) error { return s3wal.Probe(ctx, c) }, c.Close)
	t.Cleanup(store.Close)
	if err := store.InitSchema(context.Background()); err != nil {
		t.Fatalf("InitSchema (probe): %v", err)
	}
	return store
}

// TestS3WALKillMidPushDrillChild is the victim process: it pushes through
// the driver pipeline against the parent's SeaweedFS, announcing each
// stage, and PARKS at the window named by the environment until the
// parent's SIGKILL lands.
func TestS3WALKillMidPushDrillChild(t *testing.T) {
	window := os.Getenv("CODESTORAGE_S3WAL_DRILL_CHILD")
	if window == "" {
		t.Skip("drill child: spawned by TestS3WALKillMidPushDrill only")
	}
	endpoint := os.Getenv("CODESTORAGE_S3WAL_DRILL_ENDPOINT")
	if endpoint == "" {
		t.Fatal("drill child: no endpoint")
	}
	ctx := context.Background()
	store := drillStore(t, endpoint)
	drillMarker("probe ok (InitSchema §2.6)")

	dec, err := store.ReservePackBytes(ctx, []string{drillBase}, []Pack{{Key: drillKey, Size: 12}}, "drill-push", 0, ReserveNoFloorGen, 1<<20, time.Hour)
	if err != nil || !dec.OK {
		t.Fatalf("reserve = (%+v, %v)", dec, err)
	}
	drillMarker("reserved quota")

	raw := s3waltest.RawS3Client(endpoint, "us-east-1", s3waltest.SeaweedFSAccessKey, s3waltest.SeaweedFSSecretKey)
	if _, err := raw.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(drillBucket), Key: aws.String(drillKey),
		Body: bytes.NewReader([]byte("drill-pack--")),
	}); err != nil {
		t.Fatalf("pack blob PUT: %v", err)
	}
	drillMarker("uploaded pack blob")
	if window == "after-upload" {
		drillMarker("parked for SIGKILL")
		time.Sleep(time.Minute)
		t.Fatal("drill child survived its kill window")
	}

	if err := store.RegisterPacks(ctx, drillBase, []Pack{{ID: "000001", Key: drillKey, Size: 12}}); err != nil {
		t.Fatalf("RegisterPacks: %v", err)
	}
	drillMarker("registered pack")
	drillMarker("parked for SIGKILL")
	time.Sleep(time.Minute)
	t.Fatal("drill child survived its kill window")
}

// TestS3WALKillMidPushDrill is the recorded drill driver: one SIGKILL in
// the W1 window (after the blob PUT) and one in the W3 window (after
// RegisterPacks), each followed by fresh-process recovery.
func TestS3WALKillMidPushDrill(t *testing.T) {
	if os.Getenv("CODESTORAGE_S3WAL_DRILL") != "1" {
		t.Skip("set CODESTORAGE_S3WAL_DRILL=1 to run the recorded kill -9 drill (deterministic twins run in TestS3WALCrashWindows)")
	}
	ctx := context.Background()
	endpoint, terminate, err := s3waltest.StartSeaweedFSContainer(ctx)
	if err != nil {
		t.Fatalf("start seaweedfs: %v", err)
	}
	t.Cleanup(terminate)
	raw := s3waltest.RawS3Client(endpoint, "us-east-1", s3waltest.SeaweedFSAccessKey, s3waltest.SeaweedFSSecretKey)
	s3waltest.MustEnsureBucket(t, raw, drillBucket)
	t.Logf("seaweedfs up at %s, bucket %s", endpoint, drillBucket)

	// killAtPark spawns the child for one window and SIGKILLs it once it
	// parks there.
	killAtPark := func(window string) {
		t.Helper()
		cmd := exec.Command(os.Args[0], "-test.run=TestS3WALKillMidPushDrillChild$", "-test.v")
		cmd.Env = append(os.Environ(),
			"CODESTORAGE_S3WAL_DRILL_CHILD="+window,
			"CODESTORAGE_S3WAL_DRILL_ENDPOINT="+endpoint,
		)
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatalf("stdout pipe: %v", err)
		}
		cmd.Stderr = cmd.Stdout
		if err := cmd.Start(); err != nil {
			t.Fatalf("start child: %v", err)
		}
		parked := false
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			line := sc.Text()
			if strings.HasPrefix(line, "DRILL: ") {
				t.Logf("child(%s) %s", window, line)
			}
			if strings.Contains(line, "parked for SIGKILL") {
				parked = true
				break
			}
		}
		if !parked {
			t.Fatalf("child(%s) never reached its kill window", window)
		}
		if err := cmd.Process.Kill(); err != nil { // SIGKILL — no cleanup runs
			t.Fatalf("kill -9 child: %v", err)
		}
		err = cmd.Wait()
		t.Logf("child(%s) killed: %v", window, err)
	}

	// --- W1: killed after the pack blob PUT, before RegisterPacks. ---
	killAtPark("after-upload")
	s1 := drillStore(t, endpoint)
	m := mustManifest(t, s1, drillBase)
	if mem := m.Members[drillBase]; mem != nil && len(mem.Packs) != 0 {
		t.Fatalf("W1: orphan blob got registered: %+v", mem.Packs)
	}
	if m.Reservations[drillKey] == nil {
		t.Fatal("W1: crashed push's reservation vanished before its TTL")
	}
	if m.UsageBytes != 0 {
		t.Fatalf("W1: usage %d, want 0 (nothing registered)", m.UsageBytes)
	}
	t.Log("W1 recovery: safe state — blob orphaned, reservation charged until TTL, no rows, usage 0")

	// --- W3: killed after RegisterPacks, before CASRef. ---
	killAtPark("after-register")
	s2 := drillStore(t, endpoint)
	packs, err := s2.ListPacks(ctx, drillBase)
	if err != nil || len(packs) != 1 || packs[0].ID != "000001" {
		t.Fatalf("W3: registered pack not durable: (%+v, %v)", packs, err)
	}
	if _, err := s2.GetRef(ctx, drillBase, drillBranch); !errors.Is(err, ErrNotFound) {
		t.Fatalf("W3: ref moved without CASRef: %v", err)
	}
	m = mustManifest(t, s2, drillBase)
	if m.UsageBytes != 12 {
		t.Fatalf("W3: usage %d, want 12 (registration consumed the reservation)", m.UsageBytes)
	}
	// The retrying client completes the push on a fresh process.
	if won, err := (IdempotentCAS{s2}).CASRef(ctx, drillBase, drillBranch, ZeroSHA, drillSHA); err != nil || !won {
		t.Fatalf("W3: recovery CASRef = (%v, %v)", won, err)
	}
	if got, err := s2.GetRef(ctx, drillBase, drillBranch); err != nil || got != drillSHA {
		t.Fatalf("W3: GetRef = (%q, %v)", got, err)
	}
	t.Log("W3 recovery: pack durable, ref unmoved until the fresh process completed the push")
	t.Log("drill complete: two kill -9 windows, zero manual repair, invariants held")
}
