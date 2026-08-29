// Spike probes for issue #201 (Floci E2E evaluation) — THROWAWAY branch
// code, never intended for main. Two extra behaviors the standing
// qualification suite (qualify_test.go) does not cover:
//
//  1. TestSpikeContentionExternalEndpoint — the concurrent-writer CAS
//     lost-update detector (same runner as the SeaweedFS lane) pointed at
//     an external endpoint via CODESTORAGE_TEST_S3WAL_URL.
//  2. TestSpikeDeleteIfMatch — DeleteObject + If-Match fidelity, in record
//     mode against the external endpoint, SeaweedFS, and the legacy MinIO
//     pin. AWS honors it on general-purpose buckets (412 on mismatch);
//     MinIO is known to silently ignore it (memory: never rely on
//     conditional DELETE as a guard). This probe establishes where each
//     emulator lands; it logs VERDICT lines and never fails on behavior.
package s3wal_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	awshttp "github.com/aws/smithy-go/transport/http"

	"github.com/coder/code-storage/internal/ref/s3wal"
)

// TestSpikeContentionExternalEndpoint mirrors
// TestContentionLostUpdateDetectorSeaweedFS against the endpoint named by
// CODESTORAGE_TEST_S3WAL_URL: 8 writers race If-Match advances on one key
// for 3 s; runContentionWindow's verifyTransitions fails the test on any
// lost update (two writers both believing they won the same transition).
func TestSpikeContentionExternalEndpoint(t *testing.T) {
	blobURL := os.Getenv(EnvTestS3WALURL)
	if blobURL == "" {
		t.Skipf("set %s plus AWS credentials to run", EnvTestS3WALURL)
	}
	raw, bucket := openExternalS3(t, blobURL)
	prefix := "qualify-spike/" + randomHexTB(t, 8) + "/"
	t.Cleanup(func() { deleteAllPrefix(t, raw, bucket, prefix) })
	cas := s3wal.New(raw, bucket)
	cas.DecodeSeq = decodeSeq
	res := runContentionWindow(t, cas, prefix+"_wal/repo/detect", 8, 3*time.Second, 256)
	if res.wins == 0 {
		t.Fatal("no advance succeeded during the detector window — over-rejection or livelock")
	}
	t.Logf("detector: wins=%d lostRaces=%d contention=%d ambiguous=%d maxStarve=%v elapsed=%v",
		res.wins, res.lostRaces, res.contentionErrs, res.ambiguous, res.maxStarve, res.elapsed)
}

// deleteIfMatchProbe classifies a store's DeleteObject+If-Match behavior:
//   - "aws-compliant": mismatched If-Match → 412 and the object survives;
//     matched If-Match → deleted.
//   - "ignores-if-match": mismatched If-Match deletes anyway (MinIO).
//   - "rejects-if-match": the store refuses the header outright (e.g. 501).
func deleteIfMatchProbe(t *testing.T, label string, raw *s3.Client, bucket, key string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	put := func() string {
		out, err := raw.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(key),
			Body: bytes.NewReader([]byte("delete-if-match probe")),
		})
		if err != nil {
			t.Fatalf("%s: seed PutObject: %v", label, err)
		}
		if out.ETag == nil || *out.ETag == "" {
			t.Fatalf("%s: PutObject returned no ETag", label)
		}
		return *out.ETag
	}
	exists := func() bool {
		_, err := raw.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		return err == nil
	}
	httpStatus := func(err error) int {
		var re *awshttp.ResponseError
		if errors.As(err, &re) {
			return re.HTTPStatusCode()
		}
		return 0
	}

	etag := put()
	// Step 1: mismatched If-Match must NOT delete (AWS: 412).
	staleTag := `"00000000000000000000000000000000"`
	_, err := raw.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), IfMatch: aws.String(staleTag),
	})
	switch {
	case err == nil && !exists():
		t.Logf("VERDICT store=%s surface=delete-if-match verdict=ignores-if-match detail=%q",
			label, "mismatched If-Match deleted the object (no 412)")
		return
	case err == nil && exists():
		t.Logf("VERDICT store=%s surface=delete-if-match verdict=mishandles detail=%q",
			label, "mismatched If-Match returned success but did not delete")
		return
	case httpStatus(err) == 412:
		if !exists() {
			t.Logf("VERDICT store=%s surface=delete-if-match verdict=mishandles detail=%q",
				label, "412 returned but object deleted anyway")
			return
		}
		// fall through to step 2
	default:
		t.Logf("VERDICT store=%s surface=delete-if-match verdict=rejects-if-match detail=%q",
			label, fmt.Sprintf("mismatched If-Match: status=%d err=%v", httpStatus(err), err))
		return
	}
	// Step 2: matched If-Match must delete.
	_, err = raw.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), IfMatch: aws.String(etag),
	})
	if err != nil {
		t.Logf("VERDICT store=%s surface=delete-if-match verdict=mishandles detail=%q",
			label, fmt.Sprintf("matched If-Match rejected: status=%d err=%v", httpStatus(err), err))
		return
	}
	if exists() {
		t.Logf("VERDICT store=%s surface=delete-if-match verdict=mishandles detail=%q",
			label, "matched If-Match returned success but object survives")
		return
	}
	t.Logf("VERDICT store=%s surface=delete-if-match verdict=aws-compliant detail=%q",
		label, "mismatch → 412 + object survives; match → deleted")
}

// TestSpikeTornReadClassifier pins down WHAT breaks when a GET races an
// overwrite on the external endpoint: AWS GETs are atomic (one consistent
// version: body, ETag, and checksum all from the same version). A store
// can violate that two ways — torn BODY (mixed bytes from two versions)
// or torn METADATA (intact body of one version with the ETag/checksum of
// another). The classifier disables SDK response-checksum validation,
// hammers one key with alternating distinct-length PUTs, and MD5s every
// GET body against the response ETag (the emulator's ETag is the body
// MD5, verified out-of-band).
func TestSpikeTornReadClassifier(t *testing.T) {
	blobURL := os.Getenv(EnvTestS3WALURL)
	if blobURL == "" {
		t.Skipf("set %s plus AWS credentials to run", EnvTestS3WALURL)
	}
	raw, bucket := openExternalS3(t, blobURL)
	prefix := "qualify-spike/" + randomHexTB(t, 8) + "/"
	t.Cleanup(func() { deleteAllPrefix(t, raw, bucket, prefix) })
	key := prefix + "torn-probe"

	bodyA := bytes.Repeat([]byte("a"), 256)
	bodyB := bytes.Repeat([]byte("b"), 300)
	md5hex := func(b []byte) string {
		sum := md5.Sum(b)
		return `"` + hex.EncodeToString(sum[:]) + `"`
	}
	etagA, etagB := md5hex(bodyA), md5hex(bodyB)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := raw.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(bodyA),
	}); err != nil {
		t.Fatalf("seed put: %v", err)
	}

	// Reader client with response checksum validation off so a mismatched
	// checksum cannot abort the read before classification.
	noCheck := func(o *s3.Options) {
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	}

	// Torn framing can poison a pooled keep-alive connection (observed on
	// Floci 1.7.0: "Unsolicited response received on idle HTTP channel"
	// followed by an indefinitely stalled next request), so every op gets
	// its own short deadline and errors are counted, never fatal.
	stop := make(chan struct{})
	var writeErrs int
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			body := bodyA
			if i%2 == 1 {
				body = bodyB
			}
			opCtx, opCancel := context.WithTimeout(ctx, 2*time.Second)
			_, err := raw.PutObject(opCtx, &s3.PutObjectInput{
				Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body),
			})
			opCancel()
			if err != nil {
				writeErrs++
			}
		}
	}()

	var reads, tornBody, tornMeta, readErrs int
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		opCtx, opCancel := context.WithTimeout(ctx, 2*time.Second)
		out, err := raw.GetObject(opCtx, &s3.GetObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(key),
		}, noCheck)
		if err != nil {
			opCancel()
			readErrs++
			continue
		}
		body, err := io.ReadAll(out.Body)
		_ = out.Body.Close()
		opCancel()
		if err != nil {
			readErrs++
			continue
		}
		reads++
		wholeA, wholeB := bytes.Equal(body, bodyA), bytes.Equal(body, bodyB)
		if !wholeA && !wholeB {
			tornBody++
			continue
		}
		etag := aws.ToString(out.ETag)
		if (wholeA && etag != etagA) || (wholeB && etag != etagB) {
			tornMeta++
		}
	}
	close(stop)
	<-writerDone
	verdict := "atomic"
	switch {
	case tornBody > 0:
		verdict = "torn-body"
	case tornMeta > 0:
		verdict = "torn-metadata"
	}
	t.Logf("VERDICT store=external surface=get-under-overwrite verdict=%s detail=%q",
		verdict, fmt.Sprintf("reads=%d tornBody=%d tornMeta=%d readErrs=%d writeErrs=%d", reads, tornBody, tornMeta, readErrs, writeErrs))
}

func TestSpikeDeleteIfMatchExternalEndpoint(t *testing.T) {
	blobURL := os.Getenv(EnvTestS3WALURL)
	if blobURL == "" {
		t.Skipf("set %s plus AWS credentials to run", EnvTestS3WALURL)
	}
	raw, bucket := openExternalS3(t, blobURL)
	prefix := "qualify-spike/" + randomHexTB(t, 8) + "/"
	t.Cleanup(func() { deleteAllPrefix(t, raw, bucket, prefix) })
	deleteIfMatchProbe(t, "external", raw, bucket, prefix+"dim-probe")
}

func TestSpikeDeleteIfMatchSeaweedFS(t *testing.T) {
	if testing.Short() {
		t.Skip("needs docker")
	}
	endpoint := startSeaweedFS(t)
	raw := rawS3Client(t, endpoint, "us-east-1", "seaweedfs", "seaweedfs")
	ensureBucket(t, raw, "s3wal-spike")
	deleteIfMatchProbe(t, "seaweedfs-4.42", raw, "s3wal-spike", "dim-probe")
}

func TestSpikeDeleteIfMatchMinIOLegacy(t *testing.T) {
	if testing.Short() {
		t.Skip("needs docker")
	}
	endpoint, user, pass := startMinIOLegacy(t)
	raw := rawS3Client(t, endpoint, "us-east-1", user, pass)
	ensureBucket(t, raw, "s3wal-spike")
	deleteIfMatchProbe(t, "minio-legacy-2024-01", raw, "s3wal-spike", "dim-probe")
}
