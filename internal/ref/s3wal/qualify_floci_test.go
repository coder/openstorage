// The Floci row of the s3wal qualification matrix (issue #205): a
// RECORD-mode lane — behavioral findings are the deliverable, never a
// test failure — over the standard qualification surfaces plus the two
// gap surfaces the #201 spike found Floci violating: DeleteObject +
// If-Match fidelity and GET atomicity under same-key overwrite. The lane
// runs nightly (floci-nightly.yml) against the pinned s3waltest.FlociImage
// so an upstream gap closure surfaces automatically when the pin is bumped
// to a new Floci release. Require mode stays SeaweedFS-only: nothing gates
// on Floci.
package s3wal_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/coder/code-storage/internal/ref/s3wal"

	"github.com/coder/code-storage/internal/ref/s3wal/s3waltest"
)

// TestQualifyFloci runs the full qualification surface set plus the Floci
// gap surfaces in record mode. Infrastructure/harness errors
// (verdictError) still fail — an incomplete matrix row is not a finding.
func TestQualifyFloci(t *testing.T) {
	if testing.Short() {
		t.Skip("floci qualification needs docker")
	}
	endpoint := s3waltest.StartFloci(t)
	raw := rawS3Client(t, endpoint, "us-east-1", "floci", "floci")
	ensureBucket(t, raw, "s3wal-qualify")
	// The store label carries the pinned release so the VERDICT transcript
	// stays a per-release matrix row across pin bumps.
	tag, ok := strings.CutPrefix(s3waltest.FlociImage, "floci/floci:")
	assertf(ok && tag != "", "FlociImage %q does not carry the floci/floci: prefix", s3waltest.FlociImage)
	e := newQualEnv(t, "floci-"+tag, raw, "s3wal-qualify", "", s3wal.ProbeKey)
	runQualification(t, e, false)
	runSurfaces(t, e, false, []qualSurface{
		{"delete-if-match", surfaceDeleteIfMatch},
		{"get-under-overwrite", surfaceGetUnderOverwrite},
	})
}

// surfaceDeleteIfMatch records a store's DeleteObject+If-Match fidelity.
// AWS honors the condition on general-purpose buckets: mismatched If-Match
// ⇒ 412 and the object survives; matched If-Match ⇒ deleted. The
// corruption-class divergence is silently deleting on a mismatch (legacy
// MinIO, Floci ≤ 1.7.0 — issue #201). s3wal deliberately never uses
// conditional DELETE as a guard (ADR-46), so this surface is a matrix row,
// not a design dependency.
func surfaceDeleteIfMatch(t *testing.T, e *qualEnv) []verdict {
	t.Helper()
	const name = "delete-if-match"
	ctx := t.Context()
	key := e.prefix + "delete-if-match-" + randomHexTB(t, 4)

	out, err := e.raw.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(e.bucket), Key: aws.String(key),
		Body: bytes.NewReader([]byte("delete-if-match probe")),
	})
	if err != nil {
		return []verdict{{name, verdictError, fmt.Sprintf("seed PutObject: %v", err)}}
	}
	etag := aws.ToString(out.ETag)
	if etag == "" {
		return []verdict{{name, verdictError, "seed PutObject returned no ETag"}}
	}
	exists := func() (bool, error) {
		_, err := e.raw.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(e.bucket), Key: aws.String(key)})
		if err == nil {
			return true, nil
		}
		if status, _ := s3wal.ErrorStatus(err); status == http.StatusNotFound {
			return false, nil
		}
		return false, err
	}

	// Step 1: mismatched If-Match must NOT delete (AWS: 412).
	const staleTag = `"00000000000000000000000000000000"`
	_, err = e.raw.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(e.bucket), Key: aws.String(key), IfMatch: aws.String(staleTag),
	})
	status, _ := s3wal.ErrorStatus(err)
	switch {
	case err == nil:
		alive, herr := exists()
		if herr != nil {
			return []verdict{{name, verdictError, fmt.Sprintf("HeadObject after mismatched delete: %v", herr)}}
		}
		if alive {
			return []verdict{{name, verdictMishandles, "mismatched If-Match returned success but did not delete"}}
		}
		return []verdict{{name, verdictIgnores, "mismatched If-Match deleted the object (no 412) — silent-corruption class"}}
	case status == 412:
		alive, herr := exists()
		if herr != nil {
			return []verdict{{name, verdictError, fmt.Sprintf("HeadObject after 412: %v", herr)}}
		}
		if !alive {
			return []verdict{{name, verdictMishandles, "412 returned but object deleted anyway"}}
		}
		// AWS-compliant so far; fall through to step 2.
	default:
		return []verdict{{name, rejectVerdict(err), fmt.Sprintf("mismatched If-Match: %v", err)}}
	}

	// Step 2: matched If-Match must delete.
	if _, err := e.raw.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(e.bucket), Key: aws.String(key), IfMatch: aws.String(etag),
	}); err != nil {
		return []verdict{{name, verdictMishandles, fmt.Sprintf("matched If-Match rejected: %v", err)}}
	}
	alive, herr := exists()
	if herr != nil {
		return []verdict{{name, verdictError, fmt.Sprintf("HeadObject after matched delete: %v", herr)}}
	}
	if alive {
		return []verdict{{name, verdictMishandles, "matched If-Match returned success but object survives"}}
	}
	return []verdict{{name, verdictPass, "mismatch → 412 + object survives; match → deleted"}}
}

// surfaceGetUnderOverwrite records whether GETs racing same-key overwrites
// stay atomic. AWS GETs return one consistent version — body, ETag, and
// checksum all from the same version. Two violation shapes: a torn BODY
// (bytes from neither whole version — on Floci ≤ 1.7.0 also corrupted HTTP
// framing: surplus bytes past Content-Length that poison the pooled
// keep-alive connection, issue #201) and torn METADATA (an intact body
// paired with the other version's ETag). This is s3wal's production shape
// — manifest GETs race If-Match overwrites of the same key — hence the
// torn-read bug is the single gate on any require-mode Floci lane.
//
// The probe hammers one key with alternating distinct-length PUTs while
// readers classify every GET body (SDK response-checksum validation off so
// a mismatch is classified instead of aborting the read). It exits early
// on the first torn observation; a clean run needs the full window plus a
// minimum read count before recording pass. All ops run on a dedicated
// HTTP transport with short per-op deadlines: torn framing poisons
// keep-alive connections, and the quarantine keeps that damage away from
// the shared fixture pool (and other surfaces).
func surfaceGetUnderOverwrite(t *testing.T, e *qualEnv) []verdict {
	t.Helper()
	const name = "get-under-overwrite"
	const (
		raceWindow = 15 * time.Second
		opTimeout  = 2 * time.Second
		readers    = 2
		minReads   = 50 // below this a "pass" would be too weak to record
	)
	opts := e.raw.Options()
	opts.HTTPClient = awshttp.NewBuildableClient()
	opts.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	client := s3.New(opts)

	key := e.prefix + "get-under-overwrite-" + randomHexTB(t, 4)
	bodyA := bytes.Repeat([]byte("a"), 256)
	bodyB := bytes.Repeat([]byte("b"), 300) // distinct length: a torn body can never alias a whole version

	put := func(ctx context.Context, body []byte) (string, error) {
		opCtx, cancel := context.WithTimeout(ctx, opTimeout)
		defer cancel()
		out, err := client.PutObject(opCtx, &s3.PutObjectInput{
			Bucket: aws.String(e.bucket), Key: aws.String(key), Body: bytes.NewReader(body),
		})
		if err != nil {
			return "", err
		}
		return aws.ToString(out.ETag), nil
	}

	// Establish per-version ETags AND their stability: torn-metadata
	// classification compares a read's ETag against the ETag of the whole
	// version it returned, which is meaningful only if re-PUTting the same
	// bytes reproduces the same ETag (content-addressed, MD5 on every
	// probed store) and the two versions' ETags differ.
	ctx := t.Context()
	etagA, errA := put(ctx, bodyA)
	etagB, errB := put(ctx, bodyB)
	etagA2, errA2 := put(ctx, bodyA)
	if errA != nil || errB != nil || errA2 != nil {
		return []verdict{{name, verdictError, fmt.Sprintf("seed PUTs: %v / %v / %v", errA, errB, errA2)}}
	}
	metaCheck := etagA != "" && etagB != "" && etagA != etagB && etagA == etagA2
	etagOf := map[string]string{string(bodyA): etagA, string(bodyB): etagB}

	raceCtx, stopRace := context.WithCancel(ctx)
	defer stopRace()
	var reads, tornBody, tornMeta, readErrs, writeErrs atomic.Int64
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for i := 0; raceCtx.Err() == nil; i++ {
			body := bodyA
			if i%2 == 1 {
				body = bodyB
			}
			if _, err := put(raceCtx, body); err != nil && raceCtx.Err() == nil {
				writeErrs.Add(1)
			}
		}
	}()
	readersDone := make(chan struct{}, readers)
	for range readers {
		go func() {
			defer func() { readersDone <- struct{}{} }()
			for raceCtx.Err() == nil {
				opCtx, cancel := context.WithTimeout(raceCtx, opTimeout)
				body, etag, err := getWithETag(opCtx, client, e.bucket, key)
				cancel()
				if err != nil {
					if raceCtx.Err() == nil {
						readErrs.Add(1)
					}
					continue
				}
				reads.Add(1)
				want, whole := etagOf[string(body)]
				switch {
				case !whole:
					tornBody.Add(1)
					stopRace() // first torn observation settles the verdict
				case metaCheck && etag != want:
					tornMeta.Add(1)
					stopRace()
				}
			}
		}()
	}
	select {
	case <-time.After(raceWindow):
	case <-raceCtx.Done():
	}
	stopRace()
	<-writerDone
	for range readers {
		<-readersDone
	}

	detail := fmt.Sprintf("reads=%d tornBody=%d tornMeta=%d readErrs=%d writeErrs=%d metaCheck=%t window=%s",
		reads.Load(), tornBody.Load(), tornMeta.Load(), readErrs.Load(), writeErrs.Load(), metaCheck, raceWindow)
	switch {
	case tornBody.Load() > 0:
		return []verdict{{name, verdictMishandles, "torn body: a racing GET returned bytes of neither whole version — " + detail}}
	case tornMeta.Load() > 0:
		return []verdict{{name, verdictMishandles, "torn metadata: a racing GET returned one whole version with the other version's ETag — " + detail}}
	case reads.Load() < minReads:
		return []verdict{{name, verdictError, "too few successful reads to classify atomicity — " + detail}}
	default:
		return []verdict{{name, verdictPass, "every racing GET returned one whole version with its own ETag — " + detail}}
	}
}

// getWithETag fetches key and returns the full body plus the response
// ETag. The caller's client must have response-checksum validation
// disabled so a torn body reaches classification instead of aborting the
// read mid-stream.
func getWithETag(ctx context.Context, client *s3.Client, bucket, key string) (body []byte, etag string, err error) {
	out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = out.Body.Close() }()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, "", err
	}
	return b, aws.ToString(out.ETag), nil
}
