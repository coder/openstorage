// The Floci row of the s3wal qualification matrix (issues #205, #212) over
// the standard qualification surfaces plus the two gap surfaces the #201
// spike found Floci violating: DeleteObject + If-Match fidelity and GET
// atomicity under same-key overwrite. The lane runs nightly
// (floci-nightly.yml) against the pinned s3waltest.FlociImage.
//
// Modes: the 22 standard surfaces and get-under-overwrite run in REQUIRE
// mode — the pinned build passes them all now that upstream fixed the
// torn-read bug (floci-io/floci#2874), so a regression in either Floci or
// s3wal's own S3 usage fails the test. delete-if-match stays a RECORD-mode
// row: Floci ignored the condition through 2.1.0 (issue #201) and since 2.2.0
// honors it, but s3wal never depends on it (ADR-46). Like the SeaweedFS
// and MinIO lanes, this test is gated only by -short, so it runs wherever
// `go test ./...` runs — the per-PR `test` job included, as the
// record-mode row already did.
// This is second-implementation regression armor only; it is not
// promotion evidence for the s3wal backend (#192), whose gates
// (docs/how-to/deploy-production.md) are unchanged by anything here.
package s3wal_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/coder/openstorage/internal/ref/s3wal"

	"github.com/coder/openstorage/internal/ref/s3wal/s3waltest"
)

// TestQualifyFloci runs the full qualification surface set and
// get-under-overwrite in require mode, then delete-if-match in record
// mode. Infrastructure/harness errors (verdictError) fail in every mode —
// an incomplete matrix row is not a finding.
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
	runQualification(t, e, true)
	runSurfaces(t, e, true, []qualSurface{{"get-under-overwrite", surfaceGetUnderOverwrite}})
	runSurfaces(t, e, false, []qualSurface{{"delete-if-match", surfaceDeleteIfMatch}})
}

// surfaceDeleteIfMatch records a store's DeleteObject+If-Match fidelity.
// AWS honors the condition on general-purpose buckets: mismatched If-Match
// ⇒ 412 and the object survives; matched If-Match ⇒ deleted. The
// corruption-class divergence is silently deleting on a mismatch (legacy
// MinIO; Floci through 2.1.0 — issue #201). s3wal deliberately never
// uses conditional DELETE as a guard (ADR-46), so this surface is a
// matrix row, not a design dependency.
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
		class := rejectVerdict(err)
		if class == verdictError {
			return []verdict{{name, verdictError, fmt.Sprintf("mismatched If-Match: %v", err)}}
		}
		// A definitive non-412 rejection still needs the survival check:
		// a store could destroy the object AND reject the request, which
		// would otherwise hide the corruption behind a wrong-status
		// verdict.
		alive, herr := exists()
		if herr != nil {
			return []verdict{{name, verdictError, fmt.Sprintf("HeadObject after rejected mismatched delete: %v", herr)}}
		}
		if !alive {
			return []verdict{{name, verdictMishandles, fmt.Sprintf("mismatched If-Match deleted the object despite a non-412 rejection: %v", err)}}
		}
		return []verdict{{name, class, fmt.Sprintf("mismatched If-Match: %v (object survives, want 412)", err)}}
	}

	// Step 2: matched If-Match must delete.
	if _, err := e.raw.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(e.bucket), Key: aws.String(key), IfMatch: aws.String(etag),
	}); err != nil {
		// Only a DEFINITIVE store rejection is a behavioral finding; a
		// timeout/5xx/transport failure produced no verdict on whether a
		// matched condition deletes, so it must fail the row instead of
		// recording a finding that was never observed.
		if rejectVerdict(err) == verdictError {
			return []verdict{{name, verdictError, fmt.Sprintf("matched If-Match delete produced no store verdict: %v", err)}}
		}
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

// surfaceGetUnderOverwrite checks whether GETs racing same-key overwrites
// stay atomic. AWS GETs return one consistent version — body, ETag, and
// checksum all from the same version. Two violation shapes: a torn BODY
// (bytes from neither whole version — on Floci through 2.0.1 also
// corrupted HTTP framing: surplus bytes past Content-Length that poison
// the pooled keep-alive connection, issue #201) and torn METADATA (an
// intact body paired with the other version's ETag). This is s3wal's
// production shape — manifest GETs race If-Match overwrites of the same
// key — hence the torn-read bug was the single gate on any require-mode
// Floci lane until upstream fixed it (floci-io/floci#2874, first shipped
// in nightly-09012026); on the pinned build this surface is required.
//
// The probe hammers one key with alternating distinct-length CONDITIONAL
// PUTs — each overwrite carries If-Match with the previous write's ETag,
// mirroring the production read-mutate-CAS loop — while readers classify
// every GET body (SDK response-checksum validation off so a mismatch is
// classified instead of aborting the read). It exits early on the first
// torn observation; a clean run needs the full window plus minimum read
// AND verified version-transition counts before recording pass (readers
// alone — or same-body re-writes when one version's PUTs selectively
// fail — could otherwise exceed the floor against unchanging content).
// All ops run on a dedicated HTTP transport with short per-op deadlines:
// torn framing poisons keep-alive connections, and the quarantine keeps
// that damage away from the shared fixture pool (and other surfaces).
func surfaceGetUnderOverwrite(t *testing.T, e *qualEnv) []verdict {
	t.Helper()
	const name = "get-under-overwrite"
	const (
		raceWindow      = 15 * time.Second
		opTimeout       = 2 * time.Second
		readers         = 2
		minReads        = 50  // below this a "pass" would be too weak to record
		minWrites       = 50  // verified A↔B version transitions needed to have exercised the race at all
		maxLogged       = 3   // read errors quoted in the verdict detail (issue #566)
		maxReadFailures = 256 // failed reads kept for classification; the rest count as unexplained
	)
	opts := e.raw.Options()
	opts.HTTPClient = awshttp.NewBuildableClient()
	opts.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	// No SDK retries, like the production CAS client (casobj.go): a silent
	// retry of a conditional PUT whose first attempt landed would turn a
	// successful overwrite into a stale-ETag 412, and a retried GET could
	// mask the exact framing corruption this surface exists to observe.
	opts.Retryer = aws.NopRetryer{}
	client := s3.New(opts)

	key := e.prefix + "get-under-overwrite-" + randomHexTB(t, 4)
	bodyA := bytes.Repeat([]byte("a"), 256)
	bodyB := bytes.Repeat([]byte("b"), 300) // distinct length: a torn body can never alias a whole version

	put := func(ctx context.Context, body []byte, ifMatch string) (string, error) {
		opCtx, cancel := context.WithTimeout(ctx, opTimeout)
		defer cancel()
		in := &s3.PutObjectInput{
			Bucket: aws.String(e.bucket), Key: aws.String(key), Body: bytes.NewReader(body),
		}
		if ifMatch != "" {
			in.IfMatch = aws.String(ifMatch)
		}
		out, err := client.PutObject(opCtx, in)
		if err != nil {
			return "", err
		}
		// A success without an ETag cannot count as a conditional
		// overwrite: the writer would carry curETag="" and silently
		// degrade every later PUT to unconditional.
		if aws.ToString(out.ETag) == "" {
			return "", fmt.Errorf("PutObject succeeded without an ETag")
		}
		return aws.ToString(out.ETag), nil
	}
	headETag := func(ctx context.Context) (string, error) {
		opCtx, cancel := context.WithTimeout(ctx, opTimeout)
		defer cancel()
		out, err := client.HeadObject(opCtx, &s3.HeadObjectInput{Bucket: aws.String(e.bucket), Key: aws.String(key)})
		if err != nil {
			return "", err
		}
		if aws.ToString(out.ETag) == "" {
			return "", fmt.Errorf("HeadObject returned no ETag")
		}
		return aws.ToString(out.ETag), nil
	}

	// Establish per-version ETags AND their stability for BOTH versions:
	// torn-metadata classification compares a read's ETag against the ETag
	// of the whole version it returned, which is meaningful only if
	// re-PUTting the same bytes reproduces the same ETag (content-
	// addressed, MD5 on every probed store) and the two versions' ETags
	// differ. A version whose ETag varies per PUT would otherwise turn its
	// own legitimate reads into false torn-metadata findings.
	ctx := t.Context()
	etagA, errA := put(ctx, bodyA, "")
	etagB, errB := put(ctx, bodyB, "")
	etagA2, errA2 := put(ctx, bodyA, "")
	etagB2, errB2 := put(ctx, bodyB, "")
	if errA != nil || errB != nil || errA2 != nil || errB2 != nil {
		return []verdict{{name, verdictError, fmt.Sprintf("seed PUTs: %v / %v / %v / %v", errA, errB, errA2, errB2)}}
	}
	metaCheck := etagA != "" && etagB != "" && etagA != etagB && etagA == etagA2 && etagB == etagB2
	etagOf := map[string]string{string(bodyA): etagA, string(bodyB): etagB}

	raceCtx, stopRace := context.WithCancel(ctx)
	defer stopRace()
	var reads, tornBody, tornMeta, readErrs, writeOKs, writeErrs atomic.Int64
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		curETag := etagB2 // the last seed write
		next := bodyA     // always the version OPPOSITE the last known content
		for raceCtx.Err() == nil {
			etag, err := put(raceCtx, next, curETag)
			if err != nil {
				if raceCtx.Err() == nil {
					writeErrs.Add(1)
				}
				// Re-resolve the current ETag: an ambiguous outcome (a
				// timeout after the write landed) would otherwise wedge
				// every later If-Match on a stale tag. Best effort — on
				// Head failure the next iteration retries. The target
				// version is NOT flipped on failure, so a store that
				// selectively rejects one version keeps being asked for
				// the version transition instead of racking up
				// same-body overwrites.
				if cur, herr := headETag(raceCtx); herr == nil {
					curETag = cur
				}
				continue
			}
			// Count only VERIFIED version transitions: on the content-
			// addressed ETags the seed stability check establishes, a
			// same-body overwrite (possible when an ambiguous failure
			// actually landed and the retry re-wrote the same version)
			// returns the tag it matched against and must not reach the
			// minWrites floor.
			if etag != curETag {
				writeOKs.Add(1)
			}
			curETag = etag
			if bytes.Equal(next, bodyA) {
				next = bodyB
			} else {
				next = bodyA
			}
		}
	}()
	// Failed reads are kept (up to maxReadFailures) and classified once the
	// race is over, when the heartbeat has recorded every stall of the test
	// process (issue #566).
	var failuresMu sync.Mutex
	var failures []readFailure
	stopStalls := watchStalls()
	defer stopStalls()
	raceStart := time.Now()
	readersDone := make(chan time.Duration, readers) // each reader's slowest successful GET
	for range readers {
		go func() {
			var slowest time.Duration
			defer func() { readersDone <- slowest }()
			for raceCtx.Err() == nil {
				opStart := time.Now()
				opCtx, cancel := context.WithTimeout(raceCtx, opTimeout)
				body, etag, err := getWithETag(opCtx, client, e.bucket, key)
				cancel()
				opEnd := time.Now()
				if err != nil {
					if raceCtx.Err() == nil {
						readErrs.Add(1)
						failuresMu.Lock()
						if len(failures) < maxReadFailures {
							failures = append(failures, readFailure{start: opStart, end: opEnd, err: err})
						}
						failuresMu.Unlock()
					}
					continue
				}
				reads.Add(1)
				slowest = max(slowest, opEnd.Sub(opStart))
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
	var maxRead time.Duration
	for range readers {
		maxRead = max(maxRead, <-readersDone)
	}

	// Every reader has exited, so failures is no longer written.
	spans := stopStalls()
	explained := int64(0)
	var quoted []string
	for _, f := range failures {
		stalled := explainedByStall(f, spans, opTimeout)
		if stalled {
			explained++
		}
		if len(quoted) < maxLogged {
			quoted = append(quoted, fmt.Sprintf("{at=+%s elapsed=%s awake=%s class=%s explainedByStall=%t err=%q}",
				f.start.Sub(raceStart).Round(time.Millisecond), f.elapsed().Round(time.Millisecond),
				(f.elapsed()-f.stalledFor(spans)).Round(time.Millisecond), readErrClass(f.err), stalled, f.err.Error()))
		}
	}
	assertf(int64(len(failures)) == min(readErrs.Load(), maxReadFailures), "kept %d read failures, counted %d", len(failures), readErrs.Load())
	detail := fmt.Sprintf("reads=%d tornBody=%d tornMeta=%d readErrs=%d stallErrs=%d writeOKs=%d writeErrs=%d metaCheck=%t window=%s maxRead=%s maxStall=%s",
		reads.Load(), tornBody.Load(), tornMeta.Load(), readErrs.Load(), explained, writeOKs.Load(), writeErrs.Load(), metaCheck, raceWindow,
		maxRead.Round(time.Millisecond), longestStall(spans).Round(time.Millisecond))
	if len(quoted) > 0 {
		detail += fmt.Sprintf(" firstReadErrs(%d of %d)=%s", len(quoted), readErrs.Load(), strings.Join(quoted, " "))
	}
	tally := raceTally{reads.Load(), tornBody.Load(), tornMeta.Load(), readErrs.Load(), explained, writeOKs.Load()}
	return []verdict{raceVerdict(name, tally, minReads, minWrites, detail)}
}

// raceTally is what one get-under-overwrite race window counted.
// stallErrs are the failed reads that a stall of the test process explains.
type raceTally struct{ reads, tornBody, tornMeta, readErrs, stallErrs, writeOKs int64 }

// raceVerdict turns a race window's counts into the surface's verdict.
func raceVerdict(name string, c raceTally, minReads, minWrites int64, detail string) verdict {
	switch {
	case c.tornBody > 0:
		return verdict{name, verdictMishandles, "torn body: a racing GET returned bytes of neither whole version — " + detail}
	case c.tornMeta > 0:
		return verdict{name, verdictMishandles, "torn metadata: a racing GET returned one whole version with the other version's ETag — " + detail}
	case c.readErrs > c.stallErrs:
		// Framing corruption can surface as failed reads rather than a
		// classifiable torn body (a short-for-Content-Length response
		// blocks until the op deadline; a poisoned keep-alive breaks the
		// next request). A read failure counts as runner noise only when
		// the test process itself stopped for it (explainedByStall). Any
		// other failure makes the probe inconclusive, never a clean pass.
		// firstReadErrs names the class of the first few (issue #566):
		// "short-body" is the framing shape, "reset" a closed connection,
		// and "deadline" with a long awake time a Floci or network stall.
		return verdict{name, verdictError, "racing GETs failed without a classifiable torn read — inconclusive (possible framing corruption) — " + detail}
	case c.reads < minReads || c.writeOKs < minWrites:
		return verdict{name, verdictError, "too few successful reads/overwrites to have exercised the race — " + detail}
	default:
		return verdict{name, verdictPass, "every racing GET returned one whole version with its own ETag — " + detail}
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
		// A short body that stalls until the deadline is the framing
		// shape; a stall before the headers is not (issue #566).
		return nil, "", fmt.Errorf("read body after headers (%d of Content-Length %d bytes): %w", len(b), aws.ToInt64(out.ContentLength), err)
	}
	return b, aws.ToString(out.ETag), nil
}
