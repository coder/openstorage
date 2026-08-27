// Package s3wal is the S3-manifest-WAL RefStore experiment (issue #133,
// docs/plans/s3wal-plan.md): one JSON manifest object per base repo group,
// advanced by conditional PUT (If-Match / If-None-Match: *). Phase 1a ships
// only the conditional-object CAS primitive (this file) and the boot-time
// store probe (probe.go); manifest schema and the RefStore driver are
// Phase 1b.
package s3wal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/coder/code-storage/internal/s3oplog"
)

// assertf is a fail-fast invariant check (crash-and-burn style).
func assertf(cond bool, format string, args ...any) {
	if !cond {
		panic(fmt.Sprintf("s3wal assertion failed: "+format, args...))
	}
}

// Typed sentinels for the plan §2.3 error taxonomy. Callers branch with
// errors.Is; every sentinel below the ambiguity line means the conditional
// write did NOT execute, so retrying can never double-apply.
var (
	// ErrNotExist reports a Get on a key that does not exist (404/NoSuchKey).
	ErrNotExist = errors.New("s3wal: object does not exist")
	// ErrNotModified reports a GetIfChanged whose ETag still matches (304).
	ErrNotModified = errors.New("s3wal: object not modified")
	// ErrExists reports a Create that lost to an existing object: the
	// duplicate create was rejected with 412 and did not execute.
	ErrExists = errors.New("s3wal: object already exists")
	// ErrLostRace reports an Advance rejected with 412 PreconditionFailed:
	// someone else advanced the object first and our write did NOT happen.
	// The caller re-reads fresh state and re-evaluates (Phase 1b).
	ErrLostRace = errors.New("s3wal: conditional write lost the race")
	// ErrNotExecuted reports a request that provably never reached the
	// store (dial failure, ctx expired while queued) — the PG notExecuted
	// twin, never recovered as a win.
	ErrNotExecuted = errors.New("s3wal: request not executed")
	// ErrCASContention reports 409 ConditionalRequestConflict retries
	// exhausted — surfaced, never silently dropped.
	ErrCASContention = errors.New("s3wal: conditional-write contention retries exhausted")
)

// AmbiguousError reports a conditional write whose fate is unknown: the
// request was (or may have been) sent, and the transport failed before a
// definitive store verdict arrived (timeout, connection loss, 5xx). The
// write MAY have landed; recovery predicates are Phase 1b — Phase 1a only
// surfaces the class. When classification is uncertain we default to
// ambiguous (fail-closed direction per plan §2.3).
type AmbiguousError struct {
	Op    string
	Key   string
	Cause error
}

func (e *AmbiguousError) Error() string {
	return fmt.Sprintf("s3wal: %s %s: ambiguous outcome (write may have landed): %v", e.Op, e.Key, e.Cause)
}

func (e *AmbiguousError) Unwrap() error { return e.Cause }

// Object is one fetched conditional object: full body plus the store ETag,
// handled verbatim as returned (quoted) and never compared to MD5.
type Object struct {
	Body []byte
	ETag string
	// LastModified is the STORE's write instant for this version
	// (Last-Modified header, 1s resolution). Paired with the Date witness
	// (StoreDate) it lets freshness be judged entirely on the store's
	// clock — the instance lease depends on it, because comparing two
	// hosts' wall clocks is exactly what cross-machine skew breaks. Zero
	// when the store omitted the header; consumers fail closed.
	LastModified time.Time
}

// Contention backoff (§2.3): full-jitter exponential, 25ms → 2s cap, ≤16
// attempts, ctx-bounded.
const (
	defaultBackoffBase = 25 * time.Millisecond
	defaultBackoffCap  = 2 * time.Second
	defaultMaxAttempts = 16
)

// Client performs conditional single-object operations over the raw
// vendored s3.Client (never gocloud — plan §5-R1). It carries no manifest
// or RefStore logic.
type Client struct {
	s3     *s3.Client
	bucket string

	// backoff knobs — defaults above; tests shrink them to keep the
	// exhaustion path fast.
	backoffBase time.Duration
	backoffCap  time.Duration
	maxAttempts int

	// DecodeSeq optionally decodes a payload sequence number from a fetched
	// body. When set, Get/GetIfChanged enforce the §2.3 read-side defensive
	// assert: a per-key seq regression means the store served a stale read —
	// fail loudly. Acknowledged Create/Advance writes also RAISE the per-key
	// watermark to the seq of the body they wrote (see raiseSeq), so a read
	// older than this session's own accepted write trips the assert too.
	// Phase 1b plugs in the manifest decoder; Phase 1a tests and benches
	// decode their {seq, nonce} payloads.
	//
	// The assert is SESSION-scoped: it compares reads made through this
	// Client, which processes responses sequentially per caller. Two
	// goroutines sharing one Client can legitimately process responses out
	// of store-serialization order (an older read's response arriving after
	// a newer read's — benign reordering, not store staleness), so give
	// each concurrent reader of a hot key its own Client. Phase 1b must
	// keep this in mind when sharing a Client across request handlers.
	DecodeSeq func(body []byte) (seq int64, ok bool)

	seqMu   sync.Mutex
	lastSeq map[string]int64 // allocated lazily, only used when DecodeSeq is set

	// httpCloser, when non-nil, is the Dial-owned pooled HTTP client whose
	// idle connections Close releases. Nil for Clients wrapped around
	// caller-owned s3.Clients.
	httpCloser interface{ CloseIdleConnections() }

	// Store-clock witness (§2.4-3): the freshest HTTP Date header
	// observed on any response through this client, plus the LOCAL
	// monotonic-anchored instant it was observed. See StoreDate.
	dateMu      sync.Mutex
	storeDate   time.Time
	storeDateAt time.Time
}

// New wraps an s3.Client for conditional object operations on one bucket.
// It always derives its own no-retry client from the input client's
// options: the SDK silently re-sending a conditional PUT after an
// ambiguous failure can turn our own landed write into a spurious 412 —
// misclassified as a lost race. Taxonomy fidelity requires exactly one
// wire attempt per classified attempt, so auto-retry is disabled by
// construction, regardless of how the caller built the input client.
func New(s3c *s3.Client, bucket string) *Client {
	assertf(s3c != nil, "New: nil s3 client")
	assertf(bucket != "", "New: empty bucket")
	c := &Client{
		bucket:      bucket,
		backoffBase: defaultBackoffBase,
		backoffCap:  defaultBackoffCap,
		maxAttempts: defaultMaxAttempts,
	}
	c.s3 = s3.New(s3c.Options(), func(o *s3.Options) {
		o.Retryer = aws.NopRetryer{}
		o.APIOptions = append(o.APIOptions, c.captureStoreDate)
		// Round-trip census seam (issue #182): installed here rather than
		// in Dial so wrapped caller-owned clients are counted too. Inert
		// unless CODESTORAGE_S3_OP_LOG is set.
		if opt, ok := s3oplog.APIOption(); ok {
			o.APIOptions = append(o.APIOptions, opt)
		}
	})
	return c
}

// captureStoreDate installs the §2.4-3 store-clock witness: every S3
// response — success or error, GET and PUT alike — carries an HTTP Date
// header stamped by the store, an absolute wall-clock reading no local
// clock trouble can forge. A deserialize-step middleware is the cleanest
// SDK seam: the typed outputs carry no Date field, and the raw
// *smithyhttp.Response is visible here for every operation uniformly.
func (c *Client) captureStoreDate(stack *middleware.Stack) error {
	return stack.Deserialize.Add(middleware.DeserializeMiddlewareFunc("s3walStoreDate",
		func(ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler) (middleware.DeserializeOutput, middleware.Metadata, error) {
			out, md, err := next.HandleDeserialize(ctx, in)
			if resp, ok := out.RawResponse.(*smithyhttp.Response); ok && resp != nil && resp.Response != nil {
				if d, perr := http.ParseTime(resp.Header.Get("Date")); perr == nil {
					c.recordStoreDate(d, time.Now())
				}
			}
			return out, md, err
		}), middleware.Before)
}

// recordStoreDate stores the witness; last write wins (the observation
// instants of concurrently processed responses differ by jitter far
// below the ±DateTolerance resolution floor).
func (c *Client) recordStoreDate(date, observedAt time.Time) {
	c.dateMu.Lock()
	defer c.dateMu.Unlock()
	c.storeDate, c.storeDateAt = date, observedAt
}

// StoreDate returns the freshest observed store wall clock (HTTP Date,
// 1s resolution), the local monotonic-anchored instant it was observed,
// and whether any observation exists yet — the §2.4-3 absolute clock
// witness destructive TTL verdicts cross-check (see ref.S3WAL).
func (c *Client) StoreDate() (date, observedAt time.Time, ok bool) {
	c.dateMu.Lock()
	defer c.dateMu.Unlock()
	return c.storeDate, c.storeDateAt, !c.storeDateAt.IsZero()
}

// Get fetches key unconditionally. Missing keys return ErrNotExist.
func (c *Client) Get(ctx context.Context, key string) (Object, error) {
	assertf(key != "", "Get: empty key")
	return c.get(ctx, key, "")
}

// GetIfChanged fetches key only when its ETag differs from etag (the cheap
// generation-poll path, §2.3): an unchanged object returns ErrNotModified
// from the store's 304 without a body transfer.
func (c *Client) GetIfChanged(ctx context.Context, key, etag string) (Object, error) {
	assertf(key != "", "GetIfChanged: empty key")
	assertf(etag != "", "GetIfChanged: empty etag")
	return c.get(ctx, key, etag)
}

func (c *Client) get(ctx context.Context, key, ifNoneMatch string) (Object, error) {
	in := &s3.GetObjectInput{Bucket: aws.String(c.bucket), Key: aws.String(key)}
	if ifNoneMatch != "" {
		in.IfNoneMatch = aws.String(ifNoneMatch)
	}
	out, err := c.s3.GetObject(ctx, in)
	if err != nil {
		status, code := errorStatus(err)
		switch {
		case status == 304:
			return Object{}, fmt.Errorf("get %s: %w", key, ErrNotModified)
		case status == 404 && code == "NoSuchKey":
			// Only the object-level miss is ErrNotExist. GetObject 404s
			// carry an error code (the codeless-404 shape exists only on
			// HeadObject, which this client never issues), so gating on
			// NoSuchKey is safe here.
			return Object{}, fmt.Errorf("get %s: %w", key, ErrNotExist)
		case status == 404:
			// A non-key 404 (NoSuchBucket, …) executed with a definitive
			// response but reports broken infrastructure, not a missing
			// object: callers must not follow the create path, and the
			// GET-after-DELETE surface must not read a bootstrap failure
			// as a clean miss. Plain definitive error naming the code —
			// never ErrNotExist, never ambiguous.
			return Object{}, fmt.Errorf("s3wal: get %s: 404 %s is not a missing object: %w", key, code, err)
		}
		return Object{}, fmt.Errorf("s3wal: get %s: %w", key, err)
	}
	defer func() { _ = out.Body.Close() }()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		return Object{}, fmt.Errorf("s3wal: get %s: read body: %w", key, err)
	}
	etag := aws.ToString(out.ETag)
	assertf(etag != "", "get %s: store returned no ETag", key)
	c.checkSeq(key, body)
	obj := Object{Body: body, ETag: etag}
	if out.LastModified != nil {
		obj.LastModified = *out.LastModified
	}
	return obj, nil
}

// checkSeq enforces the read-side seq-monotonicity assert when DecodeSeq
// is configured: the store serving an older seq than this process already
// observed for the key is a stale read — a store-fidelity violation, not a
// recoverable condition.
func (c *Client) checkSeq(key string, body []byte) {
	if c.DecodeSeq == nil {
		return
	}
	seq, ok := c.DecodeSeq(body)
	if !ok {
		return
	}
	c.seqMu.Lock()
	defer c.seqMu.Unlock()
	if c.lastSeq == nil {
		c.lastSeq = make(map[string]int64)
	}
	last, seen := c.lastSeq[key]
	assertf(!seen || seq >= last, "stale read on %s: store served seq %d after this process observed %d", key, seq, last)
	if seq > last {
		c.lastSeq[key] = seq
	}
}

// raiseSeq lifts the per-key watermark to the seq encoded in an
// ACKNOWLEDGED write's body (when DecodeSeq is configured): once the store
// accepts our conditional write of seq N, any later read serving < N is a
// stale read. Without this write-side raise the watermark trails at the
// last READ seq — after reading 4 and advancing to 5, a stale seq-4 GET
// would pass checkSeq, and chained wins leave the watermark further
// behind, defeating the fail-loud defense. Monotonic raise only: a write
// never lowers the watermark, and unlike the read side it never asserts
// (an acknowledgment makes no ordering claim against other sessions).
func (c *Client) raiseSeq(key string, body []byte) {
	if c.DecodeSeq == nil {
		return
	}
	seq, ok := c.DecodeSeq(body)
	if !ok {
		return
	}
	c.seqMu.Lock()
	defer c.seqMu.Unlock()
	if c.lastSeq == nil {
		c.lastSeq = make(map[string]int64)
	}
	if seq > c.lastSeq[key] {
		c.lastSeq[key] = seq
	}
}

// Create writes key with If-None-Match: * — it succeeds only when the key
// does not exist. A 412 rejection returns ErrExists (not executed).
func (c *Client) Create(ctx context.Context, key string, body []byte) (string, error) {
	assertf(key != "", "Create: empty key")
	assertf(body != nil, "Create: nil body")
	return c.putConditional(ctx, "create", key, body, "")
}

// Advance overwrites key with If-Match: etag — the CAS advance. A 412
// rejection returns ErrLostRace (not executed; re-read and re-evaluate).
func (c *Client) Advance(ctx context.Context, key string, body []byte, etag string) (string, error) {
	assertf(key != "", "Advance: empty key")
	assertf(body != nil, "Advance: nil body")
	assertf(etag != "", "Advance: empty etag")
	return c.putConditional(ctx, "advance", key, body, etag)
}

// putConditional issues one conditional PUT, retrying only 409
// ConditionalRequestConflict (a concurrent conditional write in flight —
// retry the same attempt) under the §2.3 backoff. Every other outcome maps
// to exactly one taxonomy class.
func (c *Client) putConditional(ctx context.Context, op, key string, body []byte, ifMatch string) (string, error) {
	assertf(c.maxAttempts > 0, "putConditional: non-positive attempt budget %d", c.maxAttempts)
	for attempt := 0; ; attempt++ {
		// Ctx already expired while queued = provably not sent.
		if err := ctx.Err(); err != nil {
			return "", fmt.Errorf("s3wal: %s %s: %w (%w)", op, key, ErrNotExecuted, err)
		}
		in := &s3.PutObjectInput{
			Bucket: aws.String(c.bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(body),
		}
		if ifMatch == "" {
			in.IfNoneMatch = aws.String("*")
		} else {
			in.IfMatch = aws.String(ifMatch)
		}
		out, err := c.s3.PutObject(ctx, in)
		if err == nil {
			etag := aws.ToString(out.ETag)
			assertf(etag != "", "%s %s: store accepted the write but returned no ETag", op, key)
			c.raiseSeq(key, body)
			return etag, nil
		}
		status, code := errorStatus(err)
		switch {
		case status == 412:
			// Not executed: the precondition lost. Distinct sentinels keep
			// create-vs-advance recovery paths honest in Phase 1b.
			if op == "create" {
				return "", fmt.Errorf("%s %s: %w", op, key, ErrExists)
			}
			return "", fmt.Errorf("%s %s: %w", op, key, ErrLostRace)
		case status == 409 && code == "ConditionalRequestConflict":
			// Concurrent conditional write in flight: retry the same attempt.
			if attempt+1 >= c.maxAttempts {
				return "", fmt.Errorf("%s %s: %d attempts: %w", op, key, c.maxAttempts, ErrCASContention)
			}
			if err := c.sleepJitter(ctx, attempt); err != nil {
				return "", fmt.Errorf("s3wal: %s %s: %w (%w)", op, key, ErrNotExecuted, err)
			}
		case status >= 500:
			// The store executed something and failed after the request was
			// sent — the write may have landed.
			return "", &AmbiguousError{Op: op, Key: key, Cause: err}
		case status > 0:
			// Executed with a definitive non-conditional failure (auth,
			// missing bucket, malformed request): plain error, not ambiguous.
			return "", fmt.Errorf("s3wal: %s %s: %w", op, key, err)
		default:
			return "", classifyTransport(op, key, err)
		}
	}
}

// Delete removes key unconditionally. The protocol never guards a DELETE
// with a precondition (§2.7); Phase 1a uses this only for probe scratch
// cleanup and test teardown.
func (c *Client) Delete(ctx context.Context, key string) error {
	assertf(key != "", "Delete: empty key")
	if _, err := c.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	}); err != nil {
		return fmt.Errorf("s3wal: delete %s: %w", key, err)
	}
	return nil
}

// classifyTransport splits no-HTTP-response failures: a dial-class error
// provably never sent the request (not executed); anything else may have
// reached the store mid-flight and defaults to ambiguous (fail-closed).
func classifyTransport(op, key string, err error) error {
	var oe *net.OpError
	if errors.As(err, &oe) && oe.Op == "dial" {
		return fmt.Errorf("s3wal: %s %s: %w (%w)", op, key, ErrNotExecuted, err)
	}
	return &AmbiguousError{Op: op, Key: key, Cause: err}
}

// errorStatus extracts the HTTP status and API error code from an SDK
// error. status 0 means no HTTP response was observed (transport-level
// failure).
func errorStatus(err error) (status int, code string) {
	var re *awshttp.ResponseError
	if errors.As(err, &re) {
		status = re.HTTPStatusCode()
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		code = ae.ErrorCode()
	}
	return status, code
}

// sleepJitter blocks for the §2.3 full-jitter backoff delay:
// rand(0, min(cap, base·2^attempt)), bounded by ctx.
func (c *Client) sleepJitter(ctx context.Context, attempt int) error {
	assertf(attempt >= 0 && attempt < 63, "sleepJitter: attempt %d out of range", attempt)
	ceiling := min(c.backoffCap, c.backoffBase<<uint(attempt))
	assertf(ceiling > 0, "sleepJitter: non-positive backoff ceiling %v", ceiling)
	timer := time.NewTimer(rand.N(ceiling))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
