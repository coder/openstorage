package s3wal_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// getUnderOverwrite names the get-under-overwrite surface in its verdicts.
const getUnderOverwrite = "get-under-overwrite"

// overwriteRace is one window of the get-under-overwrite probe: a writer
// flips one key between two versions with conditional PUTs while readers GET
// it. The race reaches the store only through put, head and get, so a test
// can run it against a store that misbehaves on demand (issue #566).
type overwriteRace struct {
	window, opTimeout   time.Duration
	readers             int
	minReads, minWrites int64 // below these a "pass" would be too weak to record
	bodyA, bodyB        []byte
	etagOf              map[string]string // whole version → its ETag
	metaCheck           bool              // the two ETags are stable and distinct, so torn metadata can be told
	startETag           string            // the ETag of the last seed write
	put                 func(ctx context.Context, body []byte, ifMatch string) (etag string, err error)
	head                func(ctx context.Context) (etag string, err error)
	get                 func(ctx context.Context) (body []byte, etag string, err error)
}

// raceObservation is everything one race window recorded.
type raceObservation struct {
	reads, tornBody, tornMeta, readErrs, writeOKs, writeErrs int64
	failures                                                 []readFailure // the first maxReadFailures failed reads
	spans                                                    []stallSpan
	maxRead                                                  time.Duration // the slowest successful GET
}

// run races the writer against the readers for the window and returns what
// they saw. The window ends only the loops. A GET in flight at its end keeps
// its own op deadline: cancelling it would hide a store that hangs for the
// last seconds of the window (issue #566).
func (r overwriteRace) run(ctx context.Context) raceObservation {
	raceCtx, stopRace := context.WithCancel(ctx)
	defer stopRace()
	var reads, tornBody, tornMeta, readErrs, writeOKs, writeErrs atomic.Int64
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		curETag := r.startETag
		next := r.bodyA // always the version OPPOSITE the last known content
		for raceCtx.Err() == nil {
			etag, err := r.put(raceCtx, next, curETag)
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
				if cur, herr := r.head(raceCtx); herr == nil {
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
			if bytes.Equal(next, r.bodyA) {
				next = r.bodyB
			} else {
				next = r.bodyA
			}
		}
	}()
	// Failed reads are kept (up to maxReadFailures) and classified once the
	// race is over, when the heartbeat has recorded every stall of the test
	// process (issue #566).
	var mu sync.Mutex // guards failures and maxRead
	var failures []readFailure
	var maxRead time.Duration
	base := time.Now()
	stopStalls := watchStalls(base, stallTick)
	defer stopStalls()
	var readersDone sync.WaitGroup
	for range r.readers {
		readersDone.Add(1)
		go func() {
			defer readersDone.Done()
			for raceCtx.Err() == nil {
				opCtx, cancel := context.WithTimeout(ctx, r.opTimeout)
				start := time.Since(base)
				body, etag, err := r.get(opCtx)
				end := time.Since(base)
				cancel()
				if err != nil {
					readErrs.Add(1)
					mu.Lock()
					if len(failures) < maxReadFailures {
						failures = append(failures, readFailure{start: start, end: end, err: err})
					}
					mu.Unlock()
					continue
				}
				reads.Add(1)
				mu.Lock()
				maxRead = max(maxRead, end-start)
				mu.Unlock()
				want, whole := r.etagOf[string(body)]
				switch {
				case !whole:
					tornBody.Add(1)
					stopRace() // first torn observation settles the verdict
				case r.metaCheck && etag != want:
					tornMeta.Add(1)
					stopRace()
				}
			}
		}()
	}
	select {
	case <-time.After(r.window):
	case <-raceCtx.Done():
	}
	stopRace()
	<-writerDone
	readersDone.Wait()
	// Every reader has exited, so failures is no longer written.
	return raceObservation{
		reads: reads.Load(), tornBody: tornBody.Load(), tornMeta: tornMeta.Load(), readErrs: readErrs.Load(),
		writeOKs: writeOKs.Load(), writeErrs: writeErrs.Load(),
		failures: failures, spans: stopStalls(), maxRead: maxRead,
	}
}

// judge turns what a race window recorded into the surface's verdict. A
// failed read that nothing explains never gives a pass, and a freeze of the
// test process explains only the reads it covers.
func (r overwriteRace) judge(o raceObservation) verdict {
	stallErrs := int64(0)
	var quoted []string
	for _, f := range o.failures {
		stalled := explainedByStall(f, o.spans, r.opTimeout)
		if stalled {
			stallErrs++
		}
		if len(quoted) < maxLogged {
			quoted = append(quoted, fmt.Sprintf("{at=+%s elapsed=%s awake=%s class=%s explainedByStall=%t err=%q}",
				f.start.Round(time.Millisecond), f.elapsed().Round(time.Millisecond),
				(f.elapsed()-f.stalledFor(o.spans)).Round(time.Millisecond), readErrClass(f.err), stalled, f.err.Error()))
		}
	}
	assertf(int64(len(o.failures)) == min(o.readErrs, maxReadFailures), "kept %d read failures, counted %d", len(o.failures), o.readErrs)
	detail := fmt.Sprintf("reads=%d tornBody=%d tornMeta=%d readErrs=%d stallErrs=%d writeOKs=%d writeErrs=%d metaCheck=%t window=%s maxRead=%s maxStall=%s",
		o.reads, o.tornBody, o.tornMeta, o.readErrs, stallErrs, o.writeOKs, o.writeErrs, r.metaCheck, r.window,
		o.maxRead.Round(time.Millisecond), longestStall(o.spans).Round(time.Millisecond))
	if len(quoted) > 0 {
		detail += fmt.Sprintf(" firstReadErrs(%d of %d)=%s", len(quoted), o.readErrs, strings.Join(quoted, " "))
	}
	switch {
	case o.tornBody > 0:
		return verdict{getUnderOverwrite, verdictMishandles, "torn body: a racing GET returned bytes of neither whole version — " + detail}
	case o.tornMeta > 0:
		return verdict{getUnderOverwrite, verdictMishandles, "torn metadata: a racing GET returned one whole version with the other version's ETag — " + detail}
	case o.readErrs > stallErrs:
		// Framing corruption can surface as failed reads rather than a
		// classifiable torn body (a short-for-Content-Length response
		// blocks until the op deadline; a poisoned keep-alive breaks the
		// next request). A read failure counts as runner noise only when
		// the test process itself stopped for it (explainedByStall). Any
		// other failure makes the probe inconclusive, never a clean pass.
		// firstReadErrs names the class of the first few (issue #566):
		// "short-body" is the framing shape, "reset" a closed connection,
		// and "deadline" with a long awake time a Floci or network stall.
		return verdict{getUnderOverwrite, verdictError, "racing GETs failed without a classifiable torn read — inconclusive (possible framing corruption) — " + detail}
	case o.reads < r.minReads || o.writeOKs < r.minWrites:
		return verdict{getUnderOverwrite, verdictError, "too few successful reads/overwrites to have exercised the race — " + detail}
	case stallErrs > 0:
		return verdict{getUnderOverwrite, verdictPass, fmt.Sprintf("every racing GET that completed returned one whole version with its own ETag, and %d failed GETs hit the op deadline while the test process was frozen — ", stallErrs) + detail}
	default:
		return verdict{getUnderOverwrite, verdictPass, "every racing GET returned one whole version with its own ETag — " + detail}
	}
}

// memRace is an overwriteRace over a store in memory: ETags are content
// addressed like the real stores', and a PUT honours If-Match. Each op takes
// a moment, so the loops do not spin.
func memRace(window time.Duration) overwriteRace {
	bodyA, bodyB := bytes.Repeat([]byte("a"), 256), bytes.Repeat([]byte("b"), 300)
	tag := func(b []byte) string { sum := md5.Sum(b); return `"` + hex.EncodeToString(sum[:]) + `"` }
	var mu sync.Mutex
	body, etag := bodyB, tag(bodyB)
	return overwriteRace{
		window: window, opTimeout: 500 * time.Millisecond, readers: 2, minReads: 20, minWrites: 20,
		bodyA: bodyA, bodyB: bodyB, etagOf: map[string]string{string(bodyA): tag(bodyA), string(bodyB): tag(bodyB)},
		metaCheck: true, startETag: etag,
		put: func(_ context.Context, b []byte, ifMatch string) (string, error) {
			time.Sleep(200 * time.Microsecond)
			mu.Lock()
			defer mu.Unlock()
			if ifMatch != etag {
				return "", errors.New("412 precondition failed")
			}
			body, etag = b, tag(b)
			return etag, nil
		},
		head: func(context.Context) (string, error) {
			mu.Lock()
			defer mu.Unlock()
			return etag, nil
		},
		get: func(context.Context) ([]byte, string, error) {
			time.Sleep(200 * time.Microsecond)
			mu.Lock()
			defer mu.Unlock()
			return body, etag, nil
		},
	}
}

func TestOverwriteRaceCleanPass(t *testing.T) {
	r := memRace(400 * time.Millisecond)
	o := r.run(t.Context())
	if v := r.judge(o); v.class != verdictPass {
		t.Fatalf("verdict %q (%s), want pass; observation %+v", v.class, v.detail, o)
	}
	if o.readErrs != 0 || len(o.failures) != 0 || o.maxRead <= 0 {
		t.Fatalf("clean race observed readErrs=%d failures=%v maxRead=%s", o.readErrs, o.failures, o.maxRead)
	}
}

// TestOverwriteRaceTornBodyEndsTheRace: the first torn GET settles the
// verdict, so the race does not run out its window.
func TestOverwriteRaceTornBodyEndsTheRace(t *testing.T) {
	r := memRace(5 * time.Second)
	ok := r.get
	var calls atomic.Int64
	r.get = func(ctx context.Context) ([]byte, string, error) {
		if calls.Add(1) == 30 {
			return []byte("aaaabbbb"), `"x"`, nil
		}
		return ok(ctx)
	}
	start := time.Now()
	o := r.run(t.Context())
	if took := time.Since(start); took > time.Second {
		t.Fatalf("race ran %s after a torn body", took)
	}
	if v := r.judge(o); v.class != verdictMishandles || o.tornBody != 1 {
		t.Fatalf("verdict %q tornBody=%d, want mishandles and 1", v.class, o.tornBody)
	}
}

// TestOverwriteRaceCountsAReadHungAtTheWindowEnd: a store that hangs for the
// last part of the window is not a pass. The read in flight when the window
// ends keeps its own deadline and fails on it (issue #566).
func TestOverwriteRaceCountsAReadHungAtTheWindowEnd(t *testing.T) {
	r := memRace(500 * time.Millisecond)
	ok := r.get
	start := time.Now()
	r.get = func(ctx context.Context) ([]byte, string, error) {
		if time.Since(start) > 300*time.Millisecond {
			<-ctx.Done()
			return nil, "", ctx.Err()
		}
		return ok(ctx)
	}
	o := r.run(t.Context())
	if o.readErrs != 2 || len(o.failures) != 2 {
		t.Fatalf("readErrs=%d failures=%v, want the 2 hung reads", o.readErrs, o.failures)
	}
	for _, f := range o.failures {
		if !errors.Is(f.err, context.DeadlineExceeded) || f.end <= r.window {
			t.Fatalf("failure %+v, want an op deadline after the window end", f)
		}
	}
	if v := r.judge(o); v.class != verdictError {
		t.Fatalf("verdict %q (%s), want error", v.class, v.detail)
	}
}

// TestOverwriteRaceKeepsTheFirstFailures: a store that fails every read fills
// the cap, and the reads beyond it are still counted.
func TestOverwriteRaceKeepsTheFirstFailures(t *testing.T) {
	r := memRace(100 * time.Millisecond)
	r.get = func(context.Context) ([]byte, string, error) { return nil, "", io.ErrUnexpectedEOF }
	o := r.run(t.Context())
	if len(o.failures) != 256 || o.readErrs <= 256 {
		t.Fatalf("kept %d of %d failed reads, want the first 256 of more", len(o.failures), o.readErrs)
	}
	if v := r.judge(o); v.class != verdictError {
		t.Fatalf("verdict %q, want error", v.class)
	}
}

func TestOverwriteRaceReportsTheSlowestRead(t *testing.T) {
	r := memRace(300 * time.Millisecond)
	ok := r.get
	var calls atomic.Int64
	r.get = func(ctx context.Context) ([]byte, string, error) {
		if calls.Add(1) == 10 {
			time.Sleep(60 * time.Millisecond)
		}
		return ok(ctx)
	}
	if o := r.run(t.Context()); o.maxRead < 60*time.Millisecond {
		t.Fatalf("maxRead = %s, want at least the 60 ms read", o.maxRead)
	}
}

// TestJudgeRace is the rule of the surface: a failed read that nothing
// explains never gives a pass, and a freeze of the test process explains only
// the reads it covers.
func TestJudgeRace(t *testing.T) {
	r := overwriteRace{window: 15 * time.Second, opTimeout: 2 * time.Second, minReads: 50, minWrites: 50, metaCheck: true}
	ms := func(n int) time.Duration { return time.Duration(n) * time.Millisecond }
	deadline := fmt.Errorf("operation error S3: GetObject: %w", context.DeadlineExceeded)
	failed := func(start, end int, err error) readFailure { return readFailure{ms(start), ms(end), err} }
	frozen := failed(0, 2500, deadline) // the process froze for all of it
	frozenSpan := []stallSpan{{ms(5), ms(2500)}}
	clean := raceObservation{reads: 20000, writeOKs: 7000}
	with := func(f func(o *raceObservation)) raceObservation { o := clean; f(&o); return o }

	for _, tc := range []struct {
		name string
		o    raceObservation
		want string
		text string // the verdict detail says this
		not  string // and does not say this
	}{
		{"clean race", clean, verdictPass, "every racing GET returned one whole version", "frozen"},
		{"every failed read in a freeze of the test process", with(func(o *raceObservation) {
			o.readErrs, o.failures, o.spans = 2, []readFailure{frozen, frozen}, frozenSpan
		}), verdictPass, "every racing GET that completed returned one whole version with its own ETag, and 2 failed GETs hit the op deadline while the test process was frozen", "every racing GET returned"},
		{"one failed read nothing explains", with(func(o *raceObservation) {
			o.readErrs, o.failures = 1, []readFailure{failed(0, 2000, deadline)}
		}), verdictError, "inconclusive", ""},
		{"one failed read of two explained", with(func(o *raceObservation) {
			o.readErrs, o.failures, o.spans = 2, []readFailure{frozen, failed(5000, 7000, deadline)}, frozenSpan
		}), verdictError, "inconclusive", ""},
		{"a hung store while the process froze for 1.5 s of the read", with(func(o *raceObservation) {
			o.readErrs, o.failures, o.spans = 1, []readFailure{failed(0, 2000, deadline)}, []stallSpan{{0, ms(1500)}}
		}), verdictError, "explainedByStall=false", ""},
		{"a failed read that is no deadline, in a freeze", with(func(o *raceObservation) {
			o.readErrs, o.failures, o.spans = 1, []readFailure{failed(0, 2500, io.ErrUnexpectedEOF)}, frozenSpan
		}), verdictError, "class=other", ""},
		{"more failed reads than the kept ones", with(func(o *raceObservation) {
			o.readErrs, o.spans = maxReadFailures+1, frozenSpan
			for range maxReadFailures {
				o.failures = append(o.failures, frozen)
			}
		}), verdictError, "inconclusive", ""},
		{"torn body", with(func(o *raceObservation) { o.tornBody = 1 }), verdictMishandles, "torn body", ""},
		{"torn metadata beside an explained freeze", with(func(o *raceObservation) {
			o.tornMeta, o.readErrs, o.failures, o.spans = 1, 1, []readFailure{frozen}, frozenSpan
		}), verdictMishandles, "torn metadata", ""},
		{"too few reads", with(func(o *raceObservation) { o.reads = 49 }), verdictError, "too few", ""},
		{"too few overwrites", with(func(o *raceObservation) { o.writeOKs = 49 }), verdictError, "too few", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := r.judge(tc.o)
			if v.class != tc.want || !strings.Contains(v.detail, tc.text) || (tc.not != "" && strings.Contains(v.detail, tc.not)) {
				t.Fatalf("verdict %q %q, want %q saying %q and not %q", v.class, v.detail, tc.want, tc.text, tc.not)
			}
		})
	}
}

// TestJudgeRaceDetail pins what the transcript of a failed read says.
func TestJudgeRaceDetail(t *testing.T) {
	r := overwriteRace{window: 15 * time.Second, opTimeout: 2 * time.Second, minReads: 50, minWrites: 50, metaCheck: true}
	f := readFailure{3207 * time.Millisecond, 5208 * time.Millisecond, fmt.Errorf("op: %w", context.DeadlineExceeded)}
	v := r.judge(raceObservation{
		reads: 20000, writeOKs: 7000, writeErrs: 1, readErrs: 1, failures: []readFailure{f},
		spans: []stallSpan{{3220 * time.Millisecond, 5200 * time.Millisecond}}, maxRead: 17 * time.Millisecond,
	})
	for _, want := range []string{
		"reads=20000 tornBody=0 tornMeta=0 readErrs=1 stallErrs=1 writeOKs=7000 writeErrs=1 metaCheck=true window=15s maxRead=17ms maxStall=1.98s",
		`firstReadErrs(1 of 1)={at=+3.207s elapsed=2.001s awake=21ms class=deadline explainedByStall=true err="op: context deadline exceeded"}`,
	} {
		if !strings.Contains(v.detail, want) {
			t.Fatalf("detail %q lacks %q", v.detail, want)
		}
	}
	many := raceObservation{reads: 20000, writeOKs: 7000, readErrs: 5, failures: []readFailure{f, f, f, f, f}}
	if v := r.judge(many); strings.Count(v.detail, "{at=") != 3 || !strings.Contains(v.detail, "firstReadErrs(3 of 5)=") {
		t.Fatalf("detail %q, want 3 quoted failures of 5", v.detail)
	}
}
