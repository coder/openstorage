package s3wal_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// A racing GET that fails its op deadline is runner noise when the test
// process itself stopped: every goroutine freezes together (a paused runner,
// a throttled cgroup, SIGSTOP), so the deadline timer runs out while no
// response can be read. A Floci that stalls leaves the process running.
// watchStalls records the freezes, and explainedByStall tells the two
// apart for one failed read (issue #566).

const (
	stallTick = 5 * time.Millisecond // heartbeat period
	// stallGap is the gap between two ticks that counts as a freeze. A busy
	// host delays the heartbeat by 20 to 80 ms without freezing the process,
	// and a read that Floci holds must not look frozen because of that.
	stallGap        = 100 * time.Millisecond
	maxReadFailures = 256 // failed reads kept for classification; the rest count as unexplained
	maxLogged       = 3   // failed reads quoted in the verdict detail
)

// Every time here is an offset from the start of the race, taken with
// time.Since on a time.Now base, so it reads the monotonic clock only: a
// correction of the wall clock cannot move a stall or a read (issue #566).

// stallSpan is a time during which the test process ran nothing.
type stallSpan struct{ from, to time.Duration }

// stallTracker turns heartbeat ticks into the spans between two ticks that
// were at least minGap apart.
type stallTracker struct {
	last   time.Duration
	minGap time.Duration
	spans  []stallSpan
}

func (s *stallTracker) tick(at time.Duration) {
	if at-s.last >= s.minGap {
		s.spans = append(s.spans, stallSpan{s.last, at})
	}
	s.last = at
}

// watchStalls starts a heartbeat that ticks every tick, with offsets taken
// from base. The returned stop ends it with one last tick and returns the
// spans. It may be called more than once.
func watchStalls(base time.Time, tick time.Duration) (stop func() []stallSpan) {
	tr := &stallTracker{minGap: stallGap}
	quit, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(tick)
		defer t.Stop()
		for {
			select {
			case <-quit:
				tr.tick(time.Since(base))
				return
			case <-t.C:
				tr.tick(time.Since(base))
			}
		}
	}()
	var once sync.Once
	return func() []stallSpan {
		once.Do(func() { close(quit); <-done })
		return tr.spans
	}
}

func longestStall(spans []stallSpan) (longest time.Duration) {
	for _, s := range spans {
		longest = max(longest, s.to-s.from)
	}
	return longest
}

// readFailure is one racing GET that failed.
type readFailure struct {
	start, end time.Duration
	err        error
}

func (f readFailure) elapsed() time.Duration { return f.end - f.start }

// stalledFor is how long the test process ran nothing during the read.
func (f readFailure) stalledFor(spans []stallSpan) (frozen time.Duration) {
	for _, s := range spans {
		if from, to := max(f.start, s.from), min(f.end, s.to); to > from {
			frozen += to - from
		}
	}
	return frozen
}

// explainedByStall reports whether a stall of the test process explains the
// failed read: it hit the op deadline, and the process was awake for under a
// twentieth of that deadline. A healthy GET answers in milliseconds and a read
// that a freeze took stays awake for only as long as the freeze took to start
// and end, so a read that ran awake for 100 ms was waiting for Floci or the
// network, not for the test process.
func explainedByStall(f readFailure, spans []stallSpan, opTimeout time.Duration) bool {
	return errors.Is(f.err, context.DeadlineExceeded) && f.elapsed()-f.stalledFor(spans) < opTimeout/20
}

// shortBodyError marks a GET whose headers arrived and whose body did not
// complete.
type shortBodyError struct{ error }

func (e shortBodyError) Unwrap() error { return e.error }

// readErrClass names the shape of a failed read for the verdict detail.
// "short-body" is the framing shape: the headers came and the body ended
// early or stopped coming before its Content-Length. "deadline" is a read
// that hit the op deadline before any header. "status" is an error reply
// from the store.
func readErrClass(err error) string {
	var reply *awshttp.ResponseError
	switch {
	case errors.As(err, new(shortBodyError)):
		return "short-body"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, io.EOF), errors.Is(err, syscall.ECONNRESET), errors.Is(err, syscall.EPIPE), errors.Is(err, net.ErrClosed):
		return "reset"
	case errors.As(err, &reply) && reply.HTTPStatusCode() != 0:
		return "status"
	default:
		return "other"
	}
}

func TestExplainedByStall(t *testing.T) {
	const opTimeout = 2 * time.Second
	span := func(from, to time.Duration) stallSpan { return stallSpan{from, to} }
	deadline := fmt.Errorf("read body after headers (0 of Content-Length 256 bytes): %w", context.DeadlineExceeded)
	for _, tc := range []struct {
		name  string
		err   error
		start time.Duration
		end   time.Duration
		spans []stallSpan
		want  bool
	}{
		{"process frozen for the whole read", deadline, 0, 2500 * time.Millisecond, []stallSpan{span(5*time.Millisecond, 2500*time.Millisecond)}, true},
		{"freeze started before the read", deadline, 100 * time.Millisecond, 2100 * time.Millisecond, []stallSpan{span(0, 2150*time.Millisecond)}, true},
		{"freeze ended after the read", deadline, 0, 2200 * time.Millisecond, []stallSpan{span(30*time.Millisecond, 4*time.Second)}, true},
		{"many short freezes add up", deadline, 0, 2100 * time.Millisecond,
			[]stallSpan{span(0, 520*time.Millisecond), span(540*time.Millisecond, 1060*time.Millisecond), span(1080*time.Millisecond, 1600*time.Millisecond), span(1620*time.Millisecond, 2080*time.Millisecond)}, true},
		{"no freeze: Floci or the network held the read", deadline, 0, 2000 * time.Millisecond, nil, false},
		{"a short freeze inside a read held by Floci", deadline, 0, 2000 * time.Millisecond, []stallSpan{span(300*time.Millisecond, 600*time.Millisecond)}, false},
		{"freeze outside the read", deadline, 0, 2000 * time.Millisecond, []stallSpan{span(3*time.Second, 6*time.Second)}, false},
		{"awake just under a twentieth of the deadline", deadline, 0, 2500 * time.Millisecond, []stallSpan{span(0, 2401*time.Millisecond)}, true},
		{"awake a twentieth of the deadline", deadline, 0, 2500 * time.Millisecond, []stallSpan{span(0, 2400*time.Millisecond)}, false},
		{"Floci held the read while the process froze for 1.5 s of it", deadline, 0, 2000 * time.Millisecond, []stallSpan{span(0, 1500*time.Millisecond)}, false},
		{"Floci paused 2.5 s and the process froze 1.6 s of it (awake 410 ms)", deadline, 3207 * time.Millisecond, 5208 * time.Millisecond, []stallSpan{span(3220*time.Millisecond, 4810*time.Millisecond)}, false},
		{"not a deadline error", io.ErrUnexpectedEOF, 0, 2500 * time.Millisecond, []stallSpan{span(0, 2500*time.Millisecond)}, false},
		{"connection reset during a freeze", syscall.ECONNRESET, 0, 2500 * time.Millisecond, []stallSpan{span(0, 2500*time.Millisecond)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := readFailure{start: tc.start, end: tc.end, err: tc.err}
			if got := explainedByStall(f, tc.spans, opTimeout); got != tc.want {
				t.Fatalf("explainedByStall = %t, want %t (elapsed %s, stalled %s)", got, tc.want, f.elapsed(), f.stalledFor(tc.spans))
			}
		})
	}
}

// TestStallGapIgnoresHostJitter: the delays a busy host gives the heartbeat
// are not freezes, and a stop of the whole process is.
func TestStallGapIgnoresHostJitter(t *testing.T) {
	tr := &stallTracker{minGap: stallGap}
	var now time.Duration
	for _, step := range []time.Duration{5, 25, 70, 40, 80, 5, 60, 99} {
		now += step * time.Millisecond
		tr.tick(now)
	}
	if len(tr.spans) != 0 {
		t.Fatalf("host jitter recorded as freezes: %v", tr.spans)
	}
	tr.tick(now + 2500*time.Millisecond)
	if len(tr.spans) != 1 || longestStall(tr.spans) != 2500*time.Millisecond {
		t.Fatalf("a 2.5 s stop recorded as %v", tr.spans)
	}
}

func TestStallTracker(t *testing.T) {
	ms := func(n int) time.Duration { return time.Duration(n) * time.Millisecond }
	tr := &stallTracker{minGap: 20 * time.Millisecond}
	for _, n := range []int{5, 10, 15, 2500, 2505, 2530, 2550} {
		tr.tick(ms(n))
	}
	// 15 ms to 2500 ms is a stall. So are 2505 ms to 2530 ms (25 ms) and
	// 2530 ms to 2550 ms (exactly the 20 ms gap).
	want := []stallSpan{{ms(15), ms(2500)}, {ms(2505), ms(2530)}, {ms(2530), ms(2550)}}
	if !slices.Equal(tr.spans, want) {
		t.Fatalf("spans = %v, want %v", tr.spans, want)
	}
	if got := longestStall(tr.spans); got != 2485*time.Millisecond {
		t.Fatalf("longestStall = %s, want 2.485s", got)
	}
}

// TestReadFailureShapes reads from a server that misbehaves the three ways
// a verdict detail has to tell apart, through the probe's own client
// options and getWithETag, so the class names describe the real SDK errors.
func TestReadFailureShapes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/hang"):
			<-r.Context().Done()
		case strings.HasSuffix(r.URL.Path, "/stallbody"):
			w.Header().Set("Content-Length", "100")
			_, _ = w.Write(bytes.Repeat([]byte("a"), 10))
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		case strings.HasSuffix(r.URL.Path, "/short"):
			w.Header().Set("Content-Length", "100")
			_, _ = w.Write(bytes.Repeat([]byte("a"), 10))
			w.(http.Flusher).Flush()
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			_ = conn.Close()
		case strings.HasSuffix(r.URL.Path, "/closed"):
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			_ = conn.Close()
		default:
			http.Error(w, "boom", http.StatusInternalServerError)
		}
	}))
	t.Cleanup(srv.Close)
	opts := rawS3Client(t, srv.URL, "us-east-1", "k", "s").Options()
	opts.HTTPClient = awshttp.NewBuildableClient()
	opts.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	opts.Retryer = aws.NopRetryer{}
	client := s3.New(opts)
	for _, tc := range []struct {
		key, class string
		timeout    time.Duration // only the hang may reach it
	}{
		{"hang", "deadline", 300 * time.Millisecond}, {"stallbody", "short-body", 300 * time.Millisecond},
		{"short", "short-body", time.Minute}, {"closed", "reset", time.Minute}, {"status", "status", time.Minute},
	} {
		t.Run(tc.key, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), tc.timeout)
			defer cancel()
			_, _, err := getWithETag(ctx, client, "b", tc.key)
			if err == nil {
				t.Fatal("getWithETag succeeded")
			}
			if got := readErrClass(err); got != tc.class {
				t.Fatalf("readErrClass = %q, want %q for %v", got, tc.class, err)
			}
		})
	}
}

// TestReadErrClass names every failure the SDK can hand back, wrapped as it
// arrives.
func TestReadErrClass(t *testing.T) {
	wrap := func(err error) error { return fmt.Errorf("operation error S3: GetObject: %w", err) }
	status := &awshttp.ResponseError{ResponseError: &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusInternalServerError}},
		Err:      errors.New("boom"),
	}}
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"op deadline", wrap(context.DeadlineExceeded), "deadline"},
		{"body that stopped coming", shortBodyError{wrap(context.DeadlineExceeded)}, "short-body"},
		{"body that ended early", shortBodyError{wrap(io.ErrUnexpectedEOF)}, "short-body"},
		{"connection closed before the reply", wrap(io.EOF), "reset"},
		{"connection reset", wrap(syscall.ECONNRESET), "reset"},
		{"broken pipe", wrap(syscall.EPIPE), "reset"},
		{"closed connection", wrap(net.ErrClosed), "reset"},
		{"error reply from the store", status, "status"},
		{"anything else", wrap(errors.New("boom")), "other"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := readErrClass(tc.err); got != tc.want {
				t.Fatalf("readErrClass = %q, want %q for %v", got, tc.want, tc.err)
			}
		})
	}
}

// TestWatchStallsClosingTick: a heartbeat that never ticks is a process that
// ran nothing, and the closing tick of stop reports the whole run as one
// freeze.
func TestWatchStallsClosingTick(t *testing.T) {
	stop := watchStalls(time.Now(), time.Hour)
	time.Sleep(2 * stallGap)
	spans := stop()
	if len(spans) != 1 || spans[0].from != 0 || spans[0].to < 2*stallGap {
		t.Fatalf("spans = %v, want one from 0 of at least %s", spans, 2*stallGap)
	}
}
