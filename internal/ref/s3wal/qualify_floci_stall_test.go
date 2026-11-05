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
)

// A racing GET that fails its op deadline is runner noise when the test
// process itself stopped: every goroutine freezes together (a paused runner,
// a throttled cgroup, SIGSTOP), so the deadline timer runs out while no
// response can be read. A Floci that stalls leaves the process running.
// watchStalls records the freezes, and explainedByStall tells the two
// apart for one failed read (issue #566).

const (
	stallTick       = 5 * time.Millisecond  // heartbeat period
	stallGap        = 20 * time.Millisecond // two ticks this far apart mean the process was frozen
	maxReadFailures = 256                   // failed reads kept for classification; the rest count as unexplained
)

// stallSpan is a time during which the test process ran nothing.
type stallSpan struct{ from, to time.Time }

// stallTracker turns heartbeat ticks into the spans between two ticks that
// were at least minGap apart.
type stallTracker struct {
	last   time.Time
	minGap time.Duration
	spans  []stallSpan
}

func (s *stallTracker) tick(now time.Time) {
	if now.Sub(s.last) >= s.minGap {
		s.spans = append(s.spans, stallSpan{s.last, now})
	}
	s.last = now
}

// watchStalls starts the heartbeat. The returned stop ends it and returns
// the spans. It may be called more than once.
func watchStalls() (stop func() []stallSpan) {
	tr := &stallTracker{last: time.Now(), minGap: stallGap}
	quit, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(stallTick)
		defer t.Stop()
		for {
			select {
			case <-quit:
				tr.tick(time.Now())
				return
			case <-t.C:
				tr.tick(time.Now())
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
		longest = max(longest, s.to.Sub(s.from))
	}
	return longest
}

// readFailure is one racing GET that failed.
type readFailure struct {
	start, end time.Time
	err        error
}

func (f readFailure) elapsed() time.Duration { return f.end.Sub(f.start) }

// stalledFor is how long the test process ran nothing during the read.
func (f readFailure) stalledFor(spans []stallSpan) (frozen time.Duration) {
	for _, s := range spans {
		if from, to := max(f.start.UnixNano(), s.from.UnixNano()), min(f.end.UnixNano(), s.to.UnixNano()); to > from {
			frozen += time.Duration(to - from)
		}
	}
	return frozen
}

// explainedByStall reports whether a stall of the test process explains the
// failed read: it hit the op deadline, and the process was awake for under a
// quarter of that deadline. A healthy GET answers in milliseconds, so a read
// that ran awake for longer was waiting for Floci or the network, not for
// the test process.
func explainedByStall(f readFailure, spans []stallSpan, opTimeout time.Duration) bool {
	return errors.Is(f.err, context.DeadlineExceeded) && f.elapsed()-f.stalledFor(spans) < opTimeout/4
}

// readErrClass names the shape of a failed read for the verdict detail.
// "short-body" is the framing shape: the response ended before its
// Content-Length. "status" is an error reply from the store.
func readErrClass(err error) string {
	var reply *awshttp.ResponseError
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "short-body"
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
	t0 := time.Unix(1000, 0)
	at := func(d time.Duration) time.Time { return t0.Add(d) }
	span := func(from, to time.Duration) stallSpan { return stallSpan{at(from), at(to)} }
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
			[]stallSpan{span(0, 500*time.Millisecond), span(600*time.Millisecond, 1100*time.Millisecond), span(1200*time.Millisecond, 1700*time.Millisecond), span(1800*time.Millisecond, 2080*time.Millisecond)}, true},
		{"no freeze: Floci or the network held the read", deadline, 0, 2000 * time.Millisecond, nil, false},
		{"a short freeze inside a read held by Floci", deadline, 0, 2000 * time.Millisecond, []stallSpan{span(300*time.Millisecond, 600*time.Millisecond)}, false},
		{"freeze outside the read", deadline, 0, 2000 * time.Millisecond, []stallSpan{span(3*time.Second, 6*time.Second)}, false},
		{"awake just under a quarter of the deadline", deadline, 0, 2500 * time.Millisecond, []stallSpan{span(0, 2001*time.Millisecond)}, true},
		{"awake a quarter of the deadline", deadline, 0, 2500 * time.Millisecond, []stallSpan{span(0, 2000*time.Millisecond)}, false},
		{"not a deadline error", io.ErrUnexpectedEOF, 0, 2500 * time.Millisecond, []stallSpan{span(0, 2500*time.Millisecond)}, false},
		{"connection reset during a freeze", syscall.ECONNRESET, 0, 2500 * time.Millisecond, []stallSpan{span(0, 2500*time.Millisecond)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := readFailure{start: at(tc.start), end: at(tc.end), err: tc.err}
			if got := explainedByStall(f, tc.spans, opTimeout); got != tc.want {
				t.Fatalf("explainedByStall = %t, want %t (elapsed %s, stalled %s)", got, tc.want, f.elapsed(), f.stalledFor(tc.spans))
			}
		})
	}
}

func TestStallTracker(t *testing.T) {
	t0 := time.Unix(1000, 0)
	tr := &stallTracker{last: t0, minGap: 20 * time.Millisecond}
	for _, n := range []int{5, 10, 15, 2500, 2505, 2530, 2550} {
		tr.tick(t0.Add(time.Duration(n) * time.Millisecond))
	}
	// 15 ms to 2500 ms is a stall. So are 2505 ms to 2530 ms (25 ms) and
	// 2530 ms to 2550 ms (exactly the 20 ms gap).
	ms := func(n int) time.Time { return t0.Add(time.Duration(n) * time.Millisecond) }
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
	for _, tc := range []struct{ key, class string }{
		{"hang", "deadline"}, {"short", "short-body"}, {"closed", "reset"}, {"status", "status"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
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

// TestRaceVerdict is the rule of the surface: a failed read that nothing
// explains never gives a pass, and a stall explains only the reads it covers.
func TestRaceVerdict(t *testing.T) {
	clean := raceTally{reads: 20000, writeOKs: 7000}
	for _, tc := range []struct {
		name string
		c    raceTally
		want string
	}{
		{"clean race", clean, verdictPass},
		{"every failed read explained by a stall", raceTally{reads: 20000, writeOKs: 7000, readErrs: 2, stallErrs: 2}, verdictPass},
		{"one failed read nothing explains", raceTally{reads: 20000, writeOKs: 7000, readErrs: 1}, verdictError},
		{"one failed read of two explained", raceTally{reads: 20000, writeOKs: 7000, readErrs: 2, stallErrs: 1}, verdictError},
		{"torn body", raceTally{reads: 20000, writeOKs: 7000, tornBody: 1}, verdictMishandles},
		{"torn metadata beside an explained stall", raceTally{reads: 20000, writeOKs: 7000, tornMeta: 1, readErrs: 1, stallErrs: 1}, verdictMishandles},
		{"too few reads", raceTally{reads: 49, writeOKs: 7000}, verdictError},
		{"too few overwrites", raceTally{reads: 20000, writeOKs: 49}, verdictError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := raceVerdict("get-under-overwrite", tc.c, 50, 50, "detail").class; got != tc.want {
				t.Fatalf("verdict class = %q, want %q", got, tc.want)
			}
		})
	}
}
