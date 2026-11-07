package gitread

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/coder/openstorage/internal/gitlimit"
)

// Resource bounds for read-side git children (#313). Every git child that
// gitread starts runs through run, which applies them before git does any
// work. docs/reference/http-api.md ("Resource limits on git reads") is the
// client-facing description and ADR-049 the design; keep them in step.
const (
	// HeavyHeapBytes caps the heap (RLIMIT_DATA, Linux) of a child whose
	// work grows with history length, tree size, blob size, or output size.
	// Measured peak VmData on a 300,000-commit linear history (git 2.55):
	// `log -1 -- <file from the root commit>` 184 MB, `blame` of that file
	// 126 MB, `merge-base <tip> <root>` 130 MB, `log --skip=299990` 75 MB.
	// So a 300k history fits; histories past roughly 400k commits can fail
	// the per-file walks. Read-only pack mappings are shared page cache and
	// are not counted.
	HeavyHeapBytes = gitlimit.HeavyHeapBytes
	// LightHeapBytes caps a child whose work is bounded by one object:
	// ref resolution and single-commit reads. A commit object larger than
	// this cannot be read through the API.
	LightHeapBytes int64 = 32 << 20
	// DefaultOutputBytes is the buffered-stdout allowance of one request (a
	// Budget), shared by all of its children. Streaming children (CatBlobTo
	// into a caller's writer) are not counted; they never buffer in memory.
	DefaultOutputBytes int64 = 64 << 20
	// MaxBlobBytes is the largest blob the API streams (file, archive).
	// git reads a blob below core.bigFileThreshold (512 MiB) fully into its
	// heap, and a deltified blob also needs its base, so larger blobs would
	// hit HeavyHeapBytes part-way; callers check sizes up front instead.
	MaxBlobBytes int64 = 100 << 20

	// stderrKeepBytes bounds the stderr kept per child (its tail: git
	// prints its fatal message last).
	stderrKeepBytes = 64 << 10
	// maxErrArgvBytes bounds the git command line that errors quote.
	// An argv can be request-sized (a grep pattern, a list of paths, a
	// commit message; past 128 KiB, exec fails with E2BIG), and these
	// errors reach the server's log. 4096 keeps a command with any one path (PATH_MAX on Linux) whole.
	maxErrArgvBytes = 4096
	// childWaitDelay bounds how long Wait may block on git's output pipes
	// after git is killed (exec.Cmd.WaitDelay).
	childWaitDelay = 2 * time.Second
)

// Class sorts read-side git children by what their cost scales with.
type Class int

const (
	// Light: bounded by one object (ref resolution, single-commit reads).
	// Runs under LightHeapBytes.
	Light Class = iota + 1
	// Heavy: grows with history, tree, blob, or output size. Runs under
	// HeavyHeapBytes.
	Heavy
)

var (
	// ErrBusy: no admission slot freed up in time; git was not started.
	ErrBusy = gitlimit.ErrBusy
	// ErrTimeBudget: the request's git time budget ran out; the running
	// child (if any) was killed and reaped.
	ErrTimeBudget = errors.New("git reads exceeded the request time budget")
	// ErrOutputLimit: git's buffered output exceeded the request's
	// allowance; git was killed and reaped as soon as that happened.
	ErrOutputLimit = errors.New("git output exceeded the request output budget")
	// ErrBlobTooLarge: a blob is larger than MaxBlobBytes; git was not
	// started for it.
	ErrBlobTooLarge = errors.New("blob exceeds the API read size limit")
	// ErrMemoryLimit: git needed more heap than its class's cap and was
	// stopped by its own allocation failure (Linux only).
	ErrMemoryLimit = gitlimit.ErrMemoryLimit
	// ErrStop may be returned by a streaming Cmd.Stdout's Write to end git
	// early (a history walk that has seen enough): Run then kills and reaps
	// git and returns no error.
	ErrStop = errors.New("stop streaming git output")
)

// heapLimitAvailable reports whether heap caps are enforced here; a
// variable so tests can exercise the no-cap path on Linux.
var heapLimitAvailable = HeapLimitSupported

// IsBudgetError reports whether err is a resource-limit outcome (ErrBusy,
// ErrTimeBudget, ErrOutputLimit, ErrBlobTooLarge, ErrMemoryLimit). Callers
// that turn git failures into "not found" or "no match" must check it
// first so a limit is never reported as missing data.
func IsBudgetError(err error) bool {
	return errors.Is(err, ErrBusy) || errors.Is(err, ErrTimeBudget) || errors.Is(err, ErrOutputLimit) ||
		errors.Is(err, ErrBlobTooLarge) || errors.Is(err, ErrMemoryLimit)
}

// CheckBlobSize fails with ErrBlobTooLarge when a blob of size bytes is
// over the blob limit (MaxBlobBytes, or the Budget's in ctx). Streaming
// callers check before they start git or send response headers.
func CheckBlobSize(ctx context.Context, path string, size int64) error {
	limit := MaxBlobBytes
	if b := BudgetFrom(ctx); b != nil {
		limit = b.limits.MaxBlob
	}
	if size > limit {
		return fmt.Errorf("%q is %d bytes: %w (%d bytes)", path, size, ErrBlobTooLarge, limit)
	}
	return nil
}

// Limits are the per-child caps a Budget applies.
type Limits struct {
	HeavyHeap int64 // RLIMIT_DATA of Heavy children
	LightHeap int64 // RLIMIT_DATA of Light children
	MaxBlob   int64 // CheckBlobSize limit
}

// DefaultLimits returns HeavyHeapBytes, LightHeapBytes and MaxBlobBytes.
func DefaultLimits() Limits {
	return Limits{HeavyHeap: HeavyHeapBytes, LightHeap: LightHeapBytes, MaxBlob: MaxBlobBytes}
}

// Slots is a process-wide pool of request admission slots. A request
// holds at most one slot, from its first git child until it ends (see
// Budget). One key (a repository) may hold at most perKey of them, so slow
// requests against one repository cannot take the whole pool from the
// others. It is gitlimit's pool type, shared with cache maintenance.
type Slots = gitlimit.Slots

// NewSlots returns a pool of n slots, at most perKey of them per key. A
// request waits at most wait for a free slot before failing with ErrBusy.
func NewSlots(n, perKey int, wait time.Duration) *Slots { return gitlimit.NewSlots(n, perKey, wait) }

// Budget is one request's share of the read-side git limits. A handler
// middleware creates it, attaches it with WithBudget, and calls Close when
// the request has finished, including delivering its response. The
// request's first git child takes one admission slot, and the request
// keeps it until Close: the slot bounds everything the request retains
// (git children, buffered output, parsed results, spools), not just its
// git processes. Without a Budget in the context (write endpoints), Run
// applies none of these limits.
type Budget struct {
	slots   *Slots
	key     string // the Slots key (repository) this request counts against
	timeout time.Duration
	limits  Limits

	mu         sync.Mutex
	held       bool      // this request holds one slot
	deadline   time.Time // zero until the first child starts
	outputLeft int64
	closed     bool
}

// NewBudget returns a request budget drawing admission from slots under
// key (the request's repository), with a git time budget of timeout
// (counted from the first child, so repository hydration before it is not
// included) and outputBytes of buffered output.
func NewBudget(slots *Slots, key string, timeout time.Duration, outputBytes int64) *Budget {
	assertf(slots != nil, "NewBudget: nil slots")
	assertf(timeout > 0, "NewBudget: timeout %v", timeout)
	assertf(outputBytes > 0, "NewBudget: outputBytes %d", outputBytes)
	return &Budget{slots: slots, key: key, timeout: timeout, limits: DefaultLimits(), outputLeft: outputBytes}
}

// WithLimits replaces the per-child caps (tests lower them). It returns b.
func (b *Budget) WithLimits(l Limits) *Budget {
	assertf(l.HeavyHeap > 0 && l.LightHeap > 0 && l.MaxBlob > 0, "WithLimits: %+v", l)
	b.limits = l
	return b
}

// Limits returns the per-child caps b applies.
func (b *Budget) Limits() Limits { return b.limits }

type budgetKey struct{}

// WithBudget attaches b to ctx for every gitread call made with it.
func WithBudget(ctx context.Context, b *Budget) context.Context {
	assertf(b != nil, "WithBudget: nil budget")
	return context.WithValue(ctx, budgetKey{}, b)
}

// BudgetFrom returns the Budget attached to ctx, or nil.
func BudgetFrom(ctx context.Context) *Budget {
	b, _ := ctx.Value(budgetKey{}).(*Budget)
	return b
}

// Close ends the budget and releases its slot. Every child has been reaped
// by then (Run waits for git before returning).
func (b *Budget) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.held {
		b.slots.Release(b.key)
		b.held = false
	}
	b.closed = true
}

// start admits one child: the request's first child takes its slot
// (waiting at most the pool's wait), and starts the git clock. It returns
// the child's deadline.
func (b *Budget) start(ctx context.Context) (deadline time.Time, err error) {
	b.mu.Lock()
	assertf(!b.closed, "Budget: child started after Close")
	if b.deadline.IsZero() {
		b.deadline = time.Now().Add(b.timeout)
	}
	deadline = b.deadline
	if time.Now().After(deadline) {
		b.mu.Unlock()
		return time.Time{}, fmt.Errorf("%w (%v)", ErrTimeBudget, b.timeout)
	}
	if b.held {
		b.mu.Unlock()
		return deadline, nil
	}
	b.mu.Unlock()
	// Wait for a slot without holding mu.
	actx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	if err := b.slots.Acquire(actx, b.key); err != nil {
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			return time.Time{}, fmt.Errorf("%w (%v)", ErrTimeBudget, b.timeout)
		}
		return time.Time{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.held { // a concurrent child of the same request got one first
		b.slots.Release(b.key)
	} else {
		b.held = true
	}
	return deadline, nil
}

// remaining returns the buffered-output allowance left.
func (b *Budget) remaining() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.outputLeft
}

// take reserves n bytes of buffered output; false means the allowance is
// exhausted (the reservation is not made).
func (b *Budget) take(n int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n > b.outputLeft {
		b.outputLeft = 0
		return false
	}
	b.outputLeft -= n
	return true
}

// limitedBuffer buffers stdout against a Budget's allowance (nil: no
// limit). On overflow it kills git at once (instead of letting it block on
// a full pipe) and refuses the write.
type limitedBuffer struct {
	buf      bytes.Buffer
	allow    *Budget
	kill     context.CancelFunc
	overflow bool
}

var errOutputOverflow = errors.New("output allowance exhausted")

func (l *limitedBuffer) Write(p []byte) (int, error) {
	if l.overflow || (l.allow != nil && !l.allow.take(int64(len(p)))) {
		l.overflow = true
		l.kill()
		return 0, errOutputOverflow
	}
	return l.buf.Write(p)
}

// stopWriter forwards git's streamed output; when the consumer returns
// ErrStop it records that and kills git at once.
type stopWriter struct {
	w       io.Writer
	kill    context.CancelFunc
	stopped atomic.Bool
}

func (s *stopWriter) Write(p []byte) (int, error) {
	n, err := s.w.Write(p)
	if errors.Is(err, ErrStop) {
		s.stopped.Store(true)
		s.kill()
	}
	return n, err
}

// tailBuffer keeps the last stderrKeepBytes written to it.
type tailBuffer struct{ b []byte }

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.b = append(t.b, p...)
	if over := len(t.b) - stderrKeepBytes; over > 0 {
		t.b = append(t.b[:0], t.b[over:]...)
	}
	return len(p), nil
}

func (t *tailBuffer) String() string { return strings.TrimSpace(string(t.b)) }

// Cmd is one read-side git child for Run.
type Cmd struct {
	Class Class
	Dir   string    // repository (or scratch) directory; required
	Env   []string  // nil inherits the server's environment (see childEnv)
	Stdin io.Reader // nil: no stdin
	// Stdout, when set, receives git's output as it is produced and is not
	// counted against the output allowance (streaming callers, such as a
	// `rev-list` walk read incrementally). Its Write may return ErrStop to
	// end git early. When nil, output is buffered, counted, and returned.
	Stdout io.Writer
	Args   []string
}

// localeCategories are glibc's locale categories other than LC_MESSAGES.
var localeCategories = []string{
	"LC_CTYPE", "LC_NUMERIC", "LC_TIME", "LC_COLLATE", "LC_MONETARY", "LC_PAPER",
	"LC_NAME", "LC_ADDRESS", "LC_TELEPHONE", "LC_MEASUREMENT", "LC_IDENTIFICATION",
}

// childEnv returns a git child's environment: env (nil: the server's) with
// git's messages in the untranslated C locale, because heapLimitFailure
// matches git's English diagnostics (#339). Only LC_MESSAGES changes:
// LC_CTYPE and LC_COLLATE drive git grep's regex matching (-i on non-ASCII
// text, "." over multibyte characters), so LC_ALL=C would change results.
// An inherited LC_ALL overrides every category, so it is replaced by the
// same value on each other category. gettext ignores LANGUAGE when
// LC_MESSAGES is C. exec.Cmd uses the last value of a repeated variable.
func childEnv(env []string) []string {
	if env == nil {
		env = os.Environ()
	}
	out := make([]string, 0, len(env)+len(localeCategories)+1)
	all := ""
	for _, kv := range env {
		if v, ok := strings.CutPrefix(kv, "LC_ALL="); ok {
			all = v // the last value is the effective one
			continue
		}
		out = append(out, kv)
	}
	if all != "" { // an empty LC_ALL is ignored, like an unset one
		for _, c := range localeCategories {
			out = append(out, c+"="+all)
		}
	}
	return append(out, "LC_MESSAGES=C")
}

// Run starts one read-side git child under the limits above and waits for
// it (git is always reaped before Run returns). Limit outcomes are
// returned as ErrBusy, ErrTimeBudget, ErrOutputLimit, or ErrMemoryLimit,
// and never wrap *exec.ExitError, so exit-status checks cannot mistake them
// for git's own "no match" or "not found" statuses. Other failures wrap
// git's *exec.ExitError.
func Run(ctx context.Context, c Cmd) (string, error) {
	assertf(c.Class == Light || c.Class == Heavy, "Run: class %d", c.Class)
	assertf(c.Dir != "", "Run: empty Dir")
	assertf(len(c.Args) > 0, "Run: no git args")
	name := CommandLine(c.Args)

	b := BudgetFrom(ctx)
	var heap int64 // no Budget (write endpoints): no limits at all
	limitCtx := ctx
	if b != nil {
		heap = b.limits.HeavyHeap
		if c.Class == Light {
			heap = b.limits.LightHeap
		}
		deadline, err := b.start(ctx)
		if err != nil {
			return "", fmt.Errorf("%s: %w", name, err)
		}
		var cancel context.CancelFunc
		limitCtx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	if !heapLimitAvailable() {
		// No qualified heap-cap mechanism outside Linux (ADR-049): the
		// other limits still apply.
		heap = 0
	}
	runCtx, kill := context.WithCancel(limitCtx)
	defer kill()

	cmd := heapLimitedGit(runCtx, heap, c.Args...)
	cmd.Dir = c.Dir
	cmd.Env = childEnv(c.Env)
	cmd.Stdin = c.Stdin
	cmd.WaitDelay = childWaitDelay
	var stderr tailBuffer
	cmd.Stderr = &stderr
	var out *limitedBuffer
	var stream *stopWriter
	if c.Stdout != nil {
		stream = &stopWriter{w: c.Stdout, kill: kill}
		cmd.Stdout = stream
	} else {
		out = &limitedBuffer{allow: b, kill: kill}
		cmd.Stdout = out
	}
	err := gitlimit.Run(cmd)
	stdout := ""
	if out != nil {
		stdout = out.buf.String()
	}
	if err == nil || (stream != nil && stream.stopped.Load()) {
		// A stopped stream: git was killed on the consumer's request.
		return stdout, nil
	}
	msg := stderr.String()
	switch {
	case out != nil && out.overflow:
		return stdout, fmt.Errorf("%s: %w", name, ErrOutputLimit)
	case ctx.Err() == nil && errors.Is(limitCtx.Err(), context.DeadlineExceeded):
		return stdout, fmt.Errorf("%s: %w", name, ErrTimeBudget)
	case heap > 0 && heapLimitFailure(err, msg):
		return stdout, fmt.Errorf("%s: %w (%d bytes): %s", name, ErrMemoryLimit, heap, msg)
	}
	return stdout, fmt.Errorf("%s: %w: %s", name, err, msg)
}

// CommandLine is the git command line that an error quotes. Past
// maxErrArgvBytes it keeps the head, cut at a rune boundary, and the size.
func CommandLine(args []string) string {
	name := "git " + strings.Join(args, " ")
	if len(name) <= maxErrArgvBytes {
		return name
	}
	cut := maxErrArgvBytes
	for cut > maxErrArgvBytes-utf8.UTFMax && !utf8.RuneStart(name[cut]) {
		cut--
	}
	return fmt.Sprintf("%s... (%d bytes)", name[:cut], len(name))
}
