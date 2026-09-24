package api

import (
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/coder/openstorage/internal/gitread"
)

// Process-wide limits on read-side git work (#313). Every git:read route
// runs its handler under a gitread.Budget (withGitReadBudget); gitread.Run
// applies the per-child caps. docs/reference/http-api.md ("Resource limits
// on git reads") is the client-facing description and ADR-048 the design;
// keep them in step.
const (
	// gitReadSlots caps requests running Heavy git children at once in
	// this process. With gitread.HeavyHeapBytes per child, their git heap
	// is at most 2 GiB.
	gitReadSlots = 8
	// gitReadSlotWait is how long a request waits for a slot before it
	// is answered 503 + Retry-After.
	gitReadSlotWait = time.Second
	// gitReadTimeout is one request's git time budget, counted from its
	// first git child (repository hydration is not included) and shared
	// by all of them: a files/metadata page with hundreds of per-file
	// walks gets 30 s in total, not 30 s per walk.
	gitReadTimeout = 30 * time.Second
	// gitArchiveTimeout replaces gitReadTimeout for archives, which
	// alternate git children with writes at the client's pace. Stream
	// spools each blob and gives the slot back before writing it, so a
	// stalled client holds no slot.
	gitArchiveTimeout = 10 * time.Minute
)

// gitReadPolicy is the configuration withGitReadBudget builds each
// request's budget from; a struct so tests can swap in small limits.
type gitReadPolicy struct {
	slots          *gitread.Slots
	timeout        time.Duration
	archiveTimeout time.Duration
	outputBytes    int64
	limits         gitread.Limits
}

// gitReads is the process-wide policy shared by every server instance.
var gitReads = gitReadPolicy{
	slots:          gitread.NewSlots(gitReadSlots, gitReadSlotWait),
	timeout:        gitReadTimeout,
	archiveTimeout: gitArchiveTimeout,
	outputBytes:    gitread.DefaultOutputBytes,
	limits:         gitread.DefaultLimits(),
}

// withGitReadBudget runs h with a fresh request budget in its context. The
// budget's admission slot is given back as soon as the response starts
// (gitReadWriter), and in any case when h returns; gitread reaps every git
// child before its call returns, so no child outlives the request.
func withGitReadBudget(h http.HandlerFunc) http.HandlerFunc {
	return gitReadBudget(h, false)
}

// withGitArchiveBudget is withGitReadBudget with the archive time budget.
func withGitArchiveBudget(h http.HandlerFunc) http.HandlerFunc {
	return gitReadBudget(h, true)
}

func gitReadBudget(h http.HandlerFunc, archive bool) http.HandlerFunc {
	assertf(h != nil, "gitReadBudget: nil handler")
	return func(w http.ResponseWriter, r *http.Request) {
		p := gitReads
		timeout := p.timeout
		if archive {
			timeout = p.archiveTimeout
		}
		b := gitread.NewBudget(p.slots, timeout, p.outputBytes).WithLimits(p.limits)
		defer b.Close()
		h(&gitReadWriter{ResponseWriter: w, b: b}, r.WithContext(gitread.WithBudget(r.Context(), b)))
	}
}

// gitReadWriter releases the request's admission slot when the response
// starts, so a slow reader cannot hold a slot while no git child runs.
type gitReadWriter struct {
	http.ResponseWriter
	b    *gitread.Budget
	once sync.Once
}

func (w *gitReadWriter) started() { w.once.Do(w.b.Release) }

func (w *gitReadWriter) WriteHeader(status int) {
	w.started()
	w.ResponseWriter.WriteHeader(status)
}

func (w *gitReadWriter) Write(p []byte) (int, error) {
	w.started()
	return w.ResponseWriter.Write(p)
}

// Unwrap exposes the wrapped writer to http.ResponseController.
func (w *gitReadWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// writeGitLimitErr answers a read-side git resource-limit error and
// reports whether err was one. Capacity is transient (503 + Retry-After);
// the time budget is 503 without Retry-After, because a page that needs
// more git time fails the same way on retry; memory, output and blob-size
// limits are properties of the requested data (422).
func (s *server) writeGitLimitErr(w http.ResponseWriter, r *http.Request, op string, err error) bool {
	var status int
	var msg string
	switch {
	case errors.Is(err, gitread.ErrBusy):
		w.Header().Set("Retry-After", "1")
		status, msg = http.StatusServiceUnavailable, "server is busy running git reads; retry shortly"
	case errors.Is(err, gitread.ErrTimeBudget):
		status, msg = http.StatusServiceUnavailable, "request exceeded the server's git time budget"
	case errors.Is(err, gitread.ErrMemoryLimit):
		status, msg = http.StatusUnprocessableEntity, "history or object too large for the server's git memory limit"
	case errors.Is(err, gitread.ErrOutputLimit):
		status, msg = http.StatusUnprocessableEntity, "result too large for the server's git output limit; narrow the request"
	case errors.Is(err, gitread.ErrBlobTooLarge):
		limit := gitread.MaxBlobBytes
		if b := gitread.BudgetFrom(r.Context()); b != nil {
			limit = b.Limits().MaxBlob
		}
		status, msg = http.StatusUnprocessableEntity, fmt.Sprintf("file exceeds the API read size limit of %d bytes; fetch it with git", limit)
	default:
		return false
	}
	s.logger.Warn("git read limit", "op", op, "method", r.Method, "path", r.URL.Path, "status", status, "err", err)
	writeErr(w, status, msg)
	return true
}
