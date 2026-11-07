package api

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/coder/openstorage/internal/gitlimit"
	"github.com/coder/openstorage/internal/gitread"
	"github.com/coder/openstorage/internal/meta"
)

// Process-wide limits on read-side git work (#313). Every git:read route
// runs its handler under a gitread.Budget (withGitReadBudget,
// withGitStreamBudget); gitread.Run applies the per-child caps.
// docs/reference/http-api.md ("Resource limits on git reads") is the
// client-facing description and ADR-049 the design; keep them in step.
//
// A request takes its admission slot at its first git child and keeps it
// until its response is delivered: the slot bounds everything the request
// retains (git children, buffered output, parsed results, spools). Write
// deadlines bound how long a slow reader can keep it.
const (
	// gitStreamSlots is a separate pool for file and archive downloads,
	// whose delivery can take minutes, so they cannot starve JSON reads.
	gitStreamSlots = 8
	// gitRepoSlots is how many slots of each pool one repository may hold,
	// so slow readers of one repository cannot take a whole pool from the
	// others.
	gitRepoSlots = gitlimit.RepoSlotCount
	// gitReadSlotWait is how long a request waits for a slot before it
	// is answered 503 + Retry-After.
	gitReadSlotWait = gitlimit.ReadSlotWait
	// gitReadTimeout is one request's git time budget, counted from its
	// first git child (repository hydration is not included) and shared
	// by all of them: a files/metadata page with hundreds of per-file
	// walks gets 30 s in total, not 30 s per walk.
	gitReadTimeout = 30 * time.Second
	// gitArchiveTimeout replaces gitReadTimeout for archives, which
	// alternate git children with writes at the client's pace.
	gitArchiveTimeout = 10 * time.Minute
	// gitWriteIdle is how long one chunk (gitWriteChunk) of a git:read
	// response may take to reach the client before the connection is
	// closed.
	gitWriteIdle  = 30 * time.Second
	gitWriteChunk = 64 << 10
	// gitDeliveryTimeout and gitStreamDeliveryTimeout are absolute limits
	// on delivering a response, counted from its first byte, so a client
	// that reads a trickle cannot keep a slot and its payload forever.
	gitDeliveryTimeout       = 2 * time.Minute
	gitStreamDeliveryTimeout = 10 * time.Minute
)

// gitReadPolicy is the configuration the budget middleware builds each
// request's budget from; a struct so tests can swap in small limits.
type gitReadPolicy struct {
	slots          *gitread.Slots
	streamSlots    *gitread.Slots
	timeout        time.Duration
	archiveTimeout time.Duration
	outputBytes    int64
	limits         gitread.Limits
	writeIdle      time.Duration
	delivery       time.Duration
	streamDelivery time.Duration
}

// gitReads is the process-wide policy shared by every server instance.
var gitReads = gitReadPolicy{
	// gitlimit.ReadSlots (gitlimit.ReadSlotCount = 8, gitRepoSlots per
	// repository) caps requests with JSON responses that run git at once
	// in this process. It is shared with cache maintenance, which may hold
	// at most gitlimit.MaintenanceShare of its slots (#322). Their git
	// heap is at most 8 x 256 MiB while a child runs; buffered output adds
	// at most 64 MiB (plus parsed copies) each. Downloads (streamSlots)
	// add the same heap bound again.
	slots:          gitlimit.ReadSlots,
	streamSlots:    gitread.NewSlots(gitStreamSlots, gitRepoSlots, gitReadSlotWait),
	timeout:        gitReadTimeout,
	archiveTimeout: gitArchiveTimeout,
	outputBytes:    gitread.DefaultOutputBytes,
	limits:         gitread.DefaultLimits(),
	writeIdle:      gitWriteIdle,
	delivery:       gitDeliveryTimeout,
	streamDelivery: gitStreamDeliveryTimeout,
}

// gitRouteKind selects a route's pool and time limits.
type gitRouteKind int

const (
	gitRouteJSON    gitRouteKind = iota // JSON responses
	gitRouteFile                        // GET/HEAD file: blob staged to disk, then streamed
	gitRouteArchive                     // archive: git children interleaved with streaming
)

// withGitReadBudget runs a JSON-response handler under a request budget.
func (s *server) withGitReadBudget(h http.HandlerFunc) http.HandlerFunc {
	return s.gitReadBudget(h, gitRouteJSON)
}

// withGitFileBudget runs the file download handler under a request budget
// from the stream pool.
func (s *server) withGitFileBudget(h http.HandlerFunc) http.HandlerFunc {
	return s.gitReadBudget(h, gitRouteFile)
}

// withGitArchiveBudget runs the archive handler under a request budget
// from the stream pool, with the archive time budget.
func (s *server) withGitArchiveBudget(h http.HandlerFunc) http.HandlerFunc {
	return s.gitReadBudget(h, gitRouteArchive)
}

// gitRepoSlotKey is the Slots key of the repository a request reads: its
// canonical RepoID, so every claim spelling (URL or ID) of one repository
// shares one quota. If the claim does not resolve, the handler answers
// without running git; the raw claim is then a safe key.
func (s *server) gitRepoSlotKey(r *http.Request) string {
	// Every git:read route is repo-scoped (requireAuth has verified the
	// claims).
	claims := mustClaims(r)
	assertf(claims.Repo != "", "gitRepoSlotKey: route without a repo claim")
	repo, err := meta.ResolveRepoKey(r.Context(), s.store, claims.Iss, claims.Repo)
	if err != nil {
		return "claim\x00" + claims.Iss + "\x00" + claims.Repo
	}
	assertf(repo.RepoID != "", "store returned repo with empty RepoID")
	return gitlimit.RepoKey(repo.RepoID)
}

func (s *server) gitReadBudget(h http.HandlerFunc, kind gitRouteKind) http.HandlerFunc {
	assertf(h != nil, "gitReadBudget: nil handler")
	return func(w http.ResponseWriter, r *http.Request) {
		p := gitReads
		slots, timeout, delivery := p.slots, p.timeout, p.delivery
		switch kind {
		case gitRouteFile:
			slots, delivery = p.streamSlots, p.streamDelivery
		case gitRouteArchive:
			slots, timeout, delivery = p.streamSlots, p.archiveTimeout, p.streamDelivery
		}
		b := gitread.NewBudget(slots, s.gitRepoSlotKey(r), timeout, p.outputBytes).WithLimits(p.limits)
		gw := &gitReadWriter{ResponseWriter: w, rc: http.NewResponseController(w), idle: p.writeIdle, delivery: delivery}
		// LIFO: the response is flushed under its deadlines, then the slot
		// is released, also when the handler aborts with a panic.
		defer b.Close()
		defer gw.finish()
		h(gw, r.WithContext(gitread.WithBudget(r.Context(), b)))
	}
}

// gitReadWriter puts write deadlines on a git:read response: each chunk of
// gitWriteChunk bytes must reach the client within the idle limit, and the
// whole response within the delivery limit counted from its first byte. A
// stalled or trickling client therefore gets its connection closed, the
// handler returns, and the request's slot and payload are released.
type gitReadWriter struct {
	http.ResponseWriter
	rc             *http.ResponseController
	idle, delivery time.Duration
	expiry         time.Time // zero until the response starts
}

// arm sets the write deadline for the next chunk. Writers without deadline
// support (tests' recorders) get none; there is no connection to hold.
func (w *gitReadWriter) arm() {
	now := time.Now()
	if w.expiry.IsZero() {
		w.expiry = now.Add(w.delivery)
	}
	deadline := now.Add(w.idle)
	if deadline.After(w.expiry) {
		deadline = w.expiry
	}
	_ = w.rc.SetWriteDeadline(deadline)
}

func (w *gitReadWriter) WriteHeader(status int) {
	w.arm()
	w.ResponseWriter.WriteHeader(status)
}

func (w *gitReadWriter) Write(p []byte) (int, error) {
	n := 0
	for len(p) > 0 {
		chunk := p[:min(len(p), gitWriteChunk)]
		w.arm()
		m, err := w.ResponseWriter.Write(chunk)
		n += m
		if err != nil {
			return n, err
		}
		p = p[len(chunk):]
	}
	return n, nil
}

// finish flushes a started response under its deadlines, then clears the
// deadline so a later request on the same keep-alive connection starts
// without it.
func (w *gitReadWriter) finish() {
	if w.expiry.IsZero() {
		return
	}
	w.arm()
	_ = w.rc.Flush()
	_ = w.rc.SetWriteDeadline(time.Time{})
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
	case errors.Is(err, gitread.ErrSymlinkTooLong):
		status, msg = http.StatusUnprocessableEntity, fmt.Sprintf("a symlink target exceeds the API read size limit of %d bytes; fetch it with git", gitread.MaxSymlinkTarget)
	default:
		return false
	}
	s.logger.Warn("git read limit", "op", op, "method", r.Method, "path", r.URL.Path, "status", status, "err", err)
	writeErr(w, status, msg)
	return true
}
