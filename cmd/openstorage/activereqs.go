package main

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"
)

// activeRequests tracks in-flight HTTP requests, so a shutdown drain that
// times out can name the handlers still running (issue #440).
type activeRequests struct {
	mu   sync.Mutex
	next uint64
	reqs map[uint64]activeRequest
}

type activeRequest struct {
	method, path string
	start        time.Time
}

func newActiveRequests() *activeRequests {
	return &activeRequests{reqs: make(map[uint64]activeRequest)}
}

// wrap tracks each request for as long as h runs. It records the path
// only: a query string can carry request parameters the log has no need for.
func (a *activeRequests) wrap(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a.mu.Lock()
		id := a.next
		a.next++
		a.reqs[id] = activeRequest{method: r.Method, path: r.URL.Path, start: time.Now()}
		a.mu.Unlock()
		defer func() {
			a.mu.Lock()
			delete(a.reqs, id)
			a.mu.Unlock()
		}()
		h.ServeHTTP(w, r)
	})
}

// describe returns one line per in-flight request, oldest first, with the
// path quoted (it can hold any byte, newlines included).
func (a *activeRequests) describe(now time.Time) []string {
	a.mu.Lock()
	reqs := make([]activeRequest, 0, len(a.reqs))
	for _, r := range a.reqs {
		reqs = append(reqs, r)
	}
	a.mu.Unlock()
	sort.Slice(reqs, func(i, j int) bool { return reqs[i].start.Before(reqs[j].start) })
	lines := make([]string, len(reqs))
	for i, r := range reqs {
		lines[i] = fmt.Sprintf("%s %q running for %v", r.method, r.path, now.Sub(r.start).Round(time.Millisecond))
	}
	return lines
}
