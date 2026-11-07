package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// describe names each in-flight request at a shutdown drain that times
// out. A request line can be up to 1 MiB, so each line keeps at most
// 1024 bytes of the method and of the path.
func TestActiveRequestsDescribeBoundsTheRequestLine(t *testing.T) {
	a := newActiveRequests()
	release := make(chan struct{})
	started := make(chan struct{}, 2)
	h := a.wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		started <- struct{}{}
		<-release
	}))
	done := make(chan struct{}, 2)
	for _, r := range []*http.Request{
		httptest.NewRequest(http.MethodGet, "/"+strings.Repeat("p", 700<<10), nil),
		httptest.NewRequest(strings.Repeat("M", 200<<10), "/x", nil),
	} {
		go func() { h.ServeHTTP(httptest.NewRecorder(), r); done <- struct{}{} }()
	}
	<-started
	<-started
	lines := a.describe(time.Now())
	close(release)
	<-done
	<-done
	if len(lines) != 2 {
		t.Fatalf("describe listed %d requests, want 2", len(lines))
	}
	for _, l := range lines {
		if len(l) > 4<<10 || !strings.Contains(l, " bytes)") {
			t.Errorf("describe line is %d bytes, want the method and path cut to 1024 bytes each: %.80q", len(l), l)
		}
	}
}
