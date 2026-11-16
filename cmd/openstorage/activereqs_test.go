package main

import (
	"strings"
	"testing"
	"time"
)

// The shutdown drain names each request that is still running. A client
// chooses the method and the path of a request, and holds the request open
// with a declared body length, so the line must not carry them whole.
func TestActiveRequestsDescribeBoundsTheClientsValues(t *testing.T) {
	a := newActiveRequests()
	now := time.Now()
	a.reqs[1] = activeRequest{method: "GET", path: "/" + strings.Repeat("p", 100_000), start: now.Add(-time.Second)}
	a.reqs[2] = activeRequest{method: strings.Repeat("M", 100_000), path: "/x", start: now.Add(-2 * time.Second)}
	a.reqs[3] = activeRequest{method: "POST", path: "/a\nb", start: now}

	lines := a.describe(now)
	if len(lines) != 3 {
		t.Fatalf("%d lines, want 3: %.200q", len(lines), lines)
	}
	for i, line := range lines {
		if len(line) > 1024 {
			t.Errorf("line %d is %d bytes, want it bounded: %.200s", i, len(line), line)
		}
	}
	if want := "...(100000 bytes) \"/x\" running for 2s"; !strings.HasSuffix(lines[0], want) {
		t.Errorf("oldest line %.300q does not end with %q", lines[0], want)
	}
	if want := "...(100001 bytes)\" running for 1s"; !strings.HasSuffix(lines[1], want) || !strings.HasPrefix(lines[1], "GET \"/ppp") {
		t.Errorf("second line %.300q does not start with GET \"/ppp and end with %q", lines[1], want)
	}
	if want := `POST "/a\nb" running for 0s`; lines[2] != want {
		t.Errorf("short request: line %q, want %q", lines[2], want)
	}
}
