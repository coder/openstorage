package githttp

import (
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
)

// TestServeAnswers404ToLongRedirects: http.ServeMux redirects a path that
// is not clean, and /api, to a Location that repeats the path and the
// query, and a GET also gets an HTML body that repeats them with each & as
// &amp;. A path of 900,000 `&` got an answer of 5.4 MB. A Location of more
// than 16384 bytes gets the 404 of the surface it leads to instead.
func TestServeAnswers404ToLongRedirects(t *testing.T) {
	amp := strings.Repeat("&", 900_000)
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for _, tc := range []struct {
		name, method, target string
		status               int
		location             string // whole Location, or its length for a long one
		contentType, body    string // body: a substring
		maxBody              int    // bytes
	}{
		{"api path", http.MethodGet, "/api/v1/repos//" + amp, 404, "", "application/problem+json", `"code":"route_not_found"`, 120_000},
		{"api query", http.MethodPost, "/api?" + amp, 404, "", "application/problem+json", `"instance":"/api"`, 300},
		{"git path", http.MethodGet, "//" + amp, 404, "", "application/json", `{"error":"not a smart-http path"}`, 100},
		{"at the bound", http.MethodGet, "/api/v1//" + strings.Repeat("a", 16376), 307, "16384", "text/html; charset=utf-8", "Temporary Redirect", 16500},
		{"past the bound", http.MethodGet, "/api/v1//" + strings.Repeat("a", 16377), 404, "", "application/problem+json", `"code":"route_not_found"`, 16700},
		{"short", http.MethodGet, "/x/..//api/v1/./repos?a=1", 307, "/api/v1/repos?a=1", "text/html; charset=utf-8", `<a href="/api/v1/repos?a=1">Temporary Redirect</a>.` + "\n\n", 53}, // the whole body
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(tc.method, "http://"+nodeA.addr+tc.target, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			loc := resp.Header.Get("Location")
			if len(loc) > 100 {
				loc = strconv.Itoa(len(loc))
			}
			if resp.StatusCode != tc.status || loc != tc.location || resp.Header.Get("Content-Type") != tc.contentType {
				t.Fatalf("got %d, Location %q, Content-Type %q; want %d, %q, %q", resp.StatusCode, loc, resp.Header.Get("Content-Type"), tc.status, tc.location, tc.contentType)
			}
			if len(body) > tc.maxBody || !strings.Contains(string(body), tc.body) {
				t.Fatalf("body of %d bytes (at most %d) lacks %q: %.200s", len(body), tc.maxBody, tc.body, body)
			}
		})
	}
}
