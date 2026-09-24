package metrics

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"
)

// scrape serves the metrics handler once and returns the exposition text.
func scrape(t *testing.T, h http.Handler) string {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics = %d, want 200: %s", rec.Code, rec.Body)
	}
	return rec.Body.String()
}

// seriesLabels extracts every label value per metric family from an
// exposition, so tests can assert the label sets stay fixed.
func seriesLabels(t *testing.T, text string) map[string]map[string]bool {
	t.Helper()
	out := map[string]map[string]bool{}
	pair := regexp.MustCompile(`(\w+)="([^"]*)"`)
	sc := bufio.NewScanner(strings.NewReader(text))
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "#") || !strings.HasPrefix(line, "openstorage_") {
			continue
		}
		name, _, _ := strings.Cut(line, "{")
		name, _, _ = strings.Cut(name, " ")
		if out[name] == nil {
			out[name] = map[string]bool{}
		}
		for _, m := range pair.FindAllStringSubmatch(line, -1) {
			if m[1] == "le" {
				continue // histogram bucket bounds are fixed by the code
			}
			out[name][m[1]+"="+m[2]] = true
		}
	}
	return out
}

func TestExpositionHasEveryFamilyWithInitializedSeries(t *testing.T) {
	text := scrape(t, Handler())
	for _, want := range []string{
		`openstorage_http_requests_total{code_class="2xx",surface="api"} `,
		`openstorage_http_requests_total{code_class="5xx",surface="git"} `,
		`openstorage_http_request_duration_seconds_count{surface="health"} `,
		`openstorage_s3wal_cas_contention_exhausted_total{scope="refs"} `,
		`openstorage_s3wal_cas_contention_exhausted_total{scope="scan_state"} `,
		`openstorage_s3wal_cas_contention_exhausted_total{scope="meta"} `,
		`openstorage_s3wal_lease_renewal_failures_total `,
		`openstorage_s3wal_lease_held `,
		`openstorage_s3wal_manifest_size_guard_total{result="warn"} `,
		`openstorage_s3wal_manifest_size_guard_total{result="refuse"} `,
		`openstorage_job_runs_total{job="orphan_sweep",result="success"} `,
		`openstorage_job_runs_total{job="repack",result="noop"} `,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("exposition missing series %q", want)
		}
	}
	// Only our families: the private registry carries no Go runtime,
	// process, or third-party collectors.
	for line := range strings.SplitSeq(text, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.HasPrefix(line, "openstorage_") {
			t.Errorf("foreign series on the private registry: %q", line)
		}
	}
}

// TestLabelValuesAreFixed drives request data that would leak into labels
// if any label were request-derived (paths, repo names, tokens, methods,
// hosts) and asserts that every exposed label value is from the fixed set.
func TestLabelValuesAreFixed(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("s") {
		case "418":
			w.WriteHeader(http.StatusTeapot)
		case "503":
			w.WriteHeader(http.StatusServiceUnavailable)
		case "early":
			w.WriteHeader(http.StatusEarlyHints) // informational, then final
			w.WriteHeader(http.StatusCreated)
		default:
			_, _ = io.WriteString(w, "ok") // implicit 200
		}
	})
	for _, s := range []Surface{SurfaceAPI, SurfaceGit, SurfaceHealth} {
		h := InstrumentHTTP(s, inner)
		for _, target := range []string{
			"/secret-repo-name.git/info/refs?service=git-upload-pack&s=418",
			"/api/v1/repos/tenant-org/x?s=503",
			"/?s=early",
			"/weird/%2e%2e/path",
		} {
			req := httptest.NewRequest("PROPFIND", target, nil)
			req.Host = "tenant.example.com"
			req.Header.Set("Authorization", "Bearer eyJhbGciOi.token.value")
			h.ServeHTTP(httptest.NewRecorder(), req)
		}
	}
	allowed := map[string]bool{
		"surface=api": true, "surface=git": true, "surface=health": true,
		"code_class=1xx": true, "code_class=2xx": true, "code_class=3xx": true, "code_class=4xx": true, "code_class=5xx": true,
		"scope=refs": true, "scope=scan_state": true, "scope=meta": true,
		"result=warn": true, "result=refuse": true,
		"job=orphan_sweep": true, "job=repack": true,
		"result=success": true, "result=noop": true, "result=error": true,
	}
	for name, labels := range seriesLabels(t, scrape(t, Handler())) {
		for l := range labels {
			if !allowed[l] {
				t.Errorf("%s exposes non-fixed label %q", name, l)
			}
		}
	}
}

func TestInstrumentHTTPCountsFinalStatusClass(t *testing.T) {
	cases := []struct {
		name  string
		write func(w http.ResponseWriter)
		class string
	}{
		{"implicit 200 with body", func(w http.ResponseWriter) { _, _ = io.WriteString(w, "x") }, "2xx"},
		{"nothing written", func(http.ResponseWriter) {}, "2xx"},
		{"404", func(w http.ResponseWriter) { w.WriteHeader(http.StatusNotFound) }, "4xx"},
		{"early hints then 201", func(w http.ResponseWriter) {
			w.WriteHeader(http.StatusEarlyHints)
			w.WriteHeader(http.StatusCreated)
		}, "2xx"},
		{"500", func(w http.ResponseWriter) { w.WriteHeader(http.StatusInternalServerError) }, "5xx"},
		{"302", func(w http.ResponseWriter) { w.WriteHeader(http.StatusFound) }, "3xx"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := Value("openstorage_http_requests_total", map[string]string{"surface": "api", "code_class": tc.class})
			durBefore := Value("openstorage_http_request_duration_seconds_count", map[string]string{"surface": "api"})
			InstrumentHTTP(SurfaceAPI, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { tc.write(w) })).
				ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/v1/x", nil))
			if got := Value("openstorage_http_requests_total", map[string]string{"surface": "api", "code_class": tc.class}) - before; got != 1 {
				t.Errorf("%s requests delta = %v, want 1", tc.class, got)
			}
			if got := Value("openstorage_http_request_duration_seconds_count", map[string]string{"surface": "api"}) - durBefore; got != 1 {
				t.Errorf("duration observations delta = %v, want 1", got)
			}
		})
	}
}

// TestInstrumentHTTPPreservesResponseController pins the Unwrap contract:
// git smart HTTP sets read deadlines and flushes through
// http.ResponseController, which must reach net/http's writer.
func TestInstrumentHTTPPreservesResponseController(t *testing.T) {
	errs := make(chan error, 2)
	h := InstrumentHTTP(SurfaceGit, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		rc := http.NewResponseController(w)
		errs <- rc.SetReadDeadline(time.Now().Add(time.Minute))
		_, _ = io.WriteString(w, "chunk")
		errs <- rc.Flush()
	}))
	srv := httptest.NewServer(h)
	defer srv.Close()
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("ResponseController through the instrumented writer: %v", err)
		}
	}
}

func TestSignalHelpersIncrement(t *testing.T) {
	type probe struct {
		name   string
		labels map[string]string
		fire   func()
	}
	for _, p := range []probe{
		{"openstorage_s3wal_cas_contention_exhausted_total", map[string]string{"scope": "meta"}, func() { S3WALContentionExhausted(ScopeMeta) }},
		{"openstorage_s3wal_lease_renewal_failures_total", nil, S3WALLeaseRenewalFailed},
		{"openstorage_s3wal_manifest_size_guard_total", map[string]string{"result": "refuse"}, func() { S3WALManifestSizeGuard(SizeGuardRefuse) }},
		{"openstorage_job_runs_total", map[string]string{"job": "repack", "result": "error"}, func() { JobRun(JobRepack, ResultError) }},
	} {
		before := Value(p.name, p.labels)
		p.fire()
		if got := Value(p.name, p.labels) - before; got != 1 {
			t.Errorf("%s%v delta = %v, want 1", p.name, p.labels, got)
		}
	}
	SetS3WALLeaseHeld(true)
	if got := Value("openstorage_s3wal_lease_held", nil); got != 1 {
		t.Errorf("lease_held after acquire = %v, want 1", got)
	}
	SetS3WALLeaseHeld(false)
	if got := Value("openstorage_s3wal_lease_held", nil); got != 0 {
		t.Errorf("lease_held after release = %v, want 0", got)
	}
}

// TestServerServesOnlyMetrics pins the metrics listener's route table:
// /metrics answers, every other path (including the pprof and expvar
// debug routes) is 404, and other methods on /metrics are refused.
func TestServerServesOnlyMetrics(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer()
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	base := "http://" + ln.Addr().String()
	for path, want := range map[string]int{
		"/metrics":      http.StatusOK,
		"/":             http.StatusNotFound,
		"/debug/vars":   http.StatusNotFound,
		"/debug/pprof/": http.StatusNotFound,
		"/healthz":      http.StatusNotFound,
		"/api/v1/repos": http.StatusNotFound,
	} {
		resp, err := http.Get(base + path)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("GET %s = %d, want %d", path, resp.StatusCode, want)
		}
	}
	resp, err := http.Post(base+"/metrics", "text/plain", strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("POST /metrics = %d, want 405", resp.StatusCode)
	}
	if srv.ReadHeaderTimeout <= 0 || srv.ReadTimeout <= 0 || srv.WriteTimeout <= 0 || srv.IdleTimeout <= 0 || srv.MaxHeaderBytes <= 0 {
		t.Errorf("metrics server lacks fixed bounds: %+v", srv)
	}
}

func TestIsUnspecifiedBind(t *testing.T) {
	for addr, want := range map[string]bool{
		":9090": true, "0.0.0.0:9090": true, "[::]:9090": true,
		"127.0.0.1:9090": false, "[::1]:9090": false, "10.0.0.5:9090": false, "localhost:9090": false,
	} {
		if got := IsUnspecifiedBind(addr); got != want {
			t.Errorf("IsUnspecifiedBind(%q) = %v, want %v", addr, got, want)
		}
	}
}

// TestInstrumentMuxCountsMuxGeneratedResponses pins the Codex P2 on
// PR #327: http.ServeMux answers some requests itself (the /api → /api/
// redirect, path-cleaning redirects), so per-registration wrappers never
// saw them. Wrapping the whole mux counts every response exactly once,
// under the surface the request resolves to.
func TestInstrumentMuxCountsMuxGeneratedResponses(t *testing.T) {
	mux := http.NewServeMux()
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") })
	mux.Handle("/api/", ok)
	mux.Handle("/healthz", ok)
	mux.Handle("/", ok)
	classify := func(pattern string) Surface {
		switch {
		case strings.HasPrefix(pattern, "/api/"):
			return SurfaceAPI
		case pattern == "/healthz":
			return SurfaceHealth
		default:
			return SurfaceGit
		}
	}
	h := InstrumentMux(mux, classify)
	for _, tc := range []struct {
		target, surface, class string
	}{
		{"/api", "api", "3xx"},               // mux-generated redirect to /api/
		{"/x/../api/v1/repos", "api", "3xx"}, // path-cleaning redirect
		{"//healthz", "health", "3xx"},
		{"/api/v1/repos", "api", "2xx"},
		{"/healthz", "health", "2xx"},
		{"/repo.git/info/refs", "git", "2xx"},
	} {
		t.Run(tc.target, func(t *testing.T) {
			totals := func() float64 { return Value("openstorage_http_requests_total", nil) }
			one := map[string]string{"surface": tc.surface, "code_class": tc.class}
			beforeAll, beforeOne := totals(), Value("openstorage_http_requests_total", one)
			req := httptest.NewRequest(http.MethodGet, "http://example.test"+tc.target, nil)
			req.URL.Path = tc.target // keep the uncleaned path, as a raw client sends it
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if got := Value("openstorage_http_requests_total", one) - beforeOne; got != 1 {
				t.Errorf("%s: %s/%s delta = %v (status %d), want 1", tc.target, tc.surface, tc.class, got, rec.Code)
			}
			if got := totals() - beforeAll; got != 1 {
				t.Errorf("%s: total delta = %v, want exactly 1 (no double counting)", tc.target, got)
			}
		})
	}
}
