// Package metrics owns the opt-in Prometheus exposition (ADR-47, issue
// #325). Collectors live on a private registry, never the global default
// one, and every label value is a constant chosen here: callers pass
// typed enums, so no request data can reach a label. Incrementing is
// cheap and harmless when no metrics listener runs (hook subprocesses,
// or serve without OPENSTORAGE_METRICS_LISTEN).
package metrics

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Surface is the fixed HTTP surface label, chosen per mux registration.
type Surface int

const (
	SurfaceAPI Surface = iota
	SurfaceGit
	SurfaceHealth
)

var surfaceNames = [...]string{SurfaceAPI: "api", SurfaceGit: "git", SurfaceHealth: "health"}

// ContentionScope names the S3WAL mutation loop that exhausted its CAS
// attempts.
type ContentionScope int

const (
	ScopeRefs ContentionScope = iota
	ScopeScanState
	ScopeMeta
)

var scopeNames = [...]string{ScopeRefs: "refs", ScopeScanState: "scan_state", ScopeMeta: "meta"}

// SizeGuardResult is the manifest size-guard outcome.
type SizeGuardResult int

const (
	SizeGuardWarn SizeGuardResult = iota
	SizeGuardRefuse
)

var sizeGuardNames = [...]string{SizeGuardWarn: "warn", SizeGuardRefuse: "refuse"}

// Job names a background job whose executions are counted.
type Job int

const (
	JobOrphanSweep Job = iota
	JobRepack
)

var jobNames = [...]string{JobOrphanSweep: "orphan_sweep", JobRepack: "repack"}

// Result is a job execution outcome.
type Result int

const (
	ResultSuccess Result = iota
	ResultNoop
	ResultError
)

var resultNames = [...]string{ResultSuccess: "success", ResultNoop: "noop", ResultError: "error"}

var codeClasses = [...]string{"1xx", "2xx", "3xx", "4xx", "5xx"}

var (
	registry = prometheus.NewRegistry()

	httpRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "openstorage_http_requests_total",
		Help: "HTTP requests served on the API/Git listener, by fixed surface and final status class.",
	}, []string{"surface", "code_class"})
	httpDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "openstorage_http_request_duration_seconds",
		Help:    "Wall time of HTTP requests on the API/Git listener, by fixed surface (Git includes streamed pack transfer).",
		Buckets: prometheus.DefBuckets,
	}, []string{"surface"})
	s3walContention = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "openstorage_s3wal_cas_contention_exhausted_total",
		Help: "S3WAL mutations in this process that returned conditional-write contention retries exhausted (Git hook subprocesses are not counted).",
	}, []string{"scope"})
	s3walLeaseRenewalFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "openstorage_s3wal_lease_renewal_failures_total",
		Help: "S3WAL instance-lease renewal write attempts that returned an error, including ambiguous writes later resolved as landed; not a count of lost leases.",
	})
	s3walLeaseHeld = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "openstorage_s3wal_lease_held",
		Help: "1 while this process holds the S3WAL instance lease by its own account; local state, not proof of bucket ownership.",
	})
	s3walSizeGuard = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "openstorage_s3wal_manifest_size_guard_total",
		Help: "S3WAL manifest size-guard events in this process: warn = once-per-repository warning logged, refuse = final refusal of a growing mutation.",
	}, []string{"result"})
	jobRuns = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "openstorage_job_runs_total",
		Help: "Executions of background job operations in this process, by fixed job and outcome.",
	}, []string{"job", "result"})
)

func init() {
	registry.MustRegister(httpRequests, httpDuration, s3walContention, s3walLeaseRenewalFailures,
		s3walLeaseHeld, s3walSizeGuard, jobRuns)
	// Initialize every finite series so a clean process exposes zeros
	// instead of missing series.
	for _, s := range surfaceNames {
		for _, c := range codeClasses {
			httpRequests.WithLabelValues(s, c)
		}
		httpDuration.WithLabelValues(s)
	}
	for _, s := range scopeNames {
		s3walContention.WithLabelValues(s)
	}
	for _, r := range sizeGuardNames {
		s3walSizeGuard.WithLabelValues(r)
	}
	jobRuns.WithLabelValues(jobNames[JobOrphanSweep], resultNames[ResultSuccess])
	jobRuns.WithLabelValues(jobNames[JobOrphanSweep], resultNames[ResultError])
	for _, r := range resultNames {
		jobRuns.WithLabelValues(jobNames[JobRepack], r)
	}
}

// name looks up a label value for enum v, crashing on an out-of-range
// value: an unknown enum is a programming error, never request input.
func name[E ~int](table []string, v E) string {
	assertf(int(v) >= 0 && int(v) < len(table) && table[v] != "", "metrics: label enum %d out of range", int(v))
	return table[v]
}

// S3WALContentionExhausted counts one S3WAL mutation that returned
// ErrCASContention to its caller.
func S3WALContentionExhausted(scope ContentionScope) {
	s3walContention.WithLabelValues(name(scopeNames[:], scope)).Inc()
}

// S3WALLeaseRenewalFailed counts one lease renewal write attempt that
// returned an error.
func S3WALLeaseRenewalFailed() { s3walLeaseRenewalFailures.Inc() }

// SetS3WALLeaseHeld records whether this process holds the instance lease.
func SetS3WALLeaseHeld(held bool) {
	if held {
		s3walLeaseHeld.Set(1)
		return
	}
	s3walLeaseHeld.Set(0)
}

// S3WALManifestSizeGuard counts one manifest size-guard event.
func S3WALManifestSizeGuard(r SizeGuardResult) {
	s3walSizeGuard.WithLabelValues(name(sizeGuardNames[:], r)).Inc()
}

// JobRun counts one background job execution outcome.
func JobRun(j Job, r Result) {
	assertf(!(j == JobOrphanSweep && r == ResultNoop), "metrics: orphan sweep has no noop outcome")
	jobRuns.WithLabelValues(name(jobNames[:], j), name(resultNames[:], r)).Inc()
}

// statusWriter records the final status class. Informational 1xx headers
// do not replace the final status; a handler that writes nothing or only
// a body produces net/http's implicit 200.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 && code >= 200 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

// Unwrap lets http.ResponseController reach net/http's writer (deadlines,
// Flush). Git smart HTTP depends on it mid-stream.
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func codeClass(status int) string {
	switch {
	case status == 0:
		return "2xx" // nothing written: net/http sends 200
	case status < 200:
		return "1xx"
	case status < 300:
		return "2xx"
	case status < 400:
		return "3xx"
	case status < 500:
		return "4xx"
	default:
		return "5xx"
	}
}

// InstrumentHTTP counts requests and their duration under the fixed
// surface label. Nothing from the request (path, host, headers, method)
// becomes a label value.
func InstrumentHTTP(s Surface, next http.Handler) http.Handler {
	assertf(next != nil, "metrics.InstrumentHTTP: nil handler")
	label := name(surfaceNames[:], s)
	requests := [len(codeClasses)]prometheus.Counter{}
	for i, c := range codeClasses {
		requests[i] = httpRequests.WithLabelValues(label, c)
	}
	duration := httpDuration.WithLabelValues(label)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w}
		start := time.Now()
		defer func() {
			duration.Observe(time.Since(start).Seconds())
			class := codeClass(sw.status)
			for i, c := range codeClasses {
				if c == class {
					requests[i].Inc()
				}
			}
		}()
		next.ServeHTTP(sw, r)
	})
}

// InstrumentMux instruments a whole ServeMux, so responses the mux
// generates itself (the /api → /api/ redirect, path-cleaning redirects)
// are counted too. The surface comes from classify applied to the pattern
// mux.Handler reports: the matched registration, or for a mux redirect the
// path the redirect leads to. classify must map every input to one of the
// fixed surfaces; the pattern itself never becomes a label.
func InstrumentMux(mux *http.ServeMux, classify func(pattern string) Surface) http.Handler {
	assertf(mux != nil && classify != nil, "metrics.InstrumentMux: nil mux or classifier")
	bySurface := make([]http.Handler, len(surfaceNames))
	for s := range surfaceNames {
		bySurface[s] = InstrumentHTTP(Surface(s), mux)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, pattern := mux.Handler(r)
		s := classify(pattern)
		name(surfaceNames[:], s) // crash on an out-of-range classifier result
		bySurface[s].ServeHTTP(w, r)
	})
}

// Fixed bounds for the unauthenticated metrics listener (ADR-47): no
// configuration knobs.
const (
	scrapeTimeout        = 10 * time.Second
	maxConcurrentScrapes = 4
	maxHeaderBytes       = 8 << 10
)

// Handler serves the private registry's exposition, bounded in time and
// concurrency.
func Handler() http.Handler {
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		MaxRequestsInFlight: maxConcurrentScrapes,
		Timeout:             scrapeTimeout,
	})
}

// NewServer builds the metrics listener's server: a private mux with the
// single GET /metrics route (HEAD is served by the same pattern; other
// methods get 405, other paths 404) and fixed timeouts.
func NewServer() *http.Server {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", Handler())
	return &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      2 * scrapeTimeout,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    maxHeaderBytes,
	}
}

// IsUnspecifiedBind reports whether addr binds every interface (empty
// host or an unspecified IP). Such binds get a startup warning; they are
// not refused (ADR-47). The warning is not exposure detection.
func IsUnspecifiedBind(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false // the bind itself reports the malformed address
	}
	if host == "" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsUnspecified()
}

// Value returns the current value of one series (counter, gauge, or a
// histogram's _count/_sum), summed over series matching labels. It exists
// for tests in other packages, which assert deltas around fault seams.
func Value(metric string, labels map[string]string) float64 {
	families, err := registry.Gather()
	assertf(err == nil, "metrics.Value: gather: %v", err)
	family, suffix := metric, ""
	for _, s := range []string{"_count", "_sum"} {
		if len(metric) > len(s) && metric[len(metric)-len(s):] == s {
			family, suffix = metric[:len(metric)-len(s)], s
		}
	}
	var total float64
	for _, f := range families {
		if f.GetName() != family && f.GetName() != metric {
			continue
		}
	next:
		for _, m := range f.GetMetric() {
			have := map[string]string{}
			for _, lp := range m.GetLabel() {
				have[lp.GetName()] = lp.GetValue()
			}
			for k, v := range labels {
				if have[k] != v {
					continue next
				}
			}
			switch {
			case m.Counter != nil:
				total += m.GetCounter().GetValue()
			case m.Gauge != nil:
				total += m.GetGauge().GetValue()
			case m.Histogram != nil && f.GetName() == family && suffix == "_count":
				total += float64(m.GetHistogram().GetSampleCount())
			case m.Histogram != nil && f.GetName() == family && suffix == "_sum":
				total += m.GetHistogram().GetSampleSum()
			}
		}
	}
	return total
}

func assertf(cond bool, format string, args ...any) {
	if !cond {
		panic(fmt.Sprintf(format, args...))
	}
}
