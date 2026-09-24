# ADR-48: Metrics ride an opt-in, separate listener with fixed labels

> Decision record for issue #325 (opt-in Prometheus metrics listener),
> which implements the C3 GA observability item of #192. The desk chose
> Option A of the
> [proposal](https://github.com/coder/openstorage/issues/192#issuecomment-5821063012)
> in its [decision](https://github.com/coder/openstorage/issues/192#issuecomment-5821373544).
> Until now, operators watched log lines and stderr only; `/healthz` was the
> only unauthenticated route. This addresses C3 only: every other #192
> gate and S3WAL's experimental status stay unchanged.

**Off by default; one variable turns it on.** `serve` starts a metrics
listener only when `OPENSTORAGE_METRICS_LISTEN` is set to a non-empty
`host:port`. Unset or empty means no listener, no socket, and no change to
today's behavior. The generic legacy-prefix fallback of
`internal/envconfig` applies to this name as it does to every
`OPENSTORAGE_*` name. The `hook` subcommand never reads the variable and
never listens.

**A separate listener, never the API or Git port.** The metrics listener
has its own `net.Listener`, its own `http.Server`, and its own private
`http.ServeMux`, serving `promhttp.HandlerFor` over a private registry.
Nothing uses `http.DefaultServeMux`, `promhttp.Handler()`, or the default
registry. The metrics mux registers one pattern, `GET /metrics` (Go's
mux also answers `HEAD` for it and returns 405 for other methods on that
path); every other path returns 404. The API/Git mux never registers
`/metrics`. Isolation is structural: the metrics handler exists only on
the metrics mux, so no route, header, or credential on the API port can
reach it. Tests assert that `/metrics` on the API port, with and without
a valid token, returns no metrics body. Pointing both variables at the
same address cannot merge the surfaces: the second bind fails and startup
stops (below).

**No authentication, so network isolation is the operator's control.** The
endpoint answers without credentials, and aggregate operational telemetry
is still sensitive. The docs say plainly: bind it to loopback or to a
private network that only the scraper can reach, and never route it
through the public load balancer. When the host part is empty or an
unspecified address (`:9090`, `0.0.0.0`, `[::]`), `serve` logs a warning
at startup. It does not refuse, because container deployments bind all
interfaces inside a private network namespace. The warning is not
exposure detection or access control: a concrete public IP or hostname
exposes the endpoint just as well, and the operator remains responsible
for isolation in every case.

**Startup fails on a bad address; it never degrades silently.** Both
listeners bind before `serve` announces `listening on …` or serves any
request. An unparsable address, a host that does not resolve, or an
address already in use makes `serve` return the bind error and exit 1;
listeners already bound are closed on that and every later startup error.
There is no retry, fallback port, or "metrics disabled" continuation. On a
successful bind `serve` prints `metrics listening on <addr>` (the actual
address, so `:0` is testable). If the metrics server stops unexpectedly
after startup, `serve` treats it like an API serve error and exits,
rather than running on unmonitored. On the termination signal the metrics
server shuts down with the same bounded drain as the API server. The
unauthenticated server has fixed, non-configurable bounds: read-header,
read, write, and idle timeouts, a small header-size limit, and a cap on
concurrent scrapes.

**Fixed-cardinality labels; nothing from request data.** Six signal groups
become seven metric families (the lease group has a counter and a gauge).
Every label value is a constant chosen in code, and every finite series
is initialized at startup so a clean process exposes zeros instead of
missing series:

| Metric | Type | Labels (all values fixed) |
|---|---|---|
| `openstorage_http_requests_total` | counter | `surface` = `api` \| `git` \| `health`; `code_class` = `1xx` \| `2xx` \| `3xx` \| `4xx` \| `5xx` |
| `openstorage_http_request_duration_seconds` | histogram | `surface` |
| `openstorage_s3wal_cas_contention_exhausted_total` | counter | `scope` = `refs` \| `scan_state` \| `meta` |
| `openstorage_s3wal_lease_renewal_failures_total` | counter | none |
| `openstorage_s3wal_lease_held` | gauge | none |
| `openstorage_s3wal_manifest_size_guard_total` | counter | `result` = `warn` \| `refuse` |
| `openstorage_job_runs_total` | counter | `job` = `orphan_sweep` \| `repack`; `result` = `success` \| `noop` \| `error` |

`surface` follows the API/Git mux's own routing, not a parse of the
request path: the whole mux is instrumented, and the pattern
`mux.Handler` reports (the matched registration, or for a redirect the
mux generates itself, such as `/api` → `/api/` or a path-cleaning
redirect, the path it leads to) is classified into `api` (the `/api/`
subtree), `health` (`/healthz`), or `git` (everything else, including LFS
and unmatched paths). The classifier's output is always one of those three
constants; the pattern never becomes a label. Wrapping the whole mux means
mux-generated responses are counted too. `code_class` comes from the final status the
handler wrote (an implicit 200 counts as `2xx`; informational `1xx`
headers do not replace the final status), clamped to the five classes.
The status wrapper keeps `Unwrap`, so `http.ResponseController` deadlines
and flushing on Git streams keep working.

What each counter means:

- **Contention:** a terminal S3WAL mutation (refs manifest, scan state, or
  metadata document) returned `ErrCASContention` to its caller. It counts
  at the mutation's return boundary only, not per conditional-write
  attempt, and no lower layer counts the same event again.
- **Size guard:** `warn` counts when the once-per-base warning is actually
  logged; `refuse` counts only a final, non-seeded refusal (a seeded
  attempt's refusal re-derives from fresh state and is not final).
- **Lease renewal failures:** renewal write attempts that returned an
  error, including ambiguous writes later resolved as landed. It is not a
  count of lost leases.
- **Lease held:** 1 after acquisition; set to 0 on graceful release and
  just before a heartbeat fail-stop exits. It is local process state, not
  proof of bucket ownership, fencing, or a completed delete, and a
  scraper may never see the final 0.
- **Job runs:** executions of the orphan sweep (`success` or `error`) and
  of a repack fold (`success` = folded cleanly; `noop` = nothing to fold
  or a lost race; `error` = any error, including a fold that committed
  but failed verification). These are operation outcomes, not final River
  job states: a later purge or completion-transaction failure is outside
  them. There are no repository, org, token, user,
path, host, or error-text labels. Metric names and help text are
constants and contain no configuration values, secrets, or tenant
identifiers. The registry is private to the process (not the global
default registry), so a dependency cannot add collectors to the endpoint.
Go runtime and process collectors are left out of this increment.

**pprof and expvar stay out.** No `net/http/pprof` import, no expvar
collector, and no debug route on either listener. The Prometheus client
package imports `expvar` transitively, which registers `/debug/vars` on
`http.DefaultServeMux`; no server here uses that mux. Tests assert that
`/debug/pprof/` and `/debug/vars` return 404 on **both** listeners.

**Per-process scope, stated honestly.** Each `serve` process exposes its
own counters. Work done in Git hook subprocesses (the ref updates of a
`git push`) runs in a separate process and is **not** counted by the S3WAL
counters; those failures stay visible through the hook's stderr and the
client-visible error, as the log contract documents. The lease gauge
drops to 0 on graceful release; on a heartbeat fail-stop the process
exits, so the scraper usually sees the target disappear; supervisor logs
and target availability remain the crash signals. River jobs count in the
replica that ran them. Cross-process aggregation is out of scope.

**The log contract stays.** The rows of the "What to monitor" and "S3WAL
operational signals" tables that have no metric keep their log lines as
the documented signal; the deployment guide maps every row to a metric or
to "log only". The contract covers the stable event text or prefix, its
documented fields, the emission condition, and the channel (structured log
or stderr), not the free-form error text appended after it.

**One new direct dependency.** `github.com/prometheus/client_golang`
(with `client_model`, `common`, and `procfs` as indirect modules) is
adopted under the charter's full dependency qualification and vendored in
the same change. An OpenTelemetry bridge remains possible later without
changing this listener contract.
