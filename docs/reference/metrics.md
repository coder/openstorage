# Metrics

`openstorage serve` can expose Prometheus metrics on a separate, opt-in
listener. It is off by default. The design and its security rules are in
[ADR-48](../adr/048-metrics-ride-an-opt-in-separate-listener-with-fixed-labels.md).

## Turn it on

Set [`OPENSTORAGE_METRICS_LISTEN`](configuration.md#openstorage_metrics_listen)
to a `host:port`, then scrape `GET /metrics` on that address.

```sh
OPENSTORAGE_METRICS_LISTEN=127.0.0.1:9464 openstorage serve ...
curl -s http://127.0.0.1:9464/metrics
```

> [!WARNING]
> The metrics listener has **no authentication**. Bind it to loopback or to
> a private network that only your scraper can reach, and never route it
> through the public load balancer. `serve` warns at startup when the
> address binds every interface (`:9464`, `0.0.0.0`, `[::]`), but that
> warning is not access control: a public IP or hostname exposes the
> endpoint too.

- The listener serves only `GET /metrics` (and `HEAD`). Other paths return
  404; there is no pprof, expvar, or `/healthz` on it.
- The API/Git listener never serves `/metrics`, with or without a token.
- An invalid, unresolvable, or busy address, including the API's own
  address, stops `serve` at startup with exit 1.
- On success `serve` prints `metrics listening on <addr>` right after
  `listening on <addr>`.

## What it exposes

Each `serve` process exposes its own counters. Git hook subprocesses (the
ref updates of a `git push`) run in separate processes and are **not**
counted; their failures reach the client and the hook's stderr.

| Metric | Type | Labels | Counts |
|---|---|---|---|
| `openstorage_http_requests_total` | counter | `surface` (`api`, `git`, `health`), `code_class` (`1xx`–`5xx`) | Requests on the API/Git listener by final status class. `git` includes LFS and unmatched paths. |
| `openstorage_http_request_duration_seconds` | histogram | `surface` | Request wall time, including streamed Git pack transfer. |
| `openstorage_s3wal_cas_contention_exhausted_total` | counter | `scope` (`refs`, `scan_state`, `meta`) | S3WAL mutations in this process that returned "conditional-write contention retries exhausted". Once per mutation, not per attempt. |
| `openstorage_s3wal_lease_renewal_failures_total` | counter | none | Instance-lease renewal writes that returned an error, including ambiguous writes later resolved as landed. Not a count of lost leases. |
| `openstorage_s3wal_lease_held` | gauge | none | 1 while this process holds the instance lease by its own account. Local state, not proof of bucket ownership. A fail-stop exits the process, so the scraper usually sees the target disappear instead of a 0. |
| `openstorage_s3wal_manifest_size_guard_total` | counter | `result` (`warn`, `refuse`) | `warn`: the once-per-repository size warning was logged. `refuse`: a growing mutation was finally refused. |
| `openstorage_job_runs_total` | counter | `job` (`orphan_sweep`, `repack`), `result` (`success`, `noop`, `error`) | Executions in this process. Repack `noop` = nothing to fold or a lost race; `error` includes a committed fold that failed verification. These are operation outcomes, not final River job states. |
| `openstorage_commit_graph_refresh_total` | counter | `outcome` (`written`, `written_plain`, `skipped_tip_cap`, `skipped_busy`, `lock_held`, `memory_limit`, `timed_out`, `canceled`, `failed`) | Cache commit-graph refreshes after full hydrates in this process, one per refresh. A repository with no refs counts as `written`. The outcomes are the [commit-graph log contract](../how-to/deploy-production.md#commit-graph-refresh). No repository label: the `repo=` field of the log line names the repository. |

- Every label value is fixed in code. No repository, org, token, user,
  path, or host value ever becomes a label.
- All series exist from startup with value 0.
- S3WAL metrics stay at 0 on the PostgreSQL and SQLite backends.
- There are no Go runtime or process metrics in this release.

For which log lines remain the signal where no metric exists, see the
[metrics and log mapping](../how-to/deploy-production.md#metrics-and-log-signal-mapping).
