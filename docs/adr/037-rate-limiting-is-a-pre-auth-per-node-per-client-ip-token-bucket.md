# ADR-37: Rate limiting is a pre-auth, per-node, per-client-IP token bucket

> Extracted verbatim from [`15-phase6-implementation-notes.md`](../archive/phases/15-phase6-implementation-notes.md)
> (“Phase-6 Implementation Notes & ADRs (LFS, named-repo aliases, repo-urls, rate limiting)”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

docs/03 describes gateway-level 429s. Decision: **admission control is a hand-rolled
in-memory token bucket per client IP, applied at every REST route registration site
(v1 + aliases + repo-urls) *before* the auth middleware**, disabled unless configured.

- **Hand-rolled** with x/time/rate semantics (float tokens, monotonic refill, burst cap)
  rather than importing the module: the bucket logic is a few dozen lines, go.mod stays
  untouched, and the injectable clock makes refill deterministic in tests. Buckets live
  in a lazy map under a mutex; a periodic sweep drops stale (fully refilled) entries —
  behavior-neutral, since a dropped bucket is recreated full.
- **Key = `RemoteAddr` host.** `X-Forwarded-For` is deliberately not trusted — a
  spoofable header as the limit key would let any client mint fresh buckets at will.
  Deployments behind a trusted proxy terminate client identity there. *(Issue #30
  later added an opt-in `CODESTORAGE_TRUSTED_PROXIES` CIDR allowlist: when the peer
  is a listed proxy, the key becomes the rightmost untrusted `X-Forwarded-For` hop;
  the default stays `RemoteAddr`-only.)*
- **Per node, in memory.** Nodes are stateless (no shared limiter state to operate), and
  per-node limiting is the documented "basic" scope. Pre-auth 429s mean unauthenticated
  clients consume budget keyed by IP — accepted for this scope.
- **429 shape.** `Retry-After` (whole seconds, rounded up) plus the flavor-matched body:
  read routes answer the `{"error"}` envelope, write routes the docs/04 result envelope
  with status `"unavailable"` — `statusForHTTP` in errors.go gained the 429 row, so the
  mapping stays centralized.
- **Config is fail-fast.** `CODESTORAGE_RATE_RPS` / `CODESTORAGE_RATE_BURST` must be set
  as a pair (`RPS > 0` requires `BURST >= 1`); malformed, negative, NaN, or infinite
  values abort startup naming the variable. Unset (or explicit zero) disables limiting —
  the middleware becomes the identity.
