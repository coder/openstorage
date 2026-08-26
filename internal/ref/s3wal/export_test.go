// Internal-to-external test shim: the heavy suites (qualify_test.go,
// bench_test.go) live in package s3wal_test because they import package
// ref (SQLite bench baseline) — which now imports s3wal, so internal test
// files importing ref would cycle. This file re-exports the few internals
// those suites legitimately touch.
package s3wal

import "time"

// ProbeAtKey runs the §2.6 probe against a caller-chosen scratch-key
// prefix so stub-server suites never touch a real deployment's ProbeKey.
var ProbeAtKey = probeAtKey

// ErrorStatus exposes the SDK error classifier for verdict tabulation.
var ErrorStatus = errorStatus

// SetTestBackoff shrinks a client's contention backoff so exhaustion paths
// stay fast in tests.
func SetTestBackoff(c *Client, base, cap time.Duration) {
	assertf(c != nil, "SetTestBackoff: nil client")
	assertf(base > 0 && cap >= base, "SetTestBackoff(%v, %v)", base, cap)
	c.backoffBase = base
	c.backoffCap = cap
}

// PerSessionClient derives a Client sharing c's transport, bucket, backoff
// and decoder but with its own session-scoped seq map (see the DecodeSeq
// doc: the read-side assert is sound only per sequential session).
func PerSessionClient(c *Client) *Client {
	assertf(c != nil, "PerSessionClient: nil client")
	return &Client{
		s3:          c.s3,
		bucket:      c.bucket,
		backoffBase: c.backoffBase,
		backoffCap:  c.backoffCap,
		maxAttempts: c.maxAttempts,
		DecodeSeq:   c.DecodeSeq,
	}
}

// DefaultMaxAttempts exposes the §2.3 contention attempt budget for the
// 409-exhaustion suite.
const DefaultMaxAttempts = defaultMaxAttempts
