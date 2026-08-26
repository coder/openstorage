// MemCAS is the in-memory ConditionalStore fake: the fast (no Docker)
// substrate for the driver's recovery-predicate unit tests and the Phase 2
// DST fault plan. It reproduces the store semantics the driver depends on
// — conditional create/advance with content-derived ETags (identical bytes
// ⇒ identical ETag, so the §5-R4 ETag-ABA hazard is reproducible), byte-
// ordered listing — plus a small, explicit fault surface: each conditional
// PUT can be directed to fail ambiguously WITH or WITHOUT the write
// applying (the crucial §2.3 window), to fail before execution, or to
// exhaust contention retries; reads can serve the previous version once
// (stale read). It is deliberately not a general mock framework.
package s3wal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"
)

// PutFault directs one MemCAS conditional PUT attempt.
type PutFault int

const (
	// FaultNone applies normal conditional-write semantics.
	FaultNone PutFault = iota
	// FaultAmbiguousApplied applies the write, then reports *AmbiguousError
	// — the "response lost after commit" window.
	FaultAmbiguousApplied
	// FaultAmbiguousDropped drops the write and reports *AmbiguousError —
	// the "request lost before commit" window.
	FaultAmbiguousDropped
	// FaultNotExecuted drops the write and reports ErrNotExecuted — the
	// provably-never-sent class.
	FaultNotExecuted
	// FaultContention drops the write and reports ErrCASContention — the
	// exhausted-409-retries class.
	FaultContention
)

// MemCASHooks is the per-op fault surface. Nil members mean no faults.
type MemCASHooks struct {
	// PutFault classifies one conditional PUT attempt (op is "create" or
	// "advance"). It runs under the store lock: it must not call back into
	// the MemCAS.
	PutFault func(op, key string) PutFault
	// StaleGet, when it returns true, serves the key's PREVIOUS version
	// (body and ETag) for this read — the stale-read fidelity violation
	// the seq watermark must catch. Reads of keys with no previous
	// version serve normally.
	StaleGet func(key string) bool
	// AfterPut, when set, runs after every conditional PUT attempt
	// (op is "create" or "advance"), OUTSIDE the store lock and
	// regardless of the attempt's outcome: recovery tests use it to
	// interleave a competing writer between a faulted PUT and the
	// driver's recovery read. Unlike PutFault, it MAY call back into
	// the MemCAS.
	AfterPut func(op, key string)
}

type memVersion struct {
	body    []byte
	etag    string
	lastMod time.Time // store-clock write instant (Object.LastModified)
}

// MemCAS implements ConditionalStore in memory. The zero value is not
// usable; call NewMemCAS.
type MemCAS struct {
	mu    sync.Mutex
	cur   map[string]memVersion
	prev  map[string]memVersion
	Hooks MemCASHooks

	// Now, when set, is the fake STORE wall clock served as the §2.4-3
	// Date witness and stamped as every written version's LastModified:
	// like the real store, every operation records an
	// observation (StoreDate). Nil serves the real clock — correct for
	// every real-clock test, while fake-clock tests wire Now to their
	// local fake (agreeing clocks) or to a second clock to simulate
	// boot skew in either direction.
	Now func() time.Time

	dateMu      sync.Mutex
	storeDate   time.Time
	storeDateAt time.Time
}

// storeNow reads the fake store clock (Now, or the real clock when unset)
// — the single clock source behind both the Date witness and every
// version's LastModified stamp, exactly like a real store.
func (m *MemCAS) storeNow() time.Time {
	if m.Now != nil {
		return m.Now()
	}
	return time.Now()
}

// observeDate records the fake Date witness for one operation, exactly
// when the real client would (response processing time).
func (m *MemCAS) observeDate() {
	m.dateMu.Lock()
	defer m.dateMu.Unlock()
	m.storeDate, m.storeDateAt = m.storeNow(), time.Now()
}

// StoreDate — see ConditionalStore.
func (m *MemCAS) StoreDate() (date, observedAt time.Time, ok bool) {
	m.dateMu.Lock()
	defer m.dateMu.Unlock()
	return m.storeDate, m.storeDateAt, !m.storeDateAt.IsZero()
}

var _ ConditionalStore = (*MemCAS)(nil)

// NewMemCAS returns an empty in-memory conditional store.
func NewMemCAS() *MemCAS {
	return &MemCAS{
		cur:  make(map[string]memVersion),
		prev: make(map[string]memVersion),
	}
}

// memETag derives the content ETag: identical bytes yield identical ETags,
// exactly the real-store property the seq+nonce protocol must defeat.
func memETag(body []byte) string {
	sum := sha256.Sum256(body)
	return `"` + hex.EncodeToString(sum[:16]) + `"`
}

// Get fetches key (possibly the injected stale version).
func (m *MemCAS) Get(_ context.Context, key string) (Object, error) {
	assertf(key != "", "MemCAS.Get: empty key")
	m.observeDate()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Hooks.StaleGet != nil && m.Hooks.StaleGet(key) {
		if v, ok := m.prev[key]; ok {
			return v.object(), nil
		}
	}
	v, ok := m.cur[key]
	if !ok {
		return Object{}, fmt.Errorf("memcas get %s: %w", key, ErrNotExist)
	}
	return v.object(), nil
}

// object copies a stored version into the caller-facing Object shape.
func (v memVersion) object() Object {
	return Object{Body: append([]byte(nil), v.body...), ETag: v.etag, LastModified: v.lastMod}
}

// GetIfChanged fetches key unless etag is still current.
func (m *MemCAS) GetIfChanged(ctx context.Context, key, etag string) (Object, error) {
	assertf(etag != "", "MemCAS.GetIfChanged: empty etag")
	m.observeDate()
	m.mu.Lock()
	if v, ok := m.cur[key]; ok && v.etag == etag {
		m.mu.Unlock()
		return Object{}, fmt.Errorf("memcas get %s: %w", key, ErrNotModified)
	}
	m.mu.Unlock()
	return m.Get(ctx, key)
}

// Create writes key iff absent (the If-None-Match: * semantics).
func (m *MemCAS) Create(_ context.Context, key string, body []byte) (string, error) {
	assertf(key != "" && body != nil, "MemCAS.Create(%q)", key)
	m.observeDate()
	etag, err := m.createLocked(key, body)
	if m.Hooks.AfterPut != nil {
		m.Hooks.AfterPut("create", key)
	}
	return etag, err
}

func (m *MemCAS) createLocked(key string, body []byte) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.cur[key]; ok {
		// Precondition evaluated before fault injection: a 412 is a real
		// store verdict either way.
		return "", fmt.Errorf("memcas create %s: %w", key, ErrExists)
	}
	if err := m.fault("create", key, body); err != nil {
		return "", err
	}
	m.apply(key, body)
	return m.cur[key].etag, nil
}

// Advance overwrites key iff etag is current (the If-Match semantics).
func (m *MemCAS) Advance(_ context.Context, key string, body []byte, etag string) (string, error) {
	assertf(key != "" && body != nil && etag != "", "MemCAS.Advance(%q)", key)
	m.observeDate()
	newETag, err := m.advanceLocked(key, body, etag)
	if m.Hooks.AfterPut != nil {
		m.Hooks.AfterPut("advance", key)
	}
	return newETag, err
}

func (m *MemCAS) advanceLocked(key string, body []byte, etag string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.cur[key]
	if !ok || cur.etag != etag {
		// Precondition evaluated before fault injection: a 412 is a real
		// store verdict either way.
		return "", fmt.Errorf("memcas advance %s: %w", key, ErrLostRace)
	}
	if err := m.fault("advance", key, body); err != nil {
		return "", err
	}
	m.apply(key, body)
	return m.cur[key].etag, nil
}

// fault consults the PutFault hook and executes its directive; a non-nil
// return is the error the attempt must surface (FaultAmbiguousApplied
// commits the write FIRST — the write landed but the response was lost).
// Caller holds the lock and has already validated the precondition.
func (m *MemCAS) fault(op, key string, body []byte) error {
	if m.Hooks.PutFault == nil {
		return nil
	}
	switch f := m.Hooks.PutFault(op, key); f {
	case FaultNone:
		return nil
	case FaultAmbiguousApplied:
		m.apply(key, body)
		return &AmbiguousError{Op: op, Key: key, Cause: fmt.Errorf("memcas injected post-commit response loss")}
	case FaultAmbiguousDropped:
		return &AmbiguousError{Op: op, Key: key, Cause: fmt.Errorf("memcas injected pre-commit request loss")}
	case FaultNotExecuted:
		return fmt.Errorf("memcas %s %s: %w", op, key, ErrNotExecuted)
	case FaultContention:
		return fmt.Errorf("memcas %s %s: %w", op, key, ErrCASContention)
	default:
		panic(fmt.Sprintf("s3wal assertion failed: unknown MemCAS put fault %d", f))
	}
}

// apply commits body as key's current version, retaining the previous one
// for stale-read injection. Caller holds the lock.
func (m *MemCAS) apply(key string, body []byte) {
	if v, ok := m.cur[key]; ok {
		m.prev[key] = v
	}
	stored := append([]byte(nil), body...)
	m.cur[key] = memVersion{body: stored, etag: memETag(stored), lastMod: m.storeNow()}
}

// Prev returns key's most recently superseded version, if one exists. The
// DST harness (internal/simtest) serves it for targeted stale-read
// injection against fresh driver instances; in-package recovery tests use
// the StaleGet hook instead.
func (m *MemCAS) Prev(key string) (Object, bool) {
	assertf(key != "", "MemCAS.Prev: empty key")
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.prev[key]
	if !ok {
		return Object{}, false
	}
	return v.object(), true
}

// Delete removes key unconditionally.
func (m *MemCAS) Delete(_ context.Context, key string) error {
	assertf(key != "", "MemCAS.Delete: empty key")
	m.observeDate()
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.cur, key)
	delete(m.prev, key)
	return nil
}

// List returns up to limit keys under prefix in byte order, strictly
// greater than startAfter.
func (m *MemCAS) List(_ context.Context, prefix, startAfter string, limit int) ([]string, error) {
	assertf(prefix != "", "MemCAS.List: empty prefix")
	assertf(limit > 0, "MemCAS.List: non-positive limit %d", limit)
	m.observeDate()
	m.mu.Lock()
	defer m.mu.Unlock()
	var keys []string
	for k := range m.cur {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix && k > startAfter {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	if len(keys) > limit {
		keys = keys[:limit]
	}
	return keys, nil
}
