// S3 implementation of Backend for the s3wal zero-infrastructure mode
// (issue #133, docs/plans/s3wal-plan.md §5-Q4): ALL metadata tables live in
// ONE conditional JSON object, `_wal/meta.json`, advanced by CAS through
// the same s3wal.ConditionalStore substrate the ref manifests use
// (If-None-Match:* create, If-Match advance, seq + fresh writer nonce per
// write for ambiguous-PUT recovery — plan §2.3, simplified for a single
// low-write-rate object).
//
// Semantics mirror sqlite.go/sqlite_eventing.go 1:1 wherever a SQL twin
// exists: same sentinels, soft delete with the 404/409 distinction,
// (org_id, url) uniqueness spanning soft-deleted rows, the same opaque
// cursors, and Go-side unixnano timestamps (WithNowFunc injects the
// clock, so claim-lease expiry is a pure function of it — the DB-clock
// stand-in, exactly like meta.SQLite). Cross-writer arbitration never
// reads clocks: it is the object CAS itself, so the §2.4 manifest-clock
// machinery is deliberately absent here.
//
// Concurrency: one object ⇒ one process-local mutex serializes every
// operation (the meta twin of ref.S3WAL.lockBase); cross-process writers
// serialize on the object CAS. Reads validate a process-local
// (etag, body) cache via conditional GET (304) and decode fresh per use,
// so no decoded state is ever aliased across operations.
package meta

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	mrand "math/rand/v2"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/coder/openstorage/internal/metrics"
	"github.com/coder/openstorage/internal/ref/s3wal"
	"github.com/coder/openstorage/internal/s3oplog"
	"github.com/coder/openstorage/internal/secrets"
)

// ErrValueNotUTF8 rejects caller-supplied strings carrying invalid UTF-8
// before any document mutation (the ref.ErrRefNameNotUTF8 rationale):
// encoding/json silently rewrites invalid bytes to U+FFFD, so two distinct
// raw values (or map keys — org names key the orgs table) would collapse
// onto one stored spelling, silently corrupting lookups and uniqueness.
// The SQL twins diverge here exactly like the ref stores: PostgreSQL TEXT
// rejects invalid UTF-8 at the wire, SQLite stores raw bytes verbatim.
// Reads need no guard — no document can contain such a value, so raw-byte
// lookups are clean misses.
var ErrValueNotUTF8 = errors.New("s3walmeta: string value is not valid UTF-8")

// s3walMetaKey is the single conditional object holding every meta table.
// Unlike manifests it may carry an extension: it is a fixed key, never
// part of an order-sensitive prefix listing (plan §2.1 applies to
// `_wal/repo/` only), and the sweeper excludes the whole `_wal/` prefix.
const s3walMetaKey = "_wal/meta.json"

// s3walMetaVersion is the only document schema version this binary's
// normal code paths read or write. Only the format conversion
// (s3walmeta_convert.go, ADR-51) reads and writes the others.
const s3walMetaVersion = 2

// The document schema versions (ADR-51). v2 is v1 plus four fields whose
// absent value means exactly what v1 means (the zero-value rule):
// s3walMetaRepo.CreateState and s3walMetaUpstream.PushAttempt, PushDue,
// SyncDue.
const (
	s3walMetaV1 = 1
	s3walMetaV2 = 2
)

// s3walKeyRefreshInterval is the org public-key snapshot's background
// refresh cadence — the cross-process staleness bound for
// PublicKeysForOrg: a key written by ANOTHER process becomes visible here
// within one interval (plus one conditional GET). 15s keeps a quiet
// store's cost negligible (four 304-answered conditional GETs per minute)
// while bounding foreign-rotation lag far below token lifetimes. The
// bound is acceptable in the zero-infra topology: production org keys are
// written only by this process's own serve bootstrap (immediately visible
// — mutate installs the snapshot synchronously) and the instance lease
// fail-stops dual serves, so long-lived foreign writers exist only in
// multi-process test fixtures, which disable the lease and tolerate the
// interval.
const s3walKeyRefreshInterval = 15 * time.Second

// withKeyRefreshInterval overrides the org-key snapshot refresh cadence.
// In-package tests only — production always runs the documented
// s3walKeyRefreshInterval.
func withKeyRefreshInterval(d time.Duration) Option {
	assertf(d > 0, "withKeyRefreshInterval: non-positive interval %v", d)
	return func(o *storeOptions) { o.keyRefreshEvery = d }
}

// s3walMetaDoc is the whole metadata state — every table of the SQL
// schema as one JSON document. Timestamps are int64 unixnano written
// Go-side (the sqlite.go storage convention, so rows round-trip exactly
// and returned time.Time values byte-match later reads); zero means
// NULL/cleared where the SQL column is nullable or an epoch lease.
type s3walMetaDoc struct {
	V int `json:"v"`
	// Seq is strictly monotonic, bumped by every CAS; with the fresh
	// Writer nonce it guarantees distinct bytes per version ⇒ distinct
	// ETag (the §5-R4 ETag-ABA defense, same as manifests).
	Seq    int64  `json:"seq"`
	Writer string `json:"writer"`
	// NextOrgID / NextKeyID replace the SQL AUTOINCREMENT columns; IDs
	// stay monotonic so registration order survives deletes.
	NextOrgID int64 `json:"next_org_id"`
	NextKeyID int64 `json:"next_key_id"`

	Orgs        map[string]*s3walMetaOrg        `json:"orgs,omitempty"`        // by org name
	Repos       map[string]*s3walMetaRepo       `json:"repos,omitempty"`       // by repo_id
	Webhooks    map[string]*s3walMetaWebhook    `json:"webhooks,omitempty"`    // by webhook id
	Credentials map[string]*s3walMetaCredential `json:"credentials,omitempty"` // by credential id
	Upstreams   map[string]*s3walMetaUpstream   `json:"upstreams,omitempty"`   // by repo_id
}

type s3walMetaOrg struct {
	ID        int64 `json:"id"`
	CreatedAt int64 `json:"created_at"`
	// PublicKeys in registration order (replaced wholesale by upsert).
	PublicKeys []string `json:"public_keys"`
	// SigningKeys ordered by ID (append-only mint from NextKeyID).
	SigningKeys []s3walMetaSigningKey `json:"signing_keys,omitempty"`
}

type s3walMetaSigningKey struct {
	ID        int64  `json:"id"`
	KeyText   string `json:"key_text"`
	CreatedAt int64  `json:"created_at"`
}

type s3walMetaRepo struct {
	OrgID         int64  `json:"org_id"`
	URL           string `json:"url"`
	DefaultBranch string `json:"default_branch"`
	CreatedAt     int64  `json:"created_at"`
	DeletedAt     int64  `json:"deleted_at,omitempty"` // 0 = live
	PurgedAt      int64  `json:"purged_at,omitempty"`  // 0 = never purged
	// CreateState is v2 only (ADR-51): CreateState as stored by the SQL
	// backends; absent (0) is CreateStateLegacy, which is what every v1
	// row means.
	CreateState int `json:"create_state,omitempty"`
}

type s3walMetaWebhook struct {
	OrgID     int64  `json:"org_id"`
	URL       string `json:"url"`
	Secret    string `json:"secret"` // stored form: plaintext or envelope (secrets.go)
	CreatedAt int64  `json:"created_at"`
}

type s3walMetaCredential struct {
	RepoID    string `json:"repo_id"`
	Username  string `json:"username"`
	Password  string `json:"password"` // stored form: plaintext or envelope (secrets.go)
	CreatedAt int64  `json:"created_at"`
}

type s3walMetaUpstream struct {
	Provider     string `json:"provider"`
	Owner        string `json:"owner"`
	Name         string `json:"name"`
	UpstreamHost string `json:"upstream_host"`
	RunCount     int    `json:"run_count"`
	SuccessCount int    `json:"success_count"`
	Syncing      bool   `json:"syncing"`
	SyncPending  bool   `json:"sync_pending"`
	// Claim leases: holder token plus absolute unixnano expiry (0 =
	// cleared, the SQL epoch default) — compared against the injected
	// clock exactly like sqlite_eventing.go.
	SyncClaimHolder    string `json:"sync_claim_holder,omitempty"`
	SyncClaimExpiresAt int64  `json:"sync_claim_expires_at,omitempty"`
	ForwardOnPush      bool   `json:"forward_on_push,omitempty"`
	PushPending        int    `json:"push_pending,omitempty"`
	PushSyncing        bool   `json:"push_syncing,omitempty"`
	PushClaimHolder    string `json:"push_claim_holder,omitempty"`
	PushClaimExpiresAt int64  `json:"push_claim_expires_at,omitempty"`
	PushClaimForce     bool   `json:"push_claim_force,omitempty"`
	PushRunCount       int    `json:"push_run_count,omitempty"`
	PushSuccessCount   int    `json:"push_success_count,omitempty"`
	CreatedAt          int64  `json:"created_at"`
	// Detaching marks a sink-wired DeleteRepoUpstream in flight (phase 1
	// of the fence-revoking detach; ordering proof on that method): once
	// durable, TryBeginSync refuses the row so no new fence can be
	// installed, and only the detach's final phase removes the row. Never
	// unset. No legacy-schema concern: no pre-s3walmeta binary exists
	// (the package-wide argument), so absent means false, never unknown.
	Detaching bool `json:"detaching,omitempty"`
	// The ADR-50 schedule fields, v2 only (ADR-51). Absent (0) means the
	// first attempt, due now: what every v1 lane means.
	PushAttempt int   `json:"push_attempt,omitempty"`
	PushDue     int64 `json:"push_due,omitempty"`
	SyncDue     int64 `json:"sync_due,omitempty"`
}

// toRepoUpstream renders the stored row as the interface type, converting
// unixnano back to UTC time.Time per the RepoUpstream contract (cleared
// leases read as the epoch instant, matching the SQL twins).
func (u *s3walMetaUpstream) toRepoUpstream(repoID string) RepoUpstream {
	return RepoUpstream{
		RepoID:             repoID,
		Provider:           u.Provider,
		Owner:              u.Owner,
		Name:               u.Name,
		UpstreamHost:       u.UpstreamHost,
		RunCount:           u.RunCount,
		SuccessCount:       u.SuccessCount,
		Syncing:            u.Syncing,
		SyncPending:        u.SyncPending,
		SyncClaimHolder:    u.SyncClaimHolder,
		SyncClaimExpiresAt: time.Unix(0, u.SyncClaimExpiresAt).UTC(),
		ForwardOnPush:      u.ForwardOnPush,
		PushPending:        u.PushPending,
		PushSyncing:        u.PushSyncing,
		PushClaimHolder:    u.PushClaimHolder,
		PushClaimExpiresAt: time.Unix(0, u.PushClaimExpiresAt).UTC(),
		PushClaimForce:     u.PushClaimForce,
		PushRunCount:       u.PushRunCount,
		PushSuccessCount:   u.PushSuccessCount,
		CreatedAt:          time.Unix(0, u.CreatedAt).UTC(),
	}
}

// newS3WALMetaDoc is the empty (never-written) state: Seq 0, so the first
// encodeAdvance writes seq 1 through the If-None-Match:* create path. A
// missing object and this value are interchangeable.
func newS3WALMetaDoc() *s3walMetaDoc {
	return &s3walMetaDoc{V: s3walMetaVersion}
}

// decodeS3WALMetaDoc parses and validates one fetched document body.
// Corruption is a crash-and-burn error, never a silent empty state.
func decodeS3WALMetaDoc(body []byte) (*s3walMetaDoc, error) {
	return decodeS3WALMetaDocVersion(body, s3walMetaVersion)
}

// decodeS3WALMetaDocVersion is decodeS3WALMetaDoc accepting any of the
// listed versions. Only the format conversion lists more than one.
func decodeS3WALMetaDocVersion(body []byte, versions ...int) (*s3walMetaDoc, error) {
	assertf(len(versions) > 0, "decodeS3WALMetaDocVersion: no versions")
	var doc s3walMetaDoc
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, &CorruptDocumentError{Err: fmt.Errorf("document decode: %w", err)}
	}
	if !slices.Contains(versions, doc.V) {
		return nil, &FormatVersionError{Got: doc.V, Want: versions}
	}
	if doc.Seq < 1 {
		return nil, &CorruptDocumentError{Err: fmt.Errorf("document seq %d invalid (stored documents start at 1)", doc.Seq)}
	}
	if doc.Writer == "" {
		return nil, &CorruptDocumentError{Err: fmt.Errorf("document seq %d has no writer nonce", doc.Seq)}
	}
	if err := doc.checkIDs(); err != nil {
		return nil, &CorruptDocumentError{Err: err}
	}
	if err := doc.checkRows(); err != nil {
		return nil, &CorruptDocumentError{Err: fmt.Errorf("document seq %d: %w", doc.Seq, err)}
	}
	return &doc, nil
}

// s3walRepoIDPattern is the only repo id shape newRepoID mints.
var s3walRepoIDPattern = regexp.MustCompile(`^repo_[0-9a-f]{12}$`)

// checkRows rejects row contents no writer produces and later code relies
// on (issue #492): an empty org name, webhook or credential id, a null row
// in any table, a repo or upstream key that
// is not a minted repo id, and the strings every write path requires (a
// repo's url and default branch, an upstream's owner, name and host, a
// webhook's url and stored secret). Decode is the one boundary, so the
// readers keep their assertions as internal invariants. Sorted keys give
// one stable message.
func (doc *s3walMetaDoc) checkRows() error {
	if _, ok := doc.Orgs[""]; ok {
		return errors.New("an org has an empty name")
	}
	for _, id := range slices.Sorted(maps.Keys(doc.Repos)) {
		r := doc.Repos[id]
		switch {
		case r == nil:
			return fmt.Errorf("repo %q has no row", id)
		case !s3walRepoIDPattern.MatchString(id):
			return fmt.Errorf("repo id %q is not a minted repo id", id)
		case r.URL == "" || r.DefaultBranch == "":
			return fmt.Errorf("repo %q has an empty url or default branch", id)
		}
	}
	for _, id := range slices.Sorted(maps.Keys(doc.Upstreams)) {
		u := doc.Upstreams[id]
		switch {
		case u == nil:
			return fmt.Errorf("upstream %q has no row", id)
		case !s3walRepoIDPattern.MatchString(id):
			return fmt.Errorf("upstream key %q is not a minted repo id", id)
		case u.Owner == "" || u.Name == "" || u.UpstreamHost == "":
			return fmt.Errorf("upstream %q has an empty owner, name or host", id)
		}
	}
	for _, id := range slices.Sorted(maps.Keys(doc.Webhooks)) {
		switch w := doc.Webhooks[id]; {
		case id == "":
			return errors.New("a webhook has an empty id")
		case w == nil:
			return fmt.Errorf("webhook %q has no row", id)
		case w.URL == "" || w.Secret == "":
			return fmt.Errorf("webhook %q has an empty url or secret", id)
		}
	}
	for _, id := range slices.Sorted(maps.Keys(doc.Credentials)) {
		switch {
		case id == "":
			return errors.New("a credential has an empty id")
		case doc.Credentials[id] == nil:
			return fmt.Errorf("credential %q has no row", id)
		}
	}
	return nil
}

// checkIDs rejects id counters no writer can produce (issue #478): the
// bytes come from the bucket, so a violation is a damaged document, not a
// broken invariant, and must not panic serve or the hook. Writers mint ids
// as counter+1, so every org id lies in 1..NextOrgID and every signing key
// id in 1..NextKeyID, each used once. With these checked, lookupOrg's
// positive-id assertion stays a real internal invariant.
func (doc *s3walMetaDoc) checkIDs() error {
	if doc.NextOrgID < 0 || doc.NextKeyID < 0 {
		return fmt.Errorf("document seq %d has negative id counters (%d, %d)", doc.Seq, doc.NextOrgID, doc.NextKeyID)
	}
	// Upstream lanes (issue #481): no writer leaves a claim without its
	// holder, and the schedule fields only count up from 0.
	for _, repoID := range slices.Sorted(maps.Keys(doc.Upstreams)) {
		if u := doc.Upstreams[repoID]; u != nil {
			if err := checkUpstreamShape(repoID, u); err != nil {
				return fmt.Errorf("document seq %d: %w", doc.Seq, err)
			}
		}
	}
	orgIDs := make(map[int64]bool, len(doc.Orgs))
	keyIDs := make(map[int64]bool)
	for _, name := range slices.Sorted(maps.Keys(doc.Orgs)) { // sorted: one stable message
		org := doc.Orgs[name]
		if org == nil {
			return fmt.Errorf("document seq %d: org %q has no row", doc.Seq, name)
		}
		if org.ID < 1 || org.ID > doc.NextOrgID {
			return fmt.Errorf("document seq %d: org %q has id %d outside 1..%d", doc.Seq, name, org.ID, doc.NextOrgID)
		}
		if orgIDs[org.ID] {
			return fmt.Errorf("document seq %d: org id %d is used twice", doc.Seq, org.ID)
		}
		orgIDs[org.ID] = true
		for _, key := range org.SigningKeys {
			if key.ID < 1 || key.ID > doc.NextKeyID {
				return fmt.Errorf("document seq %d: org %q signing key id %d outside 1..%d", doc.Seq, name, key.ID, doc.NextKeyID)
			}
			if keyIDs[key.ID] {
				return fmt.Errorf("document seq %d: signing key id %d is used twice", doc.Seq, key.ID)
			}
			keyIDs[key.ID] = true
		}
	}
	return nil
}

// FormatVersionError reports a readable metadata document whose format
// version this read does not accept (ADR-51). Unlike a store error it is
// not transient: the store answered, and only a format conversion (or the
// matching binary) helps. Callers that fail open on store trouble, such as
// the hook's eventing (issue #445), refuse on this error instead.
type FormatVersionError struct {
	Got  int   // the stored document's version
	Want []int // the versions this read accepts
}

func (e *FormatVersionError) Error() string {
	if e.Got == s3walMetaV1 {
		// ADR-51 rollout R1: only the offline conversion reads v1.
		return "s3walmeta: document version 1 needs conversion: stop serve, then run `openstorage s3wal-meta convert --to 2` (docs/how-to/deploy-production.md#upgrading-the-s3wal-metadata-format)"
	}
	return fmt.Sprintf("s3walmeta: document version %d unsupported (want %s)", e.Got, wantVersions(e.Want))
}

// CorruptDocumentError reports a stored metadata document that does not
// decode or fails a structural guard. Like FormatVersionError it is not
// transient: the store answered, every metadata read fails on the same
// bytes, and only a restore helps. The hook refuses on it (issue #466).
type CorruptDocumentError struct {
	Err error // what is wrong with the document
}

func (e *CorruptDocumentError) Error() string {
	return "s3walmeta: " + s3walMetaKey + " is corrupt (" + e.Err.Error() + "): do not edit it; keep serve stopped and restore the bucket from a verified stopped-node backup (docs/how-to/backup-restore-s3wal.md#restore-and-validate-before-cutover)"
}

func (e *CorruptDocumentError) Unwrap() error { return e.Err }

// CheckFormat reads b's stored metadata document once and returns its
// error: a *FormatVersionError when the format is wrong, a
// *CorruptDocumentError when the document is damaged, a store error when
// the read failed, nil otherwise (including a missing document, the empty
// state). Backends without a format version (PostgreSQL, SQLite)
// return nil without I/O. meta.Open never reads the document, so this is
// the cheap way to learn the format before a write that must not proceed
// on the wrong one.
func CheckFormat(ctx context.Context, b Backend) error {
	s, ok := b.(*S3WAL)
	if !ok {
		return nil
	}
	_, err := s.viewDoc(ctx)
	return err
}

// wantVersions renders the accepted versions for an error message: "2",
// or "1 or 2".
func wantVersions(versions []int) string {
	parts := make([]string, len(versions))
	for i, v := range versions {
		parts[i] = strconv.Itoa(v)
	}
	return strings.Join(parts, " or ")
}

// encodeAdvance encodes the NEXT version of doc: seq+1 and a fresh 128-bit
// writer nonce (distinct bytes per write ⇒ distinct ETag). doc is updated
// in place so the caller can read the minted nonce for ambiguous-PUT
// recovery. The org-name key assert is the codec-boundary backstop for
// the U+FFFD aliasing hazard (see ErrValueNotUTF8): every public write
// path rejects invalid UTF-8 first, so tripping it is a missed guard.
func (doc *s3walMetaDoc) encodeAdvance() ([]byte, error) {
	assertf(doc.V == s3walMetaVersion, "encodeAdvance: version %d", doc.V)
	return doc.encodeNext()
}

// encodeNext is encodeAdvance for any known version. Only the format
// conversion calls it directly.
func (doc *s3walMetaDoc) encodeNext() ([]byte, error) {
	assertf(doc.V == s3walMetaV1 || doc.V == s3walMetaV2, "encodeNext: version %d", doc.V)
	assertf(doc.Seq >= 0, "encodeNext: seq %d", doc.Seq)
	for name := range doc.Orgs {
		assertf(utf8.ValidString(name),
			"encodeAdvance: org key %q is not valid UTF-8 — JSON encoding would alias it onto its U+FFFD spelling", name)
	}
	if doc.Seq == math.MaxInt64 {
		// The bump would wrap negative (issue #487). The document stays
		// readable; only writes are refused.
		return nil, errCounterExhausted("seq", doc.Seq)
	}
	doc.Seq++
	doc.Writer = newS3WALMetaNonce()
	body, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("s3walmeta: document encode: %w", err)
	}
	return body, nil
}

// errCounterExhausted refuses a write whose seq or id bump would wrap
// negative (issue #487). No writer reaches MaxInt64, so this only answers an
// out-of-band edit; the document itself stays readable.
func errCounterExhausted(field string, v int64) error {
	return fmt.Errorf("s3walmeta: %s %d is at its maximum: refusing the write (the document was edited out of band; see docs/how-to/backup-restore-s3wal.md#restore-and-validate-before-cutover)", field, v)
}

// newS3WALMetaNonce mints the 128-bit hex writer nonce.
func newS3WALMetaNonce() string {
	var buf [16]byte
	_, err := rand.Read(buf[:])
	assertf(err == nil, "s3walmeta: crypto/rand failed: %v", err)
	return hex.EncodeToString(buf[:])
}

// S3WAL implements Backend (plus the purge seams) over one conditional
// meta object. Construct via OpenS3WAL (production, real store + probe)
// or NewS3WAL (tests/DST, injectable substrate).
type S3WAL struct {
	cas s3wal.ConditionalStore
	// probe runs InitSchema's conditional-write fidelity check (§2.6);
	// nil (test fakes) skips it.
	probe   func(ctx context.Context) error
	closeFn func()
	// codec, when non-nil, envelope-encrypts the secret fields exactly
	// like the SQL backends (secrets.go).
	codec *secrets.Codec
	// now, when non-nil, replaces time.Now for every stored timestamp
	// AND the claim-lease expiry comparisons (WithNowFunc — the SQLite
	// DB-clock twin).
	now func() time.Time
	// sink, when non-nil, mirrors inbound sync claims into the base repo
	// group's ref manifest (§2.8 write-through; SyncClaimSink).
	sink SyncClaimSink

	// Lost-race retry budget (plan §2.3).
	casAttempts int
	backoffBase time.Duration
	backoffCap  time.Duration

	// mu serializes every operation in this process: one object ⇒ one
	// lock (the meta twin of ref.S3WAL.lockBase).
	mu sync.Mutex
	// cacheETag/cacheBody is the 304-validated read cache; lastSeq is the
	// per-object stale-read watermark (plan §2.3 — a store serving an
	// older seq than this process observed is a fidelity violation).
	cacheETag string
	cacheBody []byte
	lastSeq   int64
	// rolledBack latches once a fetched document does not continue the
	// cached one, the object vanishes (issue #488), or it changes into
	// bytes that do not decode (issue #501): from then on only
	// the cached document itself (the same bytes, or a 304 on its ETag)
	// is accepted again, because a restored or recreated lineage can
	// advance back to any seq with different contents.
	rolledBack bool
	// quarantined, once set, answers every read until the process
	// restarts: the cached document is known corrupt but decodes, so
	// neither the cache nor a 304 may serve it again (issue #481).
	quarantined error
	// cacheView is cacheBody decoded once, shared by every viewDoc caller
	// until the cache changes (issue #470: the 1 s sync sweep otherwise
	// decodes the whole document on every 304). It is READ-ONLY: viewDoc
	// callers must never mutate it. Mutations decode their own copy.
	// nil means not decoded yet.
	cacheView *s3walMetaDoc

	// orgKeys is the in-memory org public-key snapshot (PR #168 security
	// P2): PublicKeysForOrg is auth.KeySource — the verifier selects keys
	// from the UNVERIFIED iss claim of every bearer token BEFORE checking
	// the signature, so resolving it through readDoc would let
	// unauthenticated JWT floods serialize legitimate work on mu across
	// S3 round-trips and drive object-store request costs. adoptOrgKeys
	// installs the snapshot (under mu, so installs follow observation
	// order) from every fresh read, confirmed-missing object, and CAS
	// success; orgKeyRefreshLoop keeps it warm across processes. nil
	// means never populated — the cold-start lookup blocks on one real
	// read. orgKeysMu guards only the map pointer, never spanning I/O.
	orgKeysMu sync.RWMutex
	orgKeys   map[string][]string
	// keyRefreshEvery is the refresh cadence (s3walKeyRefreshInterval
	// unless a test overrides it); refreshCancel/refreshDone bound the
	// refresher goroutine's lifetime to Close.
	keyRefreshEvery time.Duration
	refreshCancel   context.CancelFunc
	refreshDone     chan struct{}
}

var _ Backend = (*S3WAL)(nil)

// newS3WALStore wraps a conditional store; probe and closeFn may be nil.
// Every store runs one org-key refresher goroutine until Close.
func newS3WALStore(cs s3wal.ConditionalStore, probe func(ctx context.Context) error, closeFn func(), o storeOptions) *S3WAL {
	assertf(cs != nil, "newS3WALStore: nil conditional store")
	s := &S3WAL{
		cas:             cs,
		probe:           probe,
		closeFn:         closeFn,
		codec:           o.codec,
		now:             o.now,
		sink:            o.claimSink,
		casAttempts:     16,
		backoffBase:     25 * time.Millisecond, // §2.3: full-jitter 25ms→2s, ≤16 attempts — same pacing as the ref store (issue #181)
		backoffCap:      2 * time.Second,
		keyRefreshEvery: o.keyRefreshEvery,
		refreshDone:     make(chan struct{}),
	}
	if s.keyRefreshEvery == 0 {
		s.keyRefreshEvery = s3walKeyRefreshInterval
	}
	// The refresher's GETs are background work, not part of any request:
	// the op log tags them (issue #484, TestS3WAL_PushOpCensus).
	ctx, cancel := context.WithCancel(s3oplog.WithScope(context.Background(), s3walKeyRefreshScope))
	s.refreshCancel = cancel
	go s.orgKeyRefreshLoop(ctx)
	return s
}

// NewS3WAL wraps an injectable conditional store, probe-less: the
// constructor unit tests and the DST harness build meta backends from
// (mirroring ref.NewS3WAL). Production serving goes through OpenS3WAL.
func NewS3WAL(cs s3wal.ConditionalStore, opts ...Option) *S3WAL {
	return newS3WALStore(cs, nil, nil, resolveOptions(opts))
}

// OpenS3WAL opens s3wal://bucket?endpoint=…&region=… — the exact URL
// grammar of the ref store (s3wal.Dial). Open performs no network I/O;
// the conditional-write probe runs at InitSchema (ADR-12: Open never runs
// DDL, and hooks reconstructing stores from env URLs must not probe).
func OpenS3WAL(ctx context.Context, rawURL string, opts ...Option) (*S3WAL, error) {
	c, err := s3wal.Dial(ctx, rawURL)
	if err != nil {
		return nil, fmt.Errorf("open s3wal meta store: %w", err)
	}
	return newS3WALStore(c, func(ctx context.Context) error { return s3wal.Probe(ctx, c) }, c.Close, resolveOptions(opts)), nil
}

// InitSchema proves the store's conditional-write fidelity before serving
// (plan §2.6) — there is no DDL, the document is created lazily by the
// first write. Unlike ref.S3WAL.InitSchema it does NOT require an HTTP
// Date witness: this backend runs no §2.4 TTL expiry verdicts (claim
// leases compare caller-written absolute stamps under the injected clock,
// the SQLite twin's semantics), so no destructive verdict ever consults
// the store clock. Idempotent.
func (s *S3WAL) InitSchema(ctx context.Context) error {
	if s.probe == nil {
		return nil
	}
	if err := s.probe(ctx); err != nil {
		return fmt.Errorf("s3walmeta init: %w", err)
	}
	return nil
}

// Close stops the org-key refresher — joining its goroutine, so no
// background read outlives Close — and releases the underlying store.
// Idempotent.
func (s *S3WAL) Close() {
	s.refreshCancel()
	<-s.refreshDone
	if s.closeFn != nil {
		s.closeFn()
	}
}

// s3walKeyRefreshScope is the op-log scope (s3oplog) of the org-key
// refresher's round trips.
const s3walKeyRefreshScope = s3oplog.ScopeKeyRefresh

// orgKeyRefreshLoop keeps the org-key snapshot warm across processes: one
// goroutine, one conditional GET per keyRefreshEvery (304-answered while
// the document is quiet), single-flight by construction — it can never
// stampede the store. Refresh errors are dropped deliberately: lookups
// fail static on the last good snapshot, the next tick retries, and
// foreground operations surface persistent store failures on their own
// reads.
func (s *S3WAL) orgKeyRefreshLoop(ctx context.Context) {
	defer close(s.refreshDone)
	ticker := time.NewTicker(s.keyRefreshEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = s.viewDoc(ctx) // adoptOrgKeys runs on every fresh read
		}
	}
}

// nowNano mirrors SQLite.nowNano: the store's current instant as stored
// (unixnano) plus its exact time.Time rendering, so returned rows
// byte-match later reads (JSON stores the integer; decode reconstructs
// the identical time.Unix(0, n).UTC()).
func (s *S3WAL) nowNano() (int64, time.Time) {
	now := time.Now()
	if s.now != nil {
		now = s.now()
	}
	nanos := now.UnixNano()
	return nanos, time.Unix(0, nanos).UTC()
}

// utf8OK rejects caller-supplied write values that JSON would coerce.
func utf8OK(what, v string) error {
	if !utf8.ValidString(v) {
		return fmt.Errorf("%s: %w", what, ErrValueNotUTF8)
	}
	return nil
}

// StaleDocumentError reports a fetched metadata document that does not
// continue the one this process last saw (plan §2.3 stale-read
// watermark): its seq is lower, or it is a different document at the
// same seq, or this process already saw such a regression. The bytes come
// from the bucket, so this is not an internal invariant: the store served
// a stale read, or the object was replaced while this process ran (issue
// #488). The process keeps its view and refuses to read or write through
// the fetched document, but it must not crash: the org-key refresher
// meets this on its next tick.
type StaleDocumentError struct {
	Got  int64 // the fetched document's seq
	Seen int64 // the seq of the document this process last saw
}

func (e *StaleDocumentError) Error() string {
	return fmt.Sprintf("s3walmeta: %s at seq %d does not continue the document this process last saw at seq %d: the store served a stale read, or the object was replaced while serve ran. This process will not read or write through it until the store serves that document again or the process restarts. If the bucket was restored on purpose, stop serve, then follow docs/how-to/backup-restore-s3wal.md#restore-and-validate-before-cutover", s3walMetaKey, e.Got, e.Seen)
}

// observeDoc applies the stale-read watermark to a fetched document and
// its bytes. The cached body is the document at lastSeq, so "the same
// document" means the same bytes: seq and writer nonce live in the body
// and survive an out-of-band edit of the other fields. Caller holds mu.
func (s *S3WAL) observeDoc(doc *s3walMetaDoc, body []byte) error {
	same := bytes.Equal(body, s.cacheBody)
	switch {
	case s.rolledBack && !same, doc.Seq < s.lastSeq, doc.Seq == s.lastSeq && !same:
		s.rolledBack = true
		return &StaleDocumentError{Got: doc.Seq, Seen: s.lastSeq}
	}
	s.lastSeq, s.rolledBack = doc.Seq, false
	return nil
}

// readDoc fetches the current document, validating the process-local
// cache with a conditional GET (304 ⇒ decode the cached body — decoding
// fresh per use keeps mutation snapshots from ever aliasing the cache).
// A missing object is the empty state with etag "" (create path). Caller
// holds mu.
func (s *S3WAL) readDoc(ctx context.Context) (*s3walMetaDoc, string, error) {
	return s.readDocShared(ctx, false)
}

// readDocShared is readDoc. With shared (read-only callers), a 304 answers
// the cached decoded view instead of decoding the body again, and a fresh
// read installs its decode as that view. Caller holds mu.
func (s *S3WAL) readDocShared(ctx context.Context, shared bool) (*s3walMetaDoc, string, error) {
	if s.quarantined != nil {
		return nil, "", s.quarantined
	}
	if s.cacheETag == "" {
		obj, err := s.cas.Get(ctx, s3walMetaKey)
		if errors.Is(err, s3wal.ErrNotExist) {
			// A confirmed-missing object is the confirmed-empty org set:
			// populate the snapshot so pre-bootstrap key lookups also
			// answer (ErrNotFound) without touching the store.
			doc := newS3WALMetaDoc()
			s.adoptOrgKeys(doc)
			return doc, "", nil
		}
		if err != nil {
			return nil, "", fmt.Errorf("s3walmeta read: %w", err)
		}
		return s.adoptRead(obj, shared)
	}
	obj, err := s.cas.GetIfChanged(ctx, s3walMetaKey, s.cacheETag)
	if errors.Is(err, s3wal.ErrNotModified) {
		s.rolledBack = false // the store holds the cached document again
		if shared && s.cacheView != nil {
			s.checkViewIntact()
			return s.cacheView, s.cacheETag, nil
		}
		doc, derr := decodeS3WALMetaDoc(s.cacheBody)
		if derr != nil {
			return nil, "", derr
		}
		if shared {
			s.cacheView = doc
		}
		return doc, s.cacheETag, nil
	}
	if errors.Is(err, s3wal.ErrNotExist) {
		// The document is never deleted once created (purge stamps a
		// tombstone field, exactly like manifests): a vanish after seq
		// lastSeq was observed is store infidelity or operator surgery.
		// Latch it: a lineage recreated after the vanish is not the
		// cached document either.
		s.rolledBack = true
		return nil, "", fmt.Errorf("s3walmeta read: %s vanished after seq %d was observed (the meta object is never deleted)", s3walMetaKey, s.lastSeq)
	}
	if err != nil {
		return nil, "", fmt.Errorf("s3walmeta read: %w", err)
	}
	return s.adoptRead(obj, shared)
}

// checkSharedView turns on checkViewIntact. This package's tests set it
// (s3walmeta_view_test.go), and so does any binary built with the
// openstorage_viewcheck tag (s3walmeta_viewcheck.go). Release builds skip
// the decode the shared view exists to avoid.
var checkSharedView bool

// ViewCheckLogEnv names a file that builds with the openstorage_viewcheck
// tag append each failed view check to before they panic. Serve can lose
// the panic (the API turns a handler panic into a 500), so the e2e harness
// in internal/githttp sets it and fails the run on any line (issue #503).
const ViewCheckLogEnv = "OPENSTORAGE_TEST_VIEWCHECK_LOG"

// reportViewCheck runs with the message of a failed view check, before the
// panic. s3walmeta_viewcheck.go writes it to ViewCheckLogEnv.
var reportViewCheck = func(string) {}

// checkViewIntact asserts that the shared view still equals a fresh decode
// of the cached body, so a viewDoc caller that wrote into it fails fast.
// It runs on every shared 304 read and before the view is discarded (a
// fresh read or our own write), so no write into the view goes unseen.
// Caller holds mu.
func (s *S3WAL) checkViewIntact() {
	if !checkSharedView || s.cacheView == nil {
		return
	}
	fresh, err := decodeS3WALMetaDoc(s.cacheBody)
	assertf(err == nil, "s3walmeta view check: cached body does not decode: %v", err)
	if !reflect.DeepEqual(fresh, s.cacheView) {
		msg := fmt.Sprintf("s3walmeta view check: a viewDoc caller mutated the shared view (seq %d)", s.cacheView.Seq)
		reportViewCheck(msg)
		assertf(false, "%s", msg)
	}
}

// adoptRead decodes a freshly fetched body, checks the seq watermark, and
// installs it as the new cache (plus the org-key snapshot — readDoc's 304
// path needs no install because the snapshot always matches the cache it
// revalidated). With shared, the decode also becomes the shared view;
// otherwise the caller may mutate it, so the view is cleared. Caller holds
// mu.
func (s *S3WAL) adoptRead(obj s3wal.Object, shared bool) (*s3walMetaDoc, string, error) {
	doc, err := decodeS3WALMetaDoc(obj.Body)
	if err != nil {
		if s.cacheETag != "" {
			// The cached object changed into bytes that do not decode
			// (issue #501). Latch like a vanish: a valid lineage that
			// replaces them later is not the cached document either.
			s.rolledBack = true
		}
		return nil, "", err
	}
	if err := s.observeDoc(doc, obj.Body); err != nil {
		return nil, "", err // keep the newer cache: nothing adopts the older body
	}
	s.checkViewIntact() // before the fresh read discards the view
	s.cacheBody, s.cacheETag, s.cacheView = obj.Body, obj.ETag, nil
	if shared {
		s.cacheView = doc
	}
	s.adoptOrgKeys(doc)
	return doc, obj.ETag, nil
}

// adoptOrgKeys installs the org public-key snapshot from a document this
// process just observed (fresh read, confirmed-missing object, or our own
// CAS success). Caller holds mu, so installs are serialized in
// observation order; the deep copy keeps the snapshot from aliasing
// document rows a mutation closure may still hold.
func (s *S3WAL) adoptOrgKeys(doc *s3walMetaDoc) {
	snap := make(map[string][]string, len(doc.Orgs))
	for name, org := range doc.Orgs {
		snap[name] = append([]string(nil), org.PublicKeys...)
	}
	s.orgKeysMu.Lock()
	s.orgKeys = snap
	s.orgKeysMu.Unlock()
}

// viewDoc is the read-only entry point: lock, fetch, return. The returned
// document is shared with other viewDoc callers (cacheView): read it, never
// write it.
func (s *S3WAL) viewDoc(ctx context.Context) (*s3walMetaDoc, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, _, err := s.readDocShared(ctx, true)
	return doc, err
}

// s3walMetaVerdict is a recovery predicate's answer for an ambiguous PUT
// whose writer nonce is not ours (plan §2.3): the write may still have
// landed and been superseded.
type s3walMetaVerdict int

const (
	// s3walMetaRerun re-evaluates the mutation on the fresh snapshot —
	// legal where re-applying is provably harmless (ID) or where fn
	// re-derives the correct outcome from the fresh state (PRED).
	s3walMetaRerun s3walMetaVerdict = iota
	// s3walMetaApplied concludes our write landed: fn's outputs stand.
	s3walMetaApplied
)

// s3walMetaMutation drives one read-mutate-CAS operation.
type s3walMetaMutation struct {
	op string
	// fn mutates doc in place, returning apply=false to finish without a
	// CAS (read-only verdicts, no-op retries) or an error to abort
	// (nothing was written). fn may run several times on fresh snapshots
	// and must reset its output closure variables at the top of every
	// run.
	fn func(doc *s3walMetaDoc) (apply bool, err error)
	// recover classifies a fresh post-ambiguity snapshot (nil =
	// s3walMetaRerun, the idempotent-re-apply fallback). There is no
	// fail-closed verdict here: unlike the ref quota ledger, every meta
	// mutation is ID or PRED (plan Appendix A — "low-rate CRUD is
	// ID/PRED"), so a rerun on the fresh snapshot is always sound.
	recover func(doc *s3walMetaDoc) s3walMetaVerdict
}

// mutate runs one read-mutate-CAS loop with the §2.3 recovery discipline,
// simplified from ref.S3WAL.mutate for a single object with no decorator
// above it (no IdempotentCAS recheck consumes a not-executed marker, so
// errors are returned plainly): 412 lost races re-read and re-evaluate
// under a full-jitter backoff; ambiguous PUTs re-read and resolve via
// writer nonce, then the per-method recovery class; provably-never-sent
// attempts retry.
func (s *S3WAL) mutate(ctx context.Context, mu s3walMetaMutation) (err error) {
	defer func() {
		// ADR-48: count terminal contention exhaustion at the return
		// boundary only (never per attempt).
		if errors.Is(err, s3wal.ErrCASContention) {
			metrics.S3WALContentionExhausted(metrics.ScopeMeta)
		}
	}()
	assertf(mu.fn != nil && mu.op != "", "mutate: incomplete mutation %+v", mu)
	s.mu.Lock()
	defer s.mu.Unlock()
	for attempt := 0; attempt < s.casAttempts; attempt++ {
		doc, etag, err := s.readDoc(ctx)
		if err != nil {
			return err
		}
		apply, err := mu.fn(doc)
		if err != nil {
			return err
		}
		if !apply {
			return nil
		}
		body, err := doc.encodeAdvance()
		if err != nil {
			return fmt.Errorf("s3walmeta %s: %w", mu.op, err)
		}
		ourNonce, ourSeq := doc.Writer, doc.Seq
		var newETag string
		var werr error
		if etag == "" {
			newETag, werr = s.cas.Create(ctx, s3walMetaKey, body)
		} else {
			newETag, werr = s.cas.Advance(ctx, s3walMetaKey, body, etag)
		}
		if werr == nil {
			// Internal invariant: ourSeq is one past a read that passed
			// observeDoc (or past our own last write).
			assertf(ourSeq > s.lastSeq && !s.rolledBack, "s3walmeta %s: wrote seq %d, not past observed seq %d (rolled back: %v)", mu.op, ourSeq, s.lastSeq, s.rolledBack)
			s.lastSeq = ourSeq
			s.checkViewIntact() // before our own write discards the view
			s.cacheBody, s.cacheETag, s.cacheView = body, newETag, nil
			// Our CAS landed: the mutated doc IS the latest observed
			// state, so a local key registration is visible to the very
			// next lookup (read-your-writes). The ambiguity paths below
			// need no install — they resolve through readDoc, whose
			// fresh reads adopt.
			s.adoptOrgKeys(doc)
			return nil
		}
		switch {
		case errors.Is(werr, s3wal.ErrExists), errors.Is(werr, s3wal.ErrLostRace):
			// Definitively not executed: someone else advanced first.
			if err := s.sleepJitter(ctx, attempt); err != nil {
				return fmt.Errorf("s3walmeta %s: %w", mu.op, err)
			}
			continue
		case errors.Is(werr, s3wal.ErrNotExecuted):
			continue // provably never sent — plain retry
		}
		if _, ok := errors.AsType[*s3wal.AmbiguousError](werr); !ok {
			// Definitive rejection (auth, bucket, exhausted contention):
			// the write did not take effect.
			return fmt.Errorf("s3walmeta %s: %w", mu.op, werr)
		}
		// Ambiguous: the write MAY have landed. Re-read and resolve.
		fresh, _, rerr := s.readDoc(ctx)
		if rerr != nil {
			// Recovery read failed: stay ambiguous (fail closed).
			return fmt.Errorf("s3walmeta %s: %w (recovery read failed: %v)", mu.op, werr, rerr)
		}
		if fresh.Writer == ourNonce {
			if fresh.Seq != ourSeq {
				// Our 128-bit nonce at another seq: only an out-of-band
				// edit produces that (issue #481).
				// The recovery read already cached that body: quarantine
				// it so no later operation reads or writes through it.
				s.quarantined = &CorruptDocumentError{Err: fmt.Errorf("document seq %d carries this process's writer nonce from seq %d", fresh.Seq, ourSeq)}
				return s.quarantined
			}
			return nil // our write landed and is the latest
		}
		verdict := s3walMetaRerun
		if mu.recover != nil {
			verdict = mu.recover(fresh)
		}
		if verdict == s3walMetaApplied {
			return nil
		}
		// Re-run fn on a fresh snapshot.
	}
	return fmt.Errorf("s3walmeta %s: %d attempts: %w", mu.op, s.casAttempts, s3wal.ErrCASContention)
}

// sleepJitter blocks for the §2.3 full-jitter lost-race backoff.
func (s *S3WAL) sleepJitter(ctx context.Context, attempt int) error {
	return sleepFullJitter(ctx, attempt, s.backoffBase, s.backoffCap)
}

// sleepFullJitter blocks for a full-jitter backoff of at most
// min(capDur, base<<attempt).
func sleepFullJitter(ctx context.Context, attempt int, base, capDur time.Duration) error {
	assertf(attempt >= 0 && attempt < 63, "sleepJitter: attempt %d out of range", attempt)
	ceiling := min(capDur, base<<uint(attempt))
	assertf(ceiling > 0, "sleepJitter: non-positive backoff ceiling %v", ceiling)
	timer := time.NewTimer(mrand.N(ceiling))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// lookupOrg resolves an org name to its row; ErrNotFound for unknown orgs
// (invalid-UTF-8 names are clean misses — no document can contain them).
func lookupOrg(doc *s3walMetaDoc, name string) (*s3walMetaOrg, error) {
	assertf(name != "", "lookupOrg: empty org name")
	org := doc.Orgs[name]
	if org == nil {
		return nil, fmt.Errorf("org %q: %w", name, ErrNotFound)
	}
	assertf(org.ID > 0, "org %q resolved to non-positive id %d", name, org.ID)
	return org, nil
}

// toRepo renders a stored repo row as the interface type.
func (r *s3walMetaRepo) toRepo(repoID string) Repo {
	repo := Repo{
		RepoID:        repoID,
		OrgID:         r.OrgID,
		URL:           r.URL,
		DefaultBranch: r.DefaultBranch,
		CreatedAt:     time.Unix(0, r.CreatedAt).UTC(),
		CreateState:   knownCreateState(repoID, CreateState(r.CreateState)),
	}
	if r.DeletedAt != 0 {
		utc := time.Unix(0, r.DeletedAt).UTC()
		repo.DeletedAt = &utc
	}
	return repo
}

// UpsertOrgWithKeys idempotently creates the org and replaces its
// registered PEM public keys. Recovery class ID: a re-run on a fresh
// snapshot re-applies the same terminal state, and the ID mint is
// re-derived per run (an org our landed write created is found, never
// re-minted).
func (s *S3WAL) UpsertOrgWithKeys(ctx context.Context, name string, pemPublicKeys []string) (Org, error) {
	assertf(name != "", "UpsertOrgWithKeys: empty org name")
	assertf(len(pemPublicKeys) > 0, "UpsertOrgWithKeys: org %q needs at least one public key", name)
	for i, pem := range pemPublicKeys {
		assertf(strings.TrimSpace(pem) != "", "UpsertOrgWithKeys: blank PEM block %d for org %q", i, name)
		if err := utf8OK("public key", pem); err != nil {
			return Org{}, fmt.Errorf("upsert org %q: %w", name, err)
		}
	}
	if err := utf8OK("org name", name); err != nil {
		return Org{}, fmt.Errorf("upsert org: %w", err)
	}
	nanos, _ := s.nowNano()
	var org Org
	err := s.mutate(ctx, s3walMetaMutation{
		op: "upsert org",
		fn: func(doc *s3walMetaDoc) (bool, error) {
			row := doc.Orgs[name]
			if row == nil {
				if doc.NextOrgID == math.MaxInt64 {
					return false, errCounterExhausted("next_org_id", doc.NextOrgID)
				}
				doc.NextOrgID++
				row = &s3walMetaOrg{ID: doc.NextOrgID, CreatedAt: nanos}
				if doc.Orgs == nil {
					doc.Orgs = make(map[string]*s3walMetaOrg)
				}
				doc.Orgs[name] = row
			}
			row.PublicKeys = append([]string(nil), pemPublicKeys...)
			org = Org{ID: row.ID, Name: name, CreatedAt: time.Unix(0, row.CreatedAt).UTC()}
			return true, nil
		},
	})
	if err != nil {
		return Org{}, fmt.Errorf("upsert org %q: %w", name, err)
	}
	return org, nil
}

// PublicKeysForOrg returns the org's registered PEM public key blocks in
// registration order — served from the in-memory snapshot, never a
// synchronous store read (PR #168 security P2; see the orgKeys field
// comment for the flood argument). Unknown orgs — including flooded
// unverified iss values — answer ErrNotFound from memory. Freshness:
// same-process writes are visible immediately (mutate installs the
// snapshot before returning); foreign writes within
// s3walKeyRefreshInterval. Cold start (nil snapshot) blocks on one real
// read: production serving populates the snapshot during bootstrap
// (UpsertOrgWithKeys runs before the listener starts), and Open must stay
// I/O-free (ADR-12), so this path only serves CLI/hook processes.
func (s *S3WAL) PublicKeysForOrg(ctx context.Context, name string) ([]string, error) {
	assertf(name != "", "PublicKeysForOrg: empty org name")
	s.orgKeysMu.RLock()
	snap := s.orgKeys
	s.orgKeysMu.RUnlock()
	if snap == nil {
		doc, err := s.viewDoc(ctx) // populates the snapshot via adoptOrgKeys
		if err != nil {
			return nil, err
		}
		org, err := lookupOrg(doc, name)
		if err != nil {
			return nil, err
		}
		return append([]string(nil), org.PublicKeys...), nil
	}
	keys, ok := snap[name]
	if !ok {
		return nil, fmt.Errorf("org %q: %w", name, ErrNotFound)
	}
	return append([]string(nil), keys...), nil
}

// OrgNames returns every org row's name, ordered by name.
func (s *S3WAL) OrgNames(ctx context.Context) ([]string, error) {
	doc, err := s.viewDoc(ctx)
	if err != nil {
		return nil, err
	}
	var names []string
	for name := range doc.Orgs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// CreateRepo inserts a live repo with a server-generated RepoID.
// ErrAlreadyExists when (org, url) is taken — even by a soft-deleted or
// purged row (the burned-name invariant). Recovery class PRED: the
// per-run RepoID is random and never reused, so its presence on the fresh
// snapshot proves OUR insert landed.
func (s *S3WAL) CreateRepo(ctx context.Context, org, url, defaultBranch string) (Repo, error) {
	return s.createRepo(ctx, org, url, defaultBranch, CreateStateLegacy)
}

// createRepo inserts a repo row with the given create state (ADR-51 v2
// field; CreateStateLegacy stores nothing).
func (s *S3WAL) createRepo(ctx context.Context, org, url, defaultBranch string, state CreateState) (Repo, error) {
	assertf(url != "", "CreateRepo: empty url for org %q", org)
	assertf(defaultBranch != "", "CreateRepo: empty default branch for %q (caller defaults to main)", url)
	for what, v := range map[string]string{"url": url, "default branch": defaultBranch} {
		if err := utf8OK(what, v); err != nil {
			return Repo{}, fmt.Errorf("create repo %q in org %q: %w", url, org, err)
		}
	}
	nanos, createdAt := s.nowNano()
	var repo Repo
	var repoID string
	err := s.mutate(ctx, s3walMetaMutation{
		op: "create repo",
		fn: func(doc *s3walMetaDoc) (bool, error) {
			orgRow, err := lookupOrg(doc, org)
			if err != nil {
				return false, err
			}
			for _, r := range doc.Repos {
				if r.OrgID == orgRow.ID && r.URL == url {
					return false, fmt.Errorf("repo %q in org %q: %w", url, org, ErrAlreadyExists)
				}
			}
			repoID = newRepoID()
			if doc.Repos == nil {
				doc.Repos = make(map[string]*s3walMetaRepo)
			}
			doc.Repos[repoID] = &s3walMetaRepo{OrgID: orgRow.ID, URL: url, DefaultBranch: defaultBranch, CreatedAt: nanos, CreateState: int(state)}
			repo = Repo{RepoID: repoID, OrgID: orgRow.ID, URL: url, DefaultBranch: defaultBranch, CreatedAt: createdAt, CreateState: state}
			return true, nil
		},
		recover: func(doc *s3walMetaDoc) s3walMetaVerdict {
			if repoID != "" && doc.Repos[repoID] != nil {
				return s3walMetaApplied
			}
			return s3walMetaRerun
		},
	})
	if err != nil {
		return Repo{}, err
	}
	return repo, nil
}

// liveRepo returns the live repo row for (orgID, url); nil when absent or
// soft-deleted.
func (doc *s3walMetaDoc) liveRepoByURL(orgID int64, url string) (string, *s3walMetaRepo) {
	for id, r := range doc.Repos {
		if r.OrgID == orgID && r.URL == url && r.DeletedAt == 0 {
			return id, r
		}
	}
	return "", nil
}

// GetRepoByURL returns the live repo with the given url; ErrNotFound when
// absent or soft-deleted.
func (s *S3WAL) GetRepoByURL(ctx context.Context, org, url string) (Repo, error) {
	assertf(url != "", "GetRepoByURL: empty url for org %q", org)
	doc, err := s.viewDoc(ctx)
	if err != nil {
		return Repo{}, err
	}
	orgRow, err := lookupOrg(doc, org)
	if err != nil {
		return Repo{}, err
	}
	id, r := doc.liveRepoByURL(orgRow.ID, url)
	if r == nil {
		return Repo{}, fmt.Errorf("repo %q in org %q: %w", url, org, ErrNotFound)
	}
	return r.toRepo(id), nil
}

// GetRepoByID returns the live repo with the given RepoID; ErrNotFound
// when absent or soft-deleted.
func (s *S3WAL) GetRepoByID(ctx context.Context, org, repoID string) (Repo, error) {
	assertf(repoID != "", "GetRepoByID: empty repoID for org %q", org)
	doc, err := s.viewDoc(ctx)
	if err != nil {
		return Repo{}, err
	}
	orgRow, err := lookupOrg(doc, org)
	if err != nil {
		return Repo{}, err
	}
	r := doc.Repos[repoID]
	if r == nil || r.OrgID != orgRow.ID || r.DeletedAt != 0 {
		return Repo{}, fmt.Errorf("repo id %q in org %q: %w", repoID, org, ErrNotFound)
	}
	return r.toRepo(repoID), nil
}

// CreateRepoWithState is CreateRepo with an explicit create state, stored
// in the v2 create_state field (ADR-51).
func (s *S3WAL) CreateRepoWithState(ctx context.Context, org, url, defaultBranch string, state CreateState) (Repo, error) {
	assertf(state == CreateStatePending || state == CreateStateComplete, "CreateRepoWithState: state %d", state)
	return s.createRepo(ctx, org, url, defaultBranch, state)
}

// CompleteRepoCreate marks a pending row complete (Store contract, issue
// #388). Recovery class PRED: an ambiguous PUT whose write is still the
// latest answers CreateCompleted (writer nonce); one that landed and was
// superseded reruns and answers CreateAlreadyComplete (or CreateDeleted),
// which callers treat like a lost race against a concurrent completion.
func (s *S3WAL) CompleteRepoCreate(ctx context.Context, org, repoID string) (CreateOutcome, error) {
	assertf(repoID != "", "CompleteRepoCreate: empty repo id for org %q", org)
	var out CreateOutcome
	err := s.mutate(ctx, s3walMetaMutation{
		op: "complete repo create",
		fn: func(doc *s3walMetaDoc) (bool, error) {
			out = 0
			orgRow, err := lookupOrg(doc, org)
			if err != nil {
				return false, err
			}
			r := doc.Repos[repoID]
			if r == nil || r.OrgID != orgRow.ID {
				return false, fmt.Errorf("repo id %q in org %q: %w", repoID, org, ErrNotFound)
			}
			if r.DeletedAt != 0 || CreateState(r.CreateState) != CreateStatePending {
				out, err = completeOutcome(r.toRepo(repoID))
				return false, err
			}
			r.CreateState = int(CreateStateComplete)
			out = CreateCompleted
			return true, nil
		},
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

// UpdateRepoDefaultBranch CASes the live repo's default branch (Store
// contract). Recovery class PRED on the semantic effect: branch == new
// proves the swap holds (an ambiguous PUT superseded by a competitor that
// wrote the SAME transition is reported won=true although SQL would grant
// exactly one of the two — an accepted divergence: the terminal state is
// identical and the corner requires a lost response).
func (s *S3WAL) UpdateRepoDefaultBranch(ctx context.Context, org, url, oldBranch, newBranch string) (bool, error) {
	assertf(url != "", "UpdateRepoDefaultBranch: empty url for org %q", org)
	assertf(oldBranch != "" && newBranch != "",
		"UpdateRepoDefaultBranch: empty branch for %q (old %q, new %q)", url, oldBranch, newBranch)
	if err := utf8OK("default branch", newBranch); err != nil {
		return false, fmt.Errorf("update default branch of %q in org %q: %w", url, org, err)
	}
	won := false
	err := s.mutate(ctx, s3walMetaMutation{
		op: "update default branch",
		fn: func(doc *s3walMetaDoc) (bool, error) {
			won = false
			orgRow, err := lookupOrg(doc, org)
			if err != nil {
				return false, err
			}
			_, r := doc.liveRepoByURL(orgRow.ID, url)
			if r == nil {
				return false, fmt.Errorf("repo %q in org %q: %w", url, org, ErrNotFound)
			}
			if r.DefaultBranch != oldBranch {
				return false, nil // changed concurrently: won=false, no error
			}
			r.DefaultBranch = newBranch
			won = true
			return true, nil
		},
		recover: func(doc *s3walMetaDoc) s3walMetaVerdict {
			orgRow := doc.Orgs[org]
			if orgRow == nil {
				return s3walMetaRerun
			}
			if _, r := doc.liveRepoByURL(orgRow.ID, url); r != nil && r.DefaultBranch == newBranch {
				won = true
				return s3walMetaApplied
			}
			return s3walMetaRerun
		},
	})
	if err != nil {
		return false, err
	}
	return won, nil
}

// SoftDeleteRepo sets the deletion stamp and returns the affected repo.
// ErrNotFound when no row exists; ErrAlreadyDeleted when already
// soft-deleted. Recovery class PRED: the per-run stamp proves OUR delete
// landed; a row deleted under a different stamp re-runs into the
// ErrAlreadyDeleted classification.
func (s *S3WAL) SoftDeleteRepo(ctx context.Context, org, url string) (Repo, error) {
	assertf(url != "", "SoftDeleteRepo: empty url for org %q", org)
	nanos, _ := s.nowNano()
	var repo Repo
	err := s.mutate(ctx, s3walMetaMutation{
		op: "soft delete repo",
		fn: func(doc *s3walMetaDoc) (bool, error) {
			orgRow, err := lookupOrg(doc, org)
			if err != nil {
				return false, err
			}
			var id string
			var row *s3walMetaRepo
			for rid, r := range doc.Repos {
				if r.OrgID == orgRow.ID && r.URL == url {
					id, row = rid, r
					break
				}
			}
			if row == nil {
				return false, fmt.Errorf("repo %q in org %q: %w", url, org, ErrNotFound)
			}
			if row.DeletedAt != 0 {
				return false, fmt.Errorf("repo %q in org %q: %w", url, org, ErrAlreadyDeleted)
			}
			row.DeletedAt = nanos
			repo = row.toRepo(id)
			return true, nil
		},
		recover: func(doc *s3walMetaDoc) s3walMetaVerdict {
			orgRow := doc.Orgs[org]
			if orgRow == nil {
				return s3walMetaRerun
			}
			for _, r := range doc.Repos {
				if r.OrgID == orgRow.ID && r.URL == url && r.DeletedAt == nanos {
					return s3walMetaApplied
				}
			}
			return s3walMetaRerun
		},
	})
	if err != nil {
		return Repo{}, err
	}
	assertf(repo.DeletedAt != nil, "soft delete of %q returned nil deleted_at", repo.RepoID)
	return repo, nil
}

// ListRepos pages live repos for the org, keyset-ordered by
// (created_at DESC, repo_id DESC) with the shared opaque cursor
// (cursor.go). The Q filter is a literal case-insensitive substring
// match; folding is Unicode-wide here (strings.ToLower) where SQLite
// folds ASCII only and PG folds per locale — an accepted divergence of
// the same kind the SQL twins already document against each other.
func (s *S3WAL) ListRepos(ctx context.Context, org string, params ListParams) (ListResult, error) {
	assertf(params.Limit >= 1 && params.Limit <= 100,
		"ListRepos: limit %d outside contract range [1,100]", params.Limit)
	doc, err := s.viewDoc(ctx)
	if err != nil {
		return ListResult{}, err
	}
	orgRow, err := lookupOrg(doc, org)
	if err != nil {
		return ListResult{}, err
	}
	var cursorNanos int64
	var cursorID string
	if params.Cursor != "" {
		createdAt, repoID, err := decodeCursor(params.Cursor)
		if err != nil {
			return ListResult{}, err
		}
		cursorNanos, cursorID = createdAt.UnixNano(), repoID
	}
	q := strings.ToLower(params.Q)
	rows := make([]Repo, 0, params.Limit+1)
	for id, r := range doc.Repos {
		if r.OrgID != orgRow.ID || r.DeletedAt != 0 {
			continue
		}
		if q != "" && !strings.Contains(strings.ToLower(r.URL), q) {
			continue
		}
		if params.Cursor != "" {
			// Keyset predicate: (created_at, repo_id) < (cursor).
			if r.CreatedAt > cursorNanos || (r.CreatedAt == cursorNanos && id >= cursorID) {
				continue
			}
		}
		rows = append(rows, r.toRepo(id))
	}
	sort.Slice(rows, func(i, j int) bool {
		if !rows[i].CreatedAt.Equal(rows[j].CreatedAt) {
			return rows[i].CreatedAt.After(rows[j].CreatedAt)
		}
		return rows[i].RepoID > rows[j].RepoID
	})
	res := ListResult{Repos: rows}
	if len(rows) > params.Limit {
		res.Repos = rows[:params.Limit]
		last := res.Repos[params.Limit-1]
		res.NextCursor = encodeCursor(last.CreatedAt, last.RepoID)
		res.HasMore = true
	}
	return res, nil
}

// AddSigningKey registers a commit-signing key for the org and returns
// the stored row. Recovery class PRED: the per-run (ID, key text, stamp)
// triple identifies OUR insert on the fresh snapshot (IDs are minted from
// the monotonic counter and never reused).
func (s *S3WAL) AddSigningKey(ctx context.Context, org, keyText string) (SigningKey, error) {
	assertf(strings.TrimSpace(keyText) != "", "AddSigningKey: blank key text for org %q", org)
	if err := utf8OK("key text", keyText); err != nil {
		return SigningKey{}, fmt.Errorf("add signing key for org %q: %w", org, err)
	}
	nanos, createdAt := s.nowNano()
	var key SigningKey
	err := s.mutate(ctx, s3walMetaMutation{
		op: "add signing key",
		fn: func(doc *s3walMetaDoc) (bool, error) {
			orgRow, err := lookupOrg(doc, org)
			if err != nil {
				return false, err
			}
			if doc.NextKeyID == math.MaxInt64 {
				return false, errCounterExhausted("next_key_id", doc.NextKeyID)
			}
			doc.NextKeyID++
			orgRow.SigningKeys = append(orgRow.SigningKeys, s3walMetaSigningKey{ID: doc.NextKeyID, KeyText: keyText, CreatedAt: nanos})
			key = SigningKey{ID: doc.NextKeyID, OrgID: orgRow.ID, KeyText: keyText, CreatedAt: createdAt}
			return true, nil
		},
		recover: func(doc *s3walMetaDoc) s3walMetaVerdict {
			orgRow := doc.Orgs[org]
			if orgRow == nil {
				return s3walMetaRerun
			}
			for _, k := range orgRow.SigningKeys {
				if k.ID == key.ID && k.KeyText == keyText && k.CreatedAt == nanos {
					return s3walMetaApplied
				}
			}
			return s3walMetaRerun
		},
	})
	if err != nil {
		return SigningKey{}, err
	}
	assertf(key.ID > 0, "signing key insert returned non-positive id %d", key.ID)
	return key, nil
}

// SigningKeysForOrg returns the org's registered signing keys in
// registration order; empty slice when none are registered.
func (s *S3WAL) SigningKeysForOrg(ctx context.Context, org string) ([]SigningKey, error) {
	doc, err := s.viewDoc(ctx)
	if err != nil {
		return nil, err
	}
	orgRow, err := lookupOrg(doc, org)
	if err != nil {
		return nil, err
	}
	keys := []SigningKey{}
	for _, k := range orgRow.SigningKeys {
		keys = append(keys, SigningKey{ID: k.ID, OrgID: orgRow.ID, KeyText: k.KeyText, CreatedAt: time.Unix(0, k.CreatedAt).UTC()})
	}
	return keys, nil
}

// RemoveSigningKey deletes the signing key with the given ID. ErrNotFound
// when the org is unknown or the ID does not belong to it. Recovery class
// PRED on the semantic effect: IDs are never reused, so an absent ID
// proves the deletion holds.
func (s *S3WAL) RemoveSigningKey(ctx context.Context, org string, id int64) error {
	assertf(id > 0, "RemoveSigningKey: non-positive id %d for org %q", id, org)
	return s.mutate(ctx, s3walMetaMutation{
		op: "remove signing key",
		fn: func(doc *s3walMetaDoc) (bool, error) {
			orgRow, err := lookupOrg(doc, org)
			if err != nil {
				return false, err
			}
			for i, k := range orgRow.SigningKeys {
				if k.ID == id {
					orgRow.SigningKeys = append(orgRow.SigningKeys[:i:i], orgRow.SigningKeys[i+1:]...)
					return true, nil
				}
			}
			return false, fmt.Errorf("signing key %d for org %q: %w", id, org, ErrNotFound)
		},
		recover: func(doc *s3walMetaDoc) s3walMetaVerdict {
			orgRow := doc.Orgs[org]
			if orgRow == nil {
				return s3walMetaRerun
			}
			for _, k := range orgRow.SigningKeys {
				if k.ID == id {
					return s3walMetaRerun
				}
			}
			return s3walMetaApplied
		},
	})
}
