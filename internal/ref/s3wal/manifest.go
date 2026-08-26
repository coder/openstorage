// Manifest schema v1 (plan §2.2) plus the §2.4 manifest logical clock and
// the §2.9 key-alphabet validators. One JSON manifest object holds the
// ENTIRE authority state of one base repo group {base, base+ephemeral,
// base+import}: refs, packs, retained tombstones, generations, quota
// ledger, reservations (incl. adoption claims and absence covers), LFS
// registry, delete leases, purge fence, and sync-claim token — so every
// audited multi-entity transaction is a single-object CAS.
package s3wal

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
	"unicode/utf8"
)

// ManifestVersion is the only schema version this binary reads or writes.
const ManifestVersion = 1

// OpsRingSize bounds the manifest's recent-mutation op-ID ring (plan §2.3,
// K=64): ambiguous retries of NON-idempotent ops test membership here
// instead of blindly re-applying. Ring eviction cannot forge a
// "not applied" verdict for a live entry because the op-ID is ALSO
// recorded on the touched reservation entry itself (Reservation.OpID),
// cleared with the entry.
const OpsRingSize = 64

// RepoPrefix is the bucket prefix manifests live under. Keys carry NO file
// extension: a suffix like ".json" would corrupt listing order between
// prefix-related repo IDs ("abc-1.json" sorts before "abc.json" although
// abc < abc-1) — plan §2.1/§2.9.
const RepoPrefix = "_wal/repo/"

// Manifest is the §2.2 v1 schema — the whole repo group's authority state.
type Manifest struct {
	V int `json:"v"`
	// Seq is strictly monotonic, bumped by every CAS. Together with the
	// fresh Writer nonce it guarantees distinct bytes per logical version
	// ⇒ distinct ETag (kills ETag-ABA, §5-R4).
	Seq int64 `json:"seq"`
	// Writer is the 128-bit random nonce of the LAST writer, fresh per
	// write — the ambiguous-PUT recovery identity check (§2.3).
	Writer string `json:"writer"`
	// Ops is the bounded ring (oldest first) of recent mutation op-IDs.
	Ops []string `json:"ops,omitempty"`
	// Clk is the manifest logical clock: every CAS writes
	// max(localNow, clk_prev + 1ms) — monotone across restarts and
	// backward local-clock jumps (§2.4). Every stamp stored in the
	// manifest uses this write-time value.
	Clk time.Time `json:"clk"`
	// PurgedAt non-nil is the terminal purge fence + tombstone stamp: the
	// manifest object is NEVER deleted (burned-name invariant).
	PurgedAt *time.Time `json:"purged_at,omitempty"`
	// Members holds per-repo state, keyed by full repo key (base,
	// base+ephemeral, base+import).
	Members map[string]*Member `json:"members,omitempty"`
	// UsageBytes is the group's pack_usage counter.
	UsageBytes int64 `json:"usage_bytes"`
	// Reservations is the pack_reservations table: in-flight admission
	// rows keyed by blob key, plus /adopt-claim entries and absence
	// covers.
	Reservations map[string]*Reservation `json:"reservations,omitempty"`
	// DeleteLocks holds the short-TTL delete leases (§2.5 W6).
	DeleteLocks map[string]*DeleteLock `json:"delete_locks,omitempty"`
	// LFS is the group's LFS registry (keyed by the BASE repo — ADR-35).
	LFS LFSState `json:"lfs"`
	// SyncClaim is the upstream-sync claim token (§2.8). Phase 1b only
	// READS it (the CASRef fence); the write-through seam is Phase 3.
	SyncClaim *SyncClaim `json:"sync_claim,omitempty"`
}

// Member is one repo's refs/packs/retained state plus its mutation
// generation (driver-bumped per changed row, replacing the SQL triggers).
type Member struct {
	Gen      int64                   `json:"gen"`
	Refs     map[string]RefRecord    `json:"refs,omitempty"`
	Packs    []PackRecord            `json:"packs,omitempty"`
	Retained map[string]RetainedPack `json:"retained,omitempty"`
}

// RefRecord is one ref row. CreatedAt is set on create, preserved across
// SHA updates, and reset by delete+recreate — the branches[].created_at
// contract.
type RefRecord struct {
	SHA       string    `json:"sha"`
	CreatedAt time.Time `json:"created_at"`
}

// PackRecord is one registered pack row (bytes persisted — absence scans
// and ListFleetPacks need them).
type PackRecord struct {
	ID        string    `json:"id"`
	Key       string    `json:"key"`
	Bytes     int64     `json:"bytes"`
	CreatedAt time.Time `json:"created_at"`
}

// RetainedPack is one superseded-blob tombstone (pack_retained row).
type RetainedPack struct {
	Bytes        int64     `json:"bytes"`
	SupersededAt time.Time `json:"superseded_at"`
}

// Reservation is one pack_reservations row. OpID records the last
// non-idempotent mutation that touched this entry (§2.3 OPS recovery); it
// is cleared with the entry, so ops-ring eviction cannot forge a
// "not applied" verdict while the entry lives.
type Reservation struct {
	Owner     string    `json:"owner"`
	Bytes     int64     `json:"bytes"`
	Attempts  int64     `json:"attempts"`
	Gen       string    `json:"gen"`
	CreatedAt time.Time `json:"created_at"`
	OpID      string    `json:"op_id,omitempty"`
}

// DeleteLock is one §2.5 W6 delete lease.
type DeleteLock struct {
	Owner     string    `json:"owner"`
	OpID      string    `json:"op_id"`
	CreatedAt time.Time `json:"created_at"`
}

// LFSState is the group's LFS registry: maintained count, object rows,
// mismatch markers, and the adoption generation.
type LFSState struct {
	Count       int64                 `json:"count"`
	Objects     map[string]*LFSObject `json:"objects,omitempty"`
	Mismatches  map[string]string     `json:"mismatches,omitempty"`
	AdoptionGen int64                 `json:"adoption_gen"`
}

// LFSObject is one lfs_objects row.
type LFSObject struct {
	Bytes             int64      `json:"bytes"`
	CreatedAt         time.Time  `json:"created_at"`
	UnreferencedSince *time.Time `json:"unreferenced_since,omitempty"`
}

// SyncClaim is the §2.8 sync-claim token.
type SyncClaim struct {
	Holder    string    `json:"holder"`
	CreatedAt time.Time `json:"created_at"`
}

// NewManifest returns the empty (never-written) manifest state: Seq 0, so
// the first EncodeAdvance writes seq 1 through the If-None-Match: * create
// path. A missing manifest object and this value are interchangeable.
func NewManifest() *Manifest {
	return &Manifest{V: ManifestVersion}
}

// DecodeManifest parses and validates one fetched manifest body.
// Corruption is a crash-and-burn error, never a silent empty state: a
// manifest that exists but cannot be trusted must stop the writer.
func DecodeManifest(body []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, fmt.Errorf("s3wal: manifest decode: %w", err)
	}
	if m.V != ManifestVersion {
		return nil, fmt.Errorf("s3wal: manifest version %d unsupported (want %d)", m.V, ManifestVersion)
	}
	if m.Seq < 1 {
		return nil, fmt.Errorf("s3wal: manifest seq %d invalid (stored manifests start at 1)", m.Seq)
	}
	if m.Writer == "" {
		return nil, fmt.Errorf("s3wal: manifest seq %d has no writer nonce", m.Seq)
	}
	if m.Clk.IsZero() {
		return nil, fmt.Errorf("s3wal: manifest seq %d has no logical clock", m.Seq)
	}
	assertf(m.UsageBytes >= 0, "manifest seq %d: negative usage %d", m.Seq, m.UsageBytes)
	assertf(m.LFS.Count >= 0, "manifest seq %d: negative lfs count %d", m.Seq, m.LFS.Count)
	for repo, mem := range m.Members {
		assertf(mem != nil, "manifest seq %d: nil member %q", m.Seq, repo)
		assertf(mem.Gen >= 0, "manifest seq %d: member %q negative gen %d", m.Seq, repo, mem.Gen)
	}
	return &m, nil
}

// EncodeAdvance encodes the NEXT version of m: seq+1, a fresh 128-bit
// writer nonce (distinct bytes per write ⇒ distinct ETag — §5-R4), and the
// given write-time clock, which the caller derived via ClkNext from the
// SAME snapshot so in-body stamps and the manifest clock agree. m is
// updated in place so the caller can read the minted Writer nonce for
// ambiguous-PUT recovery.
func (m *Manifest) EncodeAdvance(clk time.Time) ([]byte, error) {
	assertf(m.V == ManifestVersion, "EncodeAdvance: version %d", m.V)
	assertf(m.Seq >= 0, "EncodeAdvance: seq %d", m.Seq)
	assertf(clk.After(m.Clk), "EncodeAdvance: clock %v does not advance past %v", clk, m.Clk)
	assertf(len(m.Ops) <= OpsRingSize, "EncodeAdvance: ops ring overflow (%d > %d)", len(m.Ops), OpsRingSize)
	m.assertKeysUTF8()
	m.Seq++
	m.Writer = newHexNonce()
	m.Clk = clk
	body, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("s3wal: manifest encode: %w", err)
	}
	return body, nil
}

// assertKeysUTF8 is the codec-boundary defense against key aliasing
// (Codex round 4 on PR #156, security): encoding/json does not REJECT an
// invalid-UTF-8 map key — it silently rewrites the invalid bytes to
// U+FFFD on encode, so two distinct in-memory keys (a raw-0xff ref name
// and its U+FFFD spelling) collapse into DUPLICATE JSON keys, and decode
// keeps the LAST one: a silent overwrite of whichever row the spelling
// collision targets. Every writer path must reject such keys before
// mutating (CASRef does — ref names are the only caller-controlled raw
// byte strings); this assert crashes-and-burns on any missed path
// instead of ever encoding a lossy manifest.
func (m *Manifest) assertKeysUTF8() {
	check := func(kind, key string) {
		assertf(utf8.ValidString(key), "EncodeAdvance: %s key %q is not valid UTF-8 — JSON encoding would alias it onto its U+FFFD spelling", kind, key)
	}
	for repo, mem := range m.Members {
		check("member", repo)
		for name := range mem.Refs {
			check("ref", name)
		}
		for key := range mem.Retained {
			check("retained", key)
		}
	}
	for key := range m.Reservations {
		check("reservation", key)
	}
	for key := range m.LFS.Objects {
		check("lfs object", key)
	}
	for key := range m.LFS.Mismatches {
		check("lfs mismatch", key)
	}
	for key := range m.DeleteLocks {
		check("delete lock", key)
	}
}

// RecordOp appends a non-idempotent mutation's op-ID to the bounded ring,
// evicting the oldest entry past OpsRingSize.
func (m *Manifest) RecordOp(opID string) {
	assertf(opID != "", "RecordOp: empty op id")
	m.Ops = append(m.Ops, opID)
	if len(m.Ops) > OpsRingSize {
		m.Ops = append([]string(nil), m.Ops[len(m.Ops)-OpsRingSize:]...)
	}
}

// OpApplied reports whether opID is in the ops ring OR recorded on a live
// reservation entry — the §2.3 membership test for ambiguous retries of
// non-idempotent ops. Absence is NOT proof of non-application (the ring is
// bounded); callers fail closed when this and the semantic predicate are
// both inconclusive.
func (m *Manifest) OpApplied(opID string) bool {
	assertf(opID != "", "OpApplied: empty op id")
	for _, id := range m.Ops {
		if id == opID {
			return true
		}
	}
	for _, r := range m.Reservations {
		if r != nil && r.OpID == opID {
			return true
		}
	}
	return false
}

// Member returns the repo's member state, or nil when the repo has none.
func (m *Manifest) Member(repo string) *Member {
	return m.Members[repo]
}

// EnsureMember returns the repo's member state, creating it on first
// mutation.
func (m *Manifest) EnsureMember(repo string) *Member {
	assertf(repo != "", "EnsureMember: empty repo")
	if m.Members == nil {
		m.Members = make(map[string]*Member)
	}
	mem := m.Members[repo]
	if mem == nil {
		mem = &Member{}
		m.Members[repo] = mem
	}
	return mem
}

// DecodeSeq is the Client.DecodeSeq hook for manifest bodies: it arms the
// §2.3 read-side stale-read assert (and the write-side watermark raise) on
// every conditional manifest operation.
func DecodeSeq(body []byte) (int64, bool) {
	var probe struct {
		V   int   `json:"v"`
		Seq int64 `json:"seq"`
	}
	if err := json.Unmarshal(body, &probe); err != nil || probe.V != ManifestVersion || probe.Seq < 1 {
		return 0, false
	}
	return probe.Seq, true
}

// ClkNext derives the write-time manifest clock: max(localNow, prev+1ms),
// monotone across restarts and immune to backward local jumps (§2.4).
func ClkNext(localNow, prev time.Time) time.Time {
	assertf(!localNow.IsZero(), "ClkNext: zero local time")
	if next := prev.Add(time.Millisecond); next.After(localNow) {
		return next
	}
	return localNow.UTC()
}

// Elapsed computes the §2.4 elapsed-time rule for TTL/grace comparisons:
// min(localNow, clkCandidate) − stamp with clkCandidate = max(localNow,
// clk+1ms) — a backward local-clock jump (localNow < clk) charges only the
// local clock's smaller reading, so destruction is only ever DELAYED by
// clock trouble, never accelerated. Because clkCandidate >= localNow by
// construction, the reduction always equals localNow − stamp: a FORWARD
// local-clock jump inflates this reading, and it is SkewOK — not this
// helper — that fails destructive verdicts closed against it.
func Elapsed(localNow, clk, stamp time.Time) time.Duration {
	assertf(!localNow.IsZero(), "Elapsed: zero local time")
	cand := ClkNext(localNow, clk)
	at := localNow
	if cand.Before(at) {
		at = cand
	}
	return at.Sub(stamp)
}

// SkewOK is the §2.4 fail-closed skew guard for destructive/steal checks:
// false — expiry verdicts demote to report-only (log loudly, never expire
// early) — on either of two clock-trouble evidences, both bounded by
// min(15min, ttl/4):
//
//   - the manifest clock runs AHEAD of the local clock by more than the
//     bound: some writer's clock (ours or a previous one) is grossly
//     wrong. This comparison stays one-sided: a manifest clock is
//     last-WRITE time, so a merely OLD clock (quiet repo) is normal
//     aging, not skew — §2.4's literal |localNow − clk| reading would
//     demote every quiet-repo expiry forever (a deliberate, documented
//     deviation from that wording).
//   - the process wall clock has drifted from its monotonic clock by
//     more than the bound (wallDrift, either sign): a local-clock jump
//     during this process's lifetime — exactly the window in which a
//     forward jump could otherwise expire the leases of live in-process
//     holders (guarded deletes, adoption claims, reservations) early.
//
// Jumps within the bound are acceptable: the conservative grace floors
// (§2.4-4) absorb them as noise. Wall jumps predating this process are
// invisible to both arms — an AT-BOOT-wrong clock reports ~zero drift
// and an old manifest clock reads as a quiet repo — so destructive
// verdicts additionally cross-check the §2.4-3 store-Date witness
// (DateOK, wired in ref.S3WAL), the absolute clock reading no local
// trouble can forge.
func SkewOK(localNow, clk time.Time, wallDrift, ttl time.Duration) bool {
	assertf(ttl > 0, "SkewOK: non-positive ttl %v", ttl)
	bound := ttl / 4
	if bound > 15*time.Minute {
		bound = 15 * time.Minute
	}
	return !clk.After(localNow.Add(bound)) && DriftOK(wallDrift, ttl)
}

// DriftOK is the wall-vs-monotonic drift arm of SkewOK, also consulted
// alone by LIVENESS checks (which lean live under a jump): a manifest
// clock merely AHEAD already leans live through the plain elapsed
// arithmetic, so liveness needs only this arm. A degenerate bound of 0
// (ttl < 4ns — the conformance suite's "window already elapsed" idiom)
// disables the arm: at that scale measurement noise and jumps are
// indistinguishable, and ttl <= 0 is the explicit waiver spelling
// upstream.
func DriftOK(wallDrift, ttl time.Duration) bool {
	assertf(ttl > 0, "DriftOK: non-positive ttl %v", ttl)
	bound := ttl / 4
	if bound > 15*time.Minute {
		bound = 15 * time.Minute
	}
	if bound == 0 {
		return true
	}
	if wallDrift < 0 {
		wallDrift = -wallDrift
	}
	return wallDrift <= bound
}

// DateTolerance is the resolution floor of the §2.4-3 store-Date
// witness: HTTP Date carries 1s resolution, so agreement bounds tighter
// than ±2s cannot distinguish skew from truncation noise.
const DateTolerance = 2 * time.Second

// DateSkewBound is the §2.4-3 agreement bound for the store-Date
// witness: min(15min, ttl/4), the same scaling as SkewOK's arms. A
// degenerate bound below DateTolerance (ttl < 4·tolerance = 8s — the
// conformance suite's sub-second windows) returns 0 = arm DISABLED:
// at that scale legitimate second-boundary truncation is
// indistinguishable from skew, and every production window (delete
// locks ~2.75m, claims 30m, graces ≥1h) sits far above it.
func DateSkewBound(ttl time.Duration) time.Duration {
	assertf(ttl > 0, "DateSkewBound: non-positive ttl %v", ttl)
	bound := ttl / 4
	if bound > 15*time.Minute {
		bound = 15 * time.Minute
	}
	if bound < DateTolerance {
		return 0
	}
	return bound
}

// DateOK is the §2.4-3 store-clock cross-check for destructive/steal
// verdicts: false — demote to report-only, log loudly — when the local
// clock disagrees with the store's Date witness by more than
// DateSkewBound. This is the only arm that catches an AT-BOOT-wrong
// local clock (SkewOK's drift arm anchors at process start; its
// manifest arm reads an old clk as a quiet repo). Deliberately scoped
// to destructive verdicts rather than asserted on every CAS as the
// plan's crash-and-burn sketch suggested: mild skew is routine and a
// per-operation assert would DoS every read under it, while destruction
// is where wrongness is irreversible. Freshness of the witness is the
// caller's obligation (ref.S3WAL requires an observation younger than
// its per-verdict bound and fails closed without one).
func DateOK(localNow, date time.Time, ttl time.Duration) bool {
	assertf(!localNow.IsZero(), "DateOK: zero local time")
	bound := DateSkewBound(ttl)
	if bound == 0 {
		return true
	}
	assertf(!date.IsZero(), "DateOK: zero store date with an active bound")
	delta := localNow.Sub(date)
	if delta < 0 {
		delta = -delta
	}
	return delta <= bound
}

// ManifestKey builds the extension-free manifest object key for one BASE
// repo, asserting the §2.9 member-contiguity alphabet at key-build time:
// every byte must sort ABOVE '+' (0x2B) so a base's `+`-suffixed member
// keys stay contiguous in bucket listing order, and no base may itself
// contain '+' or non-ASCII/control bytes. Production repo IDs are
// server-generated [0-9a-z-]; the validator deliberately enforces only the
// weaker ordering invariant so conformance tests can exercise raw
// test-derived names (slashes, underscores, uppercase — all > 0x2B).
func ManifestKey(base string) string {
	assertf(ValidBaseRepoKey(base), "ManifestKey: base repo %q violates the s3wal key alphabet (empty, '+', or byte <= 0x2B / >= 0x7F)", base)
	return RepoPrefix + base
}

// ValidBaseRepoKey reports whether base satisfies the §2.9 ordering
// alphabet (see ManifestKey).
func ValidBaseRepoKey(base string) bool {
	if base == "" {
		return false
	}
	for i := 0; i < len(base); i++ {
		b := base[i]
		if b <= '+' || b >= 0x7F {
			return false
		}
	}
	return true
}

// BaseFromManifestKey inverts ManifestKey for bucket-listing results.
func BaseFromManifestKey(key string) (string, bool) {
	if len(key) <= len(RepoPrefix) || key[:len(RepoPrefix)] != RepoPrefix {
		return "", false
	}
	base := key[len(RepoPrefix):]
	if !ValidBaseRepoKey(base) {
		return "", false
	}
	return base, true
}

// NewOpID mints a fresh 128-bit op-ID for non-idempotent mutations (§2.3).
func NewOpID() string { return newHexNonce() }

// newHexNonce returns 128 bits of crypto-random hex.
func newHexNonce() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("s3wal assertion failed: nonce entropy: %v", err))
	}
	return hex.EncodeToString(b[:])
}
