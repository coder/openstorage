// Package ref provides the ref authority + pack registry: the RefStore
// contract plus the Postgres implementation (pgx).
package ref

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	// Registers the pure-Go "sqlite" database/sql driver the sqlite backend
	// opens (cgo-free cross-compiled single binaries).
	_ "modernc.org/sqlite"
)

// ZeroSHA is git's null object id (40 zeros) marking ref create/delete.
const ZeroSHA = "0000000000000000000000000000000000000000"

var shaRe = regexp.MustCompile(`\A[0-9a-f]{40}\z`)

// ValidSHA reports whether s is a well-formed 40-hex git object id.
func ValidSHA(s string) bool { return shaRe.MatchString(s) }

// ErrNotFound reports a GetRef miss: the ref does not currently exist.
var ErrNotFound = errors.New("ref not found")

// assertf is a fail-fast invariant check (crash-and-burn style).
func assertf(cond bool, format string, args ...any) {
	if !cond {
		panic(fmt.Sprintf("ref assertion failed: "+format, args...))
	}
}

// Pack is one registered pack for a repo. Key is the blob-store object key
// (blob.PackKey); the backing SQL column stays named s3_key in every backend
// so the pack SQL is byte-comparable across them (no PG migration).
type Pack struct {
	ID  string
	Key string
	// Size is the pack's byte size, feeding the quota ledger (issues
	// #29/#31): ReservePackBytes admits Size against the quota,
	// RegisterPacks credits it to the repo's usage counter, and
	// ReplacePacks records superseded rows' sizes as retained bytes. It is
	// accounting input, not a persisted row column — ListPacks returns it
	// zero. Writers that know the pack file must set it; a zero Size
	// registers fine but contributes nothing to the ledger (the blob
	// listing floor still covers such bytes conservatively).
	Size int64
	// Adopted marks a pack whose BLOB already existed before this
	// registration (a crash-window retry re-registering a published-but-
	// rowless pack — the pack analog of LFS adoption, Codex round 53):
	// physical listings may already contain its bytes, so RegisterPacks
	// bumps the shared floor generation IN ITS REGISTRATION TRANSACTION
	// when it credits a newly inserted Adopted pack, invalidating every
	// replica's cached uncounted floor. Fresh uploads leave it false —
	// their bytes are in no cached scan, and bumping per push would
	// defeat the floor cache under write load. Accounting input like
	// Size, never persisted.
	Adopted bool
}

// RefInfo is one ref row with its metadata. CreatedAt is the row's creation
// time (docs/reference/http-api.md branches[].created_at): set on first insert, preserved
// across SHA updates, and reset when a ref is deleted and recreated.
type RefInfo struct {
	Name      string
	SHA       string
	CreatedAt time.Time // UTC
}

// RefEntry is one (name, sha) ref row — the bounded-enumeration unit
// (issue #119): pages of these replace whole-repo ref materialization on
// the hydration path.
type RefEntry struct {
	Name string
	SHA  string
}

// RefStore is the ref-authority contract: refs only ever change through
// CASRef, and packs are registered strictly after their bytes are durable
// in blob storage.
type RefStore interface {
	// ListRefs returns all refs for repo as name -> sha.
	ListRefs(ctx context.Context, repo string) (map[string]string, error)
	// ListRefsWithMeta returns all refs for repo sorted by name ascending,
	// including creation timestamps (the read API's branches/tags source).
	ListRefsWithMeta(ctx context.Context, repo string) ([]RefInfo, error)
	// ListRefsAfter returns up to limit refs for repo whose name is
	// byte-wise greater than after, in ascending BYTE order of name (the
	// order git's own ref iteration uses, so hydration can merge-join
	// pages against for-each-ref output — issue #119). A page shorter
	// than limit means the scan is complete. Each call is one consistent
	// snapshot; cross-page consistency is the CALLER's contract:
	// githttp.Hydrate binds a paged scan to the repo generation read
	// before it, so any concurrent ref mutation bumps the generation,
	// invalidates the marker the sync publishes, and forces the next
	// hydrate down the full path — healing whatever a torn scan
	// mis-synced.
	ListRefsAfter(ctx context.Context, repo, after string, limit int) ([]RefEntry, error)
	// GetRef returns the current sha of one ref (a point-read: the
	// IdempotentCAS recovery recheck must not scan the repo's whole ref
	// table). Returns ErrNotFound when the ref does not exist.
	GetRef(ctx context.Context, repo, name string) (string, error)
	// CASRef performs a compare-and-swap ref update (old/new = ZeroSHA for
	// create/delete). Returns won=false (no error) when the precondition
	// failed, i.e. someone else won the race.
	CASRef(ctx context.Context, repo, name, old, new string) (won bool, err error)
	// RegisterPacks records packs for repo atomically; idempotent on retry.
	RegisterPacks(ctx context.Context, repo string, packs []Pack) error
	// ListPacks returns all registered packs for repo, oldest first.
	ListPacks(ctx context.Context, repo string) ([]Pack, error)
	// HasPack reports whether repo has a registered pack with the given
	// blob key (a point read: the orphan-pack sweeper classifies each
	// listed blob individually so its memory never scales with a repo's
	// registered-pack count, which pre-policy pushes can grow).
	HasPack(ctx context.Context, repo, key string) (bool, error)
	// ReplacePacks atomically installs a repack fold (issue #1): in ONE
	// transaction it deletes exactly the old rows and inserts newPack.
	// The delete doubles as the CAS precondition — every old row must
	// still exist — so a fold racing another fold loses cleanly:
	// won=false, no error, zero DB changes. Rows registered concurrently
	// by new pushes are NOT part of the precondition and survive the
	// swap; the caller guarantees newPack covers every object of the old
	// rows' packs (never a reachability-trimmed subset), so surviving
	// rows and refs stay consistent regardless of interleaving. Callers
	// must have published newPack's bytes to blob storage BEFORE calling
	// (publish-before-flip); on won=false they own deleting that blob.
	// A won swap bumps the repo generation via the InitSchema triggers.
	ReplacePacks(ctx context.Context, repo string, old []Pack, newPack Pack) (won bool, err error)
	// ReservePackBytes atomically admits the given incoming packs against
	// the per-repo pack-bytes quota (issue #29), inserting one durable
	// reservation row per pack key on success. See ledger.go for the
	// admission formula and serialization discipline.
	// The floorGen argument is the shared floor generation the caller's
	// usageFloor was validated against (ReserveNoFloorGen skips the
	// transactional recheck — cover reservations only, Codex round 60).
	ReservePackBytes(ctx context.Context, group []string, packs []Pack, owner string, usageFloor, floorGen, quotaBytes int64, ttl time.Duration) (ReserveDecision, error)
	// QuotaLedgerState reads one accounting group's quota ledger in one
	// snapshot (usage counter sum, active reservations, retained bytes).
	// The LFS batch surface reads it for its ADVISORY fast-fail admission
	// (issue #51 — batches insert no rows; the PUT's transactional
	// admission is authoritative); also tests, oracles, and operator
	// inspection.
	// excludeKeys omits those keys' active reservations from the
	// ReservedBytes sum (Codex round 24), mirroring ReservePackBytes's
	// same-key exclusion: an admission check for specific keys must not
	// double-count a reservation those keys already hold (a failed
	// verified upload's settle-or-TTL row would otherwise 507 its own
	// retry until the TTL).
	QuotaLedgerState(ctx context.Context, group []string, ttl time.Duration, excludeKeys ...string) (PackQuotaLedgerState, error)
	// RetirePackAttempts retires one same-key reservation attempt per
	// entry — the PROVEN-clean-failure cleanup (Codex rounds 30+33 on
	// #51). Every reserve/refresh increments the row's attempt count, so
	// a failed writer retires exactly its own attempt: the row is deleted
	// only when no attempts remain, never out from under a live same-key
	// writer (the round-22 unsoundness of owner-scoped deletes). gens
	// maps each key to the row-incarnation gen ReserveDecision.Gens
	// returned: a replacement row minted after this caller's expired and
	// was pruned carries a different gen and is untouched (round 33).
	// Callers retire only with EVIDENCE of non-publication (the blob
	// provably absent) — ambiguous outcomes leave the row to settlement
	// or TTL, and crashes never retire. Absent or re-minted keys are
	// no-ops. Mixed fleets are fenced structurally: this binary's rows
	// live under an era-spelled key (reservKeySuffix) no v0.1.0
	// statement addresses, and retirement touches only that spelling —
	// a legacy same-key twin row is left to its own binary's lifecycle.
	RetirePackAttempts(ctx context.Context, group []string, gens map[string]string) error
	// SweepExpiredPackReservations deletes reservation rows at least
	// olderThan old (crash-swept leak reclamation, issue #29; the orphan
	// sweeper calls it on its cadence). olderThan=0 expires everything —
	// a test/recovery escape hatch. Returns the number of rows deleted.
	SweepExpiredPackReservations(ctx context.Context, olderThan time.Duration) (int64, error)
	// ReconcileLegacyLFSObjects deletes legacy-spelled lfs_objects rows
	// whose canonical twin exists, refunding the count and bytes each
	// double-charged, and returns how many it reconciled (ADR-41, Codex
	// round 79): a rolling upgrade can commit a v0.2.0 legacy
	// registration alongside the canonical row, after which the fast
	// paths never call registration for the object again — without this
	// janitor the twin would overstate usage and the object cap forever.
	ReconcileLegacyLFSObjects(ctx context.Context) (reconciled int64, partial bool, err error)
	// IsRetainedPack reports whether (repo, key) is recorded as a
	// superseded-by-repack blob (issue #31): its row was folded away by
	// ReplacePacks but its bytes still occupy the pack prefix. A point
	// read, mirroring HasPack, for the sweeper's per-object classification.
	IsRetainedPack(ctx context.Context, repo, key string) (bool, error)
	// ReclaimRetainedPack removes (repo, key) from the retained set and
	// subtracts its recorded bytes from the repo's usage counter, returning
	// the bytes reclaimed (0 when no retained row exists — idempotent).
	// Callers invoke it exactly when the superseded blob's bytes leave the
	// pack prefix (the sweeper's safeDelete, or its heal pass discovering
	// the blob already gone).
	ReclaimRetainedPack(ctx context.Context, repo, key string) (int64, error)
	// RegisterLFSObject records one verified LFS object for repo (always
	// the BASE RepoID — namespaces share the LFS space, ADR-35) strictly
	// after its bytes are durable in blob storage, mirroring RegisterPacks
	// (issue #51). In ONE transaction it inserts the row (idempotent on
	// retry), consumes any admission reservation for key (deleted by KEY,
	// any owner — the bytes become counter-accounted), and credits a NEWLY
	// inserted row's bytes to the repo's usage counter. bytes may be zero
	// (the empty object registers without ledger effect).
	RegisterLFSObject(ctx context.Context, repo, key string, bytes int64) error
	// RegisterAdoptedLFSObject is RegisterLFSObject for ADOPTED objects
	// (pre-registry blobs whose bytes physical listings already contain):
	// the same transaction also bumps the adoption generation (Codex
	// round 51 — a separate best-effort bump could time out after the
	// registration committed, leaving remote floors double-counting the
	// adopted bytes with no retry, because adoption short-circuits once
	// the row exists). A re-registration of an existing row commits
	// nothing and bumps nothing.
	RegisterAdoptedLFSObject(ctx context.Context, repo, key string, bytes int64) error
	// RecordLFSMismatch persists that a full content verification proved
	// the legacy blob at key (fingerprint at hash time) wrong for its
	// OID (#104, Codex round 132): the marker must survive restarts and
	// be visible fleet-wide, or its loss makes a registered object's
	// corrupt legacy spelling implicitly clean. Upsert; purge-fenced
	// repos silently no-op like every row-creating write.
	RecordLFSMismatch(ctx context.Context, repo, key, fingerprint string) error
	// LFSMismatchFingerprint returns key's pending mismatch marker.
	LFSMismatchFingerprint(ctx context.Context, repo, key string) (string, bool, error)
	// ClearLFSMismatch removes key's marker after a decisive verdict.
	ClearLFSMismatch(ctx context.Context, repo, key string) error
	// HasLFSObject reports whether (repo, key) is a registered LFS object
	// (a point read mirroring HasPack, for the sweeper's per-object
	// classification of the lfs/ prefix).
	HasLFSObject(ctx context.Context, repo, key string) (bool, error)
	// TouchLFSObject clears (repo, key)'s reclamation mark (issue #73)
	// and reports whether the row still exists. It is the liveness
	// handshake behind every zero-growth "already have it" answer: a
	// client that just learned the object is stored may reference it
	// from a pointer file it pushes NEXT, so the touch guarantees at
	// least one full reclamation grace window before retirement can
	// reconsider the row. touched=false means the row is gone — the
	// caller must fall through to its upload path instead of promising
	// an object the reclaimer just retired. The single-statement UPDATE
	// serializes against RetireUnreferencedLFSObject's guarded delete on
	// the row itself: whichever commits first wins, and the loser
	// observes it (the retire recheck sees the cleared mark; the touch
	// sees zero rows).
	TouchLFSObject(ctx context.Context, repo, key string) (touched bool, err error)
	// LFSObjectRegisteredAt returns (repo, key)'s registration time — the
	// row's created_at, which the in-place legacy→canonical migration
	// REFRESHES (Codex round 89): downloads presign the still-rowless
	// LEGACY spelling right up to the moment the canonical row exists, so
	// the row's clock is the only trustworthy "no more legacy URLs are
	// being minted" anchor. Blob timestamps are not: a rowless canonical
	// copy can be hours old (publication or adoption copied it and only
	// the registration failed) while its late registration retry
	// verifies it in place without rewriting it. bytes is the row's
	// registered size, the sweeper's completeness yardstick (issue
	// #127): the reap gate must spare a retained legacy source whose
	// canonical copy is SHORTER than registered, not merely present. ok
	// is false when no row exists.
	LFSObjectRegisteredAt(ctx context.Context, repo, key string) (at time.Time, bytes int64, ok bool, err error)
	// BumpLFSAdoptionGen increments repo's shared adoption generation
	// (Codex round 49 on #51): adopting a pre-registry LFS object moves
	// bytes that physical listings ALREADY contain into the usage
	// counter, so every replica's cached uncounted floor scanned before
	// the adoption would double-count them (floor + counter) until
	// expiry. Floor caches snapshot this generation at scan start and
	// reject entries whose generation moved — the ref store is the only
	// state all replicas and hook processes share.
	BumpLFSAdoptionGen(ctx context.Context, repo string) error
	// LFSAdoptionGen returns repo's adoption generation (0 before any
	// adoption) — a point read on the floor-serve path.
	LFSAdoptionGen(ctx context.Context, repo string) (int64, error)
	// CountLFSObjects returns repo's registered LFS object count — the
	// admission-time read behind the per-repo object-count cap (issue
	// #51): bytes bound row count at nothing, and registered objects are
	// permanent, so their population needs its own bound.
	CountLFSObjects(ctx context.Context, repo string) (int64, error)
	// GuardedLFSDelete runs del (a blob deletion for a stored key whose
	// bytes failed content verification) serialized against concurrent
	// LFS registrations of the same repo (issue #51, Codex round 18):
	// within one transaction it takes the lock RegisterLFSObject's
	// counter update takes, re-reads the registry, and only if key is
	// still unregistered runs del. A concurrent verified upload therefore
	// either registered first (deleted=false, del never runs) or blocks
	// until this commits — its post-registration blob.EnsurePublished
	// then republishes its bytes, so no interleaving can leave a
	// registered row pointing at deleted bytes.
	GuardedLFSDelete(ctx context.Context, repo, key string, del func(context.Context) error) (deleted bool, err error)
	// TryAdoptionClaim atomically claims exclusive fleet-wide ownership
	// of one legacy-object adoption (Codex security round 73): the
	// verification hash downloads the whole object, and without a shared
	// claim every replica races the same hash — tiny write-scoped
	// batches fanned across the fleet multiply backend reads by fleet
	// size. claimed=false means a live claim exists (another replica is
	// hashing; the caller denies transiently and the client retries —
	// usually landing on the winner's committed registration). A claim
	// older than AdoptionClaimTTL is expired and taken over. The
	// returned owner token fences every later mutation (Codex round 74):
	// a worker whose claim was taken over must not be able to release or
	// renew the successor's lease. Claims live in pack_reservations
	// under a suffixed key that no settlement, retirement, or
	// admission-GC path touches; only expiry-takeover, owner-fenced
	// release, and the claim-aware sweeper prune remove them.
	TryAdoptionClaim(ctx context.Context, repo, key string) (claimed bool, owner string, err error)
	// RenewAdoptionClaim restarts the claim's TTL clock when key's live
	// claim still belongs to owner (Codex round 74 — verification is
	// size-scaled and can outlive any fixed TTL). renewed=false means
	// the lease was lost (expired and taken over, or released): the
	// caller must stop hashing.
	RenewAdoptionClaim(ctx context.Context, key, owner string) (renewed bool, err error)
	// ReleaseAdoptionClaim frees key's adoption claim if it still
	// belongs to owner (best-effort: a crashed winner's claim expires
	// via AdoptionClaimTTL instead).
	ReleaseAdoptionClaim(ctx context.Context, key, owner string) error
	// GetGeneration returns repo's mutation generation: 0 for a repo no
	// mutation ever touched, strictly positive and monotonically increasing
	// afterwards. It is an invalidation token over committed row changes,
	// not a call counter: schema-level triggers installed by InitSchema
	// bump it inside the same transaction as ANY committed change to the
	// repo's refs or packs rows — won CAS swaps, pack registrations
	// (per changed row; a no-op idempotent retry changes nothing and bumps
	// nothing), future pack rewrites (issue #1's ReplacePacks), and even
	// writers running binaries that predate the counter. An unchanged value
	// therefore proves the repo's packs and refs are unchanged; the hydrate
	// short-circuit relies on exactly that (issue #4).
	GetGeneration(ctx context.Context, repo string) (int64, error)
}

// Store extends RefStore with the lifecycle the wire seam needs: serve runs
// InitSchema once at startup and Close on shutdown. Consumers keep coding
// against RefStore; hooks never run DDL.
type Store interface {
	RefStore
	// InitSchema creates the ref/pack tables (idempotent, safe under
	// concurrent startups).
	InitSchema(ctx context.Context) error
	Close()
}

// Open dispatches on the URL scheme to the matching Store backend:
//
//	postgres:// or postgresql://  → OpenPG
//	sqlite:///abs/path/db.sqlite  → pure-Go SQLite store
//
// Open never runs InitSchema: hooks reconstruct stores from env URLs and
// must not run DDL — the serve/wire seam owns schema init.
func Open(ctx context.Context, dbURL string) (Store, error) {
	assertf(dbURL != "", "Open: empty ref store URL")
	u, err := url.Parse(dbURL)
	if err != nil {
		return nil, fmt.Errorf("parse ref store URL: %w", err)
	}
	switch u.Scheme {
	case "postgres", "postgresql":
		return OpenPG(ctx, dbURL)
	case "sqlite":
		return OpenSQLite(ctx, dbURL)
	default:
		return nil, fmt.Errorf("ref store URL scheme %q unsupported (want postgres, postgresql, or sqlite)", u.Scheme)
	}
}

// PG implements Store on Postgres.
type PG struct {
	pool *pgxpool.Pool
}

var _ Store = (*PG)(nil)

// OpenPG connects to pgURL and pings the database.
func OpenPG(ctx context.Context, pgURL string) (*PG, error) {
	assertf(pgURL != "", "OpenPG: empty PG URL")
	pool, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		return nil, fmt.Errorf("pg connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pg ping: %w", err)
	}
	return &PG{pool: pool}, nil
}

func (s *PG) Close() { s.pool.Close() }

// InitSchema creates the ref/pack tables (idempotent). The
// concurrentIndexes entries (packs_repo_s3_key, refs_repo_name_c) are
// deliberately NOT part of the transactional DDL: they are built
// out-of-transaction with CREATE INDEX CONCURRENTLY (issue #24, rationale on
// buildIndexesLocked). Once every index is valid — every boot after the
// first — only the transactional DDL runs. While an index is missing or
// invalid, a SESSION advisory lock is held across BOTH the transactional DDL
// and the index builds: the DDL takes ACCESS EXCLUSIVE locks on refs/packs
// (DROP TRIGGER), and a concurrent replica's CONCURRENTLY build both
// conflicts with them and waits out every older transaction snapshot, so
// letting the two phases interleave across replicas deadlocks (SQLSTATE
// 40P01, found via the concurrent-boot test). Skipping the lock when every
// index is valid is sound because builders re-check state under the lock
// and never build over a valid index — a valid read means no build can be
// running.
func (s *PG) InitSchema(ctx context.Context) error {
	if allValid, err := concurrentIndexesState(ctx, s.pool); err != nil {
		return err
	} else if allValid {
		return initSchemaTx(ctx, s.pool) // steady state: indexes built by an earlier boot
	}

	// First boot (or crashed-build repair): one dedicated conn owns the
	// session lock; the index build below runs on it. Acquisition POLLS
	// pg_try_advisory_lock instead of blocking inside SELECT
	// pg_advisory_lock — a blocked SELECT sits in an open (implicit)
	// transaction holding a snapshot, and the builder's CREATE INDEX
	// CONCURRENTLY waits out every older snapshot before it can finish, so
	// blocking waiters would deadlock the build. Polls are millisecond
	// statements and a waiter holds nothing between them; each poll
	// re-checks state so waiters finish as soon as the builder commits.
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("schema indexes: acquire lock conn: %w", err)
	}
	defer conn.Release()
	for {
		var locked bool
		if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(0x5350494458)`).Scan(&locked); err != nil { // "SPIDX"
			return fmt.Errorf("schema indexes: try advisory lock: %w", err)
		}
		if locked {
			break
		}
		if allValid, err := concurrentIndexesState(ctx, conn); err != nil {
			return err
		} else if allValid {
			return initSchemaTx(ctx, conn) // the lock holder finished the builds while we waited
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	// The unlock must survive ctx cancellation, and a conn silently keeping
	// the session lock must never return to the pool — later boots would
	// poll against a lock nobody is working under until this process exits.
	// Mirror the river migration lock: unlock on a cancel-free deadline,
	// destroy the conn if even that fails (the server then drops the lock
	// with the session).
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if _, err := conn.Exec(unlockCtx, `SELECT pg_advisory_unlock(0x5350494458)`); err != nil {
			_ = conn.Conn().Close(unlockCtx)
		}
	}()
	if err := initSchemaTx(ctx, conn); err != nil {
		return err
	}
	return buildIndexesLocked(ctx, conn)
}

// initSchemaTx is InitSchema's transactional DDL, run on db: the pool on
// steady-state boots, but the HELD dedicated conn on every path that has
// acquired one — that conn may be the pool's only slot (pool_max_conns=1),
// and beginning the transaction on the pool from under it would wait for a
// second slot forever on a non-expiring boot context (Codex P1 on #43).
// The advisory xact lock serializes concurrent node startups: two racing
// CREATE TABLE IF NOT EXISTS can fail with a duplicate pg_type key error
// (found via dogfooding).
func initSchemaTx(ctx context.Context, db interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}) error {
	return pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(0x5350494B45)`); err != nil { // "SPIKE"
			return err
		}
		_, err := tx.Exec(ctx, `
CREATE TABLE IF NOT EXISTS refs  (repo text, name text, sha text NOT NULL,
                                  created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (repo, name));
CREATE TABLE IF NOT EXISTS packs (repo text, id text, s3_key text NOT NULL,
                                  created_at timestamptz DEFAULT now(),
                                  bytes bigint NOT NULL DEFAULT 0, PRIMARY KEY (repo, id));
-- Registered pack size (issue #101): written by RegisterPacks/ReplacePacks
-- so the absence reconciler can charge a missing pack's bytes as a restore
-- cover. Rows registered before this migration keep 0 (size unknown —
-- reported, never covered); repack folds rewrite them with real sizes.
ALTER TABLE packs ADD COLUMN IF NOT EXISTS bytes bigint NOT NULL DEFAULT 0;
-- Per-repo mutation generation (issue #4). No row means "never mutated"
-- (GetGeneration reports 0); the first bump creates the row at 1, so a
-- hydrate marker taken before the first mutation can never match the state
-- after it.
CREATE TABLE IF NOT EXISTS repo_generations (repo text PRIMARY KEY,
                                             generation bigint NOT NULL DEFAULT 1);
-- Pack-quota ledger (issues #29/#31, see ledger.go). These tables carry
-- accounting state only — they never affect clone content, so none of
-- them get repo_generations triggers.
CREATE TABLE IF NOT EXISTS pack_reservations (key text PRIMARY KEY, repo text NOT NULL,
                                              bytes bigint NOT NULL, owner text NOT NULL,
                                              created_at timestamptz NOT NULL DEFAULT now(),
                                              attempts bigint NOT NULL DEFAULT 1,
                                              gen text NOT NULL DEFAULT '');
-- Rolling upgrade from pre-attempt schemas (issue #51, Codex round 30):
-- rows minted by older binaries count as one attempt.
ALTER TABLE pack_reservations ADD COLUMN IF NOT EXISTS attempts bigint NOT NULL DEFAULT 1;
-- gen identifies the ROW INCARNATION (issue #51, Codex round 33): minted
-- at INSERT, preserved across same-key refreshes, so a stale writer's
-- retirement can never touch a replacement row created after its own
-- expired and was pruned.
ALTER TABLE pack_reservations ADD COLUMN IF NOT EXISTS gen text NOT NULL DEFAULT '';
-- Admission sums active reservations per base repo.
CREATE INDEX IF NOT EXISTS pack_reservations_repo ON pack_reservations (repo);
CREATE TABLE IF NOT EXISTS pack_retained (repo text, key text, bytes bigint NOT NULL,
                                          superseded_at timestamptz NOT NULL DEFAULT now(),
                                          PRIMARY KEY (repo, key));
CREATE TABLE IF NOT EXISTS pack_usage (repo text PRIMARY KEY, bytes bigint NOT NULL DEFAULT 0);
-- Registered LFS objects (issue #51): one row per verified upload, keyed by
-- the base RepoID and the content-addressed blob key (lfs/<repo>/<oid>).
-- The registry the LFS orphan sweep classifies against, and the settle
-- record that moves reserved bytes into pack_usage. Accounting/registry
-- state only — LFS objects never affect clone content, so no generation
-- triggers. unreferenced_since is the reclamation mark (issue #73): the
-- moment a reachability scan first observed no reachable pointer file
-- referencing the row's OID, NULL while referenced or unscanned. Cleared
-- by every liveness signal (re-registration, zero-growth batch/PUT
-- touch); retirement requires it to have survived a full grace window.
CREATE TABLE IF NOT EXISTS lfs_objects (repo text, key text, bytes bigint NOT NULL,
                                        created_at timestamptz NOT NULL DEFAULT now(),
                                        unreferenced_since timestamptz,
                                        PRIMARY KEY (repo, key));
ALTER TABLE lfs_objects ADD COLUMN IF NOT EXISTS unreferenced_since timestamptz;
-- Maintained count of lfs_objects rows per repo (issue #51): the
-- object-count cap reads it in O(1) instead of COUNT(*)-ing an
-- attacker-growable table under lock. Incremented transactionally with
-- every row insert; the only deletion paths — repository purge (issue
-- #52), legacy-twin reconciliation (ADR-41), and unreferenced-object
-- retirement (issue #73) — decrement or drop it in the same transaction,
-- so the cap can turn stricter on crash, never looser.
CREATE TABLE IF NOT EXISTS lfs_object_counts (repo text PRIMARY KEY, count bigint NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS lfs_adoption_gens (repo text PRIMARY KEY, gen bigint NOT NULL DEFAULT 0);
-- Fleet-persistent LFS mismatch markers (#104, Codex round 132): a FULL
-- content verification proved the legacy blob's bytes wrong for their
-- OID. Process-local markers were per-replica and evictable, and losing
-- one made a registered object's corrupt legacy spelling implicitly
-- clean forever. Cleared only by a decisive verdict (deleted spelling or
-- current version proven valid).
CREATE TABLE IF NOT EXISTS lfs_mismatches (repo text NOT NULL, key text NOT NULL, fingerprint text NOT NULL, PRIMARY KEY (repo, key));
-- Repository purge fence (issue #52, PR #105 security round): one row per
-- BASE repo whose purge began. Pack/LFS registrations check it under the
-- base-ledger lock and fail closed, so no write settling after the fence
-- can strand registry rows behind a completed purge.
CREATE TABLE IF NOT EXISTS purged_repos (repo text PRIMARY KEY,
                                         fenced_at timestamptz NOT NULL DEFAULT now());
-- Idempotent additive migration for pre-p3 dev databases (backfills
-- existing rows via the NOT NULL DEFAULT).
ALTER TABLE refs ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now();
-- The generation bump lives in SCHEMA triggers, not application code:
-- during a rolling upgrade a writer running an older binary still commits
-- plain ref/pack DML, and a trigger is the only way its mutations bump the
-- counter too. Any reader that trusts generation markers installed these
-- triggers at its own boot (InitSchema runs before serving), so no marker
-- can predate them. INSERT ... DO NOTHING + UPDATE instead of one upsert
-- keeps the body byte-comparable with the SQLite triggers.
CREATE OR REPLACE FUNCTION bump_repo_generation() RETURNS trigger LANGUAGE plpgsql AS $bump$
BEGIN
    INSERT INTO repo_generations (repo, generation)
    VALUES (COALESCE(NEW.repo, OLD.repo), 0)
    ON CONFLICT (repo) DO NOTHING;
    UPDATE repo_generations SET generation = generation + 1
    WHERE repo = COALESCE(NEW.repo, OLD.repo);
    RETURN NULL;
END $bump$;
-- Constraint triggers, DEFERRABLE INITIALLY DEFERRED: the bump still
-- commits atomically with the mutating transaction, but the generation-row
-- lock is taken at COMMIT, strictly after every refs/packs row lock — a
-- multi-row registration interleaving row locks with an immediate per-row
-- bump could otherwise deadlock against a concurrent same-repo writer.
-- DROP + CREATE (constraint triggers lack CREATE OR REPLACE) is safe here:
-- the advisory lock above serializes InitSchema callers.
DROP TRIGGER IF EXISTS refs_bump_generation ON refs;
CREATE CONSTRAINT TRIGGER refs_bump_generation
    AFTER INSERT OR UPDATE OR DELETE ON refs
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION bump_repo_generation();
DROP TRIGGER IF EXISTS packs_bump_generation ON packs;
CREATE CONSTRAINT TRIGGER packs_bump_generation
    AFTER INSERT OR UPDATE OR DELETE ON packs
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION bump_repo_generation();
`)
		return err
	})
}

// concurrentIndexes are the catalog-managed indexes built OUT of the
// transactional DDL with CREATE INDEX CONCURRENTLY (issue #24 rationale on
// buildIndexesLocked):
//
//   - packs_repo_s3_key serves the orphan sweeper's per-object membership
//     check (HasPack, issue #5), which filters on (repo, s3_key) — the
//     (repo, id) primary key cannot serve it, and without it a sweep over
//     a repo with attacker-growable pack count turns O(N²).
//   - refs_repo_name_c serves hydration's bounded byte-ordered keyset
//     enumeration (ListRefsAfter, issue #119): the (repo, name) primary
//     key is built with the database's default collation, so it cannot
//     serve COLLATE "C" range scans — without this index every page would
//     re-sort the repo's whole remaining ref set, turning an
//     attacker-scaled ref fanout into quadratic server work.
var concurrentIndexes = []struct{ name, create string }{
	{"packs_repo_s3_key", `CREATE INDEX CONCURRENTLY IF NOT EXISTS packs_repo_s3_key ON packs (repo, s3_key)`},
	{"refs_repo_name_c", `CREATE INDEX CONCURRENTLY IF NOT EXISTS refs_repo_name_c ON refs (repo, name COLLATE "C")`},
}

// indexState reports whether the named index exists and is valid.
// to_regclass resolves via search_path and yields NULL (no row) when the
// index does not exist.
func indexState(ctx context.Context, q interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}, name string) (exists, valid bool, err error) {
	err = q.QueryRow(ctx,
		`SELECT i.indisvalid FROM pg_index i WHERE i.indexrelid = to_regclass($1)`, name).Scan(&valid)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, fmt.Errorf("%s index state: %w", name, err)
	}
	return true, valid, nil
}

// concurrentIndexesState reports whether every managed index exists and is
// valid.
func concurrentIndexesState(ctx context.Context, q interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}) (allValid bool, err error) {
	for _, idx := range concurrentIndexes {
		exists, valid, err := indexState(ctx, q, idx.name)
		if err != nil {
			return false, err
		}
		if !exists || !valid {
			return false, nil
		}
	}
	return true, nil
}

// buildIndexesLocked builds every missing concurrentIndexes entry with
// CREATE INDEX CONCURRENTLY on conn, which must hold the SPIDX session lock
// (issue #24). A plain transactional CREATE INDEX holds a SHARE lock on the
// table for the whole build, so the FIRST rollout against a database with a
// large pre-existing table would block every writer (pre-receive
// RegisterPacks on packs; every push's CASRef on refs) until the build
// finished.
//
// Crash repair: a failed or interrupted CONCURRENTLY build leaves the index
// behind as INVALID (still maintained by writers, never used by queries) —
// and CREATE INDEX CONCURRENTLY IF NOT EXISTS silently SKIPS such a
// leftover instead of fixing it. Each attempt therefore checks
// pg_index.indisvalid first and drops an invalid leftover with DROP INDEX
// CONCURRENTLY (the plain form queues an ACCESS EXCLUSIVE lock on the table
// behind running writers and stalls everyone queued after it — the very
// stall this path exists to avoid). The session lock makes the drop safe:
// no other InitSchema can be mid-build, and nothing else creates these
// indexes. Statements run one per Exec: pgx sends zero-arg Execs over the
// simple protocol, where a multi-statement string is one implicit
// transaction block, which CONCURRENTLY rejects.
func buildIndexesLocked(ctx context.Context, conn *pgxpool.Conn) error {
	for _, idx := range concurrentIndexes {
		// check → drop-invalid → build, re-verified from the catalog each
		// iteration (the build's own failure can leave a fresh INVALID
		// entry, which the next iteration repairs). Three iterations = the
		// success path (build, verify) plus one full repair cycle.
		var lastErr error
		built := false
		for range 3 {
			exists, valid, err := indexState(ctx, conn, idx.name)
			if err != nil {
				return err
			}
			if exists && valid {
				built = true
				break
			}
			if exists {
				if _, err := conn.Exec(ctx, `DROP INDEX CONCURRENTLY IF EXISTS `+idx.name); err != nil {
					lastErr = fmt.Errorf("%s index: drop invalid leftover: %w", idx.name, err)
					continue
				}
			}
			if _, err := conn.Exec(ctx, idx.create); err != nil {
				lastErr = fmt.Errorf("%s index: concurrent build: %w", idx.name, err)
				continue
			}
		}
		if built {
			continue
		}
		if lastErr == nil {
			lastErr = fmt.Errorf("%s index: still not valid after repeated builds", idx.name)
		}
		return lastErr
	}
	return nil
}

// ListRefs returns all refs for repo as name -> sha.
func (s *PG) ListRefs(ctx context.Context, repo string) (map[string]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT name, sha FROM refs WHERE repo = $1`, repo)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	refs := make(map[string]string)
	for rows.Next() {
		var name, sha string
		if err := rows.Scan(&name, &sha); err != nil {
			return nil, err
		}
		assertf(ValidSHA(sha), "refs table holds malformed sha %q for %s/%s", sha, repo, name)
		refs[name] = sha
	}
	return refs, rows.Err()
}

// ListRefsWithMeta returns all refs for repo sorted by name ascending.
func (s *PG) ListRefsWithMeta(ctx context.Context, repo string) ([]RefInfo, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT name, sha, created_at FROM refs WHERE repo = $1 ORDER BY name`, repo)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var refs []RefInfo
	for rows.Next() {
		var r RefInfo
		if err := rows.Scan(&r.Name, &r.SHA, &r.CreatedAt); err != nil {
			return nil, err
		}
		assertf(ValidSHA(r.SHA), "refs table holds malformed sha %q for %s/%s", r.SHA, repo, r.Name)
		r.CreatedAt = r.CreatedAt.UTC()
		refs = append(refs, r)
	}
	return refs, rows.Err()
}

// ListRefsAfter returns up to limit refs for repo above the after cursor in
// ascending byte order (issue #119). COLLATE "C" pins byte-wise comparison
// regardless of the database's default collation — the order git's ref
// iteration uses and the only one hydration's merge-join can trust; the
// refs_repo_name_c index (see concurrentIndexes) serves the keyset scan so
// a page never sorts the repo's whole remaining ref set.
func (s *PG) ListRefsAfter(ctx context.Context, repo, after string, limit int) ([]RefEntry, error) {
	assertf(repo != "", "ListRefsAfter: empty repo")
	assertf(limit > 0, "ListRefsAfter: non-positive limit %d", limit)
	rows, err := s.pool.Query(ctx,
		`SELECT name, sha FROM refs WHERE repo = $1 AND name COLLATE "C" > $2
		 ORDER BY name COLLATE "C" LIMIT $3`, repo, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var refs []RefEntry
	for rows.Next() {
		var e RefEntry
		if err := rows.Scan(&e.Name, &e.SHA); err != nil {
			return nil, err
		}
		assertf(ValidSHA(e.SHA), "refs table holds malformed sha %q for %s/%s", e.SHA, repo, e.Name)
		refs = append(refs, e)
	}
	return refs, rows.Err()
}

// GetRef returns the current sha of one ref, or ErrNotFound.
func (s *PG) GetRef(ctx context.Context, repo, name string) (string, error) {
	assertf(repo != "" && name != "", "GetRef: empty repo %q or name %q", repo, name)
	var sha string
	err := s.pool.QueryRow(ctx,
		`SELECT sha FROM refs WHERE repo = $1 AND name = $2`, repo, name).Scan(&sha)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", err
	}
	assertf(ValidSHA(sha), "refs table holds malformed sha %q for %s/%s", sha, repo, name)
	return sha, nil
}

// syncClaimFenceKey carries an optional sync-claim fence (issue #86) to
// CASRef implementations via context — deliberately NOT a RefStore method:
// every decorator in the stack (events, idempotent recovery, test fault
// injectors) forwards the context untouched, so the fence reaches the
// backend without widening the interface.
type syncClaimFenceKey struct{}

type syncClaimFence struct{ repo, holder string }

// WithSyncClaimFence fences every CASRef executed under the returned
// context: the swap applies only while repo's upstream-sync claim is held
// by holder, evaluated ATOMICALLY inside the CAS statement itself against
// the repo_upstreams table (deployment invariant: meta and ref share one
// database — a single --data-dir sqlite file or the single serve PG URL).
// A fenced CAS whose holder no longer matches (its claim was stolen, or
// the upstream was detached) reports won=false, exactly like losing the
// SHA race: the stale sync push's update is refused before publication.
// Only the update hook sets this, and only for loopback sync pushes
// (EnvSyncClaim).
func WithSyncClaimFence(ctx context.Context, repo, holder string) context.Context {
	assertf(repo != "" && holder != "", "WithSyncClaimFence: empty repo %q or holder %q", repo, holder)
	return context.WithValue(ctx, syncClaimFenceKey{}, syncClaimFence{repo: repo, holder: holder})
}

// casFence extracts the sync-claim fence, verifying it targets this repo.
func casFence(ctx context.Context, repo string) (syncClaimFence, bool) {
	f, ok := ctx.Value(syncClaimFenceKey{}).(syncClaimFence)
	if !ok {
		return syncClaimFence{}, false
	}
	assertf(f.repo == repo, "sync claim fence for %q used on CAS for %q", f.repo, repo)
	return f, true
}

// CASRef performs the compare-and-swap ref update. Returns won=false (no
// error) when the precondition failed, i.e. someone else won the race.
// Refs only ever change inside these single statements, so a broken/partial
// ref state is impossible by construction; the statement runs inside an
// explicit transaction whose only other work happens strictly BEFORE it
// (the base-ledger lock and the purge-fence read below), so the statement
// remains the single mutation unit and the IdempotentCAS ambiguity
// classification (idempotent.go) is unchanged: pre-statement failures are
// tagged notExecuted, statement failures classify exactly as before, and a
// lost COMMIT response stays ambiguous (recovered by the recheck). A won
// swap also bumps the repo generation via the InitSchema-installed
// deferred constraint trigger at commit.
//
// The transaction takes the base-ledger advisory lock first and rejects
// purge-fenced repos with ErrRepoPurged (issue #52, PR #105 security
// round): ref writers do not all pass through pack registration — REST
// branch/tag creation CASes an existing sha with no new pack — so without
// this, a create landing between the purge's zero-row verification and its
// completion would strand a ref row forever. The lock totally orders every
// CAS against FenceRepoPurge's insert; registration paths already take it,
// so per-base writer serialization is the established cost of the fence.
//
// Under WithSyncClaimFence the statement additionally requires the repo's
// sync claim to be held by the fencing holder, with FOR UPDATE on the
// repo_upstreams row: a concurrent claim steal serializes against the CAS
// instead of racing its snapshot, so a stolen run's update can never
// commit after the thief's claim (issue #86).
func (s *PG) CASRef(ctx context.Context, repo, name, old, new string) (won bool, err error) {
	assertf(ValidSHA(old), "CASRef: malformed old sha %q", old)
	assertf(ValidSHA(new), "CASRef: malformed new sha %q", new)
	assertf(old != ZeroSHA || new != ZeroSHA, "CASRef: zero -> zero update for %s/%s", repo, name)

	// Acquire explicitly so acquisition failures (pool exhausted, deadline
	// expired while waiting) are distinguishable from statement failures:
	// the statement provably never executed, so IdempotentCAS must not
	// recover such errors (see notExecuted).
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return false, fmt.Errorf("cas acquire %s/%s: %w", repo, name, notExecuted(err))
	}
	defer conn.Release()
	// Pre-dispatch guard: a context canceled after acquire but before Exec
	// sends the statement is definitive non-execution too. (The residual
	// race — cancellation between this check and the wire write — is
	// covered by pgconn itself, whose pre-send check returns a
	// SafeToRetry-tagged contextAlreadyDoneError.)
	if cerr := ctx.Err(); cerr != nil {
		return false, fmt.Errorf("cas %s/%s: %w", repo, name, notExecuted(cerr))
	}

	// Everything up to (but excluding) the CAS statement is provably
	// non-executing: an error here means the mutation was never sent.
	tx, err := conn.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("cas begin %s/%s: %w", repo, name, notExecuted(err))
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := lockBaseLedger(ctx, tx, baseRepoOf(repo)); err != nil {
		return false, fmt.Errorf("cas %s/%s: %w", repo, name, notExecuted(err))
	}
	purged, err := repoPurgeFencedPG(ctx, tx, repo)
	if err != nil {
		return false, fmt.Errorf("cas %s/%s: purge fence: %w", repo, name, notExecuted(err))
	}
	if purged {
		return false, fmt.Errorf("cas %s/%s: %w", repo, name, notExecuted(ErrRepoPurged))
	}

	fence, fenced := casFence(ctx, repo)
	var tag string
	var ct interface{ RowsAffected() int64 }
	switch {
	case old == ZeroSHA && !fenced: // create
		// created_at is set here once; SHA updates below never touch it.
		c, cerr := tx.Exec(ctx,
			`INSERT INTO refs (repo, name, sha, created_at) VALUES ($1, $2, $3, now()) ON CONFLICT DO NOTHING`,
			repo, name, new)
		ct, err, tag = c, cerr, "create"
	case old == ZeroSHA: // fenced create
		c, cerr := tx.Exec(ctx,
			`INSERT INTO refs (repo, name, sha, created_at)
			 SELECT $1, $2, $3, now()
			 WHERE EXISTS (SELECT 1 FROM repo_upstreams WHERE repo_id = $1 AND sync_claim_holder = $4 FOR UPDATE)
			 ON CONFLICT DO NOTHING`,
			repo, name, new, fence.holder)
		ct, err, tag = c, cerr, "fenced create"
	case new == ZeroSHA && !fenced: // delete
		c, cerr := tx.Exec(ctx,
			`DELETE FROM refs WHERE repo = $1 AND name = $2 AND sha = $3`,
			repo, name, old)
		ct, err, tag = c, cerr, "delete"
	case new == ZeroSHA: // fenced delete
		c, cerr := tx.Exec(ctx,
			`DELETE FROM refs WHERE repo = $1 AND name = $2 AND sha = $3
			 AND EXISTS (SELECT 1 FROM repo_upstreams WHERE repo_id = $1 AND sync_claim_holder = $4 FOR UPDATE)`,
			repo, name, old, fence.holder)
		ct, err, tag = c, cerr, "fenced delete"
	case !fenced: // update
		c, cerr := tx.Exec(ctx,
			`UPDATE refs SET sha = $1 WHERE repo = $2 AND name = $3 AND sha = $4`,
			new, repo, name, old)
		ct, err, tag = c, cerr, "update"
	default: // fenced update
		c, cerr := tx.Exec(ctx,
			`UPDATE refs SET sha = $1 WHERE repo = $2 AND name = $3 AND sha = $4
			 AND EXISTS (SELECT 1 FROM repo_upstreams WHERE repo_id = $2 AND sync_claim_holder = $5 FOR UPDATE)`,
			new, repo, name, old, fence.holder)
		ct, err, tag = c, cerr, "fenced update"
	}
	if err != nil {
		return false, fmt.Errorf("cas %s %s/%s: %w", tag, repo, name, err)
	}
	// A commit failure is NOT notExecuted: the response may have been
	// lost after the backend committed — exactly the ambiguity class the
	// IdempotentCAS recovery recheck exists for.
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("cas %s %s/%s: commit: %w", tag, repo, name, err)
	}
	n := ct.RowsAffected()
	assertf(n == 0 || n == 1, "cas %s %s/%s affected %d rows", tag, repo, name, n)
	return n == 1, nil
}

// RegisterPacks inserts pack rows in ONE transaction (crash-ordering: this
// runs strictly after the blob upload succeeded). Idempotent on retry: a
// re-pushed identical pack has the same id.
//
// The same transaction settles the quota ledger (issues #29/#31): each
// pack's admission reservation is consumed (deleted by KEY, any owner —
// the bytes become counter-accounted below, and identical keys are
// identical bytes, so a same-key reservation another push re-acquired
// would double-count if kept), and each NEWLY inserted row credits its
// size to the repo's usage counter unless the key was retained (superseded
// bytes were counted at their original registration and never debited —
// re-registering just makes them live again). Atomicity here is what lets
// ReservePackBytes read one snapshot without locking against registration.
func (s *PG) RegisterPacks(ctx context.Context, repo string, packs []Pack) error {
	assertf(len(packs) > 0, "RegisterPacks called with no packs")
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// Lock-first before any row work, unconditionally: an adoption
		// bump below must be serialized with the admission recheck (Codex
		// round 65, ledger.go lockBaseLedger), and the purge-fence check
		// must be ordered against FenceRepoPurge's insert (issue #52) —
		// without the lock, a registration could read the fence as absent
		// and commit after the purge's zero-row verification.
		if err := lockBaseLedger(ctx, tx, baseRepoOf(repo)); err != nil {
			return err
		}
		if fenced, err := repoPurgeFencedPG(ctx, tx, repo); err != nil {
			return err
		} else if fenced {
			return fmt.Errorf("register packs %s: %w", repo, ErrRepoPurged)
		}
		var credit int64
		adopted := false
		for _, p := range packs {
			assertf(p.ID != "" && p.Key != "", "RegisterPacks: empty pack id/key %+v", p)
			ct, err := tx.Exec(ctx,
				`INSERT INTO packs (repo, id, s3_key, bytes) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`,
				repo, p.ID, p.Key, p.Size)
			if err != nil {
				return err
			}
			if ct.RowsAffected() > 0 {
				c, err := registerLedgerPG(ctx, tx, repo, p)
				if err != nil {
					return err
				}
				credit += c
				adopted = adopted || (p.Adopted && c > 0)
			}
			// Settlement consumes BOTH era spellings (reservKeySpellings):
			// the key's bytes are in the usage counter now.
			if _, err := tx.Exec(ctx,
				`DELETE FROM pack_reservations WHERE key = ANY($1)`, reservKeySpellings(p.Key)); err != nil {
				return err
			}
		}
		if adopted {
			// A crash-retry adopted a published-but-rowless pack whose
			// bytes cached floors may already contain (Codex round 53) —
			// atomic with the credit, like RegisterAdoptedLFSObject.
			if _, err := tx.Exec(ctx, `
INSERT INTO lfs_adoption_gens (repo, gen) VALUES ($1, 1)
ON CONFLICT (repo) DO UPDATE SET gen = lfs_adoption_gens.gen + 1`, baseRepoOf(repo)); err != nil {
				return err
			}
		}
		return creditUsagePG(ctx, tx, repo, credit)
	})
}

// errReplaceLost aborts the ReplacePacks transaction (rollback) when the
// delete precondition fails; mapped to won=false by the callers below.
var errReplaceLost = errors.New("replace packs: old pack set changed")

// validateReplacePacks asserts the ReplacePacks argument invariants shared
// by both backends. Old ids must be unique: the delete-rowcount CAS counts
// rows, and a duplicated id would make len(old) unreachable.
func validateReplacePacks(repo string, old []Pack, newPack Pack) {
	assertf(repo != "", "ReplacePacks: empty repo")
	assertf(len(old) > 0, "ReplacePacks: empty old pack set")
	assertf(newPack.ID != "" && newPack.Key != "", "ReplacePacks: empty new pack id/key %+v", newPack)
	seen := make(map[string]bool, len(old))
	for _, p := range old {
		assertf(p.ID != "" && p.Key != "", "ReplacePacks: empty old pack id/key %+v", p)
		assertf(!seen[p.ID], "ReplacePacks: duplicate old pack id %q", p.ID)
		seen[p.ID] = true
	}
}

// ReplacePacks installs a repack fold in one transaction (see RefStore).
// The insert tolerates an existing (repo, id) row — pack ids derive from
// pack content, so a same-id row registered concurrently is byte-identical
// (RegisterPacks invariant) and keeping it is equivalent to inserting.
func (s *PG) ReplacePacks(ctx context.Context, repo string, old []Pack, newPack Pack) (won bool, err error) {
	validateReplacePacks(repo, old, newPack)
	ids := make([]string, len(old))
	for i, p := range old {
		ids[i] = p.ID
	}
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		ct, err := tx.Exec(ctx, `DELETE FROM packs WHERE repo = $1 AND id = ANY($2)`, repo, ids)
		if err != nil {
			return err
		}
		if int(ct.RowsAffected()) != len(old) {
			return errReplaceLost // another fold got here first: roll back
		}
		// Ledger (issue #31): the deleted rows' blobs LINGER under packs/
		// until a sweep reaps them, so their bytes stay in pack_usage
		// (nothing is debited here) and each key is recorded as retained —
		// the reclaim record and the sweeper's provably-folded class. The
		// insert precedes the new-pack insert on purpose: an identity fold
		// whose newPack.ID matches an input immediately consumes its own
		// retained row below, leaving the counter untouched (no new bytes).
		for _, p := range old {
			if _, err := tx.Exec(ctx, `
INSERT INTO pack_retained (repo, key, bytes) VALUES ($1, $2, $3)
ON CONFLICT (repo, key) DO UPDATE SET bytes = EXCLUDED.bytes, superseded_at = now()`,
				repo, p.Key, p.Size); err != nil {
				return err
			}
		}
		ct, err = tx.Exec(ctx,
			`INSERT INTO packs (repo, id, s3_key, bytes) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`,
			repo, newPack.ID, newPack.Key, newPack.Size)
		if err != nil {
			return err
		}
		if ct.RowsAffected() == 0 {
			return nil // row already registered: its bytes are already counted
		}
		credit, err := registerLedgerPG(ctx, tx, repo, newPack)
		if err != nil {
			return err
		}
		return creditUsagePG(ctx, tx, repo, credit)
	})
	if errors.Is(err, errReplaceLost) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("replace packs %s: %w", repo, err)
	}
	return true, nil
}

// HasPack reports whether repo has a registered pack with blob key.
func (s *PG) HasPack(ctx context.Context, repo, key string) (bool, error) {
	assertf(repo != "" && key != "", "HasPack(%q, %q)", repo, key)
	var exists bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM packs WHERE repo = $1 AND s3_key = $2)`, repo, key).Scan(&exists)
	return exists, err
}

// GetGeneration returns repo's mutation generation (0 = never mutated).
func (s *PG) GetGeneration(ctx context.Context, repo string) (int64, error) {
	assertf(repo != "", "GetGeneration: empty repo")
	var gen int64
	err := s.pool.QueryRow(ctx,
		`SELECT generation FROM repo_generations WHERE repo = $1`, repo).Scan(&gen)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil // never mutated: the first bump creates the row at 1
	}
	if err != nil {
		return 0, err
	}
	assertf(gen >= 1, "repo_generations holds non-positive generation %d for %s", gen, repo)
	return gen, nil
}

// ListPacks returns all registered packs for repo, oldest first.
func (s *PG) ListPacks(ctx context.Context, repo string) ([]Pack, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, s3_key FROM packs WHERE repo = $1 ORDER BY created_at, id`, repo)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var packs []Pack
	for rows.Next() {
		var p Pack
		if err := rows.Scan(&p.ID, &p.Key); err != nil {
			return nil, err
		}
		packs = append(packs, p)
	}
	return packs, rows.Err()
}
