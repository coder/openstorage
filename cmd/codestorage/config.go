// Serve/CLI configuration resolution: flags take precedence, then the
// canonical CODESTORAGE_* environment variables, then the SPIKE_*/legacy
// spike aliases (PG_URL, BLOB_URL) so phase-0 invocations keep working.
package main

import (
	"encoding/pem"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/coder/code-storage/internal/githttp"
	"github.com/coder/code-storage/internal/jobs"
	"github.com/coder/code-storage/internal/ref"
	"github.com/coder/code-storage/internal/secrets"
)

// Environment variable names, canonical first; envFirst scans left to right.
var (
	envPGURL   = []string{"CODESTORAGE_PG_URL", "SPIKE_PG_URL", "PG_URL"}
	envBlobURL = []string{"CODESTORAGE_S3_URL", "SPIKE_S3_URL", "BLOB_URL"}
)

const (
	envListen     = "CODESTORAGE_LISTEN"
	envOrg        = "CODESTORAGE_ORG"
	envOrgPubKeys = "CODESTORAGE_ORG_PUBKEYS"
	envBaseURL    = "CODESTORAGE_BASE_URL"
	envDataDir    = "CODESTORAGE_DATA_DIR"
	envRateRPS    = "CODESTORAGE_RATE_RPS"
	envRateBurst  = "CODESTORAGE_RATE_BURST"

	// Trusted-proxy allowlist for rate-limit client identity (issue #30):
	// comma-separated CIDR prefixes (bare IPs accepted as single-address
	// entries). Unset keeps RemoteAddr-only keying.
	envTrustedProxies = "CODESTORAGE_TRUSTED_PROXIES"

	// Per-repo pack-bytes quota on the git surface (issue #21): receive-pack
	// refuses pushes that would take a repo's packs/ prefix past this many
	// bytes. Unset applies the generous default; "0" disables enforcement.
	envPackQuota = "CODESTORAGE_PACK_QUOTA_BYTES"

	// Reservation TTL for the quota's durable admission ledger (issue #29):
	// how long a crashed writer's reservation keeps gating admission. Must
	// comfortably exceed the longest plausible server-side pack upload —
	// an expiring in-flight reservation reopens the overshoot the ledger
	// closes. Unset applies the one-hour default; must be positive.
	envPackReservationTTL = "CODESTORAGE_PACK_RESERVATION_TTL"

	// Decoded-byte cap on one git push (issue #55): receive-pack requests
	// whose body (after any gzip decode) exceeds this many bytes are
	// rejected with 413 before the stream reaches git. Unset derives the
	// cap from the pack quota (a push cannot legitimately exceed what
	// quota admission would accept, plus protocol overhead); "0" disables
	// the cap.
	envMaxPushBytes = "CODESTORAGE_MAX_PUSH_BYTES"

	// Per-push verified-commit budget (issue #59): the maximum number of
	// commits one push may signature-verify across all its refs before it
	// is rejected (fail closed). Unset applies the generous default; "0"
	// disables the budget.
	envVerifySigBudget = "CODESTORAGE_VERIFY_SIG_BUDGET"

	// Orphan-pack sweep (issue #5): interval between sweeps ("0" disables
	// sweeping), minimum blob age before an unregistered pack counts as an
	// orphan, and the explicit opt-in for actually deleting candidates
	// (list-only by default).
	//
	// Upgrade ordering for delete mode: enable it only once EVERY node
	// (serve replicas AND hook-running git nodes) runs a build with the
	// post-registration publish verify (blob.EnsurePublished). A mixed
	// fleet cannot lose data — quarantine plus the heal pass restore any
	// mis-reaped registered pack — but an old writer racing a delete-mode
	// sweep can see its freshly registered pack unavailable until the next
	// sweep heals it. List-only mode is safe on any fleet.
	envSweepInterval      = "CODESTORAGE_SWEEP_INTERVAL"
	envSweepAgeThreshold  = "CODESTORAGE_SWEEP_AGE_THRESHOLD"
	envSweepDeleteEnabled = "CODESTORAGE_SWEEP_DELETE_ENABLED"

	// Narrow superseded-class deletion (issue #31): reap aged blobs whose
	// registry rows a repack fold provably removed (durable pack_retained
	// records), even while general orphan deletion stays list-only. ON by
	// default — set "false" to disable. On S3 it degrades to list-only per
	// run when the bucket is versioned (the DeleteGuard), rather than
	// failing startup like the explicit general delete opt-in.
	envSweepDeleteSuperseded = "CODESTORAGE_SWEEP_DELETE_SUPERSEDED"

	// LFS-class deletion (issue #51): reap aged lfs/ blobs with no
	// lfs_objects registry row. OFF by default and separate from the
	// general delete opt-in: objects uploaded before the LFS registry
	// existed have no rows, so a fleet already running general deletes
	// must not start reaping its pre-existing LFS population on upgrade.
	// Enable only after list-only reports stop naming live objects
	// (write-scoped touches adopt stored objects into the registry).
	envSweepDeleteLFS = "CODESTORAGE_SWEEP_DELETE_LFS"
	// envSweepDeleteLFSLegacy additionally admits LEGACY-SPELLED LFS keys
	// to the LFS reap — the explicit post-roll cleanup opt-in of the
	// ADR-41 two-phase key migration (Codex round 81). Keep it off while
	// any pre-upgrade replica may still serve reads.
	envSweepDeleteLFSLegacy = "CODESTORAGE_SWEEP_DELETE_LFS_LEGACY"

	// Soft-deleted repository retention (issue #52): how long after its
	// soft-delete a repository's registry rows and blobs are permanently
	// reclaimed by a purge pass on the sweep cadence. Unset/0 disables the
	// background purge (soft-deleted repos are kept forever; the
	// `codestorage purge-repo` command still reclaims individually). When
	// set it must be at least jobs.MinRepoPurgeAge and sweeping must be
	// enabled (the purge rides the sweep schedule). Purged repositories
	// cannot be restored, and the purge releases the deleted repository's
	// burned (org, url) name — pick a retention comfortably above any
	// issued token lifetime.
	envRepoRetention = "CODESTORAGE_REPO_RETENTION"

	// Node-wide LFS upload staging budget (issue #51): proxy uploads
	// stage their full Content-Length on local disk before verification,
	// so a replica needs a disk bound independent of the per-repo quota
	// (which is per-repository and may be disabled). "0" disables.
	envLFSStagingBytes = "CODESTORAGE_LFS_STAGING_BYTES"
	// envLFSV020WriteCompat keeps the v0.2.0 LFS read surface alive
	// during a rolling upgrade (ADR-41): set to "1"/"true" on upgraded
	// replicas until the fleet rolls, then drop it.
	envLFSV020WriteCompat = "CODESTORAGE_LFS_V020_WRITE_COMPAT"

	// Unreferenced-LFS-object reclamation (issue #73): a bounded
	// background scan walks each repo's reachable pointer files and
	// retires registered lfs_objects rows no reachable pointer references
	// once they stay unreferenced for a full grace window — unwedging the
	// per-repo object-count cap after junk uploads. Interval 0 (the
	// default) disables the job entirely and is the emergency stop.
	envLFSGCInterval = "CODESTORAGE_LFS_GC_INTERVAL"
	// envLFSGCGrace is how long a row must stay CONTINUOUSLY observed
	// unreferenced before retirement (default 24h). Retirement re-checks
	// the grace inside the guarded delete, so any liveness signal —
	// batch/PUT touch, re-registration, a pointer push — restarts the
	// window.
	envLFSGCGrace = "CODESTORAGE_LFS_GC_GRACE"
	// envLFSGCObjectBudget caps the git objects one reclamation run may
	// list across its reachability walks (default 1,000,000). A repo
	// whose walk alone exceeds the full budget is skipped (loudly) until
	// the budget is raised; nothing is ever retired on a partial walk.
	envLFSGCObjectBudget = "CODESTORAGE_LFS_GC_OBJECT_BUDGET"
	// envLFSGCDeleteEnabled arms retirement. OFF by default: the scan
	// then only marks/clears rows and reports what it WOULD retire, so
	// operators can watch report-only passes before opting in — the same
	// staged rollout as the sweep delete flags.
	envLFSGCDeleteEnabled = "CODESTORAGE_LFS_GC_DELETE_ENABLED"

	// Per-repo LFS object-count cap override (issue #73 option 3): the
	// operator escape hatch that unwedges a cap-exhausted repository
	// without waiting for reclamation. Default 100000.
	envLFSMaxObjectsPerRepo = "CODESTORAGE_LFS_MAX_OBJECTS_PER_REPO"

	// Background repack (issue #1): registered-pack count at which a
	// post-write trigger schedules a fold ("0" disables triggering).
	envRepackThreshold = "CODESTORAGE_REPACK_THRESHOLD"

	// Webhook SSRF-guard escape hatch (issue #53): "true" admits
	// loopback/private/link-local/ULA delivery targets for genuinely
	// internal deployments. Default off; the redirect refusal is NOT
	// relaxed. Enabling it logs a boot-time warning.
	envWebhookAllowPrivate = "CODESTORAGE_WEBHOOK_ALLOW_PRIVATE"

	// At-rest envelope encryption for the metadata store's secret columns
	// (issue #54): comma-separated base64 32-byte AES-256 keys, first =
	// active encrypt key, rest decrypt-only (rotation). Unset keeps the
	// legacy plaintext behavior. Hook subprocesses inherit serve's
	// environment, so the same value reaches their meta stores.
	envSecretsKey = "CODESTORAGE_SECRETS_KEY"
	// Staging mode for enabling the key on multi-replica fleets:
	// "decrypt-only" loads the keys for reading but writes plaintext and
	// performs no rewrites, so a rolling restart never produces an envelope
	// some replica cannot read yet; "encrypt" (or unset) is normal
	// operation. Requires the key to be set.
	envSecretsMode = "CODESTORAGE_SECRETS_MODE"

	defaultListen = "127.0.0.1:0"
)

const (
	defaultSweepInterval     = time.Hour
	defaultSweepAgeThreshold = 24 * time.Hour
	defaultRepackThreshold   = 10

	// defaultPackQuotaBytes is the generous default per-repo pack-bytes
	// quota (10 GiB): far above any healthy repository's pack footprint,
	// while still bounding the packs/ growth a single repo-scoped
	// git:write token can inflict (issue #21).
	defaultPackQuotaBytes = int64(10) << 30

	// defaultLFSStagingBytes is the default node-wide LFS upload staging
	// budget (issue #51). Sized as a WORKING SET, not an object-size cap
	// (Codex round 38): per-repository byte fairness caps a single
	// repository's staged hold at budget/(div+1) of the free budget
	// (githttp's lfsRepoStagingByteShareDiv, div 2), so supporting an
	// object of size S needs a budget of about (div+1)·S — no budget
	// equal to the largest object can both stage that object and reserve
	// capacity for other repositories. 32 GiB keeps one
	// default-quota-sized (10 GiB) object stageable (32/3 ≈ 10.7 GiB)
	// with cross-repo capacity intact.
	defaultLFSStagingBytes = int64(32) << 30

	// defaultPackReservationTTL clears a default-quota-sized (10 GiB)
	// upload at ~3 MB/s before a live reservation can expire (issue #29).
	defaultPackReservationTTL = time.Hour

	// defaultMaxPushQuotaMultiple derives the default decoded push-size cap
	// from the pack quota (issue #55): a push cannot legitimately need more
	// than the quota admission would accept, so twice the quota leaves
	// ample room for commands, pkt-line framing, and multi-pack pushes
	// while still bounding ingress work at the same order of magnitude the
	// quota bounds retained storage.
	defaultMaxPushQuotaMultiple = 2

	// defaultMaxPushFloorBytes floors the DERIVED cap (Codex round 10): the
	// quota accepts any positive value, and a byte-scale quota must not
	// shrink the cap below protocol overhead — delete-only and zero-growth
	// pushes carry command lists (plus up to a 1 MiB client-controlled
	// shallow prefix the atomic sniffer reads) and are exactly the recovery
	// path the quota admits for over-quota repositories. 2 MiB covers that
	// comfortably while still bounding ingress work. An EXPLICIT
	// CODESTORAGE_MAX_PUSH_BYTES is taken verbatim.
	defaultMaxPushFloorBytes = int64(2) << 20

	// defaultVerifySigBudget bounds verifier spawns per push (issue #59).
	// Generous by design: a verifier spawn costs tens of milliseconds, so
	// 10000 admits any plausible signed-history push while capping the
	// abusive fan-out at minutes, not days.
	defaultVerifySigBudget = 10000
)

// envFirst returns the first non-empty value among names. getenv is injected
// so tests never mutate process env.
func envFirst(getenv func(string) string, names ...string) string {
	assertf(getenv != nil && len(names) > 0, "envFirst: missing getenv or names")
	for _, n := range names {
		if v := getenv(n); v != "" {
			return v
		}
	}
	return ""
}

// serveConfig is the fully resolved `serve` configuration.
type serveConfig struct {
	listen   string
	cacheDir string
	dbURL    string
	blobURL  string

	// dataDir, when set, is the local-mode data directory (always absolute):
	// database/blob/cache locations are derived from it for slots not set
	// explicitly (sqlite://<dir>/codestorage.db, file://<dir>/blobs,
	// <dir>/cache). Explicit URLs/flags always win.
	dataDir string

	// org/orgKeysPath configure single-org serve mode (ADR-8): the /api/v1
	// HTTP API and the git smart-HTTP path both authenticate against the
	// org's registered keys. Both are required.
	org         string
	orgKeysPath string

	// baseURL, when non-empty, is the externally reachable base
	// (scheme://host[:port], no trailing slash) used for `http_url` values
	// in API responses. Empty means derive it from the bound listen
	// address — which is unroutable when listening on 0.0.0.0.
	baseURL string

	// rateRPS/rateBurst configure the per-client-IP rate limit (docs/reference/http-api.md
	// gateway-level 429) applied to the REST surface and — with separate
	// bucket state — the git smart-HTTP surface (issue #21). Both zero
	// means disabled.
	rateRPS   float64
	rateBurst int

	// trustedProxies is the reverse-proxy allowlist for rate-limit client
	// identity (issue #30): when the TCP peer is inside one of these
	// prefixes, both limiters key buckets on the rightmost untrusted
	// X-Forwarded-For hop instead of RemoteAddr. Empty keeps
	// RemoteAddr-only keying (the default).
	trustedProxies []netip.Prefix

	// lfsStagingBytes caps aggregate in-flight LFS upload staging per
	// replica (issue #51); 0 disables.
	lfsStagingBytes int64

	// lfsV020WriteCompat dual-publishes LFS objects to the legacy
	// spelling during a rolling upgrade (ADR-41); default off.
	lfsV020WriteCompat bool
	// packQuotaBytes is the per-repo pack-bytes quota on the git surface
	// (issue #21); 0 disables enforcement.
	packQuotaBytes int64

	// packReservationTTL bounds how long a crashed writer's quota
	// reservation gates admission (issue #29); always positive.
	packReservationTTL time.Duration

	// maxPushBytes caps the decoded bytes one git push may stream into
	// receive-pack (issue #55); 0 disables the cap.
	maxPushBytes int64

	// verifySigBudget caps how many commits one push may signature-verify
	// (issue #59); 0 disables the budget.
	verifySigBudget int

	// sweep configures the periodic orphan-pack sweep (issue #5).
	sweep sweepSettings

	// lfsGC configures the unreferenced-LFS-object reclamation job
	// (issue #73); interval 0 disables it.
	lfsGC lfsGCSettings

	// lfsMaxObjectsPerRepo overrides the per-repo LFS object-count cap
	// (issue #73 option 3); always positive.
	lfsMaxObjectsPerRepo int64

	// repackThreshold is the registered-pack count at which a post-write
	// trigger schedules a background repack (issue #1); 0 disables
	// triggering (already-queued durable jobs still run).
	repackThreshold int

	// webhookAllowPrivate relaxes the webhook delivery SSRF guard's
	// private-range denial (issue #53); false (the default) keeps it
	// strict.
	webhookAllowPrivate bool

	// secretsCodec envelope-encrypts the metadata store's secret columns
	// (issue #54); nil (the default) keeps them plaintext.
	secretsCodec *secrets.Codec
}

// sweepSettings is the resolved orphan-pack sweep configuration.
type sweepSettings struct {
	interval         time.Duration // 0 = sweeping disabled
	ageThreshold     time.Duration // > 0
	deleteEnabled    bool          // false = list-only (the default)
	deleteSuperseded bool          // true = reap provably-folded blobs (the default, issue #31)
	deleteLFS        bool          // false = list-only for LFS orphans (the default, issue #51)
	deleteLFSLegacy  bool          // false = legacy-spelled LFS keys are never reaped (ADR-41 transition gate)
	repoRetention    time.Duration // 0 = never purge soft-deleted repos (the default, issue #52)
}

// lfsGCSettings is the resolved unreferenced-LFS-object reclamation
// configuration (issue #73).
type lfsGCSettings struct {
	interval      time.Duration // 0 = reclamation disabled (the default)
	grace         time.Duration // > 0; how long a row must stay unreferenced
	objectBudget  int64         // > 0; git objects one run may walk
	deleteEnabled bool          // false = report-only marking (the default)
}

// storageURLs is the resolved storage location set shared by every
// subcommand that opens the stores (serve, signing-key, list-packs):
// explicit env URLs win, then --data-dir/$CODESTORAGE_DATA_DIR derivation
// (local mode) fills the empty slots. Callers validate the slots they need.
type storageURLs struct {
	dataDir string // absolute when set; "" outside local mode
	dbURL   string // "" when neither env nor data-dir provided one
	blobURL string // "" when neither env nor data-dir provided one
	// cacheDir is derived from dataDir only ("" otherwise); serve applies
	// its own flag/default precedence on top.
	cacheDir string
}

// resolveStorageURLs merges the storage env vars with the local-mode data
// dir (flag > env). The data dir is made absolute up front: hooks re-exec
// this binary from a different working directory (the bare cache repo), so
// relative sqlite/file URLs would silently point somewhere else across the
// re-exec.
func resolveStorageURLs(getenv func(string) string, flagDataDir string) (storageURLs, error) {
	assertf(getenv != nil, "resolveStorageURLs: nil getenv")
	u := storageURLs{
		dbURL:   envFirst(getenv, envPGURL...),
		blobURL: envFirst(getenv, envBlobURL...),
		dataDir: firstNonEmpty(flagDataDir, getenv(envDataDir)),
	}
	if u.dataDir != "" {
		abs, err := filepath.Abs(u.dataDir)
		if err != nil {
			return storageURLs{}, fmt.Errorf("--data-dir %q: %w", u.dataDir, err)
		}
		u.dataDir = abs
		if u.dbURL == "" {
			u.dbURL = "sqlite://" + filepath.Join(abs, "codestorage.db")
		}
		if u.blobURL == "" {
			u.blobURL = "file://" + filepath.Join(abs, "blobs")
		}
		u.cacheDir = filepath.Join(abs, "cache")
	}
	assertf(u.dataDir == "" || filepath.IsAbs(u.dataDir), "resolveStorageURLs: dataDir %q not absolute", u.dataDir)
	return u, nil
}

// errDBURLRequired/errBlobURLRequired are the shared missing-slot errors so
// every subcommand names the same remediation (env URL or local mode).
func errDBURLRequired() error {
	return fmt.Errorf("database URL required: set %s (or legacy %s), or pass --data-dir / set %s for local mode", envPGURL[0], envPGURL[2], envDataDir)
}

func errBlobURLRequired() error {
	return fmt.Errorf("blob URL required: set %s (or legacy %s), or pass --data-dir / set %s for local mode", envBlobURL[0], envBlobURL[2], envDataDir)
}

// resolveServeConfig merges flags and environment into a validated config.
// Precedence per slot: explicit flag > explicit env > --data-dir derivation
// (local mode) > built-in default (cacheDir only).
func resolveServeConfig(getenv func(string) string, flagListen, flagAddr, flagCacheDir, flagDataDir string) (serveConfig, error) {
	assertf(getenv != nil, "resolveServeConfig: nil getenv")
	urls, err := resolveStorageURLs(getenv, flagDataDir)
	if err != nil {
		return serveConfig{}, err
	}
	cfg := serveConfig{
		listen:      firstNonEmpty(flagListen, flagAddr, getenv(envListen), defaultListen),
		cacheDir:    firstNonEmpty(flagCacheDir, urls.cacheDir),
		dbURL:       urls.dbURL,
		blobURL:     urls.blobURL,
		dataDir:     urls.dataDir,
		org:         getenv(envOrg),
		orgKeysPath: getenv(envOrgPubKeys),
	}
	if cfg.cacheDir == "" {
		cfg.cacheDir = filepath.Join(os.TempDir(), "codestorage-cache")
	}
	if cfg.dbURL == "" {
		return serveConfig{}, errDBURLRequired()
	}
	if cfg.blobURL == "" {
		return serveConfig{}, errBlobURLRequired()
	}
	if cfg.org == "" || cfg.orgKeysPath == "" {
		return serveConfig{}, fmt.Errorf("%s and %s must both be set (single-org serve mode)", envOrg, envOrgPubKeys)
	}
	if raw := getenv(envBaseURL); raw != "" {
		baseURL, err := normalizeBaseURL(raw)
		if err != nil {
			return serveConfig{}, fmt.Errorf("%s: %w", envBaseURL, err)
		}
		cfg.baseURL = baseURL
	}
	cfg.rateRPS, cfg.rateBurst, err = resolveRateLimit(getenv)
	if err != nil {
		return serveConfig{}, err
	}
	cfg.trustedProxies, err = resolveTrustedProxies(getenv)
	if err != nil {
		return serveConfig{}, err
	}
	cfg.packQuotaBytes, err = resolvePackQuota(getenv)
	if err != nil {
		return serveConfig{}, err
	}
	cfg.packReservationTTL, err = resolvePackReservationTTL(getenv)
	if err != nil {
		return serveConfig{}, err
	}
	cfg.lfsStagingBytes, err = resolveLFSStagingBytes(getenv)
	if err != nil {
		return serveConfig{}, err
	}
	if raw := getenv(envLFSV020WriteCompat); raw != "" {
		cfg.lfsV020WriteCompat, err = strconv.ParseBool(raw)
		if err != nil {
			return serveConfig{}, fmt.Errorf("%s: %w", envLFSV020WriteCompat, err)
		}
	}
	cfg.maxPushBytes, err = resolveMaxPushBytes(getenv, cfg.packQuotaBytes)
	if err != nil {
		return serveConfig{}, err
	}
	cfg.verifySigBudget, err = resolveVerifySigBudget(getenv)
	if err != nil {
		return serveConfig{}, err
	}
	cfg.sweep, err = resolveSweepSettings(getenv)
	if err != nil {
		return serveConfig{}, err
	}
	if cfg.lfsV020WriteCompat && cfg.sweep.deleteLFS && cfg.sweep.interval > 0 {
		// Rolling-upgrade tripwire (Codex round 101, P1): the compat flag
		// declares a mixed v0.2.0 fleet, and during that roll DESTRUCTIVE
		// LFS sweeping must be off FLEET-WIDE — the in-place row
		// migration makes a touched object's legacy blob rowless from a
		// v0.2.0 sweeper's perspective, and an old reap-enabled sweeper
		// (whose binary has no legacy gate) deletes it immediately,
		// breaking old-replica reads and freshly issued presigned GETs.
		// Old binaries cannot be guarded retroactively; failing startup
		// on the nodes we do control forces the staged rollout the docs
		// describe (list-only during the roll, opt-ins after). A zero
		// interval constructs no schedule at all (Codex round 111): the
		// delete flag is inert then, and rejecting it would block the
		// documented fleet-wide sweep emergency stop — re-enabling the
		// interval restarts the process and re-runs this validation.
		return serveConfig{}, fmt.Errorf(
			"%s and %s cannot be combined: destructive LFS sweeping must stay disabled fleet-wide (old v0.2.0 sweepers included) until the roll completes and the compat flag is dropped",
			envLFSV020WriteCompat, envSweepDeleteLFS)
	}
	cfg.lfsGC, err = resolveLFSGCSettings(getenv)
	if err != nil {
		return serveConfig{}, err
	}
	if cfg.lfsGC.deleteEnabled && cfg.sweep.interval > 0 {
		// Slot refunds must couple to bounded physical deletion (Codex
		// security round 13): retirement refunds the object-cap slot the
		// moment the row drops, but the stranded canonical blob is reaped
		// only by the sweeper's destructive LFS opt-in. Without it, a
		// write-scoped token can cycle upload → grace → retire →
		// re-upload and grow lfs/<repo>/ far past the cap that quota
		// floors and sweep listings rely on to stay bounded.
		if !cfg.sweep.deleteLFS {
			return serveConfig{}, fmt.Errorf(
				"%s requires destructive LFS sweeping (%s=true): retirement refunds object-cap slots whose stranded blobs only the LFS reap deletes — enable the reap, or keep reclamation report-only",
				envLFSGCDeleteEnabled, envSweepDeleteLFS)
		}
		// The refund cadence must not outrun physical reap eligibility: a
		// grace shorter than the sweeper's age threshold or its cadence
		// retires slots faster than the reap frees their blobs,
		// re-opening attacker-scaled transient growth of the prefix. The
		// floor also covers quarantine retention (Codex security round
		// 14): every reap strands one globally-scanned quarantine copy
		// for at least jobs.QuarantinePurgeFloor, so sub-hour cadences
		// would pile up quarantine generations the purge cannot free in
		// time.
		if floor := max(cfg.sweep.ageThreshold, cfg.sweep.interval, jobs.QuarantinePurgeFloor); cfg.lfsGC.grace < floor {
			return serveConfig{}, fmt.Errorf(
				"%s (%s) must be at least the physical reap cadence — max(%s, %s, quarantine retention %s) = %s — when %s is set",
				envLFSGCGrace, cfg.lfsGC.grace, envSweepAgeThreshold, envSweepInterval, jobs.QuarantinePurgeFloor, floor, envLFSGCDeleteEnabled)
		}
		// A stopped sweep schedule (interval 0, the fleet-wide destructive
		// emergency stop) is deliberately NOT rejected: reclamation
		// demotes itself to report-only at wiring instead (the round-111
		// pattern — the stop switch must never crash-loop the fleet).
	}
	cfg.lfsMaxObjectsPerRepo, err = resolveLFSMaxObjectsPerRepo(getenv)
	if err != nil {
		return serveConfig{}, err
	}
	cfg.repackThreshold, err = resolveRepackThreshold(getenv)
	if err != nil {
		return serveConfig{}, err
	}
	cfg.webhookAllowPrivate, err = resolveWebhookAllowPrivate(getenv)
	if err != nil {
		return serveConfig{}, err
	}
	cfg.secretsCodec, err = resolveSecretsCodec(getenv)
	if err != nil {
		return serveConfig{}, err
	}
	assertf(cfg.dbURL != "" && cfg.blobURL != "" && cfg.cacheDir != "", "resolveServeConfig: unresolved slot in %+v", cfg)
	assertf(cfg.dataDir == "" || filepath.IsAbs(cfg.dataDir), "resolveServeConfig: dataDir %q not absolute", cfg.dataDir)
	return cfg, nil
}

// resolveRateLimit parses CODESTORAGE_RATE_RPS / CODESTORAGE_RATE_BURST into
// the (rps, burst) pair for api.RateLimitConfig. Fail-fast semantics:
//
//   - both unset, or RPS explicitly 0 with burst unset/0 → (0, 0) disabled
//   - RPS > 0 requires BURST >= 1 (half-set pairs are misconfigurations)
//   - BURST set without RPS is a misconfiguration
//   - malformed, negative, NaN, or infinite values error naming the env var
func resolveRateLimit(getenv func(string) string) (float64, int, error) {
	assertf(getenv != nil, "resolveRateLimit: nil getenv")
	rpsRaw, burstRaw := getenv(envRateRPS), getenv(envRateBurst)

	var rps float64
	if rpsRaw != "" {
		v, err := strconv.ParseFloat(rpsRaw, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("%s: %w", envRateRPS, err)
		}
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
			return 0, 0, fmt.Errorf("%s must be a non-negative finite number, got %q", envRateRPS, rpsRaw)
		}
		rps = v
	}
	var burst int
	if burstRaw != "" {
		v, err := strconv.Atoi(burstRaw)
		if err != nil {
			return 0, 0, fmt.Errorf("%s: %w", envRateBurst, err)
		}
		if v < 0 {
			return 0, 0, fmt.Errorf("%s must be non-negative, got %q", envRateBurst, burstRaw)
		}
		burst = v
	}

	switch {
	case rps == 0 && burst == 0:
		return 0, 0, nil // disabled (unset or explicit zero)
	case rps == 0:
		return 0, 0, fmt.Errorf("%s must be set (> 0) when %s is set", envRateRPS, envRateBurst)
	case burst == 0:
		return 0, 0, fmt.Errorf("%s must be set (>= 1) when %s is set", envRateBurst, envRateRPS)
	}
	return rps, burst, nil
}

// resolveTrustedProxies parses CODESTORAGE_TRUSTED_PROXIES: a
// comma-separated CIDR allowlist (bare IPs accepted as single-address
// entries) of reverse proxies whose forwarded client identity both rate
// limiters may trust (issue #30). Fail-fast semantics matching
// resolveRateLimit: malformed, empty, or zone-scoped entries error naming
// the env var — validated even while rate limiting is disabled, so a typo
// surfaces at startup rather than when limits are later enabled. Unset
// means no trusted proxies (RemoteAddr-only keying, the default).
func resolveTrustedProxies(getenv func(string) string) ([]netip.Prefix, error) {
	assertf(getenv != nil, "resolveTrustedProxies: nil getenv")
	raw := getenv(envTrustedProxies)
	if raw == "" {
		return nil, nil
	}
	var prefixes []netip.Prefix
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			return nil, fmt.Errorf("%s: empty entry in %q", envTrustedProxies, raw)
		}
		if p, err := netip.ParsePrefix(entry); err == nil {
			// Normalize IPv4-mapped prefixes (::ffff:a.b.c.d/N) to IPv4:
			// matching unmaps peer addresses, so a stored 4-in-6 prefix
			// would never match anything (Codex round 1). Below /96 the
			// entry covers more than the mapped-IPv4 space and has no
			// IPv4 equivalent — reject rather than store dead config.
			if p.Addr().Is4In6() {
				if p.Bits() < 96 {
					return nil, fmt.Errorf("%s: IPv4-mapped prefix %q needs at least /96 (use an IPv4 CIDR or a real IPv6 range)", envTrustedProxies, entry)
				}
				p = netip.PrefixFrom(p.Addr().Unmap(), p.Bits()-96)
			}
			prefixes = append(prefixes, p)
			continue
		}
		addr, err := netip.ParseAddr(entry)
		if err != nil {
			return nil, fmt.Errorf("%s: %q is neither a CIDR prefix nor an IP address", envTrustedProxies, entry)
		}
		if addr.Zone() != "" {
			// netip.PrefixFrom on a zoned address yields the zero Prefix.
			return nil, fmt.Errorf("%s: %q must not carry a zone", envTrustedProxies, entry)
		}
		// Unmap so an IPv4-mapped spelling still matches IPv4 peers.
		addr = addr.Unmap()
		prefixes = append(prefixes, netip.PrefixFrom(addr, addr.BitLen()))
	}
	return prefixes, nil
}

// resolvePackQuota parses CODESTORAGE_PACK_QUOTA_BYTES (issue #21).
// Fail-fast semantics matching resolveRateLimit: malformed or negative
// values error naming the env var. Unset applies the generous default;
// an explicit "0" disables quota enforcement.
func resolvePackQuota(getenv func(string) string) (int64, error) {
	assertf(getenv != nil, "resolvePackQuota: nil getenv")
	raw := getenv(envPackQuota)
	if raw == "" {
		return defaultPackQuotaBytes, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", envPackQuota, err)
	}
	if v < 0 {
		return 0, fmt.Errorf("%s must be non-negative (0 disables the quota), got %q", envPackQuota, raw)
	}
	return v, nil
}

// resolveMaxPushBytes parses CODESTORAGE_MAX_PUSH_BYTES (issue #55).
// Fail-fast semantics matching resolvePackQuota: malformed or negative
// values error naming the env var. Unset derives the cap from the pack
// quota (defaultMaxPushQuotaMultiple × quota, saturating; a disabled quota
// leaves the cap disabled too — the operator opted out of size bounds);
// an explicit "0" disables the cap.
func resolveMaxPushBytes(getenv func(string) string, packQuotaBytes int64) (int64, error) {
	assertf(getenv != nil, "resolveMaxPushBytes: nil getenv")
	assertf(packQuotaBytes >= 0, "resolveMaxPushBytes: negative quota %d", packQuotaBytes)
	raw := getenv(envMaxPushBytes)
	if raw == "" {
		if packQuotaBytes == 0 {
			return 0, nil
		}
		if packQuotaBytes > math.MaxInt64/defaultMaxPushQuotaMultiple {
			return math.MaxInt64, nil
		}
		return max(defaultMaxPushQuotaMultiple*packQuotaBytes, defaultMaxPushFloorBytes), nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", envMaxPushBytes, err)
	}
	if v < 0 {
		return 0, fmt.Errorf("%s must be non-negative (0 disables the cap), got %q", envMaxPushBytes, raw)
	}
	return v, nil
}

// resolveVerifySigBudget parses CODESTORAGE_VERIFY_SIG_BUDGET (issue #59).
// Fail-fast semantics matching resolvePackQuota: malformed or negative
// values error naming the env var. Unset applies the generous default; an
// explicit "0" disables the budget.
func resolveVerifySigBudget(getenv func(string) string) (int, error) {
	assertf(getenv != nil, "resolveVerifySigBudget: nil getenv")
	raw := getenv(envVerifySigBudget)
	if raw == "" {
		return defaultVerifySigBudget, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", envVerifySigBudget, err)
	}
	if v < 0 {
		return 0, fmt.Errorf("%s must be non-negative (0 disables the budget), got %q", envVerifySigBudget, raw)
	}
	return v, nil
}

// resolvePackReservationTTL parses CODESTORAGE_PACK_RESERVATION_TTL
// (issue #29). Fail-fast: malformed or non-positive values error naming the
// env var (a zero TTL would expire live reservations instantly, silently
// reopening the overshoot), and the TTL must strictly exceed the
// usage-floor cache lifetime (Codex round 36 on #51: at or below it, a
// cached floor outlives the reservations covering objects published after
// that listing — see githttp.MinPackReservationTTL). Unset applies the
// one-hour default.
func resolvePackReservationTTL(getenv func(string) string) (time.Duration, error) {
	assertf(getenv != nil, "resolvePackReservationTTL: nil getenv")
	raw := getenv(envPackReservationTTL)
	if raw == "" {
		return defaultPackReservationTTL, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", envPackReservationTTL, err)
	}
	if d <= githttp.MinPackReservationTTL {
		return 0, fmt.Errorf("%s must exceed the %v usage-floor freshness budget (cache lifetime + admission handoff margin), got %q",
			envPackReservationTTL, githttp.MinPackReservationTTL, raw)
	}
	return d, nil
}

// resolveLFSStagingBytes parses CODESTORAGE_LFS_STAGING_BYTES (issue #51):
// the node-wide cap on bytes of proxy LFS uploads staging on this replica
// at once. Fail-fast like resolvePackQuota; unset applies the default, an
// explicit "0" disables the bound.
func resolveLFSStagingBytes(getenv func(string) string) (int64, error) {
	assertf(getenv != nil, "resolveLFSStagingBytes: nil getenv")
	raw := getenv(envLFSStagingBytes)
	if raw == "" {
		return defaultLFSStagingBytes, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", envLFSStagingBytes, err)
	}
	if v < 0 {
		return 0, fmt.Errorf("%s must be non-negative (0 disables the staging bound), got %q", envLFSStagingBytes, raw)
	}
	return v, nil
}

// resolveSweepSettings parses the orphan-pack sweep env vars. Fail-fast
// semantics matching resolveRateLimit: malformed or negative values error
// naming the env var. Defaults: sweep hourly, 24h age threshold, list-only
// for general orphans, delete-enabled for the provably-folded superseded
// class (issue #31). CODESTORAGE_SWEEP_INTERVAL=0 disables sweeping
// entirely.
func resolveSweepSettings(getenv func(string) string) (sweepSettings, error) {
	assertf(getenv != nil, "resolveSweepSettings: nil getenv")
	s := sweepSettings{interval: defaultSweepInterval, ageThreshold: defaultSweepAgeThreshold, deleteSuperseded: true}
	if raw := getenv(envSweepInterval); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return sweepSettings{}, fmt.Errorf("%s: %w", envSweepInterval, err)
		}
		if d < 0 {
			return sweepSettings{}, fmt.Errorf("%s must be non-negative (0 disables sweeping), got %q", envSweepInterval, raw)
		}
		s.interval = d
	}
	if raw := getenv(envSweepAgeThreshold); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return sweepSettings{}, fmt.Errorf("%s: %w", envSweepAgeThreshold, err)
		}
		if d <= 0 {
			return sweepSettings{}, fmt.Errorf("%s must be positive, got %q", envSweepAgeThreshold, raw)
		}
		s.ageThreshold = d
	}
	if raw := getenv(envSweepDeleteEnabled); raw != "" {
		v, err := strconv.ParseBool(raw)
		if err != nil {
			return sweepSettings{}, fmt.Errorf("%s: %w", envSweepDeleteEnabled, err)
		}
		s.deleteEnabled = v
	}
	if raw := getenv(envSweepDeleteSuperseded); raw != "" {
		v, err := strconv.ParseBool(raw)
		if err != nil {
			return sweepSettings{}, fmt.Errorf("%s: %w", envSweepDeleteSuperseded, err)
		}
		s.deleteSuperseded = v
	}
	if raw := getenv(envSweepDeleteLFS); raw != "" {
		v, err := strconv.ParseBool(raw)
		if err != nil {
			return sweepSettings{}, fmt.Errorf("%s: %w", envSweepDeleteLFS, err)
		}
		s.deleteLFS = v
	}
	if raw := getenv(envSweepDeleteLFSLegacy); raw != "" {
		v, err := strconv.ParseBool(raw)
		if err != nil {
			return sweepSettings{}, fmt.Errorf("%s: %w", envSweepDeleteLFSLegacy, err)
		}
		s.deleteLFSLegacy = v
	}
	if raw := getenv(envRepoRetention); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return sweepSettings{}, fmt.Errorf("%s: %w", envRepoRetention, err)
		}
		switch {
		case d < 0:
			return sweepSettings{}, fmt.Errorf("%s must be non-negative (0 disables the background purge), got %q", envRepoRetention, raw)
		case d > 0 && d < jobs.MinRepoPurgeAge:
			// The purge floor bounds in-flight pre-delete writers (see
			// jobs.MinRepoPurgeAge); a shorter retention would silently
			// behave like the floor, so reject it loudly instead.
			return sweepSettings{}, fmt.Errorf("%s must be at least %v (or 0 to disable), got %q", envRepoRetention, jobs.MinRepoPurgeAge, raw)
		case d > 0 && s.interval == 0:
			// The purge rides the sweep schedule; retention with sweeping
			// disabled would silently never run.
			return sweepSettings{}, fmt.Errorf("%s requires sweeping enabled (%s > 0)", envRepoRetention, envSweepInterval)
		}
		s.repoRetention = d
	}
	return s, nil
}

// resolveLFSGCSettings parses the unreferenced-LFS-object reclamation
// knobs (issue #73), fail-fast like the sweep settings.
func resolveLFSGCSettings(getenv func(string) string) (lfsGCSettings, error) {
	assertf(getenv != nil, "resolveLFSGCSettings: nil getenv")
	s := lfsGCSettings{grace: jobs.DefaultLFSReclaimGrace, objectBudget: jobs.DefaultLFSReclaimObjectBudget}
	if raw := getenv(envLFSGCInterval); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return lfsGCSettings{}, fmt.Errorf("%s: %w", envLFSGCInterval, err)
		}
		if d < 0 {
			return lfsGCSettings{}, fmt.Errorf("%s must be non-negative (0 disables reclamation), got %q", envLFSGCInterval, raw)
		}
		s.interval = d
	}
	if raw := getenv(envLFSGCGrace); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return lfsGCSettings{}, fmt.Errorf("%s: %w", envLFSGCGrace, err)
		}
		if d <= 0 {
			return lfsGCSettings{}, fmt.Errorf("%s must be positive, got %q", envLFSGCGrace, raw)
		}
		s.grace = d
	}
	if raw := getenv(envLFSGCObjectBudget); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return lfsGCSettings{}, fmt.Errorf("%s: %w", envLFSGCObjectBudget, err)
		}
		if v <= 0 {
			return lfsGCSettings{}, fmt.Errorf("%s must be positive, got %q", envLFSGCObjectBudget, raw)
		}
		s.objectBudget = v
	}
	if raw := getenv(envLFSGCDeleteEnabled); raw != "" {
		v, err := strconv.ParseBool(raw)
		if err != nil {
			return lfsGCSettings{}, fmt.Errorf("%s: %w", envLFSGCDeleteEnabled, err)
		}
		if v && s.interval == 0 {
			// Arming deletes with the job disabled would silently never
			// retire anything; reject the misconfiguration loudly.
			return lfsGCSettings{}, fmt.Errorf("%s requires the reclamation schedule enabled (%s > 0)", envLFSGCDeleteEnabled, envLFSGCInterval)
		}
		s.deleteEnabled = v
	}
	return s, nil
}

// resolveLFSMaxObjectsPerRepo parses the per-repo LFS object-count cap
// override (issue #73 option 3). Unset keeps the built-in default; the cap
// must stay positive — 0 would brick every LFS upload, which is what
// disabling LFS-less deployments already get for free.
func resolveLFSMaxObjectsPerRepo(getenv func(string) string) (int64, error) {
	assertf(getenv != nil, "resolveLFSMaxObjectsPerRepo: nil getenv")
	raw := getenv(envLFSMaxObjectsPerRepo)
	if raw == "" {
		return ref.MaxLFSObjectsPerRepo, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", envLFSMaxObjectsPerRepo, err)
	}
	if v <= 0 {
		return 0, fmt.Errorf("%s must be positive, got %q", envLFSMaxObjectsPerRepo, raw)
	}
	return v, nil
}

// resolveRepackThreshold parses CODESTORAGE_REPACK_THRESHOLD. Fail-fast
// semantics matching the sweep settings: malformed values error naming the
// env var. 0 disables triggering; 1 is rejected (folding fewer than two
// packs is a no-op, so a 1-threshold would busy-loop the trigger); unset
// uses the default.
func resolveRepackThreshold(getenv func(string) string) (int, error) {
	assertf(getenv != nil, "resolveRepackThreshold: nil getenv")
	raw := getenv(envRepackThreshold)
	if raw == "" {
		return defaultRepackThreshold, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", envRepackThreshold, err)
	}
	if v < 0 || v == 1 {
		return 0, fmt.Errorf("%s must be 0 (disabled) or >= 2, got %q", envRepackThreshold, raw)
	}
	return v, nil
}

// resolveWebhookAllowPrivate parses CODESTORAGE_WEBHOOK_ALLOW_PRIVATE
// (issue #53): unset/false keeps the webhook delivery SSRF guard strict.
// Fail-fast semantics: a malformed value is a boot error, never a silent
// fallback in either direction.
func resolveWebhookAllowPrivate(getenv func(string) string) (bool, error) {
	assertf(getenv != nil, "resolveWebhookAllowPrivate: nil getenv")
	raw := getenv(envWebhookAllowPrivate)
	if raw == "" {
		return false, nil
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s: %w", envWebhookAllowPrivate, err)
	}
	return v, nil
}

// resolveSecretsCodec parses CODESTORAGE_SECRETS_KEY plus the
// CODESTORAGE_SECRETS_MODE staging switch (issue #54): unset key keeps the
// metadata store's secret columns plaintext; a malformed key list or mode
// is a boot error (secrets.ParseKeys never echoes key material). Shared by
// serve and the signing-key CLI so every store handle in the process tree
// carries identical secret semantics.
func resolveSecretsCodec(getenv func(string) string) (*secrets.Codec, error) {
	assertf(getenv != nil, "resolveSecretsCodec: nil getenv")
	codec, err := secrets.ParseKeys(getenv(envSecretsKey))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", envSecretsKey, err)
	}
	switch mode := getenv(envSecretsMode); mode {
	case "", "encrypt":
		// Normal operation.
	case "decrypt-only":
		if codec == nil {
			return nil, fmt.Errorf("%s=decrypt-only requires %s to be set", envSecretsMode, envSecretsKey)
		}
		codec.DecryptOnly = true
	default:
		return nil, fmt.Errorf("%s: want \"encrypt\" or \"decrypt-only\", got %q", envSecretsMode, mode)
	}
	return codec, nil
}

// normalizeBaseURL validates an externally reachable http(s) base URL and
// strips any trailing slash (http_url values append "/<repo_id>.git").
func normalizeBaseURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("want http(s)://host[:port], got %q", raw)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("base URL must not carry a query or fragment: %q", raw)
	}
	return strings.TrimRight(u.String(), "/"), nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// splitPEMBlocks re-encodes each PEM block of data as its own string, the
// shape meta.Store.UpsertOrgWithKeys expects. Errors when no block decodes.
func splitPEMBlocks(data []byte) ([]string, error) {
	var blocks []string
	for {
		var b *pem.Block
		b, data = pem.Decode(data)
		if b == nil {
			break
		}
		blocks = append(blocks, string(pem.EncodeToMemory(b)))
	}
	if len(blocks) == 0 {
		return nil, errors.New("no PEM blocks found")
	}
	return blocks, nil
}

// assertf is a fail-fast invariant check (matches the internal packages'
// crash-and-burn style). Only for impossible conditions, never input errors.
func assertf(cond bool, format string, args ...any) {
	if !cond {
		panic(fmt.Sprintf("codestorage assertion failed: "+format, args...))
	}
}
