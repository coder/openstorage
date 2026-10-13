// openstorage is the service binary: `serve` runs a stateless node exposing
// the /api/v1 HTTP API (single-org self-host mode) and the git smart-HTTP
// path; `hook` is re-exec'd by git as pre-receive/update hooks;
// `signing-key` manages the org's commit-signing key registry (docs/reference/authentication.md
// verify-sig); keygen/mint/list-packs are local dogfooding helpers.
//
// Configuration comes from OPENSTORAGE_* environment variables, with the
// phase-0 spike names (SPIKE_*, PG_URL, BLOB_URL) kept as fallback aliases —
// see config.go.
package main

import (
	"bytes"
	"context"
	"crypto/rsa"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/coder/openstorage/internal/api"
	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/envconfig"
	"github.com/coder/openstorage/internal/githttp"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/metrics"
	"github.com/coder/openstorage/internal/ref"
	"github.com/coder/openstorage/internal/ref/s3wal"
	"github.com/coder/openstorage/internal/version"
)

func main() {
	if len(os.Args) < 2 {
		fatalf("usage: openstorage <serve|hook|keygen|mint|signing-key|list-packs|purge-repo|s3wal-meta|version> ...")
	}
	// Refuse old-only CODESTORAGE_* settings before any command reads config.
	// Hooks inherit the serve environment, which already passed this check.
	if cmd := os.Args[1]; cmd != "hook" && cmd != "version" && cmd != "--version" {
		if err := envconfig.CheckLegacy(); err != nil {
			fatalf("openstorage %s: %v", cmd, err)
		}
	}
	ctx := context.Background()
	var err error
	switch os.Args[1] {
	case "serve":
		err = cmdServe(ctx, os.Args[2:])
	case "hook":
		// Eventing wiring for the git surface (docs/reference/webhooks.md): decorate the
		// update hook's ref store so won pushes emit push events. No-op
		// (identity default) when the serve env is absent.
		hookCleanup := wireHookEventing(ctx, slogForHook())
		err = githttp.RunHook(ctx, os.Args[2:])
		hookCleanup()
	case "keygen":
		err = cmdKeygen(os.Args[2:])
	case "mint":
		err = cmdMint(os.Args[2:])
	case "signing-key":
		err = cmdSigningKey(ctx, os.Args[2:])
	case "list-packs":
		err = cmdListPacks(ctx, os.Args[2:])
	case "purge-repo":
		err = cmdPurgeRepo(ctx, os.Args[2:])
	case "s3wal-meta":
		err = cmdS3WALMeta(ctx, os.Args[2:])
	case "version", "--version":
		fmt.Println(version.String())
	default:
		err = fmt.Errorf("unknown subcommand %q", os.Args[1])
	}
	if err != nil {
		fatalf("openstorage %s: %v", os.Args[1], err)
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

// leaseReleaseTimeout bounds the shutdown-path instance-lease Release —
// long enough for one Get + Delete round trip, short enough that a stalled
// store never hangs an otherwise graceful shutdown.
const leaseReleaseTimeout = 5 * time.Second

// shutdownDrainTimeout bounds the graceful HTTP drain after a termination
// signal: long enough for ordinary requests to finish, short enough that a
// long-running push cannot hold the process past a typical supervisor kill
// grace window. Cutting a drain short is safe by construction — pack data
// publishes before any ref CAS, so an interrupted push loses the client's
// attempt, never data (docs/how-to/deploy-production.md).
const shutdownDrainTimeout = 10 * time.Second

// shutdownRunWait is the part of shutdownDrainTimeout that scheduled sync
// and push runs get to finish, before the listener stops, while it still
// serves their loopback pushes (issue #342). Runs still going then are
// cancelled and keep their requests (ADR-50), so a longer wait would only
// save reruns.
const shutdownRunWait = 5 * time.Second

// shutdownRestoreWait bounds how long cancelled runs get to record their
// kept requests (one SQLite row write each) before the HTTP drain starts,
// so the restore commits inside a supervisor's kill grace window.
const shutdownRestoreWait = 2 * time.Second

func cmdServe(ctx context.Context, args []string) error {
	// A termination signal cancels the serve context (issue #174): the
	// select at the bottom then drains HTTP and RETURNS, letting every
	// deferred cleanup run — most visibly the s3wal instance-lease
	// Release, without which each restart makes the successor wait out
	// the full lease TTL. Portable: syscall.SIGTERM is defined on every
	// platform Go supports (a no-op signal value on Windows, where
	// os.Interrupt still covers Ctrl+C).
	ctx, stopSignals := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	listen := fs.String("listen", "", "listen address (falls back to --addr, then $"+envListen+", then "+defaultListen+")")
	addr := fs.String("addr", "", "spike-compat alias for --listen")
	cacheDir := fs.String("cache-dir", "", "rebuildable cache directory for bare repos (default: $TMPDIR/codestorage-cache)")
	dataDir := fs.String("data-dir", "", "local-mode data directory (falls back to $"+envDataDir+"); derives the database/blob/cache locations for slots not set explicitly")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := resolveServeConfig(envconfig.LookupEnv, *listen, *addr, *cacheDir, *dataDir)
	if err != nil {
		return err
	}
	// The pre-receive/update hooks re-exec this binary with the server's
	// inherited environment (internal/githttp/hook.go), so propagate the
	// resolved values under the canonical names regardless of which alias or
	// --data-dir derivation set them; the bare names keep older hook binaries
	// sharing this cache working.
	for _, kv := range [][2]string{
		{"OPENSTORAGE_PG_URL", cfg.dbURL}, {"OPENSTORAGE_S3_URL", cfg.blobURL},
		{"PG_URL", cfg.dbURL}, {"BLOB_URL", cfg.blobURL},
	} {
		if err := os.Setenv(kv[0], kv[1]); err != nil {
			return err
		}
	}

	// Per-repo LFS object-count cap override (issue #73 option 3): the
	// package-level cap is read by every LFS registration transaction and
	// admission fast-fail, all of which run in THIS serve process (the
	// hook subprocess registers packs, never LFS objects); set it once
	// before any store or handler exists.
	ref.MaxLFSObjectsPerRepo = cfg.lfsMaxObjectsPerRepo

	st, err := ref.Open(ctx, cfg.dbURL)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.InitSchema(ctx); err != nil {
		return fmt.Errorf("init ref schema: %w", err)
	}

	// Instance lease (issue #133, plan §2.11): the best-effort dual-serve
	// guard for the zero-infra mode, s3wal scheme only. Acquired AFTER
	// InitSchema so the §2.6 probe has already proven the store's
	// conditional-write fidelity — the lease CAS means nothing on a store
	// that ignores the headers. Refuses startup over a fresh foreign
	// lease; on heartbeat loss the process fail-stops (crash, not limp —
	// two live writers would corrupt nothing, CAS serializes every state
	// change, but §2.4 TTL verdicts and caches degrade silently).
	if s3, ok := st.(*ref.S3WAL); ok {
		if cfg.s3walLeaseTTL < 0 {
			// Explicit "0": test suites deliberately run several
			// processes against one bucket. Never silent.
			fmt.Fprintf(os.Stderr, "warning: %s=0 disables the s3wal instance lease — accidental dual-serve topologies will NOT be detected\n", envS3WALLeaseTTL)
		} else {
			lease, err := s3wal.AcquireInstanceLease(ctx, s3.ConditionalStore(), s3wal.LeaseConfig{TTL: cfg.s3walLeaseTTL})
			if err != nil {
				return err
			}
			metrics.SetS3WALLeaseHeld(true) // ADR-48: local state, not proof of ownership
			defer func() {
				// Release is best-effort by design (an abandoned lease
				// expires on its TTL), so bound it: a stalled endpoint
				// must not hang an otherwise graceful shutdown.
				metrics.SetS3WALLeaseHeld(false)
				rctx, cancel := context.WithTimeout(context.Background(), leaseReleaseTimeout)
				defer cancel()
				lease.Release(rctx)
			}()
			go func() {
				if err := lease.Heartbeat(ctx); err != nil {
					metrics.SetS3WALLeaseHeld(false)
					fatalf("openstorage serve: %v", err)
				}
			}()
		}
	}

	bl, err := blob.Open(ctx, cfg.blobURL)
	if err != nil {
		return err
	}
	defer bl.Close()
	if err := bl.EnsureBucket(ctx); err != nil {
		return err
	}

	// Listen before building handlers: the default listen address uses an
	// ephemeral port (127.0.0.1:0), and the API needs the bound address to
	// build the `http_url` values it returns. $OPENSTORAGE_BASE_URL
	// overrides the derived base, which is unroutable when binding 0.0.0.0.
	ln, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		return err
	}
	defer ln.Close()
	// Opt-in metrics listener (ADR-48): its own socket, server, and mux.
	// Bind it before anything is announced or served; a bad or busy
	// address fails startup instead of silently disabling metrics.
	var metricsLn net.Listener
	if cfg.metricsListen != "" {
		metricsLn, err = net.Listen("tcp", cfg.metricsListen)
		if err != nil {
			return fmt.Errorf("%s=%q: metrics listener: %w", envMetricsListen, cfg.metricsListen, err)
		}
		defer metricsLn.Close()
		if metrics.IsUnspecifiedBind(cfg.metricsListen) {
			fmt.Fprintf(os.Stderr, "warning: %s=%s binds every interface and the metrics listener has no authentication; restrict it to loopback or a private network only the scraper can reach\n", envMetricsListen, cfg.metricsListen)
		}
	}
	httpBaseURL := cfg.baseURL
	if httpBaseURL == "" {
		httpBaseURL = "http://" + ln.Addr().String()
	}
	// The sync worker's loopback pushes must terminate in THIS process
	// (issue #45): the external base URL often points at a load balancer,
	// which could route the push to a replica that cannot verify the
	// minting process's in-memory loopback key.
	loopbackBaseURL := selfLoopbackURL(ln.Addr())

	mux := http.NewServeMux()

	// Load the org's public keys, validating every block up front (crash
	// early on bad key material). Single-org mode is mandatory (ADR-8).
	pemData, err := os.ReadFile(cfg.orgKeysPath)
	if err != nil {
		return err
	}
	if _, err := auth.ParsePublicKeysPEM(pemData); err != nil {
		return fmt.Errorf("%s: %w", cfg.orgKeysPath, err)
	}
	orgPEMBlocks, err := splitPEMBlocks(pemData)
	if err != nil {
		return fmt.Errorf("%s: %w", cfg.orgKeysPath, err)
	}

	if err := os.MkdirAll(cfg.cacheDir, 0o755); err != nil {
		return err
	}
	// Fail at boot, not mid-push (Codex round-25 P2 on #173): a cache
	// root deep enough that push-hold bookkeeping paths would exceed the
	// OS PATH_MAX must be an operator-visible configuration error.
	if err := githttp.ValidateCacheDir(cfg.cacheDir); err != nil {
		return err
	}

	// HTTP API (single-org self-host mode). The read endpoints share the
	// ref/blob stores and cache dir with the githttp fallback below.
	// The REST pack writers' admission floor late-binds to the git
	// server's shared single-flight cache (issue #51, Codex round 21):
	// the API handler is built before the git server (it produces the
	// verifier the git server needs), and gitSrv is assigned strictly
	// before the listener serves any request.
	var gitSrv *githttp.Server
	usageFloor := func(ctx context.Context, repo string) (int64, int64, error) {
		assertf(gitSrv != nil, "usageFloor called before the git server was wired")
		return gitSrv.CachedUsageFloorGen(ctx, repo)
	}
	apiHandler, metaStore, verifier, maybeRepack, drainRuns, cleanup, err := buildAPIHandler(ctx, cfg.dbURL, cfg.org, httpBaseURL, loopbackBaseURL, orgPEMBlocks, st, bl, cfg.cacheDir,
		api.RateLimitConfig{RPS: cfg.rateRPS, Burst: cfg.rateBurst, TrustedProxies: cfg.trustedProxies}, cfg.packQuotaBytes, cfg.packReservationTTL, cfg.sweep, cfg.lfsGC, cfg.absence, cfg.lfsV020WriteCompat, cfg.repackThreshold, cfg.webhookAllowPrivate, cfg.syncEgress, cfg.secretsCodec, usageFloor)
	if err != nil {
		return err
	}
	defer cleanup()
	// The API handler owns the whole /api subtree: /api/v1/*, the named-repo
	// aliases /api/repos/{repo_name}/*, and /api/repo-urls/{repo_id}.
	// Mounting only /api/v1/ would strand the phase-6 routes on the git
	// fallback handler below.
	mux.Handle("/api/", apiHandler)

	// Git smart-HTTP path, authenticated with the same verifier as the API
	// (ADR-8: iss-resolved org keys; the spike's single-key mode is gone).
	srv := githttp.NewServer(st, bl, cfg.cacheDir, verifier, cfg.org)
	gitSrv = srv // late-binds the REST writers' usageFloor above
	// Post-push repack trigger (issue #1): pre-receive registered any
	// pushed packs strictly before receive-pack returned, so the threshold
	// check runs in the serve process for BOTH queue backends (the local
	// hook subprocess has no channel to the in-process queue).
	srv.MaybeRepack = maybeRepack
	srv.PackQuotaBytes = cfg.packQuotaBytes
	srv.PackReservationTTL = cfg.packReservationTTL
	// LFS proxy transfer hrefs must be externally routable (issue #51):
	// same canonical base the API's http_url values use.
	srv.BaseURL = httpBaseURL
	srv.LFSStagingBytes = cfg.lfsStagingBytes
	srv.LFSV020WriteCompat = cfg.lfsV020WriteCompat
	srv.MaxPushBytes = cfg.maxPushBytes
	srv.VerifySigBudget = cfg.verifySigBudget
	// docs/explanation/git-protocol-and-namespaces.md git URLs address repos by repo_id (name-style urls accepted
	// too, ADR-21); the resolved RepoID is the only storage key, and the
	// record backs claim matching for url- or id-carrying tokens (docs/reference/authentication.md).
	org := cfg.org
	srv.ResolveRepo = func(ctx context.Context, base string) (meta.Repo, bool) {
		repo, err := meta.ResolveRepoKey(ctx, metaStore, org, base)
		if err != nil {
			if !errors.Is(err, meta.ErrNotFound) {
				fmt.Fprintf(os.Stderr, "resolve repo %q: %v\n", base, err)
			}
			return meta.Repo{}, false
		}
		return repo, true
	}
	// The same rate-limit knob covers the git surface (issue #21):
	// receive-pack uploads packs before any ref policy runs, so the git
	// routes must not stay outside the limiter. Separate bucket state from
	// the REST limiter — one surface exhausting its budget must not starve
	// the other.
	gitLimit, err := api.NewGitRateLimit(api.RateLimitConfig{RPS: cfg.rateRPS, Burst: cfg.rateBurst, TrustedProxies: cfg.trustedProxies})
	if err != nil {
		return err
	}
	mux.Handle("/", gitLimit(srv.Handler()))

	// Unauthenticated liveness probe: lets quickstarts and compose
	// healthchecks confirm this server (not a stale process squatting the
	// port) is answering before any authenticated call.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintln(w, `{"status":"ok"}`)
	})

	fmt.Printf("listening on %s\n", ln.Addr())
	// Printed after the API line so parsers of the first stdout line keep
	// working; the bind itself already happened above.
	var metricsSrv *http.Server
	metricsErr := make(chan error, 1)
	if metricsLn != nil {
		fmt.Printf("metrics listening on %s\n", metricsLn.Addr())
		metricsSrv = metrics.NewServer()
		go func() { metricsErr <- metricsSrv.Serve(metricsLn) }()
	}
	// Serve on an http.Server rather than bare http.Serve: only Shutdown
	// gives the termination signal a drain seam. In-flight requests keep
	// their own request contexts (and the stores hold no long-lived
	// context), so work admitted before the signal completes normally
	// within the bounded drain.
	// Instrument the whole mux (ADR-48) so its own redirects count too.
	httpSrv := &http.Server{Handler: metrics.InstrumentMux(mux, metricsSurface)}
	serveErr := make(chan error, 1)
	go func() { serveErr <- httpSrv.Serve(ln) }()
	select {
	case err := <-serveErr:
		return err
	case err := <-metricsErr:
		// The metrics server stopped on its own: exit rather than keep
		// serving unmonitored (ADR-48).
		return fmt.Errorf("metrics listener stopped: %w", err)
	case <-ctx.Done():
		// Restore default signal disposition first: a second signal
		// during the drain kills immediately (operator escape hatch).
		stopSignals()
		fmt.Fprintln(os.Stderr, "openstorage serve: termination signal received; draining")
		dctx, cancel := context.WithTimeout(context.Background(), shutdownDrainTimeout)
		defer cancel()
		// Scheduled sync runs push to THIS listener (issue #342): stop
		// starting runs and let in-flight ones finish while it still
		// serves. Runs still going when the wait ends are cancelled and
		// record their kept requests before the HTTP drain.
		rctx, rcancel := context.WithTimeout(dctx, shutdownRunWait+shutdownRestoreWait)
		drainRuns(rctx, shutdownRunWait)
		rcancel()
		if err := httpSrv.Shutdown(dctx); err != nil {
			fmt.Fprintf(os.Stderr, "openstorage serve: drain incomplete after %v: %v\n", shutdownDrainTimeout, err)
		}
		if metricsSrv != nil {
			if err := metricsSrv.Shutdown(dctx); err != nil {
				fmt.Fprintf(os.Stderr, "openstorage serve: metrics drain incomplete: %v\n", err)
			}
		}
		// Orderly signal shutdown exits 0; the deferred teardown above
		// (workers, stores, instance lease) runs on this return.
		return nil
	}
}

// selfLoopbackURL derives the sync worker's push target from this process's
// bound listener (issue #45): loopback pushes must terminate in the minting
// process, so the external OPENSTORAGE_BASE_URL — typically a load balancer
// that may route to any replica — is unusable. An unspecified bind address
// is rewritten to the matching loopback address; a concrete one is dialable
// as-is.
func selfLoopbackURL(addr net.Addr) string {
	tcp, ok := addr.(*net.TCPAddr)
	assertf(ok, "selfLoopbackURL: non-TCP listener address %T", addr)
	assertf(tcp.Port > 0, "selfLoopbackURL: unresolved port in %q", addr.String())
	var host string
	switch {
	case tcp.IP == nil:
		// ":port" binds every interface dual-stack; IPv4 loopback works.
		host = "127.0.0.1"
	case tcp.IP.IsUnspecified():
		if tcp.IP.To4() != nil {
			host = "127.0.0.1"
		} else {
			host = "::1"
		}
	default:
		host = tcp.IP.String()
		if tcp.Zone != "" {
			// Scoped IPv6 (e.g. fe80::1%eth0): RFC 6874 requires the
			// zone separator to appear as %25 inside a URL host.
			host += "%25" + tcp.Zone
		}
	}
	return "http://" + net.JoinHostPort(host, strconv.Itoa(tcp.Port))
}

func cmdKeygen(args []string) error {
	fs := flag.NewFlagSet("keygen", flag.ExitOnError)
	out := fs.String("out", "", "output path prefix (writes <out>.pem and <out>.pub.pem)")
	alg := fs.String("alg", "ES256", "fresh key algorithm: one of "+strings.Join(auth.AllowedAlgorithms, ", "))
	ifMissing := fs.Bool("if-missing", false, "reuse a valid existing pair unchanged; generate only when both files are absent")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" {
		return fmt.Errorf("--out is required")
	}
	privPath, pubPath, err := generateKeyPEMFiles(*alg, *out, *ifMissing)
	if err != nil {
		return err
	}
	status := "wrote"
	if *ifMissing {
		status = "ready"
	}
	fmt.Printf("%s %s and %s\n", status, privPath, pubPath)
	return nil
}

// generateKeyPEMFiles creates a fresh keypair for alg and writes
// <outPrefix>.pem (PKCS#8 private, 0600) and <outPrefix>.pub.pem (PKIX
// public — the format the org key registry accepts). Never commit either.
// ifMissing reuses a valid pair without writes, ignoring alg on reuse.
func generateKeyPEMFiles(alg, outPrefix string, ifMissing bool) (privPath, pubPath string, err error) {
	assertf(outPrefix != "", "generateKeyPEMFiles: empty output prefix")
	privPath, pubPath = outPrefix+".pem", outPrefix+".pub.pem"
	if ifMissing {
		reused, err := reuseKeyPEMFiles(privPath, pubPath)
		if err != nil {
			return "", "", err
		}
		if reused {
			return privPath, pubPath, nil
		}
	}
	priv, err := auth.GenerateKey(alg)
	if err != nil {
		return "", "", err
	}
	privPEM, err := auth.MarshalPrivateKeyPEM(priv)
	if err != nil {
		return "", "", err
	}
	pubPEM, err := auth.MarshalPublicKeyPEM(priv.Public())
	if err != nil {
		return "", "", err
	}
	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	if ifMissing {
		flags = os.O_WRONLY | os.O_CREATE | os.O_EXCL
	}
	for _, file := range []struct {
		path string
		data []byte
		mode os.FileMode
	}{{privPath, privPEM, 0o600}, {pubPath, pubPEM, 0o644}} {
		f, err := os.OpenFile(file.path, flags, file.mode)
		if err != nil {
			return "", "", err
		}
		_, writeErr := f.Write(file.data)
		if err := errors.Join(writeErr, f.Close()); err != nil {
			return "", "", err
		}
	}
	return privPath, pubPath, nil
}

// reuseKeyPEMFiles returns false, nil only when both paths are absent. Never
// repair partial creation: O_EXCL protects each fresh file, not an atomic pair.
func reuseKeyPEMFiles(privPath, pubPath string) (bool, error) {
	var data [2][]byte
	found := 0
	for i, path := range []string{privPath, pubPath} {
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return false, err
		}
		if !info.Mode().IsRegular() {
			return false, fmt.Errorf("%s: key must be a regular file (not a symlink)", path)
		}
		if i == 0 && info.Mode().Perm()&0o077 != 0 {
			return false, fmt.Errorf("%s: private key must have no group or other permissions", path)
		}
		if i == 1 && info.Mode().Perm()&0o022 != 0 {
			return false, fmt.Errorf("%s: public key must have no group or other write permissions", path)
		}
		data[i], err = os.ReadFile(path)
		if err != nil {
			return false, err
		}
		data[i] = bytes.TrimSpace(data[i])
		block, rest := pem.Decode(data[i])
		// Decode can skip garbage or a malformed block before a valid one.
		if block == nil || len(bytes.TrimSpace(rest)) != 0 ||
			!bytes.HasPrefix(data[i], []byte("-----BEGIN ")) ||
			bytes.Contains(data[i], []byte("\n-----BEGIN ")) {
			return false, fmt.Errorf("%s: expected exactly one PEM block with only surrounding whitespace", path)
		}
		found++
	}
	if found == 0 {
		return false, nil
	}
	if found != 2 {
		return false, fmt.Errorf("incomplete keypair: both %s and %s must exist", privPath, pubPath)
	}
	priv, err := auth.ParsePrivateKeyPEM(data[0])
	if err != nil {
		return false, fmt.Errorf("%s: %w", privPath, err)
	}
	if key, ok := priv.(*rsa.PrivateKey); ok && key.N.BitLen() < 2048 {
		return false, fmt.Errorf("%s: RSA signing key must be at least 2048 bits", privPath)
	}
	alg, err := auth.DefaultAlgorithm(priv)
	if err != nil {
		return false, fmt.Errorf("%s: %w", privPath, err)
	}
	// Parsing alone can accept keys that cannot sign with an allowed algorithm.
	if _, err := auth.MintToken(priv, alg, auth.Claims{}); err != nil {
		return false, fmt.Errorf("%s: unusable signing key: %w", privPath, err)
	}
	pubs, err := auth.ParsePublicKeysPEM(data[1])
	if err != nil {
		return false, fmt.Errorf("%s: %w", pubPath, err)
	}
	if len(pubs) != 1 {
		return false, fmt.Errorf("%s: expected exactly one public key", pubPath)
	}
	want, err := auth.MarshalPublicKeyPEM(priv.Public())
	if err != nil {
		return false, err
	}
	got, err := auth.MarshalPublicKeyPEM(pubs[0])
	if err != nil {
		return false, err
	}
	if !bytes.Equal(want, got) {
		return false, fmt.Errorf("%s and %s: keys do not match", privPath, pubPath)
	}
	return true, nil
}

func cmdMint(args []string) error {
	fs := flag.NewFlagSet("mint", flag.ExitOnError)
	keyPath := fs.String("key", "", "PEM path of the org private key (PKCS#8 or SEC1)")
	repo := fs.String("repo", "", `repo claim (repo url, or "org" for org-level operations)`)
	scopes := fs.String("scopes", "git:read,git:write", "comma-separated scopes")
	refs := fs.String("refs", "", `optional refs policy JSON, e.g. [["refs/heads/main",["no-force-push"]],["*",[]]]`)
	iss := fs.String("iss", "", "issuer (org) claim — required")
	alg := fs.String("alg", "", "signature algorithm (default: inferred from the key type)")
	ttl := fs.Duration("ttl", time.Hour, "token lifetime")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *keyPath == "" || *repo == "" || *iss == "" {
		return fmt.Errorf("--key, --repo, and --iss are required")
	}
	token, err := mintToken(*keyPath, *iss, *repo, *scopes, *refs, *alg, *ttl)
	if err != nil {
		return err
	}
	fmt.Println(token)
	return nil
}

// mintToken signs a docs/reference/authentication.md claim set with the private key at keyPath.
// alg == "" infers the algorithm from the key type; refsJSON, when given,
// must decode as ordered [pattern, ops] tuples (validated here so a typo
// fails at mint time, not as a mid-push policy error).
func mintToken(keyPath, iss, repo, scopes, refsJSON, alg string, ttl time.Duration) (string, error) {
	assertf(keyPath != "" && iss != "" && repo != "", "mintToken: missing keyPath/iss/repo")
	if ttl <= 0 {
		return "", fmt.Errorf("--ttl must be positive, got %v", ttl)
	}
	pemData, err := os.ReadFile(keyPath)
	if err != nil {
		return "", err
	}
	priv, err := auth.ParsePrivateKeyPEM(pemData)
	if err != nil {
		return "", fmt.Errorf("%s: %w", keyPath, err)
	}
	if alg == "" {
		alg, err = auth.DefaultAlgorithm(priv)
		if err != nil {
			return "", err
		}
	}
	var refsRaw json.RawMessage
	if refsJSON != "" {
		var policies []auth.RefPolicy
		if err := json.Unmarshal([]byte(refsJSON), &policies); err != nil {
			return "", fmt.Errorf("--refs: %w", err)
		}
		refsRaw = json.RawMessage(refsJSON)
	}
	now := time.Now()
	return auth.MintToken(priv, alg, auth.Claims{
		Iss:    iss,
		Sub:    auth.SubjectLiteral,
		Repo:   repo,
		Scopes: strings.Split(scopes, ","),
		Iat:    now.Unix(),
		Exp:    now.Add(ttl).Unix(),
		Refs:   refsRaw,
	})
}

func cmdListPacks(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("list-packs", flag.ExitOnError)
	repo := fs.String("repo", "", "repo whose packs to list")
	dataDir := fs.String("data-dir", "", "local-mode data directory (falls back to $"+envDataDir+"); derives the blob location when no URL is set")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *repo == "" {
		return fmt.Errorf("--repo is required")
	}
	// Shared derivation with serve/signing-key: env URLs win, local mode
	// (--data-dir / $OPENSTORAGE_DATA_DIR) derives the fileblob location.
	urls, err := resolveStorageURLs(envconfig.LookupEnv, *dataDir)
	if err != nil {
		return err
	}
	if urls.blobURL == "" {
		return errBlobURLRequired()
	}
	bl, err := blob.Open(ctx, urls.blobURL)
	if err != nil {
		return err
	}
	defer bl.Close()
	objs, err := bl.List(ctx, blob.PackPrefix(*repo))
	if err != nil {
		return err
	}
	for _, o := range objs {
		fmt.Printf("%s\t%d bytes\n", o.Key, o.Size)
	}
	fmt.Printf("total: %d pack(s)\n", len(objs))
	return nil
}

// metricsSurface maps the mux pattern for a request to its fixed metrics
// surface (ADR-48). It mirrors the registrations above: /api/ subtree,
// /healthz, and the Git fallback for everything else. For a mux redirect
// the pattern is the redirect's target path, classified by the same rule.
func metricsSurface(pattern string) metrics.Surface {
	switch {
	case strings.HasPrefix(pattern, "/api/"):
		return metrics.SurfaceAPI
	case pattern == "/healthz":
		return metrics.SurfaceHealth
	default:
		return metrics.SurfaceGit
	}
}
