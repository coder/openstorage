// codestorage is the service binary: `serve` runs a stateless node exposing
// the /api/v1 HTTP API (single-org self-host mode) and the git smart-HTTP
// path; `hook` is re-exec'd by git as pre-receive/update hooks;
// keygen/mint/list-packs are local dogfooding helpers.
//
// Configuration comes from CODESTORAGE_* environment variables, with the
// phase-0 spike names (SPIKE_*, PG_URL, BLOB_URL) kept as fallback aliases —
// see config.go.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"codestorage/internal/auth"
	"codestorage/internal/blob"
	"codestorage/internal/githttp"
	"codestorage/internal/meta"
	"codestorage/internal/ref"
)

func main() {
	if len(os.Args) < 2 {
		fatalf("usage: codestorage <serve|hook|keygen|mint|list-packs> ...")
	}
	ctx := context.Background()
	var err error
	switch os.Args[1] {
	case "serve":
		err = cmdServe(ctx, os.Args[2:])
	case "hook":
		err = githttp.RunHook(ctx, os.Args[2:])
	case "keygen":
		err = cmdKeygen(os.Args[2:])
	case "mint":
		err = cmdMint(os.Args[2:])
	case "list-packs":
		err = cmdListPacks(ctx, os.Args[2:])
	default:
		err = fmt.Errorf("unknown subcommand %q", os.Args[1])
	}
	if err != nil {
		fatalf("codestorage %s: %v", os.Args[1], err)
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func cmdServe(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	listen := fs.String("listen", "", "listen address (falls back to --addr, then $"+envListen+", then "+defaultListen+")")
	addr := fs.String("addr", "", "spike-compat alias for --listen")
	cacheDir := fs.String("cache-dir", "", "rebuildable cache directory for bare repos (default: $TMPDIR/codestorage-cache)")
	pubKeyPath := fs.String("jwt-pubkey", "", "PEM path of the ES256 public key for the git smart-HTTP path (default: first ECDSA key in $"+envOrgPubKeys+")")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := resolveServeConfig(os.Getenv, *listen, *addr, *cacheDir, *pubKeyPath)
	if err != nil {
		return err
	}
	// The pre-receive/update hooks re-exec this binary with the server's
	// inherited environment and read the legacy names (internal/githttp/hook.go),
	// so propagate the resolved values regardless of which alias set them.
	for _, kv := range [][2]string{{"PG_URL", cfg.pgURL}, {"BLOB_URL", cfg.blobURL}} {
		if err := os.Setenv(kv[0], kv[1]); err != nil {
			return err
		}
	}

	st, err := ref.OpenPG(ctx, cfg.pgURL)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.InitSchema(ctx); err != nil {
		return fmt.Errorf("init ref schema: %w", err)
	}
	bl, err := blob.OpenS3(ctx, cfg.blobURL)
	if err != nil {
		return err
	}
	defer bl.Close()
	if err := bl.EnsureBucket(ctx); err != nil {
		return err
	}

	// Listen before building handlers: the default listen address uses an
	// ephemeral port (127.0.0.1:0), and the API needs the bound address to
	// build the `http_url` values it returns.
	ln, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		return err
	}
	httpBaseURL := "http://" + ln.Addr().String()

	mux := http.NewServeMux()
	mounted := 0

	// HTTP API (single-org self-host mode), enabled by CODESTORAGE_ORG.
	var orgPEMBlocks []string
	var metaStore meta.Store
	if cfg.org != "" {
		pemData, err := os.ReadFile(cfg.orgKeysPath)
		if err != nil {
			return err
		}
		// Validate every block up front (crash early on bad key material).
		if _, err := auth.ParsePublicKeysPEM(pemData); err != nil {
			return fmt.Errorf("%s: %w", cfg.orgKeysPath, err)
		}
		orgPEMBlocks, err = splitPEMBlocks(pemData)
		if err != nil {
			return fmt.Errorf("%s: %w", cfg.orgKeysPath, err)
		}
		apiHandler, ms, cleanup, err := buildAPIHandler(ctx, cfg.pgURL, cfg.org, httpBaseURL, orgPEMBlocks)
		if err != nil {
			return err
		}
		defer cleanup()
		metaStore = ms
		mux.Handle("/api/v1/", apiHandler)
		mounted++
	}

	// Git smart-HTTP path (spike fallback), mounted when an ECDSA key is
	// available from --jwt-pubkey or the org key set.
	gitPub, err := resolveGitPublicKey(cfg.gitPubKeyPath, orgPEMBlocks)
	if err != nil {
		return err
	}
	if gitPub != nil {
		if err := os.MkdirAll(cfg.cacheDir, 0o755); err != nil {
			return err
		}
		srv := githttp.NewServer(st, bl, cfg.cacheDir, gitPub)
		if metaStore != nil {
			// docs/05 git URLs address repos by repo_id while JWT repo
			// claims carry the url (docs/02): resolve id->url for auth.
			org := cfg.org
			srv.ResolveRepoURL = func(ctx context.Context, repoID string) (string, bool) {
				repo, err := metaStore.GetRepoByID(ctx, org, repoID)
				if err != nil {
					if !errors.Is(err, meta.ErrNotFound) {
						fmt.Fprintf(os.Stderr, "resolve repo id %q: %v\n", repoID, err)
					}
					return "", false
				}
				return repo.URL, true
			}
		}
		mux.Handle("/", srv.Handler())
		mounted++
	} else {
		fmt.Fprintln(os.Stderr, "git smart-HTTP path disabled: no ECDSA public key (pass --jwt-pubkey or include one in $"+envOrgPubKeys+")")
	}
	// resolveServeConfig guarantees at least one of org/gitPubKeyPath, and an
	// explicit --jwt-pubkey either loads or errors above.
	assertf(mounted > 0, "serve resolved a config with nothing mounted")

	fmt.Printf("listening on %s\n", ln.Addr())
	return http.Serve(ln, mux)
}

func cmdKeygen(args []string) error {
	fs := flag.NewFlagSet("keygen", flag.ExitOnError)
	out := fs.String("out", "", "output path prefix (writes <out>.pem and <out>.pub.pem)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" {
		return fmt.Errorf("--out is required")
	}
	priv, err := githttp.GenerateKeyPair()
	if err != nil {
		return err
	}
	privPath, pubPath, err := githttp.WriteKeyPairPEM(priv, *out)
	if err != nil {
		return err
	}
	fmt.Printf("wrote %s and %s\n", privPath, pubPath)
	return nil
}

func cmdMint(args []string) error {
	fs := flag.NewFlagSet("mint", flag.ExitOnError)
	keyPath := fs.String("key", "", "PEM path of the ES256 private key")
	repo := fs.String("repo", "", "repo claim")
	scopes := fs.String("scopes", "git:read,git:write", "comma-separated scopes")
	refs := fs.String("refs", "", `optional refs policy JSON, e.g. [["refs/heads/main",["no-force-push"]],["*",[]]]`)
	iss := fs.String("iss", "spike", "issuer (org) claim")
	ttl := fs.Duration("ttl", time.Hour, "token lifetime")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *keyPath == "" || *repo == "" {
		return fmt.Errorf("--key and --repo are required")
	}
	priv, err := githttp.LoadPrivateKeyPEM(*keyPath)
	if err != nil {
		return err
	}
	token, err := githttp.MintToken(priv, *iss, *repo, strings.Split(*scopes, ","), *refs, *ttl)
	if err != nil {
		return err
	}
	fmt.Println(token)
	return nil
}

func cmdListPacks(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("list-packs", flag.ExitOnError)
	repo := fs.String("repo", "", "repo whose packs to list")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *repo == "" {
		return fmt.Errorf("--repo is required")
	}
	blobURL := envFirst(os.Getenv, envBlobURL...)
	if blobURL == "" {
		return fmt.Errorf("blob URL required: set %s (or legacy %s)", envBlobURL[0], envBlobURL[2])
	}
	bl, err := blob.OpenS3(ctx, blobURL)
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
