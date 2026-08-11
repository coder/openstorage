// codestorage is the de-risk spike binary: `serve` runs a stateless git
// smart-HTTP node; `hook` is re-exec'd by git as pre-receive/update hooks;
// keygen/mint/list-packs are throwaway dogfooding helpers.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"codestorage/internal/blob"
	"codestorage/internal/ref"
	"codestorage/internal/spike"
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
		err = spike.RunHook(ctx, os.Args[2:])
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

func mustEnv(name string) string {
	v := os.Getenv(name)
	if v == "" {
		fatalf("required env %s is empty", name)
	}
	return v
}

func cmdServe(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:0", "listen address (prints 'listening on <addr>')")
	cacheDir := fs.String("cache-dir", "", "rebuildable cache directory for bare repos")
	pubKeyPath := fs.String("jwt-pubkey", "", "PEM path of the org's JWT public key")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *cacheDir == "" || *pubKeyPath == "" {
		return fmt.Errorf("--cache-dir and --jwt-pubkey are required")
	}
	if err := os.MkdirAll(*cacheDir, 0o755); err != nil {
		return err
	}
	pub, err := spike.LoadPublicKeyPEM(*pubKeyPath)
	if err != nil {
		return err
	}
	st, err := ref.OpenPG(ctx, mustEnv("PG_URL"))
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.InitSchema(ctx); err != nil {
		return fmt.Errorf("init schema: %w", err)
	}
	bl, err := blob.OpenS3(ctx, mustEnv("BLOB_URL"))
	if err != nil {
		return err
	}
	defer bl.Close()
	if err := bl.EnsureBucket(ctx); err != nil {
		return err
	}

	srv := spike.NewServer(st, bl, *cacheDir, pub)
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	fmt.Printf("listening on %s\n", ln.Addr())
	return http.Serve(ln, srv.Handler())
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
	priv, err := spike.GenerateKeyPair()
	if err != nil {
		return err
	}
	privPath, pubPath, err := spike.WriteKeyPairPEM(priv, *out)
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
	priv, err := spike.LoadPrivateKeyPEM(*keyPath)
	if err != nil {
		return err
	}
	token, err := spike.MintToken(priv, *iss, *repo, strings.Split(*scopes, ","), *refs, *ttl)
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
	bl, err := blob.OpenS3(ctx, mustEnv("BLOB_URL"))
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
