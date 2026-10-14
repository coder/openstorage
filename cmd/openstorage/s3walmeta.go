// s3wal-meta convert rewrites an S3WAL bucket's metadata document between
// format versions 1 and 2 (ADR-51, rollout R1): serve reads only v2, so an
// operator converts a stopped bucket once per format release, and back with
// --to 1 before a downgrade.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/coder/openstorage/internal/envconfig"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref/s3wal"
)

// s3walConvertReleaseTimeout bounds the lease release, as serve does.
const s3walConvertReleaseTimeout = 5 * time.Second

func cmdS3WALMeta(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] != "convert" {
		return errors.New("usage: openstorage s3wal-meta convert --to <1|2> [--force]")
	}
	fs := flag.NewFlagSet("s3wal-meta convert", flag.ExitOnError)
	to := fs.Int("to", 0, "target metadata format version: 2 to upgrade, 1 to downgrade")
	force := fs.Bool("force", false, "with --to 1: convert even while a fork or copy create is unfinished")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("s3wal-meta convert: unexpected arguments %q", fs.Args())
	}
	if *to != 1 && *to != 2 {
		return errors.New("--to must be 1 or 2")
	}
	if *force && *to != 1 {
		return errors.New("--force applies only to --to 1")
	}
	urls, err := resolveStorageURLs(envconfig.LookupEnv, "")
	if err != nil {
		return err
	}
	if !strings.HasPrefix(urls.dbURL, "s3wal://") {
		return fmt.Errorf("s3wal-meta convert needs an s3wal:// database URL in %s", envPGURL[0])
	}
	// SIGINT/SIGTERM cancel the conversion, so the lease is released on
	// the normal path instead of blocking serve until its TTL expires.
	ctx, stopSignals := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	c, err := s3wal.Dial(ctx, urls.dbURL)
	if err != nil {
		return err
	}
	defer c.Close()
	// The probe proves the conditional writes the lease and the
	// conversion rely on, as serve's InitSchema does.
	if err := s3wal.Probe(ctx, c); err != nil {
		return fmt.Errorf("s3wal-meta convert: %w", err)
	}
	return runS3WALMetaConvert(ctx, c, *to, meta.S3WALConvertOptions{Force: *force}, s3wal.LeaseConfig{}, os.Stdout)
}

// runS3WALMetaConvert converts under the instance lease. It ignores
// OPENSTORAGE_S3WAL_LEASE_TTL: the lease is always on, so a running serve
// (which holds a fresh lease) makes it refuse. The conversion context is
// cancelled when a renewal fails, and the heartbeat is joined before the
// lease is released.
func runS3WALMetaConvert(ctx context.Context, cs s3wal.ConditionalStore, to int, opts meta.S3WALConvertOptions, lc s3wal.LeaseConfig, out io.Writer) error {
	assertf(cs != nil && out != nil, "runS3WALMetaConvert: nil store or writer")
	lease, err := s3wal.AcquireInstanceLease(ctx, cs, lc)
	if err != nil {
		return fmt.Errorf("s3wal-meta convert: %w (stop serve first; see docs/how-to/deploy-production.md#upgrading-the-s3wal-metadata-format)", err)
	}
	cctx, cancel := context.WithCancelCause(ctx)
	hbDone := make(chan error, 1)
	go func() {
		err := lease.Heartbeat(cctx)
		if err != nil {
			cancel(err)
		}
		hbDone <- err
	}()
	res, convErr := meta.ConvertS3WALMeta(cctx, cs, to, opts)
	cancel(nil)
	hbErr := <-hbDone
	rctx, rcancel := context.WithTimeout(context.Background(), s3walConvertReleaseTimeout)
	lease.Release(rctx)
	rcancel()
	if hbErr != nil {
		return fmt.Errorf("s3wal-meta convert: instance lease lost (%w); the document is %s", hbErr, convertOutcome(res, convErr))
	}
	if convErr != nil {
		return convErr
	}
	switch {
	case res.Missing:
		fmt.Fprintln(out, "no metadata document: nothing to convert (serve creates a v2 document on its first write)")
	case res.From == res.To:
		fmt.Fprintf(out, "already at version %d (seq %d): nothing to do\n", res.To, res.SeqAfter)
	default:
		fmt.Fprintf(out, "converted version %d -> %d (seq %d -> %d), verified\n", res.From, res.To, res.SeqBefore, res.SeqAfter)
		fmt.Fprintf(out, "rows before: %s\nrows after:  %s\n", formatS3WALMetaCounts(res.Before), formatS3WALMetaCounts(res.After))
		if n := len(res.RepairedSyncCreates); n > 0 {
			fmt.Fprintf(out, "unfinished sync creates: %d, first sync requested (the v1 release runs it at boot): %s\n",
				n, strings.Join(res.RepairedSyncCreates, ", "))
		}
		if n := len(res.ForcedCreates); n > 0 {
			names := make([]string, n)
			for i, c := range res.ForcedCreates {
				names[i] = c.String() // quotes the URL: it comes from a token claim
			}
			fmt.Fprintf(out, "--force: %d unfinished fork or copy creates now read as finished, possibly empty repositories: %s\n",
				n, strings.Join(names, ", "))
		}
	}
	return nil
}

// convertOutcome names what a lease loss left behind.
func convertOutcome(res meta.S3WALMetaConversion, err error) string {
	if err != nil || res.Missing || res.From == res.To {
		return "unchanged unless the write was in flight; run the command again"
	}
	return fmt.Sprintf("written at version %d (seq %d); run the command again to confirm", res.To, res.SeqAfter)
}

func formatS3WALMetaCounts(c meta.S3WALMetaCounts) string {
	return fmt.Sprintf("orgs=%d repos=%d webhooks=%d credentials=%d upstreams=%d", c.Orgs, c.Repos, c.Webhooks, c.Credentials, c.Upstreams)
}
