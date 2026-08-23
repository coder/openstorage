// purge-repo permanently reclaims one soft-deleted repository's registry
// rows and blobs (issue #52) — the synchronous operator twin of the
// CODESTORAGE_REPO_RETENTION background purge, running the exact same
// jobs.RepoPurger primitive. Purged repositories CANNOT be restored, and
// the burned (org, url) name is never released (see meta/purge.go).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/coder/code-storage/internal/blob"
	"github.com/coder/code-storage/internal/jobs"
	"github.com/coder/code-storage/internal/ref"
)

func cmdPurgeRepo(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("purge-repo", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), `usage: codestorage purge-repo [flags] <repo-id>

Permanently reclaims a soft-deleted repository: drops its registry rows
(refs, packs, LFS objects, credentials, upstream links) and deletes its
blobs through the sweeper's quarantine discipline. IRREVERSIBLE — the
repository cannot be restored afterwards. Its (org, url) name stays burned
forever (URL-scoped tokens may outlive the purge, so the name is never
released). The repository must have been soft-deleted at least %v ago (an
in-flight pre-delete push could otherwise still be writing).

Flags:
`, jobs.MinRepoPurgeAge)
		fs.PrintDefaults()
	}
	yes := fs.Bool("yes", false, "confirm the irreversible purge (required)")
	dataDir := fs.String("data-dir", "", "local-mode data directory (falls back to $"+envDataDir+"); derives the database and blob locations when no URLs are set")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return errors.New("exactly one <repo-id> argument is required")
	}
	repoID := fs.Arg(0)

	// Shared derivation with serve/signing-key/list-packs: env URLs win,
	// local mode (--data-dir / $CODESTORAGE_DATA_DIR) fills the rest.
	urls, err := resolveStorageURLs(os.Getenv, *dataDir)
	if err != nil {
		return err
	}
	if urls.dbURL == "" {
		return errDBURLRequired()
	}
	if urls.blobURL == "" {
		return errBlobURLRequired()
	}
	secretsCodec, err := resolveSecretsCodec(os.Getenv)
	if err != nil {
		return err
	}
	// The purge consults the same quota/reservation knobs as serve: a
	// defensive quarantine restore must take cover reservations exactly
	// when admission would consult them.
	quotaBytes, err := resolvePackQuota(os.Getenv)
	if err != nil {
		return err
	}
	reservationTTL, err := resolvePackReservationTTL(os.Getenv)
	if err != nil {
		return err
	}

	ms, msCleanup, err := openMetaStore(ctx, urls.dbURL, secretsCodec)
	if err != nil {
		return err
	}
	defer msCleanup()
	rs, err := ref.Open(ctx, urls.dbURL)
	if err != nil {
		return fmt.Errorf("open ref store: %w", err)
	}
	defer rs.Close()
	// Operator commands own schema init like serve (hooks never run DDL):
	// when purge-repo is the first upgraded binary to touch a database,
	// the purge fence table must exist before the seams run.
	if err := rs.InitSchema(ctx); err != nil {
		return fmt.Errorf("init ref schema: %w", err)
	}
	bl, err := blob.Open(ctx, urls.blobURL)
	if err != nil {
		return fmt.Errorf("open blob store: %w", err)
	}
	defer bl.Close()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	purgeReg, ok := rs.(jobs.RepoPurgeRegistry)
	assertf(ok, "ref store %T lacks the repo purge seams", rs)
	purgeMeta, ok := ms.(jobs.RepoPurgeMeta)
	assertf(ok, "meta store %T lacks the repo purge seams", ms)
	sweepCfg := jobs.SweepConfig{
		Threshold:       defaultSweepAgeThreshold,
		PurgedRepoProbe: purgeReg.IsRepoPurged,
		QuotaEnforced:   quotaBytes > 0,
		ReservationTTL:  reservationTTL,
	}
	if s3store, ok := bl.(*blob.S3); ok {
		// Versioned buckets turn deletes into billed delete markers; fail
		// closed exactly like serve's delete-mode gate.
		sweepCfg.DeleteGuard = func(ctx context.Context) error {
			versioned, err := s3store.VersioningEnabled(ctx)
			if err != nil {
				return fmt.Errorf("verify bucket versioning: %w", err)
			}
			if versioned {
				return fmt.Errorf("bucket has S3 versioning enabled or suspended; deletes would only write delete markers")
			}
			return nil
		}
	}
	sweeper := jobs.NewSweeper(bl, rs, sweepCfg, logger)
	purger := jobs.NewRepoPurger(sweeper, purgeReg, purgeMeta, 0, logger)

	// Summarize what would be purged before demanding confirmation, so an
	// operator without --yes gets a dry look instead of a bare refusal.
	state, found, err := purgeMeta.RepoPurgeState(ctx, repoID)
	if err != nil {
		return fmt.Errorf("read repo state: %w", err)
	}
	if !found {
		return fmt.Errorf("repo %s not found (never created, or purged by an older build)", repoID)
	}
	if state.Repo.DeletedAt == nil {
		return fmt.Errorf("repo %s (%s) is live — delete it first (DELETE /api/v1/repos/delete)", repoID, state.Repo.URL)
	}
	fmt.Printf("repo:       %s\n", state.Repo.RepoID)
	fmt.Printf("url:        %s\n", state.Repo.URL)
	fmt.Printf("deleted at: %s (%s ago)\n",
		state.Repo.DeletedAt.Format(time.RFC3339), time.Since(*state.Repo.DeletedAt).Round(time.Second))
	if state.PurgedAt != nil {
		// Completed purge: re-running rescans the blob prefixes for
		// stragglers an in-flight pre-delete writer published after the
		// completed pass's scan (still gated on --yes, like any purge).
		fmt.Printf("purged at:  %s — rescanning for straggler blobs\n", state.PurgedAt.Format(time.RFC3339))
	}
	if !*yes {
		return errors.New("refusing to purge without --yes: purging is IRREVERSIBLE (no restore)")
	}

	report, err := purger.PurgeRepo(ctx, repoID)
	if err != nil {
		return err
	}
	fmt.Printf("purged %s: %d refs, %d packs, %d LFS objects unregistered; %d blobs (%d bytes) quarantined; %d ledger rows removed\n",
		report.RepoID, report.RefsDropped, report.PacksRetired, report.LFSDropped,
		report.BlobsReaped, report.BytesReaped, report.RowsPurged)
	fmt.Println("quarantine backups are collected by the next aged sweep pass; the repository name remains burned (URL-scoped tokens may outlive the purge)")
	return nil
}
