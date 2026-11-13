// repair-refs removes the refs of one repository that git can never lock.
// Before the API refused such names, a git:write token could create a notes
// ref whose last part is 251 to 255 bytes. git locks a ref at <last
// part>.lock, so hydration of that repository fails and every git read of it
// answers 500. No API route, push or sync can delete a notes ref, so an
// operator removes it here, in the ref store.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/coder/openstorage/internal/envconfig"
	"github.com/coder/openstorage/internal/githttp"
	"github.com/coder/openstorage/internal/jobs"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref"
)

// repairRefsPage is how many refs one ListRefsAfter call reads, as many as a
// hydration page. The scan keeps only the refs that match, so memory does not
// grow with the repo, and a large S3-WAL repository costs few manifest reads.
const repairRefsPage = 10_000

// repairRef is one listed ref.
type repairRef struct{ name, sha string }

// repairTarget is the repository that the command works on, as its row in the
// metadata store describes it. url may be empty, defaultBranch may not.
type repairTarget struct {
	repoID, url, defaultBranch string
}

// isDefaultBranch reports whether the ref is the branch that the repository
// records as its default, the one that DELETE /repos/branches refuses to
// remove.
func (t repairTarget) isDefaultBranch(name string) bool {
	return name == githttp.HeadTarget(t.defaultBranch)
}

func cmdRepairRefs(ctx context.Context, args []string) error {
	return runRepairRefs(ctx, args, os.Stdout)
}

func runRepairRefs(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("repair-refs", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), `usage: openstorage repair-refs [flags] <repo-id>

Lists the refs of one repository that git can never lock: a ref name with a
part over 255 bytes, or a last part over 250 bytes (git locks a ref at
<last part>.lock). Every hydration of such a repository fails, so git reads of
it answer 500. Without --yes the command changes nothing. With --yes it
deletes each listed ref from the ref store, but only if the ref still holds
the SHA that it listed. A ref that changed in between is reported and kept.

The command changes the ref store only. It sends no push webhook and no sync
poke. With --yes it lists the refs again and deletes what it finds. It keeps a
ref that is the repository's default branch: set another default branch first
(PATCH /repo), then run the command again.

Flags:
`)
		fs.PrintDefaults()
	}
	yes := fs.Bool("yes", false, "delete the listed refs (without it the command only lists them)")
	dataDir := fs.String("data-dir", "", "local-mode data directory (falls back to $"+envDataDir+"); derives the database location when no URL is set")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return errors.New("exactly one <repo-id> argument is required (flags go before it)")
	}
	repoID := fs.Arg(0)
	if repoID == "" {
		return errors.New("<repo-id> must not be empty")
	}

	urls, err := resolveStorageURLs(envconfig.LookupEnv, *dataDir)
	if err != nil {
		return err
	}
	if urls.dbURL == "" {
		return errDBURLRequired()
	}
	// Only the metadata row is read, so no schema setup and no secrets key.
	ms, err := meta.Open(ctx, urls.dbURL)
	if err != nil {
		return fmt.Errorf("open metadata store: %w", err)
	}
	defer ms.Close()
	purgeMeta, ok := ms.(jobs.RepoPurgeMeta)
	assertf(ok, "meta store %T lacks the repo state seam", ms)
	state, found, err := purgeMeta.RepoPurgeState(ctx, repoID)
	if err != nil {
		return fmt.Errorf("read repo state: %w", err)
	}
	if !found {
		return fmt.Errorf("repo %s not found: check the repo ID (repo_ and 12 hex digits, no other text) and that the database URL or --data-dir is the one that serve uses", sanitizeTerm(repoID))
	}
	rs, err := ref.Open(ctx, urls.dbURL)
	if err != nil {
		return fmt.Errorf("open ref store: %w", err)
	}
	defer rs.Close()
	if *yes {
		// Like serve and purge-repo, the operator command owns schema init.
		// A delete must bump the repository generation (a trigger that
		// InitSchema installs), or replicas keep a fresh hydrate marker. A
		// dry run only reads, so it leaves the schema alone.
		if err := rs.InitSchema(ctx); err != nil {
			return fmt.Errorf("init ref schema: %w", err)
		}
	}
	target := repairTarget{repoID: repoID, url: state.Repo.URL, defaultBranch: state.Repo.DefaultBranch}
	return repairRefs(ctx, rs, target, *yes, out)
}

// repairRefs lists the refs of repoID that git can never lock and, when yes
// is set, deletes them. It returns an error when a delete fails or a ref
// moved since it was listed.
func repairRefs(ctx context.Context, rs ref.RefStore, target repairTarget, yes bool, out io.Writer) error {
	repoID := target.repoID
	assertf(repoID != "", "repairRefs: empty repo ID")
	found, scanned, err := scanUnlockableRefs(ctx, rs, repoID, repairRefsPage)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "repo:    %s\n", sanitizeTerm(repoID))
	if target.url != "" {
		fmt.Fprintf(out, "url:     %s\n", sanitizeTerm(target.url))
	}
	fmt.Fprintf(out, "scanned: %d refs\n", scanned)
	if scanned == 0 {
		// The metadata row exists, and the row and the refs share one
		// database URL, so the repository has no refs.
		fmt.Fprintln(out, "this repository has no refs: it is empty, it was purged, or its last refs were just deleted")
		return nil
	}
	if len(found) == 0 {
		fmt.Fprintln(out, "no ref that git can never lock")
		return nil
	}
	fmt.Fprintf(out, "refs that git can never lock (%d):\n", len(found))
	for _, r := range found {
		note := ""
		if target.isDefaultBranch(r.name) {
			note = "  (the default branch: not deleted)"
		}
		fmt.Fprintf(out, "  %s %s%s\n", r.sha, sanitizeTerm(r.name), note)
	}
	if !yes {
		fmt.Fprintln(out, "nothing changed: run the command again with --yes to delete these refs")
		return nil
	}
	var deleted, lost, kept int
	for _, r := range found {
		if target.isDefaultBranch(r.name) {
			// DELETE /repos/branches refuses the default branch, and so does
			// this command: the repository would be left with a default
			// branch that does not exist.
			kept++
			fmt.Fprintf(out, "kept: %s is the default branch: set another default branch (PATCH /repo), then run the command again\n", sanitizeTerm(r.name))
			continue
		}
		won, err := deleteUnlockableRef(ctx, rs, repoID, r)
		if err != nil {
			// The store's error text can quote the ref name.
			return fmt.Errorf("delete %s: %s (%d deleted before the error)", sanitizeTerm(r.name), sanitizeTerm(err.Error()), deleted)
		}
		if !won {
			lost++
			fmt.Fprintf(out, "lost CAS, not deleted: %s changed or was removed since it was listed\n", sanitizeTerm(r.name))
			continue
		}
		deleted++
		fmt.Fprintf(out, "deleted: %s\n", sanitizeTerm(r.name))
	}
	fmt.Fprintf(out, "deleted %d of %d refs\n", deleted, len(found))
	if lost > 0 || kept > 0 {
		return fmt.Errorf("%d of %d refs were kept (%d changed since they were listed, %d are the default branch): see above, then run the command again", lost+kept, len(found), lost, kept)
	}
	return nil
}

// scanUnlockableRefs reads every ref of repoID, pageSize at a time, and
// returns the ones that git can never lock, and how many refs it read.
func scanUnlockableRefs(ctx context.Context, rs ref.RefStore, repoID string, pageSize int) (found []repairRef, scanned int, err error) {
	assertf(pageSize > 0, "scanUnlockableRefs: page size %d", pageSize)
	after := ""
	for {
		page, err := rs.ListRefsAfter(ctx, repoID, after, pageSize)
		if err != nil {
			return nil, 0, fmt.Errorf("list refs of %s: %w", sanitizeTerm(repoID), err)
		}
		for _, e := range page {
			assertf(e.Name > after, "ListRefsAfter yielded %q not above cursor %q", e.Name, after)
			after = e.Name
			scanned++
			if ref.Unlockable(e.Name) {
				found = append(found, repairRef{name: e.Name, sha: e.SHA})
			}
		}
		if len(page) < pageSize {
			return found, scanned, nil
		}
	}
}

// deleteUnlockableRef swaps r's listed SHA for the zero SHA. It refuses a
// ref that git can lock: such a ref is a healthy ref, whatever listed it.
// won is false when the ref no longer holds the listed SHA. The swap is not
// retried.
func deleteUnlockableRef(ctx context.Context, rs ref.RefStore, repoID string, r repairRef) (won bool, err error) {
	if !ref.Unlockable(r.name) {
		return false, errors.New("git can lock this ref: refusing to delete it")
	}
	return rs.CASRef(ctx, repoID, r.name, r.sha, ref.ZeroSHA)
}
