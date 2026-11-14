// repair-refs removes the refs of one repository that wedge its hydration.
// There are three kinds, and each stops the hydration of the repository, so
// every git read of it answers 500.
//
// A ref that git can never lock: before the API refused such names, a
// git:write token could create a notes ref whose last part is 251 to 255
// bytes. git locks a ref at <last part>.lock. No API route, push or sync can
// delete a notes ref, so an operator removes it here, in the ref store.
//
// A branch whose tip is not a commit: before the update hook refused such a
// push, a git:write token could set a branch to a blob, a tree or an
// annotated tag. The ref store took the value, and git refuses to write it
// into the cache repository. The command reads the object type of each tip
// from the repository's packs in the blob store.
//
// A ref whose tip object no pack holds: git refuses to write a ref at an
// object that the cache repository lacks, whatever the namespace of the ref
// (a branch, a tag, a note), and the cache holds only what the registered
// packs hold. The command deletes such a ref only with --yes.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"

	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/envconfig"
	"github.com/coder/openstorage/internal/githttp"
	"github.com/coder/openstorage/internal/gitread"
	"github.com/coder/openstorage/internal/jobs"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref"
)

// repairRefsPage is how many refs one ListRefsAfter call reads, as many as a
// hydration page. The scan keeps only the refs that match, so memory does not
// grow with the repo, and a large S3-WAL repository costs few manifest reads.
const repairRefsPage = 10_000

// repairBranchPrefix starts the name of a branch. git refuses to write a
// value that is not a commit under it (trying to write non-commit object),
// and the update hook refuses such a push with the same prefix.
const repairBranchPrefix = "refs/heads/"

// repairRef is one listed ref. kind is the object type of the tip (blob, tree
// or tag) of a branch that is listed because its tip is not a commit, or
// missing for a ref of any namespace whose tip object no pack holds. It is
// empty for a ref that is listed for its name.
type repairRef struct{ name, sha, kind string }

// repairKindNames is how the list names the object types that a branch
// cannot point at.
var repairKindNames = map[string]string{"blob": "a blob", "tree": "a tree", "tag": "an annotated tag"}

// objectKinds returns the git object type of each id (commit, tree, blob or
// tag), or "missing" when the repository has no such object.
type objectKinds func(ctx context.Context, ids []string) (map[string]string, error)

// repairTarget is the repository that the command works on, as its row in the
// metadata store describes it. url may be empty, defaultBranch may not. kinds
// reads the object types of the ref tips, and a repository with a ref that
// git can lock needs it. release, when set, frees what kinds holds:
// repairRefs calls it once the scan ends, so that nothing outlives the scan,
// not even a run that a closed pipe on stdout kills. packsUnchanged, when
// set, reports whether the repository's registered packs are still the ones
// that kinds read.
type repairTarget struct {
	repoID, url, defaultBranch string
	kinds                      objectKinds
	release                    func()
	packsUnchanged             func(ctx context.Context) (bool, error)
}

// isDefaultBranch reports whether the ref is the branch that the repository
// records as its default, the one that DELETE /repos/branches refuses to
// remove.
func (t repairTarget) isDefaultBranch(name string) bool {
	return name == githttp.HeadTarget(t.defaultBranch)
}

// repairSignals lists the signals that cancel the run, so that the temporary
// directory of packs goes away on the normal path. SIGHUP (a closed terminal)
// is left out when the parent started the command with it ignored, as nohup
// does: Notify would replace that, and a long run would end at the logout.
func repairSignals(ignored func(os.Signal) bool) []os.Signal {
	sigs := []os.Signal{os.Interrupt, syscall.SIGTERM}
	if !ignored(syscall.SIGHUP) {
		sigs = append(sigs, syscall.SIGHUP)
	}
	return sigs
}

func cmdRepairRefs(ctx context.Context, args []string) error {
	ctx, stopSignals := signal.NotifyContext(ctx, repairSignals(signal.Ignored)...)
	defer stopSignals()
	return runRepairRefs(ctx, args, os.Stdout)
}

func runRepairRefs(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("repair-refs", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), `usage: openstorage repair-refs [flags] <repo-id>

Lists the refs of one repository that stop its hydration, so that git reads of
it answer 500:

  - a ref that git can never lock: a ref name with a part over 255 bytes, or a
    last part over 250 bytes (git locks a ref at <last part>.lock);
  - a branch (a ref under refs/heads/) whose tip is not a commit but a blob, a
    tree or an annotated tag. git refuses to write such a branch;
  - a ref of any namespace (a branch, a tag, a note) whose tip object no
    registered pack holds. git refuses to write such a ref.

The command reads the object type of each tip from the repository's packs in
the blob store, so a repository with a ref that git can lock needs the blob
store URL ($OPENSTORAGE_S3_URL, or --data-dir). It downloads the packs into a
temporary directory under $TMPDIR and removes it as soon as it has read the
tips, and also after an error, SIGINT, SIGTERM or SIGHUP (a run that nohup
started ignores SIGHUP). After SIGKILL or SIGQUIT, delete the
openstorage-repair-refs-* directory by hand.

Without --yes the command changes nothing. With --yes it deletes each listed
ref from the ref store, but only if the ref still holds the SHA that it
listed. A ref that changed in between is reported and kept. The objects and
packs stay.

The command changes the ref store only. It sends no push webhook and no sync
poke. With --yes it lists the refs again and deletes what it finds. It keeps a
ref that is the repository's default branch: set another default branch first
(PATCH /repo), then run the command again. A ref whose tip object no pack holds
is listed with the other refs, and with --yes it is deleted too. Check the packs
first (openstorage list-packs), and keep the list: it shows the SHA of each ref,
and only the ref goes. If the registered packs change while the command reads
them, it stops with an error and deletes nothing: run it again.

Flags:
`)
		fs.PrintDefaults()
	}
	yes := fs.Bool("yes", false, "delete the listed refs (without it the command only lists them)")
	dataDir := fs.String("data-dir", "", "local-mode data directory (falls back to $"+envDataDir+"); derives the database and blob locations when no URLs are set")
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
	tips := &packTipKinds{rs: rs, repoID: repoID, blobURL: urls.blobURL}
	// repairRefs calls release when the scan returns. A panic in the scan
	// skips that call, and this defer still removes the packs.
	defer tips.close()
	target := repairTarget{repoID: repoID, url: state.Repo.URL, defaultBranch: state.Repo.DefaultBranch, kinds: tips.kinds, release: tips.close, packsUnchanged: tips.unchanged}
	return repairRefs(ctx, rs, target, *yes, out)
}

// repairRefs lists the refs of repoID that wedge its hydration and, when yes
// is set, deletes them. It returns an error when a delete fails, a ref moved
// since it was listed, or the default branch is kept.
func repairRefs(ctx context.Context, rs ref.RefStore, target repairTarget, yes bool, out io.Writer) error {
	repoID := target.repoID
	assertf(repoID != "", "repairRefs: empty repo ID")
	scan, err := scanRepairRefs(ctx, rs, target, repairRefsPage)
	if target.release != nil {
		target.release()
	}
	if err != nil {
		return err
	}
	if len(scan.missing) > 0 && target.packsUnchanged != nil {
		// A pack that a push registered during the scan can hold an object
		// that the scan read as missing. A push registers its pack before it
		// moves a ref, so the packs of the scan's start are every pack that
		// a ref listed by the scan's end can need, when the set has not
		// changed since.
		unchanged, err := target.packsUnchanged(ctx)
		if err != nil {
			return fmt.Errorf("list the packs of %s again: %w", sanitizeTerm(repoID), err)
		}
		if !unchanged {
			return fmt.Errorf("the registered packs of %s changed while the command read them: no ref is listed or deleted, run the command again", sanitizeTerm(repoID))
		}
	}
	fmt.Fprintf(out, "repo:    %s\n", sanitizeTerm(repoID))
	if target.url != "" {
		fmt.Fprintf(out, "url:     %s\n", sanitizeTerm(target.url))
	}
	fmt.Fprintf(out, "scanned: %d refs\n", scan.scanned)
	if scan.scanned == 0 {
		// The metadata row exists, and the row and the refs share one
		// database URL, so the repository has no refs.
		fmt.Fprintln(out, "this repository has no refs: it is empty, it was purged, or its last refs were just deleted")
		return nil
	}
	found := append(append(append([]repairRef(nil), scan.unlockable...), scan.nonCommit...), scan.missing...)
	if len(found) == 0 {
		fmt.Fprintln(out, "no ref that git can never lock, no branch whose tip is not a commit, and no ref whose tip object is missing")
		return nil
	}
	if len(scan.unlockable) > 0 {
		fmt.Fprintf(out, "refs that git can never lock (%d):\n", len(scan.unlockable))
		printRepairRefs(out, target, scan.unlockable)
	}
	if len(scan.nonCommit) > 0 {
		fmt.Fprintf(out, "branches whose tip is not a commit (%d):\n", len(scan.nonCommit))
		printRepairRefs(out, target, scan.nonCommit)
	}
	if len(scan.missing) > 0 {
		fmt.Fprintf(out, "refs whose tip object is missing (%d):\n", len(scan.missing))
		printRepairRefs(out, target, scan.missing)
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
		won, err := deleteRepairRef(ctx, rs, repoID, r)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				// A signal ended the swap: say that, not the text of the
				// store's transaction. With PostgreSQL the swap can have
				// committed, and the next run lists what is left.
				return fmt.Errorf("delete %s: %w (%d deleted before the error)", sanitizeTerm(r.name), ctxErr, deleted)
			}
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
	if len(found) > 0 {
		fmt.Fprintf(out, "deleted %d of %d refs\n", deleted, len(found))
	}
	if lost > 0 || kept > 0 {
		return fmt.Errorf("%d of %d refs were kept (%d changed since they were listed, %d are the default branch): see above, then run the command again", lost+kept, len(found), lost, kept)
	}
	return nil
}

// printRepairRefs prints one line for each ref: its SHA, its name, and what
// the command knows about it.
func printRepairRefs(out io.Writer, target repairTarget, refs []repairRef) {
	for _, r := range refs {
		var notes []string
		if name, ok := repairKindNames[r.kind]; ok {
			notes = append(notes, name)
		}
		if target.isDefaultBranch(r.name) {
			notes = append(notes, "the default branch: not deleted")
		}
		line := fmt.Sprintf("  %s %s", r.sha, sanitizeTerm(r.name))
		if len(notes) > 0 {
			line += "  (" + strings.Join(notes, ", ") + ")"
		}
		fmt.Fprintln(out, line)
	}
}

// repairScan is what scanRepairRefs found in one repository.
type repairScan struct {
	unlockable []repairRef // names that git can never lock
	nonCommit  []repairRef // branches at a blob, a tree or an annotated tag
	missing    []repairRef // refs of any namespace whose tip object the packs do not hold
	scanned    int
}

// scanRepairRefs reads every ref of the repository, pageSize at a time. It
// keeps the refs that wedge hydration and counts the rest, so memory does not
// grow with the repository. The tips of the refs that git can lock are read
// for their object types once for each page.
func scanRepairRefs(ctx context.Context, rs ref.RefStore, target repairTarget, pageSize int) (repairScan, error) {
	assertf(pageSize > 0, "scanRepairRefs: page size %d", pageSize)
	var scan repairScan
	after := ""
	for {
		page, err := rs.ListRefsAfter(ctx, target.repoID, after, pageSize)
		if err != nil {
			return repairScan{}, fmt.Errorf("list refs of %s: %w", sanitizeTerm(target.repoID), err)
		}
		var lockable []repairRef
		for _, e := range page {
			assertf(e.Name > after, "ListRefsAfter yielded %q not above cursor %q", e.Name, after)
			after = e.Name
			scan.scanned++
			if ref.Unlockable(e.Name) {
				// The name rule comes first and needs no object.
				scan.unlockable = append(scan.unlockable, repairRef{name: e.Name, sha: e.SHA})
				continue
			}
			lockable = append(lockable, repairRef{name: e.Name, sha: e.SHA})
		}
		if err := scan.classify(ctx, target, lockable); err != nil {
			return repairScan{}, err
		}
		if len(page) < pageSize {
			return scan, nil
		}
	}
}

// classify reads the object type of each ref tip, lists a ref whose tip no
// pack holds, whatever its namespace, and lists a branch whose tip is not a
// commit. git accepts a blob, a tree or an annotated tag under any other
// name. A type that it does not know is an error: the command deletes
// nothing on a guess.
func (s *repairScan) classify(ctx context.Context, target repairTarget, refs []repairRef) error {
	if len(refs) == 0 {
		return nil
	}
	assertf(target.kinds != nil, "repairRefs: %d refs and no reader of object types", len(refs))
	var ids []string
	seen := map[string]bool{}
	for _, r := range refs {
		if !seen[r.sha] {
			seen[r.sha] = true
			ids = append(ids, r.sha)
		}
	}
	kinds, err := target.kinds(ctx, ids)
	if err != nil {
		return fmt.Errorf("read the object types of the ref tips of %s: %w", sanitizeTerm(target.repoID), err)
	}
	for _, r := range refs {
		kind, ok := kinds[r.sha]
		if !ok {
			return fmt.Errorf("no object type for the tip %s of %s", sanitizeTerm(r.sha), sanitizeTerm(r.name))
		}
		r.kind = kind
		switch kind {
		case "commit":
		case "blob", "tree", "tag":
			if strings.HasPrefix(r.name, repairBranchPrefix) {
				s.nonCommit = append(s.nonCommit, r)
			}
		case "missing":
			s.missing = append(s.missing, r)
		default:
			return fmt.Errorf("unknown object type %q for the tip %s of %s", sanitizeTerm(kind), sanitizeTerm(r.sha), sanitizeTerm(r.name))
		}
	}
	return nil
}

// deleteRepairRef swaps r's listed SHA for the zero SHA. It refuses a ref
// that is healthy: one that git can lock, whose tip object a pack holds, and
// that is no branch at a blob, a tree or an annotated tag, whatever listed
// it. won is false when the ref no longer holds the listed SHA. The swap is
// not retried.
func deleteRepairRef(ctx context.Context, rs ref.RefStore, repoID string, r repairRef) (won bool, err error) {
	_, nonCommit := repairKindNames[r.kind]
	if !ref.Unlockable(r.name) && !(nonCommit && strings.HasPrefix(r.name, repairBranchPrefix)) && r.kind != "missing" {
		return false, errors.New("git can lock this ref, and its tip is a commit or an object that git accepts under this name: refusing to delete it")
	}
	return rs.CASRef(ctx, repoID, r.name, r.sha, ref.ZeroSHA)
}

// packTipKinds reads the object types of ref tips from the packs that the
// repository has in the blob store, without a ref sync: a repository with a
// wedging ref cannot sync its refs. The first read downloads the registered
// packs into a temporary cache repository (a snapshot: an object that a later
// push adds reads as missing, and unchanged tells whether that can have
// happened), and close removes it.
type packTipKinds struct {
	rs      ref.RefStore
	repoID  string
	blobURL string

	blobs    blob.BlobStore
	dir      string
	repoPath string
	packIDs  []string // the registered packs when the first read began
}

func (p *packTipKinds) kinds(ctx context.Context, ids []string) (map[string]string, error) {
	assertf(len(ids) > 0, "packTipKinds: no ids")
	if p.repoPath == "" {
		if err := p.stage(ctx); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr // not the text of a git that was killed
			}
			return nil, err
		}
	}
	return readObjectKinds(ctx, p.repoPath, ids)
}

// stage downloads the packs of the repository into a temporary cache.
func (p *packTipKinds) stage(ctx context.Context) (err error) {
	if p.blobURL == "" {
		return errBlobURLRequired()
	}
	defer func() {
		if err != nil {
			p.close()
		}
	}()
	// Not p.blobs = blob.Open(...): on an error Open returns a typed nil
	// inside the interface, and close would call Close on it.
	blobs, err := blob.Open(ctx, p.blobURL)
	if err != nil {
		return fmt.Errorf("open blob store: %w", err)
	}
	p.blobs = blobs
	if p.dir, err = os.MkdirTemp("", "openstorage-repair-refs-"); err != nil {
		return err
	}
	repoPath, err := githttp.EnsureCacheRepoCtx(ctx, p.dir, p.repoID)
	if err != nil {
		return fmt.Errorf("create the temporary cache repository: %w", err)
	}
	// Listed before the download: a pack that registers after this list is
	// not in packIDs, and unchanged then answers false.
	if p.packIDs, err = p.registeredPacks(ctx); err != nil {
		return err
	}
	if err := githttp.HydratePacks(ctx, p.rs, p.blobs, repoPath, p.repoID); err != nil {
		return fmt.Errorf("download the packs of %s: %w", sanitizeTerm(p.repoID), err)
	}
	p.repoPath = repoPath
	return nil
}

// registeredPacks lists the IDs of the packs that the repository has now.
func (p *packTipKinds) registeredPacks(ctx context.Context) ([]string, error) {
	packs, err := p.rs.ListPacks(ctx, p.repoID)
	if err != nil {
		return nil, fmt.Errorf("list the packs of %s: %w", sanitizeTerm(p.repoID), err)
	}
	ids := make([]string, 0, len(packs))
	for _, pk := range packs {
		ids = append(ids, pk.ID)
	}
	slices.Sort(ids)
	return ids, nil
}

// unchanged reports whether the repository has the same registered packs as
// when the first read began. It is true without a first read.
func (p *packTipKinds) unchanged(ctx context.Context) (bool, error) {
	if p.packIDs == nil && p.repoPath == "" {
		return true, nil
	}
	now, err := p.registeredPacks(ctx)
	if err != nil {
		return false, err
	}
	return slices.Equal(now, p.packIDs), nil
}

func (p *packTipKinds) close() {
	if p.blobs != nil {
		_ = p.blobs.Close()
		p.blobs = nil
	}
	if p.dir != "" {
		_ = os.RemoveAll(p.dir)
		p.dir, p.repoPath = "", ""
	}
}

// readObjectKinds asks git for the object type of each id, which is a full
// SHA, in one batch-check child. The answer is git's own: a type name, or
// "missing" for an object that the repository does not hold.
func readObjectKinds(ctx context.Context, repoPath string, ids []string) (map[string]string, error) {
	for _, id := range ids {
		if !ref.ValidSHA(id) {
			return nil, fmt.Errorf("%q is not a full SHA", sanitizeTerm(id))
		}
	}
	out, err := gitread.Run(ctx, gitread.Cmd{
		Class: gitread.Light, Dir: repoPath,
		Stdin: strings.NewReader(strings.Join(ids, "\n") + "\n"),
		Args:  []string{"cat-file", "--batch-check=%(objectname) %(objecttype)"},
	})
	if err != nil {
		return nil, err
	}
	var lines []string
	if out != "" {
		lines = strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	}
	if len(lines) != len(ids) {
		return nil, fmt.Errorf("cat-file --batch-check answered %d lines for %d ids", len(lines), len(ids))
	}
	kinds := make(map[string]string, len(ids))
	for i, line := range lines {
		id, kind, ok := strings.Cut(line, " ")
		if !ok || id != ids[i] {
			return nil, fmt.Errorf("cat-file --batch-check answered %q for %s", sanitizeTerm(line), ids[i])
		}
		switch kind {
		case "commit", "tree", "blob", "tag", "missing":
			kinds[id] = kind
		default:
			return nil, fmt.Errorf("cat-file --batch-check answered %q for %s", sanitizeTerm(line), ids[i])
		}
	}
	return kinds, nil
}
