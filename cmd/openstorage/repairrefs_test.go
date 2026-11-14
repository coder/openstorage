package main

// Tests for repair-refs: on every ref store, a repository that holds a ref
// git can never lock fails hydration, the dry run lists only that ref, --yes
// deletes only it, and hydration then works.

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/githttp"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref"
	"github.com/coder/openstorage/internal/ref/s3wal"
	"github.com/coder/openstorage/internal/testutil/testimages"
)

const repairRepo = "repo_0123456789ab"

var (
	// A notes ref name whose last part git cannot lock (251 bytes), and
	// the longest one it can (250 bytes).
	repairLongRef = "refs/notes/x/" + strings.Repeat("n", 251)
	repairOKRef   = "refs/notes/x/" + strings.Repeat("n", 250)
)

// repairGitRun runs git in an isolated environment and returns its output and
// its error text.
func repairGitRun(dir, stdin string, args ...string) (stdout, stderr string, err error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = []string{
		"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_TERMINAL_PROMPT=0",
		"GIT_AUTHOR_NAME=repair-test", "GIT_AUTHOR_EMAIL=repair@test.invalid",
		"GIT_COMMITTER_NAME=repair-test", "GIT_COMMITTER_EMAIL=repair@test.invalid",
		"GIT_AUTHOR_DATE=@1700000000 +0000", "GIT_COMMITTER_DATE=@1700000000 +0000",
		"HOME=/dev/null", "PATH=" + os.Getenv("PATH"),
	}
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	var errBuf bytes.Buffer
	cmd.Stderr = &errBuf
	out, err := cmd.Output()
	return string(out), errBuf.String(), err
}

func repairGit(t *testing.T, dir, stdin string, args ...string) string {
	t.Helper()
	out, stderr, err := repairGitRun(dir, stdin, args...)
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, stderr)
	}
	return strings.TrimSpace(out)
}

// repairFixture is one repository in a ref store: a commit, a blob, a tree
// and an annotated tag, their pack in a file blob store, and the refs that a
// test plants.
type repairFixture struct {
	repo    string
	rs      ref.RefStore
	blobs   blob.BlobStore
	blobURL string
	tips    *packTipKinds
	scratch string
	commit  string
	other   string // another valid SHA to move a ref to
	late    string // a commit that no registered pack holds until registerLatePack
	blob    string
	tree    string
	tag     string
}

func newRepairFixture(t *testing.T, rs ref.Store) *repairFixture {
	t.Helper()
	return newRepairFixtureFor(t, rs, repairRepo)
}

// createMetaRepo makes a repository row in the metadata store at dbURL, as
// the API does, and returns its server-generated ID.
func createMetaRepo(t *testing.T, dbURL, defaultBranch string) string {
	t.Helper()
	ctx := t.Context()
	ms, err := meta.Open(ctx, dbURL)
	if err != nil {
		t.Fatalf("open meta store: %v", err)
	}
	defer ms.Close()
	if err := ms.InitSchema(ctx); err != nil {
		t.Fatalf("meta InitSchema: %v", err)
	}
	if _, err := ms.UpsertOrgWithKeys(ctx, "repair-org", []string{"-----BEGIN PUBLIC KEY-----\ndummy\n-----END PUBLIC KEY-----\n"}); err != nil {
		t.Fatalf("seed org: %v", err)
	}
	repo, err := ms.CreateRepo(ctx, "repair-org", "https://example.com/repair.git", defaultBranch)
	if err != nil {
		t.Fatalf("CreateRepo: %v", err)
	}
	return repo.RepoID
}

// target is the repository as repairRefs takes it, with default branch main.
func (f *repairFixture) target() repairTarget {
	return repairTarget{repoID: f.repo, defaultBranch: "main", kinds: f.tips.kinds, release: f.tips.close, packsUnchanged: f.tips.unchanged}
}

func newRepairFixtureFor(t *testing.T, rs ref.Store, repo string) *repairFixture {
	t.Helper()
	ctx := t.Context()
	t.Cleanup(rs.Close)
	if err := rs.InitSchema(ctx); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	dir := t.TempDir()
	blobURL := "file://" + filepath.Join(dir, "blobs")
	bl, err := blob.Open(ctx, blobURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bl.Close() })
	if err := bl.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}

	scratch := filepath.Join(dir, "scratch.git")
	repairGit(t, "", "", "init", "--bare", "--initial-branch=main", scratch)
	blobSHA := repairGit(t, scratch, "hi\n", "hash-object", "-w", "--stdin")
	treeSHA := repairGit(t, scratch, "100644 blob "+blobSHA+"\tf.txt\n", "mktree")
	commit := repairGit(t, scratch, "", "commit-tree", treeSHA, "-m", "c1")
	other := repairGit(t, scratch, "", "commit-tree", treeSHA, "-p", commit, "-m", "c2")
	repairGit(t, scratch, "", "tag", "-a", "-m", "annotated", "v1", commit)
	tagSHA := repairGit(t, scratch, "", "rev-parse", "refs/tags/v1")
	late := repairGit(t, scratch, "", "commit-tree", treeSHA, "-p", other, "-m", "c3")
	packDir := t.TempDir()
	objs := strings.Join([]string{blobSHA, treeSHA, commit, other, tagSHA}, "\n") + "\n"
	id := repairGit(t, scratch, objs, "pack-objects", "--threads=1", "-q", filepath.Join(packDir, "p"))
	packPath := filepath.Join(packDir, "p-"+id+".pack")
	key := blob.PackKey(repo, id)
	if err := bl.UploadFile(ctx, key, packPath); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(packPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := rs.RegisterPacks(ctx, repo, []ref.Pack{{ID: id, Key: key, Size: fi.Size()}}); err != nil {
		t.Fatalf("RegisterPacks: %v", err)
	}
	tips := &packTipKinds{rs: rs, repoID: repo, blobURL: blobURL}
	t.Cleanup(tips.close)
	return &repairFixture{repo: repo, rs: rs, blobs: bl, blobURL: blobURL, tips: tips, scratch: scratch,
		commit: commit, other: other, late: late, blob: blobSHA, tree: treeSHA, tag: tagSHA}
}

// registerLatePack uploads and registers a pack that holds the late commit,
// as a push that lands while the command runs.
func (f *repairFixture) registerLatePack(t *testing.T) {
	t.Helper()
	packDir := t.TempDir()
	id := repairGit(t, f.scratch, f.late+"\n", "pack-objects", "--threads=1", "-q", filepath.Join(packDir, "p"))
	packPath := filepath.Join(packDir, "p-"+id+".pack")
	key := blob.PackKey(f.repo, id)
	if err := f.blobs.UploadFile(t.Context(), key, packPath); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(packPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.rs.RegisterPacks(t.Context(), f.repo, []ref.Pack{{ID: id, Key: key, Size: fi.Size()}}); err != nil {
		t.Fatalf("RegisterPacks: %v", err)
	}
}

// plant writes refs into the store the way an old API node did: the raw
// store takes any name.
func (f *repairFixture) plant(t *testing.T, refs map[string]string) {
	t.Helper()
	for name, sha := range refs {
		won, err := f.rs.CASRef(t.Context(), f.repo, name, ref.ZeroSHA, sha)
		if err != nil || !won {
			t.Fatalf("plant %.40s: won=%v err=%v", name, won, err)
		}
	}
}

// hydrateInto hydrates the repository into a cache directory. A fresh one
// models another replica, and reuse models the same replica.
func (f *repairFixture) hydrateInto(t *testing.T, cacheDir string) (repoPath string, hydrateErr error) {
	t.Helper()
	repoPath, refsErr, err := githttp.Hydrate(t.Context(), f.rs, f.blobs, cacheDir, f.repo, "main")
	return repoPath, errors.Join(refsErr, err)
}

func (f *repairFixture) refs(t *testing.T) map[string]string {
	t.Helper()
	got, err := f.rs.ListRefs(t.Context(), f.repo)
	if err != nil {
		t.Fatalf("ListRefs: %v", err)
	}
	return got
}

// movingStore moves one ref right after the scan lists it, as a concurrent
// writer would between the list and the delete.
type movingStore struct {
	ref.RefStore
	t        *testing.T
	name     string
	from, to string
	moved    bool
}

func (s *movingStore) ListRefsAfter(ctx context.Context, repo, after string, limit int) ([]ref.RefEntry, error) {
	page, err := s.RefStore.ListRefsAfter(ctx, repo, after, limit)
	if err == nil && len(page) < limit && !s.moved {
		s.moved = true
		won, err := s.RefStore.CASRef(ctx, repo, s.name, s.from, s.to)
		if err != nil || !won {
			s.t.Fatalf("move %.40s: won=%v err=%v", s.name, won, err)
		}
	}
	return page, err
}

// repairBackend opens an empty ref store and returns the database URL that
// the command takes from the environment ("" when the store has none).
type repairBackend struct {
	name string
	open func(t *testing.T) (ref.Store, string)
}

func repairBackends() []repairBackend {
	return []repairBackend{
		{"sqlite", func(t *testing.T) (ref.Store, string) {
			dbURL := "sqlite://" + filepath.Join(t.TempDir(), "refs.db")
			rs, err := ref.OpenSQLite(t.Context(), dbURL)
			if err != nil {
				t.Fatal(err)
			}
			return rs, dbURL
		}},
		{"s3wal", func(t *testing.T) (ref.Store, string) {
			return ref.NewS3WAL(s3wal.NewMemCAS(), time.Now), ""
		}},
		{"pg", func(t *testing.T) (ref.Store, string) {
			if testing.Short() {
				t.Skip("needs docker")
			}
			ctx := t.Context()
			pgC, err := tcpostgres.Run(ctx, testimages.Postgres,
				tcpostgres.WithDatabase("postgres"),
				tcpostgres.WithUsername("postgres"),
				tcpostgres.WithPassword("postgres"),
				tcpostgres.BasicWaitStrategies(),
			)
			if err != nil {
				t.Fatalf("start postgres container: %v", err)
			}
			t.Cleanup(func() { _ = pgC.Terminate(context.Background()) })
			dbURL, err := pgC.ConnectionString(ctx, "sslmode=disable")
			if err != nil {
				t.Fatal(err)
			}
			rs, err := ref.OpenPG(ctx, dbURL)
			if err != nil {
				t.Fatal(err)
			}
			return rs, dbURL
		}},
	}
}

func TestRepairRefs(t *testing.T) {
	for _, b := range repairBackends() {
		t.Run(b.name, func(t *testing.T) {
			rs, dbURL := b.open(t)
			repo := repairRepo
			if dbURL != "" {
				repo = createMetaRepo(t, dbURL, "main")
			}
			testRepairRefs(t, newRepairFixtureFor(t, rs, repo), dbURL)
		})
	}
}

func testRepairRefs(t *testing.T, f *repairFixture, dbURL string) {
	ctx := t.Context()
	f.plant(t, map[string]string{
		"refs/heads/main": f.commit,
		repairOKRef:       f.commit,
		repairLongRef:     f.commit,
	})
	want := f.refs(t)

	// Hydration fails first, on the ref that git cannot lock.
	failedCache := filepath.Join(t.TempDir(), "cache")
	if _, err := f.hydrateInto(t, failedCache); err == nil || !strings.Contains(err.Error(), "File name too long") {
		t.Fatalf("hydrate before the repair = %v, want a File name too long failure", err)
	}

	// The dry run lists only that ref and changes nothing.
	var out bytes.Buffer
	if err := repairRefs(ctx, f.rs, f.target(), false, &out); err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if !strings.Contains(out.String(), f.commit+" "+repairLongRef+"\n") ||
		strings.Contains(out.String(), repairOKRef+"\n") || strings.Contains(out.String(), "refs/heads/main") ||
		!strings.Contains(out.String(), "scanned: 3 refs") || !strings.Contains(out.String(), "nothing changed") {
		t.Fatalf("dry run output:\n%s", out.String())
	}
	if got := f.refs(t); !mapsEqual(got, want) {
		t.Fatalf("dry run changed the refs: %v", got)
	}

	// A ref that moves between the list and the delete is a lost CAS: it
	// is reported, kept, and not retried.
	out.Reset()
	moving := &movingStore{RefStore: f.rs, t: t, name: repairLongRef, from: f.commit, to: f.other}
	err := repairRefs(ctx, moving, f.target(), true, &out)
	if err == nil || !strings.Contains(err.Error(), "changed since they were listed") ||
		!strings.Contains(out.String(), "lost CAS") || !strings.Contains(out.String(), "deleted 0 of 1 refs") {
		t.Fatalf("lost CAS = %v\n%s", err, out.String())
	}
	if got := f.refs(t)[repairLongRef]; got != f.other {
		t.Fatalf("the moved ref holds %q after the lost CAS, want %q", got, f.other)
	}

	// --yes deletes only the ref that git cannot lock.
	out.Reset()
	if err := repairRefs(ctx, f.rs, f.target(), true, &out); err != nil {
		t.Fatalf("--yes: %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "deleted: "+repairLongRef+"\n") || !strings.Contains(out.String(), "deleted 1 of 1 refs") {
		t.Fatalf("--yes output:\n%s", out.String())
	}
	delete(want, repairLongRef)
	if got := f.refs(t); !mapsEqual(got, want) {
		t.Fatalf("refs after --yes = %v, want %v", got, want)
	}

	// Hydration works again: on the replica that failed, and on a new one.
	for _, cache := range []string{failedCache, filepath.Join(t.TempDir(), "cache")} {
		repoPath, err := f.hydrateInto(t, cache)
		if err != nil {
			t.Fatalf("hydrate after the repair (%s): %v", cache, err)
		}
		cached := map[string]string{}
		for _, line := range strings.Split(repairGit(t, repoPath, "", "for-each-ref", "--format=%(refname) %(objectname)"), "\n") {
			name, sha, _ := strings.Cut(line, " ")
			cached[name] = sha
		}
		if !mapsEqual(cached, want) {
			t.Fatalf("cache refs after the repair = %v, want %v", cached, want)
		}
	}

	// A second run finds nothing.
	out.Reset()
	if err := repairRefs(ctx, f.rs, f.target(), true, &out); err != nil || !strings.Contains(out.String(), "no ref that git can never lock") {
		t.Fatalf("second run = %v\n%s", err, out.String())
	}

	// A lost CAS on one ref does not stop the command: it still deletes
	// the others, then exits with an error.
	secondLong := "refs/notes/y/" + strings.Repeat("n", 251)
	f.plant(t, map[string]string{repairLongRef: f.commit, secondLong: f.commit})
	out.Reset()
	moving = &movingStore{RefStore: f.rs, t: t, name: repairLongRef, from: f.commit, to: f.other}
	err = repairRefs(ctx, moving, f.target(), true, &out)
	if err == nil || !strings.Contains(out.String(), "deleted 1 of 2 refs") || !strings.Contains(out.String(), "deleted: "+secondLong+"\n") {
		t.Fatalf("lost CAS with two refs = %v\n%s", err, out.String())
	}
	if got := f.refs(t); got[repairLongRef] != f.other || got[secondLong] != "" || len(got) != len(want)+1 {
		t.Fatalf("refs after a lost CAS with two refs = %v", got)
	}
	if err := repairRefs(ctx, f.rs, f.target(), true, &out); err != nil {
		t.Fatalf("clean up: %v", err)
	}
	if got := f.refs(t); !mapsEqual(got, want) {
		t.Fatalf("refs after the clean up = %v, want %v", got, want)
	}

	// The command line opens the store from the database URL, as an
	// operator runs it, and repairs the same way.
	if dbURL == "" {
		return
	}
	t.Setenv("OPENSTORAGE_PG_URL", dbURL)
	t.Setenv("OPENSTORAGE_S3_URL", f.blobURL)
	if err := cmdRepairRefs(ctx, []string{"--yes", "repo_000000000000"}); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("command with an ID that has no repository = %v, want not found", err)
	}
	f.plant(t, map[string]string{repairLongRef: f.commit})
	want[repairLongRef] = f.commit
	if err := cmdRepairRefs(ctx, []string{f.repo}); err != nil {
		t.Fatalf("command dry run: %v", err)
	}
	if got := f.refs(t); !mapsEqual(got, want) {
		t.Fatalf("command dry run changed the refs: %v", got)
	}
	if err := cmdRepairRefs(ctx, []string{"--yes", f.repo}); err != nil {
		t.Fatalf("command --yes: %v", err)
	}
	delete(want, repairLongRef)
	if got := f.refs(t); !mapsEqual(got, want) {
		t.Fatalf("refs after the command's --yes = %v, want %v", got, want)
	}
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// The scan pages through the refs: a matching ref on any page, including the
// last, is found, and the refs that git can lock are not.
func TestScanUnlockableRefsPages(t *testing.T) {
	rs, err := ref.OpenSQLite(t.Context(), "sqlite://"+filepath.Join(t.TempDir(), "refs.db"))
	if err != nil {
		t.Fatal(err)
	}
	f := newRepairFixture(t, rs)
	longMiddle := "refs/m/" + strings.Repeat("m", 256) + "/x"
	longLast := "refs/z/" + strings.Repeat("z", 251)
	f.plant(t, map[string]string{
		"refs/heads/a": f.commit, "refs/heads/b": f.commit, longMiddle: f.commit, repairLongRef: f.commit,
		"refs/tags/t": f.commit, longLast: f.commit, "refs/m2/" + strings.Repeat("m", 255) + "/x": f.commit,
	})
	for _, page := range []int{1, 2, 3, 7, 8, 100} {
		scan, err := scanRepairRefs(t.Context(), rs, f.target(), page)
		found := scan.unlockable
		if err != nil || scan.scanned != 7 || len(found) != 3 || found[0].name != longMiddle || found[1].name != repairLongRef || found[2].name != longLast ||
			len(scan.nonCommit) != 0 || len(scan.missing) != 0 {
			t.Errorf("page %d: found %d, scanned %d, err %v", page, len(found), scan.scanned, err)
		}
	}
}

// A ref that git can lock and that holds a commit is never deleted, whatever
// listed it. A branch at a blob, a tree or an annotated tag is deletable only
// when the scan found its type, and a tag ref at a blob never is. A ref whose
// tip object is missing is deletable in any namespace.
func TestRepairRefsRefusesLockableRef(t *testing.T) {
	rs, err := ref.OpenSQLite(t.Context(), "sqlite://"+filepath.Join(t.TempDir(), "refs.db"))
	if err != nil {
		t.Fatal(err)
	}
	f := newRepairFixture(t, rs)
	f.plant(t, map[string]string{"refs/heads/main": f.commit, repairOKRef: f.commit})
	for _, name := range []string{"refs/heads/main", repairOKRef} {
		won, err := deleteRepairRef(t.Context(), rs, f.repo, repairRef{name: name, sha: f.commit})
		if won || err == nil || !strings.Contains(err.Error(), "refusing") {
			t.Errorf("delete %.40s: won=%v err=%v, want a refusal", name, won, err)
		}
	}
	if got := f.refs(t); len(got) != 2 {
		t.Fatalf("a refusal changed the refs: %v", got)
	}
	f.plant(t, map[string]string{"refs/tags/blobtag": f.blob, "refs/heads/ghost": f.blob})
	for _, r := range []repairRef{
		{name: "refs/tags/blobtag", sha: f.blob, kind: "blob"}, // a tag may hold a blob
		{name: "refs/heads/ghost", sha: f.blob},                // no type was read
		{name: "refs/heads/ghost", sha: f.blob, kind: "commit"},
	} {
		if won, err := deleteRepairRef(t.Context(), rs, f.repo, r); won || err == nil || !strings.Contains(err.Error(), "refusing") {
			t.Errorf("delete %s as %q: won=%v err=%v, want a refusal", r.name, r.kind, won, err)
		}
	}
	if got := f.refs(t); len(got) != 4 {
		t.Fatalf("a refusal changed the refs: %v", got)
	}
}

// A repository with no refs is reported as such, and nothing is deleted.
func TestRepairRefsReportsARepoWithNoRefs(t *testing.T) {
	rs, err := ref.OpenSQLite(t.Context(), "sqlite://"+filepath.Join(t.TempDir(), "refs.db"))
	if err != nil {
		t.Fatal(err)
	}
	f := newRepairFixture(t, rs)
	f.plant(t, map[string]string{"refs/heads/main": f.commit, repairLongRef: f.commit})
	for _, yes := range []bool{false, true} {
		var out bytes.Buffer
		if err := repairRefs(t.Context(), rs, repairTarget{repoID: f.repo + "-empty", defaultBranch: "main"}, yes, &out); err != nil {
			t.Fatalf("yes=%v: %v", yes, err)
		}
		if !strings.Contains(out.String(), "scanned: 0 refs") || !strings.Contains(out.String(), "this repository has no refs") ||
			strings.Contains(out.String(), "no ref that git can never lock") {
			t.Fatalf("yes=%v: output:\n%s", yes, out.String())
		}
	}
	if got := f.refs(t); len(got) != 2 {
		t.Fatalf("refs changed: %v", got)
	}
}

// A ref name or repo ID with control bytes cannot forge a line of the
// output: not in the list, not in the deleted lines, not in the lost-CAS line.
func TestRepairRefsSanitizesNames(t *testing.T) {
	rs, err := ref.OpenSQLite(t.Context(), "sqlite://"+filepath.Join(t.TempDir(), "refs.db"))
	if err != nil {
		t.Fatal(err)
	}
	f := newRepairFixture(t, rs)
	hostile := "refs/notes/x\n  deleted: forged\x1b[2J/" + strings.Repeat("n", 251)
	f.plant(t, map[string]string{hostile: f.commit})
	forged := func(out string) bool {
		return strings.Contains(out, "\x1b") || strings.Contains(out, "\n  deleted: forged") || strings.Contains(out, "\ndeleted: forged")
	}
	var out bytes.Buffer
	if err := repairRefs(t.Context(), rs, f.target(), false, &out); err != nil {
		t.Fatal(err)
	}
	if forged(out.String()) || !strings.Contains(out.String(), "refs that git can never lock (1)") {
		t.Fatalf("the list forged output:\n%q", out.String())
	}
	out.Reset()
	moving := &movingStore{RefStore: rs, t: t, name: hostile, from: f.commit, to: f.other}
	if err := repairRefs(t.Context(), moving, f.target(), true, &out); err == nil || forged(out.String()) || !strings.Contains(out.String(), "lost CAS") {
		t.Fatalf("the lost-CAS line forged output (err %v):\n%q", err, out.String())
	}
	out.Reset()
	if err := repairRefs(t.Context(), rs, f.target(), true, &out); err != nil || forged(out.String()) || !strings.Contains(out.String(), "deleted 1 of 1 refs") {
		t.Fatalf("the deleted line forged output (err %v):\n%q", err, out.String())
	}
	out.Reset()
	if err := repairRefs(t.Context(), rs, repairTarget{repoID: "repo\x1b[2J\nforged", defaultBranch: "main"}, false, &out); err != nil || strings.Contains(out.String(), "\x1b") || strings.Contains(out.String(), "\nforged") {
		t.Fatalf("the repo line forged output (err %v):\n%q", err, out.String())
	}
}

// failingCASStore fails every swap.
type failingCASStore struct {
	ref.RefStore
	calls int
}

func (s *failingCASStore) CASRef(_ context.Context, repo, name, _, _ string) (bool, error) {
	s.calls++
	return false, fmt.Errorf("cas %s/%s: store is down", repo, name)
}

// A store error stops the command at the first ref and is reported with the
// ref and the count of refs deleted so far. The error text of the store can
// quote the ref name, so it cannot forge output either.
func TestRepairRefsStopsOnStoreError(t *testing.T) {
	rs, err := ref.OpenSQLite(t.Context(), "sqlite://"+filepath.Join(t.TempDir(), "refs.db"))
	if err != nil {
		t.Fatal(err)
	}
	f := newRepairFixture(t, rs)
	f.plant(t, map[string]string{"refs/notes/x\x1b[2J/" + strings.Repeat("n", 251): f.commit, "refs/notes/y/" + strings.Repeat("n", 251): f.commit})
	failing := &failingCASStore{RefStore: rs}
	var out bytes.Buffer
	err = repairRefs(t.Context(), failing, f.target(), true, &out)
	if err == nil || !strings.Contains(err.Error(), "store is down") || !strings.Contains(err.Error(), "0 deleted before the error") ||
		strings.Contains(err.Error(), "\x1b") || failing.calls != 1 {
		t.Fatalf("store error = %v after %d swaps\n%s", err, failing.calls, out.String())
	}
	if got := f.refs(t); len(got) != 2 {
		t.Fatalf("refs changed: %v", got)
	}
}

func TestCmdRepairRefs(t *testing.T) {
	for _, env := range []string{
		"OPENSTORAGE_PG_URL", "SPIKE_PG_URL", "PG_URL",
		"OPENSTORAGE_S3_URL", "SPIKE_S3_URL", "BLOB_URL",
		envDataDir, envSecretsKey, envPackQuota, envPackReservationTTL,
	} {
		t.Setenv(env, "")
	}
	ctx := t.Context()
	dir := t.TempDir()
	dbURL := "sqlite://" + filepath.Join(dir, "codestorage.db")
	repo := createMetaRepo(t, dbURL, "main")
	rs, err := ref.OpenSQLite(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	f := newRepairFixtureFor(t, rs, repo)
	t.Setenv("OPENSTORAGE_S3_URL", f.blobURL)
	f.plant(t, map[string]string{"refs/heads/main": f.commit, repairOKRef: f.commit, repairLongRef: f.commit})
	want := f.refs(t)

	if err := cmdRepairRefs(ctx, []string{"--data-dir", dir}); err == nil || !strings.Contains(err.Error(), "exactly one <repo-id>") {
		t.Fatalf("no arg = %v, want usage error", err)
	}
	if err := cmdRepairRefs(ctx, []string{"--data-dir", dir, repo, "extra"}); err == nil || !strings.Contains(err.Error(), "exactly one <repo-id>") || !strings.Contains(err.Error(), "flags go before it") {
		t.Fatalf("two args = %v, want usage error", err)
	}
	if err := cmdRepairRefs(ctx, []string{"--data-dir", dir, ""}); err == nil || !strings.Contains(err.Error(), "must not be empty") {
		t.Fatalf("empty ID = %v, want a refusal", err)
	}
	if err := cmdRepairRefs(ctx, []string{repo}); err == nil || !strings.Contains(err.Error(), "database URL required") {
		t.Fatalf("no database = %v, want the missing URL error", err)
	}

	// An ID with no repository (a typo, or the colon that follows the ID in
	// the serve log) is an error, not a clean repository.
	for _, id := range []string{"repo_000000000000", repo + ":"} {
		if err := cmdRepairRefs(ctx, []string{"--data-dir", dir, "--yes", id}); err == nil || !strings.Contains(err.Error(), "not found") {
			t.Fatalf("ID %q = %v, want not found", id, err)
		}
	}

	// The output names the repository and its URL from the metadata row.
	var listed bytes.Buffer
	if err := runRepairRefs(ctx, []string{"--data-dir", dir, repo}, &listed); err != nil ||
		!strings.Contains(listed.String(), "repo:    "+repo+"\n") || !strings.Contains(listed.String(), "url:     https://example.com/repair.git\n") {
		t.Fatalf("list = %v\n%s", err, listed.String())
	}

	// Without --yes it changes nothing.
	if err := cmdRepairRefs(ctx, []string{"--data-dir", dir, repo}); err != nil {
		t.Fatalf("dry run = %v", err)
	}
	if got := f.refs(t); !mapsEqual(got, want) {
		t.Fatalf("dry run changed the refs: %v", got)
	}
	if err := cmdRepairRefs(ctx, []string{"--data-dir", dir, "--yes", repo}); err != nil {
		t.Fatalf("--yes = %v", err)
	}
	delete(want, repairLongRef)
	if got := f.refs(t); !mapsEqual(got, want) {
		t.Fatalf("refs after --yes = %v, want %v", got, want)
	}
}

// Only --yes runs the schema setup: the delete bumps the repository
// generation through a trigger that InitSchema installs, and the list only
// reads.
func TestCmdRepairRefsRunsSchemaSetupOnlyWithYes(t *testing.T) {
	for _, env := range []string{
		"OPENSTORAGE_PG_URL", "SPIKE_PG_URL", "PG_URL",
		"OPENSTORAGE_S3_URL", "SPIKE_S3_URL", "BLOB_URL",
		envDataDir, envSecretsKey, envPackQuota, envPackReservationTTL,
	} {
		t.Setenv(env, "")
	}
	ctx := t.Context()
	dir := t.TempDir()
	repo := createMetaRepo(t, "sqlite://"+filepath.Join(dir, "codestorage.db"), "main")
	db, err := sql.Open("sqlite", filepath.Join(dir, "codestorage.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	hasRefsTable := func() bool {
		t.Helper()
		var n int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'refs'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n == 1
	}
	if hasRefsTable() {
		t.Fatal("a database with only the metadata schema already has the refs table")
	}
	_ = cmdRepairRefs(ctx, []string{"--data-dir", dir, repo})
	if hasRefsTable() {
		t.Fatal("the list ran the schema setup")
	}
	if err := cmdRepairRefs(ctx, []string{"--data-dir", dir, "--yes", repo}); err != nil {
		t.Fatalf("--yes on a new database = %v", err)
	}
	if !hasRefsTable() {
		t.Fatal("--yes did not run the schema setup")
	}
}

// The default branch is never deleted, as DELETE /repos/branches refuses it:
// the list marks it, --yes keeps it and deletes the other refs, and the
// command exits with an error.
func TestRepairRefsKeepsTheDefaultBranch(t *testing.T) {
	rs, err := ref.OpenSQLite(t.Context(), "sqlite://"+filepath.Join(t.TempDir(), "refs.db"))
	if err != nil {
		t.Fatal(err)
	}
	f := newRepairFixture(t, rs)
	longBranch := "x/" + strings.Repeat("b", 251)
	f.plant(t, map[string]string{"refs/heads/main": f.commit, "refs/heads/" + longBranch: f.commit, repairLongRef: f.commit})
	for _, def := range []string{longBranch, "refs/heads/" + longBranch} {
		target := repairTarget{repoID: f.repo, defaultBranch: def, kinds: f.tips.kinds}
		var out bytes.Buffer
		if err := repairRefs(t.Context(), rs, target, false, &out); err != nil || !strings.Contains(out.String(), "(the default branch: not deleted)") {
			t.Fatalf("default %.12q: dry run = %v\n%s", def, err, out.String())
		}
		out.Reset()
		err := repairRefs(t.Context(), rs, target, true, &out)
		if err == nil || !strings.Contains(err.Error(), "1 are the default branch") || !strings.Contains(out.String(), "kept: refs/heads/"+longBranch) ||
			!strings.Contains(out.String(), "deleted: "+repairLongRef) || !strings.Contains(out.String(), "deleted 1 of 2 refs") {
			t.Fatalf("default %.12q: --yes = %v\n%s", def, err, out.String())
		}
		if got := f.refs(t); got["refs/heads/"+longBranch] != f.commit || got[repairLongRef] != "" || len(got) != 2 {
			t.Fatalf("default %.12q: refs after --yes = %v", def, got)
		}
		f.plant(t, map[string]string{repairLongRef: f.commit})
	}
}

// The command reads the default branch from the metadata store.
func TestCmdRepairRefsKeepsTheDefaultBranchOfTheRepo(t *testing.T) {
	for _, env := range []string{
		"OPENSTORAGE_PG_URL", "SPIKE_PG_URL", "PG_URL",
		"OPENSTORAGE_S3_URL", "SPIKE_S3_URL", "BLOB_URL",
		envDataDir, envSecretsKey, envPackQuota, envPackReservationTTL,
	} {
		t.Setenv(env, "")
	}
	ctx := t.Context()
	dir := t.TempDir()
	dbURL := "sqlite://" + filepath.Join(dir, "codestorage.db")
	longBranch := strings.Repeat("d", 251)
	repo := createMetaRepo(t, dbURL, longBranch)
	rs, err := ref.OpenSQLite(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	f := newRepairFixtureFor(t, rs, repo)
	t.Setenv("OPENSTORAGE_S3_URL", f.blobURL)
	f.plant(t, map[string]string{"refs/heads/main": f.commit, "refs/heads/" + longBranch: f.commit, repairLongRef: f.commit})
	if err := cmdRepairRefs(ctx, []string{"--data-dir", dir, "--yes", repo}); err == nil || !strings.Contains(err.Error(), "default branch") {
		t.Fatalf("--yes = %v, want the default-branch error", err)
	}
	if got := f.refs(t); got["refs/heads/"+longBranch] != f.commit || got[repairLongRef] != "" || len(got) != 2 {
		t.Fatalf("refs after --yes = %v", got)
	}
}

// The repository's URL comes from the metadata store, and a URL with control
// bytes cannot forge a line either.
func TestRepairRefsPrintsTheSanitizedURL(t *testing.T) {
	rs, err := ref.OpenSQLite(t.Context(), "sqlite://"+filepath.Join(t.TempDir(), "refs.db"))
	if err != nil {
		t.Fatal(err)
	}
	f := newRepairFixture(t, rs)
	f.plant(t, map[string]string{"refs/heads/main": f.commit})
	target := repairTarget{repoID: f.repo, url: "https://x/\x1b[2J\nforged", defaultBranch: "main", kinds: f.tips.kinds}
	var out bytes.Buffer
	if err := repairRefs(t.Context(), rs, target, false, &out); err != nil || !strings.Contains(out.String(), "url:     https://x/") ||
		strings.Contains(out.String(), "\x1b") || strings.Contains(out.String(), "\nforged") {
		t.Fatalf("url line (err %v):\n%q", err, out.String())
	}
}

func isolateStorageEnv(t *testing.T) {
	t.Helper()
	for _, env := range []string{
		"OPENSTORAGE_PG_URL", "SPIKE_PG_URL", "PG_URL",
		"OPENSTORAGE_S3_URL", "SPIKE_S3_URL", "BLOB_URL",
		envDataDir, envSecretsKey, envPackQuota, envPackReservationTTL,
	} {
		t.Setenv(env, "")
	}
}

// sqliteTables lists the tables of the SQLite database at path.
func sqliteTables(t *testing.T, path string) map[string]bool {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.QueryContext(t.Context(), `SELECT name FROM sqlite_master WHERE type = 'table'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	tables := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		tables[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	// Every caller has set up the metadata schema, so a listing without its
	// repos table is a broken helper, and the absence checks prove nothing.
	if !tables["repos"] {
		t.Fatalf("test setup: no repos table in %s: %v", path, tables)
	}
	return tables
}

// The command reads the repository row and sets up no metadata schema, with
// or without --yes, and it needs no secrets key: a table that the metadata
// schema setup would create stays absent, and a key that fails to parse does
// not matter.
func TestCmdRepairRefsLeavesTheMetadataSchemaAlone(t *testing.T) {
	isolateStorageEnv(t)
	t.Setenv(envSecretsKey, "not a key")
	if _, err := resolveSecretsCodec(os.Getenv); err == nil {
		t.Fatal("test setup: the secrets key parses")
	}
	ctx := t.Context()
	dir := t.TempDir()
	path := filepath.Join(dir, "codestorage.db")
	repo := createMetaRepo(t, "sqlite://"+path, "main")
	rs, err := ref.OpenSQLite(ctx, "sqlite://"+path)
	if err != nil {
		t.Fatal(err)
	}
	f := newRepairFixtureFor(t, rs, repo)
	t.Setenv("OPENSTORAGE_S3_URL", f.blobURL)
	f.plant(t, map[string]string{"refs/heads/main": f.commit, repairLongRef: f.commit})
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DROP TABLE webhooks`); err != nil {
		t.Fatalf("drop a metadata table: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"--data-dir", dir, repo}, {"--data-dir", dir, "--yes", repo}} {
		var out bytes.Buffer
		if err := runRepairRefs(ctx, args, &out); err != nil {
			t.Fatalf("%v: %v\n%s", args, err, out.String())
		}
		if sqliteTables(t, path)["webhooks"] {
			t.Fatalf("%v: the command ran the metadata schema setup", args)
		}
	}
	if _, ok := f.refs(t)[repairLongRef]; ok {
		t.Fatal("--yes did not delete the ref")
	}
}

// An ID with no repository is refused before the ref store is touched: no
// ref schema is set up for it, not even with --yes.
func TestCmdRepairRefsSetsUpNothingForAnUnknownID(t *testing.T) {
	isolateStorageEnv(t)
	ctx := t.Context()
	dir := t.TempDir()
	path := filepath.Join(dir, "codestorage.db")
	createMetaRepo(t, "sqlite://"+path, "main")
	if sqliteTables(t, path)["refs"] {
		t.Fatal("test setup: the refs table exists")
	}
	if err := cmdRepairRefs(ctx, []string{"--data-dir", dir, "--yes", "repo_000000000000"}); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("--yes with an unknown ID = %v, want not found", err)
	}
	if sqliteTables(t, path)["refs"] {
		t.Fatal("an unknown ID got the ref schema setup")
	}
}

// A database without the metadata schema (not the one that serve uses) is an
// error, not a clean repository.
func TestCmdRepairRefsFailsWithoutTheMetadataSchema(t *testing.T) {
	isolateStorageEnv(t)
	dir := t.TempDir()
	rs, err := ref.OpenSQLite(t.Context(), "sqlite://"+filepath.Join(dir, "codestorage.db"))
	if err != nil {
		t.Fatal(err)
	}
	f := newRepairFixture(t, rs)
	f.plant(t, map[string]string{repairLongRef: f.commit})
	if err := cmdRepairRefs(t.Context(), []string{"--data-dir", dir, "--yes", f.repo}); err == nil || !strings.Contains(err.Error(), "read repo state") {
		t.Fatalf("a database without the metadata schema = %v, want a read error", err)
	}
	if got := f.refs(t); len(got) != 1 {
		t.Fatalf("refs changed: %v", got)
	}
}
