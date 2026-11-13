package main

// Tests for repair-refs: on every ref store, a repository that holds a ref
// git can never lock fails hydration, the dry run lists only that ref, --yes
// deletes only it, and hydration then works.

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/githttp"
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

func repairGit(t *testing.T, dir, stdin string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = []string{
		"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null", "GIT_CONFIG_NOSYSTEM=1",
		"GIT_AUTHOR_NAME=repair-test", "GIT_AUTHOR_EMAIL=repair@test.invalid",
		"GIT_COMMITTER_NAME=repair-test", "GIT_COMMITTER_EMAIL=repair@test.invalid",
		"GIT_AUTHOR_DATE=@1700000000 +0000", "GIT_COMMITTER_DATE=@1700000000 +0000",
		"HOME=/dev/null", "PATH=" + os.Getenv("PATH"),
	}
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, stderr.String())
	}
	return strings.TrimSpace(string(out))
}

// repairFixture is one repository in a ref store: a commit, its pack in a
// file blob store, and the refs that a test plants.
type repairFixture struct {
	rs     ref.RefStore
	blobs  blob.BlobStore
	commit string
	other  string // another valid SHA to move a ref to
}

func newRepairFixture(t *testing.T, rs ref.Store) *repairFixture {
	t.Helper()
	ctx := t.Context()
	t.Cleanup(rs.Close)
	if err := rs.InitSchema(ctx); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	dir := t.TempDir()
	bl, err := blob.Open(ctx, "file://"+filepath.Join(dir, "blobs"))
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
	packDir := t.TempDir()
	objs := strings.Join([]string{blobSHA, treeSHA, commit, other}, "\n") + "\n"
	id := repairGit(t, scratch, objs, "pack-objects", "--threads=1", "-q", filepath.Join(packDir, "p"))
	packPath := filepath.Join(packDir, "p-"+id+".pack")
	key := blob.PackKey(repairRepo, id)
	if err := bl.UploadFile(ctx, key, packPath); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(packPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := rs.RegisterPacks(ctx, repairRepo, []ref.Pack{{ID: id, Key: key, Size: fi.Size()}}); err != nil {
		t.Fatalf("RegisterPacks: %v", err)
	}
	return &repairFixture{rs: rs, blobs: bl, commit: commit, other: other}
}

// plant writes refs into the store the way an old API node did: the raw
// store takes any name.
func (f *repairFixture) plant(t *testing.T, refs map[string]string) {
	t.Helper()
	for name, sha := range refs {
		won, err := f.rs.CASRef(t.Context(), repairRepo, name, ref.ZeroSHA, sha)
		if err != nil || !won {
			t.Fatalf("plant %.40s: won=%v err=%v", name, won, err)
		}
	}
}

// hydrateInto hydrates the repository into a cache directory. A fresh one
// models another replica, and reuse models the same replica.
func (f *repairFixture) hydrateInto(t *testing.T, cacheDir string) (repoPath string, hydrateErr error) {
	t.Helper()
	repoPath, refsErr, err := githttp.Hydrate(t.Context(), f.rs, f.blobs, cacheDir, repairRepo, "main")
	return repoPath, errors.Join(refsErr, err)
}

func (f *repairFixture) refs(t *testing.T) map[string]string {
	t.Helper()
	got, err := f.rs.ListRefs(t.Context(), repairRepo)
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
			testRepairRefs(t, newRepairFixture(t, rs), dbURL)
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
	if err := repairRefs(ctx, f.rs, repairRepo, false, &out); err != nil {
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
	err := repairRefs(ctx, moving, repairRepo, true, &out)
	if err == nil || !strings.Contains(err.Error(), "changed since they were listed") ||
		!strings.Contains(out.String(), "lost CAS") || !strings.Contains(out.String(), "deleted 0 of 1 refs") {
		t.Fatalf("lost CAS = %v\n%s", err, out.String())
	}
	if got := f.refs(t)[repairLongRef]; got != f.other {
		t.Fatalf("the moved ref holds %q after the lost CAS, want %q", got, f.other)
	}

	// --yes deletes only the ref that git cannot lock.
	out.Reset()
	if err := repairRefs(ctx, f.rs, repairRepo, true, &out); err != nil {
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
	if err := repairRefs(ctx, f.rs, repairRepo, true, &out); err != nil || !strings.Contains(out.String(), "no ref that git can never lock") {
		t.Fatalf("second run = %v\n%s", err, out.String())
	}

	// The command line opens the store from the database URL, as an
	// operator runs it, and repairs the same way.
	if dbURL == "" {
		return
	}
	t.Setenv("OPENSTORAGE_PG_URL", dbURL)
	f.plant(t, map[string]string{repairLongRef: f.commit})
	want[repairLongRef] = f.commit
	if err := cmdRepairRefs(ctx, []string{repairRepo}); err != nil {
		t.Fatalf("command dry run: %v", err)
	}
	if got := f.refs(t); !mapsEqual(got, want) {
		t.Fatalf("command dry run changed the refs: %v", got)
	}
	if err := cmdRepairRefs(ctx, []string{"--yes", repairRepo}); err != nil {
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
	long2 := "refs/z/" + strings.Repeat("z", 251)
	f.plant(t, map[string]string{
		"refs/heads/a": f.commit, "refs/heads/b": f.commit, repairLongRef: f.commit,
		"refs/tags/t": f.commit, long2: f.commit,
	})
	for _, page := range []int{1, 2, 3, 5, 6, 100} {
		found, scanned, err := scanUnlockableRefs(t.Context(), rs, repairRepo, page)
		if err != nil || scanned != 5 || len(found) != 2 || found[0].name != repairLongRef || found[1].name != long2 {
			t.Errorf("page %d: found %d, scanned %d, err %v", page, len(found), scanned, err)
		}
	}
}

// A ref that git can lock is never deleted, whatever listed it.
func TestRepairRefsRefusesLockableRef(t *testing.T) {
	rs, err := ref.OpenSQLite(t.Context(), "sqlite://"+filepath.Join(t.TempDir(), "refs.db"))
	if err != nil {
		t.Fatal(err)
	}
	f := newRepairFixture(t, rs)
	f.plant(t, map[string]string{"refs/heads/main": f.commit, repairOKRef: f.commit})
	for _, name := range []string{"refs/heads/main", repairOKRef} {
		won, err := deleteUnlockableRef(t.Context(), rs, repairRepo, repairRef{name: name, sha: f.commit})
		if won || err == nil || !strings.Contains(err.Error(), "refusing") {
			t.Errorf("delete %.40s: won=%v err=%v, want a refusal", name, won, err)
		}
	}
	if got := f.refs(t); len(got) != 2 {
		t.Fatalf("a refusal changed the refs: %v", got)
	}
}

// An ID that matches no refs (a typo, or the colon that follows the ID in
// the serve log) is not reported as a clean repository.
func TestRepairRefsWarnsWhenNoRefsMatchTheID(t *testing.T) {
	rs, err := ref.OpenSQLite(t.Context(), "sqlite://"+filepath.Join(t.TempDir(), "refs.db"))
	if err != nil {
		t.Fatal(err)
	}
	f := newRepairFixture(t, rs)
	f.plant(t, map[string]string{"refs/heads/main": f.commit, repairLongRef: f.commit})
	for _, yes := range []bool{false, true} {
		var out bytes.Buffer
		if err := repairRefs(t.Context(), rs, repairRepo+":", yes, &out); err != nil {
			t.Fatalf("yes=%v: %v", yes, err)
		}
		if !strings.Contains(out.String(), "scanned: 0 refs") || !strings.Contains(out.String(), "no refs under this ID") ||
			strings.Contains(out.String(), "no ref that git can never lock") {
			t.Fatalf("yes=%v: output:\n%s", yes, out.String())
		}
	}
	if got := f.refs(t); len(got) != 2 {
		t.Fatalf("refs changed: %v", got)
	}
}

// A ref name with control bytes cannot forge a line of the listing.
func TestRepairRefsSanitizesNames(t *testing.T) {
	rs, err := ref.OpenSQLite(t.Context(), "sqlite://"+filepath.Join(t.TempDir(), "refs.db"))
	if err != nil {
		t.Fatal(err)
	}
	f := newRepairFixture(t, rs)
	name := "refs/notes/x\n  deleted: forged\x1b[2J/" + strings.Repeat("n", 251)
	f.plant(t, map[string]string{name: f.commit})
	var out bytes.Buffer
	if err := repairRefs(t.Context(), rs, repairRepo, false, &out); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "\x1b") || strings.Contains(out.String(), "\n  deleted: forged") {
		t.Fatalf("a ref name forged output:\n%q", out.String())
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
	rs, err := ref.OpenSQLite(ctx, "sqlite://"+filepath.Join(dir, "codestorage.db"))
	if err != nil {
		t.Fatal(err)
	}
	f := newRepairFixture(t, rs)
	f.plant(t, map[string]string{"refs/heads/main": f.commit, repairOKRef: f.commit, repairLongRef: f.commit})
	want := f.refs(t)

	if err := cmdRepairRefs(ctx, []string{"--data-dir", dir}); err == nil || !strings.Contains(err.Error(), "exactly one <repo-id>") {
		t.Fatalf("no arg = %v, want usage error", err)
	}
	if err := cmdRepairRefs(ctx, []string{"--data-dir", dir, repairRepo, "extra"}); err == nil || !strings.Contains(err.Error(), "exactly one <repo-id>") {
		t.Fatalf("two args = %v, want usage error", err)
	}
	if err := cmdRepairRefs(ctx, []string{repairRepo}); err == nil || !strings.Contains(err.Error(), "database URL required") {
		t.Fatalf("no database = %v, want the missing URL error", err)
	}

	// Without --yes it changes nothing.
	if err := cmdRepairRefs(ctx, []string{"--data-dir", dir, repairRepo}); err != nil {
		t.Fatalf("dry run = %v", err)
	}
	if got := f.refs(t); !mapsEqual(got, want) {
		t.Fatalf("dry run changed the refs: %v", got)
	}
	if err := cmdRepairRefs(ctx, []string{"--data-dir", dir, "--yes", repairRepo}); err != nil {
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
	rs, err := ref.OpenSQLite(ctx, "sqlite://"+filepath.Join(dir, "codestorage.db"))
	if err != nil {
		t.Fatal(err)
	}
	rs.Close()
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
		t.Fatal("a new database already has the refs table")
	}
	_ = cmdRepairRefs(ctx, []string{"--data-dir", dir, repairRepo})
	if hasRefsTable() {
		t.Fatal("the list ran the schema setup")
	}
	if err := cmdRepairRefs(ctx, []string{"--data-dir", dir, "--yes", repairRepo}); err != nil {
		t.Fatalf("--yes on a new database = %v", err)
	}
	if !hasRefsTable() {
		t.Fatal("--yes did not run the schema setup")
	}
}
