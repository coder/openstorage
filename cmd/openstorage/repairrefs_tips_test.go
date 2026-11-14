package main

// Tests for the branches that repair-refs lists because their tip is not a
// commit. Before the update hook refused such a push, a branch at a blob, a
// tree or an annotated tag reached the ref store, where git refuses to write
// it into a cache repository: every hydration of the repository fails, and
// git reads of it answer 500.

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/githttp"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref"
)

// repairVerifier accepts any token as one that reads the repository.
type repairVerifier struct{ org, repo string }

func (v repairVerifier) Verify(context.Context, string) (*auth.Claims, error) {
	return &auth.Claims{Iss: v.org, Repo: v.repo, Scopes: []string{auth.ScopeGitRead}}, nil
}

// remote serves the repository over smart HTTP, as a node does, with a new
// cache directory, and returns its URL.
func (f *repairFixture) remote(t *testing.T) string {
	t.Helper()
	srv := &githttp.Server{
		Store: f.rs, Blob: f.blobs, CacheDir: t.TempDir(), Org: "repair-org",
		Verifier: repairVerifier{org: "repair-org", repo: f.repo},
		ResolveRepo: func(_ context.Context, base string) (meta.Repo, bool) {
			if base != f.repo {
				return meta.Repo{}, false
			}
			return meta.Repo{RepoID: f.repo, URL: f.repo, DefaultBranch: "main"}, true
		},
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return "http://t:token@" + strings.TrimPrefix(ts.URL, "http://") + "/" + f.repo + ".git"
}

// answers500 fails the test unless an ls-remote and a clone both end in the
// server's 500.
func answers500(t *testing.T, url string) {
	t.Helper()
	for _, args := range [][]string{{"ls-remote", url}, {"clone", url, filepath.Join(t.TempDir(), "clone")}} {
		if _, stderr, err := repairGitRun("", "", args...); err == nil || !strings.Contains(stderr, "error: 500") {
			t.Fatalf("git %s before the repair: err=%v, stderr %q, want the server's 500", args[0], err, stderr)
		}
	}
}

func TestRepairRefsNonCommitTips(t *testing.T) {
	for _, b := range repairBackends() {
		t.Run(b.name, func(t *testing.T) {
			rs, dbURL := b.open(t)
			repo := repairRepo
			if dbURL != "" {
				repo = createMetaRepo(t, dbURL, "main")
			}
			testRepairNonCommitTips(t, newRepairFixtureFor(t, rs, repo), dbURL)
		})
	}
}

func testRepairNonCommitTips(t *testing.T, f *repairFixture, dbURL string) {
	ctx := t.Context()
	// The ref store takes any value, as it did before the update hook
	// refused such a push. A tag may hold a blob.
	f.plant(t, map[string]string{
		"refs/heads/main": f.commit, "refs/heads/blobby": f.blob, "refs/heads/treeish": f.tree, "refs/heads/tagged": f.tag,
		"refs/tags/blobtag": f.blob,
	})
	want := f.refs(t)
	url := f.remote(t)

	if _, err := f.hydrateInto(t, filepath.Join(t.TempDir(), "cache")); err == nil || !strings.Contains(err.Error(), "non-commit") {
		t.Fatalf("hydrate before the repair = %v, want git's refusal of a non-commit branch", err)
	}
	answers500(t, url)

	// The dry run lists the three branches with their types, and changes nothing.
	var out bytes.Buffer
	if err := repairRefs(ctx, f.rs, f.target(), false, &out); err != nil {
		t.Fatalf("dry run: %v", err)
	}
	for _, line := range []string{
		"  " + f.blob + " refs/heads/blobby  (a blob)\n",
		"  " + f.tree + " refs/heads/treeish  (a tree)\n",
		"  " + f.tag + " refs/heads/tagged  (an annotated tag)\n",
		"branches whose tip is not a commit (3):\n", "scanned: 5 refs\n", "nothing changed",
	} {
		if !strings.Contains(out.String(), line) {
			t.Fatalf("dry run output lacks %q:\n%s", line, out.String())
		}
	}
	if strings.Contains(out.String(), "refs/heads/main") || strings.Contains(out.String(), "refs/tags/blobtag") {
		t.Fatalf("dry run lists a ref that git can write:\n%s", out.String())
	}
	if got := f.refs(t); !mapsEqual(got, want) {
		t.Fatalf("dry run changed the refs: %v", got)
	}

	// --yes deletes the three branches, and the packs stay.
	out.Reset()
	if err := repairRefs(ctx, f.rs, f.target(), true, &out); err != nil {
		t.Fatalf("--yes: %v\n%s", err, out.String())
	}
	for _, name := range []string{"blobby", "treeish", "tagged"} {
		if !strings.Contains(out.String(), "deleted: refs/heads/"+name+"\n") {
			t.Fatalf("--yes output lacks the deletion of %s:\n%s", name, out.String())
		}
		delete(want, "refs/heads/"+name)
	}
	if !strings.Contains(out.String(), "deleted 3 of 3 refs") {
		t.Fatalf("--yes output:\n%s", out.String())
	}
	if got := f.refs(t); !mapsEqual(got, want) {
		t.Fatalf("refs after --yes = %v, want %v", got, want)
	}

	// The repository reads again: hydration, ls-remote and a clone.
	if _, err := f.hydrateInto(t, filepath.Join(t.TempDir(), "cache")); err != nil {
		t.Fatalf("hydrate after the repair: %v", err)
	}
	listed, stderr, err := repairGitRun("", "", "ls-remote", url)
	if err != nil || !strings.Contains(listed, f.commit+"\trefs/heads/main\n") || !strings.Contains(listed, f.blob+"\trefs/tags/blobtag\n") {
		t.Fatalf("ls-remote after the repair: %v\n%s\n%s", err, listed, stderr)
	}
	clone := filepath.Join(t.TempDir(), "clone")
	repairGit(t, "", "", "clone", url, clone)
	if got := repairGit(t, clone, "", "rev-parse", "HEAD"); got != f.commit {
		t.Fatalf("the clone is at %s, want %s", got, f.commit)
	}

	// A second run finds nothing.
	out.Reset()
	if err := repairRefs(ctx, f.rs, f.target(), true, &out); err != nil || !strings.Contains(out.String(), "no ref that git can never lock and no branch whose tip is not a commit") {
		t.Fatalf("second run = %v\n%s", err, out.String())
	}

	// A branch that moves between the list and the delete is a lost CAS: it
	// is reported, kept, and not retried, and the others are still deleted.
	f.plant(t, map[string]string{"refs/heads/blobby": f.blob, "refs/heads/treeish": f.tree})
	out.Reset()
	moving := &movingStore{RefStore: f.rs, t: t, name: "refs/heads/blobby", from: f.blob, to: f.commit}
	err = repairRefs(ctx, moving, f.target(), true, &out)
	if err == nil || !strings.Contains(err.Error(), "changed since they were listed") ||
		!strings.Contains(out.String(), "lost CAS, not deleted: refs/heads/blobby") ||
		!strings.Contains(out.String(), "deleted: refs/heads/treeish\n") || !strings.Contains(out.String(), "deleted 1 of 2 refs") {
		t.Fatalf("lost CAS = %v\n%s", err, out.String())
	}
	want["refs/heads/blobby"] = f.commit
	if got := f.refs(t); !mapsEqual(got, want) {
		t.Fatalf("refs after a lost CAS = %v, want %v", got, want)
	}

	// A branch whose tip object the packs do not hold is reported and kept,
	// and git still cannot hydrate it.
	ghost := strings.Repeat("ab", 20)
	f.plant(t, map[string]string{"refs/heads/ghost": ghost})
	out.Reset()
	if err := repairRefs(ctx, f.rs, f.target(), false, &out); err != nil || !strings.Contains(out.String(), "never deleted:\n  "+ghost+" refs/heads/ghost\n") {
		t.Fatalf("dry run with a missing tip = %v\n%s", err, out.String())
	}
	out.Reset()
	err = repairRefs(ctx, f.rs, f.target(), true, &out)
	if err == nil || !strings.Contains(err.Error(), "branches with a missing tip object are kept: 1") ||
		!strings.Contains(out.String(), "branches whose tip object is missing (1), never deleted:\n  "+ghost+" refs/heads/ghost\n") {
		t.Fatalf("a missing tip = %v\n%s", err, out.String())
	}
	if got := f.refs(t)["refs/heads/ghost"]; got != ghost {
		t.Fatalf("the missing tip was changed to %q", got)
	}
	if _, err := f.hydrateInto(t, filepath.Join(t.TempDir(), "cache")); err == nil {
		t.Fatal("hydration works with a branch at a missing object: the report is not needed")
	}
	if won, err := f.rs.CASRef(ctx, f.repo, "refs/heads/ghost", ghost, ref.ZeroSHA); err != nil || !won {
		t.Fatalf("remove the missing tip: won=%v err=%v", won, err)
	}

	// The command line opens both stores from the environment, as an
	// operator runs it.
	if dbURL == "" {
		return
	}
	t.Setenv("OPENSTORAGE_PG_URL", dbURL)
	t.Setenv("OPENSTORAGE_S3_URL", f.blobURL)
	f.plant(t, map[string]string{"refs/heads/treeish": f.tree})
	if err := cmdRepairRefs(ctx, []string{"--yes", f.repo}); err != nil {
		t.Fatalf("command --yes: %v", err)
	}
	if got := f.refs(t); !mapsEqual(got, want) {
		t.Fatalf("refs after the command's --yes = %v, want %v", got, want)
	}
}

// A name that git can never lock and a branch at a blob are both listed and
// both deleted in one run.
func TestRepairRefsListsBothKinds(t *testing.T) {
	rs, err := ref.OpenSQLite(t.Context(), "sqlite://"+filepath.Join(t.TempDir(), "refs.db"))
	if err != nil {
		t.Fatal(err)
	}
	f := newRepairFixture(t, rs)
	f.plant(t, map[string]string{"refs/heads/main": f.commit, "refs/heads/blobby": f.blob, repairLongRef: f.commit})
	var out bytes.Buffer
	if err := repairRefs(t.Context(), rs, f.target(), true, &out); err != nil || !strings.Contains(out.String(), "deleted 2 of 2 refs") ||
		!strings.Contains(out.String(), "refs that git can never lock (1):") || !strings.Contains(out.String(), "branches whose tip is not a commit (1):") {
		t.Fatalf("--yes = %v\n%s", err, out.String())
	}
	if got := f.refs(t); len(got) != 1 || got["refs/heads/main"] != f.commit {
		t.Fatalf("refs after --yes = %v", got)
	}
}

// The default branch is kept, at a blob too: the repository would be left
// with a default branch that does not exist.
func TestRepairRefsKeepsTheDefaultBranchAtANonCommit(t *testing.T) {
	rs, err := ref.OpenSQLite(t.Context(), "sqlite://"+filepath.Join(t.TempDir(), "refs.db"))
	if err != nil {
		t.Fatal(err)
	}
	f := newRepairFixture(t, rs)
	f.plant(t, map[string]string{"refs/heads/main": f.blob, "refs/heads/other": f.tree})
	var out bytes.Buffer
	if err := repairRefs(t.Context(), rs, f.target(), false, &out); err != nil || !strings.Contains(out.String(), "refs/heads/main  (a blob, the default branch: not deleted)\n") {
		t.Fatalf("dry run = %v\n%s", err, out.String())
	}
	out.Reset()
	err = repairRefs(t.Context(), rs, f.target(), true, &out)
	if err == nil || !strings.Contains(err.Error(), "1 are the default branch") || !strings.Contains(out.String(), "kept: refs/heads/main is the default branch") ||
		!strings.Contains(out.String(), "deleted: refs/heads/other\n") {
		t.Fatalf("--yes = %v\n%s", err, out.String())
	}
	if got := f.refs(t); len(got) != 1 || got["refs/heads/main"] != f.blob {
		t.Fatalf("refs after --yes = %v", got)
	}
}

// A type that the command cannot read, or does not know, deletes nothing.
func TestRepairRefsFailsClosedOnObjectTypes(t *testing.T) {
	rs, err := ref.OpenSQLite(t.Context(), "sqlite://"+filepath.Join(t.TempDir(), "refs.db"))
	if err != nil {
		t.Fatal(err)
	}
	f := newRepairFixture(t, rs)
	f.plant(t, map[string]string{"refs/heads/main": f.commit, "refs/heads/blobby": f.blob})
	for name, kinds := range map[string]objectKinds{
		"git fails": func(context.Context, []string) (map[string]string, error) { return nil, errors.New("git is down") },
		"unknown type": func(_ context.Context, ids []string) (map[string]string, error) {
			return map[string]string{ids[0]: "bogus", ids[1]: "bogus"}, nil
		},
		"an id is left": func(_ context.Context, ids []string) (map[string]string, error) {
			return map[string]string{ids[0]: "commit"}, nil
		},
	} {
		target := f.target()
		target.kinds = kinds
		var out bytes.Buffer
		if err := repairRefs(t.Context(), rs, target, true, &out); err == nil || strings.Contains(out.String(), "deleted") {
			t.Fatalf("%s: err=%v\n%s", name, err, out.String())
		}
		if got := f.refs(t); len(got) != 2 {
			t.Fatalf("%s: refs changed: %v", name, got)
		}
	}
}

// A repository with branches needs the blob store to read their tips, and one
// with no branch does not: the command still repairs it from the database URL
// alone.
func TestCmdRepairRefsNeedsTheBlobStoreOnlyForBranches(t *testing.T) {
	isolateStorageEnv(t)
	ctx := t.Context()
	dir := t.TempDir()
	dbURL := "sqlite://" + filepath.Join(dir, "codestorage.db")
	repo := createMetaRepo(t, dbURL, "main")
	rs, err := ref.OpenSQLite(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	f := newRepairFixtureFor(t, rs, repo)
	t.Setenv("OPENSTORAGE_PG_URL", dbURL)
	f.plant(t, map[string]string{repairLongRef: f.commit, "refs/heads/main": f.commit})
	if err := cmdRepairRefs(ctx, []string{"--yes", repo}); err == nil || !strings.Contains(err.Error(), "blob URL required") {
		t.Fatalf("a branch and no blob URL = %v, want the missing URL error", err)
	}
	if got := f.refs(t); len(got) != 2 {
		t.Fatalf("refs changed: %v", got)
	}
	if won, err := rs.CASRef(ctx, repo, "refs/heads/main", f.commit, ref.ZeroSHA); err != nil || !won {
		t.Fatalf("remove the branch: won=%v err=%v", won, err)
	}
	if err := cmdRepairRefs(ctx, []string{"--yes", repo}); err != nil {
		t.Fatalf("no branch and no blob URL = %v", err)
	}
	if got := f.refs(t); len(got) != 0 {
		t.Fatalf("refs after --yes = %v", got)
	}
}

// The temporary cache repository that reads the tips goes away with the
// command, with its packs.
func TestPackTipKindsRemovesItsTemporaryDirectory(t *testing.T) {
	rs, err := ref.OpenSQLite(t.Context(), "sqlite://"+filepath.Join(t.TempDir(), "refs.db"))
	if err != nil {
		t.Fatal(err)
	}
	f := newRepairFixture(t, rs)
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	tips := &packTipKinds{rs: rs, repoID: f.repo, blobURL: f.blobURL}
	got, err := tips.kinds(t.Context(), []string{f.commit, f.blob, f.tree, f.tag, strings.Repeat("cd", 20)})
	if err != nil || got[f.commit] != "commit" || got[f.blob] != "blob" || got[f.tree] != "tree" || got[f.tag] != "tag" || got[strings.Repeat("cd", 20)] != "missing" {
		t.Fatalf("kinds = %v, %v", got, err)
	}
	if _, err := tips.kinds(t.Context(), []string{f.commit}); err != nil {
		t.Fatal(err)
	}
	if entries, err := os.ReadDir(tmp); err != nil || len(entries) != 1 {
		t.Fatalf("temporary directory while the reader is open and read twice: %v, %v", entries, err)
	}
	tips.close()
	if entries, err := os.ReadDir(tmp); err != nil || len(entries) != 0 {
		t.Fatalf("temporary directory after close: %v, %v", entries, err)
	}
}

// What git answers is read as it is: a count, an id or a type that does not
// fit the question is an error, never a guess.
func TestReadObjectKindsFailsClosed(t *testing.T) {
	a, b := strings.Repeat("a", 40), strings.Repeat("b", 40)
	fakeGit := func(t *testing.T, answer string) {
		t.Helper()
		bin := t.TempDir()
		script := "#!/bin/sh\ncat >/dev/null\n" + answer + "\n"
		if err := os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
		t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	}
	// The control: the fake git is the one that runs, and a good answer reads.
	t.Run("a good answer", func(t *testing.T) {
		fakeGit(t, "echo "+a+" commit; echo "+b+" missing")
		if got, err := readObjectKinds(t.Context(), t.TempDir(), []string{a, b}); err != nil || got[a] != "commit" || got[b] != "missing" {
			t.Fatalf("kinds = %v, %v", got, err)
		}
	})
	for name, answer := range map[string]string{
		"git fails":      "exit 1",
		"too few lines":  "echo " + a + " commit",
		"too many lines": "echo " + a + " commit; echo " + b + " blob; echo " + b + " blob",
		"another id":     "echo " + a + " commit; echo " + a + " blob",
		"unknown type":   "echo " + a + " commit; echo " + b + " bogus",
		"no type":        "echo " + a + " commit; echo " + b,
		"no answer":      "true",
	} {
		t.Run(name, func(t *testing.T) {
			fakeGit(t, answer)
			if got, err := readObjectKinds(t.Context(), t.TempDir(), []string{a, b}); err == nil {
				t.Fatalf("kinds = %v, want an error", got)
			}
		})
	}
	if _, err := readObjectKinds(t.Context(), t.TempDir(), []string{"HEAD"}); err == nil || !strings.Contains(err.Error(), "not a full SHA") {
		t.Fatalf("a revision instead of a SHA = %v, want a refusal", err)
	}
}

// A run leaves no temporary directory: not when it ends well, not on an
// error, not when the context ends (SIGINT and SIGTERM end it through
// signal.NotifyContext), with the packs half downloaded or not.
func TestRepairRefsRemovesTheTemporaryDirectoryWhenItEnds(t *testing.T) {
	isolateStorageEnv(t)
	dir := t.TempDir()
	dbURL := "sqlite://" + filepath.Join(dir, "codestorage.db")
	repo := createMetaRepo(t, dbURL, "main")
	rs, err := ref.OpenSQLite(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	f := newRepairFixtureFor(t, rs, repo)
	f.plant(t, map[string]string{"refs/heads/main": f.commit})
	t.Setenv("OPENSTORAGE_S3_URL", f.blobURL)
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)

	// A run that ends well.
	var out bytes.Buffer
	if err := runRepairRefs(t.Context(), []string{"--data-dir", dir, repo}, &out); err != nil || !strings.Contains(out.String(), "no ref that git can never lock") {
		t.Fatalf("run = %v\n%s", err, out.String())
	}
	if entries, err := os.ReadDir(tmp); err != nil || len(entries) != 0 {
		t.Fatalf("temporary directory after a run: %v, %v", entries, err)
	}

	// A blob URL that cannot be opened is an error, not a panic.
	for _, url := range []string{"s3://", "file:///nonexistent/dir/of/no/store"} {
		bad := &packTipKinds{rs: rs, repoID: repo, blobURL: url}
		if _, err := bad.kinds(t.Context(), []string{f.commit}); err == nil || !strings.Contains(err.Error(), "open blob store") {
			t.Fatalf("blob URL %q: kinds = %v, want an open error", url, err)
		}
		bad.close()
	}

	// An error while the packs are staged.
	tips := &packTipKinds{rs: rs, repoID: repo, blobURL: "file://" + filepath.Join(t.TempDir(), "no", "such", "store") + ""}
	if _, err := tips.kinds(t.Context(), []string{f.commit}); err == nil {
		t.Fatal("the packs of a store that does not exist were read")
	}
	if entries, err := os.ReadDir(tmp); err != nil || len(entries) != 0 {
		t.Fatalf("temporary directory after an error: %v, %v", entries, err)
	}

	// The run ends while git initializes the temporary repository, by a
	// canceled context or by a signal. The fake git parks there, and passes
	// every other call to the real one.
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	bin, parked := t.TempDir(), filepath.Join(t.TempDir(), "parked")
	script := "#!/bin/sh\nif [ \"$1\" = init ]; then touch '" + parked + "'; exec sleep 60; fi\nexec '" + realGit + "' \"$@\"\n"
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	args := []string{"--data-dir", dir, repo}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	for name, run := range map[string]struct {
		start func() error
		end   func()
	}{
		"canceled context": {func() error { return runRepairRefs(ctx, args, &bytes.Buffer{}) }, cancel},
		"SIGINT":           {func() error { return cmdRepairRefs(t.Context(), args) }, func() { _ = syscall.Kill(os.Getpid(), syscall.SIGINT) }},
		"SIGTERM":          {func() error { return cmdRepairRefs(t.Context(), args) }, func() { _ = syscall.Kill(os.Getpid(), syscall.SIGTERM) }},
		"SIGHUP":           {func() error { return cmdRepairRefs(t.Context(), args) }, func() { _ = syscall.Kill(os.Getpid(), syscall.SIGHUP) }},
	} {
		t.Run(name, func(t *testing.T) {
			_ = os.Remove(parked)
			done := make(chan error, 1)
			go func() { done <- run.start() }()
			deadline := time.Now().Add(30 * time.Second)
			for {
				if _, err := os.Stat(parked); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("the command never reached git init")
				}
				time.Sleep(10 * time.Millisecond)
			}
			if entries, err := os.ReadDir(tmp); err != nil || len(entries) != 1 {
				t.Fatalf("temporary directory while git runs: %v, %v", entries, err)
			}
			run.end()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) || !strings.HasSuffix(err.Error(), "context canceled") {
					t.Fatalf("the interrupted command returned %v, want the canceled context and not the text of a killed git", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("the interrupted command did not stop")
			}
			if entries, err := os.ReadDir(tmp); err != nil || len(entries) != 0 {
				t.Fatalf("temporary directory after the interrupt: %v, %v", entries, err)
			}
		})
	}
}
