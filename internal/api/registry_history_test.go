package api

// A ref write must not store a commit whose history the registered packs
// cannot walk. receive-pack checks the commits that a push's refs reach and no
// other commit of the pack, so a push can leave a commit in a registered pack
// whose parent, tree or blob no pack holds. A ref at it hydrates, and every
// clone, fetch and archive that walks the history fails (#896 held the commit
// itself and left this "closure variant" open).

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/gitlimit"
	"github.com/coder/openstorage/internal/gitread"
	"github.com/coder/openstorage/internal/ref"
)

const absentID = "1111111111111111111111111111111111111111"

// refusedWalkLine is the log line of a ref write that names a commit whose
// history the registered packs cannot walk. The operator's only sign of such a
// request: the response is the same 404 as for a commit that does not exist.
const refusedWalkLine = "a ref write names a commit that the registered packs cannot walk"

// logLines returns the lines of the log that hold every one of the substrings.
func logLines(l *lockedLog, substrings ...string) []string {
	var out []string
next:
	for _, line := range strings.Split(l.String(), "\n") {
		for _, sub := range substrings {
			if !strings.Contains(line, sub) {
				continue next
			}
		}
		out = append(out, line)
	}
	return out
}

// scratchGit runs git in dir with stdin and no GIT_ variable of the host.
func scratchGit(t *testing.T, dir, stdin string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "GIT_") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
	cmd.Stdin = strings.NewReader(stdin)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git %v: %v", args, err)
	}
	return strings.TrimSpace(string(out))
}

// commitText is the raw text of a commit with the given tree and parents.
func commitText(tree string, parents ...string) string {
	var b strings.Builder
	b.WriteString("tree " + tree + "\n")
	for _, p := range parents {
		b.WriteString("parent " + p + "\n")
	}
	b.WriteString("author a <a@b.invalid> 1 +0000\ncommitter a <a@b.invalid> 1 +0000\n\nplanted\n")
	return b.String()
}

// registerLooseObjects packs exactly the listed objects of dir (no walk), uploads
// the pack and registers it for the repository, as a push that carries objects
// that its refs do not reach would.
func registerLooseObjects(t *testing.T, e *testEnv, repoID, dir string, ids ...string) {
	t.Helper()
	ctx := context.Background()
	packBase := filepath.Join(t.TempDir(), "p")
	id := scratchGit(t, dir, strings.Join(ids, "\n")+"\n", "pack-objects", "--threads=1", "-q", packBase)
	key := blob.PackKey(repoID, id)
	if err := e.blobStore.UploadFile(ctx, key, packBase+"-"+id+".pack"); err != nil {
		t.Fatalf("upload pack %s: %v", key, err)
	}
	if err := e.refStore.RegisterPacks(ctx, repoID, []ref.Pack{{ID: id, Key: key}}); err != nil {
		t.Fatalf("register pack %s: %v", id, err)
	}
}

// plantHistory registers a pack with a commit, and returns the commit, for each
// way that the pack can lack part of what the commit names. c2's tree and
// commits are in the repository's first pack.
func plantHistory(t *testing.T, e *testEnv, fx wrefFixture, shape string) string {
	t.Helper()
	dir := t.TempDir()
	scratchGit(t, dir, "", "init", "-q", "-b", "main", ".")
	hash := func(kind, text string) string {
		return scratchGit(t, dir, text, "hash-object", "-t", kind, "-w", "--stdin", "--literally")
	}
	tree := func(entries string) string {
		return scratchGit(t, dir, entries, "mktree", "--missing")
	}
	switch shape {
	case "parent":
		// The tree is the empty tree: it is the commit's own parent that is absent.
		empty := tree("")
		c := hash("commit", commitText(empty, absentID))
		registerLooseObjects(t, e, fx.repoID, dir, c, empty)
		return c
	case "grandparent":
		empty := tree("")
		p := hash("commit", commitText(empty, absentID))
		c := hash("commit", commitText(empty, p))
		registerLooseObjects(t, e, fx.repoID, dir, c, p, empty)
		return c
	case "tree":
		c := hash("commit", commitText(absentID, fx.c2))
		registerLooseObjects(t, e, fx.repoID, dir, c)
		return c
	case "blob":
		tr := tree("100644 blob " + absentID + "\tf.txt\n")
		c := hash("commit", commitText(tr, fx.c2))
		registerLooseObjects(t, e, fx.repoID, dir, c, tr)
		return c
	case "complete":
		// A commit that no ref reaches, with everything that it names in the pack.
		blobID := hash("blob", "complete\n")
		tr := tree("100644 blob " + blobID + "\tf.txt\n")
		c := hash("commit", commitText(tr, fx.c2))
		registerLooseObjects(t, e, fx.repoID, dir, c, tr, blobID)
		return c
	}
	t.Fatalf("unknown shape %q", shape)
	return ""
}

func TestRefWritesRefuseACommitWhoseHistoryThePacksLack(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		for _, history := range []string{"parent", "grandparent", "tree", "blob"} {
			for _, tc := range refWriteShapes() {
				t.Run(history+"/"+tc.name, func(t *testing.T) {
					fx := wrefSeedFixture(t, e)
					planted := plantHistory(t, e, fx, history)
					logs := &lockedLog{}
					srv := newWrefServerLogging(t, e, slog.New(slog.NewTextHandler(logs, nil)))
					before := wrefRefs(t, e, fx.repoID)

					resp, body := wrefDo(t, srv, http.MethodPost, tc.path, fx.token, tc.body(planted))
					if resp.StatusCode != http.StatusNotFound {
						t.Fatalf("a commit whose %s the packs lack: status %d, want 404, body %v", history, resp.StatusCode, body)
					}
					// The operator hears of the refusal: one line per refused commit, with the repository and the commit.
					if got := logLines(logs, refusedWalkLine, "level=WARN", "repo="+fx.repoID, "sha="+planted); len(got) == 0 {
						t.Fatalf("no %q line for %s in the log:\n%s", refusedWalkLine, planted, logs.String())
					}
					after := wrefRefs(t, e, fx.repoID)
					if len(after) != len(before) || after[tc.ref] != before[tc.ref] {
						t.Fatalf("refs changed after the refused request: before %v, after %v", before, after)
					}

					// A commit that the packs hold completely still works.
					refused := len(logLines(logs, refusedWalkLine))
					resp, body = wrefDo(t, srv, http.MethodPost, tc.path, fx.token, tc.body(fx.c1))
					if resp.StatusCode != http.StatusOK {
						t.Fatalf("a commit that the packs hold: status %d, want 200, body %v", resp.StatusCode, body)
					}
					if got := len(logLines(logs, refusedWalkLine)); got != refused {
						t.Fatalf("a commit that the packs hold added %d %q lines", got-refused, refusedWalkLine)
					}
				})
			}
		}
	})
}

// A commit that no ref reaches is no reason for a refusal when the packs hold
// everything that it names, and a commit that a ref reaches costs no walk.
func TestRefWritesAcceptACommitWhoseHistoryThePacksHold(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := wrefSeedFixture(t, e)
		complete := plantHistory(t, e, fx, "complete")
		srv := newWrefServer(t, e)
		for i, tc := range []struct{ name, sha string }{{"unreferenced and complete", complete}, {"the root", fx.c1}, {"the tip", fx.c2}} {
			t.Run(tc.name, func(t *testing.T) {
				resp, body := wrefDo(t, srv, http.MethodPost, "/api/v1/repos/branches/create", fx.token,
					map[string]any{"target_branch": "ok" + strconv.Itoa(i), "base_ref": tc.sha})
				if resp.StatusCode != http.StatusOK {
					t.Fatalf("status %d, want 200, body %v", resp.StatusCode, body)
				}
				if body["commit_sha"] != tc.sha {
					t.Fatalf("body %v, want commit_sha %s", body, tc.sha)
				}
			})
		}
		// Every other node hydrates the repository and reads the history.
		repoPath, err := gitread.Materialize(context.Background(), e.refStore, e.blobStore, t.TempDir(), fx.repoID, "main")
		if err != nil {
			t.Fatal(err)
		}
		scratchGit(t, repoPath, "", "rev-list", "--objects", "--all")
	})
}

// The walk is git work that a writer starts, so it runs under the admission
// slots, the time budget and the heap cap of the git:read routes. With every
// slot of the repository taken, a create answers 503 with Retry-After and
// writes nothing, and it frees its own slot when it is done.
func TestRefWritesWalkUnderTheGitAdmissionSlots(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		for _, tc := range refWriteShapes() {
			t.Run(tc.name, func(t *testing.T) {
				fx := wrefSeedFixture(t, e)
				p := withGitReadPolicy(t, testGitReadPolicy(1, time.Minute, gitread.DefaultOutputBytes, gitread.DefaultLimits()))
				srv := newWrefServer(t, e)
				before := wrefRefs(t, e, fx.repoID)

				// c2 is the tip of main: a base that every shape has to walk to.
				key := gitlimit.RepoKey(fx.repoID)
				if err := p.slots.Acquire(t.Context(), key); err != nil {
					t.Fatal(err)
				}
				resp, body := wrefDo(t, srv, http.MethodPost, tc.path, fx.token, tc.body(fx.c2))
				if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
					t.Fatalf("with no free slot: status %d, Retry-After %q, body %v, want 503 with Retry-After", resp.StatusCode, resp.Header.Get("Retry-After"), body)
				}
				if after := wrefRefs(t, e, fx.repoID); len(after) != len(before) || after[tc.ref] != before[tc.ref] {
					t.Fatalf("refs changed while no slot was free: before %v, after %v", before, after)
				}
				p.slots.Release(key)

				resp, body = wrefDo(t, srv, http.MethodPost, tc.path, fx.token, tc.body(fx.c2))
				if resp.StatusCode != http.StatusOK {
					t.Fatalf("with a free slot: status %d, body %v, want 200", resp.StatusCode, body)
				}
				requireSlotsFree(t, p)
			})
		}
	})
}
