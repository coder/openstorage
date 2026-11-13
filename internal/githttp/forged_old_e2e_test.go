package githttp

// A git client names the old value of every ref it updates, and git hands
// that name to the hooks before it compares it with the stored ref. Under a
// no-force-push policy the hooks ask git whether old is an ancestor of new.
// An old or new id that names no commit must end in a refusal for that ref,
// never in a Go panic whose stack git relays to the pusher.

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/ref"
)

const forgedOldID = "1111111111111111111111111111111111111111"

// rawPushCommand is one update command of a hand-built receive-pack
// request.
type rawPushCommand struct{ old, new, ref string }

// packObjects returns the pack that `git pack-objects --revs` writes for
// the revision lines, run in dir.
func packObjects(t *testing.T, dir string, revs ...string) []byte {
	t.Helper()
	cmd := exec.Command("git", "pack-objects", "--revs", "--stdout", "-q")
	cmd.Dir = dir
	cmd.Stdin = strings.NewReader(strings.Join(revs, "\n") + "\n")
	var out, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("git pack-objects %q: %v\n%s", revs, err, stderr.String())
	}
	return out.Bytes()
}

// pktPayloads splits a pkt-line stream into its payloads (flush packets
// dropped).
func pktPayloads(t *testing.T, s string) []string {
	t.Helper()
	var out []string
	for len(s) > 0 {
		if len(s) < 4 {
			t.Fatalf("truncated pkt-line header %q", s)
		}
		n, err := strconv.ParseUint(s[:4], 16, 16)
		if err != nil || int(n) > len(s) || (n > 0 && n < 4) {
			t.Fatalf("bad pkt-line header %q (%d bytes left)", s[:4], len(s))
		}
		if n == 0 {
			s = s[4:]
			continue
		}
		out = append(out, s[4:n])
		s = s[n:]
	}
	return out
}

// rawReceivePack posts one hand-built receive-pack request with
// report-status and side-band-64k. It returns the report-status line of
// each ref ("ok" or "ng <reason>") and the side-band text git relays to the
// pusher as "remote:" lines.
func rawReceivePack(t *testing.T, n *testNode, repo, token string, atomic bool, pack []byte, cmds ...rawPushCommand) (report map[string]string, remote string) {
	t.Helper()
	caps := "report-status side-band-64k agent=githttp-test"
	if atomic {
		caps += " atomic"
	}
	var body bytes.Buffer
	for i, c := range cmds {
		line := c.old + " " + c.new + " " + c.ref
		if i == 0 {
			line += "\x00" + caps
		}
		body.WriteString(pktLine(line))
	}
	body.WriteString("0000")
	body.Write(pack)
	status, resp := postReceivePack(t, n, repo, token, bytes.NewReader(body.Bytes()), false)
	if status != 200 {
		t.Fatalf("git-receive-pack status = %d, want 200\n%.1500s", status, resp)
	}
	var rep, rem strings.Builder
	for _, p := range pktPayloads(t, resp) {
		if p == "" {
			continue
		}
		switch p[0] {
		case 1:
			rep.WriteString(p[1:])
		case 2, 3:
			rem.WriteString(p[1:])
		default:
			t.Fatalf("unknown side-band %d in %q", p[0], p)
		}
	}
	report = map[string]string{}
	for _, l := range pktPayloads(t, rep.String()) {
		if status, ref, ok := strings.Cut(strings.TrimSuffix(l, "\n"), " "); ok && (status == "ok" || status == "ng") {
			ref, why, _ := strings.Cut(ref, " ")
			report[ref] = strings.TrimSpace(status + " " + why)
		}
	}
	return report, rem.String()
}

// assertNoCrashText fails when the pusher's text shows a Go panic: the
// panic line, the goroutine dump or the assertion prefix.
func assertNoCrashText(t *testing.T, text string) {
	t.Helper()
	for _, bad := range []string{"panic", "goroutine", "assertion failed", ".go:"} {
		if strings.Contains(text, bad) {
			t.Fatalf("the pusher saw %q in:\n%.2000s", bad, text)
		}
	}
}

func TestForgedOldIDUnderNoForcePushIsRefused(t *testing.T) {
	repo := registerRepo(t, "t-forged-old")
	work := initWorkRepo(t)
	c1 := revParse(t, work, "HEAD")
	setup := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, setup), "HEAD:refs/heads/main")

	c2 := commitFile(t, work, "b.txt", "two\n", "two", nil)
	tree1 := revParse(t, work, c1+"^{tree}")
	blob := revParse(t, work, c1+":a.txt")
	rewrite := strings.TrimSpace(mustGit(t, work, "commit-tree", "-m", "rewrite", tree1))

	token := mintTestToken(t, repo, "git:read,git:write", `[["refs/heads/main",["no-force-push"]],["*",[]]]`)
	signedOnly := mintTestToken(t, repo, "git:read,git:write", `[["refs/heads/main",["verify-sig"]],["*",[]]]`)
	const main, feature = "refs/heads/main", "refs/heads/feature"
	const noff = "no-force-push denies non-fast-forward update to " + main
	const noCommit = "verify-sig: the old value of " + main + " is not a readable commit in this repository"
	ffPack := packObjects(t, work, c2, "^"+c1)

	// Each push below is refused for main, which stays at c1. A mixed push
	// also creates feature, so main's update hook runs after pre-receive
	// let the pack through. The fast-forward runs last.
	for _, tc := range []struct {
		name   string
		token  string
		atomic bool
		pack   []byte
		cmds   []rawPushCommand
		want   string // text the pusher must read in the refusal
		landed bool   // feature lands at c2
	}{
		{"old id the server does not have", token, false, ffPack, []rawPushCommand{{forgedOldID, c2, main}}, noff, false},
		{"old id the server does not have, atomic push", token, true, ffPack, []rawPushCommand{{forgedOldID, c2, main}}, noff, false},
		{"old id the server does not have, mixed push", token, false, ffPack, []rawPushCommand{{forgedOldID, c2, main}, {ref.ZeroSHA, c2, feature}}, noff, true},
		{"old id names a tree", token, false, ffPack, []rawPushCommand{{tree1, c2, main}}, noff, false},
		{"new id names a blob", token, false, packObjects(t, work, blob), []rawPushCommand{{c1, blob, main}}, noff, false},
		{"real force push", token, false, packObjects(t, work, rewrite, "^"+c1), []rawPushCommand{{c1, rewrite, main}}, noff, false},
		{"old id the server does not have, verify-sig", signedOnly, false, ffPack, []rawPushCommand{{forgedOldID, c2, main}}, noCommit, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report, remote := rawReceivePack(t, nodeA, repo, tc.token, tc.atomic, tc.pack, tc.cmds...)
			assertNoCrashText(t, remote)
			if !strings.HasPrefix(report[main], "ng") || !strings.Contains(remote, tc.want) {
				t.Fatalf("main was not refused with %q: report %v\nremote:\n%s", tc.want, report, remote)
			}
			refs := pgRefs(t, repo)
			if refs[main] != c1 {
				t.Fatalf("main = %s after a refused push, want %s", refs[main], c1)
			}
			if tc.landed != (report[feature] == "ok" && refs[feature] == c2) {
				t.Fatalf("feature landed = %v, want %v: report %v, feature %s", !tc.landed, tc.landed, report, refs[feature])
			}
		})
	}

	t.Run("fast-forward lands", func(t *testing.T) {
		report, remote := rawReceivePack(t, nodeA, repo, token, false, ffPack, rawPushCommand{c1, c2, main})
		if report[main] != "ok" || pgRefs(t, repo)[main] != c2 {
			t.Fatalf("fast-forward report %v, main %s, want ok at %s\nremote:\n%s", report, pgRefs(t, repo)[main], c2, remote)
		}
	})
}

// plantCommit writes a commit with the given parent into dir's object
// store, even when the parent does not exist, and returns its id.
func plantCommit(t *testing.T, dir, tree, parent string) string {
	t.Helper()
	cmd := exec.Command("git", "hash-object", "-t", "commit", "-w", "--stdin", "--literally")
	cmd.Dir = dir
	cmd.Stdin = strings.NewReader("tree " + tree + "\nparent " + parent + "\nauthor a <a@b.invalid> 1 +0000\ncommitter a <a@b.invalid> 1 +0000\n\nplanted\n")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git hash-object: %v", err)
	}
	return strings.TrimSpace(string(out))
}

// packObjectList returns a pack of exactly the listed objects (no walk).
func packObjectList(t *testing.T, dir string, ids ...string) []byte {
	t.Helper()
	cmd := exec.Command("git", "pack-objects", "--stdout", "-q")
	cmd.Dir = dir
	cmd.Stdin = strings.NewReader(strings.Join(ids, "\n") + "\n")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git pack-objects %q: %v", ids, err)
	}
	return out
}

// receive-pack checks the tips that a push updates, not every object that
// its pack carries, so a pusher can leave a commit with a missing parent in
// the store and name it as an old id. A commit-graph would answer from the
// commits it holds, so this node reads none: git then cannot read the
// planted commit.
func TestPlantedCommitWithoutParentAsOldIDIsRefused(t *testing.T) {
	node := startNodeT(t, "GIT_CONFIG_COUNT=1", "GIT_CONFIG_KEY_0=core.commitGraph", "GIT_CONFIG_VALUE_0=false")
	repo := registerRepo(t, "t-planted")
	work := initWorkRepo(t)
	c1 := revParse(t, work, "HEAD")
	mustGit(t, work, "push", remoteURL(node, repo, mintTestToken(t, repo, "git:read,git:write", "")), "HEAD:refs/heads/main")
	c2 := commitFile(t, work, "b.txt", "two\n", "two", nil)
	planted := plantCommit(t, work, revParse(t, work, c1+"^{tree}"), forgedOldID)
	ids := []string{planted}
	for l := range strings.Lines(mustGit(t, work, "rev-list", "--objects", c2, "^"+c1)) {
		ids = append(ids, strings.Fields(l)[0])
	}

	token := mintTestToken(t, repo, "git:read,git:write", `[["refs/heads/main",["no-force-push"]],["*",[]]]`)
	report, remote := rawReceivePack(t, node, repo, token, false, packObjectList(t, work, ids...), rawPushCommand{planted, c2, "refs/heads/main"})
	assertNoCrashText(t, remote)
	if !strings.HasPrefix(report["refs/heads/main"], "ng") || !strings.Contains(remote, "no-force-push denies non-fast-forward update to refs/heads/main") {
		t.Fatalf("main was not refused: report %v\nremote:\n%s", report, remote)
	}
	if got := pgRefs(t, repo)["refs/heads/main"]; got != c1 {
		t.Fatalf("main = %s after a refused push, want %s", got, c1)
	}
}

// A git that fails for two readable commits is a broken git or repository,
// not a pusher's input: the ancestry check must not turn it into a refusal.
func TestBrokenGitStillFailsFast(t *testing.T) {
	sha := strings.Repeat("a", 40)
	for _, tc := range []struct{ name, failing string }{
		{"merge-base fails for two readable commits", "merge-base"},
		{"rev-parse fails with a corrupt object", "merge-base|rev-parse"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bin := t.TempDir()
			script := "#!/bin/sh\ncase \"$1\" in " + tc.failing + ") exit 128;; esac\nexit 0\n"
			if err := os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0o755); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("isAncestorUncached answered for a broken git instead of failing")
				}
			}()
			isAncestorUncached(sha, strings.Repeat("b", 40))
		})
	}
}
