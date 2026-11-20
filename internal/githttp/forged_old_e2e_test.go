package githttp

// A git client names the old value of every ref it updates, and git hands
// that name to the hooks before it compares it with the stored ref. Under a
// no-force-push policy the hooks ask git whether old is an ancestor of new.
// An old or new id that names no commit must end in a refusal for that ref,
// never in a Go panic whose stack git relays to the pusher.

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

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
	const noCommit = "verify-sig: the old or new value of " + main + " is not a readable commit in this repository"
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
		// A branch at a blob is refused as a branch tip before the policy asks
		// for ancestry. The tag test below keeps the ancestry case.
		{"new id names a blob", token, false, packObjects(t, work, blob), []rawPushCommand{{c1, blob, main}}, "must point at a commit, not a blob", false},
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

// A tag can name a blob, a tree or an annotated tag, so a forced update of a tag
// moves between a commit and a non-commit under a no-force-push tag policy. The
// ancestry check must refuse it like any non-fast-forward update, and the
// pusher must read no Go trace.
func TestForcedTagUpdateBetweenACommitAndANonCommitIsRefused(t *testing.T) {
	repo := registerRepo(t, "t-forced-tag")
	work := initWorkRepo(t)
	c1 := revParse(t, work, "HEAD")
	setup := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, setup), "HEAD:refs/heads/main")
	blob := revParse(t, work, c1+":a.txt")
	tree := revParse(t, work, c1+"^{tree}")
	const toBlob, toCommit, toTree = "refs/tags/to-blob", "refs/tags/to-commit", "refs/tags/to-tree"
	// Tags at a commit and at a blob, made with a token that has no policy.
	if report, remote := rawReceivePack(t, nodeA, repo, setup, false, packObjects(t, work, blob), rawPushCommand{ref.ZeroSHA, blob, toCommit}); report[toCommit] != "ok" {
		t.Fatalf("create %s at a blob: %v\n%s", toCommit, report, remote)
	}
	mustGit(t, work, "push", remoteURL(nodeA, repo, setup), c1+":"+toBlob)
	tagToken := mintTestToken(t, repo, "git:read,git:write", `[["refs/tags/*",["no-force-push"]],["*",[]]]`)
	for _, tc := range []struct {
		name string
		pack []byte
		cmd  rawPushCommand
	}{
		{"a tag at a commit moves to a blob", packObjects(t, work, blob), rawPushCommand{c1, blob, toBlob}},
		{"a tag at a commit moves to a tree", packObjects(t, work, tree), rawPushCommand{c1, tree, toTree}},
		{"a tag at a blob moves to a commit", packObjects(t, work, c1), rawPushCommand{blob, c1, toCommit}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.cmd.ref == toTree {
				if report, remote := rawReceivePack(t, nodeA, repo, setup, false, packObjects(t, work, c1), rawPushCommand{ref.ZeroSHA, c1, toTree}); report[toTree] != "ok" {
					t.Fatalf("create %s: %v\n%s", toTree, report, remote)
				}
			}
			report, remote := rawReceivePack(t, nodeA, repo, tagToken, false, tc.pack, tc.cmd)
			assertNoCrashText(t, remote)
			want := "no-force-push denies non-fast-forward update to " + tc.cmd.ref
			if !strings.HasPrefix(report[tc.cmd.ref], "ng") || !strings.Contains(remote, want) {
				t.Fatalf("%s was not refused with %q: report %v\nremote:\n%s", tc.cmd.ref, want, report, remote)
			}
			if got := pgRefs(t, repo)[tc.cmd.ref]; got != tc.cmd.old {
				t.Fatalf("%s = %s after a refused push, want %s", tc.cmd.ref, got, tc.cmd.old)
			}
		})
	}
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
// the store, and git still hands a command that names one to pre-receive. A
// commit-graph would answer from the commits it holds, so this node reads
// none: git then cannot read the planted commit.
//
// git cannot walk the history between two real commits there, so the
// operator also gets a line in serve's log, through the hook-errors file.
func TestPlantedCommitWithoutParentIsRefused(t *testing.T) {
	var serveLog syncBuffer
	node, err := startBinaryNode(binPath, t.TempDir(), io.MultiWriter(os.Stderr, &serveLog),
		"GIT_CONFIG_COUNT=1", "GIT_CONFIG_KEY_0=core.commitGraph", "GIT_CONFIG_VALUE_0=false")
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(node.stop)
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
	pack := packObjectList(t, work, ids...)

	const main = "refs/heads/main"
	noForce := mintTestToken(t, repo, "git:read,git:write", `[["refs/heads/main",["no-force-push"]],["*",[]]]`)
	signed := mintTestToken(t, repo, "git:read,git:write", `[["refs/heads/main",["verify-sig"]],["*",[]]]`)
	const noff = "no-force-push denies non-fast-forward update to " + main
	const unreadable = "is not a readable commit in this repository"
	for _, tc := range []struct {
		name, token, want string
		cmd               rawPushCommand
	}{
		{"planted old id, no-force-push", noForce, noff, rawPushCommand{planted, c2, main}},
		{"planted new id, no-force-push", noForce, noff, rawPushCommand{c1, planted, main}},
		{"planted new id, verify-sig", signed, unreadable, rawPushCommand{c1, planted, main}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logStart := len(serveLog.String())
			report, remote := rawReceivePack(t, node, repo, tc.token, false, pack, tc.cmd)
			assertNoCrashText(t, remote)
			if !strings.HasPrefix(report[main], "ng") || !strings.Contains(remote, tc.want) {
				t.Fatalf("main was not refused with %q: report %v\nremote:\n%s", tc.want, report, remote)
			}
			if got := pgRefs(t, repo)[main]; got != c1 {
				t.Fatalf("main = %s after a refused push, want %s", got, c1)
			}
			// serve logs the hook-errors file after the push ends.
			pushLog := func() string { return serveLog.String()[logStart:] }
			for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline) && unwalkableLine(pushLog(), planted) == ""; {
				time.Sleep(20 * time.Millisecond)
			}
			if unwalkableLine(pushLog(), planted) == "" {
				t.Fatalf("serve's log has no line that git cannot walk the history to %s:\n%s", planted, pushLog())
			}
		})
	}
}

// unwalkableLine returns the operator line of logUnwalkableHistory that
// names sha in log, or "".
func unwalkableLine(log, sha string) string {
	for l := range strings.Lines(log) {
		if strings.Contains(l, "git cannot walk the history between two commits") && strings.Contains(l, sha) {
			return l
		}
	}
	return ""
}

// standInGit puts a git on PATH that logs the first argument of each command
// it runs to the returned file and answers by cases, a shell case list on $1
// ($4 is the id of a rev-parse --verify --quiet), and with exit 0 otherwise.
// It also points the hook-errors file at a new file and returns that too.
func standInGit(t *testing.T, cases string) (calls, records string) {
	t.Helper()
	bin := t.TempDir()
	calls = filepath.Join(bin, "calls")
	script := "#!/bin/sh\necho \"$1\" >> " + calls + "\ncase \"$1\" in " + cases + " esac\nexit 0\n"
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	records, err := createHookErrors(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvHookErrors, records)
	return calls, records
}

// noWalkAfter fails the test when the stand-in git ran rev-list.
func noWalkAfter(t *testing.T, calls string) {
	t.Helper()
	b, err := os.ReadFile(calls)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "rev-list") {
		t.Fatalf("the check ran rev-list after merge-base:\n%s", b)
	}
}

// merge-base exits 128 for two commits when git cannot walk their history:
// a planted commit or a damaged repository. That is a refusal with an
// operator line in the hook-errors file, and no further walk decides which
// one it is. An id that names no commit, old or new, is a refusal without
// that line. A corrupt object, or any other failure of git, is a broken git
// or repository and fails fast.
func TestAncestryCheckOnAGitThatExits128(t *testing.T) {
	old, new := strings.Repeat("a", 40), strings.Repeat("b", 40)
	for _, tc := range []struct {
		name, cases, panics string
		line                bool
	}{
		{"two commits git cannot walk", "merge-base) exit 128;;", "", true},
		{"an id that names no commit", "merge-base) exit 128;; rev-parse) exit 1;;", "", false},
		{"only the old id names no commit", "merge-base) exit 128;; rev-parse) case \"$4\" in a*) exit 1;; esac;;", "", false},
		{"only the new id names no commit", "merge-base) exit 128;; rev-parse) case \"$4\" in b*) exit 1;; esac;;", "", false},
		{"rev-parse fails with a corrupt object", "merge-base|rev-parse) exit 128;;", "rev-parse --verify", false},
		{"merge-base fails for another reason", "merge-base) exit 129;;", "merge-base --is-ancestor", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls, records := standInGit(t, tc.cases)
			if tc.panics != "" {
				defer func() {
					if r := recover(); r == nil || !strings.Contains(fmt.Sprint(r), tc.panics) {
						t.Fatalf("isAncestorUncached recovered %v, want a panic naming %q", r, tc.panics)
					}
				}()
			}
			if isAncestorUncached(old, new) {
				t.Fatal("isAncestorUncached = true for a walk that git refused")
			}
			if line := unwalkableLine(readRecords(t, records), new); (line != "") != tc.line {
				t.Fatalf("operator line %q, want one: %v", line, tc.line)
			}
			// No walk beyond merge-base: only the two cheap commit checks.
			noWalkAfter(t, calls)
		})
	}
}

// The verify-sig walk answers like the ancestry check when its rev-list
// exits 128: a refusal, with the operator line only when both ids name
// commits, and a branch that a push creates has no old id to check. Any
// other failure of git is an error.
func TestIntroducedPendingOnAGitThatExits128(t *testing.T) {
	old, new := strings.Repeat("a", 40), strings.Repeat("b", 40)
	const unreadable = "is not a readable commit in this repository"
	for _, tc := range []struct {
		name, old, cases string
		line             bool
	}{
		{"two commits git cannot walk", old, "rev-list) exit 128;;", true},
		{"a new branch at a commit git cannot walk", ref.ZeroSHA, "rev-list) exit 128;;", true},
		{"the new id names no commit", old, "rev-list) exit 128;; rev-parse) case \"$4\" in b*) exit 1;; esac;;", false},
		{"the old id names no commit", old, "rev-list) exit 128;; rev-parse) case \"$4\" in a*) exit 1;; esac;;", false},
		{"a new branch at an id that names no commit", ref.ZeroSHA, "rev-list) exit 128;; rev-parse) exit 1;;", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls, records := standInGit(t, tc.cases)
			st, err := openVerifyState()
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = introducedPending(t.Context(), st, "refs/heads/main", tc.old, new, 1000)
			if !ShownToPusher(err) || !strings.Contains(err.Error(), unreadable) {
				t.Fatalf("introducedPending = %v, want a refusal that says %q", err, unreadable)
			}
			if line := unwalkableLine(readRecords(t, records), new); (line != "") != tc.line {
				t.Fatalf("operator line %q, want one: %v", line, tc.line)
			}
			// rev-list is the walk itself: the refusal runs no second one.
			if b, err := os.ReadFile(calls); err != nil || strings.Count(string(b), "rev-list") != 1 {
				t.Fatalf("calls %q (%v), want one rev-list", b, err)
			}
		})
	}
	t.Run("rev-list fails for another reason", func(t *testing.T) {
		_, records := standInGit(t, "rev-list) exit 129;;")
		st, err := openVerifyState()
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = introducedPending(t.Context(), st, "refs/heads/main", old, new, 1000)
		if err == nil || ShownToPusher(err) {
			t.Fatalf("introducedPending = %v, want an error that is no refusal", err)
		}
		if line := unwalkableLine(readRecords(t, records), new); line != "" {
			t.Fatalf("operator line %q for a rev-list that exited 129", line)
		}
	})
}
