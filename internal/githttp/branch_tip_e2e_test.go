package githttp

// git refuses to store anything but a commit under refs/heads/, but it
// checks after the hooks have run, and the update hook's compare-and-swap
// has committed the value to the ref store by then. The hooks must refuse
// such a tip first: the store would otherwise hold a value that no cache can
// materialize, and every read of the repository would fail.

import (
	"bytes"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/ref"
)

type tipCommand struct{ old, new, ref string }

// tipObjects returns the pack that `git pack-objects --revs` writes for the
// revision lines, run in dir.
func tipObjects(t *testing.T, dir string, revs ...string) []byte {
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

// tipPayloads splits a pkt-line stream into its payloads (flush packets
// dropped).
func tipPayloads(t *testing.T, s string) []string {
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

// tipPush posts one hand-built receive-pack request with report-status and
// side-band-64k. It returns each ref's report ("ok" or "ng <reason>") and
// the side-band text that git relays to the pusher as "remote:" lines.
func tipPush(t *testing.T, n *testNode, repo, token string, atomic bool, pack []byte, cmds ...tipCommand) (report map[string]string, remote string) {
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
	for _, p := range tipPayloads(t, resp) {
		switch {
		case p == "":
		case p[0] == 1:
			rep.WriteString(p[1:])
		case p[0] == 2 || p[0] == 3:
			rem.WriteString(p[1:])
		default:
			t.Fatalf("unknown side-band %d in %q", p[0], p)
		}
	}
	report = map[string]string{}
	for _, l := range tipPayloads(t, rep.String()) {
		if kind, rest, ok := strings.Cut(strings.TrimSuffix(l, "\n"), " "); ok && (kind == "ok" || kind == "ng") {
			name, why, _ := strings.Cut(rest, " ")
			report[name] = strings.TrimSpace(kind + " " + why)
		}
	}
	return report, rem.String()
}

func TestBranchTipThatIsNoCommitIsRefused(t *testing.T) {
	repo := registerRepo(t, "t-branch-tip")
	work := initWorkRepo(t)
	c1 := revParse(t, work, "HEAD")
	setup := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, setup), "HEAD:refs/heads/main")

	// Objects the server does not have yet, so a refused push carries a
	// pack that only the refused tip needs.
	c2 := commitFile(t, work, "b.txt", "two\n", "two", nil)
	blob := strings.TrimSpace(mustGitStdin(t, work, "brand new blob\n", "hash-object", "-w", "--stdin"))
	tree := strings.TrimSpace(mustGitStdin(t, work, "100644 blob "+blob+"\tnew.txt\n", "mktree"))
	mustGit(t, work, "tag", "-a", "-m", "annotated", "v1", c1)
	annotated := revParse(t, work, "v1")
	const zero = ref.ZeroSHA

	token := mintTestToken(t, repo, "git:read,git:write", "")
	noForce := mintTestToken(t, repo, "git:read,git:write", `[["refs/heads/main",["no-force-push"]],["*",[]]]`)

	for _, tc := range []struct {
		name   string
		token  string
		atomic bool
		pack   []byte
		cmds   []tipCommand
		want   string // text the pusher must read, per refused ref
		landed string // a ref of the push that must land at c2
	}{
		{"create at a blob", token, false, tipObjects(t, work, blob, "^"+c1), []tipCommand{{zero, blob, "refs/heads/blobby"}}, "must point at a commit, not a blob", ""},
		{"create at a tree", token, false, tipObjects(t, work, tree, "^"+c1), []tipCommand{{zero, tree, "refs/heads/treeish"}}, "must point at a commit, not a tree", ""},
		{"create at an annotated tag", token, false, tipObjects(t, work, annotated, "^"+c1), []tipCommand{{zero, annotated, "refs/heads/tagged"}}, "must point at a commit, not an annotated tag", ""},
		{"update at a blob", token, false, tipObjects(t, work, blob, "^"+c1), []tipCommand{{c1, blob, "refs/heads/main"}}, "must point at a commit, not a blob", ""},
		{"update at a blob under no-force-push", noForce, false, tipObjects(t, work, blob, "^"+c1), []tipCommand{{c1, blob, "refs/heads/main"}}, "must point at a commit, not a blob", ""},
		{"atomic push", token, true, tipObjects(t, work, c2, blob, "^"+c1), []tipCommand{{zero, c2, "refs/heads/ok"}, {zero, blob, "refs/heads/blobby"}}, "must point at a commit, not a blob", ""},
		{"mixed push", token, false, tipObjects(t, work, c2, blob, "^"+c1), []tipCommand{{zero, c2, "refs/heads/ok"}, {zero, blob, "refs/heads/blobby"}}, "must point at a commit, not a blob", "refs/heads/ok"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report, remote := tipPush(t, nodeA, repo, tc.token, tc.atomic, tc.pack, tc.cmds...)
			for _, bad := range []string{"panic", "goroutine", ".go:"} {
				if strings.Contains(remote, bad) {
					t.Fatalf("the pusher saw %q in:\n%.1500s", bad, remote)
				}
			}
			refs := pgRefs(t, repo)
			for _, c := range tc.cmds {
				switch {
				case c.new == blob || c.new == tree || c.new == annotated:
					if !strings.HasPrefix(report[c.ref], "ng") {
						t.Fatalf("%s was not refused: report %v\nremote:\n%s", c.ref, report, remote)
					}
				case c.ref == tc.landed:
					if report[c.ref] != "ok" || refs[c.ref] != c2 {
						t.Fatalf("%s did not land at %s: report %v, ref %s", c.ref, c2, report, refs[c.ref])
					}
					continue
				default:
					// An atomic push lands nothing when one command is refused.
				}
				if c.ref != "refs/heads/main" {
					if got, ok := refs[c.ref]; ok {
						t.Fatalf("the ref store holds %s = %s after the push", c.ref, got)
					}
				}
			}
			if !strings.Contains(remote, tc.want) {
				t.Fatalf("the refusal does not say %q:\n%s", tc.want, remote)
			}
			if refs["refs/heads/main"] != c1 {
				t.Fatalf("main = %s after a refused push, want %s", refs["refs/heads/main"], c1)
			}
		})
	}

	t.Run("a refused tip stores no pack", func(t *testing.T) {
		before := len(pgPacks(t, repo))
		blob2 := strings.TrimSpace(mustGitStdin(t, work, "another brand new blob\n", "hash-object", "-w", "--stdin"))
		report, remote := tipPush(t, nodeA, repo, token, false, tipObjects(t, work, blob2, "^"+c1), tipCommand{zero, blob2, "refs/heads/second"})
		if !strings.HasPrefix(report["refs/heads/second"], "ng") {
			t.Fatalf("second was not refused: report %v\nremote:\n%s", report, remote)
		}
		if after := len(pgPacks(t, repo)); after != before {
			t.Fatalf("registered packs = %d after a refused tip, want %d: the pack was stored for nothing", after, before)
		}
	})

	t.Run("tags take any object and a commit tip lands", func(t *testing.T) {
		report, remote := tipPush(t, nodeA, repo, token, false, tipObjects(t, work, blob, "^"+c1), tipCommand{zero, blob, "refs/tags/blobtag"})
		if report["refs/tags/blobtag"] != "ok" {
			t.Fatalf("a tag at a blob was refused: report %v\nremote:\n%s", report, remote)
		}
		report, remote = tipPush(t, nodeA, repo, token, false, tipObjects(t, work, c2, "^"+c1), tipCommand{c1, c2, "refs/heads/main"})
		if report["refs/heads/main"] != "ok" || pgRefs(t, repo)["refs/heads/main"] != c2 {
			t.Fatalf("a fast-forward did not land: report %v\nremote:\n%s", report, remote)
		}
	})

	// The repository still answers reads: no refused tip reached the ref
	// store, so hydration has nothing that git cannot write.
	if got := lsRemoteSHA(t, work, remoteURL(nodeA, repo, token), "refs/heads/main"); got != c2 {
		t.Fatalf("main = %q, want %q", got, c2)
	}
}

// mustGitStdin runs git in dir with stdin and returns its stdout.
func mustGitStdin(t *testing.T, dir, stdin string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append(append([]string{}, gitIdentity...), args...)...)
	cmd.Dir = dir
	cmd.Stdin = strings.NewReader(stdin)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git %s: %v", strings.Join(args, " "), err)
	}
	return string(out)
}
