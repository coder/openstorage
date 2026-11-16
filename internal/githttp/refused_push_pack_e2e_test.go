package githttp

// Regression for junk pack rows. git refuses some commands BEFORE the
// update hook (missing objects, shallow update not allowed) yet still hands
// them to pre-receive. Such a command can never land, so the publication
// gate (preReceivePolicyGate) must not store a pack that only it needs: a
// registered pack that no ref reaches is never swept, counts against the
// quota, and every repack carries it forward.

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

// refusedGit runs git in dir with stdin and returns its raw stdout.
func refusedGit(t *testing.T, dir, stdin string, args ...string) []byte {
	t.Helper()
	cmd := exec.Command("git", append(append([]string{}, gitIdentity...), args...)...)
	cmd.Dir = dir
	cmd.Stdin = strings.NewReader(stdin)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, stderr.String())
	}
	return out
}

// assertOnlySetupPack fails unless repo holds exactly want packs, the ones
// its setup push stored, as rows and as blobs.
func assertOnlySetupPack(t *testing.T, repo string, want int) {
	t.Helper()
	if packs := pgPacks(t, repo); len(packs) != want {
		t.Errorf("registered packs = %d, want %d: a push git refused registered its pack", len(packs), want)
	}
	if objs := s3Packs(t, repo); len(objs) != want {
		t.Errorf("stored pack blobs = %d, want %d: a push git refused uploaded its pack", len(objs), want)
	}
}

func TestPackPublication_GitRefusedCommandStoresNothing(t *testing.T) {
	t.Run("shallow push", func(t *testing.T) {
		src := initWorkRepo(t)
		commitFile(t, src, "b.txt", "two\n", "two", nil)
		shallow := filepath.Join(t.TempDir(), "shallow")
		mustGit(t, "", "clone", "-q", "--depth", "1", "file://"+src, shallow)
		repo := registerRepo(t, "refused-shallow")
		token := mintTestToken(t, repo, "git:read,git:write", "")
		out, err := tryGit(shallow, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")
		if err == nil || !strings.Contains(out, "shallow update not allowed") {
			t.Fatalf("want git's shallow refusal, got err=%v:\n%s", err, out)
		}
		assertOnlySetupPack(t, repo, 0)
	})

	t.Run("many refs at one incomplete commit", func(t *testing.T) {
		// A writer can name one incomplete commit with hundreds of refs. The
		// gate asks git once for the commit, and refuses the push.
		repo, work, token, stored := refusedRawSetup(t, "refused-many")
		bad := refusedBadCommits(t, work, stored, 1)
		resp := refusedRawPush(t, repo, token, work, bad, 200)
		if !strings.Contains(resp, "needed only by ref updates that git refuses") {
			t.Fatalf("the pusher does not read the refusal:\n%.600q", resp)
		}
		assertOnlySetupPack(t, repo, 1)
	})

	t.Run("more distinct incomplete commits than the probe budget", func(t *testing.T) {
		// The gate spends at most maxGitRefusedProbes failing checks. Past that
		// it publishes as it did before the check, and git still refuses each ref.
		repo, work, token, stored := refusedRawSetup(t, "refused-distinct")
		bad := refusedBadCommits(t, work, stored, maxGitRefusedProbes+1)
		resp := refusedRawPush(t, repo, token, work, bad, 1)
		if !strings.Contains(resp, "ng refs/heads/x0") || strings.Contains(resp, "ok refs/heads/x") {
			t.Fatalf("git must refuse every ref:\n%.600q", resp)
		}
		assertOnlySetupPack(t, repo, 2)
	})

	t.Run("within the probe budget", func(t *testing.T) {
		repo, work, token, stored := refusedRawSetup(t, "refused-budget")
		bad := refusedBadCommits(t, work, stored, maxGitRefusedProbes)
		refusedRawPush(t, repo, token, work, bad, 1)
		assertOnlySetupPack(t, repo, 1)
	})

	for _, tc := range []struct {
		name  string
		decoy bool // also create refs/heads/y at the stored commit
	}{
		{"commit with a missing tree", false},
		{"beside a create at a stored commit", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := registerRepo(t, "refused-raw")
			work := initWorkRepo(t)
			token := mintTestToken(t, repo, "git:read,git:write", "")
			mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")
			stored := revParse(t, work, "HEAD")
			junk := strings.TrimSpace(string(refusedGit(t, work, strings.Repeat("junk ", 500), "hash-object", "-w", "--stdin")))
			bad := strings.TrimSpace(string(refusedGit(t, work, "tree "+strings.Repeat("1", 40)+"\nparent "+stored+
				"\nauthor a <a@x> 1700000000 +0000\ncommitter a <a@x> 1700000000 +0000\n\nmissing tree\n",
				"hash-object", "-t", "commit", "-w", "--literally", "--stdin")))
			pack := refusedGit(t, work, bad+"\n"+junk+"\n", "pack-objects", "--stdout", "-q")

			var body bytes.Buffer
			caps := "\x00report-status side-band-64k"
			if tc.decoy {
				body.WriteString(pktLine(ref.ZeroSHA + " " + stored + " refs/heads/y" + caps + "\n"))
				caps = ""
			}
			body.WriteString(pktLine(ref.ZeroSHA + " " + bad + " refs/heads/x" + caps + "\n"))
			body.WriteString("0000")
			body.Write(pack)
			_, resp := postReceivePack(t, nodeA, repo, token, bytes.NewReader(body.Bytes()), false)
			if !strings.Contains(resp, "ng refs/heads/x") {
				t.Fatalf("git accepted refs/heads/x:\n%q", resp)
			}
			// The pusher reads why nothing was stored when the whole push is refused.
			if !strings.Contains(resp, "needed only by ref updates that git refuses") {
				t.Fatalf("the pusher does not read the refusal:\n%q", resp)
			}
			assertOnlySetupPack(t, repo, 1)
		})
	}
}

// reachesUpdateHook answers "no" only when git exits 128, the status of a
// connectivity check that cannot read an object. A git that does not know an
// option (129), a signal and an out-of-memory kill say nothing about the
// command, so the gate must not refuse a push on them.
func TestReachesUpdateHookRefusesOnlyOnExit128(t *testing.T) {
	for _, tc := range []struct {
		status string
		script string // the body of the git stand-in
		want   bool
	}{
		{"0", "exit 0", true}, {"1", "exit 1", true}, {"128", "exit 128", false},
		{"129", "exit 129", true}, {"137", "exit 137", true},
		// A real signal reaches Go as exit code -1, not 137.
		{"signal", "kill -9 $$", true},
	} {
		t.Run("exit "+tc.status, func(t *testing.T) {
			bin := t.TempDir()
			script := "#!/bin/sh\n" + tc.script + "\n"
			if err := os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0o755); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
			if got := reachesUpdateHook(strings.Repeat("a", 40)); got != tc.want {
				t.Fatalf("reachesUpdateHook with a git that exits %s = %v, want %v", tc.status, got, tc.want)
			}
		})
	}
}

// refusedRawSetup stores one commit through a real push and returns the
// repository, the work tree, the token and the stored commit.
func refusedRawSetup(t *testing.T, name string) (repo, work, token, stored string) {
	t.Helper()
	repo = registerRepo(t, name)
	work = initWorkRepo(t)
	token = mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")
	return repo, work, token, revParse(t, work, "HEAD")
}

// refusedBadCommits writes n distinct commits whose tree no store holds.
func refusedBadCommits(t *testing.T, work, parent string, n int) []string {
	t.Helper()
	var ids []string
	for i := range n {
		ids = append(ids, strings.TrimSpace(string(refusedGit(t, work, "tree "+strings.Repeat("1", 40)+"\nparent "+parent+
			"\nauthor a <a@x> 1700000000 +0000\ncommitter a <a@x> 1700000000 +0000\n\nmissing tree "+strconv.Itoa(i)+"\n",
			"hash-object", "-t", "commit", "-w", "--literally", "--stdin"))))
	}
	return ids
}

// refusedRawPush sends, in one raw receive-pack request, perCommit creates of
// refs/heads/x<i>-<j> at each commit in bad, with a pack that holds only those
// commits. It returns git's response.
func refusedRawPush(t *testing.T, repo, token, work string, bad []string, perCommit int) string {
	t.Helper()
	var body bytes.Buffer
	caps := "\x00report-status side-band-64k"
	for i, id := range bad {
		for j := range perCommit {
			name := "refs/heads/x" + strconv.Itoa(i)
			if perCommit > 1 {
				name += "-" + strconv.Itoa(j)
			}
			body.WriteString(pktLine(ref.ZeroSHA + " " + id + " " + name + caps + "\n"))
			caps = ""
		}
	}
	body.WriteString("0000")
	body.Write(refusedGit(t, work, strings.Join(bad, "\n")+"\n", "pack-objects", "--stdout", "-q"))
	_, resp := postReceivePack(t, nodeA, repo, token, bytes.NewReader(body.Bytes()), false)
	return resp
}

// The probe asks once for each distinct target and stops asking after
// maxGitRefusedProbes failures.
func TestGitRefusalProbeBoundsTheChecks(t *testing.T) {
	// The docs say eight, so the value is pinned here and in the docs together.
	if maxGitRefusedProbes != 8 {
		t.Fatalf("maxGitRefusedProbes = %d, want 8: docs/explanation/wire-compatibility.md says eight", maxGitRefusedProbes)
	}
	var asked []string
	probe := newGitRefusalProbe(func(sha string) bool { asked = append(asked, sha); return false })
	for range 1000 {
		if probe.reaches("a") {
			t.Fatal("a target that failed reads as reaching the update hook")
		}
	}
	if len(asked) != 1 {
		t.Fatalf("1000 commands at one target asked git %d times, want 1", len(asked))
	}
	asked = nil
	probe = newGitRefusalProbe(func(sha string) bool { asked = append(asked, sha); return false })
	var reached int
	for i := range 1000 {
		if probe.reaches(strconv.Itoa(i)) {
			reached++
		}
	}
	if len(asked) != maxGitRefusedProbes || reached != 1000-maxGitRefusedProbes {
		t.Fatalf("1000 distinct failing targets asked git %d times (%d read as reaching), want %d and %d",
			len(asked), reached, maxGitRefusedProbes, 1000-maxGitRefusedProbes)
	}
	// A target that passes is remembered and never costs a failure.
	asked = nil
	probe = newGitRefusalProbe(func(sha string) bool { asked = append(asked, sha); return sha == "ok" })
	first, again := probe.reaches("ok"), probe.reaches("ok")
	if !first || !again || len(asked) != 1 {
		t.Fatalf("a passing target asked git %d times (answers %v, %v), want 1 and true, true", len(asked), first, again)
	}
}
