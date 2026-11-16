package githttp

// Regression for junk pack rows. git refuses some commands BEFORE the
// update hook (missing objects, shallow update not allowed) yet still hands
// them to pre-receive. Such a command can never land, so the publication
// gate (preReceivePolicyGate) must not store a pack that only it needs: a
// registered pack that no ref reaches is never swept, counts against the
// quota, and every repack carries it forward.

import (
	"bytes"
	"os/exec"
	"path/filepath"
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
			caps := "\x00report-status"
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
			assertOnlySetupPack(t, repo, 1)
		})
	}
}
