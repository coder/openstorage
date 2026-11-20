package githttp

// git sends the old value of a ref that it deletes. When the replica's cache
// does not list the ref, git sends the zero id for it, so the command is
// zero to zero, and stock git's receive-pack treats it as a delete of a ref
// that does not exist: a no-op that it reports as ok, with a warning. The
// update hook gets the command as it came. A zero to zero swap in the ref
// store is an assertion, so the hook must answer the command itself, and the
// pusher must never read a Go trace.

import (
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/ref"
)

func TestDeleteOfAnAbsentRefIsAnsweredNotCrashed(t *testing.T) {
	var serveLog syncBuffer
	node, err := startBinaryNode(binPath, t.TempDir(), io.MultiWriter(os.Stderr, &serveLog))
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(node.stop)
	repo := registerRepo(t, "t-delete-absent")
	work := initWorkRepo(t)
	c1 := revParse(t, work, "HEAD")
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/main")
	noPush := mintTestToken(t, repo, "git:read,git:write", `[["refs/notes/*",["no-push"]],["*",[]]]`)

	const held = "refs/notes/held"
	if won, err := testStore.CASRef(t.Context(), repo, held, ref.ZeroSHA, c1); err != nil || !won {
		t.Fatalf("plant %s in the ref store: won=%v err=%v", held, won, err)
	}
	// The branch and tag names are the shapes of the delete in the runbook.
	for _, tc := range []struct {
		name, token, ref string
		wantOK           bool
		wantText         string
	}{
		{"a tag that no store holds", token, "refs/tags/ghost", true, ""},
		{"a branch that no store holds", token, "refs/heads/ghost", true, ""},
		{"a notes ref that no store holds", token, "refs/notes/ghost", true, ""},
		{"a notes ref that the ref store holds", token, held, false, held + " exists at " + c1[:8]},
		{"a notes ref under a no-push policy", noPush, "refs/notes/ghost", false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logStart := len(serveLog.String())
			before := pgRefs(t, repo)
			report, remote := rawReceivePack(t, node, repo, tc.token, false, nil, rawPushCommand{ref.ZeroSHA, ref.ZeroSHA, tc.ref})
			assertNoCrashText(t, remote)
			if got := strings.HasPrefix(report[tc.ref], "ok"); got != tc.wantOK {
				t.Fatalf("%s: report %q, want ok = %v\nremote:\n%s", tc.ref, report[tc.ref], tc.wantOK, remote)
			}
			if !strings.Contains(remote, tc.wantText) {
				t.Fatalf("the pusher does not read %q:\n%s", tc.wantText, remote)
			}
			after := pgRefs(t, repo)
			if len(after) != len(before) || after[held] != c1 || after[tc.ref] != before[tc.ref] {
				t.Fatalf("refs changed: before %v, after %v", before, after)
			}
			if tc.wantText != "" && !tc.wantOK && tc.ref == held {
				// serve logs the hook-errors file after the push ends.
				pushLog := func() string { return serveLog.String()[logStart:] }
				for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline) && !strings.Contains(pushLog(), held); {
					time.Sleep(20 * time.Millisecond)
				}
				if !strings.Contains(pushLog(), held) {
					t.Fatalf("serve's log does not name %s:\n%s", held, pushLog())
				}
			}
		})
	}
}
