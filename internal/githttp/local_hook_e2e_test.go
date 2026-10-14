package githttp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref"
)

// The external serve binary is essential: in-process fixtures dispatch hooks
// through this package's TestMain, which does not wire command-side eventing.
func TestLocalHookAcceptedPushRecordsIntent(t *testing.T) {
	skipUnlessLocalSchedule(t)
	ctx := t.Context()
	repo := "https://example.com/local-hook-intent.git"
	id := registerRepo(t, repo)
	if _, err := testMeta.SetRepoUpstream(ctx, meta.RepoUpstream{
		RepoID: id, Provider: "github", Owner: "acme", Name: "demo",
		UpstreamHost: "http://127.0.0.1:1", ForwardOnPush: true,
	}); err != nil {
		t.Fatal(err)
	}
	// No seed marker or active claim: boot recovery cannot manufacture the
	// trigger we are testing. The closed loopback endpoint avoids external upstream I/O.
	work := initWorkRepo(t)
	head := revParse(t, work, "HEAD")
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, id, token), "HEAD:refs/heads/main")
	if got := pgRefs(t, id)["refs/heads/main"]; got != head {
		t.Fatalf("authority main=%q, want accepted commit %q", got, head)
	}
	up, err := testMeta.GetRepoUpstream(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if up.PushPending == 0 && !up.PushSyncing && up.PushRunCount == 0 {
		t.Fatal("authority accepted the commit, but no durable push intent or claimed run exists")
	}
}

// C10 of the #266 design note: while a pull run holds its claim, an
// accepted push records no pull intent (the running pull mirrors the
// upstream anyway). Once the claim is released, the next push records
// intent again. On S3WAL this needs the hook to read the claim from the
// metadata document in SeaweedFS.
func TestLocalHookLivePullClaimSuppressesPullIntent(t *testing.T) {
	skipUnlessLocalSchedule(t)
	ctx := t.Context()
	url := fmt.Sprintf("https://example.com/local-hook-c10-%d.git", time.Now().UnixNano())
	id := registerRepo(t, url)
	if _, err := testMeta.SetRepoUpstream(ctx, meta.RepoUpstream{
		RepoID: id, Provider: "github", Owner: "acme", Name: "demo", UpstreamHost: "http://127.0.0.1:1",
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = testMeta.DeleteRepoUpstream(context.Background(), id) })
	// A claim no serve process holds: its lease outlives the test, so no
	// sweep can steal it.
	const holder = "c10-live-run"
	if _, claimed, _, err := testMeta.TryBeginSync(ctx, id, holder, 10*time.Minute); err != nil || !claimed {
		t.Fatalf("claim = (%v, %v), want claimed", claimed, err)
	}
	work := initWorkRepo(t)
	token := mintTestToken(t, url, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, id, token), "HEAD:refs/heads/main")
	up, err := testMeta.GetRepoUpstream(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if up.SyncPending || !up.Syncing || up.SyncClaimHolder != holder || up.RunCount != 0 {
		t.Fatalf("push under a live pull claim: row %+v, want the claim kept and no pull intent", up)
	}

	if _, err := testMeta.FinishSync(ctx, id, holder, true); err != nil {
		t.Fatal(err)
	}
	mustGit(t, work, "commit", "--allow-empty", "-m", "after the claim")
	mustGit(t, work, "push", remoteURL(nodeA, id, token), "HEAD:refs/heads/main")
	up, err = testMeta.GetRepoUpstream(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	// The serve sweep may already have claimed or run the recorded pull.
	if !up.SyncPending && !up.Syncing && up.RunCount < 2 {
		t.Fatalf("push after the claim was released: row %+v, want pull intent", up)
	}
}

// A non-zero Git exit is not proof that every command was rejected. Conversely
// no-op and atomic/rejected pushes must not manufacture sync intent.
func TestLocalHookIntentTracksAcceptedCommands(t *testing.T) {
	skipUnlessLocalSchedule(t)
	for _, tc := range []struct {
		name                                   string
		noop, partial, atomic, denied, forward bool
		wantErr, wantIntent                    bool
	}{
		{name: "no_op", noop: true},
		{name: "rejected", denied: true, wantErr: true},
		{name: "partial_cannot_pull", partial: true, denied: true, wantErr: true},
		{name: "partial_unpoliced", partial: true, wantIntent: true},
		{name: "atomic_rejected", partial: true, atomic: true, denied: true, wantErr: true},
		{name: "atomic_accepted", partial: true, atomic: true, wantIntent: true},
		{name: "partial_cannot_forward_or_pull", partial: true, denied: true, forward: true, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			url := "https://example.com/local-intent-" + tc.name + ".git"
			id := registerRepo(t, url)
			work := initWorkRepo(t)
			head := revParse(t, work, "HEAD")
			rw := mintTestToken(t, url, "git:read,git:write", "")
			if tc.noop {
				mustGit(t, work, "push", remoteURL(nodeA, id, rw), "HEAD:refs/heads/main")
			}
			if _, err := testMeta.SetRepoUpstream(t.Context(), meta.RepoUpstream{
				RepoID: id, Provider: "github", Owner: "acme", Name: "demo", UpstreamHost: "http://127.0.0.1:1", ForwardOnPush: tc.forward,
			}); err != nil {
				t.Fatal(err)
			}
			token := rw
			if tc.denied {
				token = mintTestToken(t, url, "git:read,git:write", `[["refs/heads/aa-denied",["no-push"]],["*",[]]]`)
			}
			args := []string{"push"}
			if tc.atomic {
				args = append(args, "--atomic")
			}
			args = append(args, remoteURL(nodeA, id, token))
			if tc.noop {
				args = append(args, "HEAD:refs/heads/main")
			} else {
				args = append(args, "HEAD:refs/heads/aa-denied")
				if tc.partial {
					args = append(args, "HEAD:refs/heads/zz-allowed")
				}
			}
			out, err := tryGit(work, args...)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Git err=%v, wantErr=%v: %s", err, tc.wantErr, out)
			}
			refs := pgRefs(t, id)
			if tc.noop {
				if len(refs) != 1 || refs["refs/heads/main"] != head {
					t.Fatalf("no-op changed authority: %v", refs)
				}
			} else if tc.atomic && tc.denied || !tc.partial && tc.denied {
				if len(refs) != 0 {
					t.Fatalf("rejected push landed authority: %v", refs)
				}
			} else {
				if refs["refs/heads/zz-allowed"] != head {
					t.Fatalf("allowed update missing: %v", refs)
				}
				if tc.denied && refs["refs/heads/aa-denied"] != "" {
					t.Fatalf("denied update landed: %v", refs)
				}
			}
			up, err := testMeta.GetRepoUpstream(t.Context(), id)
			if err != nil {
				t.Fatal(err)
			}
			intent := up.SyncPending || up.Syncing || up.RunCount > 0 || up.PushPending > 0 || up.PushSyncing || up.PushRunCount > 0
			if intent != tc.wantIntent {
				t.Fatalf("intent=%v, want=%v after accepted refs %v: %+v", intent, tc.wantIntent, refs, up)
			}
		})
	}
}

func TestLocalHookCommittedDeleteThen413KeepsIntent(t *testing.T) {
	skipUnlessLocalSchedule(t)
	node := startNodeT(t, fmt.Sprintf("OPENSTORAGE_MAX_PUSH_BYTES=%d", testPushCap))
	url := "https://example.com/local-hook-413.git"
	id := registerRepo(t, url)
	token := mintTestToken(t, url, "git:read,git:write", "")
	work := initWorkRepo(t)
	head := revParse(t, work, "HEAD")
	mustGit(t, work, "push", remoteURL(node, id, token), "HEAD:refs/heads/main", "HEAD:refs/heads/disposable")
	if _, err := testMeta.SetRepoUpstream(t.Context(), meta.RepoUpstream{
		RepoID: id, Provider: "github", Owner: "acme", Name: "demo", UpstreamHost: "http://127.0.0.1:1", ForwardOnPush: true,
	}); err != nil {
		t.Fatal(err)
	}
	command := head + " " + ref.ZeroSHA + " refs/heads/disposable\x00report-status"
	body := append([]byte(pktLine(command)+"0000"), shallowFlood(3*testPushCap)...)
	status, reply := postReceivePack(t, node, id, token, io.MultiReader(bytes.NewReader(body)), false)
	assertCapEnvelope(t, status, reply)
	if refs := pgRefs(t, id); len(refs) != 1 || refs["refs/heads/main"] != head {
		t.Fatalf("disposable branch delete did not commit before 413: %v", refs)
	}
	up, err := testMeta.GetRepoUpstream(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if up.PushPending == 0 && !up.PushSyncing && up.PushRunCount == 0 {
		t.Fatal("413 erased the accepted delete's scheduling intent")
	}
}
