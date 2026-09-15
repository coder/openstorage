package githttp

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref"
)

// The external serve binary is essential: in-process fixtures dispatch hooks
// through this package's TestMain, which does not wire command-side eventing.
func TestLocalHookAcceptedPushRecordsIntent(t *testing.T) {
	if testBackend == backendPG {
		t.Skip("local hook scheduling contract")
	}
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

// A non-zero Git exit is not proof that every command was rejected. Conversely
// no-op and atomic/rejected pushes must not manufacture sync intent.
func TestLocalHookIntentTracksAcceptedCommands(t *testing.T) {
	if testBackend == backendPG {
		t.Skip("local hook scheduling contract")
	}
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
	if testBackend == backendPG {
		t.Skip("local hook scheduling contract")
	}
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
