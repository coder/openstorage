package meta

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestPGLaneWorkMatchesClaimGuards: PGLaneWork and PGListLaneWork call a
// lane work exactly when TryBeginSync or TryBeginPush would claim it with
// a request to serve, or adopt a legacy claim: an unclaimed lane with a
// pending request, or an expired claim. Every holder, expiry and pending
// combination runs with the legacy steal opt-in off and on. Deleted repos,
// repos without a link and other orgs' repos have no lane work.
func TestPGLaneWorkMatchesClaimGuards(t *testing.T) {
	s, ok := pgBackend(t).(*PG)
	if !ok {
		t.Fatal("pgBackend is not *PG")
	}
	ctx := t.Context()
	org := seedOrg(t, s)
	n := 0
	create := func(org string) string {
		t.Helper()
		n++
		repo, err := s.CreateRepo(ctx, org, fmt.Sprintf("https://example.com/lanes-%d.git", n), "main")
		if err != nil {
			t.Fatal(err)
		}
		return repo.RepoID
	}
	linked := func(org string) string {
		t.Helper()
		id := create(org)
		if _, err := s.SetRepoUpstream(ctx, RepoUpstream{RepoID: id, Provider: "gitea", Owner: "o", Name: "n", UpstreamHost: "example.test"}); err != nil {
			t.Fatal(err)
		}
		return id
	}
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := s.pool.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	laneWork := func(id string) LocalWork {
		t.Helper()
		var w LocalWork
		if err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
			var err error
			w, err = PGLaneWork(ctx, tx, id)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return w
	}
	listed := func(org string) map[string]LocalWork {
		t.Helper()
		got := map[string]LocalWork{}
		if err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
			after := ""
			for {
				page, err := PGListLaneWork(ctx, tx, org, after, 7)
				if err != nil {
					return err
				}
				for _, w := range page {
					got[w.RepoID] = w
					after = w.RepoID
				}
				if len(page) < 7 {
					return nil
				}
			}
		}); err != nil {
			t.Fatal(err)
		}
		return got
	}
	expiries := map[string]string{"epoch": "'epoch'", "past": "now() - interval '2 minutes'", "future": "now() + interval '1 hour'"}
	holders := map[string]string{"empty": "", "legacy": LegacyClaimHolder, "other": "h-dead"}

	for _, steal := range []bool{false, true} {
		t.Setenv(EnvSyncStealLegacy, strconv.FormatBool(steal))
		type pull struct {
			desc string
			want bool
		}
		pulls := map[string]pull{}
		for _, syncing := range []bool{false, true} {
			for _, pending := range []bool{false, true} {
				for hn, h := range holders {
					for en, ex := range expiries {
						id := linked(org)
						exec(`UPDATE repo_upstreams SET syncing = $2, sync_pending = $3, sync_claim_holder = $4,
							sync_claim_expires_at = `+ex+` WHERE repo_id = $1`, id, syncing, pending, h)
						pulls[id] = pull{desc: fmt.Sprintf("steal=%v syncing=%v pending=%v holder=%s expiry=%s", steal, syncing, pending, hn, en)}
					}
				}
			}
		}
		got, list := map[string]bool{}, listed(org)
		for id := range pulls {
			got[id] = laneWork(id).Pull
		}
		for id, p := range pulls {
			before, err := s.GetRepoUpstream(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			_, claimed, _, err := s.TryBeginSync(ctx, id, "probe", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			after, err := s.GetRepoUpstream(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			adopted := before.Syncing && before.SyncClaimHolder == "" && after.SyncClaimHolder == LegacyClaimHolder
			p.want = (before.Syncing && (claimed || adopted)) || (!before.Syncing && before.SyncPending)
			if got[id] != p.want || list[id].Pull != p.want {
				t.Errorf("pull %s: PGLaneWork %v, listed %v; TryBeginSync claimed %v adopted %v, want %v",
					p.desc, got[id], list[id].Pull, claimed, adopted, p.want)
			}
		}
	}

	pushes := map[string]string{}
	for _, syncing := range []bool{false, true} {
		for _, pending := range []int{0, 1, 2} {
			for en, ex := range expiries {
				id := linked(org)
				exec(`UPDATE repo_upstreams SET push_syncing = $2, push_pending = $3, push_claim_holder = 'h-dead',
					push_claim_expires_at = `+ex+` WHERE repo_id = $1`, id, syncing, pending)
				pushes[id] = fmt.Sprintf("syncing=%v pending=%d expiry=%s", syncing, pending, en)
			}
		}
	}
	list := listed(org)
	for id, desc := range pushes {
		got := laneWork(id).Push
		before, err := s.GetRepoUpstream(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		_, claimed, _, _, err := s.TryBeginPush(ctx, id, "probe", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		want := (before.PushSyncing && claimed) || (!before.PushSyncing && before.PushPending > 0)
		if got != want || list[id].Push != want {
			t.Errorf("push %s: PGLaneWork %v, listed %v; TryBeginPush claimed %v, want %v", desc, got, list[id].Push, claimed, want)
		}
	}

	// Pending work that is not the served org's live, linked work.
	deleted := linked(org)
	exec(`UPDATE repo_upstreams SET push_pending = 1, sync_pending = true WHERE repo_id = $1`, deleted)
	exec(`UPDATE repos SET deleted_at = now() WHERE repo_id = $1`, deleted)
	other := org + "-other"
	if _, err := s.UpsertOrgWithKeys(ctx, other, []string{"-----BEGIN PUBLIC KEY-----\ndummy\n-----END PUBLIC KEY-----\n"}); err != nil {
		t.Fatal(err)
	}
	foreign := linked(other)
	exec(`UPDATE repo_upstreams SET push_pending = 1 WHERE repo_id = $1`, foreign)
	unlinked := create(org)
	list = listed(org)
	for name, id := range map[string]string{"deleted": deleted, "unlinked": unlinked} {
		if w := laneWork(id); w.Push || w.Pull {
			t.Errorf("%s repo: PGLaneWork %+v, want no lane work", name, w)
		}
	}
	for name, id := range map[string]string{"deleted": deleted, "other org": foreign} {
		if w, ok := list[id]; ok {
			t.Errorf("%s repo listed for %s: %+v", name, org, w)
		}
	}
	if w := listed(other)[foreign]; !w.Push {
		t.Errorf("the other org's own listing lacks its push lane: %+v", w)
	}
}
