package sync

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// upstreamSyncEventProps are the property names of upstream's
// GitSyncStartedEvent / GitSyncSucceededEvent / GitSyncFailedEvent schemas
// (docs/upstream/openapi.yaml, 2026-09-29 20:33:38 GMT snapshot). All three
// set additionalProperties: false, so a repo.sync.* body may carry no other
// key.
var upstreamSyncEventProps = map[string]bool{
	"type": true, "repository": true, "run_count": true, "is_first_sync": true,
	"started_at": true, "completed_at": true, "error": true, "lifecycle_id": true,
	"org": true,
}

// upstreamWebhookRepositoryProps are the property names of upstream's
// WebhookRepository schema (same snapshot, additionalProperties: false).
var upstreamWebhookRepositoryProps = map[string]bool{"id": true, "url": true, "repo_name": true}

// upstreamSyncEventRequired is those schemas' `required` list.
var upstreamSyncEventRequired = []string{"type", "repository", "run_count", "is_first_sync", "started_at"}

// assertUpstreamSyncShape fails unless body matches the upstream sync event
// schemas' key set: every required key present, no key outside the schema.
func assertUpstreamSyncShape(t *testing.T, body []byte) {
	t.Helper()
	var got map[string]json.RawMessage
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	for k := range got {
		if !upstreamSyncEventProps[k] {
			t.Errorf("repo.sync.* body carries %q, which upstream's schema forbids: %s", k, body)
		}
	}
	for _, k := range upstreamSyncEventRequired {
		if _, ok := got[k]; !ok {
			t.Errorf("repo.sync.* body lacks required %q: %s", k, body)
		}
	}
	var repo map[string]json.RawMessage
	if err := json.Unmarshal(got["repository"], &repo); err != nil {
		t.Fatalf("repo.sync.* body repository: %v: %s", err, body)
	}
	for k := range repo {
		if !upstreamWebhookRepositoryProps[k] {
			t.Errorf("repo.sync.* repository carries %q, which upstream's schema forbids: %s", k, body)
		}
	}
}

// assertOrgAndRepoName fails unless body carries org (the served org name)
// and repository.repo_name equal to repository.url (issue #367). Upstream
// marks both optional; open.storage always sends them.
func assertOrgAndRepoName(t *testing.T, body []byte, org string) {
	t.Helper()
	var got struct {
		Org        *string `json:"org"`
		Repository struct {
			URL      string  `json:"url"`
			RepoName *string `json:"repo_name"`
		} `json:"repository"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Org == nil || *got.Org != org {
		t.Errorf("body org = %v, want %q: %s", got.Org, org, body)
	}
	if got.Repository.RepoName == nil || *got.Repository.RepoName != got.Repository.URL || got.Repository.URL == "" {
		t.Errorf("body repository.repo_name = %v, want url %q: %s", got.Repository.RepoName, got.Repository.URL, body)
	}
}

// TestInboundSyncEventsMatchUpstreamSchema pins the inbound repo.sync.*
// bodies to upstream's schema key set (issue #354).
func TestInboundSyncEventsMatchUpstreamSchema(t *testing.T) {
	base := Event{
		Org:        "acme",
		Repository: EventRepository{ID: "repo_x", URL: "acme/x", RepoName: "acme/x"},
		StartedAt:  "2026-09-28T05:00:00Z",
	}
	started := base
	started.Type = EventSyncStarted
	succeeded := base
	succeeded.Type, succeeded.RunCount, succeeded.CompletedAt = EventSyncSucceeded, 1, "2026-09-28T05:00:01Z"
	failed := succeeded
	failed.Type, failed.Error = EventSyncFailed, FailClone
	for _, e := range []Event{started, succeeded, failed} {
		assertUpstreamSyncShape(t, e.marshal())
		assertOrgAndRepoName(t, e.marshal(), "acme")
	}
}

// TestRunEventsCarryOrgAndRepoName drives real inbound (success and
// failure) and outbound runs and checks every emitted body: org is the
// job's org (the served org, the only accepted JWT iss) and
// repository.repo_name equals repository.url (issue #367). Inbound bodies
// must still match upstream's schema key set.
func TestRunEventsCarryOrgAndRepoName(t *testing.T) {
	f := newWorkerFixture(t)
	seedUpstream(t, f.upstreamDir, "own", "proj")
	f.addLoopbackRepo(t, "repo_ok")
	f.putUpstream(t, "repo_ok", "own", "proj", "file://"+f.upstreamDir)
	if err := f.worker.Run(context.Background(), Job{Org: "acme", RepoID: "repo_ok"}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// No loopback bare repo: the inbound run fails at the loopback push.
	f.putUpstream(t, "repo_bad", "own", "proj", "file://"+f.upstreamDir)
	_ = f.worker.Run(context.Background(), Job{Org: "acme", RepoID: "repo_bad"})
	seedLocalRepo(t, f, "repo_out")
	addEmptyUpstream(t, f.upstreamDir, "own", "out")
	f.putUpstream(t, "repo_out", "own", "out", "file://"+f.upstreamDir)
	if err := f.worker.RunPush(context.Background(), Job{Org: "acme", RepoID: "repo_out"}); err != nil {
		t.Fatalf("RunPush: %v", err)
	}

	raws := f.rec.rawList()
	var types []string
	for _, r := range raws {
		types = append(types, r.eventType)
		if r.org != "acme" {
			t.Errorf("%s delivered for org %q, want acme", r.eventType, r.org)
		}
		assertOrgAndRepoName(t, r.body, "acme")
		if r.eventType == EventSyncStarted || r.eventType == EventSyncSucceeded || r.eventType == EventSyncFailed {
			assertUpstreamSyncShape(t, r.body)
		}
	}
	want := []string{EventSyncStarted, EventSyncSucceeded, EventSyncStarted, EventSyncFailed,
		EventPushUpstreamStarted, EventPushUpstreamSucceeded}
	if strings.Join(types, ",") != strings.Join(want, ",") {
		t.Fatalf("event types = %v, want %v", types, want)
	}
}

// TestSyncEventsRefuseOutboundFields: the outbound-only fields cannot ride a
// repo.sync.* event (they would break upstream's additionalProperties:
// false), so marshal treats that as a programming error.
func TestSyncEventsRefuseOutboundFields(t *testing.T) {
	for _, typ := range []string{EventSyncStarted, EventSyncSucceeded, EventSyncFailed} {
		e := Event{
			Type:       typ,
			Org:        "acme",
			Repository: EventRepository{ID: "repo_x", URL: "acme/x", RepoName: "acme/x"},
			StartedAt:  "2026-09-28T05:00:00Z",
			Direction:  DirectionPush,
		}
		if typ != EventSyncStarted {
			e.CompletedAt = "2026-09-28T05:00:01Z"
		}
		if typ == EventSyncFailed {
			e.Error = FailForward
		}
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("marshal accepted direction=push on %s", typ)
				}
			}()
			e.marshal()
		}()
	}
}

// TestPushRunEmitsPushUpstreamEvents: outbound runs use their own event
// types, in both the X-Pierre-Event header and the body type (eventRec
// asserts they match), and keep the outbound body fields (issue #354).
func TestPushRunEmitsPushUpstreamEvents(t *testing.T) {
	f := newWorkerFixture(t)
	seedLocalRepo(t, f, "repo_pe")
	addEmptyUpstream(t, f.upstreamDir, "own", "proj")
	f.putUpstream(t, "repo_pe", "own", "proj", "file://"+f.upstreamDir)
	if err := f.worker.RunPush(context.Background(), Job{Org: "acme", RepoID: "repo_pe"}); err != nil {
		t.Fatalf("RunPush: %v", err)
	}
	events := f.rec.list()
	assertEventTypes(t, events, "repo.push_upstream.started", "repo.push_upstream.succeeded")
	for _, e := range events {
		if e.Direction != DirectionPush {
			t.Fatalf("outbound event lost direction: %+v", e)
		}
	}
}
