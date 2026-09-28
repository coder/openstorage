package sync

import (
	"context"
	"encoding/json"
	"testing"
)

// upstreamSyncEventProps are the property names of upstream's
// GitSyncStartedEvent / GitSyncSucceededEvent / GitSyncFailedEvent schemas
// (docs/upstream/openapi.yaml, 2026-09-28 snapshot). All three set
// additionalProperties: false, so a repo.sync.* body may carry no other key.
var upstreamSyncEventProps = map[string]bool{
	"type": true, "repository": true, "run_count": true, "is_first_sync": true,
	"started_at": true, "completed_at": true, "error": true, "lifecycle_id": true,
}

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
}

// TestInboundSyncEventsMatchUpstreamSchema pins the inbound repo.sync.*
// bodies to upstream's schema key set (issue #354).
func TestInboundSyncEventsMatchUpstreamSchema(t *testing.T) {
	base := Event{
		Repository: EventRepository{ID: "repo_x", URL: "acme/x"},
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
	}
}

// TestSyncEventsRefuseOutboundFields: the outbound-only fields cannot ride a
// repo.sync.* event (they would break upstream's additionalProperties:
// false), so marshal treats that as a programming error.
func TestSyncEventsRefuseOutboundFields(t *testing.T) {
	for _, typ := range []string{EventSyncStarted, EventSyncSucceeded, EventSyncFailed} {
		e := Event{
			Type:       typ,
			Repository: EventRepository{ID: "repo_x", URL: "acme/x"},
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
