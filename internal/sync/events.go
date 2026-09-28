package sync

import (
	"encoding/json"
	"time"
)

// Sync lifecycle event types (docs/reference/webhooks.md, X-Pierre-Event values).
// Inbound runs only: upstream's schemas for these set additionalProperties:
// false, so their bodies never carry the outbound fields below.
const (
	EventSyncStarted   = "repo.sync.started"
	EventSyncSucceeded = "repo.sync.succeeded"
	EventSyncFailed    = "repo.sync.failed"
)

// Outbound push-upstream run event types (open.storage extension, issue
// #354). Upstream has no outbound-run event, so these carry the same body as
// the sync events plus direction/forced/rejected_refs under their own names.
const (
	EventPushUpstreamStarted   = "repo.push_upstream.started"
	EventPushUpstreamSucceeded = "repo.push_upstream.succeeded"
	EventPushUpstreamFailed    = "repo.push_upstream.failed"
)

// Exact documented repo.sync.failed error strings (docs/reference/webhooks.md — receivers may
// switch on these verbatim; never reword them).
const (
	FailAuth        = "authentication failed"
	FailClone       = "failed to clone repository"
	FailPush        = "failed to push to storage"
	FailConfig      = "repository configuration error"
	FailUnreachable = "upstream repository unreachable"
	FailInternal    = "internal error"
	FailSession     = "session unavailable"
	FailGeneric     = "sync failed"
	// Outbound push-upstream failure strings (issue #90, direction
	// "push"). FailRejected carries per-ref reasons in rejected_refs.
	FailForward  = "failed to push to upstream"
	FailRejected = "push rejected by upstream"
)

// DirectionPush marks outbound (local -> upstream) runs in event payloads
// (issue #90). Inbound runs omit the field entirely so pre-#90 pull event
// bodies stay byte-identical on the wire. Since issue #354 the event type
// already tells the directions apart; the field stays so receivers that
// checked it keep working.
const DirectionPush = "push"

// Event is the repo.sync.* and repo.push_upstream.* webhook payload
// (docs/upstream/guides/webhooks.md field tables). CompletedAt/Error are only
// present on terminal events.
type Event struct {
	Type        string          `json:"type"`
	Repository  EventRepository `json:"repository"`
	RunCount    int             `json:"run_count"`
	IsFirstSync bool            `json:"is_first_sync"`
	StartedAt   string          `json:"started_at"`
	CompletedAt string          `json:"completed_at,omitempty"`
	Error       string          `json:"error,omitempty"`
	// Direction discriminates outbound runs (DirectionPush, issue #90).
	// Absent means inbound pull — the pre-#90 payload shape.
	Direction string `json:"direction,omitempty"`
	// Forced reports that an outbound run used force refspecs.
	Forced bool `json:"forced,omitempty"`
	// RejectedRefs carries per-ref rejection reasons on FailRejected
	// outbound failures (issue #90: a non-fast-forward ref fails with a
	// clear reason instead of silently rewriting upstream history).
	RejectedRefs []RejectedRef `json:"rejected_refs,omitempty"`
}

// RejectedRef is one upstream-rejected ref of an outbound push run.
type RejectedRef struct {
	Ref    string `json:"ref"`
	Reason string `json:"reason"`
}

// EventRepository identifies the synced repo: id = repo_id, url = the JWT
// `repo` claim value.
type EventRepository struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

// eventTime renders timestamps the way the payload examples do (RFC 3339,
// UTC, second precision).
func eventTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

// marshal serializes the event payload. Guarded by asserts because every
// field is produced internally — a malformed event is a programming error.
func (e Event) marshal() []byte {
	inbound := e.Type == EventSyncStarted || e.Type == EventSyncSucceeded || e.Type == EventSyncFailed
	outbound := e.Type == EventPushUpstreamStarted || e.Type == EventPushUpstreamSucceeded || e.Type == EventPushUpstreamFailed
	assertf(inbound || outbound, "marshal: bad event type %q", e.Type)
	started := e.Type == EventSyncStarted || e.Type == EventPushUpstreamStarted
	failed := e.Type == EventSyncFailed || e.Type == EventPushUpstreamFailed
	assertf(e.Repository.ID != "" && e.Repository.URL != "", "marshal: incomplete repository (%+v)", e.Repository)
	assertf(e.StartedAt != "", "marshal: missing started_at")
	assertf(failed == (e.Error != ""), "marshal: error field mismatch for %s", e.Type)
	assertf(!started == (e.CompletedAt != ""), "marshal: completed_at mismatch for %s", e.Type)
	assertf(e.Direction == "" || e.Direction == DirectionPush, "marshal: bad direction %q", e.Direction)
	// The type names the direction; the body field must agree (issue #354).
	assertf(outbound == (e.Direction == DirectionPush), "marshal: direction %q on %s", e.Direction, e.Type)
	assertf(e.Direction == DirectionPush || (!e.Forced && len(e.RejectedRefs) == 0),
		"marshal: push-only fields on a pull event")
	assertf(len(e.RejectedRefs) == 0 || e.Error == FailRejected,
		"marshal: rejected_refs without %q error", FailRejected)
	buf, err := json.Marshal(e)
	assertf(err == nil, "marshal event: %v", err)
	return buf
}
