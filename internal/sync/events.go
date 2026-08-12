package sync

import (
	"encoding/json"
	"time"
)

// Sync lifecycle event types (docs/06, X-Pierre-Event values).
const (
	EventSyncStarted   = "repo.sync.started"
	EventSyncSucceeded = "repo.sync.succeeded"
	EventSyncFailed    = "repo.sync.failed"
)

// Exact documented repo.sync.failed error strings (docs/06 — receivers may
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
)

// Event is the repo.sync.* webhook payload (docs/upstream/guides/webhooks.md
// field tables). CompletedAt/Error are only present on terminal events.
type Event struct {
	Type        string          `json:"type"`
	Repository  EventRepository `json:"repository"`
	RunCount    int             `json:"run_count"`
	IsFirstSync bool            `json:"is_first_sync"`
	StartedAt   string          `json:"started_at"`
	CompletedAt string          `json:"completed_at,omitempty"`
	Error       string          `json:"error,omitempty"`
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
	assertf(e.Type == EventSyncStarted || e.Type == EventSyncSucceeded || e.Type == EventSyncFailed,
		"marshal: bad event type %q", e.Type)
	assertf(e.Repository.ID != "" && e.Repository.URL != "", "marshal: incomplete repository (%+v)", e.Repository)
	assertf(e.StartedAt != "", "marshal: missing started_at")
	assertf((e.Type == EventSyncFailed) == (e.Error != ""), "marshal: error field mismatch for %s", e.Type)
	assertf((e.Type != EventSyncStarted) == (e.CompletedAt != ""), "marshal: completed_at mismatch for %s", e.Type)
	buf, err := json.Marshal(e)
	assertf(err == nil, "marshal event: %v", err)
	return buf
}
