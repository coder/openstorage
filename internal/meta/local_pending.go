package meta

import (
	"context"
	"fmt"
	"slices"
)

// LocalPendingStore is the local serve process's live observation query. It
// does not repair claims or create intent. Unlike PendingPushRepoIDs (boot
// recovery), an active claim alone is never work. Workers must revalidate
// observations under local admission coordination before claiming them.
type LocalPendingStore interface {
	// ListPendingSyncWork returns at most limit rows, ordered by RepoID,
	// strictly after after. An empty page restarts the next traversal.
	ListPendingSyncWork(ctx context.Context, after string, limit int) ([]PendingSyncWork, error)
}

// PendingSyncWork describes durable markers, not fresh caller requests.
type PendingSyncWork struct {
	RepoID string
	Pull   bool
	Push   bool
}

var (
	_ LocalPendingStore = (*SQLite)(nil)
	_ LocalPendingStore = (*S3WAL)(nil)
)

func (s *SQLite) ListPendingSyncWork(ctx context.Context, after string, limit int) ([]PendingSyncWork, error) {
	assertf(limit > 0, "ListPendingSyncWork: non-positive limit %d", limit)
	rows, err := s.db.QueryContext(ctx, `SELECT repo_id, sync_pending, push_pending > 0
		FROM repo_upstreams WHERE repo_id > ? AND (sync_pending = 1 OR push_pending > 0)
		ORDER BY repo_id LIMIT ?`, after, limit)
	if err != nil {
		return nil, fmt.Errorf("list local pending work: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var work []PendingSyncWork
	for rows.Next() {
		var row PendingSyncWork
		if err := rows.Scan(&row.RepoID, &row.Pull, &row.Push); err != nil {
			return nil, fmt.Errorf("list local pending work: %w", err)
		}
		work = append(work, row)
	}
	return work, rows.Err()
}

func (s *S3WAL) ListPendingSyncWork(ctx context.Context, after string, limit int) ([]PendingSyncWork, error) {
	assertf(limit > 0, "ListPendingSyncWork: non-positive limit %d", limit)
	// The existing metadata format requires reading the whole document. The
	// result and enqueue batch are bounded, not the underlying document I/O.
	doc, err := s.viewDoc(ctx)
	if err != nil {
		return nil, err
	}
	var ids []string
	for id, up := range doc.Upstreams {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if id > after && (up.SyncPending || up.PushPending > 0) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	var work []PendingSyncWork
	for _, id := range ids[:min(limit, len(ids))] {
		up := doc.Upstreams[id]
		work = append(work, PendingSyncWork{RepoID: id, Pull: up.SyncPending, Push: up.PushPending > 0})
	}
	return work, nil
}
