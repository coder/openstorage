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
	// ListPendingSyncWork returns at most limit rows of org's live
	// (not soft-deleted) repos, ordered by RepoID, strictly after after. An
	// empty page restarts the next traversal; an unknown org has no work.
	ListPendingSyncWork(ctx context.Context, org, after string, limit int) ([]PendingSyncWork, error)
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

func (s *SQLite) ListPendingSyncWork(ctx context.Context, org, after string, limit int) ([]PendingSyncWork, error) {
	assertf(org != "" && limit > 0, "ListPendingSyncWork: empty org %q or non-positive limit %d", org, limit)
	// Other orgs' and soft-deleted repos' markers are not this process's
	// work: the worker rejects them without clearing them. Filter them in
	// SQL, before LIMIT, so they neither recur nor shorten a page.
	rows, err := s.db.QueryContext(ctx, `SELECT u.repo_id, u.sync_pending, u.push_pending > 0
		FROM repo_upstreams u
		JOIN repos r ON r.repo_id = u.repo_id AND r.deleted_at IS NULL
		JOIN orgs o ON o.id = r.org_id AND o.name = ?
		WHERE u.repo_id > ? AND (u.sync_pending = 1 OR u.push_pending > 0)
		ORDER BY u.repo_id LIMIT ?`, org, after, limit)
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

func (s *S3WAL) ListPendingSyncWork(ctx context.Context, org, after string, limit int) ([]PendingSyncWork, error) {
	assertf(org != "" && limit > 0, "ListPendingSyncWork: empty org %q or non-positive limit %d", org, limit)
	// The existing metadata format requires reading the whole document. The
	// result and enqueue batch are bounded, not the underlying document I/O.
	doc, err := s.viewDoc(ctx)
	if err != nil {
		return nil, err
	}
	orgRow := doc.Orgs[org]
	if orgRow == nil {
		return nil, nil
	}
	var ids []string
	for id, up := range doc.Upstreams {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// Same live served-org filter as SQLite, applied before the limit.
		r := doc.Repos[id]
		if r == nil || r.OrgID != orgRow.ID || r.DeletedAt != 0 {
			continue
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
