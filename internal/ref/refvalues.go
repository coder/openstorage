package ref

import "context"

// RefValues (implemented by every production RefStore) returns the current
// sha of each named ref of repo, omitting names that do not exist. It is
// the batched form of GetRef for the githttp atomic-push base check (issue
// #170 push holds): an atomic push must verify every command's exact
// authority base AFTER its reservation froze same-node writers and BEFORE
// any update hook's CAS can commit, so the probe must be one
// push-proportional batch — per-name GetRef would pay a manifest fetch per
// command on s3wal, and repo-scale reads are off the table while the push
// holds its namespaces.
//
// PG and SQLite answer in bounded chunks of indexed membership reads, and
// s3wal from the single manifest read every access pays anyway.

// RefValues implements the batched exact-value probe for PostgreSQL:
// indexed membership reads in bounded dfProbeChunk-sized batches (Codex
// round-21 P2 on #173: the push cap admits pushes of hundreds of
// thousands of commands, and encoding ALL their names as one array
// parameter sends one correspondingly oversized query to the shared
// database while the push holds those refs — the same reason the D/F
// probe chunks).
func (s *PG) RefValues(ctx context.Context, repo string, names []string) (map[string]string, error) {
	assertf(repo != "", "RefValues: empty repo")
	out := map[string]string{}
	for start := 0; start < len(names); start += dfProbeChunk {
		chunk := names[start:min(start+dfProbeChunk, len(names))]
		if err := s.refValuesChunk(ctx, repo, chunk, out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (s *PG) refValuesChunk(ctx context.Context, repo string, names []string, out map[string]string) error {
	rows, err := s.pool.Query(ctx,
		`SELECT name, sha FROM refs WHERE repo = $1 AND name = ANY($2)`, repo, names)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name, sha string
		if err := rows.Scan(&name, &sha); err != nil {
			return err
		}
		out[name] = sha
	}
	return rows.Err()
}

// RefValues implements the batched exact-value probe for s3wal from ONE
// manifest read (the price every access on this backend pays), joined in
// memory against the member's ref map.
func (s *S3WAL) RefValues(ctx context.Context, repo string, names []string) (map[string]string, error) {
	assertf(repo != "", "RefValues: empty repo")
	out := map[string]string{}
	if len(names) == 0 {
		return out, nil
	}
	m, err := s.viewManifest(ctx, baseRepoOf(repo))
	if err != nil {
		return nil, err
	}
	mem := m.Member(repo)
	if mem == nil {
		return out, nil
	}
	for _, name := range names {
		if r, ok := mem.Refs[name]; ok {
			assertf(ValidSHA(r.SHA), "manifest holds malformed sha %q for %s/%s", r.SHA, repo, name)
			out[name] = r.SHA
		}
	}
	return out, nil
}
