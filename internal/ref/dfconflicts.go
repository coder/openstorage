package ref

import (
	"context"
	"sort"
	"strings"
)

// DFConflicts (implemented by every production RefStore) returns, for each
// candidate name, one existing ref of repo that is a strict directory/file
// relative of it — a strict prefix directory ("refs/heads/a" for candidate
// "refs/heads/a/b") or any ref strictly under the candidate
// ("refs/heads/x/y" for candidate "refs/heads/x") — omitting candidates
// with no conflict. An existing ref EQUAL to a candidate is not a D/F
// conflict.
//
// The contract is batched and push-proportional (issue #170 push holds:
// the caller probes while its namespace holds block conflicting same-node
// writers, so neither per-candidate full-table materialization nor
// repo-sized scans are acceptable): PG answers in two round trips of
// indexed seeks, SQLite in one call of local indexed seeks, and s3wal from
// the single manifest read every access pays anyway.

// dfStrictPrefixes calls fn for each strict '/'-separated prefix of name,
// shortest first, stopping early when fn returns false.
func dfStrictPrefixes(name string, fn func(prefix string) bool) {
	for i := 0; i < len(name); i++ {
		if name[i] == '/' && !fn(name[:i]) {
			return
		}
	}
}

// dfConflictsFromSorted computes the DFConflicts result from a sorted
// (ascending byte order) snapshot of the repo's ref names.
func dfConflictsFromSorted(existing, names []string) map[string]string {
	out := map[string]string{}
	for _, name := range names {
		dfStrictPrefixes(name, func(anc string) bool {
			if j := sort.SearchStrings(existing, anc); j < len(existing) && existing[j] == anc {
				out[name] = anc
				return false
			}
			return true
		})
		if _, ok := out[name]; ok {
			continue
		}
		lo := name + "/"
		if j := sort.SearchStrings(existing, lo); j < len(existing) && strings.HasPrefix(existing[j], lo) {
			out[name] = existing[j]
		}
	}
	return out
}

// DFConflicts implements the batched authority D/F probe for PostgreSQL:
// one indexed membership read over the candidates' deduped strict prefixes
// plus one LATERAL single-row range seek per candidate — two round trips,
// index work proportional to the candidate set, never to the repository.
func (s *PG) DFConflicts(ctx context.Context, repo string, names []string) (map[string]string, error) {
	assertf(repo != "", "DFConflicts: empty repo")
	out := map[string]string{}
	if len(names) == 0 {
		return out, nil
	}
	seen := map[string]bool{}
	var prefixes []string
	for _, name := range names {
		dfStrictPrefixes(name, func(anc string) bool {
			if !seen[anc] {
				seen[anc] = true
				prefixes = append(prefixes, anc)
			}
			return true
		})
	}
	existingPrefix := map[string]bool{}
	if len(prefixes) > 0 {
		rows, err := s.pool.Query(ctx,
			`SELECT name FROM refs WHERE repo = $1 AND name = ANY($2)`, repo, prefixes)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				rows.Close()
				return nil, err
			}
			existingPrefix[name] = true
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	for _, name := range names {
		dfStrictPrefixes(name, func(anc string) bool {
			if existingPrefix[anc] {
				out[name] = anc
				return false
			}
			return true
		})
	}
	rows, err := s.pool.Query(ctx, `
		SELECT c.name, w.name FROM unnest($2::text[]) AS c(name)
		JOIN LATERAL (
			SELECT name FROM refs
			WHERE repo = $1
			  AND name COLLATE "C" > c.name || '/'
			  AND name COLLATE "C" < c.name || '0'
			LIMIT 1
		) w ON true`, repo, names)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var cand, witness string
		if err := rows.Scan(&cand, &witness); err != nil {
			return nil, err
		}
		if _, ok := out[cand]; !ok {
			out[cand] = witness
		}
	}
	return out, rows.Err()
}

// DFConflicts implements the batched authority D/F probe for s3wal from
// ONE manifest read (the price every access on this backend pays), with
// both arms joined in memory against the sorted name set.
func (s *S3WAL) DFConflicts(ctx context.Context, repo string, names []string) (map[string]string, error) {
	assertf(repo != "", "DFConflicts: empty repo")
	if len(names) == 0 {
		return map[string]string{}, nil
	}
	m, err := s.viewManifest(ctx, baseRepoOf(repo))
	if err != nil {
		return nil, err
	}
	mem := m.Member(repo)
	if mem == nil {
		return map[string]string{}, nil
	}
	existing := make([]string, 0, len(mem.Refs))
	for name := range mem.Refs {
		existing = append(existing, name)
	}
	sort.Strings(existing)
	return dfConflictsFromSorted(existing, names), nil
}
