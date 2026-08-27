package ref

import (
	"context"
	"database/sql"
	"errors"
)

// DFConflicts implements the batched authority D/F probe for SQLite: one
// chunked indexed membership read over the candidates' deduped strict
// prefixes plus one local single-row indexed range seek per candidate —
// index work proportional to the candidate set, never to the repository
// (SQLite's default BINARY collation already compares by byte order).
func (s *SQLite) DFConflicts(ctx context.Context, repo string, names []string) (map[string]string, error) {
	assertf(repo != "", "DFConflicts: empty repo")
	if len(names) == 0 {
		return map[string]string{}, nil
	}
	return dfConflictsChunked(names, func(chunk []string) (map[string]string, error) {
		return s.dfConflictsChunk(ctx, repo, chunk)
	})
}

func (s *SQLite) dfConflictsChunk(ctx context.Context, repo string, names []string) (map[string]string, error) {
	out := map[string]string{}
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
	const chunk = 500 // stay well under SQLite's bound-parameter limit
	for start := 0; start < len(prefixes); start += chunk {
		end := min(start+chunk, len(prefixes))
		args := make([]any, 0, end-start+1)
		args = append(args, repo)
		q := `SELECT name FROM refs WHERE repo = ? AND name IN (`
		for i := start; i < end; i++ {
			if i > start {
				q += ","
			}
			q += "?"
			args = append(args, prefixes[i])
		}
		q += ")"
		rows, err := s.db.QueryContext(ctx, q, args...)
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
	stmt, err := s.db.PrepareContext(ctx,
		`SELECT name FROM refs WHERE repo = ? AND name > ? AND name < ? LIMIT 1`)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	for _, name := range names {
		if _, ok := out[name]; ok {
			continue
		}
		var witness string
		switch err := stmt.QueryRowContext(ctx, repo, name+"/", name+"0").Scan(&witness); {
		case err == nil:
			out[name] = witness
		case errors.Is(err, sql.ErrNoRows):
			// no descendants
		default:
			return nil, err
		}
	}
	return out, nil
}
