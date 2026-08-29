package ref

import (
	"context"
	"strings"
)

// RefValues implements the batched exact-value probe for SQLite: chunked
// indexed membership reads over the candidate names (see refvalues.go for
// the contract; the chunking mirrors DFConflicts' parameter-limit bound).
func (s *SQLite) RefValues(ctx context.Context, repo string, names []string) (map[string]string, error) {
	assertf(repo != "", "RefValues: empty repo")
	out := map[string]string{}
	if len(names) == 0 {
		return out, nil
	}
	const chunk = 500 // stay well under SQLite's bound-parameter limit
	for start := 0; start < len(names); start += chunk {
		end := min(start+chunk, len(names))
		args := make([]any, 0, end-start+1)
		args = append(args, repo)
		var q strings.Builder
		q.WriteString(`SELECT name, sha FROM refs WHERE repo = ? AND name IN (`)
		for i := start; i < end; i++ {
			if i > start {
				q.WriteString(",")
			}
			q.WriteString("?")
			args = append(args, names[i])
		}
		q.WriteString(")")
		rows, err := s.db.QueryContext(ctx, q.String(), args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var name, sha string
			if err := rows.Scan(&name, &sha); err != nil {
				rows.Close()
				return nil, err
			}
			out[name] = sha
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return out, nil
}
