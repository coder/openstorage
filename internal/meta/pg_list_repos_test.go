package meta

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/jackc/pgx/v5"
)

// TestPGListReposLowersFilterOnce pins that the ListRepos url filter
// lowers q and prefix once per query, not once per row. `url ILIKE p`
// lowered p for every row it tested, so a 200 KiB q over 3,100 repos held
// a PostgreSQL backend for seconds. The generic plan is the one that
// pgx's statement cache can run: its per-row filter must not read p, an
// InitPlan must lower it once, and the filter must be LIKE (`~~`) on
// lower(url). ILIKE (`~~*`) lowers its pattern again for each row, even
// when an InitPlan computes that pattern.
func TestPGListReposLowersFilterOnce(t *testing.T) {
	s, ok := pgBackend(t).(*PG)
	if !ok {
		t.Fatal("pgBackend is not *PG")
	}
	ctx := t.Context()
	params := ListParams{Q: "sdk", Prefix: "pierre/", Cursor: encodeCursor(time.Unix(1, 0), "r"), Limit: 10}
	sql, args, err := pgListReposQuery(1, params)
	if err != nil {
		t.Fatal(err)
	}
	i := slices.Index(args, any(listURLPattern(params)))
	if i < 0 {
		t.Fatalf("no argument is the pattern %q: %v", listURLPattern(params), args)
	}
	pattern := regexp.MustCompile(fmt.Sprintf(`\$%d\b`, i+1))

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	// GENERIC_PLAN (PostgreSQL 16 and later) plans the $n placeholders
	// without values. The simple protocol sends them as written.
	results, err := conn.Conn().PgConn().Exec(ctx, "EXPLAIN (GENERIC_PLAN) "+sql).ReadAll()
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	var plan strings.Builder
	for _, r := range results {
		for _, row := range r.Rows {
			plan.Write(row[0])
			plan.WriteByte('\n')
		}
	}
	if !strings.Contains(plan.String(), "InitPlan") || pattern.MatchString(plan.String()) {
		t.Fatalf("want the pattern ($%d) lowered once in an InitPlan and never read per row:\n%s", i+1, plan.String())
	}
	// PostgreSQL 16 prints the InitPlan's result as `$0`, and 17 and later
	// as `(InitPlan 1).col1`, so match only the operator and its left side.
	if !strings.Contains(plan.String(), "lower(url) ~~ ") || strings.Contains(plan.String(), "~~*") {
		t.Fatalf("want the filter `lower(url) ~~ <InitPlan>`, and no ILIKE (~~*), which lowers its pattern per row:\n%s", plan.String())
	}
}

// TestPGListReposMatchesILIKE pins the ListRepos url filter to the
// answers of `url ILIKE p`, the filter it replaced, for urls and filters
// whose case folding is not ASCII-only.
func TestPGListReposMatchesILIKE(t *testing.T) {
	s, ok := pgBackend(t).(*PG)
	if !ok {
		t.Fatal("pgBackend is not *PG")
	}
	ctx := t.Context()
	org := seedOrg(t, s)
	urls := []string{
		"acme/Straße", "acme/STRASSE", "acme/ẞig", "acme/İstanbul", "acme/istanbul",
		"acme/IRMAK", "acme/ırmak", "acme/ΣΊΣΥΦΟΣ", "acme/σίσυφος", "acme/ǅemal",
		"acme/ǆ", "acme/ﬃ", "acme/\u212aelvin", "acme/kelvin", "acme/\u212bngstrom",
		"acme/Ångström", "acme/e\u0301clair", "acme/Éclair", "acme/50%Off", "acme/A_B",
		`acme/Back\Slash`, "ACME/upper", "Ünïcode/Prefix",
	}
	for _, u := range urls {
		if _, err := s.CreateRepo(ctx, org, u, "main"); err != nil {
			t.Fatalf("create %s: %v", u, err)
		}
	}
	orgID, err := s.lookupOrgID(ctx, org)
	if err != nil {
		t.Fatal(err)
	}

	// Each filter is a rune of a url, a start of a url, or both, in every
	// case that Go knows for it.
	seen := map[ListParams]bool{}
	var filters []ListParams
	add := func(prefix, q string) {
		for _, p := range []string{prefix, strings.ToUpper(prefix), strings.ToLower(prefix)} {
			for _, q := range []string{q, strings.ToUpper(q), strings.ToLower(q)} {
				f := ListParams{Prefix: strings.TrimSpace(p), Q: strings.TrimSpace(q), Limit: 100}
				if !seen[f] && (f.Prefix != "" || f.Q != "") {
					seen[f] = true
					filters = append(filters, f)
				}
			}
		}
	}
	for _, u := range urls {
		runes := []rune(u)
		for k, r := range runes {
			for f := unicode.SimpleFold(r); ; f = unicode.SimpleFold(f) {
				add("", string(f))
				add("acme/", string(f))
				if f == r {
					break
				}
			}
			add(string(runes[:k+1]), "")
		}
	}

	for _, f := range filters {
		res, err := s.ListRepos(ctx, org, f)
		if err != nil {
			t.Fatalf("ListRepos(prefix=%q, q=%q): %v", f.Prefix, f.Q, err)
		}
		got := make([]string, 0, len(res.Repos))
		for _, r := range res.Repos {
			got = append(got, r.URL)
		}
		rows, err := s.pool.Query(ctx, `SELECT url FROM repos WHERE org_id = $1 AND deleted_at IS NULL
			AND url ILIKE $2 ORDER BY created_at DESC, repo_id DESC`, orgID, listURLPattern(f))
		if err != nil {
			t.Fatal(err)
		}
		want, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			t.Fatal(err)
		}
		if res.HasMore || !slices.Equal(got, want) {
			t.Errorf("prefix=%q q=%q: got %q (HasMore=%v), ILIKE gives %q", f.Prefix, f.Q, got, res.HasMore, want)
		}
	}
	t.Logf("%d filters over %d urls", len(filters), len(urls))
}
