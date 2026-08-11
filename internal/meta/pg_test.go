// Containerized store tests: testcontainers-go boots one real Postgres for
// the package; each test isolates itself with a unique org name (t.Name()).
package meta

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

var testStore *PG

func TestMain(m *testing.M) {
	os.Exit(testMainInner(m))
}

func testMainInner(m *testing.M) int {
	ctx := context.Background()

	pgC, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("postgres"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		panic("start postgres container: " + err.Error())
	}
	defer func() { _ = pgC.Terminate(ctx) }()
	pgURL, err := pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		panic(err)
	}

	testStore, err = OpenPG(ctx, pgURL)
	if err != nil {
		panic(err)
	}
	defer testStore.Close()

	// Concurrent InitSchema on a fresh database exercises the advisory-lock
	// serialization (and idempotency: every call runs the full DDL).
	errs := make(chan error, 4)
	for range 4 {
		go func() { errs <- testStore.InitSchema(ctx) }()
	}
	for range 4 {
		if err := <-errs; err != nil {
			panic("concurrent InitSchema: " + err.Error())
		}
	}

	return m.Run()
}

// seedOrg registers a test-unique org with one dummy key and returns its name.
func seedOrg(t *testing.T) string {
	t.Helper()
	name := t.Name()
	if _, err := testStore.UpsertOrgWithKeys(t.Context(), name, []string{"-----BEGIN PUBLIC KEY-----\ndummy\n-----END PUBLIC KEY-----\n"}); err != nil {
		t.Fatalf("seed org %q: %v", name, err)
	}
	return name
}

func TestUpsertOrgWithKeys(t *testing.T) {
	ctx := t.Context()
	org1, err := testStore.UpsertOrgWithKeys(ctx, t.Name(), []string{"pem-a", "pem-b"})
	if err != nil {
		t.Fatalf("first upsert: %v", err)
	}
	if org1.ID <= 0 || org1.Name != t.Name() || org1.CreatedAt.IsZero() {
		t.Fatalf("bad org row: %+v", org1)
	}

	keys, err := testStore.PublicKeysForOrg(ctx, t.Name())
	if err != nil {
		t.Fatalf("PublicKeysForOrg: %v", err)
	}
	if !slices.Equal(keys, []string{"pem-a", "pem-b"}) {
		t.Errorf("keys = %q, want [pem-a pem-b] in registration order", keys)
	}

	// Re-upsert: same org id, keys fully replaced.
	org2, err := testStore.UpsertOrgWithKeys(ctx, t.Name(), []string{"pem-c"})
	if err != nil {
		t.Fatalf("second upsert: %v", err)
	}
	if org2.ID != org1.ID {
		t.Errorf("upsert changed org id: %d -> %d", org1.ID, org2.ID)
	}
	keys, err = testStore.PublicKeysForOrg(ctx, t.Name())
	if err != nil {
		t.Fatalf("PublicKeysForOrg after replace: %v", err)
	}
	if !slices.Equal(keys, []string{"pem-c"}) {
		t.Errorf("keys after replace = %q, want [pem-c]", keys)
	}
}

func TestPublicKeysForOrgUnknown(t *testing.T) {
	_, err := testStore.PublicKeysForOrg(t.Context(), "no-such-org-"+t.Name())
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestCreateAndGetRepo(t *testing.T) {
	ctx := t.Context()
	org := seedOrg(t)

	created, err := testStore.CreateRepo(ctx, org, "github.com/acme/demo", "main")
	if err != nil {
		t.Fatalf("CreateRepo: %v", err)
	}
	if !regexp.MustCompile(`\Arepo_[0-9a-f]{12}\z`).MatchString(created.RepoID) {
		t.Errorf("RepoID = %q, want repo_<12 hex>", created.RepoID)
	}
	if created.URL != "github.com/acme/demo" || created.DefaultBranch != "main" {
		t.Errorf("bad repo row: %+v", created)
	}
	if created.CreatedAt.IsZero() || created.CreatedAt.Location() != time.UTC {
		t.Errorf("CreatedAt = %v, want non-zero UTC", created.CreatedAt)
	}
	if created.DeletedAt != nil {
		t.Errorf("new repo has DeletedAt = %v", created.DeletedAt)
	}

	got, err := testStore.GetRepoByURL(ctx, org, "github.com/acme/demo")
	if err != nil {
		t.Fatalf("GetRepoByURL: %v", err)
	}
	if got.RepoID != created.RepoID || !got.CreatedAt.Equal(created.CreatedAt) {
		t.Errorf("get = %+v, want %+v", got, created)
	}

	// Duplicate url in the same org -> 409.
	if _, err := testStore.CreateRepo(ctx, org, "github.com/acme/demo", "main"); !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("duplicate create err = %v, want ErrAlreadyExists", err)
	}

	// Same url in ANOTHER org is fine (uniqueness is per org).
	other, err := testStore.UpsertOrgWithKeys(ctx, t.Name()+"-other", []string{"pem"})
	if err != nil {
		t.Fatalf("seed other org: %v", err)
	}
	if _, err := testStore.CreateRepo(ctx, other.Name, "github.com/acme/demo", "main"); err != nil {
		t.Errorf("cross-org create err = %v, want nil", err)
	}

	// Unknown repo / unknown org -> 404.
	if _, err := testStore.GetRepoByURL(ctx, org, "github.com/acme/absent"); !errors.Is(err, ErrNotFound) {
		t.Errorf("get absent err = %v, want ErrNotFound", err)
	}
	if _, err := testStore.GetRepoByURL(ctx, "no-such-org", "github.com/acme/demo"); !errors.Is(err, ErrNotFound) {
		t.Errorf("get unknown-org err = %v, want ErrNotFound", err)
	}
	if _, err := testStore.CreateRepo(ctx, "no-such-org", "u", "main"); !errors.Is(err, ErrNotFound) {
		t.Errorf("create unknown-org err = %v, want ErrNotFound", err)
	}
}

func TestSoftDeleteRepo(t *testing.T) {
	ctx := t.Context()
	org := seedOrg(t)
	const url = "github.com/acme/doomed"

	created, err := testStore.CreateRepo(ctx, org, url, "main")
	if err != nil {
		t.Fatalf("CreateRepo: %v", err)
	}

	deleted, err := testStore.SoftDeleteRepo(ctx, org, url)
	if err != nil {
		t.Fatalf("SoftDeleteRepo: %v", err)
	}
	if deleted.RepoID != created.RepoID {
		t.Errorf("deleted RepoID = %q, want %q", deleted.RepoID, created.RepoID)
	}
	if deleted.DeletedAt == nil || deleted.DeletedAt.IsZero() {
		t.Fatalf("DeletedAt not set: %+v", deleted)
	}

	// Soft-deleted -> GET 404, re-delete 409, re-create same url 409.
	if _, err := testStore.GetRepoByURL(ctx, org, url); !errors.Is(err, ErrNotFound) {
		t.Errorf("get deleted err = %v, want ErrNotFound", err)
	}
	if _, err := testStore.SoftDeleteRepo(ctx, org, url); !errors.Is(err, ErrAlreadyDeleted) {
		t.Errorf("re-delete err = %v, want ErrAlreadyDeleted", err)
	}
	if _, err := testStore.CreateRepo(ctx, org, url, "main"); !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("re-create deleted url err = %v, want ErrAlreadyExists", err)
	}

	// Absent repo / unknown org -> 404.
	if _, err := testStore.SoftDeleteRepo(ctx, org, "github.com/acme/absent"); !errors.Is(err, ErrNotFound) {
		t.Errorf("delete absent err = %v, want ErrNotFound", err)
	}
	if _, err := testStore.SoftDeleteRepo(ctx, "no-such-org", url); !errors.Is(err, ErrNotFound) {
		t.Errorf("delete unknown-org err = %v, want ErrNotFound", err)
	}
}

// listAll walks pages of pageSize until exhaustion, asserting cursor/HasMore
// invariants, and returns every row seen plus the page sizes.
func listAll(t *testing.T, org, q string, pageSize int) ([]Repo, []int) {
	t.Helper()
	var all []Repo
	var pages []int
	cursor := ""
	for range 100 { // hard stop: a cursor bug must not loop forever
		res, err := testStore.ListRepos(t.Context(), org, ListParams{Q: q, Cursor: cursor, Limit: pageSize})
		if err != nil {
			t.Fatalf("ListRepos(q=%q, cursor=%q): %v", q, cursor, err)
		}
		if res.Repos == nil {
			t.Fatalf("Repos is nil (contract: never nil)")
		}
		if len(res.Repos) > pageSize {
			t.Fatalf("page has %d rows > limit %d", len(res.Repos), pageSize)
		}
		if res.HasMore != (res.NextCursor != "") {
			t.Fatalf("HasMore=%v but NextCursor=%q", res.HasMore, res.NextCursor)
		}
		all = append(all, res.Repos...)
		pages = append(pages, len(res.Repos))
		if !res.HasMore {
			return all, pages
		}
		cursor = res.NextCursor
	}
	t.Fatal("pagination did not terminate after 100 pages")
	return nil, nil
}

func TestListReposPagination(t *testing.T) {
	ctx := t.Context()
	org := seedOrg(t)

	created := make(map[string]bool, 25)
	for i := range 25 {
		url := fmt.Sprintf("github.com/acme/list-%02d", i)
		if _, err := testStore.CreateRepo(ctx, org, url, "main"); err != nil {
			t.Fatalf("create %s: %v", url, err)
		}
		created[url] = true
	}
	// A soft-deleted repo must never appear in any page.
	if _, err := testStore.CreateRepo(ctx, org, "github.com/acme/list-deleted", "main"); err != nil {
		t.Fatal(err)
	}
	if _, err := testStore.SoftDeleteRepo(ctx, org, "github.com/acme/list-deleted"); err != nil {
		t.Fatal(err)
	}

	all, pages := listAll(t, org, "", 10)
	if !slices.Equal(pages, []int{10, 10, 5}) {
		t.Errorf("page sizes = %v, want [10 10 5]", pages)
	}

	// Exactly the 25 live repos, no duplicates, no omissions.
	seen := make(map[string]bool, len(all))
	for _, r := range all {
		if seen[r.URL] {
			t.Errorf("duplicate row across pages: %s", r.URL)
		}
		seen[r.URL] = true
		if !created[r.URL] {
			t.Errorf("unexpected row: %s", r.URL)
		}
	}
	if len(seen) != 25 {
		t.Errorf("saw %d distinct repos, want 25", len(seen))
	}

	// Global ordering across pages: (created_at DESC, repo_id DESC).
	sorted := slices.IsSortedFunc(all, func(a, b Repo) int {
		if c := b.CreatedAt.Compare(a.CreatedAt); c != 0 {
			return c
		}
		return strings.Compare(b.RepoID, a.RepoID)
	})
	if !sorted {
		t.Error("rows not ordered by (created_at DESC, repo_id DESC) across pages")
	}

	// One big page: no next cursor.
	res, err := testStore.ListRepos(ctx, org, ListParams{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Repos) != 25 || res.HasMore || res.NextCursor != "" {
		t.Errorf("full page = %d rows, HasMore=%v, NextCursor=%q; want 25, false, \"\"", len(res.Repos), res.HasMore, res.NextCursor)
	}

	// Cursor at the very last row: empty page, not an error.
	last := res.Repos[len(res.Repos)-1]
	tail, err := testStore.ListRepos(ctx, org, ListParams{Cursor: encodeCursor(last.CreatedAt, last.RepoID), Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if tail.Repos == nil || len(tail.Repos) != 0 || tail.HasMore || tail.NextCursor != "" {
		t.Errorf("past-the-end page = %+v, want empty non-nil, no more", tail)
	}

	// Exact multiple of the page size terminates cleanly (25 = 5*5).
	if _, pages := listAll(t, org, "", 5); !slices.Equal(pages, []int{5, 5, 5, 5, 5}) {
		t.Errorf("page sizes = %v, want five pages of 5", pages)
	}
}

func TestListReposQFilter(t *testing.T) {
	ctx := t.Context()
	org := seedOrg(t)
	for _, url := range []string{
		"github.com/acme/Alpha",
		"github.com/acme/beta-ALPHA",
		"github.com/acme/gamma",
		"github.com/acme/50%off",
		"github.com/acme/under_score",
		"github.com/acme/rXs-decoy", // matches ILIKE 'r_s' only if _ is unescaped
		`github.com/acme/back\slash`,
	} {
		if _, err := testStore.CreateRepo(ctx, org, url, "main"); err != nil {
			t.Fatalf("create %s: %v", url, err)
		}
	}

	urls := func(rs []Repo) []string {
		out := make([]string, len(rs))
		for i, r := range rs {
			out[i] = r.URL
		}
		slices.Sort(out)
		return out
	}

	// Case-insensitive substring match.
	all, _ := listAll(t, org, "alpha", 2) // page size 2 also exercises cursor+q together
	if got := urls(all); !slices.Equal(got, []string{"github.com/acme/Alpha", "github.com/acme/beta-ALPHA"}) {
		t.Errorf("q=alpha -> %v", got)
	}

	// LIKE metacharacters in q are literals, not wildcards.
	all, _ = listAll(t, org, "%", 10)
	if got := urls(all); !slices.Equal(got, []string{"github.com/acme/50%off"}) {
		t.Errorf("q=%% -> %v, want just the literal-percent url", got)
	}
	all, _ = listAll(t, org, "r_s", 10)
	if got := urls(all); !slices.Equal(got, []string{"github.com/acme/under_score"}) {
		t.Errorf("q=r_s -> %v, want just the literal-underscore url (not the rXs decoy)", got)
	}
	all, _ = listAll(t, org, `\`, 10)
	if got := urls(all); !slices.Equal(got, []string{`github.com/acme/back\slash`}) {
		t.Errorf(`q=\ -> %v, want just the literal-backslash url`, got)
	}

	// No match: empty non-nil page.
	res, err := testStore.ListRepos(ctx, org, ListParams{Q: "zzz-no-match", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if res.Repos == nil || len(res.Repos) != 0 || res.HasMore || res.NextCursor != "" {
		t.Errorf("no-match result = %+v, want empty non-nil, no more", res)
	}
}

func TestListReposErrors(t *testing.T) {
	ctx := t.Context()
	org := seedOrg(t)

	for _, cursor := range []string{"!!!", "AAAA", "bm9waXBl" /* "nopipe" */} {
		if _, err := testStore.ListRepos(ctx, org, ListParams{Cursor: cursor, Limit: 10}); !errors.Is(err, ErrInvalidCursor) {
			t.Errorf("cursor %q err = %v, want ErrInvalidCursor", cursor, err)
		}
	}

	if _, err := testStore.ListRepos(ctx, "no-such-org", ListParams{Limit: 10}); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown org err = %v, want ErrNotFound", err)
	}

	// Out-of-contract limit is a caller bug -> assertion panic, not silence.
	defer func() {
		if recover() == nil {
			t.Error("ListRepos(limit=0) did not panic")
		}
	}()
	_, _ = testStore.ListRepos(ctx, org, ListParams{Limit: 0})
}
