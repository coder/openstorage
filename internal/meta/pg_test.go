// Backend conformance tests: every test body takes a Backend and fans out
// over both implementations via runBackends (pg/sqlite subtests). The PG
// lane boots one testcontainers-go Postgres for the whole package — lazily,
// so sqlite-filtered runs (-run '/sqlite') boot zero containers. Each test
// isolates itself with a unique org name (t.Name() is subtest-unique).
package meta

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/coder/openstorage/internal/testutil/testimages"
)

var (
	pgOnce    sync.Once
	pgErr     error
	pgStore   *PG
	pgTestURL string // saved so tests can open extra handles (secrets_meta_test.go)
	pgCleanup []func()
)

// pgBackend lazily boots the package-wide Postgres container and store.
// The concurrent InitSchema exercise lives here (not in TestMain) so it
// only runs when a pg lane actually executes.
func pgBackend(t *testing.T) Backend {
	t.Helper()
	pgOnce.Do(func() {
		ctx := context.Background()

		pgC, err := tcpostgres.Run(ctx, testimages.Postgres,
			tcpostgres.WithDatabase("postgres"),
			tcpostgres.WithUsername("postgres"),
			tcpostgres.WithPassword("postgres"),
			tcpostgres.BasicWaitStrategies(),
		)
		if err != nil {
			pgErr = fmt.Errorf("start postgres container: %w", err)
			return
		}
		pgCleanup = append(pgCleanup, func() { _ = pgC.Terminate(ctx) })

		pgURL, err := pgC.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			pgErr = err
			return
		}
		pgTestURL = pgURL
		store, err := OpenPG(ctx, pgURL)
		if err != nil {
			pgErr = err
			return
		}
		pgCleanup = append(pgCleanup, store.Close)

		// Concurrent InitSchema on a fresh database exercises the
		// advisory-lock serialization (and idempotency: every call runs the
		// full DDL).
		errs := make(chan error, 4)
		for range 4 {
			go func() { errs <- store.InitSchema(ctx) }()
		}
		for range 4 {
			if err := <-errs; err != nil {
				pgErr = fmt.Errorf("concurrent InitSchema: %w", err)
				return
			}
		}
		pgStore = store
	})
	if pgErr != nil {
		t.Fatalf("pg backend: %v", pgErr)
	}
	return pgStore
}

func TestMain(m *testing.M) {
	code := m.Run()
	for _, p := range slices.Backward(pgCleanup) {
		p()
	}
	os.Exit(code)
}

// runBackends fans one conformance test body out over both Backend
// implementations. The pg lane shares one container-wide store (tests
// isolate by org name); the sqlite lane gets a fresh DB file per subtest.
func runBackends(t *testing.T, fn func(t *testing.T, s Backend)) {
	t.Run("pg", func(t *testing.T) { fn(t, pgBackend(t)) })
	t.Run("sqlite", func(t *testing.T) { fn(t, sqliteBackend(t)) })
}

// seedOrg registers a test-unique org with one dummy key and returns its name.
func seedOrg(t *testing.T, s Backend) string {
	t.Helper()
	name := t.Name()
	if _, err := s.UpsertOrgWithKeys(t.Context(), name, []string{"-----BEGIN PUBLIC KEY-----\ndummy\n-----END PUBLIC KEY-----\n"}); err != nil {
		t.Fatalf("seed org %q: %v", name, err)
	}
	return name
}

func TestUpsertOrgWithKeys(t *testing.T) { runBackends(t, testUpsertOrgWithKeys) }

func testUpsertOrgWithKeys(t *testing.T, s Backend) {
	ctx := t.Context()
	org1, err := s.UpsertOrgWithKeys(ctx, t.Name(), []string{"pem-a", "pem-b"})
	if err != nil {
		t.Fatalf("first upsert: %v", err)
	}
	if org1.ID <= 0 || org1.Name != t.Name() || org1.CreatedAt.IsZero() {
		t.Fatalf("bad org row: %+v", org1)
	}

	keys, err := s.PublicKeysForOrg(ctx, t.Name())
	if err != nil {
		t.Fatalf("PublicKeysForOrg: %v", err)
	}
	if !slices.Equal(keys, []string{"pem-a", "pem-b"}) {
		t.Errorf("keys = %q, want [pem-a pem-b] in registration order", keys)
	}

	// Re-upsert: same org id, keys fully replaced.
	org2, err := s.UpsertOrgWithKeys(ctx, t.Name(), []string{"pem-c"})
	if err != nil {
		t.Fatalf("second upsert: %v", err)
	}
	if org2.ID != org1.ID {
		t.Errorf("upsert changed org id: %d -> %d", org1.ID, org2.ID)
	}
	keys, err = s.PublicKeysForOrg(ctx, t.Name())
	if err != nil {
		t.Fatalf("PublicKeysForOrg after replace: %v", err)
	}
	if !slices.Equal(keys, []string{"pem-c"}) {
		t.Errorf("keys after replace = %q, want [pem-c]", keys)
	}
}

// TestUpsertOrgWithKeysConcurrent is the regression test for the
// DELETE/INSERT race in UpsertOrgWithKeys: without write-transaction
// serialization (PG: SELECT ... FOR UPDATE on the org row; SQLite: BEGIN
// IMMEDIATE via _txlock), two racing upserts can commit a union of their key
// sets (a blocked DELETE's statement snapshot misses the winner's fresh
// rows). Writers churn full replacements while readers assert the keyring is
// always exactly one writer's complete set — never empty, never a union.
func TestUpsertOrgWithKeysConcurrent(t *testing.T) { runBackends(t, testUpsertOrgWithKeysConcurrent) }

func testUpsertOrgWithKeysConcurrent(t *testing.T, s Backend) {
	ctx := t.Context()
	const workers = 8
	const rounds = 5

	keysFor := func(w int) []string {
		return []string{fmt.Sprintf("pem-%d-a", w), fmt.Sprintf("pem-%d-b", w)}
	}
	isOneWriterSet := func(keys []string) bool {
		for w := range workers {
			if slices.Equal(keys, keysFor(w)) {
				return true
			}
		}
		return false
	}

	// Seed so concurrent readers never hit ErrNotFound.
	if _, err := s.UpsertOrgWithKeys(ctx, t.Name(), keysFor(0)); err != nil {
		t.Fatalf("seed upsert: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, workers*rounds)
	for w := range workers {
		wg.Go(func() {
			for range rounds {
				if _, err := s.UpsertOrgWithKeys(ctx, t.Name(), keysFor(w)); err != nil {
					errs <- fmt.Errorf("worker %d: %w", w, err)
				}
			}
		})
	}

	done := make(chan struct{})
	readerErr := make(chan error, 1)
	go func() {
		defer close(readerErr)
		for {
			select {
			case <-done:
				return
			default:
			}
			keys, err := s.PublicKeysForOrg(ctx, t.Name())
			if err != nil {
				readerErr <- fmt.Errorf("concurrent PublicKeysForOrg: %w", err)
				return
			}
			if !isOneWriterSet(keys) {
				readerErr <- fmt.Errorf("concurrent reader observed keyring %q, want exactly one writer's set", keys)
				return
			}
		}
	}()

	wg.Wait()
	close(errs)
	close(done)
	for err := range errs {
		t.Fatalf("concurrent upsert: %v", err)
	}
	if err := <-readerErr; err != nil {
		t.Fatal(err)
	}

	keys, err := s.PublicKeysForOrg(ctx, t.Name())
	if err != nil {
		t.Fatalf("PublicKeysForOrg after churn: %v", err)
	}
	if !isOneWriterSet(keys) {
		t.Fatalf("final keyring = %q, want exactly one writer's set (no union/duplicates)", keys)
	}
}

func TestPublicKeysForOrgUnknown(t *testing.T) { runBackends(t, testPublicKeysForOrgUnknown) }

func testPublicKeysForOrgUnknown(t *testing.T, s Backend) {
	_, err := s.PublicKeysForOrg(t.Context(), "no-such-org-"+t.Name())
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestSigningKeys(t *testing.T) { runBackends(t, testSigningKeys) }

func testSigningKeys(t *testing.T, s Backend) {
	ctx := t.Context()
	org := seedOrg(t, s)

	// Fresh org: empty keyring, no error.
	keys, err := s.SigningKeysForOrg(ctx, org)
	if err != nil {
		t.Fatalf("SigningKeysForOrg on fresh org: %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("fresh org has %d signing keys, want 0", len(keys))
	}

	k1, err := s.AddSigningKey(ctx, org, "ssh-ed25519 AAAAtest key-one")
	if err != nil {
		t.Fatalf("AddSigningKey #1: %v", err)
	}
	if k1.ID <= 0 || k1.OrgID <= 0 || k1.CreatedAt.IsZero() {
		t.Fatalf("bad signing key row: %+v", k1)
	}
	k2, err := s.AddSigningKey(ctx, org, "ssh-ed25519 AAAAtest key-two")
	if err != nil {
		t.Fatalf("AddSigningKey #2: %v", err)
	}

	keys, err = s.SigningKeysForOrg(ctx, org)
	if err != nil {
		t.Fatalf("SigningKeysForOrg: %v", err)
	}
	var texts []string
	for _, k := range keys {
		texts = append(texts, k.KeyText)
	}
	if !slices.Equal(texts, []string{"ssh-ed25519 AAAAtest key-one", "ssh-ed25519 AAAAtest key-two"}) {
		t.Errorf("keys = %q, want both in registration order", texts)
	}

	if err := s.RemoveSigningKey(ctx, org, k1.ID); err != nil {
		t.Fatalf("RemoveSigningKey: %v", err)
	}
	keys, err = s.SigningKeysForOrg(ctx, org)
	if err != nil {
		t.Fatalf("SigningKeysForOrg after remove: %v", err)
	}
	if len(keys) != 1 || keys[0].ID != k2.ID {
		t.Errorf("keys after remove = %+v, want only id %d", keys, k2.ID)
	}

	// Removing again (or a foreign id): ErrNotFound.
	if err := s.RemoveSigningKey(ctx, org, k1.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("double remove err = %v, want ErrNotFound", err)
	}

	// Unknown org: ErrNotFound on all three methods.
	unknown := "no-such-org-" + t.Name()
	if _, err := s.AddSigningKey(ctx, unknown, "ssh-ed25519 AAAAtest"); !errors.Is(err, ErrNotFound) {
		t.Errorf("AddSigningKey unknown org err = %v, want ErrNotFound", err)
	}
	if _, err := s.SigningKeysForOrg(ctx, unknown); !errors.Is(err, ErrNotFound) {
		t.Errorf("SigningKeysForOrg unknown org err = %v, want ErrNotFound", err)
	}
	if err := s.RemoveSigningKey(ctx, unknown, k2.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("RemoveSigningKey unknown org err = %v, want ErrNotFound", err)
	}
}

func TestCreateAndGetRepo(t *testing.T) { runBackends(t, testCreateAndGetRepo) }

func testCreateAndGetRepo(t *testing.T, s Backend) {
	ctx := t.Context()
	org := seedOrg(t, s)

	created, err := s.CreateRepo(ctx, org, "github.com/acme/demo", "main")
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

	got, err := s.GetRepoByURL(ctx, org, "github.com/acme/demo")
	if err != nil {
		t.Fatalf("GetRepoByURL: %v", err)
	}
	if got.RepoID != created.RepoID || !got.CreatedAt.Equal(created.CreatedAt) {
		t.Errorf("get = %+v, want %+v", got, created)
	}

	byID, err := s.GetRepoByID(ctx, org, created.RepoID)
	if err != nil {
		t.Fatalf("GetRepoByID: %v", err)
	}
	if byID.URL != created.URL || byID.RepoID != created.RepoID {
		t.Errorf("get by id = %+v, want %+v", byID, created)
	}
	if _, err := s.GetRepoByID(ctx, org, "repo_000000000000"); !errors.Is(err, ErrNotFound) {
		t.Errorf("get absent id err = %v, want ErrNotFound", err)
	}

	// Duplicate url in the same org -> 409.
	if _, err := s.CreateRepo(ctx, org, "github.com/acme/demo", "main"); !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("duplicate create err = %v, want ErrAlreadyExists", err)
	}

	// Same url in ANOTHER org is fine (uniqueness is per org).
	other, err := s.UpsertOrgWithKeys(ctx, t.Name()+"-other", []string{"pem"})
	if err != nil {
		t.Fatalf("seed other org: %v", err)
	}
	if _, err := s.CreateRepo(ctx, other.Name, "github.com/acme/demo", "main"); err != nil {
		t.Errorf("cross-org create err = %v, want nil", err)
	}

	// Unknown repo / unknown org -> 404.
	if _, err := s.GetRepoByURL(ctx, org, "github.com/acme/absent"); !errors.Is(err, ErrNotFound) {
		t.Errorf("get absent err = %v, want ErrNotFound", err)
	}
	if _, err := s.GetRepoByURL(ctx, "no-such-org", "github.com/acme/demo"); !errors.Is(err, ErrNotFound) {
		t.Errorf("get unknown-org err = %v, want ErrNotFound", err)
	}
	if _, err := s.CreateRepo(ctx, "no-such-org", "u", "main"); !errors.Is(err, ErrNotFound) {
		t.Errorf("create unknown-org err = %v, want ErrNotFound", err)
	}
}

func TestSoftDeleteRepo(t *testing.T) { runBackends(t, testSoftDeleteRepo) }

func testSoftDeleteRepo(t *testing.T, s Backend) {
	ctx := t.Context()
	org := seedOrg(t, s)
	const url = "github.com/acme/doomed"

	created, err := s.CreateRepo(ctx, org, url, "main")
	if err != nil {
		t.Fatalf("CreateRepo: %v", err)
	}

	deleted, err := s.SoftDeleteRepo(ctx, org, url)
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
	if _, err := s.GetRepoByURL(ctx, org, url); !errors.Is(err, ErrNotFound) {
		t.Errorf("get deleted err = %v, want ErrNotFound", err)
	}
	if _, err := s.SoftDeleteRepo(ctx, org, url); !errors.Is(err, ErrAlreadyDeleted) {
		t.Errorf("re-delete err = %v, want ErrAlreadyDeleted", err)
	}
	if _, err := s.CreateRepo(ctx, org, url, "main"); !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("re-create deleted url err = %v, want ErrAlreadyExists", err)
	}

	// Absent repo / unknown org -> 404.
	if _, err := s.SoftDeleteRepo(ctx, org, "github.com/acme/absent"); !errors.Is(err, ErrNotFound) {
		t.Errorf("delete absent err = %v, want ErrNotFound", err)
	}
	if _, err := s.SoftDeleteRepo(ctx, "no-such-org", url); !errors.Is(err, ErrNotFound) {
		t.Errorf("delete unknown-org err = %v, want ErrNotFound", err)
	}
}

func TestUpdateRepoDefaultBranch(t *testing.T) { runBackends(t, testUpdateRepoDefaultBranch) }

func testUpdateRepoDefaultBranch(t *testing.T, s Backend) {
	ctx := t.Context()
	org := seedOrg(t, s)
	const url = "github.com/acme/patched"

	if _, err := s.CreateRepo(ctx, org, url, "main"); err != nil {
		t.Fatalf("CreateRepo: %v", err)
	}

	// CAS win: the expected old value matches the row.
	won, err := s.UpdateRepoDefaultBranch(ctx, org, url, "main", "dev")
	if err != nil || !won {
		t.Fatalf("UpdateRepoDefaultBranch(main->dev) = (won=%v, err=%v), want won", won, err)
	}
	got, err := s.GetRepoByURL(ctx, org, url)
	if err != nil || got.DefaultBranch != "dev" {
		t.Fatalf("after CAS: repo = %+v (err %v), want default_branch dev", got, err)
	}

	// CAS loss: the expected old value is stale — no error, no write.
	won, err = s.UpdateRepoDefaultBranch(ctx, org, url, "main", "other")
	if err != nil || won {
		t.Fatalf("stale CAS = (won=%v, err=%v), want lost without error", won, err)
	}
	if got, _ := s.GetRepoByURL(ctx, org, url); got.DefaultBranch != "dev" {
		t.Fatalf("lost CAS wrote: default_branch = %q, want dev", got.DefaultBranch)
	}

	// Idempotent same-value CAS wins (old == row == new).
	if won, err := s.UpdateRepoDefaultBranch(ctx, org, url, "dev", "dev"); err != nil || !won {
		t.Fatalf("same-value CAS = (won=%v, err=%v), want won", won, err)
	}

	// Absent repo / unknown org -> ErrNotFound.
	if _, err := s.UpdateRepoDefaultBranch(ctx, org, "github.com/acme/absent", "main", "dev"); !errors.Is(err, ErrNotFound) {
		t.Errorf("absent repo err = %v, want ErrNotFound", err)
	}
	if _, err := s.UpdateRepoDefaultBranch(ctx, "no-such-org", url, "main", "dev"); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown org err = %v, want ErrNotFound", err)
	}

	// Soft-deleted repo -> ErrNotFound even when the old value matches.
	if _, err := s.SoftDeleteRepo(ctx, org, url); err != nil {
		t.Fatalf("SoftDeleteRepo: %v", err)
	}
	if _, err := s.UpdateRepoDefaultBranch(ctx, org, url, "dev", "main"); !errors.Is(err, ErrNotFound) {
		t.Errorf("deleted repo err = %v, want ErrNotFound", err)
	}
}

// listAll walks pages of pageSize until exhaustion, asserting cursor/HasMore
// invariants, and returns every row seen plus the page sizes.
func listAll(t *testing.T, s Backend, org, q string, pageSize int) ([]Repo, []int) {
	t.Helper()
	return listAllParams(t, s, org, ListParams{Q: q, Limit: pageSize})
}

// listAllParams is listAll with every filter of p; p.Cursor must be "".
func listAllParams(t *testing.T, s Backend, org string, p ListParams) ([]Repo, []int) {
	t.Helper()
	if p.Cursor != "" {
		t.Fatalf("listAllParams: start cursor %q, want \"\"", p.Cursor)
	}
	pageSize := p.Limit
	var all []Repo
	var pages []int
	cursor := ""
	for range 100 { // hard stop: a cursor bug must not loop forever
		p.Cursor = cursor
		res, err := s.ListRepos(t.Context(), org, p)
		if err != nil {
			t.Fatalf("ListRepos(q=%q, prefix=%q, cursor=%q): %v", p.Q, p.Prefix, cursor, err)
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

func TestListReposPagination(t *testing.T) { runBackends(t, testListReposPagination) }

func testListReposPagination(t *testing.T, s Backend) {
	ctx := t.Context()
	org := seedOrg(t, s)

	created := make(map[string]bool, 25)
	for i := range 25 {
		url := fmt.Sprintf("github.com/acme/list-%02d", i)
		if _, err := s.CreateRepo(ctx, org, url, "main"); err != nil {
			t.Fatalf("create %s: %v", url, err)
		}
		created[url] = true
	}
	// A soft-deleted repo must never appear in any page.
	if _, err := s.CreateRepo(ctx, org, "github.com/acme/list-deleted", "main"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SoftDeleteRepo(ctx, org, "github.com/acme/list-deleted"); err != nil {
		t.Fatal(err)
	}

	all, pages := listAll(t, s, org, "", 10)
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
	res, err := s.ListRepos(ctx, org, ListParams{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Repos) != 25 || res.HasMore || res.NextCursor != "" {
		t.Errorf("full page = %d rows, HasMore=%v, NextCursor=%q; want 25, false, \"\"", len(res.Repos), res.HasMore, res.NextCursor)
	}

	// Cursor at the very last row: empty page, not an error.
	last := res.Repos[len(res.Repos)-1]
	tail, err := s.ListRepos(ctx, org, ListParams{Cursor: encodeCursor(last.CreatedAt, last.RepoID), Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if tail.Repos == nil || len(tail.Repos) != 0 || tail.HasMore || tail.NextCursor != "" {
		t.Errorf("past-the-end page = %+v, want empty non-nil, no more", tail)
	}

	// Exact multiple of the page size terminates cleanly (25 = 5*5).
	if _, pages := listAll(t, s, org, "", 5); !slices.Equal(pages, []int{5, 5, 5, 5, 5}) {
		t.Errorf("page sizes = %v, want five pages of 5", pages)
	}
}

func TestListReposQFilter(t *testing.T) { runBackends(t, testListReposQFilter) }

func testListReposQFilter(t *testing.T, s Backend) {
	ctx := t.Context()
	org := seedOrg(t, s)
	for _, url := range []string{
		"github.com/acme/Alpha",
		"github.com/acme/beta-ALPHA",
		"github.com/acme/gamma",
		"github.com/acme/50%off",
		"github.com/acme/under_score",
		"github.com/acme/rXs-decoy", // matches ILIKE 'r_s' only if _ is unescaped
		`github.com/acme/back\slash`,
	} {
		if _, err := s.CreateRepo(ctx, org, url, "main"); err != nil {
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
	all, _ := listAll(t, s, org, "alpha", 2) // page size 2 also exercises cursor+q together
	if got := urls(all); !slices.Equal(got, []string{"github.com/acme/Alpha", "github.com/acme/beta-ALPHA"}) {
		t.Errorf("q=alpha -> %v", got)
	}
	all, _ = listAll(t, s, org, "GAMMA", 10) // q in another case than the url
	if got := urls(all); !slices.Equal(got, []string{"github.com/acme/gamma"}) {
		t.Errorf("q=GAMMA -> %v, want the gamma url", got)
	}

	// LIKE metacharacters in q are literals, not wildcards.
	all, _ = listAll(t, s, org, "%", 10)
	if got := urls(all); !slices.Equal(got, []string{"github.com/acme/50%off"}) {
		t.Errorf("q=%% -> %v, want just the literal-percent url", got)
	}
	all, _ = listAll(t, s, org, "r_s", 10)
	if got := urls(all); !slices.Equal(got, []string{"github.com/acme/under_score"}) {
		t.Errorf("q=r_s -> %v, want just the literal-underscore url (not the rXs decoy)", got)
	}
	all, _ = listAll(t, s, org, `\`, 10)
	if got := urls(all); !slices.Equal(got, []string{`github.com/acme/back\slash`}) {
		t.Errorf(`q=\ -> %v, want just the literal-backslash url`, got)
	}

	// No match: empty non-nil page.
	res, err := s.ListRepos(ctx, org, ListParams{Q: "zzz-no-match", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if res.Repos == nil || len(res.Repos) != 0 || res.HasMore || res.NextCursor != "" {
		t.Errorf("no-match result = %+v, want empty non-nil, no more", res)
	}
}

func TestListReposPrefixFilter(t *testing.T) { runBackends(t, testListReposPrefixFilter) }

// testListReposPrefixFilter pins the upstream `prefix` semantics (spec
// 34ac6de1): case-insensitive start-of-url match, ANDed with q, and with a
// prefix set q matches only the url after the prefix.
func testListReposPrefixFilter(t *testing.T, s Backend) {
	ctx := t.Context()
	org := seedOrg(t, s)
	all := []string{
		"pierre/sdk-documentation",
		"pierre/example",
		"Pierre/Upper-SDK",
		"pierre/pierre-mirror",
		"acme/pierre-tools",
		"acme/pierre/inner", // holds the prefix 'pierre/', but not at the start
		"50%/off",
		"500/decoy", // matches prefix '50%' only if % is unescaped
		"a_b/one",
		"axb/decoy", // matches prefix 'a_b' only if _ is unescaped
		`back\slash/one`,
	}
	for _, url := range all {
		if _, err := s.CreateRepo(ctx, org, url, "main"); err != nil {
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
	sorted := func(in ...string) []string {
		out := slices.Clone(in)
		slices.Sort(out)
		return out
	}
	pierre := sorted("pierre/sdk-documentation", "pierre/example", "Pierre/Upper-SDK", "pierre/pierre-mirror")

	for _, tc := range []struct {
		name, prefix, q string
		want            []string
	}{
		{"empty prefix is no filter", "", "", sorted(all...)},
		{"prefix alone, case-insensitive", "PIERRE/", "", pierre},
		{"upstream prefix+q example", "pierre/", "sdk", sorted("pierre/sdk-documentation", "Pierre/Upper-SDK")},
		{"q matches only after the prefix", "pierre/", "pierre", sorted("pierre/pierre-mirror")},
		{"q inside the prefix does not match", "pierre/sdk", "sdk", []string{}},
		{"no prefix match", "nobody/", "", []string{}},
		{"literal percent", "50%", "", sorted("50%/off")},
		{"literal underscore", "a_b", "", sorted("a_b/one")},
		{"literal backslash", `back\`, "", sorted(`back\slash/one`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Page size 1 walks a cursor per row: every match exactly once.
			rows, _ := listAllParams(t, s, org, ListParams{Q: tc.q, Prefix: tc.prefix, Limit: 1})
			got := urls(rows)
			if !slices.Equal(got, tc.want) {
				t.Errorf("prefix=%q q=%q -> %v, want %v", tc.prefix, tc.q, got, tc.want)
			}
			res, err := s.ListRepos(ctx, org, ListParams{Q: tc.q, Prefix: tc.prefix, Limit: 100})
			if err != nil {
				t.Fatal(err)
			}
			if one := urls(res.Repos); res.HasMore || !slices.Equal(one, tc.want) {
				t.Errorf("single page prefix=%q q=%q -> %v (HasMore=%v), want %v", tc.prefix, tc.q, one, res.HasMore, tc.want)
			}
		})
	}
}

func TestListReposLongFilters(t *testing.T) { runBackends(t, testListReposLongFilters) }

// testListReposLongFilters pins q and prefix of any length: SQLite refuses
// a LIKE pattern over 50,000 bytes (SQLITE_MAX_LIKE_PATTERN_LENGTH), and
// an escaped `%` takes two of them.
func testListReposLongFilters(t *testing.T, s Backend) {
	ctx := t.Context()
	org := seedOrg(t, s)
	percents := strings.Repeat("%", 30_000)
	long := "acme/" + percents + "/end"
	for _, url := range []string{"acme/alpha", long} {
		if _, err := s.CreateRepo(ctx, org, url, "main"); err != nil {
			t.Fatalf("create %.40s: %v", url, err)
		}
	}
	for _, tc := range []struct {
		name, prefix, q string
		want            []string
	}{
		{"q of 200 KiB", "", strings.Repeat("a", 200<<10), nil},
		{"prefix of 200 KiB", strings.Repeat("a", 200<<10), "", nil},
		{"q of 30,000 percent signs", "", percents, []string{long}},
		{"q one percent sign longer", "", percents + "%", nil},
		{"prefix of 30,000 percent signs and q", "acme/" + percents, "END", []string{long}},
		{"q inside a long prefix", "acme/" + percents, "%", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := s.ListRepos(ctx, org, ListParams{Q: tc.q, Prefix: tc.prefix, Limit: 10})
			if err != nil {
				t.Fatalf("list: %.300v", err)
			}
			got := make([]string, 0, len(res.Repos))
			for _, r := range res.Repos {
				got = append(got, r.URL)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("prefix %d bytes, q %d bytes: got %d repos, want %d", len(tc.prefix), len(tc.q), len(got), len(tc.want))
			}
		})
	}
}

func TestListReposErrors(t *testing.T) { runBackends(t, testListReposErrors) }

func testListReposErrors(t *testing.T, s Backend) {
	ctx := t.Context()
	org := seedOrg(t, s)

	// A repo id with a NUL byte or invalid UTF-8 would reach PostgreSQL as
	// the keyset position, which refuses it (SQLSTATE 22021).
	nul, notUTF8 := encodeCursor(time.Unix(1, 0), "repo_\x00x"), encodeCursor(time.Unix(1, 0), "repo_\xffx")
	for _, cursor := range []string{"!!!", "AAAA", "bm9waXBl" /* "nopipe" */, nul, notUTF8} {
		if _, err := s.ListRepos(ctx, org, ListParams{Cursor: cursor, Limit: 10}); !errors.Is(err, ErrInvalidCursor) {
			t.Errorf("cursor %q err = %v, want ErrInvalidCursor", cursor, err)
		}
	}

	if _, err := s.ListRepos(ctx, "no-such-org", ListParams{Limit: 10}); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown org err = %v, want ErrNotFound", err)
	}

	// Out-of-contract limit is a caller bug -> assertion panic, not silence.
	defer func() {
		if recover() == nil {
			t.Error("ListRepos(limit=0) did not panic")
		}
	}()
	_, _ = s.ListRepos(ctx, org, ListParams{Limit: 0})
}

func TestTextKeysAreUnknown(t *testing.T) { runBackends(t, testTextKeysAreUnknown) }

// testTextKeysAreUnknown pins that a key with a NUL byte or invalid UTF-8
// names no row, on every backend. PostgreSQL refuses such a parameter
// (SQLSTATE 22021), so its store answers ErrNotFound itself; a request
// that carries one in a path or a body must see the 404 of an unknown key,
// not a 500.
func testTextKeysAreUnknown(t *testing.T, s Backend) {
	ctx := t.Context()
	org := seedOrg(t, s)
	repo, err := s.CreateRepo(ctx, org, "acme/live", "main")
	if err != nil {
		t.Fatalf("create repo: %v", err)
	}
	wh, err := s.CreateWebhook(ctx, org, "https://hooks.example.invalid/x", "secret")
	if err != nil {
		t.Fatalf("create webhook: %v", err)
	}
	cred, err := s.CreateGitCredential(ctx, GitCredential{RepoID: repo.RepoID, Username: "u", Password: "p"})
	if err != nil {
		t.Fatalf("create credential: %v", err)
	}

	for _, key := range []string{"a\x00b", "\x00", "repo_\x00", "a\xffb", "\xc3", "\xed\xa0\x80"} {
		lookups := []struct {
			name string
			call func() error
		}{
			{"SoftDeleteRepo", func() error { _, err := s.SoftDeleteRepo(ctx, org, key); return err }},
			{"GetWebhook", func() error { _, err := s.GetWebhook(ctx, key); return err }},
			{"DeleteWebhook", func() error { return s.DeleteWebhook(ctx, org, key) }},
			{"GetGitCredential", func() error { _, err := s.GetGitCredential(ctx, key); return err }},
			{"GitCredentialMeta", func() error { _, err := s.GitCredentialMeta(ctx, key); return err }},
			{"UpdateGitCredential", func() error { _, err := s.UpdateGitCredential(ctx, key, "u2", "p2"); return err }},
			{"DeleteGitCredential", func() error { return s.DeleteGitCredential(ctx, key) }},
			{"org name of GetRepoByURL", func() error { _, err := s.GetRepoByURL(ctx, key, "acme/live"); return err }},
			{"org name of PublicKeysForOrg", func() error { _, err := s.PublicKeysForOrg(ctx, key); return err }},
		}
		for _, l := range lookups {
			t.Run(fmt.Sprintf("%s %q", l.name, key), func(t *testing.T) {
				if err := l.call(); !errors.Is(err, ErrNotFound) {
					t.Errorf("err = %.200v, want ErrNotFound", err)
				}
			})
		}
	}

	// Nothing was deleted or changed on the way.
	if got, err := s.GetRepoByID(ctx, org, repo.RepoID); err != nil || got.URL != "acme/live" {
		t.Errorf("repo after the lookups = %+v, %v", got, err)
	}
	if _, err := s.GetWebhook(ctx, wh.ID); err != nil {
		t.Errorf("webhook after the lookups: %v", err)
	}
	if got, err := s.GetGitCredential(ctx, cred.ID); err != nil || got.Username != "u" || got.Password != "p" {
		t.Errorf("credential after the lookups = %+v, %v", got, err)
	}
}
