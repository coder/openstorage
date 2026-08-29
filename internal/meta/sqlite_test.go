// SQLite-specific backend tests (zero containers): URL parsing / parent-dir
// creation, concurrent InitSchema without an advisory lock, and the
// two-handle same-file topology of local mode (serve process + re-exec'd
// hook process). The shared Store conformance suite lives in pg_test.go and
// runs against sqlite via runBackends.
package meta

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
)

// openTestSQLite opens (and schema-inits) a fresh store under t.TempDir()
// and returns it plus its sqlite:// URL so tests can open second handles on
// the same file (simulating a re-exec'd hook process).
func openTestSQLite(t *testing.T) (*SQLite, string) {
	t.Helper()
	ctx := context.Background()
	dbURL := "sqlite://" + filepath.Join(t.TempDir(), "meta.sqlite")
	s, err := OpenSQLite(ctx, dbURL)
	if err != nil {
		t.Fatalf("OpenSQLite(%q): %v", dbURL, err)
	}
	t.Cleanup(s.Close)
	if err := s.InitSchema(ctx); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	return s, dbURL
}

// sqliteBackend is the sqlite lane for runBackends: a fresh DB file per
// subtest.
func sqliteBackend(t *testing.T) Backend {
	t.Helper()
	s, _ := openTestSQLite(t)
	return s
}

// TestSQLiteOpenURLForms covers URL parsing: the scheme prefix is required,
// and missing parent directories are created (data-dir bootstrap).
func TestSQLiteOpenURLForms(t *testing.T) {
	ctx := context.Background()

	for _, bad := range []string{"postgres://h/db", "/no/scheme/at/all", "sqlite://"} {
		if _, err := OpenSQLite(ctx, bad); err == nil {
			t.Fatalf("OpenSQLite(%q) must fail", bad)
		}
	}

	nested := "sqlite://" + filepath.Join(t.TempDir(), "a", "b", "c", "meta.sqlite")
	s, err := OpenSQLite(ctx, nested)
	if err != nil {
		t.Fatalf("OpenSQLite must MkdirAll missing parents: %v", err)
	}
	defer s.Close()
	if err := s.InitSchema(ctx); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
}

// TestSQLiteInitSchemaConcurrent covers repeated + concurrent InitSchema
// from two handles on one file (serve restart / racing startups). Unlike PG
// there is no advisory lock: the file write lock + busy_timeout serialize
// the racing DDL transactions.
func TestSQLiteInitSchemaConcurrent(t *testing.T) {
	ctx := context.Background()
	s, dbURL := openTestSQLite(t)

	s2, err := OpenSQLite(ctx, dbURL)
	if err != nil {
		t.Fatalf("second OpenSQLite: %v", err)
	}
	defer s2.Close()

	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for _, h := range []*SQLite{s, s2, s, s2} {
		wg.Go(func() {
			errs <- h.InitSchema(ctx)
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent InitSchema: %v", err)
		}
	}
}

// TestSQLiteTwoHandles simulates the local-mode topology: the serve process
// opens the file and runs InitSchema; a re-exec'd hook process opens a
// SECOND handle on the same file and never runs DDL. Writes through each
// handle must be visible through the other.
func TestSQLiteTwoHandles(t *testing.T) {
	ctx := context.Background()
	serve, dbURL := openTestSQLite(t)

	hook, err := OpenSQLite(ctx, dbURL) // hooks never run InitSchema
	if err != nil {
		t.Fatalf("hook OpenSQLite: %v", err)
	}
	defer hook.Close()

	// serve → hook: org, keys, and repo registered by serve are visible.
	org := t.Name()
	if _, err := serve.UpsertOrgWithKeys(ctx, org, []string{"pem-serve"}); err != nil {
		t.Fatalf("serve UpsertOrgWithKeys: %v", err)
	}
	created, err := serve.CreateRepo(ctx, org, "github.com/acme/demo", "main")
	if err != nil {
		t.Fatalf("serve CreateRepo: %v", err)
	}
	got, err := hook.GetRepoByID(ctx, org, created.RepoID)
	if err != nil {
		t.Fatalf("hook GetRepoByID: %v", err)
	}
	if got.URL != created.URL || !got.CreatedAt.Equal(created.CreatedAt) {
		t.Errorf("hook sees %+v, want %+v", got, created)
	}
	pems, err := hook.PublicKeysForOrg(ctx, org)
	if err != nil {
		t.Fatalf("hook PublicKeysForOrg: %v", err)
	}
	if len(pems) != 1 || pems[0] != "pem-serve" {
		t.Errorf("hook sees pems %q, want [pem-serve]", pems)
	}

	// hook → serve: a signing key added through the hook handle is visible.
	k, err := hook.AddSigningKey(ctx, org, "ssh-ed25519 AAAAtest hook-key")
	if err != nil {
		t.Fatalf("hook AddSigningKey: %v", err)
	}
	keys, err := serve.SigningKeysForOrg(ctx, org)
	if err != nil {
		t.Fatalf("serve SigningKeysForOrg: %v", err)
	}
	if len(keys) != 1 || keys[0].ID != k.ID || keys[0].KeyText != k.KeyText {
		t.Errorf("serve sees signing keys %+v, want exactly %+v", keys, k)
	}
}
