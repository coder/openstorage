package githttp

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref"
)

// canarySQLite makes a SQLite store with the meta and ref schemas and the
// org "t-org", runs stmts on it, and returns its URL. A view in a table's
// place that selects from a missing table fails every read of the table
// with that table's name, and a trigger fails a write with its own
// message, so a test can fail one store step with a canary in the error.
func canarySQLite(t *testing.T, stmts ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "store.db")
	dbURL := "sqlite://" + path
	ms, err := meta.Open(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.InitSchema(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.UpsertOrgWithKeys(t.Context(), "t-org", []string{"-----BEGIN PUBLIC KEY-----\ndummy\n-----END PUBLIC KEY-----\n"}); err != nil {
		t.Fatal(err)
	}
	ms.Close()
	rs, err := ref.Open(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	if err := rs.InitSchema(t.Context()); err != nil {
		t.Fatal(err)
	}
	rs.Close()
	canarySQLExec(t, dbURL, stmts...)
	return dbURL
}

// canarySQLExec runs stmts on the SQLite store at dbURL.
func canarySQLExec(t *testing.T, dbURL string, stmts ...string) {
	t.Helper()
	db, err := sql.Open("sqlite", strings.TrimPrefix(dbURL, "sqlite://"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, stmt := range stmts {
		if _, err := db.ExecContext(t.Context(), stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
}

// canaryTable is a unique name for a table that does not exist.
func canaryTable() string { return "canary_" + strings.ToLower(rand.Text()[:8]) }

// brokenReads replaces table with a view on the missing table canary.
func brokenReads(table, canary string) []string {
	return []string{"DROP TABLE " + table, "CREATE VIEW " + table + " AS SELECT * FROM " + canary}
}

// checkFailedStep fails unless the pusher got the fixed line for hook, the
// pusher's text has no canary, and the hook-errors records name step and
// the canary.
func checkFailedStep(t *testing.T, err error, records, hook, step, canary string) {
	t.Helper()
	if want := hook + " failed; push again later (the server log has the cause)"; err == nil || err.Error() != want {
		t.Fatalf("%s = %v; want %q", hook, err, want)
	}
	if n := strings.Count(err.Error(), canary); n != 0 {
		t.Fatalf("the pusher's text names the canary %d times", n)
	}
	if !strings.Contains(records, step) || !strings.Contains(records, canary) {
		t.Fatalf("the hook-errors records lack the step %q or the canary %q: %s", step, canary, records)
	}
}

// The update hook's store steps that TestUpdateHookHidesStoreErrors does
// not reach: each fails with a canary in its error, and the pusher gets
// only the fixed line.
func TestUpdateHookHidesStepErrors(t *testing.T) {
	sha := strings.Repeat("a", 40)
	t.Run("ref store open", func(t *testing.T) {
		c := pgCanary()
		records := hookEnv(t, c.url)
		err := RunHook(t.Context(), []string{"update", "refs/heads/main", ref.ZeroSHA, sha})
		checkFailedStep(t, err, readRecords(t, records), "update", "update: open ref store:", c.parts["host"])
	})
	t.Run("sync fence upstream load", func(t *testing.T) {
		canary := canaryTable()
		records := hookEnv(t, canarySQLite(t, brokenReads("repo_upstreams", canary)...))
		t.Setenv(EnvSyncClaim, "holder")
		err := RunHook(t.Context(), []string{"update", "refs/heads/main", ref.ZeroSHA, sha})
		checkFailedStep(t, err, readRecords(t, records), "update", "sync claim fence: load upstream", canary)
	})
	t.Run("verify-sig key load", func(t *testing.T) {
		canary := canaryTable()
		records := hookEnv(t, canarySQLite(t, brokenReads("org_signing_keys", canary)...))
		commit := unverifiedCommit(t)
		t.Setenv(EnvRefsPolicy, `[["refs/heads/main",["verify-sig"]]]`)
		err := RunHook(t.Context(), []string{"update", "refs/heads/main", ref.ZeroSHA, commit})
		checkFailedStep(t, err, readRecords(t, records), "update", "verify-sig: load signing keys", canary)
	})
	t.Run("WrapHookRefStore error", func(t *testing.T) {
		canary := canaryTable()
		records := hookEnv(t, canarySQLite(t))
		prev := WrapHookRefStore
		t.Cleanup(func() { WrapHookRefStore = prev })
		WrapHookRefStore = func(context.Context, ref.RefStore) (ref.RefStore, error) {
			return nil, fmt.Errorf("hook eventing: open %s", canary)
		}
		err := RunHook(t.Context(), []string{"update", "refs/heads/main", ref.ZeroSHA, sha})
		checkFailedStep(t, err, readRecords(t, records), "update", "hook eventing: open", canary)
	})
}

// preReceiveCase is one in-process pre-receive run for one ref update to
// the commit of a fresh work repository, as receive-pack runs the hook.
type preReceiveCase struct {
	old    string // the command's old value; ref.ZeroSHA creates the ref
	atomic bool
	// pack quarantines the commit in a pack, as a push that transfers it:
	// pre-receive then publishes the pack. Without it the commit is
	// already stored.
	pack     bool
	holdPath string // the push's hold file; "" makes one in a temp dir
	// beforeHook runs after the pack is made, with its ID.
	beforeHook func(t *testing.T, packID string)
}

// run runs pre-receive for refs/heads/main and returns its error.
func (c preReceiveCase) run(t *testing.T) error {
	t.Helper()
	work := initWorkRepo(t)
	commit := revParse(t, work, "HEAD")
	dir := work
	packID := ""
	if c.pack {
		bare := t.TempDir()
		mustGit(t, bare, "init", "-q", "--bare", ".")
		q := t.TempDir()
		if err := os.Mkdir(filepath.Join(q, "pack"), 0o755); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command("git", "-C", work, "pack-objects", "--revs", "-q", filepath.Join(q, "pack", "pack"))
		cmd.Stdin = strings.NewReader("HEAD\n")
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("pack-objects: %v", err)
		}
		packID = strings.TrimSpace(string(out))
		t.Setenv("GIT_DIR", bare)
		t.Setenv("GIT_QUARANTINE_PATH", q)
		t.Setenv("GIT_OBJECT_DIRECTORY", q)
		t.Setenv("GIT_ALTERNATE_OBJECT_DIRECTORIES", filepath.Join(bare, "objects"))
		dir = bare
	}
	if c.beforeHook != nil {
		c.beforeHook(t, packID)
	}
	t.Chdir(dir)
	hold := c.holdPath
	if hold == "" {
		hold = filepath.Join(t.TempDir(), pushHoldDir, "push-canary")
	}
	if err := os.MkdirAll(filepath.Dir(hold), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvPushHold, hold)
	if c.atomic {
		t.Setenv(EnvAtomic, "1")
	}
	old := c.old
	if old == "" {
		old = ref.ZeroSHA
	}
	r, w, perr := os.Pipe()
	if perr != nil {
		t.Fatal(perr)
	}
	prev := os.Stdin
	os.Stdin = r
	t.Cleanup(func() { os.Stdin = prev; _ = r.Close() })
	if _, err := fmt.Fprintf(w, "%s %s refs/heads/main\n", old, commit); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	return RunHook(t.Context(), []string{"pre-receive"})
}

// Each pre-receive store and file step fails with a canary in its error,
// and the pusher gets only the fixed line.
func TestPreReceiveHidesStepErrors(t *testing.T) {
	blobDir := func(t *testing.T) { t.Setenv("OPENSTORAGE_S3_URL", "file://"+t.TempDir()) }
	const quota = "1000000000"
	t.Run("ref store open", func(t *testing.T) {
		// An update creates no ref, so the reservation probe is skipped
		// and the store first opens after the blob stat.
		c := pgCanary()
		records := hookEnv(t, c.url)
		blobDir(t)
		err := preReceiveCase{old: strings.Repeat("b", 40), pack: true}.run(t)
		checkFailedStep(t, err, readRecords(t, records), "pre-receive", "pre-receive: open ref store:", c.parts["host"])
	})
	t.Run("authority D/F probe", func(t *testing.T) {
		canary := canaryTable()
		records := hookEnv(t, canarySQLite(t, brokenReads("refs", canary)...))
		err := preReceiveCase{}.run(t)
		checkFailedStep(t, err, readRecords(t, records), "pre-receive", "pre-receive: authority D/F probe", canary)
	})
	t.Run("verdict record", func(t *testing.T) {
		canary := canaryTable()
		records := hookEnv(t, canarySQLite(t))
		t.Setenv(EnvVerifySigBudget, "100")
		hold := filepath.Join(t.TempDir(), canary, pushHoldDir, "push-canary")
		// A directory where the verdict sidecar goes: its write fails and
		// names the path.
		if err := os.MkdirAll(hold+pushHoldVerifiedSuffix, 0o755); err != nil {
			t.Fatal(err)
		}
		err := preReceiveCase{old: strings.Repeat("b", 40), atomic: true, holdPath: hold}.run(t)
		checkFailedStep(t, err, readRecords(t, records), "pre-receive", "pre-receive: record verified verdicts", canary)
	})
	t.Run("atomic base check", func(t *testing.T) {
		canary := canaryTable()
		records := hookEnv(t, canarySQLite(t, brokenReads("refs", canary)...))
		err := preReceiveCase{old: strings.Repeat("b", 40), atomic: true}.run(t)
		checkFailedStep(t, err, readRecords(t, records), "pre-receive", "pre-receive: atomic base check", canary)
	})
	t.Run("blob store open", func(t *testing.T) {
		records := hookEnv(t, canarySQLite(t))
		// A regular file where the blob directory's parent goes.
		dir, canary := t.TempDir(), canaryTable()
		if err := os.WriteFile(filepath.Join(dir, canary), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("OPENSTORAGE_S3_URL", "file://"+filepath.Join(dir, canary, "blobs"))
		err := preReceiveCase{pack: true}.run(t)
		checkFailedStep(t, err, readRecords(t, records), "pre-receive", "create blob dir", canary)
	})
	t.Run("registry probe", func(t *testing.T) {
		canary := canaryTable()
		records := hookEnv(t, canarySQLite(t, brokenReads("packs", canary)...))
		blobDir(t)
		err := preReceiveCase{pack: true}.run(t)
		checkFailedStep(t, err, readRecords(t, records), "pre-receive", "pre-receive: registry probe", canary)
	})
	t.Run("usage floor", func(t *testing.T) {
		canary := canaryTable()
		records := hookEnv(t, canarySQLite(t, brokenReads("pack_usage", canary)...))
		blobDir(t)
		t.Setenv(EnvPackQuota, quota)
		err := preReceiveCase{pack: true}.run(t)
		checkFailedStep(t, err, readRecords(t, records), "pre-receive", "usage floor for", canary)
	})
	t.Run("reserve pack bytes", func(t *testing.T) {
		canary := canaryTable()
		records := hookEnv(t, canarySQLite(t, "CREATE TRIGGER "+canary+" BEFORE INSERT ON pack_reservations BEGIN SELECT RAISE(ABORT, '"+canary+"'); END"))
		blobDir(t)
		t.Setenv(EnvPackQuota, quota)
		err := preReceiveCase{pack: true}.run(t)
		checkFailedStep(t, err, readRecords(t, records), "pre-receive", "pre-receive: reserve pack bytes", canary)
	})
	t.Run("register packs", func(t *testing.T) {
		canary := canaryTable()
		records := hookEnv(t, canarySQLite(t, "CREATE TRIGGER "+canary+" BEFORE INSERT ON packs BEGIN SELECT RAISE(ABORT, '"+canary+"'); END"))
		blobDir(t)
		err := preReceiveCase{pack: true}.run(t)
		checkFailedStep(t, err, readRecords(t, records), "pre-receive", "pre-receive: register packs", canary)
	})
	t.Run("upload, then retire", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root writes into a read-only directory")
		}
		// A read-only blob directory with a canary name: the upload fails
		// and names it. pre-receive then retires the push's reservation,
		// because the blob is proven absent.
		dbURL := canarySQLite(t)
		records := hookEnv(t, dbURL)
		canary := canaryTable()
		dir := filepath.Join(t.TempDir(), canary)
		if err := os.Mkdir(dir, 0o500); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
		t.Setenv("OPENSTORAGE_S3_URL", "file://"+dir)
		t.Setenv(EnvPackQuota, quota)
		err := preReceiveCase{pack: true}.run(t)
		checkFailedStep(t, err, readRecords(t, records), "pre-receive", "pre-receive: upload", canary)
		rs, err := ref.Open(t.Context(), dbURL)
		if err != nil {
			t.Fatal(err)
		}
		defer rs.Close()
		ls, err := rs.QuotaLedgerState(t.Context(), blob.QuotaRepoGroup("t-refusal"), time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		if ls.ReservedBytes != 0 {
			t.Fatalf("a failed upload left %d reserved bytes; want the reservation retired", ls.ReservedBytes)
		}
	})
	t.Run("verify pack", func(t *testing.T) {
		// A registered pack whose blob is gone is a restore: pre-receive
		// skips the upload, and the verification after registration
		// takes the restore's quota cover, which the trigger fails.
		canary := canaryTable()
		dbURL := canarySQLite(t)
		records := hookEnv(t, dbURL)
		blobDir(t)
		t.Setenv(EnvPackQuota, quota)
		err := preReceiveCase{pack: true, beforeHook: func(t *testing.T, packID string) {
			rs, err := ref.Open(t.Context(), dbURL)
			if err != nil {
				t.Fatal(err)
			}
			defer rs.Close()
			if err := rs.RegisterPacks(t.Context(), "t-refusal", []ref.Pack{{ID: packID, Key: blob.PackKey("t-refusal", packID), Size: 1}}); err != nil {
				t.Fatal(err)
			}
			canarySQLExec(t, dbURL, "CREATE TRIGGER "+canary+" BEFORE INSERT ON pack_reservations BEGIN SELECT RAISE(ABORT, '"+canary+"'); END")
		}}.run(t)
		checkFailedStep(t, err, readRecords(t, records), "pre-receive", "pre-receive: verify", canary)
	})
}
