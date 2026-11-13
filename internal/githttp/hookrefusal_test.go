package githttp

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref"
)

// hookFailedLine is what the pusher sees for an update hook error that is
// not a refusal.
const hookFailedLine = "update failed; push again later (the server log has the cause)"

// storeCanary is a store URL whose every part is a unique canary.
type storeCanary struct {
	url   string
	parts map[string]string // part name -> canary; "password" must never show
}

func pgCanary() storeCanary {
	p := map[string]string{
		"user": "canary_user_" + rand.Text()[:8], "password": "canary_pass_" + rand.Text()[:8],
		"host": "canary-host-" + strings.ToLower(rand.Text()[:8]) + ".invalid", "database": "canary_db_" + rand.Text()[:8],
	}
	return storeCanary{
		url:   "postgres://" + p["user"] + ":" + p["password"] + "@" + p["host"] + ":5432/" + p["database"] + "?connect_timeout=2",
		parts: p,
	}
}

func s3walCanary() storeCanary {
	p := map[string]string{
		"bucket":   "canary-bucket-" + strings.ToLower(rand.Text()[:8]),
		"endpoint": "canary-s3-" + strings.ToLower(rand.Text()[:8]) + ".invalid",
	}
	return storeCanary{url: "s3wal://" + p["bucket"] + "?endpoint=http://" + p["endpoint"], parts: p}
}

// hookEnv sets the env of one update hook for repo "t-refusal", with a
// hook-errors file, and returns that file's path.
func hookEnv(t *testing.T, storeURL string) string {
	t.Helper()
	records, err := createHookErrors(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range map[string]string{
		EnvHookErrors: records, "OPENSTORAGE_PG_URL": storeURL, EnvRepo: "t-refusal", EnvOrg: "t-org",
		EnvPushHold: "", EnvSyncClaim: "", EnvRefsPolicy: "", EnvOps: "", EnvAtomic: "", EnvScopes: "",
		"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test", "AWS_SESSION_TOKEN": "",
		"AWS_REGION": "us-east-1", "AWS_EC2_METADATA_DISABLED": "true",
	} {
		t.Setenv(k, v)
	}
	return records
}

// checkHidden fails unless the pusher's text names no canary part and the
// hook-errors records name want (the operator gets the cause) and no
// password.
func checkHidden(t *testing.T, pusher, records string, c storeCanary, want ...string) {
	t.Helper()
	for part, v := range c.parts {
		if n := strings.Count(pusher, v); n != 0 {
			t.Errorf("the pusher's text names the canary %s %d times", part, n)
		}
	}
	if n := strings.Count(records, c.parts["password"]); c.parts["password"] != "" && n != 0 {
		t.Errorf("the hook-errors records name the canary password %d times", n)
	}
	for _, part := range want {
		if !strings.Contains(records, c.parts[part]) {
			t.Errorf("the hook-errors records do not name the canary %s: %s", part, records)
		}
	}
}

func readRecords(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// casFails is a ref store whose CAS fails with err.
type casFails struct {
	ref.RefStore
	err error
}

func (s casFails) CASRef(context.Context, string, string, string, string) (bool, error) {
	return false, s.err
}

func (s casFails) CASRefGen(context.Context, string, string, string, string) (bool, int64, error) {
	return false, 0, s.err
}

// The update hook shows the pusher no part of the store's connection
// string when a store step fails, and writes the cause to the hook-errors
// file: the ref store open, the authority D/F probe (S3-WAL), the CAS, the
// sync claim fence and verify-sig's metadata open.
func TestUpdateHookHidesStoreErrors(t *testing.T) {
	sha := strings.Repeat("a", 40)
	update := func() error {
		return RunHook(t.Context(), []string{"update", "refs/heads/main", ref.ZeroSHA, sha})
	}
	t.Run("ref store open", func(t *testing.T) {
		c := pgCanary()
		records := hookEnv(t, c.url)
		err := update()
		if err == nil || err.Error() != hookFailedLine {
			t.Fatalf("update = %v; want %q", err, hookFailedLine)
		}
		got := readRecords(t, records)
		checkHidden(t, err.Error(), got, c, "host", "user", "database")
	})
	t.Run("authority probe on S3-WAL", func(t *testing.T) {
		c := s3walCanary()
		records := hookEnv(t, c.url)
		err := update()
		if err == nil || err.Error() != hookFailedLine {
			t.Fatalf("update = %v; want %q", err, hookFailedLine)
		}
		got := readRecords(t, records)
		if !strings.Contains(got, "directory/file probe") {
			t.Fatalf("the failed step is not the authority probe: %s", got)
		}
		checkHidden(t, err.Error(), got, c, "bucket", "endpoint")
	})
	t.Run("CAS", func(t *testing.T) {
		dbURL := "sqlite://" + filepath.Join(t.TempDir(), "refs.sqlite")
		st, err := ref.Open(t.Context(), dbURL)
		if err != nil {
			t.Fatal(err)
		}
		if err := st.InitSchema(t.Context()); err != nil {
			t.Fatal(err)
		}
		st.Close()
		c := pgCanary()
		records := hookEnv(t, dbURL)
		prev := WrapHookRefStore
		t.Cleanup(func() { WrapHookRefStore = prev })
		WrapHookRefStore = func(_ context.Context, st ref.RefStore) (ref.RefStore, error) {
			return casFails{st, fmt.Errorf("pg cas: failed to connect to `user=%s database=%s`: lookup %s: no such host",
				c.parts["user"], c.parts["database"], c.parts["host"])}, nil
		}
		err = update()
		if err == nil || err.Error() != hookFailedLine {
			t.Fatalf("update = %v; want %q", err, hookFailedLine)
		}
		checkHidden(t, err.Error(), readRecords(t, records), c, "host", "user", "database")
	})
	t.Run("sync claim fence", func(t *testing.T) {
		c := pgCanary()
		records := hookEnv(t, c.url)
		t.Setenv(EnvSyncClaim, "holder")
		err := update()
		if err == nil || err.Error() != hookFailedLine {
			t.Fatalf("update = %v; want %q", err, hookFailedLine)
		}
		got := readRecords(t, records)
		if !strings.Contains(got, "sync claim fence") {
			t.Fatalf("the failed step is not the fence: %s", got)
		}
		checkHidden(t, err.Error(), got, c, "host", "user", "database")
	})
	t.Run("verify-sig metadata open", func(t *testing.T) {
		c := pgCanary()
		records := hookEnv(t, c.url)
		commit := unverifiedCommit(t)
		t.Setenv(EnvRefsPolicy, `[["refs/heads/main",["verify-sig"]]]`)
		err := RunHook(t.Context(), []string{"update", "refs/heads/main", ref.ZeroSHA, commit})
		if err == nil || err.Error() != hookFailedLine {
			t.Fatalf("update = %v; want %q", err, hookFailedLine)
		}
		got := readRecords(t, records)
		if !strings.Contains(got, "verify-sig: open metadata store") {
			t.Fatalf("the failed step is not verify-sig's metadata open: %s", got)
		}
		checkHidden(t, err.Error(), got, c, "host", "user", "database")
	})
}

// unverifiedCommit makes a commit that no ref reaches, so verify-sig must
// check it, and points the hook at its repository and at an empty verify
// state for one push. gitFlags go before git's subcommand, such as
// sshSigningArgs.
func unverifiedCommit(t *testing.T, gitFlags ...string) string {
	t.Helper()
	repo := t.TempDir()
	for _, args := range [][]string{{"init", "-q"}, {"commit", "-q", "--allow-empty", "-m", "unverified"}} {
		if out, err := exec.Command("git", append(append(append([]string{"-C", repo}, gitIdentity...), gitFlags...), args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	out, err := exec.Command("git", "-C", repo, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	commit := strings.TrimSpace(string(out))
	if out, err := exec.Command("git", "-C", repo, "update-ref", "-d", "HEAD").CombinedOutput(); err != nil {
		t.Fatalf("drop the ref: %v: %s", err, out)
	}
	t.Setenv("GIT_DIR", filepath.Join(repo, ".git"))
	state := filepath.Join(t.TempDir(), "state")
	if err := os.WriteFile(state, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvVerifyState, state)
	return commit
}

// signingKeyEnv sets up a SQLite metadata store whose org "t-org" has one
// registered signing key with keyText, and the hook env for it. It returns
// the hook-errors file's path.
func signingKeyEnv(t *testing.T, keyText string) string {
	t.Helper()
	dbURL := "sqlite://" + filepath.Join(t.TempDir(), "meta.db")
	ms, err := meta.Open(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Close()
	if err := ms.InitSchema(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.UpsertOrgWithKeys(t.Context(), "t-org", []string{"-----BEGIN PUBLIC KEY-----\ndummy\n-----END PUBLIC KEY-----\n"}); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.AddSigningKey(t.Context(), "t-org", keyText); err != nil {
		t.Fatal(err)
	}
	records := hookEnv(t, dbURL)
	t.Setenv(EnvRefsPolicy, `[["*",["verify-sig"]]]`)
	return records
}

// A registered signing key that the hook cannot use: every ref at the tip
// gets a denial that names no cause, and the hook-errors file has the
// cause from the first evaluation, which is the only one that sees it.
func TestVerifySigKeepsTheKeyringCauseOffThePusher(t *testing.T) {
	records := signingKeyEnv(t, "not a signing key")
	commit := unverifiedCommit(t)
	for _, name := range []string{"refs/heads/main", "refs/heads/other"} {
		err := RunHook(t.Context(), []string{"update", name, ref.ZeroSHA, commit})
		want := "verify-sig denied " + name + " at " + commit + ": signing keyring unusable (the server log has the cause)"
		if err == nil || err.Error() != want {
			t.Fatalf("%s = %v; want %q", name, err, want)
		}
	}
	const cause = "neither an SSH public key line nor an armored PGP block"
	if got := readRecords(t, records); strings.Count(got, cause) != 1 {
		t.Fatalf("the hook-errors file does not have the keyring cause once: %s", got)
	}
}

// A signed commit that the verifier rejects: the denial gives the
// documented reason, not the verifier's output, which names the temporary
// allowed-signers file. The hook-errors file has the output.
func TestVerifySigKeepsVerifierOutputOffThePusher(t *testing.T) {
	// An SSH key line whose key does not decode: the keyring builds, and
	// ssh-keygen rejects the allowed-signers file during the check.
	records := signingKeyEnv(t, "ssh-ed25519 AAAAnotakey")
	priv, _ := genSSHKey(t)
	commit := unverifiedCommit(t, sshSigningArgs(priv)...)
	err := RunHook(t.Context(), []string{"update", "refs/heads/main", ref.ZeroSHA, commit})
	want := "verify-sig denied refs/heads/main at " + commit + ": no registered key matches the signature (the server log has the verifier's output)"
	if err == nil || err.Error() != want {
		t.Fatalf("update = %v; want %q", err, want)
	}
	if got := readRecords(t, records); !strings.Contains(got, "allowed_signers") || !strings.Contains(got, commit) {
		t.Fatalf("the hook-errors file lacks the verifier's output for the commit: %s", got)
	}
}

// A refusal reaches the pusher whole, through wrappers too. An error that
// also wraps a non-refusal does not.
func TestShownToPusher(t *testing.T) {
	store := errors.New("dial tcp canary-host.invalid")
	deny := refusef("policy no-push denies update to %s", "refs/heads/main")
	for _, tc := range []struct {
		name string
		err  error
		safe bool
	}{
		{"refusal", deny, true},
		{"wrapped refusal", atomicDenied(deny), true},
		{"wrapped twice", fmt.Errorf("update: %w", atomicDenied(deny)), true},
		{"sentinel", fmt.Errorf("pg cas: %w", ref.ErrRepoPurged), true},
		{"LFS cap sentinel", fmt.Errorf("lfs: %w", ref.ErrLFSObjectCapExceeded), true},
		{"ref name sentinel", fmt.Errorf("cas: %w", ref.ErrRefNameNotUTF8), true},
		// The refusal mark is checked before any wrapper: a refusal shows
		// its whole text even when it wraps an error that is not one,
		// such as a hold give-up's ErrRefHeld mark. The mark inside such a
		// wrapper does not.
		{"refusal around a non-refusal", Refuse(heldUnlessCanceled(context.Background(), errors.New("a concurrent push holds refs/heads/main"))), true},
		{"refusal inside a non-refusal", heldUnlessCanceled(context.Background(), deny), false},
		{"store error", store, false},
		{"wrapped store error", fmt.Errorf("update: directory/file probe for x: %w", store), false},
		{"store error formatted with %v", fmt.Errorf("pre-receive: %v", deny), false},
		{"joined with a store error", errors.Join(deny, store), false},
		{"joined refusals", errors.Join(deny, refusef("quota exceeded")), true},
	} {
		if got := ShownToPusher(tc.err); got != tc.safe {
			t.Errorf("%s: ShownToPusher = %v, want %v", tc.name, got, tc.safe)
		}
	}
	if !errors.Is(Refuse(fmt.Errorf("x: %w", ref.ErrRepoPurged)), ref.ErrRepoPurged) {
		t.Error("Refuse hides the wrapped error from errors.Is")
	}
}

// A policy refusal reaches the pusher unchanged, and nothing goes to the
// hook-errors file.
func TestUpdateHookKeepsRefusals(t *testing.T) {
	records := hookEnv(t, pgCanary().url)
	t.Setenv(EnvRefsPolicy, `[["refs/heads/main",["no-push"]]]`)
	err := RunHook(t.Context(), []string{"update", "refs/heads/main", ref.ZeroSHA, strings.Repeat("a", 40)})
	if err == nil || err.Error() != "policy no-push denies update to refs/heads/main" {
		t.Fatalf("update = %v; want the policy refusal", err)
	}
	if got := readRecords(t, records); got != "" {
		t.Fatalf("a refusal wrote hook-errors records: %s", got)
	}
}

// hookServe serves a repository named runUnique(name) in process, with the
// hooks' store URLs taken from the environment, and returns a push of the
// work repository's HEAD to refs/heads/main that returns the pusher's
// output.
func hookServe(t *testing.T, name string) func(work string) (string, error) {
	t.Helper()
	repo := runUnique(name)
	key, pemBlock := genKeyPEM(t, "ES256")
	laneStore, laneBlob := openLaneStores(t)
	srv := &Server{
		Store:    laneStore,
		Blob:     laneBlob,
		CacheDir: t.TempDir(),
		Verifier: auth.NewVerifier(memKeys{testOrg: {pemBlock}}),
		Org:      testOrg,
		ResolveRepo: func(_ context.Context, base string) (meta.Repo, bool) {
			if base != repo {
				return meta.Repo{}, false
			}
			return meta.Repo{RepoID: repo, URL: repo, DefaultBranch: "main"}, true
		},
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	token := mintUnitToken(t, key, "ES256", unitClaims(testOrg, repo, auth.ScopeGitRead, auth.ScopeGitWrite))
	url := strings.Replace(ts.URL, "http://", "http://t:"+token+"@", 1) + "/" + repo + ".git"
	return func(work string) (string, error) { return tryGit(work, "push", url, "HEAD:refs/heads/main") }
}

// A push through serve whose pre-receive hook cannot use a store, one case
// per failed step: the pusher sees the fixed line and no part of the store
// URL, and serve's log has the failed step and the cause.
func TestServeHidesHookStoreErrors(t *testing.T) {
	check := func(t *testing.T, out string, err error, logs *syncBuffer, c storeCanary, step string, want ...string) {
		t.Helper()
		if err == nil {
			t.Fatalf("push succeeded with a store the hooks cannot use:\n%s", out)
		}
		if !strings.Contains(out, "pre-receive failed; push again later (the server log has the cause)") {
			t.Fatalf("the pusher's output lacks the fixed line: %s", out)
		}
		for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline) && !strings.Contains(logs.String(), step); {
			time.Sleep(10 * time.Millisecond)
		}
		if !strings.Contains(logs.String(), step) {
			t.Fatalf("serve's log lacks the failed step %q: %s", step, logs.String())
		}
		checkHidden(t, out, logs.String(), c, want...)
	}
	t.Run("reservation probe ref store open", func(t *testing.T) {
		c := pgCanary()
		t.Setenv("OPENSTORAGE_PG_URL", c.url)
		t.Setenv("OPENSTORAGE_S3_URL", blobURL)
		logs := captureServeLog(t)
		out, err := hookServe(t, "t-hook-store-probe")(initWorkRepo(t))
		check(t, out, err, logs, c, "open ref store for reservation probe", "host", "user", "database")
	})
	t.Run("ref store open", func(t *testing.T) {
		// An update creates no ref, so pre-receive skips the reservation
		// probe and first opens the ref store after the blob stat.
		t.Setenv("OPENSTORAGE_PG_URL", dbURL)
		t.Setenv("OPENSTORAGE_S3_URL", blobURL)
		logs := captureServeLog(t)
		push := hookServe(t, "t-hook-store-open")
		work := initWorkRepo(t)
		if out, err := push(work); err != nil {
			t.Fatalf("first push: %v\n%s", err, out)
		}
		commitFile(t, work, "b.txt", "two\n", "two", nil)
		c := pgCanary()
		t.Setenv("OPENSTORAGE_PG_URL", c.url)
		out, err := push(work)
		check(t, out, err, logs, c, c.parts["host"], "host", "user", "database")
		if strings.Contains(logs.String(), "reservation probe") {
			t.Fatalf("the update failed in the reservation probe: %s", logs.String())
		}
	})
	t.Run("SQLite path", func(t *testing.T) {
		// A regular file where the database's directory goes: the open
		// fails and its error names the path.
		dir, name := t.TempDir(), "canary-file-"+strings.ToLower(rand.Text()[:8])
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		c := storeCanary{url: "sqlite://" + filepath.Join(dir, name, "refs.sqlite"), parts: map[string]string{"path": name}}
		t.Setenv("OPENSTORAGE_PG_URL", c.url)
		t.Setenv("OPENSTORAGE_S3_URL", blobURL)
		logs := captureServeLog(t)
		out, err := hookServe(t, "t-hook-store-sqlite")(initWorkRepo(t))
		check(t, out, err, logs, c, "sqlite mkdir", "path")
	})
	t.Run("S3 blob stat", func(t *testing.T) {
		p := map[string]string{
			"bucket":   "canary-bucket-" + strings.ToLower(rand.Text()[:8]),
			"endpoint": "canary-s3-" + strings.ToLower(rand.Text()[:8]) + ".invalid",
		}
		c := storeCanary{url: "s3://" + p["bucket"] + "?endpoint=http://" + p["endpoint"] + "&s3ForcePathStyle=true&region=us-east-1", parts: p}
		// The S3 lanes set real credentials for the lane's own stores.
		if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
			t.Setenv("AWS_ACCESS_KEY_ID", "test")
			t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
		}
		t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
		t.Setenv("OPENSTORAGE_PG_URL", dbURL)
		t.Setenv("OPENSTORAGE_S3_URL", c.url)
		logs := captureServeLog(t)
		out, err := hookServe(t, "t-hook-store-stat")(initWorkRepo(t))
		check(t, out, err, logs, c, "pre-receive: stat", "bucket", "endpoint")
	})
	t.Run("blob upload", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root writes into a read-only directory")
		}
		// A read-only blob directory: the stat finds no pack, and the
		// upload fails with an error that names the path.
		name := "canary-dir-" + strings.ToLower(rand.Text()[:8])
		dir := filepath.Join(t.TempDir(), name)
		if err := os.Mkdir(dir, 0o500); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
		c := storeCanary{url: "file://" + dir, parts: map[string]string{"path": name}}
		t.Setenv("OPENSTORAGE_PG_URL", dbURL)
		t.Setenv("OPENSTORAGE_S3_URL", c.url)
		logs := captureServeLog(t)
		out, err := hookServe(t, "t-hook-store-upload")(initWorkRepo(t))
		check(t, out, err, logs, c, "pre-receive: upload", "path")
	})
}

// The real `openstorage hook` command with a store it cannot reach: its
// stderr, which git relays to the pusher, names no part of the store URL,
// and the hook-errors file has the cause of both failed steps.
func TestHookBinaryKeepsStoreErrorsOffStderr(t *testing.T) {
	c := pgCanary()
	records, err := createHookErrors(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), binPath, "hook", "update", "refs/heads/main", ref.ZeroSHA, strings.Repeat("a", 40))
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + t.TempDir(),
		"OPENSTORAGE_ORG=" + testOrg, EnvRepo + "=t-hook-binary", "OPENSTORAGE_PG_URL=" + c.url,
		"OPENSTORAGE_S3_URL=" + blobURL, EnvHookErrors + "=" + records}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err == nil {
		t.Fatal("the hook accepted an update with a store it cannot reach")
	}
	for _, want := range []string{"open meta store", "openstorage hook: " + hookFailedLine} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("stderr lacks %q: %s", want, stderr.String())
		}
	}
	got := readRecords(t, records)
	if !strings.Contains(got, "open meta store") || !strings.Contains(got, "update failed") {
		t.Fatalf("the hook-errors file lacks a failed step: %s", got)
	}
	checkHidden(t, stderr.String(), got, c, "host", "user", "database")
}

// A panic in a hook reaches the pusher as Go's panic output, which names
// server paths. RunHook turns it into the fixed line and writes the panic
// and its stack to the hook-errors file.
func TestRunHookTurnsAPanicIntoTheFixedLine(t *testing.T) {
	sha := strings.Repeat("a", 40)
	for _, args := range [][]string{{"update", "refs/heads/main", ref.ZeroSHA, sha}, {"pre-receive"}} {
		t.Run(args[0], func(t *testing.T) {
			records := hookEnv(t, canarySQLite(t))
			t.Setenv(EnvRepo, "") // mustEnv panics on it
			err := RunHook(t.Context(), args)
			if want := args[0] + " failed; push again later (the server log has the cause)"; err == nil || err.Error() != want {
				t.Fatalf("RunHook = %v, want %q", err, want)
			}
			got := readRecords(t, records)
			for _, want := range []string{"required env " + EnvRepo + " is empty", "goroutine"} {
				if !strings.Contains(got, want) {
					t.Errorf("the hook-errors records lack %q: %.600s", want, got)
				}
			}
		})
	}
}
