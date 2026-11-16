package jobs

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/sync"
)

// The insert-only client (the hook's) logs River's lines to its logger,
// never to stdout: git relays a hook's stdout to the pusher, and River's
// insert-notification failure carries the database error as a string.
func TestRiverInsertOnlyLogsToItsLogger(t *testing.T) {
	ctx := context.Background()
	env := newLaneScanEnv(t, ctx)
	// A pg_notify that fails with a canary, ahead of pg_catalog's on the
	// search path, as a database whose notify fails would.
	schema := "canary_" + strings.ToLower(rand.Text()[:8])
	canary := "canary-host-" + strings.ToLower(rand.Text()[:8]) + ".invalid"
	env.exec(t, ctx, `CREATE SCHEMA `+schema)
	env.exec(t, ctx, `CREATE FUNCTION `+schema+`.pg_notify(text, text) RETURNS void LANGUAGE plpgsql AS
		$$ BEGIN RAISE EXCEPTION 'could not reach `+canary+`'; END $$`)
	u, err := url.Parse(env.pgURL)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", schema+",pg_catalog,public")
	u.RawQuery = q.Encode()

	stdout, err := os.CreateTemp(t.TempDir(), "stdout")
	if err != nil {
		t.Fatal(err)
	}
	prev := os.Stdout
	os.Stdout = stdout
	var logs bytes.Buffer
	ins, err := NewRiverInsertOnly(ctx, u.String(), slog.New(slog.NewTextHandler(&logs, nil)))
	os.Stdout = prev
	if err != nil {
		t.Fatal(err)
	}
	defer ins.Close()
	if err := ins.EnqueueSync(ctx, sync.Job{Org: "org", RepoID: "repo-1"}); err == nil {
		t.Fatal("EnqueueSync succeeded with a failing pg_notify; want the canary error")
	}
	if !strings.Contains(logs.String(), canary) {
		t.Fatalf("the client's logger lacks River's notify failure: %q", logs.String())
	}
	if _, err := stdout.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(stdout)
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(b), canary); n != 0 {
		t.Fatalf("stdout names the canary %d times: %s", n, b)
	}
}
