package githttp

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/ref"
	"github.com/coder/openstorage/internal/ref/s3wal"
)

// The update hook gets a command whose old and new values are both the zero
// id when a client deletes a ref that its replica does not list (see
// delete_absent_ref_e2e_test.go). A swap from nothing to nothing is an
// assertion in every ref store, so the hook must answer it before it swaps:
// ok when the store holds no such ref, a refusal when it holds one.
func TestUpdateHookAnswersAZeroToZeroCommand(t *testing.T) {
	const held, ghost = "refs/notes/held", "refs/notes/ghost"
	sha := strings.Repeat("c", 40)
	dbURL := "sqlite://" + filepath.Join(t.TempDir(), "refs.sqlite")
	st, err := ref.Open(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	if err := st.InitSchema(t.Context()); err != nil {
		t.Fatal(err)
	}
	if won, err := st.CASRef(t.Context(), "t-refusal", held, ref.ZeroSHA, sha); err != nil || !won {
		t.Fatalf("plant %s: won=%v err=%v", held, won, err)
	}
	records := hookEnv(t, dbURL)
	update := func(name string) error {
		return RunHook(t.Context(), []string{"update", name, ref.ZeroSHA, ref.ZeroSHA})
	}

	if err := update(ghost); err != nil {
		t.Fatalf("a delete of a ref that the store does not hold = %v, want ok", err)
	}
	err = update(held)
	if err == nil || !ShownToPusher(err) || !strings.Contains(err.Error(), held+" exists at "+sha[:8]) {
		t.Fatalf("a delete of a ref that the store holds = %v, want a refusal that names the ref and its sha", err)
	}
	if line := readRecords(t, records); !strings.Contains(line, "a push deletes a ref that its replica does not list") || !strings.Contains(line, held) {
		t.Fatalf("the hook-errors file has no operator line for %s:\n%s", held, line)
	}
	if got, err := st.GetRef(t.Context(), "t-refusal", held); err != nil || got != sha {
		t.Fatalf("%s = %q, %v after the refused delete, want %s", held, got, err, sha)
	}
	if _, err := st.GetRef(t.Context(), "t-refusal", ghost); !errors.Is(err, ref.ErrNotFound) {
		t.Fatalf("%s: %v, want ErrNotFound: the ok delete must not create it", ghost, err)
	}
}

// getRefFails is a ref store whose point read fails.
type getRefFails struct {
	ref.RefStore
	err error
}

func (s getRefFails) GetRef(context.Context, string, string) (string, error) { return "", s.err }

// The answer rests on a point read of the ref store (every store has one), so
// it holds on each store that the hook opens. PostgreSQL runs in the e2e lane.
// A failed read is a failure of the hook, never an ok and never a refusal that
// the pusher reads.
func TestAnswerDeleteOfAbsentRefOnEachStore(t *testing.T) {
	const name = "refs/notes/x"
	sha := strings.Repeat("d", 40)
	sqlite, err := ref.Open(t.Context(), "sqlite://"+filepath.Join(t.TempDir(), "refs.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sqlite.Close() })
	if err := sqlite.InitSchema(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		store ref.RefStore
	}{
		{"sqlite", sqlite},
		{"s3wal", ref.NewS3WAL(s3wal.NewMemCAS(), time.Now)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := "repo_" + tc.name
			records, err := createHookErrors(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv(EnvHookErrors, records)
			if err := answerDeleteOfAbsentRef(t.Context(), tc.store, repo, name); err != nil {
				t.Fatalf("absent: %v, want nil", err)
			}
			if won, err := tc.store.CASRef(t.Context(), repo, name, ref.ZeroSHA, sha); err != nil || !won {
				t.Fatalf("create %s: won=%v err=%v", name, won, err)
			}
			if err := answerDeleteOfAbsentRef(t.Context(), tc.store, repo, name); err == nil || !ShownToPusher(err) {
				t.Fatalf("held: %v, want a refusal that the pusher reads", err)
			}
			boom := errors.New("the ref store is down")
			err = answerDeleteOfAbsentRef(t.Context(), getRefFails{tc.store, boom}, repo, name)
			if !errors.Is(err, boom) || ShownToPusher(err) {
				t.Fatalf("a failed read: %v, want the store's error, not shown to the pusher", err)
			}
		})
	}
}
