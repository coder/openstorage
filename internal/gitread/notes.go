package gitread

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"

	"github.com/coder/open-storage/internal/ref"
)

// Note reads the git note attached to object sha from notesRef in the repo
// at repoPath. Callers validate inputs: sha must be a full object id
// (ref.ValidSHA) and notesRef a refs/notes/ ref, so neither can smuggle
// options or rev expressions into git. A missing note wraps ErrNotFound —
// `git notes show` exits 1 for an unborn notes ref, an unknown object, and
// an object without a note alike (anything else is internal).
func Note(ctx context.Context, repoPath, notesRef, sha string) (string, error) {
	assertf(repoPath != "", "Note: empty repoPath")
	assertf(strings.HasPrefix(notesRef, "refs/notes/"), "Note: %q is not a notes ref", notesRef)
	assertf(ref.ValidSHA(sha), "Note: unvalidated sha %q", sha)
	out, err := runGit(ctx, repoPath, "notes", "--ref", notesRef, "show", sha)
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
			return "", fmt.Errorf("no note for object %s on %s: %w", sha, notesRef, ErrNotFound)
		}
		return "", err
	}
	return out, nil
}
