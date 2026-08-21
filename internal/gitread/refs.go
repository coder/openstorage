package gitread

import (
	"context"
	"fmt"
	"strings"

	"codestorage/internal/ref"
)

// Tag is one refs/tags entry. SHA is the PEELED commit sha for annotated
// tags (invariant 10, docs/01:78-80) and the target itself for lightweight
// tags.
type Tag struct {
	Name string // short tag name (refs/tags/ stripped)
	SHA  string // peeled commit sha
}

// tagRefPrefix is the fixed prefix stripped off %(refname) below. NOT
// %(refname:short): short is a non-ambiguous ABBREVIATION, so a tag whose
// name collides with a branch comes out as "tags/<name>" and exact-name
// matching (GET /repos/tag) misses it.
const tagRefPrefix = "refs/tags/"

// PeeledTags lists all tags of the repo at repoPath in lexical refname
// order (for-each-ref's default — the docs/03 GET /repos/tags order).
// Invariant 10 wants the peeled COMMIT sha, so tags that reference (or
// peel to) a non-commit object — trees or blobs, pushable with plain git —
// are skipped rather than surfaced with a non-commit sha.
func PeeledTags(ctx context.Context, repoPath string) ([]Tag, error) {
	out, err := runGit(ctx, repoPath, "for-each-ref",
		"--format=%(refname)%00%(objectname)%00%(objecttype)%00%(*objectname)%00%(*objecttype)", "refs/tags")
	if err != nil {
		return nil, err
	}
	tags := []Tag{}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if line == "" {
			continue
		}
		fields := strings.Split(line, "\x00")
		if len(fields) != 5 || !strings.HasPrefix(fields[0], tagRefPrefix) || fields[0] == tagRefPrefix {
			return nil, fmt.Errorf("for-each-ref emitted %q", line)
		}
		name, sha, objType, peeledSHA, peeledType :=
			strings.TrimPrefix(fields[0], tagRefPrefix), fields[1], fields[2], fields[3], fields[4]
		switch {
		case objType == "commit":
			// Lightweight tag directly on a commit.
		case objType == "tag" && peeledType == "commit":
			// Annotated tag: %(*objectname) peels (nested tags included).
			sha = peeledSHA
		default:
			continue // tag on a tree/blob: no commit to report
		}
		if !ref.ValidSHA(sha) {
			return nil, fmt.Errorf("for-each-ref emitted malformed sha in %q", line)
		}
		tags = append(tags, Tag{Name: name, SHA: sha})
	}
	return tags, nil
}
