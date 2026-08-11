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
	Name string // short tag name (refname:short)
	SHA  string // peeled commit sha
}

// PeeledTags lists all tags of the repo at repoPath in lexical refname
// order (for-each-ref's default — the docs/03 GET /repos/tags order).
func PeeledTags(ctx context.Context, repoPath string) ([]Tag, error) {
	out, err := runGit(ctx, repoPath, "for-each-ref",
		"--format=%(refname:short)%00%(objectname)%00%(*objectname)", "refs/tags")
	if err != nil {
		return nil, err
	}
	tags := []Tag{}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if line == "" {
			continue
		}
		fields := strings.Split(line, "\x00")
		if len(fields) != 3 || fields[0] == "" {
			return nil, fmt.Errorf("for-each-ref emitted %q", line)
		}
		sha := fields[2] // %(*objectname): peeled target, empty for lightweight tags
		if sha == "" {
			sha = fields[1]
		}
		if !ref.ValidSHA(sha) {
			return nil, fmt.Errorf("for-each-ref emitted malformed sha in %q", line)
		}
		tags = append(tags, Tag{Name: fields[0], SHA: sha})
	}
	return tags, nil
}
