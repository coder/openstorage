package api

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/auth"
)

// TestGrepNamesEveryInvalidField: POST /repos/grep names each invalid body
// field, in the order of the docs table (query.pattern, paths, context,
// limits, pagination.cursor, pagination.limit). What needs the repository
// stays after the stateless checks: a cursor alone answers after the
// repository lookup, and a cursor anchor, a ref and a pattern that git
// refuses are never part of the batch.
func TestGrepNamesEveryInvalidField(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := ipNewFixture(t, e)
		qfx := queryFixtureFor(t, e)
		missing := e.mint(t, tokenSpec{iss: e.newOrg(t), repo: "ip-missing-" + randomHex(t, 4), scopes: []string{auth.ScopeGitRead}})
		const path = "/api/v1/repos/grep"
		maxLines := fmt.Sprintf("must be between 1 and %d", grepMaxLinesCap)
		maxLinesMsg := fmt.Sprintf("invalid limits.max_lines (want 1..%d)", grepMaxLinesCap)
		// 130 empty paths are 130 problems: the answer holds 100 items.
		manyEntries := `["` + strings.Repeat(`","`, 129) + `"]`
		var manyPaths []ipParam
		var manyMsgs []string
		for i := range maxInvalidParams {
			manyPaths = append(manyPaths, ipParam{fmt.Sprintf("paths.%d", i), "must be a non-empty path"})
			manyMsgs = append(manyMsgs, `invalid paths entry ""`)
		}
		// A well-formed cursor whose commit is in no repository.
		goneCursor := encodeGrepCursor(strings.Repeat("a", 40), "f.txt", 1)

		ipRun(t, e, []ipCase{
			{
				name: "every field", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"query":{"pattern":" "},"paths":["","../x","ok","a\u0000b"],"context":{"before":-1,"after":-2},` +
					`"limits":{"max_lines":0,"max_matches_per_file":0},"pagination":{"cursor":"!!","limit":0}}`,
				wantError: `query.pattern is required; invalid paths entry ""; paths entry "../x" is outside the repository` +
					`; invalid paths entry "a\x00b"; context lines must be non-negative; ` + maxLinesMsg +
					"; invalid limits.max_matches_per_file (want >= 1); invalid cursor; invalid pagination.limit (want >= 1)",
				want: []ipParam{
					{"query.pattern", "is required"},
					{"paths.0", "must be a non-empty path"},
					{"paths.1", "is outside the repository"},
					{"paths.3", "must be a non-empty path"},
					{"context.before", "must be at least 0"},
					{"context.after", "must be at least 0"},
					{"limits.max_lines", maxLines},
					{"limits.max_matches_per_file", "must be at least 1"},
					{"pagination.cursor", "is not a valid cursor"},
					{"pagination.limit", "must be at least 1"},
				},
			},
			{
				name: "one entry of paths is one field", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"query":{"pattern":"x"},"paths":["../\u0000"],"limits":{"max_lines":0}}`,
				wantError: `invalid paths entry "../\x00"; ` + maxLinesMsg,
				want:      []ipParam{{"paths.0", "must be a non-empty path"}, {"limits.max_lines", maxLines}},
			},
			{
				name: "the context pair is one message naming each side", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"query":{"pattern":""},"context":{"before":-1,"after":-1}}`,
				wantError: "query.pattern is required; context lines must be non-negative",
				want: []ipParam{
					{"query.pattern", "is required"}, {"context.before", "must be at least 0"}, {"context.after", "must be at least 0"},
				},
			},
			{
				name: "a cursor is named with the other fields of the body", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"query":{"pattern":""},"pagination":{"cursor":"!!"}}`,
				wantError: "query.pattern is required; invalid cursor",
				want:      []ipParam{{"query.pattern", "is required"}, {"pagination.cursor", "is not a valid cursor"}},
			},
			{
				name: "a cursor goes before pagination.limit", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"query":{"pattern":"x"},"pagination":{"limit":0,"cursor":"!!"}}`,
				wantError: "invalid cursor; invalid pagination.limit (want >= 1)",
				want:      []ipParam{{"pagination.cursor", "is not a valid cursor"}, {"pagination.limit", "must be at least 1"}},
			},
			{
				name: "the body is checked before the repository, so a missing repository does not matter", method: http.MethodPost, path: path, token: missing,
				body:      `{"query":{"pattern":""},"limits":{"max_lines":0},"pagination":{"cursor":"!!"}}`,
				wantError: "query.pattern is required; " + maxLinesMsg + "; invalid cursor",
				want: []ipParam{
					{"query.pattern", "is required"}, {"limits.max_lines", maxLines}, {"pagination.cursor", "is not a valid cursor"},
				},
			},
			{
				name: "a cursor anchor that is gone is not part of the batch", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"query":{"pattern":"x"},"limits":{"max_lines":0},"pagination":{"cursor":"` + goneCursor + `"}}`,
				wantError: maxLinesMsg,
				want:      []ipParam{{"limits.max_lines", maxLines}},
			},
			{
				name: "a pattern that git refuses is not part of the batch", method: http.MethodPost, path: path, token: qfx.readToken(t, e),
				body:      `{"query":{"pattern":"["},"limits":{"max_matches_per_file":0}}`,
				wantError: "invalid limits.max_matches_per_file (want >= 1)",
				want:      []ipParam{{"limits.max_matches_per_file", "must be at least 1"}},
			},

			{
				name: "the answer holds at most 100 items", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"query":{"pattern":"x"},"paths":` + manyEntries + `}`,
				wantError: strings.Join(manyMsgs, "; ") + "; and 30 more",
				want:      manyPaths,
			},

			// One problem answers as before.
			{
				name: "one problem: pattern", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"query":{"pattern":""}}`, wantError: "query.pattern is required",
				want: []ipParam{{"query.pattern", "is required"}},
			},
			{
				name: "one problem: paths entry outside", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"query":{"pattern":"x"},"paths":["d","../nope"]}`, wantError: `paths entry "../nope" is outside the repository`,
				want: []ipParam{{"paths.1", "is outside the repository"}},
			},
			{
				name: "one problem: context.after", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"query":{"pattern":"x"},"context":{"after":-1}}`, wantError: "context lines must be non-negative",
				want: []ipParam{{"context.after", "must be at least 0"}},
			},
			{
				name: "one problem: max_lines", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"query":{"pattern":"x"},"limits":{"max_lines":5000}}`, wantError: maxLinesMsg,
				want: []ipParam{{"limits.max_lines", maxLines}},
			},
			{
				name: "one problem: max_matches_per_file", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"query":{"pattern":"x"},"limits":{"max_matches_per_file":-1}}`, wantError: "invalid limits.max_matches_per_file (want >= 1)",
				want: []ipParam{{"limits.max_matches_per_file", "must be at least 1"}},
			},
			{
				name: "one problem: pagination.limit", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"query":{"pattern":"x"},"pagination":{"limit":0}}`, wantError: "invalid pagination.limit (want >= 1)",
				want: []ipParam{{"pagination.limit", "must be at least 1"}},
			},
			{
				name: "one problem: a cursor that does not decode", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"query":{"pattern":"x"},"pagination":{"cursor":"!!"}}`, wantError: "invalid cursor",
				want: []ipParam{{"pagination.cursor", "is not a valid cursor"}},
			},
			{
				name: "one problem: a cursor whose anchor is gone", method: http.MethodPost, path: path, token: qfx.readToken(t, e),
				body: `{"query":{"pattern":"x"},"pagination":{"cursor":"` + goneCursor + `"}}`, wantError: "invalid cursor",
				want: []ipParam{{"pagination.cursor", "is not a valid cursor"}},
			},
			{
				name: "one problem: a pattern that git refuses", method: http.MethodPost, path: path, token: qfx.readToken(t, e),
				body: `{"query":{"pattern":"["}}`, wantError: "invalid query.pattern",
				want: []ipParam{{"query.pattern", "must be a valid pattern"}},
			},
		})

		// A cursor that is the only problem of the body answers after the
		// repository lookup, so a missing repository is still a 404.
		t.Run("a cursor alone on a missing repository is 404", func(t *testing.T) {
			resp, body := e.doReq(t, http.MethodPost, path, missing, []byte(`{"query":{"pattern":"x"},"pagination":{"cursor":"!!"}}`), "")
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("status = %d, want 404 (body %s)", resp.StatusCode, body)
			}
			requireProblemCode(t, resp, body, codeRepositoryNotFound)
		})
	})
}

// TestArchiveNamesEveryInvalidField: POST /repos/archive names each invalid
// body field, in the order of the docs table (include_globs, exclude_globs,
// max_blob_size, archive.prefix). Every check is stateless, so the 400
// comes before the repository lookup.
func TestArchiveNamesEveryInvalidField(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := ipNewFixture(t, e)
		missing := e.mint(t, tokenSpec{iss: e.newOrg(t), repo: "ip-missing-" + randomHex(t, 4), scopes: []string{auth.ScopeGitRead}})
		const path = "/api/v1/repos/archive"
		// 130 empty globs are 130 problems: the answer holds 100 items.
		manyGlobs := `["` + strings.Repeat(`","`, 129) + `"]`
		var manyParams []ipParam
		var manyMsgs []string
		for i := range maxInvalidParams {
			manyParams = append(manyParams, ipParam{fmt.Sprintf("include_globs.%d", i), "must be a non-empty glob"})
			manyMsgs = append(manyMsgs, `invalid glob ""`)
		}

		ipRun(t, e, []ipCase{
			{
				name: "every field", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"include_globs":["","ok"],"exclude_globs":["b","\u0000"],"max_blob_size":-1,"archive":{"prefix":"../x"}}`,
				wantError: `invalid glob ""; invalid glob "\x00"; invalid max_blob_size (want >= 0); invalid archive.prefix "../x"`,
				want: []ipParam{
					{"include_globs.0", "must be a non-empty glob"},
					{"exclude_globs.1", "must be a non-empty glob"},
					{"max_blob_size", "must be at least 0"},
					{"archive.prefix", "must be a relative path inside the archive"},
				},
			},
			{
				name: "the globs go before max_blob_size", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"max_blob_size":-1,"exclude_globs":[" "]}`,
				wantError: `invalid glob " "; invalid max_blob_size (want >= 0)`,
				want:      []ipParam{{"exclude_globs.0", "must be a non-empty glob"}, {"max_blob_size", "must be at least 0"}},
			},
			{
				name: "the body is checked before the repository, so a missing repository does not matter", method: http.MethodPost, path: path, token: missing,
				body:      `{"max_blob_size":-1,"archive":{"prefix":"/abs"}}`,
				wantError: `invalid max_blob_size (want >= 0); invalid archive.prefix "/abs"`,
				want:      []ipParam{{"max_blob_size", "must be at least 0"}, {"archive.prefix", "must be a relative path inside the archive"}},
			},
			{
				name: "the answer holds at most 100 items", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"include_globs":` + manyGlobs + `}`,
				wantError: strings.Join(manyMsgs, "; ") + "; and 30 more",
				want:      manyParams,
			},

			// One problem answers as before.
			{
				name: "one problem: max_blob_size", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"max_blob_size":-1}`, wantError: "invalid max_blob_size (want >= 0)",
				want: []ipParam{{"max_blob_size", "must be at least 0"}},
			},
			{
				name: "one problem: exclude_globs entry", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"exclude_globs":["a",""]}`, wantError: `invalid glob ""`,
				want: []ipParam{{"exclude_globs.1", "must be a non-empty glob"}},
			},
			{
				name: "one problem: archive.prefix", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"archive":{"prefix":"../x"}}`, wantError: `invalid archive.prefix "../x"`,
				want: []ipParam{{"archive.prefix", "must be a relative path inside the archive"}},
			},
		})
	})
}
