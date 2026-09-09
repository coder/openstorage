package api

// Contract tests for the notes read endpoints (docs/reference/http-api.md "Notes"): GET
// /repos/notes and GET /repos/notes/refs, fanned out over the pg and
// sqlite lanes via runBackends. Fixtures seed durable state exactly like
// the other read tests (coreGit + coreSeedStores).

import (
	"context"
	"net/http"
	"net/url"
	"testing"

	"github.com/coder/openstorage/internal/auth"
)

// rnotesFixture is one seeded repo with notes on three notes refs.
type rnotesFixture struct {
	token  string
	c1, c2 string // main history, oldest first

	commitsTip  string // refs/notes/commits tip (note "reviewed" on c1)
	sessionATip string // refs/notes/reviews/session-a tip (note on c1)
	sessionBTip string // refs/notes/reviews/session-b tip (note on c2)
}

func rnotesSeedFixture(t *testing.T, e *testEnv) rnotesFixture {
	t.Helper()
	org := e.newOrg(t)
	url := "https://example.com/" + org + "/rnotes-" + randomHex(t, 4)
	repo, err := e.store.CreateRepo(context.Background(), org, url, "main")
	if err != nil {
		t.Fatalf("create repo: %v", err)
	}

	dir := t.TempDir()
	coreGit(t, dir, "init", "-q", "-b", "main", ".")
	coreWriteFile(t, dir, "a.txt", "v1\n")
	coreGit(t, dir, "add", ".")
	coreGit(t, dir, "commit", "-q", "-m", "c1")
	c1 := coreRevParse(t, dir, "HEAD")
	coreWriteFile(t, dir, "a.txt", "v2\n")
	coreGit(t, dir, "add", ".")
	coreGit(t, dir, "commit", "-q", "-m", "c2")
	c2 := coreRevParse(t, dir, "HEAD")
	coreGit(t, dir, "notes", "add", "-m", "reviewed", c1)
	coreGit(t, dir, "notes", "--ref", "refs/notes/reviews/session-a", "add", "-m", "session a", c1)
	coreGit(t, dir, "notes", "--ref", "refs/notes/reviews/session-b", "add", "-m", "session b", c2)
	fx := rnotesFixture{
		c1:          c1,
		c2:          c2,
		commitsTip:  coreRevParse(t, dir, "refs/notes/commits"),
		sessionATip: coreRevParse(t, dir, "refs/notes/reviews/session-a"),
		sessionBTip: coreRevParse(t, dir, "refs/notes/reviews/session-b"),
	}

	coreSeedStores(t, e, repo.RepoID, dir, map[string]string{
		"refs/heads/main":              c2,
		"refs/notes/commits":           fx.commitsTip,
		"refs/notes/reviews/session-a": fx.sessionATip,
		"refs/notes/reviews/session-b": fx.sessionBTip,
	})
	fx.token = e.mint(t, tokenSpec{iss: org, repo: url, scopes: []string{auth.ScopeGitRead}})
	return fx
}

func TestReadNotesGet(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := rnotesSeedFixture(t, e)

		get := func(t *testing.T, query string) (*http.Response, []byte) {
			t.Helper()
			return coreGet(t, e, "GET", "/api/v1/repos/notes?"+query, fx.token, nil)
		}

		t.Run("default notes ref", func(t *testing.T) {
			resp, body := get(t, "sha="+fx.c1)
			coreRequireStatus(t, resp, body, 200)
			var got rnotesGetResponse
			coreDecode(t, body, &got)
			want := rnotesGetResponse{SHA: fx.c1, Note: "reviewed", RefSHA: fx.commitsTip}
			if got != want {
				t.Fatalf("body = %+v, want %+v", got, want)
			}
		})
		t.Run("bare ref name resolves under refs/notes/", func(t *testing.T) {
			resp, body := get(t, "sha="+fx.c1+"&ref="+url.QueryEscape("reviews/session-a"))
			coreRequireStatus(t, resp, body, 200)
			var got rnotesGetResponse
			coreDecode(t, body, &got)
			want := rnotesGetResponse{SHA: fx.c1, Note: "session a", RefSHA: fx.sessionATip}
			if got != want {
				t.Fatalf("body = %+v, want %+v", got, want)
			}
		})
		t.Run("fully qualified ref", func(t *testing.T) {
			resp, body := get(t, "sha="+fx.c2+"&ref="+url.QueryEscape("refs/notes/reviews/session-b"))
			coreRequireStatus(t, resp, body, 200)
			var got rnotesGetResponse
			coreDecode(t, body, &got)
			if got.Note != "session b" || got.RefSHA != fx.sessionBTip {
				t.Fatalf("body = %+v", got)
			}
		})
		t.Run("object without note is 404", func(t *testing.T) {
			resp, body := get(t, "sha="+fx.c2)
			coreRequireStatus(t, resp, body, 404)
			requireEnvelope(t, resp, body)
		})
		t.Run("unborn notes ref is 404", func(t *testing.T) {
			resp, body := get(t, "sha="+fx.c1+"&ref=missing")
			coreRequireStatus(t, resp, body, 404)
			requireEnvelope(t, resp, body)
		})
		t.Run("missing object_ref is 400", func(t *testing.T) {
			resp, body := get(t, "")
			coreRequireStatus(t, resp, body, 400)
		})
		// Issue #161: object_ref accepts revisions — a branch, tag, short
		// SHA, or full ref resolving to a commit — mirroring the write
		// surface; responses echo the resolved id in `sha`.
		t.Run("object_ref revisions", func(t *testing.T) {
			for _, c := range []struct {
				name, query string
				want        rnotesGetResponse
			}{
				{"branch name", "object_ref=main&notes_ref=" + url.QueryEscape("reviews/session-b"),
					rnotesGetResponse{SHA: fx.c2, Note: "session b", RefSHA: fx.sessionBTip}},
				{"short sha", "object_ref=" + fx.c1[:8],
					rnotesGetResponse{SHA: fx.c1, Note: "reviewed", RefSHA: fx.commitsTip}},
				{"deprecated sha alias", "sha=" + fx.c1[:8],
					rnotesGetResponse{SHA: fx.c1, Note: "reviewed", RefSHA: fx.commitsTip}},
			} {
				t.Run(c.name, func(t *testing.T) {
					resp, body := get(t, c.query)
					coreRequireStatus(t, resp, body, 200)
					var got rnotesGetResponse
					coreDecode(t, body, &got)
					if got != c.want {
						t.Fatalf("body = %+v, want %+v", got, c.want)
					}
				})
			}
			// A non-resolving revision is a clear 404.
			resp, body := get(t, "object_ref=no-such-rev")
			coreRequireStatus(t, resp, body, 404)
			requireEnvelope(t, resp, body)
		})
		t.Run("non-notes ref is 400", func(t *testing.T) {
			resp, body := get(t, "sha="+fx.c1+"&ref="+url.QueryEscape("refs/tags/v1"))
			coreRequireStatus(t, resp, body, 400)
		})
		// Issue #15: upstream deprecated sha/ref in favor of
		// object_ref/notes_ref; both spellings must work, current name wins.
		t.Run("object_ref and notes_ref aliases", func(t *testing.T) {
			resp, body := get(t, "object_ref="+fx.c1)
			coreRequireStatus(t, resp, body, 200)
			var got rnotesGetResponse
			coreDecode(t, body, &got)
			want := rnotesGetResponse{SHA: fx.c1, Note: "reviewed", RefSHA: fx.commitsTip}
			if got != want {
				t.Fatalf("object_ref body = %+v, want %+v", got, want)
			}

			resp, body = get(t, "object_ref="+fx.c1+"&notes_ref="+url.QueryEscape("reviews/session-a"))
			coreRequireStatus(t, resp, body, 200)
			coreDecode(t, body, &got)
			want = rnotesGetResponse{SHA: fx.c1, Note: "session a", RefSHA: fx.sessionATip}
			if got != want {
				t.Fatalf("notes_ref body = %+v, want %+v", got, want)
			}

			// Both styles at once: the current name wins (the deprecated
			// values would 404 — c2 has no refs/notes/commits note, and
			// session-b holds no note for c1).
			resp, body = get(t, "object_ref="+fx.c1+"&sha="+fx.c2+
				"&notes_ref="+url.QueryEscape("reviews/session-a")+"&ref="+url.QueryEscape("reviews/session-b"))
			coreRequireStatus(t, resp, body, 200)
			coreDecode(t, body, &got)
			if got != want {
				t.Fatalf("alias-conflict body = %+v, want %+v (current names win)", got, want)
			}
		})
	})
}

func TestReadNotesListRefs(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := rnotesSeedFixture(t, e)

		get := func(t *testing.T, query string) (*http.Response, []byte) {
			t.Helper()
			return coreGet(t, e, "GET", "/api/v1/repos/notes/refs?"+query, fx.token, nil)
		}
		decode := func(t *testing.T, body []byte) rnotesListRefsResponse {
			t.Helper()
			var got rnotesListRefsResponse
			coreDecode(t, body, &got)
			return got
		}

		wantAll := []struct{ ref, sha string }{
			{"refs/notes/commits", fx.commitsTip},
			{"refs/notes/reviews/session-a", fx.sessionATip},
			{"refs/notes/reviews/session-b", fx.sessionBTip},
		}

		t.Run("default prefix lists all notes refs lexically", func(t *testing.T) {
			resp, body := get(t, "")
			coreRequireStatus(t, resp, body, 200)
			got := decode(t, body)
			if got.Prefix != "refs/notes/" || got.HasMore || got.NextCursor != "" {
				t.Fatalf("prefix/has_more/next_cursor = %+v", got)
			}
			if len(got.Refs) != len(wantAll) {
				t.Fatalf("refs = %+v, want %d entries", got.Refs, len(wantAll))
			}
			for i, w := range wantAll {
				if got.Refs[i].Ref != w.ref || got.Refs[i].SHA != w.sha || got.Refs[i].Cursor == "" {
					t.Errorf("refs[%d] = %+v, want %s -> %s", i, got.Refs[i], w.ref, w.sha)
				}
			}
		})
		t.Run("bare prefix normalizes under refs/notes/", func(t *testing.T) {
			resp, body := get(t, "prefix="+url.QueryEscape("reviews/"))
			coreRequireStatus(t, resp, body, 200)
			got := decode(t, body)
			if got.Prefix != "refs/notes/reviews/" || len(got.Refs) != 2 {
				t.Fatalf("body = %+v, want 2 refs under refs/notes/reviews/", got)
			}
		})
		t.Run("pagination pages by cursor", func(t *testing.T) {
			resp, body := get(t, "limit=2")
			coreRequireStatus(t, resp, body, 200)
			first := decode(t, body)
			if len(first.Refs) != 2 || !first.HasMore || first.NextCursor != first.Refs[1].Cursor {
				t.Fatalf("page 1 = %+v", first)
			}
			resp, body = get(t, "limit=2&cursor="+url.QueryEscape(first.NextCursor))
			coreRequireStatus(t, resp, body, 200)
			second := decode(t, body)
			if len(second.Refs) != 1 || second.HasMore || second.NextCursor != "" {
				t.Fatalf("page 2 = %+v", second)
			}
			if second.Refs[0].Ref != wantAll[2].ref {
				t.Fatalf("page 2 ref = %q, want %q", second.Refs[0].Ref, wantAll[2].ref)
			}
		})
		t.Run("invalid cursor is 400", func(t *testing.T) {
			resp, body := get(t, "cursor=garbage")
			coreRequireStatus(t, resp, body, 400)
			// Cursors are namespaced per endpoint family: a tags cursor
			// must fail closed here.
			resp, body = get(t, "cursor="+url.QueryEscape(coreCursorEncode(cursorKindTags, "v1")))
			coreRequireStatus(t, resp, body, 400)
		})
		t.Run("prefix outside refs/notes/ is 400", func(t *testing.T) {
			resp, body := get(t, "prefix="+url.QueryEscape("refs/tags/"))
			coreRequireStatus(t, resp, body, 400)
			requireEnvelope(t, resp, body)
		})
	})
}
