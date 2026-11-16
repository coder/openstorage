package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/auth"
)

// Notes routes name every invalid field (upstream: `invalid_params` "names
// each invalid field"). The order is the docs table order of each route,
// so the cases below break every field of a route at once. There is no
// dependency between two fields of a note request (S2), so no row asks
// one field to hide another. Every check in the batch is stateless: the
// rows that follow it show that a bad batch answers before the state
// lookups (object_ref that does not resolve) and a clean one reaches them.

// ipNoteReq is a note request that breaks nothing: the rows that vary one
// field build on it.
const ipNoteReq = `"object_ref":"main","note":"n"`

func TestNotesNameEveryInvalidField(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := ipNewFixture(t, e)
		post, appendPath, del := "/api/v1/repos/notes", "/api/repos/"+fx.url+"/notes/append", "/api/v1/repos/notes"
		const (
			actionMsg = "action must be add or append"
			guardMsg  = "expected_notes_ref_sha must be a full 40-hex object id"
			authorMsg = "author name and email are required when author is provided"
		)
		const (
			badRefMsg = `invalid notes ref name "bad..name"`
			fullRef   = `ref "refs/heads/main" is not a valid refs/notes/ ref`
		)
		required := func(name string) ipParam { return ipParam{name, "is required"} }
		validRef := func(name string) ipParam { return ipParam{name, "must be a valid notes ref"} }
		validSHA := func(name string) ipParam { return ipParam{name, "must be a full 40-hex object id"} }
		ipRun(t, e, []ipCase{
			// POST /repos/notes: object_ref, action, note, notes_ref,
			// expected_notes_ref_sha, author.
			{
				name: "POST every field", method: http.MethodPost, path: post, token: fx.gitTok,
				body:      `{"action":"replace","notes_ref":"bad..name","expected_notes_ref_sha":"xyz","author":{}}`,
				wantError: "object_ref is required; " + actionMsg + "; note is required; " + badRefMsg + "; " + guardMsg + "; " + authorMsg,
				want: []ipParam{required("object_ref"), {"action", "must be add or append"}, required("note"), validRef("notes_ref"),
					validSHA("expected_notes_ref_sha"), required("author.name"), required("author.email")},
			},
			{
				name: "POST deprecated names are named as the request sent them", method: http.MethodPost, path: post, token: fx.gitTok,
				body:      `{"sha":"main","action":"","ref":"refs/heads/main","expected_ref_sha":"zz","author":{"name":"n"}}`,
				wantError: "note is required; " + fullRef + "; " + guardMsg + "; " + authorMsg,
				want:      []ipParam{required("note"), validRef("ref"), validSHA("expected_ref_sha"), required("author.email")},
			},
			{
				name: "POST the current name wins over its alias, and a field appears once", method: http.MethodPost, path: post, token: fx.gitTok,
				body:      `{` + ipNoteReq + `,"notes_ref":"bad..name","ref":"also..bad","expected_notes_ref_sha":"x","expected_ref_sha":"y"}`,
				wantError: badRefMsg + "; " + guardMsg,
				want:      []ipParam{validRef("notes_ref"), validSHA("expected_notes_ref_sha")},
			},
			{
				name: "POST path alias", method: http.MethodPost, path: "/api/repos/" + fx.url + "/notes", token: fx.gitTok,
				body:      `{"note":"","notes_ref":"bad..name"}`,
				wantError: "object_ref is required; note is required; " + badRefMsg,
				want:      []ipParam{required("object_ref"), required("note"), validRef("notes_ref")},
			},
			{
				name: "POST an unresolvable object_ref does not hide a bad field", method: http.MethodPost, path: post, token: fx.gitTok,
				body:      `{"object_ref":"nosuchbranch","action":"x","note":"n"}`,
				wantError: actionMsg, want: []ipParam{{"action", "must be add or append"}},
			},
			// One problem answers as before.
			{
				name: "POST one: object_ref", method: http.MethodPost, path: post, token: fx.gitTok,
				body: `{"note":"x"}`, wantError: "object_ref is required", want: []ipParam{required("object_ref")},
			},
			{
				name: "POST one: action", method: http.MethodPost, path: post, token: fx.gitTok,
				body: `{"sha":"main","action":"replace","note":"x"}`, wantError: actionMsg, want: []ipParam{{"action", "must be add or append"}},
			},
			{
				name: "POST one: note", method: http.MethodPost, path: post, token: fx.gitTok,
				body: `{"sha":"main"}`, wantError: "note is required", want: []ipParam{required("note")},
			},
			{
				name: "POST one: a full ref outside refs/notes/", method: http.MethodPost, path: post, token: fx.gitTok,
				body: `{` + ipNoteReq + `,"ref":"refs/heads/main"}`, wantError: fullRef, want: []ipParam{validRef("ref")},
			},
			{
				name: "POST one: notes_ref", method: http.MethodPost, path: post, token: fx.gitTok,
				body: `{` + ipNoteReq + `,"notes_ref":"bad..name"}`, wantError: badRefMsg, want: []ipParam{validRef("notes_ref")},
			},
			{
				name: "POST one: expected_ref_sha", method: http.MethodPost, path: post, token: fx.gitTok,
				body: `{` + ipNoteReq + `,"expected_ref_sha":"xyz"}`, wantError: guardMsg, want: []ipParam{validSHA("expected_ref_sha")},
			},
			{
				name: "POST one: author", method: http.MethodPost, path: post, token: fx.gitTok,
				body: `{` + ipNoteReq + `,"author":{"name":"n"}}`, wantError: authorMsg, want: []ipParam{required("author.email")},
			},
			// git takes each ident as an environment value, which cannot hold a NUL.
			{
				name: "POST a NUL in the author name and email", method: http.MethodPost, path: post, token: fx.gitTok,
				body:      `{` + ipNoteReq + `,"author":{"name":"a\u0000","email":"e\u0000"}}`,
				wantError: invalidTextMsg("author.name") + "; " + invalidTextMsg("author.email"),
				want:      []ipParam{{"author.name", ipcNulReason}, {"author.email", ipcNulReason}},
			},
			{
				name: "POST a NUL in the name of an author with an empty email", method: http.MethodPost, path: post, token: fx.gitTok,
				body:      `{` + ipNoteReq + `,"author":{"name":"a\u0000","email":""}}`,
				wantError: authorMsg + "; " + invalidTextMsg("author.name"),
				want:      []ipParam{required("author.email"), {"author.name", ipcNulReason}},
			},
			{
				name: "POST one: a NUL in the author email", method: http.MethodPost, path: post, token: fx.gitTok,
				body:      `{` + ipNoteReq + `,"author":{"name":"n","email":"e\u0000"}}`,
				wantError: invalidTextMsg("author.email"),
				want:      []ipParam{{"author.email", ipcNulReason}},
			},

			{
				name: "POST an uppercase guard is not a problem", method: http.MethodPost, path: post, token: fx.gitTok,
				body: `{"note":"x","expected_notes_ref_sha":"` + strings.Repeat("A", 40) + `"}`, wantError: "object_ref is required", want: []ipParam{required("object_ref")},
			},

			// POST .../notes/append: object_ref, note, notes_ref,
			// expected_notes_ref_sha, author (no action).
			{
				name: "append every field", method: http.MethodPost, path: appendPath, token: fx.gitTok,
				body:      `{"note":"","ref":"refs/heads/main","expected_ref_sha":"x","author":{"email":""}}`,
				wantError: "object_ref is required; note is required; " + fullRef + "; " + guardMsg + "; " + authorMsg,
				want: []ipParam{required("object_ref"), required("note"), validRef("ref"), validSHA("expected_ref_sha"),
					required("author.name"), required("author.email")},
			},
			{
				name: "append one: note", method: http.MethodPost, path: appendPath, token: fx.gitTok,
				body: `{"object_ref":"main"}`, wantError: "note is required", want: []ipParam{required("note")},
			},
			{
				name: "append: a decode problem answers before the field checks", method: http.MethodPost, path: appendPath, token: fx.gitTok,
				body:      `{"action":"append","note":"","notes_ref":"bad..name"}`,
				wantError: `invalid JSON body: unknown field "action"`, want: []ipParam{{"action", "is not a known field"}},
			},

			// DELETE /repos/notes: the body is the deprecated form of the
			// query: object_ref, notes_ref, expected_notes_ref_sha, author.
			// A body field the request did not send, and every query
			// parameter, name no field.
			{
				name: "DELETE body every field", method: http.MethodDelete, path: del, token: fx.gitTok,
				body:      `{"notes_ref":"bad..name","expected_ref_sha":"x","author":{}}`,
				wantError: "object_ref is required; " + badRefMsg + "; " + guardMsg + "; " + authorMsg,
				want:      []ipParam{validRef("notes_ref"), validSHA("expected_ref_sha"), required("author.name"), required("author.email")},
			},
			{
				name: "DELETE query every field", method: http.MethodDelete, path: del + "?object_ref=&notes_ref=bad..name&expected_notes_ref_sha=x", token: fx.gitTok,
				body:      `{"object_ref":"main","notes_ref":"ok","expected_notes_ref_sha":"` + strings.Repeat("0", 40) + `"}`,
				wantError: "object_ref is required; " + badRefMsg + "; " + guardMsg,
			},
			{
				name: "DELETE query and body together", method: http.MethodDelete, path: "/api/repos/" + fx.url + "/notes?notes_ref=bad..name", token: fx.gitTok,
				body:      `{"object_ref":"main","expected_notes_ref_sha":"x","author":{"email":"e"}}`,
				wantError: badRefMsg + "; " + guardMsg + "; " + authorMsg,
				want:      []ipParam{validSHA("expected_notes_ref_sha"), required("author.name")},
			},
			{
				name: "DELETE a present query parameter hides its bad body field", method: http.MethodDelete, path: del + "?object_ref=main&notes_ref=ok", token: fx.gitTok,
				body:      `{"notes_ref":"bad..name","expected_ref_sha":"x"}`,
				wantError: guardMsg, want: []ipParam{validSHA("expected_ref_sha")},
			},
			{
				name: "DELETE one: nothing sent", method: http.MethodDelete, path: del, token: fx.gitTok,
				wantError: "object_ref is required",
			},
			{
				name: "DELETE one: body guard", method: http.MethodDelete, path: del, token: fx.gitTok,
				body: `{"object_ref":"main","expected_notes_ref_sha":"x"}`, wantError: guardMsg, want: []ipParam{validSHA("expected_notes_ref_sha")},
			},
			{
				name: "DELETE one: a NUL in the body author", method: http.MethodDelete, path: del, token: fx.gitTok,
				body:      `{"object_ref":"main","author":{"name":"a\u0000","email":"e"}}`,
				wantError: invalidTextMsg("author.name"), want: []ipParam{{"author.name", ipcNulReason}},
			},
		})

		t.Run("a clean batch reaches the state checks", func(t *testing.T) {
			resp, body := e.doReq(t, http.MethodPost, post, fx.gitTok, []byte(`{"object_ref":"nosuchbranch","note":"n"}`), "")
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("status = %d, want 404 (body %s)", resp.StatusCode, body)
			}
		})
		t.Run("a missing repository answers 404 before the body", func(t *testing.T) {
			tok := e.mint(t, tokenSpec{iss: e.newOrg(t), repo: "ip-gone-" + randomHex(t, 4), scopes: []string{auth.ScopeGitRead, auth.ScopeGitWrite}})
			resp, body := e.doReq(t, http.MethodPost, post, tok, []byte(`{"action":"x"}`), "")
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("status = %d, want 404 (body %s)", resp.StatusCode, body)
			}
		})
	})
}

// TestStrictBodiesNameEveryBadMember: the strict decoders of POST
// .../notes/append and POST .../upstream/pull name each unknown, duplicate
// or null member, and each member that must be an object: the known members
// in the order of the docs body (a member's own problem before those below
// it), then the unknown ones in the order of the request. The copies of a
// repeated object are one object. A body with one bad member answers as
// before. The field checks run only when the members are fine.
func TestStrictBodiesNameEveryBadMember(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := ipNewFixture(t, e)
		path, pull := "/api/repos/"+fx.url+"/notes/append", "/api/repos/"+fx.url+"/upstream/pull"
		const prefix = "invalid JSON body: "
		var (
			null    = func(name string) ipParam { return ipParam{name, "must not be null"} }
			unknown = func(name string) ipParam { return ipParam{name, "is not a known field"} }
			twice   = func(name string) ipParam { return ipParam{name, "appears more than once"} }
		)
		ipRun(t, e, []ipCase{
			// Several bad members.
			{
				name: "every kind, in the reverse of the docs order", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"zeta":1,"author":{"x":1,"name":null},"expected_ref_sha":null,"note":null,` +
					`"sha":"a","sha":"b","object_ref":null,"alpha":2,"":3}`,
				wantError: prefix + `object_ref must not be null; duplicate field "sha"; note must not be null` +
					`; expected_ref_sha must not be null; author.name must not be null; unknown field "author.x"` +
					`; unknown field "zeta"; unknown field "alpha"; unknown field ""`,
				want: []ipParam{null("object_ref"), twice("sha"), null("note"), null("expected_ref_sha"), null("author.name"),
					unknown("author.x"), unknown("zeta"), unknown("alpha")},
			},
			{
				name: "an unknown member before a null one", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"zeta":1,"note":null}`, wantError: prefix + `note must not be null; unknown field "zeta"`,
				want: []ipParam{null("note"), unknown("zeta")},
			},
			{
				name: "a repeated object names its own problem first", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"author":{"x":1},"author":{}}`, wantError: prefix + `duplicate field "author"; unknown field "author.x"`,
				want: []ipParam{twice("author"), unknown("author.x")},
			},
			{
				name: "a repeated object names the members of each copy", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"author":{},"author":{"x":1}}`, wantError: prefix + `duplicate field "author"; unknown field "author.x"`,
				want: []ipParam{twice("author"), unknown("author.x")},
			},
			{
				name: "the copies of a repeated object are one object", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"author":{"name":"a","x":1},"author":{"x":2,"name":null}}`,
				wantError: prefix + `duplicate field "author"; author.name must not be null; unknown field "author.x"`,
				want:      []ipParam{twice("author"), null("author.name"), unknown("author.x")},
			},
			{
				name: "a member that must be an object", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"author":3,"bogus":1}`, wantError: prefix + `author must be a JSON object; unknown field "bogus"`,
				want: []ipParam{{"author", "must be an object"}, unknown("bogus")},
			},
			{
				name: "a null object and an unknown member", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"author":null,"bogus":1}`, wantError: prefix + `author must not be null; unknown field "bogus"`,
				want: []ipParam{null("author"), unknown("bogus")},
			},
			{
				name: "bad members before a syntax error", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"bogus":1,"note":null,`, wantError: prefix + `note must not be null; unknown field "bogus"`,
				want: []ipParam{null("note"), unknown("bogus")},
			},
			{
				name: "bad members before trailing data", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"bogus":1,"note":null} {}`, wantError: prefix + `note must not be null; unknown field "bogus"`,
				want: []ipParam{null("note"), unknown("bogus")},
			},
			// A member named "author.name" and the name of an author object
			// have one dotted path: one item, both messages.
			{
				name: "two members with one dotted path", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"author":{"name":null},"author.name":"v"}`, wantError: prefix + `author.name must not be null; unknown field "author.name"`,
				want: []ipParam{null("author.name")},
			},
			{
				name: "one dotted path with one message twice", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"author":{"x":1},"author.x":1,"note":null}`, wantError: prefix + `note must not be null; unknown field "author.x"`,
				want: []ipParam{null("note"), unknown("author.x")},
			},
			{
				name: "one dotted path cut to two names", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"author":{"` + strings.Repeat("x", 40) + `":1},"author.` + strings.Repeat("x", 40) + `":1}`,
				wantError: prefix + `unknown field "author.` + strings.Repeat("x", 32) + `…"; unknown field "author.` + strings.Repeat("x", 25) + `…"`,
				want:      []ipParam{unknown("author." + strings.Repeat("x", 32) + "…")},
			},
			{
				name: "empty member names add their message only", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"":1,"bogus":1,"author":{"":1}}`, wantError: prefix + `unknown field "author."; unknown field ""; unknown field "bogus"`,
				want: []ipParam{unknown("bogus")},
			},
			{
				name: "an empty name below a member with its dotted path", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"author.":1,"author":{"":1},"bogus":1}`, wantError: prefix + `unknown field "author."; unknown field "bogus"`,
				want: []ipParam{unknown("author."), unknown("bogus")},
			},
			{
				name: "pull: every kind", method: http.MethodPost, path: pull, token: fx.gitTok,
				body: `{"x":1,"after":null,"ref":"a","ref":"b"}`, wantError: prefix + `duplicate field "ref"; after must not be null; unknown field "x"`,
				want: []ipParam{twice("ref"), null("after"), unknown("x")},
			},
			// One bad member answers as before.
			{
				name: "one unknown member", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"sha":"main","note":"n","bogus":1}`, wantError: prefix + `unknown field "bogus"`, want: []ipParam{unknown("bogus")},
			},
			{
				name: "one dotted unknown member", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"author.name":"v"}`, wantError: prefix + `unknown field "author.name"`, want: []ipParam{unknown("author.name")},
			},
			{
				name: "one long unknown member sent twice", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"` + strings.Repeat("x", 40) + `":1,"note":"n","` + strings.Repeat("x", 40) + `":2}`,
				wantError: prefix + `unknown field "` + strings.Repeat("x", 40) + `"`, want: []ipParam{unknown(strings.Repeat("x", 40))},
			},
			{
				name: "one long unknown member keeps its whole name", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"` + strings.Repeat("x", 40) + `":1}`, wantError: prefix + `unknown field "` + strings.Repeat("x", 40) + `"`,
				want: []ipParam{unknown(strings.Repeat("x", 40))},
			},
			{
				name: "one null member sent again", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"note":null,"note":"n"}`, wantError: prefix + "note must not be null", want: []ipParam{null("note")},
			},
			{
				name: "one duplicate member", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"sha":"main","note":"n","note":"m"}`, wantError: prefix + `duplicate field "note"`, want: []ipParam{twice("note")},
			},
			{
				name: "one null member", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"sha":"main","note":"n","notes_ref":null}`, wantError: prefix + "notes_ref must not be null", want: []ipParam{null("notes_ref")},
			},
			{
				name: "one member that must be an object", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"author":3}`, wantError: prefix + "author must be a JSON object", want: []ipParam{{"author", "must be an object"}},
			},
			{
				name: "one null object", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"author":null}`, wantError: prefix + "author must not be null", want: []ipParam{null("author")},
			},
			{
				name: "one empty member name", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"":1}`, wantError: prefix + `unknown field ""`,
			},
			{
				name: "one empty member name sent twice", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"":1,"":2}`, wantError: prefix + `unknown field ""`,
			},
			{
				name: "one bad member before a syntax error", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"bogus":1,"note":`, wantError: prefix + `unknown field "bogus"`, want: []ipParam{unknown("bogus")},
			},
			{
				name: "pull: one unknown member", method: http.MethodPost, path: pull, token: fx.gitTok,
				body: `{"refs":"main"}`, wantError: prefix + `unknown field "refs"`, want: []ipParam{unknown("refs")},
			},
			{
				name: "not an object", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `[{"bogus":1}]`, wantError: prefix + "body must be a JSON object",
			},
			{
				name: "trailing data", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"note":"n"} {}`, wantError: prefix + "body must be a JSON object",
			},
			// A bad member answers before the field checks.
			{
				name: "a bad member and bad fields", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"note":"","notes_ref":"bad..name","bogus":1}`, wantError: prefix + `unknown field "bogus"`, want: []ipParam{unknown("bogus")},
			},
			{
				name: "fine members and bad fields", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"note":"","notes_ref":"bad..name"}`,
				wantError: `object_ref is required; note is required; invalid notes ref name "bad..name"`,
				want:      []ipParam{{"object_ref", "is required"}, {"note", "is required"}, {"notes_ref", "must be a valid notes ref"}},
			},
		})
	})
}

// TestStrictBodiesBoundTheNamesTheyRepeat: an unknown member name is
// request text. An answer that names several members repeats at most 32
// bytes of each, so it has at most 105,203 bytes and the request path: 100
// items, each with two messages in `error` and in `detail` (a name in
// author and the same name at the top level as author.<name>). An answer
// that names one member keeps its name up to 16384 bytes
// (TestLongValuesAreCutInAnswers). A name that an earlier item shows after
// the cut gives no item of its own and counts in `and N more`. Each object
// (all copies of author are one) keeps 100 unknown names, and `and N more`
// counts each member that the answer leaves out once: a repeat, or a member
// with the dotted path of a named or counted one, adds nothing.
func TestStrictBodiesBoundTheNamesTheyRepeat(t *testing.T) {
	// name is the i-th member name of 8,600 bytes of <, > and &, which JSON
	// escapes to six bytes each. The names differ in their first 32 bytes.
	name := func(i int) string {
		b := make([]byte, 8600)
		for k, x := 0, i; k < len(b); k, x = k+1, x/3 {
			b[k] = "<>&"[x%3]
		}
		return string(b)
	}
	var names strings.Builder
	var cut []ipParam
	for i := range 120 {
		names.WriteString(`"` + name(i) + `":1,`)
		if i < 99 {
			cut = append(cut, ipParam{name(i)[:32] + "…", "is not a known field"})
		}
	}
	long := strings.Repeat("<", 20000)
	// members is n members called prefix0, prefix1, ..., and unknown the
	// items that name the first n of them below the dotted path path.
	members := func(prefix string, n int) string {
		var b strings.Builder
		for i := range n {
			fmt.Fprintf(&b, `"%s%d":1,`, prefix, i)
		}
		return b.String()
	}
	unknown := func(path string, n int) []ipParam {
		var items []ipParam
		for i := range n {
			items = append(items, ipParam{fmt.Sprintf("%s%d", path, i), "is not a known field"})
		}
		return items
	}
	alike := func(n int) string { // n names with one first 32 bytes
		var b strings.Builder
		for i := range n {
			fmt.Fprintf(&b, `"%s%03d":1,`, strings.Repeat("p", 32), i)
		}
		return b.String()
	}
	// twice has 100 names of 33 bytes in author and again at the top level
	// as author.<name>: 100 items with two messages each, the longest answer.
	var inAuthor, atTop []string
	var twiceItems []ipParam
	for i := range 100 {
		inAuthor = append(inAuthor, `"`+name(i)[:33]+`":1`)
		atTop = append(atTop, `"author.`+name(i)[:33]+`":1`)
		twiceItems = append(twiceItems, ipParam{"author." + name(i)[:32] + "…", "is not a known field"})
	}
	twice := `{"author":{` + strings.Join(inAuthor, ",") + "}," + strings.Join(atTop, ",") + "}"
	nullNote, nullName := []ipParam{{"note", "must not be null"}}, []ipParam{{"author.name", "must not be null"}}
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := ipNewFixture(t, e)
		for _, tc := range []struct {
			name, path, body, tail string
			want                   []ipParam
		}{
			{"append: 120 long names", "/notes/append", "{" + names.String() + `"note":null}`, "; and 21 more",
				append([]ipParam{{"note", "must not be null"}}, cut...)},
			{"pull: 120 long names", "/upstream/pull", "{" + names.String() + `"after":null}`, "; and 21 more",
				append([]ipParam{{"after", "must not be null"}}, cut...)},
			{"append: a long name and a null member", "/notes/append", `{"` + long + `":1,"note":null}`,
				`note must not be null; unknown field "` + long[:32] + `…"`,
				[]ipParam{{"note", "must not be null"}, {long[:32] + "…", "is not a known field"}}},
			{"append: names cut to one text", "/notes/append", "{" + alike(150) + `"note":null}`,
				`; unknown field "` + strings.Repeat("p", 32) + `…"; and 149 more`,
				append(nullNote, ipParam{strings.Repeat("p", 32) + "…", "is not a known field"})},
			{"append: a name past 100 names cut to one text", "/notes/append", "{" + alike(100) + `"zzz":1}`,
				`unknown field "` + strings.Repeat("p", 32) + `…"; and 100 more`,
				[]ipParam{{strings.Repeat("p", 32) + "…", "is not a known field"}}},
			{"append: the copies of author keep 100 names", "/notes/append",
				`{"author":{` + strings.TrimSuffix(alike(100), ",") + `},"author":{"zzz":1}}`,
				`duplicate field "author"; unknown field "author.` + strings.Repeat("p", 32) + `…"; and 100 more`,
				[]ipParam{{"author", "appears more than once"}, {"author." + strings.Repeat("p", 32) + "…", "is not a known field"}}},
			{"append: 100 long names in author and at the top level", "/notes/append",
				twice,
				`; unknown field "author.` + name(99)[:25] + `…"`, twiceItems},
			{"append: repeats of left-out names", "/notes/append",
				"{" + members("u", 100) + strings.Repeat(`"zz":1,"yy":1,`, 500) + `"note":"n"}`, "; and 2 more", unknown("u", 100)},
			{"append: 120 names in author", "/notes/append", `{"note":null,"author":{` + strings.TrimSuffix(members("n", 120), ",") + "}}",
				"; and 21 more", append(nullNote, unknown("author.n", 99)...)},
			{"append: a counted field named twice", "/notes/append",
				`{"author":{"name":null,` + members("u", 100) + `"q":1},"author.u99":1,"author.q":1}`, "; and 2 more", append(nullName, unknown("author.u", 99)...)},
			{"append: a left-out name that an item names", "/notes/append",
				`{"author":{"x":1},` + members("t", 100) + `"author.x":1}`, "; and 1 more",
				append([]ipParam{{"author.x", "is not a known field"}}, unknown("t", 99)...)},
		} {
			t.Run(tc.name, func(t *testing.T) {
				resp, body := e.doReq(t, http.MethodPost, "/api/repos/"+fx.url+tc.path, fx.gitTok, []byte(tc.body), "")
				if resp.StatusCode != http.StatusBadRequest {
					t.Fatalf("status = %d, want 400 (body %.300s)", resp.StatusCode, body)
				}
				msg := requireEnvelope(t, resp, body)
				var p problemBody
				if err := json.Unmarshal(body, &p); err != nil {
					t.Fatal(err)
				}
				var got []ipParam
				for _, ip := range p.InvalidParams {
					got = append(got, ipParam{ip.Name, ip.Reason})
				}
				limit := 105_203 + len(p.Instance)
				if len(body) > limit || !strings.HasSuffix(msg, tc.tail) || !slices.Equal(got, tc.want) {
					t.Fatalf("%d bytes, message ends %q, %d items; want at most %d bytes, the end %q, %d items",
						len(body), msg[max(0, len(msg)-60):], len(got), limit, tc.tail, len(tc.want))
				}
			})
		}
	})
}
