package api

import (
	"net/http"
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

// TestNotesAppendNamesTheFirstBadMember: the strict decoder of POST
// .../notes/append (not changed by this PR) stops at the first problem: the
// first unknown, duplicate or null member, or member that must be an
// object, in the order of the body. The field checks run only when the
// members are fine.
func TestNotesAppendNamesTheFirstBadMember(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := ipNewFixture(t, e)
		path := "/api/repos/" + fx.url + "/notes/append"
		const prefix = "invalid JSON body: "
		known := func(name, reason string) []ipParam { return []ipParam{{name, reason}} }
		ipRun(t, e, []ipCase{
			{
				name: "one unknown member", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"sha":"main","note":"n","bogus":1}`, wantError: prefix + `unknown field "bogus"`, want: known("bogus", "is not a known field"),
			},
			{
				name: "one dotted unknown member", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"author.name":"v"}`, wantError: prefix + `unknown field "author.name"`, want: known("author.name", "is not a known field"),
			},
			{
				name: "one duplicate member", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"sha":"main","note":"n","note":"m"}`, wantError: prefix + `duplicate field "note"`, want: known("note", "appears more than once"),
			},
			{
				name: "one null member", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"sha":"main","note":"n","notes_ref":null}`, wantError: prefix + "notes_ref must not be null", want: known("notes_ref", "must not be null"),
			},
			{
				name: "one member that must be an object", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"author":3}`, wantError: prefix + "author must be a JSON object", want: known("author", "must be an object"),
			},
			{
				name: "one empty member name", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"":1}`, wantError: prefix + `unknown field ""`,
			},
			{
				name: "not an object", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `[{"bogus":1}]`, wantError: prefix + "body must be a JSON object",
			},
			{
				name: "trailing data", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"note":"n"} {}`, wantError: prefix + "body must be a JSON object",
			},
			// Several bad members: the first one in the order of the body.
			{
				name: "an unknown member before a null one", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"zeta":1,"note":null}`, wantError: prefix + `unknown field "zeta"`, want: known("zeta", "is not a known field"),
			},
			{
				name: "a null member before an unknown one", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"note":null,"zeta":1}`, wantError: prefix + "note must not be null", want: known("note", "must not be null"),
			},
			{
				name: "a member of an object, then a later member", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"author":{"x":1,"name":null},"bogus":1}`, wantError: prefix + `unknown field "author.x"`, want: known("author.x", "is not a known field"),
			},
			{
				name: "a bad member before a syntax error", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"bogus":1,"note":`, wantError: prefix + `unknown field "bogus"`, want: known("bogus", "is not a known field"),
			},
			// A bad member answers before the field checks.
			{
				name: "a bad member and bad fields", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"note":"","notes_ref":"bad..name","bogus":1}`, wantError: prefix + `unknown field "bogus"`, want: known("bogus", "is not a known field"),
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
