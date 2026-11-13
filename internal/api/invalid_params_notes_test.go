package api

import (
	"fmt"
	"net/http"
	"runtime"
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

// TestNotesAppendNamesEveryBadMember: the strict decoder of POST
// .../notes/append names each unknown, duplicate or null member, and each
// member that must be an object. A body that is not one JSON object names
// none.
func TestNotesAppendNamesEveryBadMember(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := ipNewFixture(t, e)
		path := "/api/repos/" + fx.url + "/notes/append"
		const prefix = "invalid JSON body: "
		known := func(name, reason string) ipParam { return ipParam{name, reason} }
		ipRun(t, e, []ipCase{
			{
				// The body lists the members in the reverse of the struct's
				// order. The known ones come back in the struct's order
				// (an object's own problem, then the problems below it),
				// the unknown ones after them in the order of the body.
				name: "every kind, known members in struct order", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"zeta":1,"author":{"x":1,"name":null},"expected_ref_sha":null,"ref":null,"note":null,` +
					`"sha":"a","sha":"b","object_ref":null,"alpha":2,"":3}`,
				wantError: prefix + `object_ref must not be null; duplicate field "sha"; note must not be null; ref must not be null` +
					`; expected_ref_sha must not be null; author.name must not be null; unknown field "author.x"` +
					`; unknown field "zeta"; unknown field "alpha"; unknown field ""`,
				want: []ipParam{known("object_ref", "must not be null"), known("sha", "appears more than once"), known("note", "must not be null"),
					known("ref", "must not be null"), known("expected_ref_sha", "must not be null"), known("author.name", "must not be null"),
					known("author.x", "is not a known field"), known("zeta", "is not a known field"), known("alpha", "is not a known field")},
			},
			{
				name: "an unknown member sent twice counts once", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"bogus":1,"note":"n","bogus":2}`,
				wantError: prefix + `unknown field "bogus"`, want: []ipParam{known("bogus", "is not a known field")},
			},
			{
				name: "an empty member name has no path", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"":1,"bogus":1,"author":{"":1}}`,
				wantError: prefix + `unknown field "author."; unknown field ""; unknown field "bogus"`, want: []ipParam{known("bogus", "is not a known field")},
			},
			{
				name: "an empty member name sent twice counts once", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"":1,"":2}`, wantError: prefix + `unknown field ""`,
			},
			{
				name: "a member that must be an object", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"author":3,"bogus":1}`,
				wantError: prefix + `author must be a JSON object; unknown field "bogus"`,
				want:      []ipParam{known("author", "must be an object"), known("bogus", "is not a known field")},
			},
			{
				name: "a repeated object names its own problem, then the ones below", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"author":{"x":1},"author":{}}`,
				wantError: prefix + `duplicate field "author"; unknown field "author.x"`,
				want:      []ipParam{known("author", "appears more than once"), known("author.x", "is not a known field")},
			},
			{
				name: "a null object, then a repeat of it", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"author":null,"author":{"name":null}}`,
				wantError: prefix + "author must not be null", want: []ipParam{known("author", "must not be null")},
			},
			// Two members can have one dotted path: a member named "author.name"
			// and the "name" of an author object. The later problem keeps its
			// message and adds no second item for the path.
			{
				name: "a dotted unknown member and a repeat below an object", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"author.name":"v","author":{"name":"n","name":"m"}}`,
				wantError: prefix + `duplicate field "author.name"; unknown field "author.name"`, want: []ipParam{known("author.name", "appears more than once")},
			},
			{
				name: "a dotted unknown member and a null below an object", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"author":{"name":null},"author.name":"v"}`,
				wantError: prefix + `author.name must not be null; unknown field "author.name"`, want: []ipParam{known("author.name", "must not be null")},
			},
			{
				name: "a dotted unknown member and an unknown below an object say the same", method: http.MethodPost, path: path, token: fx.gitTok,
				body:      `{"author.x":1,"author":{"x":1}}`,
				wantError: prefix + `unknown field "author.x"`, want: []ipParam{known("author.x", "is not a known field")},
			},
			// One problem answers as before.
			{
				name: "one dotted unknown member", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"author.name":"v"}`, wantError: prefix + `unknown field "author.name"`, want: []ipParam{known("author.name", "is not a known field")},
			},
			{
				name: "one unknown member", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"sha":"main","note":"n","bogus":1}`, wantError: prefix + `unknown field "bogus"`, want: []ipParam{known("bogus", "is not a known field")},
			},
			{
				name: "one duplicate member", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"sha":"main","note":"n","note":"m"}`, wantError: prefix + `duplicate field "note"`, want: []ipParam{known("note", "appears more than once")},
			},
			{
				name: "one null member", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"sha":"main","note":"n","notes_ref":null}`, wantError: prefix + "notes_ref must not be null", want: []ipParam{known("notes_ref", "must not be null")},
			},
			{
				name: "one empty member name", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"":1}`, wantError: prefix + `unknown field ""`,
			},
			// A body that is not one complete JSON object names no member,
			// whatever members it holds.
			{
				name: "a syntax error after bad members", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"bogus":1,"note":null,`, wantError: prefix + "body must be a JSON object",
			},
			{
				name: "a value cut short after bad members", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"bogus":1,"note":`, wantError: prefix + "body must be a JSON object",
			},
			{
				name: "a missing closing brace after bad members", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"bogus":1,"note":null`, wantError: prefix + "body must be a JSON object",
			},
			{
				name: "trailing data after bad members", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `{"bogus":1,"note":null} {}`, wantError: prefix + "body must be a JSON object",
			},
			{
				name: "not an object", method: http.MethodPost, path: path, token: fx.gitTok,
				body: `[{"bogus":1}]`, wantError: prefix + "body must be a JSON object",
			},
		})
	})
}

// TestStrictJSONOrder pins the order of the problems: the members the
// struct knows in the struct's field order, then the unknown ones in the
// order of the body. A reordered body changes only the unknown ones.
func TestStrictJSONOrder(t *testing.T) {
	type obj struct {
		Name string `json:"name"`
	}
	type dst struct {
		Ref    string `json:"ref"`
		After  string `json:"after"`
		Inner  *obj   `json:"inner"`
		Hidden string `json:"-"`
	}
	for _, tc := range []struct {
		name, body string
		want       []string
	}{
		{"known in struct order", `{"inner":null,"after":null,"ref":null}`, []string{"ref", "after", "inner"}},
		{"known in struct order, reversed body", `{"ref":null,"after":null,"inner":null}`, []string{"ref", "after", "inner"}},
		{"unknown in body order", `{"b":1,"a":2,"c":3}`, []string{"b", "a", "c"}},
		{"unknown in body order, reversed", `{"c":3,"a":2,"b":1}`, []string{"c", "a", "b"}},
		{"unknown after known", `{"z":1,"inner":null,"ref":null,"y":1}`, []string{"ref", "inner", "z", "y"}},
		{"nested stay at their member", `{"z":1,"inner":{"q":1,"name":null},"ref":null}`, []string{"ref", "inner.name", "inner.q", "z"}},
		{"a field tagged - is unknown", `{"Hidden":1,"-":2}`, []string{"Hidden", "-"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := strictJSON([]byte(tc.body), new(dst))
			if err == nil {
				t.Fatal("strictJSON succeeded")
			}
			var got []string
			for _, p := range invalidParamsFor(err) {
				got = append(got, p.Name)
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("names = %v, want %v (error %q)", got, tc.want, err)
			}
		})
	}
	// The shared decoder serves POST .../upstream/pull too.
	err := strictJSON([]byte(`{"after":null,"ref":null,"x":1}`), new(usyncPullUpstreamRequest))
	if want := `ref must not be null; after must not be null; unknown field "x"`; err == nil || err.Error() != want {
		t.Fatalf("pull request: err = %v, want %q", err, want)
	}
	// A body that decodes still decodes.
	var ok dst
	if err := strictJSON([]byte(`{"ref":"r","inner":{"name":"n"}}`), &ok); err != nil || ok.Ref != "r" || ok.Inner == nil || ok.Inner.Name != "n" {
		t.Fatalf("valid body: err %v, decoded %+v", err, ok)
	}
}

// TestStrictJSONBoundsItsAnswer: a body of 1 MiB holds 100,000 unknown
// members. The answer names at most maxInvalidParams problems and counts
// the others, however they split between a member's own problem, the ones
// below it and the unknown ones. The walk keeps no name and builds no
// problem past the bound.
func TestStrictJSONBoundsItsAnswer(t *testing.T) {
	unknown := func(n int, prefix string) string {
		var b strings.Builder
		for i := range n {
			fmt.Fprintf(&b, `"%s%03d":1,`, prefix, i)
		}
		return b.String()
	}
	type dst struct {
		Note   string `json:"note"`
		Author *struct {
			Name string `json:"name"`
		} `json:"author"`
	}
	for _, tc := range []struct {
		name, body, first, last, tail string
	}{
		{"unknown members", `{` + unknown(150, "u") + `"note":"n"}`, "u000", "u099", "; and 50 more"},
		{"a repeat of a named member counts once", `{` + unknown(150, "u") + unknown(100, "u") + `"note":"n"}`, "u000", "u099", "; and 50 more"},
		// The walk keeps the names of the 100 members it names, and no more.
		{"a repeat of a left-out member counts again", `{` + unknown(150, "u") + unknown(150, "u") + `"note":"n"}`, "u000", "u099", "; and 100 more"},
		{"known problems share the bound", `{` + unknown(150, "u") + `"note":null,"author":null}`, "note", "u097", "; and 52 more"},
		{"members below an object share it too", `{"author":{` + unknown(150, "n") + `"name":"x"}}`, "author.n000", "author.n099", "; and 50 more"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := strictJSON([]byte(tc.body), new(dst))
			if err == nil {
				t.Fatal("strictJSON succeeded")
			}
			got := invalidParamsFor(err)
			if len(got) != maxInvalidParams || got[0].Name != tc.first || got[len(got)-1].Name != tc.last {
				t.Fatalf("%d items, first %v, last %v, want %d, %s, %s", len(got), got[0], got[len(got)-1], maxInvalidParams, tc.first, tc.last)
			}
			if msg := err.Error(); !strings.HasSuffix(msg, tc.tail) || strings.Count(msg, "; ") != maxInvalidParams {
				t.Fatalf("message ends %q with %d separators, want %q and %d", msg[len(msg)-20:], strings.Count(msg, "; "), tc.tail, maxInvalidParams)
			}
		})
	}
	// Past the bound a member costs the walk only: 2 allocations (its name
	// token). Building its problem, or a new value buffer for it, costs more.
	t.Run("keeps nothing past the bound", func(t *testing.T) {
		const n = 20000
		body := []byte("{" + unknown(n, "m") + `"note":"n"}`)
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		err := strictJSON(body, new(dst))
		runtime.ReadMemStats(&after)
		if err == nil {
			t.Fatal("strictJSON succeeded")
		}
		if perMember := float64(after.Mallocs-before.Mallocs) / n; perMember > 2.5 {
			t.Errorf("%.1f allocations for each unknown member, want about 2", perMember)
		}
	})
}
