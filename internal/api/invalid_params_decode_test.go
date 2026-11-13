package api

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/meta"
)

// ipdSecret stands for a credential in a request: no answer may hold it.
const ipdSecret = "ipd-secret-value"

// ipdField is one member of a request body: kind is s (string), b
// (boolean), i (integer), a (array of strings) or o (object with kids).
type ipdField struct {
	name   string
	kind   byte
	secret bool
	kids   []ipdField
}

func ipdF(name string, kind byte, kids ...ipdField) ipdField {
	return ipdField{name: name, kind: kind, kids: kids}
}

func ipdSecretF(name string) ipdField { return ipdField{name: name, kind: 's', secret: true} }

// ipdFamily is the decode of one route family. A wrap family decodes an
// object that sits in the body member wrap, with its own message prefix.
type ipdFamily struct {
	name, method, path, tok string
	newReq                  func() any
	schema                  []ipdField
	wrap, msgPrefix         string
}

func ipdFamilies(fx ipdFixture) []ipdFamily {
	pair := func(n string) ipdField { return ipdF(n, 'o', ipdF("name", 's'), ipdF("email", 's')) }
	obj := func(n string, kids ...ipdField) ipdField { return ipdF(n, 'o', kids...) }
	f := func(name, method, path, tok string, newReq func() any, schema ...ipdField) ipdFamily {
		return ipdFamily{name: name, method: method, path: path, tok: tok, newReq: newReq, schema: schema, msgPrefix: "invalid JSON body: "}
	}
	note := []ipdField{ipdF("object_ref", 's'), ipdF("sha", 's'), ipdF("action", 's'), ipdF("note", 's'), ipdF("notes_ref", 's'),
		ipdF("ref", 's'), ipdF("expected_notes_ref_sha", 's'), ipdF("expected_ref_sha", 's'), pair("author")}
	baseRepo := f("create base_repo", http.MethodPost, "/api/v1/repos", fx.repoTok, func() any { return new(usyncBaseRepo) },
		ipdF("provider", 's'), ipdF("owner", 's'), ipdF("name", 's'), ipdF("operation", 's'), ipdF("ref", 's'), ipdF("sha", 's'),
		ipdF("default_branch", 's'), ipdF("upstream_host", 's'), obj("auth", ipdSecretF("token"), ipdF("auth_type", 's')), ipdF("forward_pushes", 'b'))
	baseRepo.wrap, baseRepo.msgPrefix = "base_repo", "invalid base_repo: "
	return []ipdFamily{
		f("create", http.MethodPost, "/api/v1/repos", fx.repoTok, func() any { return new(createRepoRequest) },
			ipdF("repo_name", 's'), ipdF("default_branch", 's')),
		baseRepo,
		f("credential POST", http.MethodPost, "/api/v1/repos/git-credentials", fx.repoTok, func() any { return new(gcredCreateRequest) },
			ipdF("repo_id", 's'), ipdSecretF("username"), ipdSecretF("password")),
		f("credential PUT", http.MethodPut, "/api/v1/repos/git-credentials", fx.orgTok, func() any { return new(gcredUpdateRequest) },
			ipdF("id", 's'), ipdSecretF("username"), ipdSecretF("password")),
		f("credential PUT by path", http.MethodPut, fx.credPath, fx.repoTok, func() any { return new(gcredPathUpdateRequest) },
			ipdSecretF("username"), ipdSecretF("password")),
		f("grep", http.MethodPost, "/api/v1/repos/grep", fx.gitTok, func() any { return new(grepRequestBody) },
			obj("query", ipdF("pattern", 's'), ipdF("case_sensitive", 'b')), ipdF("ref", 's'), ipdF("rev", 's'), ipdF("ephemeral", 'b'), ipdF("paths", 'a'),
			obj("file_filters", ipdF("include_globs", 'a'), ipdF("exclude_globs", 'a'), ipdF("extension_filters", 'a')),
			obj("context", ipdF("before", 'i'), ipdF("after", 'i')), obj("limits", ipdF("max_lines", 'i'), ipdF("max_matches_per_file", 'i')),
			obj("pagination", ipdF("cursor", 's'), ipdF("limit", 'i'))),
		f("archive", http.MethodPost, "/api/v1/repos/archive", fx.gitTok, func() any { return new(archiveRequestBody) },
			ipdF("ref", 's'), ipdF("include_globs", 'a'), ipdF("exclude_globs", 'a'), ipdF("max_blob_size", 'i'), obj("archive", ipdF("prefix", 's'))),
		f("webhook", http.MethodPost, "/api/v1/webhooks", fx.orgTok, func() any { return new(createWebhookRequest) },
			ipdF("url", 's'), ipdSecretF("secret")),
		f("merge", http.MethodPost, "/api/v1/repos/merge", fx.gitTok, func() any { return new(wmMergeRequest) },
			ipdF("source_ref", 's'), ipdF("source_branch", 's'), ipdF("target_branch", 's'), ipdF("strategy", 's'), ipdF("source_is_ephemeral", 'b'),
			ipdF("target_is_ephemeral", 'b'), ipdF("expected_target_sha", 's'), ipdF("commit_message", 's'), pair("author"), pair("committer"),
			ipdF("allow_unrelated_histories", 'b'), ipdF("squash", 'b')),
		f("branch create", http.MethodPost, "/api/v1/repos/branches/create", fx.gitTok, func() any { return new(wrefCreateBranchRequest) },
			ipdF("base_ref", 's'), ipdF("base_branch", 's'), ipdF("base_is_ephemeral", 'b'), ipdF("target_is_ephemeral", 'b'), ipdF("force", 'b'),
			ipdF("preferred_base", 's'), ipdF("expected_target_sha", 's')),
		f("branch delete", http.MethodDelete, "/api/v1/repos/branches", fx.gitTok, func() any { return new(wrefDeleteBranchRequest) },
			ipdF("target_branch", 's'), ipdF("name", 's'), ipdF("ephemeral", 'b')),
		f("tag create", http.MethodPost, "/api/v1/repos/tags", fx.gitTok, func() any { return new(wrefCreateTagRequest) },
			ipdF("name", 's'), ipdF("ref", 's'), ipdF("target", 's')),
		f("note add", http.MethodPost, "/api/v1/repos/notes", fx.gitTok, func() any { return new(wrefNoteRequest) }, note...),
		f("note delete", http.MethodDelete, "/api/v1/repos/notes", fx.gitTok, func() any { return new(wrefNoteRequest) }, note...),
	}
}

// ipdFixture is a repository, a credential and the tokens of every family.
type ipdFixture struct {
	ipFixture
	orgTok, credPath string
}

func ipdNewFixture(t *testing.T, e *testEnv) ipdFixture {
	t.Helper()
	fx := ipNewFixture(t, e)
	cred, err := e.store.CreateGitCredential(context.Background(), meta.GitCredential{RepoID: fx.repoID, Username: "u0", Password: "p0"})
	if err != nil {
		t.Fatalf("create credential: %v", err)
	}
	org := e.newOrg(t)
	return ipdFixture{ipFixture: fx, orgTok: e.mint(t, tokenSpec{iss: org, repo: auth.RepoClaimOrg, scopes: []string{auth.ScopeRepoWrite}}),
		credPath: "/api/repos/" + url.PathEscape(fx.url) + "/git-credentials/" + cred.ID}
}

// ipdWrong lists values of the wrong JSON type for each kind of field.
var ipdWrong = map[byte][]string{
	's': {`7`, `true`, `{}`, `[]`, `1.5`}, 'b': {`"x"`, `7`, `{}`, `[]`}, 'i': {`"x"`, `true`, `{}`, `[]`, `1.5`},
	'a': {`"x"`, `7`, `true`, `{}`}, 'o': {`"x"`, `7`, `true`, `[]`},
}

var ipdReason = map[byte]string{'s': "must be a string", 'b': "must be a boolean", 'i': "must be an integer", 'a': "must be an array", 'o': "must be an object"}

// ipdMember is a generated member of an object, or an element of an array:
// a leaf holds right or bad, a container (open is { or [) holds kids.
type ipdMember struct {
	name, right, bad, reason string
	open                     byte
	kids                     []ipdMember
}

// ipdFail is a field that the generator made the wrong type.
type ipdFail struct{ path, reason string }

// render returns the JSON of m. A wrong-type leaf prints as null (which
// every type accepts) unless keep says to print it wrong.
func (m ipdMember) render(path string, keep func(string) bool) string {
	if m.bad != "" {
		if keep(path) {
			return m.bad
		}
		return "null"
	}
	if m.open == 0 {
		return m.right
	}
	parts := make([]string, len(m.kids))
	for i, k := range m.kids {
		parts[i] = k.render(ipdJoin(path, k.name), keep)
		if m.open == '{' {
			parts[i] = strconv.Quote(k.name) + ":" + parts[i]
		}
	}
	return string(m.open) + strings.Join(parts, ",") + map[byte]string{'{': "}", '[': "]"}[m.open]
}

func ipdJoin(path, name string) string {
	if path == "" {
		return name
	}
	return path + "." + name
}

// fails lists the wrong-type leaves of m in body order.
func (m ipdMember) fails(path string) (out []ipdFail) {
	if m.bad != "" {
		return []ipdFail{{path, m.reason}}
	}
	for _, k := range m.kids {
		out = append(out, k.fails(ipdJoin(path, k.name))...)
	}
	return out
}

// ipdGen builds a member of field f whose leaves are the wrong type with
// probability p.
func ipdGen(rng *rand.Rand, f ipdField, name string, p float64) ipdMember {
	m := ipdMember{name: name, reason: ipdReason[f.kind]}
	if rng.Float64() < p {
		pool := ipdWrong[f.kind]
		if f.secret {
			pool = []string{`{"` + ipdSecret + `":"` + ipdSecret + `"}`, `["` + ipdSecret + `"]`}
		}
		m.bad = pool[rng.IntN(len(pool))]
		return m
	}
	switch f.kind {
	case 'a':
		m.open = '['
		for i := range rng.IntN(7) {
			m.kids = append(m.kids, ipdGen(rng, ipdField{kind: 's'}, strconv.Itoa(i), p))
		}
	case 'o':
		m.open = '{'
		for _, i := range rng.Perm(len(f.kids))[:1+rng.IntN(len(f.kids))] {
			m.kids = append(m.kids, ipdGen(rng, f.kids[i], f.kids[i].name, p))
		}
	default:
		m.right = map[byte]string{'s': `"v"`, 'b': `true`, 'i': `5`}[f.kind]
		if f.secret {
			m.right = `"` + ipdSecret + `"`
		}
		if rng.IntN(10) == 0 {
			m.right = "null"
		}
	}
	return m
}

// ipdAnswer is the 400 of one request.
type ipdAnswer struct {
	status int
	msg    string
	params []ipParam
	raw    []byte
	took   time.Duration
}

func ipdAsk(t *testing.T, e *testEnv, f ipdFamily, body string) ipdAnswer {
	t.Helper()
	start := time.Now()
	resp, raw := e.doReq(t, f.method, f.path, f.tok, []byte(body), "")
	a := ipdAnswer{status: resp.StatusCode, raw: raw, took: time.Since(start)}
	var p problemBody
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("%s: answer %.200q is not JSON: %v", f.name, raw, err)
	}
	if a.msg = p.Error; p.Detail != p.Error {
		t.Fatalf("%s: detail %q differs from error %q", f.name, p.Detail, p.Error)
	}
	for _, ip := range p.InvalidParams {
		a.params = append(a.params, ipParam{ip.Name, ip.Reason})
	}
	return a
}

// wrapBody puts the object of a wrap family into its body member.
func (f ipdFamily) wrapBody(object string) string {
	if f.wrap == "" {
		return object
	}
	return `{"` + f.wrap + `":` + object + `}`
}

// ipdCase is a generated body and the answer that the oracle expects.
type ipdCase struct {
	body, msg string
	want      []ipParam
}

// ipdCases generates each bodies with one wrong-type field and each with
// several for family f. The oracle names each wrong-type field in body
// order, and its message is the message that encoding/json gives for a
// body with that field alone (the answer of the route before this change),
// joined by "; ". It does not use unmarshalBody.
func ipdCases(t *testing.T, f ipdFamily, each int) (one, many []ipdCase) {
	t.Helper()
	rng := rand.New(rand.NewPCG(1, uint64(len(f.name))))
	all := func(string) bool { return true }
	for try := 0; (len(one) < each || len(many) < each) && try < 20*each; try++ {
		root, p := ipdMember{open: '{'}, []float64{0.1, 0.3, 0.6}[rng.IntN(3)]
		for _, i := range rng.Perm(len(f.schema))[:1+rng.IntN(len(f.schema))] {
			root.kids = append(root.kids, ipdGen(rng, f.schema[i], f.schema[i].name, p))
		}
		fails := root.fails("")
		if len(fails) == 0 || (len(fails) == 1 && len(one) == each) || (len(fails) > 1 && len(many) == each) {
			continue
		}
		var msgs []string
		c := ipdCase{body: f.wrapBody(root.render("", all))}
		for _, fail := range fails {
			alone := root.render("", func(path string) bool { return path == fail.path })
			err := json.Unmarshal([]byte(alone), f.newReq())
			if te, ok := errors.AsType[*json.UnmarshalTypeError](err); !ok || te.Field != fail.path {
				t.Fatalf("generator: %s alone gives %v, want a type error at %s", alone, err, fail.path)
			}
			msgs = append(msgs, err.Error())
			c.want = append(c.want, ipParam{ipdJoin(f.wrap, fail.path), fail.reason})
		}
		c.msg = f.msgPrefix + strings.Join(msgs, "; ")
		if len(fails) == 1 {
			one = append(one, c)
		} else {
			many = append(many, c)
		}
	}
	if len(one) < each || len(many) < each {
		t.Fatalf("generated %d bodies with one and %d with several wrong-type fields, want %d of each", len(one), len(many), each)
	}
	return one, many
}

// TestDecodeNamesEveryWrongTypeField compares the answer for generated
// bodies with the oracle of ipdCases. A body with one wrong-type field must
// answer as it did before, and a body with several must name each of them.
func TestDecodeNamesEveryWrongTypeField(t *testing.T) {
	const each = 1000
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := ipdNewFixture(t, e)
		for _, f := range ipdFamilies(fx) {
			t.Run(f.name, func(t *testing.T) {
				one, many := ipdCases(t, f, each)
				for _, group := range []struct {
					name  string
					cases []ipdCase
				}{{"one wrong-type field", one}, {"several wrong-type fields", many}} {
					for i, chunk := range slices.Collect(slices.Chunk(group.cases, 250)) {
						t.Run(group.name+" "+strconv.Itoa(i), func(t *testing.T) {
							t.Parallel()
							for _, c := range chunk {
								a := ipdAsk(t, e, f, c.body)
								if a.status != http.StatusBadRequest || a.msg != c.msg || !slices.Equal(a.params, c.want) || strings.Contains(string(a.raw), ipdSecret) {
									t.Fatalf("body %s\n answer %d %q %+v\n want   400 %q %+v", c.body, a.status, a.msg, a.params, c.msg, c.want)
								}
							}
						})
					}
				}
			})
		}
	})
}

// TestDecodeRows: a body with several wrong-type fields answers with each
// of them, in body order (not in the order of the struct or the docs), and
// nothing else: the other checks of the route run only when every field
// has its right type.
func TestDecodeRows(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := ipdNewFixture(t, e)
		fams := map[string]ipdFamily{}
		for _, f := range ipdFamilies(fx) {
			fams[f.name] = f
		}
		// msg is the message of encoding/json for one field.
		msg := func(st, field, kind, typ string) string {
			return "json: cannot unmarshal " + kind + " into Go struct field " + st + "." + field + " of type " + typ
		}
		str := func(st, field, kind string) string { return msg(st, field, kind, "string") }
		// An array element has no "Go struct field" in encoding/json's message.
		elem := func(st, field, kind string) string {
			return "json: cannot unmarshal " + kind + " into " + st + "." + field + " of type string"
		}
		row := func(family, name, body, prefix string, msgs []string, want ...ipParam) ipCase {
			f := fams[family]
			return ipCase{name: family + " " + name, method: f.method, path: f.path, token: f.tok, body: body,
				wantError: prefix + strings.Join(msgs, "; "), want: want}
		}
		const body, base = "invalid JSON body: ", "invalid base_repo: "
		s := func(names ...string) (out []ipParam) {
			for _, n := range names {
				out = append(out, ipParam{n, "must be a string"})
			}
			return out
		}
		paths := make([]string, 150)
		var capMsgs []string
		var capWant []ipParam
		for i := range paths {
			paths[i] = "7"
		}
		for i := range maxInvalidParams {
			capMsgs = append(capMsgs, elem("grepRequestBody", "paths."+strconv.Itoa(i), "number"))
			capWant = append(capWant, ipParam{"paths." + strconv.Itoa(i), "must be a string"})
		}
		ipRun(t, e, []ipCase{
			row("create", "every field", `{"default_branch":7,"repo_name":true}`, body,
				[]string{str("createRepoRequest", "default_branch", "number"), str("createRepoRequest", "repo_name", "bool")}, s("default_branch", "repo_name")...),
			row("create base_repo", "nested fields", `{"base_repo":{"owner":1,"auth":{"token":2,"auth_type":3},"name":4}}`, base,
				[]string{str("usyncBaseRepo", "owner", "number"), str("usyncBaseRepo", "auth.token", "number"), str("usyncBaseRepo", "auth.auth_type", "number"),
					str("usyncBaseRepo", "name", "number")}, s("base_repo.owner", "base_repo.auth.token", "base_repo.auth.auth_type", "base_repo.name")...),
			row("create base_repo", "a fork", `{"base_repo":{"operation":"fork","provider":"code","name":1,"ref":2}}`, base,
				[]string{str("usyncBaseRepo", "name", "number"), str("usyncBaseRepo", "ref", "number")}, s("base_repo.name", "base_repo.ref")...),
			row("credential POST", "no value in the answer", `{"password":{"`+ipdSecret+`":1},"username":["`+ipdSecret+`"],"repo_id":1}`, body,
				[]string{str("gcredCreateRequest", "password", "object"), str("gcredCreateRequest", "username", "array"), str("gcredCreateRequest", "repo_id", "number")},
				s("password", "username", "repo_id")...),
			row("credential PUT", "every field", `{"password":1,"id":2,"username":3}`, body,
				[]string{str("gcredUpdateRequest", "password", "number"), str("gcredUpdateRequest", "id", "number"), str("gcredUpdateRequest", "username", "number")},
				s("password", "id", "username")...),
			row("credential PUT by path", "every field", `{"username":1,"password":2}`, body,
				[]string{str("gcredPathUpdateRequest", "username", "number"), str("gcredPathUpdateRequest", "password", "number")}, s("username", "password")...),
			row("grep", "array elements, nested fields", `{"paths":["a",1,"b",true],"query":{"pattern":5},"limits":{"max_lines":"x"}}`, body,
				[]string{elem("grepRequestBody", "paths.1", "number"), elem("grepRequestBody", "paths.3", "bool"), str("grepRequestBody", "query.pattern", "number"),
					msg("grepRequestBody", "limits.max_lines", "string", "int")},
				ipParam{"paths.1", "must be a string"}, ipParam{"paths.3", "must be a string"}, ipParam{"query.pattern", "must be a string"},
				ipParam{"limits.max_lines", "must be an integer"}),
			{
				name: "grep the answer holds 100 items and does not say that it left some out", method: http.MethodPost, path: "/api/v1/repos/grep", token: fx.gitTok,
				body: `{"paths":[` + strings.Join(paths, ",") + `]}`, wantError: body + strings.Join(capMsgs, "; "), want: capWant,
			},
			row("archive", "every field", `{"max_blob_size":"1","include_globs":["a",2],"archive":{"prefix":3}}`, body,
				[]string{msg("archiveRequestBody", "max_blob_size", "string", "int64"), elem("archiveRequestBody", "include_globs.1", "number"),
					str("archiveRequestBody", "archive.prefix", "number")},
				ipParam{"max_blob_size", "must be an integer"}, ipParam{"include_globs.1", "must be a string"}, ipParam{"archive.prefix", "must be a string"}),
			row("webhook", "every field", `{"secret":5,"url":{}}`, body,
				[]string{str("createWebhookRequest", "secret", "number"), str("createWebhookRequest", "url", "object")}, s("secret", "url")...),
			// A semantic problem of another field (an empty source_ref, a
			// target_branch with refs/) is not named next to a type error.
			row("merge", "only the wrong-type fields", `{"strategy":5,"source_ref":"","squash":"yes","author":{"name":1,"email":[]},"target_branch":"refs/heads/x"}`, body,
				[]string{str("wmMergeRequest", "strategy", "number"), msg("wmMergeRequest", "squash", "string", "bool"), str("wmMergeRequest", "author.name", "number"),
					str("wmMergeRequest", "author.email", "array")},
				ipParam{"strategy", "must be a string"}, ipParam{"squash", "must be a boolean"}, ipParam{"author.name", "must be a string"}, ipParam{"author.email", "must be a string"}),
			row("branch create", "an unknown field is not named", `{"force":"y","base_ref":5,"typo":1}`, body,
				[]string{msg("request", "force", "string", "bool"), str("request", "base_ref", "number")}, ipParam{"force", "must be a boolean"}, ipParam{"base_ref", "must be a string"}),
			row("branch delete", "every field", `{"ephemeral":"x","name":1}`, body,
				[]string{msg("wrefDeleteBranchRequest", "ephemeral", "string", "bool"), str("wrefDeleteBranchRequest", "name", "number")},
				ipParam{"ephemeral", "must be a boolean"}, ipParam{"name", "must be a string"}),
			row("tag create", "every field", `{"ref":1,"name":2}`, body,
				[]string{str("wrefCreateTagRequest", "ref", "number"), str("wrefCreateTagRequest", "name", "number")}, s("ref", "name")...),
			row("note add", "nested field", `{"note":1,"author":{"email":2},"object_ref":3}`, body,
				[]string{str("wrefNoteRequest", "note", "number"), str("wrefNoteRequest", "author.email", "number"), str("wrefNoteRequest", "object_ref", "number")},
				s("note", "author.email", "object_ref")...),
			row("note delete", "every field", `{"note":1,"author":{"email":2},"object_ref":3}`, body,
				[]string{str("wrefNoteRequest", "note", "number"), str("wrefNoteRequest", "author.email", "number"), str("wrefNoteRequest", "object_ref", "number")},
				s("note", "author.email", "object_ref")...),

			// One wrong-type field answers exactly as before, ahead of
			// every other check and with no second name.
			row("create", "one field: ahead of the name check", `{"repo_name":"other","default_branch":7}`, body,
				[]string{str("createRepoRequest", "default_branch", "number")}, s("default_branch")...),
			// A member that repeats with another spelling counts once.
			row("webhook", "case-folded and repeated members", `{"URL":1,"Secret":2,"url":3,"SECRET":{}}`, body,
				[]string{str("createWebhookRequest", "URL", "number"), str("createWebhookRequest", "Secret", "number")}, s("URL", "Secret")...),
			row("webhook", "one field: ahead of the url check", `{"url":"not a url","secret":5}`, body,
				[]string{str("createWebhookRequest", "secret", "number")}, s("secret")...),
			row("grep", "one field", `{"paths":["a",1]}`, body, []string{elem("grepRequestBody", "paths.1", "number")}, s("paths.1")...),
			// Every other decode error answers as before, with no item.
			row("webhook", "a body that is not an object", `[1]`, body, []string{"json: cannot unmarshal array into Go value of type api.createWebhookRequest"}),
			row("grep", "a syntax error", `{"paths":[1,`, body, []string{"unexpected end of JSON input"}),
		})
	})
}

// TestDecodeHostileBodies: bodies near the 1 MiB limit with many wrong-type
// members, deep nesting, repeated and case-folded member names, invalid
// UTF-8 and escaped NUL answer 400 within a bounded time and size, name at
// most 100 fields, and never hold a request value that is a credential.
// The shapes with a known answer pin the work bound: a body names
// floor(256 KiB / its size) fields, and a body of more than 256 KiB only
// the first.
func TestDecodeHostileBodies(t *testing.T) {
	const slowLimit, sizeLimit = 10 * time.Second, 64 << 10
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := ipdNewFixture(t, e)
		var slowest time.Duration
		var slowName string
		for _, f := range ipdFamilies(fx) {
			var leaves []string
			var secret, array string // a member that holds a credential, and a top-level array member
			for _, s := range f.schema {
				switch {
				case s.kind == 'a' && array == "":
					array = s.name
				case s.kind == 'o' && s.kids[len(s.kids)-1].secret || s.kind == 'o' && s.kids[0].secret:
					secret = `"` + s.name + `":{"` + s.kids[0].name + `":"` + ipdSecret + `"}`
				case s.secret && secret == "":
					secret = `"` + s.name + `":"` + ipdSecret + `"`
				}
				if strings.Contains("sbi", string(s.kind)) {
					leaves = append(leaves, s.name)
				}
			}
			// G is the first wrong-type member of every shape, F the second.
			g, ff := `"`+leaves[0]+`":{}`, `"`+leaves[1]+`":{}`
			obj := func(members ...string) string {
				return "{" + strings.Join(slices.DeleteFunc(append([]string{secret}, members...), func(m string) bool { return m == "" }), ",") + "}"
			}
			names := func(n ...string) []string {
				for i := range n {
					n[i] = ipdJoin(f.wrap, n[i])
				}
				return n
			}
			upper := `"` + strings.ToUpper(leaves[1]) + `":[]`
			repeat := func(member string, size int) string { return strings.Repeat(","+member, size/(len(member)+1)) }
			bigObject := func(size int) string { return obj(g + repeat(`"u":0`, size-300) + "," + ff) }
			shapes := []struct {
				name, body string
				want       []string // the names of the answer; nil when it is not pinned
			}{
				{"1 MiB of one repeated member", "{" + g + repeat(ff, 1_040_000) + "}", names(leaves[0])},
				{"100 KiB of one repeated member", "{" + g + repeat(ff, 100<<10) + "}", names(leaves[0], leaves[1])},
				{"126 KiB of one repeated member", "{" + g + repeat(ff, 126<<10) + "}", names(leaves[0], leaves[1])},
				{"130 KiB of one repeated member", "{" + g + repeat(ff, 130<<10) + "}", names(leaves[0])},
				{"case-folded repeated members", "{" + g + "," + ff + repeat(upper+","+ff, 60<<10) + "}", names(leaves[0], leaves[1])},
				{"1 MiB of unknown members", bigObject(1_040_000), names(leaves[0])},
				{"120 KiB of unknown members", bigObject(120 << 10), names(leaves[0], leaves[1])},
				{"nesting of depth 5000", obj(g, `"`+leaves[1]+`":`+strings.Repeat("[", 5000)+strings.Repeat("]", 5000)), names(leaves[0], leaves[1])},
				{"nesting past the decoder's limit", obj(g, `"`+leaves[1]+`":`+strings.Repeat("[", 20000)+strings.Repeat("]", 20000)), nil},
				{"invalid UTF-8 in a name", obj(g, "\"\xff\xfe\":1", ff), names(leaves[0], leaves[1])},
				{"an escaped NUL in a name", obj(g, `"`+leaves[1]+`\u0000":1`, ff), names(leaves[0], leaves[1])},
				{"a NUL byte in a value", obj(g, `"`+leaves[1]+"\":\"a\x00b\""), nil},
			}
			if array != "" {
				shapes = append(shapes,
					struct {
						name, body string
						want       []string
					}{"1 MiB of wrong-type elements", `{"` + array + `":[7` + repeat("7", 1_040_000) + `]}`, names(array + ".0")},
					struct {
						name, body string
						want       []string
					}{"100 KiB of wrong-type elements", `{"` + array + `":[7` + repeat("7", 100<<10) + `]}`, names(array+".0", array+".1")})
			}
			for _, sh := range shapes {
				a := ipdAsk(t, e, f, f.wrapBody(sh.body))
				if a.took > slowest {
					slowest, slowName = a.took, f.name+": "+sh.name
				}
				var got []string
				for _, p := range a.params {
					got = append(got, p.name)
				}
				switch {
				case a.status != http.StatusBadRequest, a.took > slowLimit, len(a.raw) > sizeLimit, len(a.params) > maxInvalidParams,
					strings.Contains(string(a.raw), ipdSecret), sh.want != nil && !slices.Equal(got, sh.want):
					t.Errorf("%s: %s (%d bytes): status %d in %v, answer of %d bytes names %v, want 400 within %v and %d bytes, names %v, no credential (%.150q)",
						f.name, sh.name, len(sh.body), a.status, a.took, len(a.raw), got, slowLimit, sizeLimit, sh.want, a.raw)
				}
			}
		}
		t.Logf("slowest answer: %v (%s)", slowest, slowName)
	})
}
