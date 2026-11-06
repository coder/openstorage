package api

// #587: a response path that is not valid UTF-8 carries a <field>_base64
// sibling with the raw bytes. A path that is valid UTF-8 carries none.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/coder/openstorage/internal/auth"
)

const (
	rpFF    = "a\xffb"             // collides with rpFE as a JSON string
	rpFE    = "a\xfeb"             // collides with rpFF as a JSON string
	rpLat1  = "caf\xe9.txt"        // Latin-1 e acute
	rpUTF8  = "caf\u00e9-utf8.txt" // the valid control
	rpDirFE = "d\xfe/x.txt"        // the directory name is the invalid part
	rpReal  = "a\ufffdb"           // valid UTF-8 that holds a real U+FFFD
	rpPlain = "plain.txt"
	rpOld   = "old\xfdname.txt" // c2 renames it to rpNew
	rpNew   = "new\xfcname.txt"
	rpBin   = "bin\xfb.bin" // added by c2, binary: lands in filtered_files
)

// rpFixture is one repo seeded with the names above: c1 holds the first
// eight, c2 (main) renames rpOld to rpNew and adds rpBin. Branch base is c1.
type rpFixture struct {
	e       *testEnv
	token   string
	c1, c2  string
	content map[string]string // name -> file content at c2
}

func rpStage(t *testing.T, dir, name, content string) {
	t.Helper()
	tmp := filepath.Join(t.TempDir(), "blob")
	if err := os.WriteFile(tmp, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	sha := strings.TrimSpace(coreGit(t, dir, "hash-object", "-w", tmp))
	coreGit(t, dir, "update-index", "--add", "--cacheinfo", "100644,"+sha+","+name)
}

// rpBuild writes the tree through the index, so no file system has to
// accept the names.
func rpBuild(t *testing.T, e *testEnv) *rpFixture {
	t.Helper()
	org := e.newOrg(t)
	repoURL := "https://example.com/" + org + "/rawpath-" + randomHex(t, 4)
	repo, err := e.store.CreateRepo(context.Background(), org, repoURL, "main")
	if err != nil {
		t.Fatalf("create repo: %v", err)
	}
	dir := t.TempDir()
	coreGit(t, dir, "init", "-q", "-b", "main", ".")
	fx := &rpFixture{e: e, content: map[string]string{}}
	stage := func(name, content string) {
		rpStage(t, dir, name, content)
		fx.content[name] = content
	}
	for i, name := range []string{rpFF, rpFE, rpLat1, rpUTF8, rpDirFE, rpReal, rpPlain} {
		stage(name, "needle "+string(rune('a'+i))+"\n")
	}
	stage(rpOld, "renamed line\n")
	coreGit(t, dir, "commit", "-q", "-m", "c1")
	fx.c1 = coreRevParse(t, dir, "HEAD")

	coreGit(t, dir, "update-index", "--force-remove", "--", rpOld)
	delete(fx.content, rpOld)
	stage(rpNew, "renamed line\n")
	stage(rpBin, "\x00\x01 binary\n")
	coreGit(t, dir, "commit", "-q", "-m", "c2")
	fx.c2 = coreRevParse(t, dir, "HEAD")

	coreSeedStores(t, e, repo.RepoID, dir, map[string]string{"refs/heads/main": fx.c2, "refs/heads/base": fx.c1})
	fx.token = e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeGitRead}})
	return fx
}

func rpGet(t *testing.T, e *testEnv, token, path string) []byte {
	t.Helper()
	resp, body := coreGet(t, e, http.MethodGet, path, token, nil)
	coreRequireStatus(t, resp, body, http.StatusOK)
	return body
}

func (fx *rpFixture) get(t *testing.T, path string) []byte {
	t.Helper()
	return rpGet(t, fx.e, fx.token, path)
}

func (fx *rpFixture) post(t *testing.T, path string, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	resp, body := fx.e.doReq(t, http.MethodPost, path, fx.token, b, "")
	coreRequireStatus(t, resp, body, http.StatusOK)
	return body
}

// rpQuery percent-encodes the raw bytes of a path for a `path=` parameter.
func rpQuery(p string) string { return url.QueryEscape(p) }

// rpJSON decodes a JSON body without struct tags, so a missing key stays
// missing.
func rpJSON(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode %.200q: %v", body, err)
	}
	return out
}

func rpObjects(t *testing.T, doc map[string]any, key string) []map[string]any {
	t.Helper()
	list, ok := doc[key].([]any)
	if !ok {
		t.Fatalf("%q is not a list in %v", key, doc)
	}
	out := make([]map[string]any, len(list))
	for i, item := range list {
		if out[i], ok = item.(map[string]any); !ok {
			t.Fatalf("%s[%d] is not an object: %v", key, i, item)
		}
	}
	return out
}

// rpLossy reports whether a decoded JSON path shows a replaced byte. The
// real U+FFFD of the rpReal name is not one.
func rpLossy(listed string) bool {
	return strings.ContainsRune(listed, utf8.RuneError) && listed != rpReal
}

// rpRaw returns the raw path bytes of obj[field]: the sibling's decoded
// value when it is present, else the field itself. It also checks the pair
// as a whole: the lossy field holds one U+FFFD per invalid byte, and the
// sibling is present exactly when the bytes are not valid UTF-8.
func rpRaw(t *testing.T, where string, obj map[string]any, field string) string {
	t.Helper()
	listed, _ := obj[field].(string)
	sibling, hasSibling := obj[field+"_base64"]
	if !hasSibling {
		if rpLossy(listed) {
			t.Errorf("%s: %s %q has no %s_base64", where, field, listed, field)
		}
		return listed
	}
	enc, ok := sibling.(string)
	if !ok {
		t.Fatalf("%s: %s_base64 is %T", where, field, sibling)
	}
	raw, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		t.Fatalf("%s: %s_base64 %q: %v", where, field, enc, err)
	}
	if utf8.Valid(raw) {
		t.Errorf("%s: %s_base64 %q is present for valid UTF-8 %q", where, field, enc, raw)
	}
	if want := string([]rune(string(raw))); listed != want {
		t.Errorf("%s: %s = %q, want %q (one U+FFFD per invalid byte)", where, field, listed, want)
	}
	return string(raw)
}

func rpSorted(names ...string) []string {
	out := slices.Clone(names)
	slices.Sort(out)
	return out
}

func rpRawSet(t *testing.T, where string, objs []map[string]any, field string) []string {
	t.Helper()
	out := make([]string, len(objs))
	for i, o := range objs {
		out[i] = rpRaw(t, where, o, field)
	}
	return out
}

func TestRawPathSiblings(t *testing.T) { runBackends(t, testRawPathSiblings) }

func testRawPathSiblings(t *testing.T, e *testEnv) {
	fx := rpBuild(t, e)
	c1Names := []string{rpFF, rpFE, rpLat1, rpUTF8, rpDirFE, rpReal, rpPlain, rpOld}
	c2Names := []string{rpFF, rpFE, rpLat1, rpUTF8, rpDirFE, rpReal, rpPlain, rpNew, rpBin}

	t.Run("files streamed", func(t *testing.T) {
		doc := rpJSON(t, fx.get(t, "/api/v1/repos/files?recursive=true"))
		entries := rpObjects(t, doc, "entries")
		if got := rpRawSet(t, "entries", entries, "path"); !slices.Equal(got, rpSorted(c2Names...)) {
			t.Fatalf("entries = %q, want %q", got, rpSorted(c2Names...))
		}
		// The two colliding names list as one JSON string, the same as the
		// valid name rpReal, and differ in their siblings.
		var collide []map[string]any
		for _, o := range entries {
			if raw := rpRaw(t, "entries", o, "path"); raw == rpFF || raw == rpFE {
				collide = append(collide, o)
			}
		}
		if len(collide) != 2 || collide[0]["path"] != collide[1]["path"] || collide[0]["path"] != rpReal ||
			collide[0]["path_base64"] == nil || collide[0]["path_base64"] == collide[1]["path_base64"] {
			t.Errorf("colliding entries = %v", collide)
		}
		paths := doc["paths"].([]any)
		pathsB64, ok := doc["paths_base64"].([]any)
		if !ok || len(pathsB64) != len(paths) {
			t.Fatalf("paths_base64 = %v for %d paths", doc["paths_base64"], len(paths))
		}
		for i, enc := range pathsB64 {
			raw, err := base64.StdEncoding.DecodeString(enc.(string))
			if err != nil || string(raw) != rpRaw(t, "entries", entries[i], "path") {
				t.Errorf("paths_base64[%d] = %q, want the base64 of entries[%d]", i, enc, i)
			}
		}
	})

	t.Run("files paged and flat", func(t *testing.T) {
		var seen []string
		cursor := ""
		for page := 0; ; page++ {
			if page > len(c2Names) {
				t.Fatalf("no end after %d pages", page)
			}
			doc := rpJSON(t, fx.get(t, "/api/v1/repos/files?recursive=true&limit=3&cursor="+url.QueryEscape(cursor)))
			entries := rpObjects(t, doc, "entries")
			seen = append(seen, rpRawSet(t, "paged entries", entries, "path")...)
			lossy := slices.ContainsFunc(entries, func(o map[string]any) bool {
				_, has := o["path_base64"]
				return has && o["type"] == "blob"
			})
			if _, has := doc["paths_base64"]; has != lossy {
				t.Errorf("page %d: paths_base64 present = %v, but a blob entry has path_base64 = %v", page, has, lossy)
			}
			if doc["has_more"] != true {
				break
			}
			cursor = doc["next_cursor"].(string)
		}
		if !slices.Equal(seen, rpSorted(c2Names...)) {
			t.Errorf("paged names = %q, want %q", seen, rpSorted(c2Names...))
		}
		flat := rpJSON(t, fx.get(t, "/api/v1/repos/files?recursive=false"))
		got := rpRawSet(t, "flat entries", rpObjects(t, flat, "entries"), "path")
		if !slices.Contains(got, "d\xfe") {
			t.Errorf("flat entries = %q, want the tree d<0xfe>", got)
		}
	})

	t.Run("files metadata", func(t *testing.T) {
		doc := rpJSON(t, fx.get(t, "/api/v1/repos/files/metadata"))
		got := rpRawSet(t, "files", rpObjects(t, doc, "files"), "path")
		if !slices.Equal(got, rpSorted(c2Names...)) {
			t.Errorf("files = %q, want %q", got, rpSorted(c2Names...))
		}
	})

	t.Run("commit diff", func(t *testing.T) {
		root := rpJSON(t, fx.get(t, "/api/v1/repos/diff?sha="+fx.c1))
		if got := rpRawSet(t, "root diff", rpObjects(t, root, "files"), "path"); !slices.Equal(got, rpSorted(c1Names...)) {
			t.Errorf("root diff files = %q, want %q", got, rpSorted(c1Names...))
		}
		doc := rpJSON(t, fx.get(t, "/api/v1/repos/diff?sha="+fx.c2))
		files := rpObjects(t, doc, "files")
		if len(files) != 1 || rpRaw(t, "diff", files[0], "path") != rpNew || rpRaw(t, "diff", files[0], "old_path") != rpOld {
			t.Errorf("diff files = %v, want one rename %q -> %q", files, rpOld, rpNew)
		}
		filtered := rpObjects(t, doc, "filtered_files")
		if len(filtered) != 1 || rpRaw(t, "diff", filtered[0], "path") != rpBin {
			t.Errorf("filtered_files = %v, want %q", filtered, rpBin)
		}
		if _, has := filtered[0]["old_path_base64"]; has {
			t.Errorf("filtered_files[0] has old_path_base64 without an old_path: %v", filtered[0])
		}
	})

	t.Run("branches diff", func(t *testing.T) {
		doc := rpJSON(t, fx.get(t, "/api/v1/repos/branches/diff?branch=main&base=base"))
		files := rpObjects(t, doc, "files")
		if len(files) != 1 || rpRaw(t, "branches/diff", files[0], "path") != rpNew || rpRaw(t, "branches/diff", files[0], "old_path") != rpOld {
			t.Errorf("branches/diff files = %v, want one rename %q -> %q", files, rpOld, rpNew)
		}
		if got := rpRawSet(t, "branches/diff", rpObjects(t, doc, "filtered_files"), "path"); !slices.Equal(got, []string{rpBin}) {
			t.Errorf("filtered_files = %q, want %q", got, []string{rpBin})
		}
	})

	t.Run("blame", func(t *testing.T) {
		doc := rpJSON(t, fx.get(t, "/api/v1/repos/blame?path="+rpQuery(rpNew)))
		if got := rpRaw(t, "blame", doc, "path"); got != rpNew {
			t.Errorf("blame path = %q, want %q", got, rpNew)
		}
		lines := rpObjects(t, doc, "lines")
		if len(lines) != 1 || rpRaw(t, "blame line", lines[0], "original_path") != rpOld {
			t.Errorf("blame lines = %v, want original_path %q", lines, rpOld)
		}
		plain := rpJSON(t, fx.get(t, "/api/v1/repos/blame?path="+rpQuery(rpPlain)))
		if _, has := plain["path_base64"]; has {
			t.Errorf("blame of a valid path has path_base64: %v", plain)
		}
	})

	t.Run("grep", func(t *testing.T) {
		var seen []string
		cursor := ""
		for page := 0; ; page++ {
			if page > len(c2Names) {
				t.Fatalf("no end after %d pages", page)
			}
			body := map[string]any{"query": map[string]any{"pattern": "needle"}, "pagination": map[string]any{"limit": 2, "cursor": cursor}}
			doc := rpJSON(t, fx.post(t, "/api/v1/repos/grep", body))
			seen = append(seen, rpRawSet(t, "grep", rpObjects(t, doc, "matches"), "path")...)
			if doc["has_more"] != true {
				break
			}
			cursor = doc["next_cursor"].(string)
		}
		want := rpSorted(rpFF, rpFE, rpLat1, rpUTF8, rpDirFE, rpReal, rpPlain)
		if !slices.Equal(rpSorted(seen...), want) {
			t.Errorf("grep paths = %q, want %q", seen, want)
		}
	})

	t.Run("a listed path reads its file", func(t *testing.T) {
		entries := rpObjects(t, rpJSON(t, fx.get(t, "/api/v1/repos/files?recursive=true")), "entries")
		for _, o := range entries {
			raw := rpRaw(t, "entries", o, "path")
			got := fx.get(t, "/api/v1/repos/file?path="+rpQuery(raw))
			if string(got) != fx.content[raw] {
				t.Errorf("file?path=%q = %q, want %q", raw, got, fx.content[raw])
			}
		}
	})
}

// TestRawPathValidNamesAddNothing: a repo whose names are all valid UTF-8,
// including one with a real U+FFFD, answers with no _base64 key, and a
// submodule name that is not valid UTF-8 appears in entries only, because
// `paths` lists blobs only.
func TestRawPathValidNamesAddNothing(t *testing.T) { runBackends(t, testRawPathValidNamesAddNothing) }

func testRawPathValidNamesAddNothing(t *testing.T, e *testEnv) {
	org := e.newOrg(t)
	repoURL := "https://example.com/" + org + "/rawpath-valid-" + randomHex(t, 4)
	repo, err := e.store.CreateRepo(context.Background(), org, repoURL, "main")
	if err != nil {
		t.Fatalf("create repo: %v", err)
	}
	dir := t.TempDir()
	coreGit(t, dir, "init", "-q", "-b", "main", ".")
	for _, name := range []string{rpUTF8, rpReal, rpPlain, "q\"t", "d/e.txt"} {
		rpStage(t, dir, name, "needle\n")
	}
	coreGit(t, dir, "commit", "-q", "-m", "valid names")
	head := coreRevParse(t, dir, "HEAD")
	coreSeedStores(t, e, repo.RepoID, dir, map[string]string{"refs/heads/main": head})
	token := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeGitRead}})
	get := func(path string) []byte { return rpGet(t, e, token, path) }
	resp, grep := e.doReq(t, http.MethodPost, "/api/v1/repos/grep", token, []byte(`{"query":{"pattern":"needle"}}`), "")
	coreRequireStatus(t, resp, grep, http.StatusOK)
	bodies := map[string][]byte{
		"files":          get("/api/v1/repos/files"),
		"files paged":    get("/api/v1/repos/files?limit=2"),
		"files flat":     get("/api/v1/repos/files?recursive=false"),
		"files/metadata": get("/api/v1/repos/files/metadata"),
		"diff":           get("/api/v1/repos/diff?sha=" + head),
		"blame":          get("/api/v1/repos/blame?path=" + rpQuery(rpReal)),
		"grep":           grep,
	}
	for route, body := range bodies {
		if bytes.Contains(body, []byte("_base64")) {
			t.Errorf("%s answers a _base64 key for valid names: %.300s", route, body)
		}
	}

	// A submodule entry whose name is not valid UTF-8: entries get the
	// sibling, `paths` (blobs only) gets none.
	coreGit(t, dir, "update-index", "--add", "--cacheinfo", "160000,"+head+",sub\xfe")
	coreGit(t, dir, "commit", "-q", "-m", "submodule")
	next := coreRevParse(t, dir, "HEAD")
	coreSeedStores(t, e, repo.RepoID, dir, map[string]string{"refs/heads/other": next})
	doc := rpJSON(t, get("/api/v1/repos/files?ref=other"))
	if _, has := doc["paths_base64"]; has {
		t.Errorf("paths_base64 for blob names that are all valid UTF-8: %v", doc)
	}
	var sub map[string]any
	for _, o := range rpObjects(t, doc, "entries") {
		if o["type"] == "submodule" {
			sub = o
		}
	}
	if sub == nil || rpRaw(t, "entries", sub, "path") != "sub\xfe" {
		t.Errorf("entries = %v, want the submodule sub<0xfe> with path_base64", doc["entries"])
	}
}
