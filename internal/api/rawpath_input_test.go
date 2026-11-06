package api

// #587: request inputs that name a path with raw bytes. grep paths and
// globs, archive globs and commit-pack file paths take a base64 form.

import (
	"bytes"
	"encoding/base64"
	"net/http"
	"slices"
	"testing"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/ref"
)

func rpB64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

func (fx *rpFixture) grep(t *testing.T, body map[string]any) []string {
	t.Helper()
	body["query"] = map[string]any{"pattern": "needle"}
	doc := rpJSON(t, fx.post(t, "/api/v1/repos/grep", body))
	return rpRawSet(t, "grep", rpObjects(t, doc, "matches"), "path")
}

func TestRawPathInputs(t *testing.T) { runBackends(t, testRawPathInputs) }

func testRawPathInputs(t *testing.T, e *testEnv) {
	fx := rpBuild(t, e)

	t.Run("grep paths_base64 names one of two colliding files", func(t *testing.T) {
		for _, c := range []struct {
			name string
			body map[string]any
			want []string
		}{
			{"paths_base64 ff", map[string]any{"paths_base64": []string{rpB64(rpFF)}}, []string{rpFF}},
			{"paths_base64 fe", map[string]any{"paths_base64": []string{rpB64(rpFE)}}, []string{rpFE}},
			{"the listed form names the valid file", map[string]any{"paths": []string{rpReal}}, []string{rpReal}},
			{"a directory prefix", map[string]any{"paths_base64": []string{rpB64("d\xfe")}}, []string{rpDirFE}},
			{"union with paths", map[string]any{"paths": []string{rpPlain}, "paths_base64": []string{rpB64(rpFE)}}, []string{rpFE, rpPlain}},
			{"a valid name in paths_base64", map[string]any{"paths_base64": []string{rpB64(rpPlain)}}, []string{rpPlain}},
			{"include_globs_base64 is literal", map[string]any{"file_filters": map[string]any{"include_globs_base64": []string{rpB64(rpLat1)}}}, []string{rpLat1}},
			{"include and exclude", map[string]any{"file_filters": map[string]any{"include_globs": []string{"a*"}, "exclude_globs_base64": []string{rpB64(rpFF)}}}, []string{rpFE, rpReal}},
			{"a slash glob", map[string]any{"file_filters": map[string]any{"include_globs_base64": []string{rpB64("d\xfe/*.txt")}}}, []string{rpDirFE}},
		} {
			if got := fx.grep(t, c.body); !slices.Equal(rpSorted(got...), rpSorted(c.want...)) {
				t.Errorf("%s: grep paths = %q, want %q", c.name, got, rpSorted(c.want...))
			}
		}
	})

	t.Run("a bad base64 input is a 400 that names it", func(t *testing.T) {
		for _, c := range []struct{ field, in string }{
			{"paths_base64", ""},
			{"paths_base64", "!!"},
			{"paths_base64", "YQ"},   // no padding
			{"paths_base64", "YR=="}, // padding bits set
			{"paths_base64", "Yf9\ni"},
			{"paths_base64", "-_8="}, // URL-safe alphabet
			{"paths_base64", rpB64("  ")},
			{"paths_base64", rpB64("a\x00b")},
			{"file_filters.include_globs_base64", "!!"},
			{"file_filters.exclude_globs_base64", rpB64("\t")},
		} {
			list := []string{rpB64(rpPlain), c.in}
			body := map[string]any{"query": map[string]any{"pattern": "needle"}}
			switch c.field {
			case "paths_base64":
				body["paths_base64"] = list
			case "file_filters.include_globs_base64":
				body["file_filters"] = map[string]any{"include_globs_base64": list}
			default:
				body["file_filters"] = map[string]any{"exclude_globs_base64": list}
			}
			resp, raw := e.doReq(t, http.MethodPost, "/api/v1/repos/grep", fx.token, rqGrepBody(t, body), "")
			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("%s %q: status = %d, want 400 (%s)", c.field, c.in, resp.StatusCode, raw)
				continue
			}
			params, _ := rpJSON(t, raw)["invalid_params"].([]any)
			if len(params) != 1 || params[0].(map[string]any)["name"] != c.field+".1" {
				t.Errorf("%s %q: invalid_params = %v, want one entry named %s.1", c.field, c.in, params, c.field)
			}
		}
	})

	t.Run("archive globs", func(t *testing.T) {
		archive := func(body map[string]any) []string {
			t.Helper()
			resp, raw := e.doReq(t, http.MethodPost, "/api/v1/repos/archive", fx.token, rqGrepBody(t, body), "")
			coreRequireStatus(t, resp, raw, http.StatusOK)
			var names []string
			for name := range rqReadArchive(t, raw) {
				names = append(names, name)
			}
			return rpSorted(names...)
		}
		if got := archive(map[string]any{"include_globs_base64": []string{rpB64(rpFE)}}); !slices.Equal(got, []string{rpFE}) {
			t.Errorf("include_globs_base64 [a<0xfe>b] = %q", got)
		}
		all := rpSorted(rpFF, rpFE, rpLat1, rpUTF8, rpDirFE, rpReal, rpPlain, rpNew, rpBin)
		want := slices.DeleteFunc(slices.Clone(all), func(n string) bool { return n == rpFF })
		if got := archive(map[string]any{"exclude_globs_base64": []string{rpB64(rpFF)}}); !slices.Equal(got, want) {
			t.Errorf("exclude_globs_base64 [a<0xff>b] = %q, want %q", got, want)
		}
		resp, raw := e.doReq(t, http.MethodPost, "/api/v1/repos/archive", fx.token, rqGrepBody(t, map[string]any{"include_globs_base64": []string{"!!"}}), "")
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("a bad include_globs_base64: status = %d (%s)", resp.StatusCode, raw)
		}
	})

	t.Run("commit-pack path_base64", func(t *testing.T) {
		srv := wctServer(t, e)
		write := e.mint(t, tokenSpec{iss: fx.org, repo: fx.repoURL, scopes: []string{auth.ScopeGitWrite}})
		commit := func(files ...map[string]any) (*http.Response, []byte) {
			frames := []any{wctMeta("main", "raw paths", files, nil)}
			for _, f := range files {
				if f["operation"] == "upsert" {
					frames = append(frames, wctChunk(f["content_id"].(string), []byte("rewritten "+f["content_id"].(string)+"\n"), true))
				}
			}
			return wctPost(t, srv, "/api/v1/repos/commit-pack", write, bytes.NewReader(wctLines(t, frames...)))
		}
		upsert := func(id string, extra map[string]any) map[string]any {
			f := map[string]any{"content_id": id, "operation": "upsert", "mode": "100644"}
			for k, v := range extra {
				f[k] = v
			}
			return f
		}

		resp, raw := commit(
			upsert("ff", map[string]any{"path_base64": rpB64(rpFF)}),
			upsert("new", map[string]any{"path_base64": rpB64("added\xfd.txt")}),
			map[string]any{"path_base64": rpB64(rpFE), "content_id": "gone", "operation": "delete"},
		)
		wctRequireOK(t, resp, raw)
		read := func(name string) (int, string) {
			resp, body := coreGet(t, e, http.MethodGet, "/api/v1/repos/file?path="+rpQuery(name), fx.token, nil)
			return resp.StatusCode, string(body)
		}
		if code, body := read(rpFF); code != http.StatusOK || body != "rewritten ff\n" {
			t.Errorf("a<0xff>b = %d %q, want the rewritten content", code, body)
		}
		if code, body := read("added\xfd.txt"); code != http.StatusOK || body != "rewritten new\n" {
			t.Errorf("added<0xfd>.txt = %d %q, want the new file", code, body)
		}
		if code, _ := read(rpFE); code != http.StatusNotFound {
			t.Errorf("a<0xfe>b after its delete: status = %d, want 404", code)
		}
		if code, body := read(rpReal); code != http.StatusOK || body != fx.content[rpReal] {
			t.Errorf("the valid U+FFFD file = %d %q, want it untouched", code, body)
		}
		listed := rpObjects(t, rpJSON(t, fx.get(t, "/api/v1/repos/files")), "entries")
		if got := rpRawSet(t, "entries", listed, "path"); !slices.Contains(got, "added\xfd.txt") || slices.Contains(got, rpFE) {
			t.Errorf("entries = %q, want the added file and no a<0xfe>b", got)
		}

		// The base64 form and the plain form name one path, so both is a
		// duplicate, and path plus path_base64 on one operation is refused.
		for name, files := range map[string][]map[string]any{
			"both on one operation": {upsert("x", map[string]any{"path": "p.txt", "path_base64": rpB64("p.txt")})},
			"neither":               {upsert("x", nil)},
			"not canonical":         {upsert("x", map[string]any{"path_base64": "YR=="})},
			"empty":                 {upsert("x", map[string]any{"path_base64": ""})},
			"traversal":             {upsert("x", map[string]any{"path_base64": rpB64("a/../b")})},
			"a .git component":      {upsert("x", map[string]any{"path_base64": rpB64(".git/config")})},
			"a control byte":        {upsert("x", map[string]any{"path_base64": rpB64("a\x00b")})},
			"duplicate across forms": {
				upsert("x", map[string]any{"path": "dup.txt"}),
				upsert("y", map[string]any{"path_base64": rpB64("dup.txt")}),
			},
		} {
			tip := wctBranchTip(t, e, fx.token, "main", false)
			resp, raw := commit(files...)
			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("%s: status = %d, want 400 (%s)", name, resp.StatusCode, raw)
			}
			if got := wctBranchTip(t, e, fx.token, "main", false); got != tip || !ref.ValidSHA(got) {
				t.Errorf("%s: main moved from %s to %s", name, tip, got)
			}
		}
	})
}
