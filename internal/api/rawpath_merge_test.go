package api

// #587: merge conflict paths that are not valid UTF-8 carry
// conflict_paths_base64 and path_base64 siblings.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/auth"
)

// rpCommitTree commits exactly the given files, with parent as its parent
// when set, without touching HEAD or a work tree.
func rpCommitTree(t *testing.T, dir, parent string, files map[string]string, msg string) string {
	t.Helper()
	coreGit(t, dir, "read-tree", "--empty")
	for name, content := range files {
		rpStage(t, dir, name, content)
	}
	args := []string{"commit-tree", strings.TrimSpace(coreGit(t, dir, "write-tree")), "-m", msg}
	if parent != "" {
		args = append(args, "-p", parent)
	}
	return strings.TrimSpace(coreGit(t, dir, args...))
}

func rpBase64List(t *testing.T, where string, doc map[string]any, key string) []string {
	t.Helper()
	list, ok := doc[key].([]any)
	if !ok {
		t.Fatalf("%s: %q is %T in %v", where, key, doc[key], doc)
	}
	out := make([]string, len(list))
	for i, v := range list {
		raw, err := base64.StdEncoding.DecodeString(v.(string))
		if err != nil {
			t.Fatalf("%s: %s[%d] %q: %v", where, key, i, v, err)
		}
		out[i] = string(raw)
	}
	return out
}

func TestRawPathMergeConflicts(t *testing.T) { runBackends(t, testRawPathMergeConflicts) }

func testRawPathMergeConflicts(t *testing.T, e *testEnv) {
	srv := wmServer(t, e)
	org := e.newOrg(t)
	repoURL := "https://example.com/" + org + "/rawpath-merge-" + randomHex(t, 4)
	repo, err := e.store.CreateRepo(context.Background(), org, repoURL, "main")
	if err != nil {
		t.Fatalf("create repo: %v", err)
	}
	dir := t.TempDir()
	coreGit(t, dir, "init", "-q", "-b", "main", ".")
	const bigName = "big\xfa.txt"
	names := []string{rpFF, rpFE, rpPlain, bigName}
	side := func(content string) map[string]string {
		files := map[string]string{}
		for _, n := range names {
			files[n] = content
		}
		return files
	}
	base := rpCommitTree(t, dir, "", side("base\n"), "base")
	mainTip := rpCommitTree(t, dir, base, side("main\n"), "main")
	bigSide := side("feature\n")
	bigSide[bigName] = strings.Repeat("x", (1<<20)+64) + "\n" // above the inline conflict limit
	featureTip := rpCommitTree(t, dir, base, bigSide, "feature")
	plainTip := rpCommitTree(t, dir, base, map[string]string{rpFF: "base\n", rpFE: "base\n", rpPlain: "plain side\n", bigName: "base\n"}, "plain only")
	for ref, sha := range map[string]string{"main": mainTip, "feature": featureTip, "plainonly": plainTip} {
		coreGit(t, dir, "update-ref", "refs/heads/"+ref, sha)
	}
	coreSeedStores(t, e, repo.RepoID, dir, map[string]string{"refs/heads/main": mainTip, "refs/heads/feature": featureTip, "refs/heads/plainonly": plainTip})
	token := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeGitRead, auth.ScopeGitWrite}})

	wantRaw := rpSorted(rpFF, rpFE, rpPlain, bigName)
	checkPaths := func(t *testing.T, where string, doc map[string]any) {
		t.Helper()
		paths, _ := doc["conflict_paths"].([]any)
		raws := rpBase64List(t, where, doc, "conflict_paths_base64")
		if len(paths) != len(raws) || !slices.Equal(rpSorted(raws...), wantRaw) {
			t.Fatalf("%s: conflict_paths %v with conflict_paths_base64 %q, want one entry per path holding %q", where, paths, raws, wantRaw)
		}
		for i, p := range paths {
			if p != string([]rune(raws[i])) {
				t.Errorf("%s: conflict_paths[%d] = %q, want %q", where, i, p, string([]rune(raws[i])))
			}
		}
	}

	t.Run("merge 409", func(t *testing.T) {
		resp, raw := wmDo(t, srv, http.MethodPost, "/api/v1/repos/merge", token,
			wmMergeBody("feature", "main", "merge", map[string]any{"author": wmTestAuthor}))
		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("status = %d, body %s", resp.StatusCode, raw)
		}
		checkPaths(t, "merge 409", wmDecode(t, raw))
	})

	t.Run("preview without content", func(t *testing.T) {
		resp, raw := wmDo(t, srv, http.MethodGet, "/api/v1/repos/merge/preview?source_branch=feature&target_branch=main", token, nil)
		coreRequireStatus(t, resp, raw, http.StatusOK)
		checkPaths(t, "preview", wmDecode(t, raw))
	})

	t.Run("preview with content", func(t *testing.T) {
		resp, raw := wmDo(t, srv, http.MethodGet, "/api/v1/repos/merge/preview?source_branch=feature&target_branch=main&include_content=true", token, nil)
		coreRequireStatus(t, resp, raw, http.StatusOK)
		doc := wmDecode(t, raw)
		checkPaths(t, "preview", doc)
		conflicts := rpObjects(t, doc, "conflicts")
		if got := rpRawSet(t, "conflicts", conflicts, "path"); !slices.Equal(rpSorted(got...), rpSorted(rpFF, rpFE, rpPlain)) {
			t.Errorf("conflicts = %q, want %q", got, rpSorted(rpFF, rpFE, rpPlain))
		}
		for _, c := range conflicts {
			// The content comes from the raw path, so it is the main side.
			if ours := c["ours"].(map[string]any)["content"]; ours != "main\n" {
				t.Errorf("conflict %v: ours content = %q, want main", c["path"], ours)
			}
		}
		filtered := rpObjects(t, doc, "filtered_conflicts")
		if got := rpRawSet(t, "filtered_conflicts", filtered, "path"); !slices.Equal(got, []string{bigName}) {
			t.Errorf("filtered_conflicts = %q, want %q", got, []string{bigName})
		}
	})

	t.Run("valid names only add nothing", func(t *testing.T) {
		// Only plain.txt conflicts: the other three paths merge cleanly.
		for _, route := range []struct{ method, path string }{
			{http.MethodPost, "/api/v1/repos/merge"},
			{http.MethodGet, "/api/v1/repos/merge/preview?include_content=true&source_branch=" + url.QueryEscape("plainonly") + "&target_branch=main"},
		} {
			var body any
			if route.method == http.MethodPost {
				body = wmMergeBody("plainonly", "main", "merge", map[string]any{"author": wmTestAuthor})
			}
			resp, raw := wmDo(t, srv, route.method, route.path, token, body)
			if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusConflict {
				t.Fatalf("%s: status = %d, body %s", route.path, resp.StatusCode, raw)
			}
			if strings.Contains(string(raw), "_base64") {
				t.Errorf("%s: a conflict on a valid path answers a _base64 key: %s", route.path, raw)
			}
			if !strings.Contains(string(raw), `"plain.txt"`) {
				t.Errorf("%s: no conflict on plain.txt: %s", route.path, raw)
			}
		}
	})
}

// chunkRecorder is a ResponseWriter that keeps the body and the size of the
// largest single Write.
type chunkRecorder struct {
	*httptest.ResponseRecorder
	maxWrite int
}

func (c *chunkRecorder) Write(p []byte) (int, error) {
	c.maxWrite = max(c.maxWrite, len(p))
	return c.ResponseRecorder.Write(p)
}

// TestMergeConflictBodyStreams: a push can make the conflict paths as long
// and as many as it likes, so the 409 body writes them in bounded chunks and
// never holds the encoded body, or the base64 sibling, whole (#587).
func TestMergeConflictBodyStreams(t *testing.T) {
	var paths []string
	total := 0
	for i := 0; total < 4<<20; i++ {
		p := strings.Repeat("d", 200) + "/" + strings.Repeat("f", 40) + strings.Repeat("x", i%7) + string(rune('a'+i%26))
		paths = append(paths, p)
		total += len(p)
	}
	for name, withInvalid := range map[string]bool{"valid paths": false, "one invalid path": true} {
		list := slices.Clone(paths)
		if withInvalid {
			list[len(list)/2] = "inv\xffalid"
		}
		w := &chunkRecorder{ResponseRecorder: httptest.NewRecorder()}
		wmFailConflict(w, "merge has conflicts", list, strings.Repeat("a", 40))
		if w.Code != http.StatusConflict || w.Header().Get("Content-Type") != "application/json" {
			t.Fatalf("%s: %d %q", name, w.Code, w.Header().Get("Content-Type"))
		}
		if w.maxWrite > 64<<10 {
			t.Errorf("%s: one Write of %d bytes, want writes of at most 64 KiB: the body is buffered whole", name, w.maxWrite)
		}
		if !bytes.HasSuffix(w.Body.Bytes(), []byte("}\n")) {
			t.Errorf("%s: the body does not end with a newline", name)
		}
		var got struct {
			Paths  []string `json:"conflict_paths"`
			Base64 []string `json:"conflict_paths_base64"`
			Type   string   `json:"conflict_type"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(got.Paths) != len(list) || got.Type != "merge_conflict" {
			t.Fatalf("%s: %d paths of %d, type %q", name, len(got.Paths), len(list), got.Type)
		}
		if !withInvalid {
			if got.Base64 != nil || bytes.Contains(w.Body.Bytes(), []byte("_base64")) {
				t.Errorf("%s: a conflict on valid paths has conflict_paths_base64", name)
			}
			continue
		}
		if len(got.Base64) != len(list) {
			t.Fatalf("%s: %d base64 entries for %d paths", name, len(got.Base64), len(list))
		}
		for i, enc := range got.Base64 {
			if raw, err := base64.StdEncoding.DecodeString(enc); err != nil || string(raw) != list[i] {
				t.Fatalf("%s: conflict_paths_base64[%d] = %q, want the base64 of %q", name, i, enc, list[i])
			}
		}
	}
}

// TestBase64JSONStringMatchesMarshal: the streamed item writes the bytes that
// json.Marshal gives for the base64 string, for every length around the
// 32 KiB chunk and for arbitrary bytes.
func TestBase64JSONStringMatchesMarshal(t *testing.T) {
	rng := rand.New(rand.NewPCG(587, 1))
	var inputs []string
	for _, n := range []int{0, 1, 2, 3, 4, 5, 1023, 1024, 1025, 32767, 32768, 32769, 65535, 65536, 65537, 100000} {
		raw := make([]byte, n)
		for i := range raw {
			raw[i] = byte(rng.IntN(256))
		}
		inputs = append(inputs, string(raw), strings.Repeat("a", n), strings.Repeat("\xff", n))
	}
	for _, in := range inputs {
		var got bytes.Buffer
		if err := base64JSONString(in).writeJSONItem(&got); err != nil {
			t.Fatalf("len %d: %v", len(in), err)
		}
		want, err := json.Marshal(encodeRawPath(in))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got.Bytes(), want) {
			t.Fatalf("len %d: the streamed item differs from json.Marshal of the base64 string", len(in))
		}
	}
}

// discardResponse is a ResponseWriter that keeps nothing.
type discardResponse struct{ header http.Header }

func (d *discardResponse) Header() http.Header         { return d.header }
func (d *discardResponse) Write(p []byte) (int, error) { return len(p), nil }
func (d *discardResponse) WriteHeader(int)             {}

// TestMergeConflictSiblingHoldsNoCopyOfAPath: one conflict path that is far
// longer than the write chunk costs the same allocation whether or not it is
// valid UTF-8. The sibling of an invalid path adds no second string and no
// marshal buffer (#587).
func TestMergeConflictSiblingHoldsNoCopyOfAPath(t *testing.T) {
	const size = 8 << 20
	alloc := func(path string) uint64 {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		wmFailConflict(&discardResponse{header: http.Header{}}, "merge has conflicts", []string{path}, strings.Repeat("a", 40))
		runtime.ReadMemStats(&after)
		return after.TotalAlloc - before.TotalAlloc
	}
	valid := strings.Repeat("a", size)
	invalid := valid[:size/2] + "\xff" + valid[size/2+1:]
	if !strings.Contains(invalid, "\xff") || len(invalid) != size {
		t.Fatal("bad fixture")
	}
	base, withSibling := alloc(valid), alloc(invalid)
	if withSibling > base+(1<<20) {
		t.Errorf("an invalid %d MiB path allocates %d MiB against %d MiB for a valid one: the sibling holds a copy of the path", size>>20, withSibling>>20, base>>20)
	}
}

// TestMergePreviewStreams: a conflicted preview body streams its lists. Its
// bytes equal what json.Marshal gives, with or without conflict_paths_base64,
// and the sibling of an invalid path adds no allocation (#587).
func TestMergePreviewStreams(t *testing.T) {
	conflict := func(p string) wmPreviewConflict {
		return wmPreviewConflict{Path: p, PathBase64: rawPathBase64(p), Ours: wmPreviewBlob{OID: strings.Repeat("3", 40), Content: "x\n"}}
	}
	for _, paths := range [][]string{{"plain.txt", "caf\u00e9.txt"}, {"a\xffb", "plain.txt", "caf\xe9.txt"}} {
		resp := wmPreviewResponse{
			Status: "conflicted", Result: "merge_commit", SourceBranch: "s", TargetBranch: "t",
			SourceTipSHA: strings.Repeat("1", 40), TargetTipSHA: strings.Repeat("2", 40),
			ConflictPaths: paths, Conflicts: []wmPreviewConflict{conflict(paths[0]), conflict(paths[1])},
			FilteredConflicts: []wmPreviewFiltered{{Path: paths[1], PathBase64: rawPathBase64(paths[1]), Reason: "too_large"}},
		}
		eager := resp
		if anyInvalidUTF8(paths) {
			for _, p := range paths {
				eager.ConflictPathsBase64 = append(eager.ConflictPathsBase64, encodeRawPath(p))
			}
		}
		rec := httptest.NewRecorder()
		wmWritePreview(rec, resp)
		want, err := json.Marshal(eager)
		if err != nil {
			t.Fatal(err)
		}
		if got := bytes.TrimSuffix(rec.Body.Bytes(), []byte("\n")); !bytes.Equal(got, want) {
			t.Errorf("paths %q: body\n got %s\nwant %s", paths, got, want)
		}
		if has := bytes.Contains(rec.Body.Bytes(), []byte("conflict_paths_base64")); has != anyInvalidUTF8(paths) {
			t.Errorf("paths %q: conflict_paths_base64 present = %v", paths, has)
		}
	}

	// An empty conflict list (clean, fast-forward, no-op) is plain writeJSON.
	clean := wmPreviewResponse{Status: "clean", Result: "fast_forward", ConflictPaths: []string{}, Conflicts: []wmPreviewConflict{}, FilteredConflicts: []wmPreviewFiltered{}}
	rec := httptest.NewRecorder()
	wmWritePreview(rec, clean)
	if want, _ := json.Marshal(clean); !bytes.Equal(bytes.TrimSuffix(rec.Body.Bytes(), []byte("\n")), want) {
		t.Errorf("clean body = %s, want %s", rec.Body.Bytes(), want)
	}

	// Many paths with one invalid: the body is written in bounded chunks,
	// where a buffered body is one write of the whole.
	var many []string
	for total := 0; total < 4<<20; total += 250 {
		many = append(many, strings.Repeat("d", 200)+"/"+strings.Repeat("f", 40)+string(rune('a'+len(many)%26)))
	}
	many[len(many)/2] = "inv\xffalid"
	mk := func(paths []string) wmPreviewResponse {
		return wmPreviewResponse{Status: "conflicted", Result: "merge_commit", ConflictPaths: paths, Conflicts: []wmPreviewConflict{}, FilteredConflicts: []wmPreviewFiltered{}}
	}
	chunks := &chunkRecorder{ResponseRecorder: httptest.NewRecorder()}
	wmWritePreview(chunks, mk(many))
	if chunks.maxWrite > 64<<10 {
		t.Errorf("one Write of %d bytes, want writes of at most 64 KiB: the preview body is buffered whole", chunks.maxWrite)
	}
	var decoded struct {
		Paths  []string `json:"conflict_paths"`
		Base64 []string `json:"conflict_paths_base64"`
	}
	if err := json.Unmarshal(chunks.Body.Bytes(), &decoded); err != nil || len(decoded.Paths) != len(many) || len(decoded.Base64) != len(many) {
		t.Fatalf("streamed preview: %d paths, %d base64 entries for %d, %v", len(decoded.Paths), len(decoded.Base64), len(many), err)
	}

	// One path far above the write chunk: the sibling adds no copy of it.
	const size = 8 << 20
	valid := strings.Repeat("a", size)
	invalid := valid[:size/2] + "\xff" + valid[size/2+1:]
	one := func(path string) uint64 {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		wmWritePreview(&discardResponse{header: http.Header{}}, mk([]string{path}))
		runtime.ReadMemStats(&after)
		return after.TotalAlloc - before.TotalAlloc
	}
	if base, withSibling := one(valid), one(invalid); withSibling > base+(1<<20) {
		t.Errorf("an invalid %d MiB path allocates %d MiB against %d MiB for a valid one: the sibling holds a copy of the path", size>>20, withSibling>>20, base>>20)
	}
}
