package api

// Contract integration tests for the core git:read endpoints (docs/archive/phases/07-implementation-roadmap.md p3
// phases 2a/2b): GET /repos/branches, /repos/tags, /repos/commits,
// /repos/commit, /repos/files, /repos/files/metadata, and GET+HEAD
// /repos/file. Wired through runBackends so the pg and sqlite lanes assert
// identical behavior.
//
// Seeding writes through the production store contracts the git hooks use:
// a fixture repo is built with real git in a temp dir, repacked, its pack
// uploaded via blob.PackKey/UploadFile + RegisterPacks, and its refs
// CAS-inserted into the ref authority — exactly the durable state a real
// push produces (the in-process test binary cannot host the hook re-exec).
// All test helpers carry a core* prefix to stay collision-free with sibling
// endpoint-slice test files in this package.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref"
)

// coreGitIdentity pins author/committer so fixture shas are self-consistent.
var coreGitIdentity = []string{
	"-c", "user.name=core-test",
	"-c", "user.email=core@test.invalid",
	"-c", "init.defaultBranch=main",
}

// coreGit runs git in dir with a hermetic environment (no host global or
// system config, hooks, or GIT_* identity overrides — those would leak
// developer-machine templates/trailers into the fixture history) and fails
// the test on error.
func coreGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	full := append(append([]string{}, coreGitIdentity...), args...)
	cmd := exec.Command("git", full...)
	cmd.Dir = dir
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "GIT_") {
			continue
		}
		cmd.Env = append(cmd.Env, kv)
	}
	cmd.Env = append(cmd.Env,
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_CONFIG_SYSTEM=/dev/null",
		"GIT_AUTHOR_NAME=core-test",
		"GIT_AUTHOR_EMAIL=core@test.invalid",
		"GIT_COMMITTER_NAME=core-test",
		"GIT_COMMITTER_EMAIL=core@test.invalid",
		// Distinct pinned dates so author-vs-committer date wiring is
		// assertable (files/metadata reports the AUTHOR date, commit
		// listings the COMMITTER date).
		"GIT_AUTHOR_DATE="+coreAuthorDate,
		"GIT_COMMITTER_DATE="+coreCommitterDate,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

func coreRevParse(t *testing.T, dir, rev string) string {
	t.Helper()
	return strings.TrimSpace(coreGit(t, dir, "rev-parse", rev))
}

func coreWriteFile(t *testing.T, dir, name, content string) {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// coreSeedStores publishes the work repo's current objects and the given
// refs into the lane's blob/ref stores under repoID (the durable state a
// real push through the hooks would leave behind).
func coreSeedStores(t *testing.T, e *testEnv, repoID, workDir string, refs map[string]string) {
	t.Helper()
	ctx := context.Background()
	coreGit(t, workDir, "repack", "-a", "-d", "-q")
	packFiles, err := filepath.Glob(filepath.Join(workDir, ".git", "objects", "pack", "pack-*.pack"))
	if err != nil || len(packFiles) == 0 {
		t.Fatalf("repack produced no packs (err %v)", err)
	}
	var packs []ref.Pack
	for _, pf := range packFiles {
		base := filepath.Base(pf)
		id := strings.TrimSuffix(strings.TrimPrefix(base, "pack-"), ".pack")
		key := blob.PackKey(repoID, id)
		if err := e.blobStore.UploadFile(ctx, key, pf); err != nil {
			t.Fatalf("upload pack %s: %v", key, err)
		}
		packs = append(packs, ref.Pack{ID: id, Key: key})
	}
	if err := e.refStore.RegisterPacks(ctx, repoID, packs); err != nil {
		t.Fatalf("register packs: %v", err)
	}
	for name, sha := range refs {
		won, err := e.refStore.CASRef(ctx, repoID, name, ref.ZeroSHA, sha)
		if err != nil || !won {
			t.Fatalf("cas %s -> %s: won=%v err=%v", name, sha, won, err)
		}
	}
}

// coreFixture is one fully seeded read-endpoint fixture repo.
type coreFixture struct {
	repo  meta.Repo
	token string // git:read JWT for the repo
	work  string // fixture work-tree path

	c1, c2, c3 string // main history, oldest first (c1 = root)
	c4         string // feature tip (parent c3)
	signed     string // commit with a gpgsig header, branch "signed"
	tagObj     string // annotated tag OBJECT sha (refs/tags/v1-annot target)

	rawSigned string // cat-file of c4 (== expected signed payload)
}

const (
	coreRangeContent   = "0123456789abcdefghij" // 20 bytes, /repos/file matrix
	coreFakeSignature  = "-----BEGIN PGP SIGNATURE-----\nZmFrZQ==\n-----END PGP SIGNATURE-----"
	coreReadmeV2       = "hello readme v2\n"
	coreSymlinkTarget  = "README.md"
	coreFeatureContent = "feature work\n"

	// Pinned distinct author/committer dates (see coreGit) and their
	// RFC 3339 UTC wire forms.
	coreAuthorDate       = "2024-01-02T03:04:05+00:00"
	coreCommitterDate    = "2024-02-03T04:05:06+00:00"
	coreAuthorDateUTC    = "2024-01-02T03:04:05Z"
	coreCommitterDateUTC = "2024-02-03T04:05:06Z"
)

// coreSeedFixture builds the shared multi-shape history:
//
//	c1 (root): README.md, a.txt              <- tag v0-light (lightweight)
//	c2: README.md v2, docs/guide.md, src/{main.go,util/helper.go}
//	c3: script.sh (0755), link.txt -> README.md, vendor/dep (gitlink),
//	    range.bin                             <- main, tag v1-annot (annotated)
//	c4: feature.txt, a.txt v2                 <- feature, ephemeral "scratch"
//	signed: c4 + fake gpgsig header           <- signed
func coreSeedFixture(t *testing.T, e *testEnv, org string) coreFixture {
	t.Helper()
	ctx := context.Background()

	url := "https://example.com/" + org + "/core-" + randomHex(t, 4)
	repo, err := e.store.CreateRepo(ctx, org, url, "main")
	if err != nil {
		t.Fatalf("create repo: %v", err)
	}

	dir := t.TempDir()
	coreGit(t, dir, "init", "-q", "-b", "main", ".")

	coreWriteFile(t, dir, "README.md", "hello readme v1\n")
	coreWriteFile(t, dir, "a.txt", "aaa\n")
	coreGit(t, dir, "add", ".")
	coreGit(t, dir, "commit", "-q", "-m", "c1: root")
	c1 := coreRevParse(t, dir, "HEAD")

	coreWriteFile(t, dir, "README.md", coreReadmeV2)
	coreWriteFile(t, dir, "docs/guide.md", "guide\n")
	coreWriteFile(t, dir, "src/main.go", "package main\n")
	coreWriteFile(t, dir, "src/util/helper.go", "package util\n")
	coreGit(t, dir, "add", ".")
	coreGit(t, dir, "commit", "-q", "-m", "c2: docs and src")
	c2 := coreRevParse(t, dir, "HEAD")

	coreWriteFile(t, dir, "script.sh", "#!/bin/sh\necho hi\n")
	if err := os.Chmod(filepath.Join(dir, "script.sh"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(coreSymlinkTarget, filepath.Join(dir, "link.txt")); err != nil {
		t.Fatal(err)
	}
	coreWriteFile(t, dir, "range.bin", coreRangeContent)
	coreGit(t, dir, "add", ".")
	// Gitlink (submodule entry) without .gitmodules: enough for type mapping.
	coreGit(t, dir, "update-index", "--add", "--cacheinfo", "160000,"+c1+",vendor/dep")
	coreGit(t, dir, "commit", "-q", "-m", "c3: modes, symlink, submodule")
	c3 := coreRevParse(t, dir, "HEAD")

	coreGit(t, dir, "tag", "v0-light", c1)
	coreGit(t, dir, "tag", "-a", "-m", "release", "v1-annot", c3)
	tagObj := coreRevParse(t, dir, "refs/tags/v1-annot")
	if tagObj == c3 {
		t.Fatal("annotated tag object sha unexpectedly equals commit sha")
	}

	coreGit(t, dir, "checkout", "-q", "-b", "feature")
	coreWriteFile(t, dir, "feature.txt", coreFeatureContent)
	coreWriteFile(t, dir, "a.txt", "aaa v2\n")
	coreGit(t, dir, "add", ".")
	coreGit(t, dir, "commit", "-q", "-m", "c4: feature work")
	c4 := coreRevParse(t, dir, "HEAD")

	// A commit with a (fake) gpgsig header: signature extraction only needs
	// the header shape, not a verifiable signature.
	rawSigned := coreGit(t, dir, "cat-file", "commit", c4)
	var signedRaw strings.Builder
	inserted := false
	headers, body, ok := strings.Cut(rawSigned, "\n\n")
	if !ok {
		t.Fatalf("malformed commit object %q", rawSigned)
	}
	for line := range strings.SplitSeq(headers, "\n") {
		signedRaw.WriteString(line + "\n")
		if strings.HasPrefix(line, "committer ") {
			for i, sigLine := range strings.Split(coreFakeSignature, "\n") {
				if i == 0 {
					signedRaw.WriteString("gpgsig " + sigLine + "\n")
				} else {
					signedRaw.WriteString(" " + sigLine + "\n")
				}
			}
			inserted = true
		}
	}
	if !inserted {
		t.Fatal("no committer header found")
	}
	signedRaw.WriteString("\n" + body)
	signedPath := filepath.Join(t.TempDir(), "signed-commit")
	if err := os.WriteFile(signedPath, []byte(signedRaw.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	signed := strings.TrimSpace(coreGit(t, dir, "hash-object", "-t", "commit", "-w", signedPath))
	coreGit(t, dir, "update-ref", "refs/heads/signed", signed)

	coreSeedStores(t, e, repo.RepoID, dir, map[string]string{
		"refs/heads/main":                c3,
		"refs/heads/feature":             c4,
		"refs/heads/signed":              signed,
		"refs/tags/v0-light":             c1,
		"refs/tags/v1-annot":             tagObj,
		auth.EphemeralRefName("scratch"): c4,
	})

	token := e.mint(t, tokenSpec{iss: org, repo: url, scopes: []string{auth.ScopeGitRead}})
	return coreFixture{
		repo: repo, token: token, work: dir,
		c1: c1, c2: c2, c3: c3, c4: c4, signed: signed, tagObj: tagObj,
		rawSigned: rawSigned,
	}
}

// coreFreshRepo creates a repo with an unborn default branch (no seeding).
func coreFreshRepo(t *testing.T, e *testEnv, org string) (meta.Repo, string) {
	t.Helper()
	url := "https://example.com/" + org + "/fresh-" + randomHex(t, 4)
	repo, err := e.store.CreateRepo(context.Background(), org, url, "main")
	if err != nil {
		t.Fatalf("create repo: %v", err)
	}
	return repo, e.mint(t, tokenSpec{iss: org, repo: url, scopes: []string{auth.ScopeGitRead}})
}

// coreGet performs a GET with optional extra headers against the env server.
func coreGet(t *testing.T, e *testEnv, method, path, token string, headers map[string]string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, e.srv.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := e.srv.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	body := make([]byte, 0)
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		body = append(body, buf[:n]...)
		if err != nil {
			break
		}
	}
	return resp, body
}

// coreDecode JSON-decodes body into v, failing the test on error.
func coreDecode(t *testing.T, body []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(body, v); err != nil {
		t.Fatalf("decode %q: %v", body, err)
	}
}

// coreRequireStatus asserts one status code, dumping the body on mismatch.
func coreRequireStatus(t *testing.T, resp *http.Response, body []byte, want int) {
	t.Helper()
	if resp.StatusCode != want {
		t.Fatalf("%s %s = %d, want %d (body %s)", resp.Request.Method, resp.Request.URL, resp.StatusCode, want, body)
	}
}

// ---------------------------------------------------------------------------

func TestReadCoreBranchesAndTags(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		fx := coreSeedFixture(t, e, org)

		t.Run("branches", func(t *testing.T) {
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/branches", fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			var got listBranchesResponse
			coreDecode(t, body, &got)
			if got.HasMore || got.NextCursor != "" {
				t.Errorf("has_more=%v next_cursor=%q, want exhausted", got.HasMore, got.NextCursor)
			}
			wantNames := []string{"feature", "main", "signed"} // lexical
			if len(got.Branches) != len(wantNames) {
				t.Fatalf("branches = %+v, want names %v", got.Branches, wantNames)
			}
			wantSHAs := map[string]string{"feature": fx.c4, "main": fx.c3, "signed": fx.signed}
			for i, b := range got.Branches {
				if b.Name != wantNames[i] {
					t.Errorf("branches[%d].name = %q, want %q", i, b.Name, wantNames[i])
				}
				if b.HeadSHA != wantSHAs[b.Name] {
					t.Errorf("%s head_sha = %q, want %q", b.Name, b.HeadSHA, wantSHAs[b.Name])
				}
				if b.Cursor == "" {
					t.Errorf("%s cursor empty", b.Name)
				}
				if _, err := time.Parse(time.RFC3339, b.CreatedAt); err != nil {
					t.Errorf("%s created_at %q: %v", b.Name, b.CreatedAt, err)
				}
			}
		})

		t.Run("branches pagination walk", func(t *testing.T) {
			var names []string
			cursor := ""
			for page := 0; ; page++ {
				if page > 5 {
					t.Fatal("pagination never exhausted")
				}
				u := "/api/v1/repos/branches?limit=1"
				if cursor != "" {
					u += "&cursor=" + cursor
				}
				resp, body := coreGet(t, e, "GET", u, fx.token, nil)
				coreRequireStatus(t, resp, body, 200)
				var got listBranchesResponse
				coreDecode(t, body, &got)
				if len(got.Branches) > 1 {
					t.Fatalf("limit=1 returned %d entries", len(got.Branches))
				}
				for _, b := range got.Branches {
					names = append(names, b.Name)
				}
				if !got.HasMore {
					if got.NextCursor != "" {
						t.Errorf("exhausted page has next_cursor %q", got.NextCursor)
					}
					break
				}
				if got.NextCursor == "" {
					t.Fatal("has_more with empty next_cursor")
				}
				cursor = got.NextCursor
			}
			if strings.Join(names, ",") != "feature,main,signed" {
				t.Errorf("walked names = %v", names)
			}
		})

		t.Run("branches ephemeral namespace", func(t *testing.T) {
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/branches?ephemeral=true", fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			var got listBranchesResponse
			coreDecode(t, body, &got)
			if len(got.Branches) != 1 || got.Branches[0].Name != "scratch" || got.Branches[0].HeadSHA != fx.c4 {
				t.Errorf("ephemeral branches = %+v, want scratch@%s", got.Branches, fx.c4)
			}
		})

		t.Run("branches bad params", func(t *testing.T) {
			for _, u := range []string{
				"/api/v1/repos/branches?limit=abc",
				"/api/v1/repos/branches?limit=-1",
				"/api/v1/repos/branches?limit=101",
				"/api/v1/repos/branches?ephemeral=yes",
				"/api/v1/repos/branches?cursor=%21%21not-base64",
			} {
				resp, body := coreGet(t, e, "GET", u, fx.token, nil)
				coreRequireStatus(t, resp, body, 400)
				requireEnvelope(t, resp, body)
			}
		})

		t.Run("branch lookup", func(t *testing.T) {
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/branch?name=main", fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			var got getBranchResponse
			coreDecode(t, body, &got)
			if got.Branch.Name != "main" || got.Branch.HeadSHA != fx.c3 {
				t.Errorf("branch = %+v, want main@%s", got.Branch, fx.c3)
			}
			if _, err := time.Parse(time.RFC3339, got.Branch.CreatedAt); err != nil {
				t.Errorf("created_at %q: %v", got.Branch.CreatedAt, err)
			}
			// Upstream get_branch omits the list pagination fields.
			var raw map[string]map[string]any
			coreDecode(t, body, &raw)
			if _, ok := raw["branch"]["cursor"]; ok {
				t.Errorf("branch lookup body %s carries a cursor field", body)
			}
		})

		t.Run("branch lookup ephemeral", func(t *testing.T) {
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/branch?name=scratch&ephemeral=true", fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			var got getBranchResponse
			coreDecode(t, body, &got)
			if got.Branch.Name != "scratch" || got.Branch.HeadSHA != fx.c4 {
				t.Errorf("ephemeral branch = %+v, want scratch@%s", got.Branch, fx.c4)
			}
			// The namespaces must not bleed into each other.
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/branch?name=scratch", fx.token, nil)
			coreRequireStatus(t, resp, body, 404)
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/branch?name=main&ephemeral=true", fx.token, nil)
			coreRequireStatus(t, resp, body, 404)
		})

		t.Run("branch lookup missing", func(t *testing.T) {
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/branch?name=nope", fx.token, nil)
			coreRequireStatus(t, resp, body, 404)
			requireEnvelope(t, resp, body)
		})

		t.Run("branch lookup bad params", func(t *testing.T) {
			for _, u := range []string{
				"/api/v1/repos/branch", // name is required
				"/api/v1/repos/branch?name=main&ephemeral=yes",
			} {
				resp, body := coreGet(t, e, "GET", u, fx.token, nil)
				coreRequireStatus(t, resp, body, 400)
				requireEnvelope(t, resp, body)
			}
		})

		t.Run("tags", func(t *testing.T) {
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/tags", fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			var got listTagsResponse
			coreDecode(t, body, &got)
			if len(got.Tags) != 2 || got.HasMore {
				t.Fatalf("tags = %+v has_more=%v", got.Tags, got.HasMore)
			}
			// Lexical order; the annotated tag's sha is the PEELED commit,
			// never the tag object (invariant 10).
			if got.Tags[0].Name != "v0-light" || got.Tags[0].SHA != fx.c1 {
				t.Errorf("tags[0] = %+v, want v0-light@%s", got.Tags[0], fx.c1)
			}
			if got.Tags[1].Name != "v1-annot" || got.Tags[1].SHA != fx.c3 {
				t.Errorf("tags[1] = %+v, want v1-annot@%s (peeled, tag obj %s)", got.Tags[1], fx.c3, fx.tagObj)
			}
		})

		t.Run("tag lookup", func(t *testing.T) {
			// Lightweight tag: sha is the commit itself.
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/tag?name=v0-light", fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			var got getTagResponse
			coreDecode(t, body, &got)
			if got.Tag.Name != "v0-light" || got.Tag.SHA != fx.c1 {
				t.Errorf("tag = %+v, want v0-light@%s", got.Tag, fx.c1)
			}
			// Annotated tag: sha is the PEELED commit, never the tag object
			// (invariant 10, matching the list endpoint).
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/tag?name=v1-annot", fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			coreDecode(t, body, &got)
			if got.Tag.Name != "v1-annot" || got.Tag.SHA != fx.c3 {
				t.Errorf("tag = %+v, want v1-annot@%s (peeled, tag obj %s)", got.Tag, fx.c3, fx.tagObj)
			}
		})

		t.Run("tag lookup branch-name collision", func(t *testing.T) {
			// A tag and a branch sharing one name: the tag must be
			// retrievable by its plain short name (git's %(refname:short)
			// would disambiguate it to "tags/<name>", which must NOT match).
			repo, token := coreFreshRepo(t, e, org)
			coreSeedStores(t, e, repo.RepoID, fx.work, map[string]string{
				"refs/heads/release": fx.c2,
				"refs/tags/release":  fx.c1,
			})
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/tag?name=release", token, nil)
			coreRequireStatus(t, resp, body, 200)
			var got getTagResponse
			coreDecode(t, body, &got)
			if got.Tag.Name != "release" || got.Tag.SHA != fx.c1 {
				t.Errorf("tag = %+v, want release@%s", got.Tag, fx.c1)
			}
			// The disambiguated spelling is not a tag name.
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/tag?name=tags/release", token, nil)
			coreRequireStatus(t, resp, body, 404)
			// The branch side resolves independently.
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/branch?name=release", token, nil)
			coreRequireStatus(t, resp, body, 200)
			var gotBranch getBranchResponse
			coreDecode(t, body, &gotBranch)
			if gotBranch.Branch.HeadSHA != fx.c2 {
				t.Errorf("branch = %+v, want release@%s", gotBranch.Branch, fx.c2)
			}
		})

		t.Run("tag lookup missing", func(t *testing.T) {
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/tag?name=nope", fx.token, nil)
			coreRequireStatus(t, resp, body, 404)
			requireEnvelope(t, resp, body)
		})

		t.Run("tag lookup bad params", func(t *testing.T) {
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/tag", fx.token, nil) // name is required
			coreRequireStatus(t, resp, body, 400)
			requireEnvelope(t, resp, body)
		})

		t.Run("tags pagination walk", func(t *testing.T) {
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/tags?limit=1", fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			var page1 listTagsResponse
			coreDecode(t, body, &page1)
			if len(page1.Tags) != 1 || !page1.HasMore || page1.NextCursor == "" {
				t.Fatalf("page1 = %+v", page1)
			}
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/tags?limit=1&cursor="+page1.NextCursor, fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			var page2 listTagsResponse
			coreDecode(t, body, &page2)
			if len(page2.Tags) != 1 || page2.HasMore || page2.Tags[0].Name != "v1-annot" {
				t.Fatalf("page2 = %+v", page2)
			}
			// A cursor minted by another endpoint family fails closed.
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/branches?cursor="+page1.NextCursor, fx.token, nil)
			coreRequireStatus(t, resp, body, 400)
		})

		t.Run("fresh repo lists empty", func(t *testing.T) {
			_, token := coreFreshRepo(t, e, org)
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/branches", token, nil)
			coreRequireStatus(t, resp, body, 200)
			var branches listBranchesResponse
			coreDecode(t, body, &branches)
			if len(branches.Branches) != 0 || branches.HasMore {
				t.Errorf("fresh branches = %+v", branches)
			}
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/tags", token, nil)
			coreRequireStatus(t, resp, body, 200)
			var tags listTagsResponse
			coreDecode(t, body, &tags)
			if len(tags.Tags) != 0 || tags.HasMore {
				t.Errorf("fresh tags = %+v", tags)
			}
		})

		t.Run("unknown repo 404", func(t *testing.T) {
			token := e.mint(t, tokenSpec{iss: org, repo: "https://example.com/" + org + "/nope", scopes: []string{auth.ScopeGitRead}})
			for _, u := range []string{
				"/api/v1/repos/branches", "/api/v1/repos/tags", "/api/v1/repos/commits",
				"/api/v1/repos/commit?sha=main", "/api/v1/repos/files",
				"/api/v1/repos/files/metadata", "/api/v1/repos/file?path=README.md",
			} {
				resp, body := coreGet(t, e, "GET", u, token, nil)
				coreRequireStatus(t, resp, body, 404)
			}
		})
	})
}

func TestReadCoreCommits(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		fx := coreSeedFixture(t, e, org)

		t.Run("default branch newest first", func(t *testing.T) {
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/commits", fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			var got listCommitsResponse
			coreDecode(t, body, &got)
			if got.HasMore || len(got.Commits) != 3 {
				t.Fatalf("commits = %+v", got)
			}
			wantOrder := []string{fx.c3, fx.c2, fx.c1}
			for i, c := range got.Commits {
				if c.SHA != wantOrder[i] {
					t.Errorf("commits[%d] = %s, want %s", i, c.SHA, wantOrder[i])
				}
				if c.AuthorName != "core-test" || c.AuthorEmail != "core@test.invalid" ||
					c.CommitterName != "core-test" || c.CommitterEmail != "core@test.invalid" {
					t.Errorf("commits[%d] identity = %+v", i, c)
				}
				// commitInfo.date is the COMMITTER date (invariant 9 orders
				// history by commit date).
				if c.Date != coreCommitterDateUTC {
					t.Errorf("commits[%d].date = %q, want committer date %q", i, c.Date, coreCommitterDateUTC)
				}
			}
			if got.Commits[0].Message != "c3: modes, symlink, submodule" {
				t.Errorf("message = %q", got.Commits[0].Message)
			}
			// Root commit: parent_shas [] (invariant 2), non-root: chained.
			if len(got.Commits[2].ParentSHAs) != 0 {
				t.Errorf("root parent_shas = %v", got.Commits[2].ParentSHAs)
			}
			if len(got.Commits[0].ParentSHAs) != 1 || got.Commits[0].ParentSHAs[0] != fx.c2 {
				t.Errorf("c3 parent_shas = %v", got.Commits[0].ParentSHAs)
			}
			// The wire field must be [], not null.
			if !strings.Contains(string(body), `"parent_shas":[]`) {
				t.Errorf("body lacks empty parent_shas array: %s", body)
			}
		})

		t.Run("pagination walk anchored", func(t *testing.T) {
			var shas []string
			cursor := ""
			for page := 0; ; page++ {
				if page > 5 {
					t.Fatal("pagination never exhausted")
				}
				u := "/api/v1/repos/commits?limit=1"
				if cursor != "" {
					u += "&cursor=" + cursor
				}
				resp, body := coreGet(t, e, "GET", u, fx.token, nil)
				coreRequireStatus(t, resp, body, 200)
				var got listCommitsResponse
				coreDecode(t, body, &got)
				for _, c := range got.Commits {
					shas = append(shas, c.SHA)
				}
				if !got.HasMore {
					break
				}
				cursor = got.NextCursor
			}
			want := []string{fx.c3, fx.c2, fx.c1}
			if strings.Join(shas, ",") != strings.Join(want, ",") {
				t.Errorf("walked %v, want %v", shas, want)
			}
		})

		t.Run("branch and path filters", func(t *testing.T) {
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/commits?branch=feature", fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			var got listCommitsResponse
			coreDecode(t, body, &got)
			if len(got.Commits) != 4 || got.Commits[0].SHA != fx.c4 {
				t.Fatalf("feature commits = %+v", got.Commits)
			}
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/commits?path=docs/guide.md", fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			coreDecode(t, body, &got)
			if len(got.Commits) != 1 || got.Commits[0].SHA != fx.c2 {
				t.Fatalf("path-filtered commits = %+v", got.Commits)
			}
		})

		// Issue #15: upstream deprecated `branch` in favor of `ref`.
		t.Run("ref alias deprecates branch", func(t *testing.T) {
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/commits?ref=feature", fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			var got listCommitsResponse
			coreDecode(t, body, &got)
			if len(got.Commits) != 4 || got.Commits[0].SHA != fx.c4 {
				t.Fatalf("ref=feature commits = %+v", got.Commits)
			}
			// Both styles at once: the current name wins (branch=main would
			// list 3 commits from c3).
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/commits?ref=feature&branch=main", fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			coreDecode(t, body, &got)
			if len(got.Commits) != 4 || got.Commits[0].SHA != fx.c4 {
				t.Fatalf("ref=feature&branch=main commits = %+v (want ref to win)", got.Commits)
			}
		})

		t.Run("ephemeral branch", func(t *testing.T) {
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/commits?branch=scratch&ephemeral=true", fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			var got listCommitsResponse
			coreDecode(t, body, &got)
			if len(got.Commits) != 4 || got.Commits[0].SHA != fx.c4 {
				t.Fatalf("ephemeral commits = %+v", got.Commits)
			}
			// The same name does not resolve persistently.
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/commits?branch=scratch", fx.token, nil)
			coreRequireStatus(t, resp, body, 404)
		})

		t.Run("errors", func(t *testing.T) {
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/commits?branch=missing", fx.token, nil)
			coreRequireStatus(t, resp, body, 404)

			_, freshToken := coreFreshRepo(t, e, org)
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/commits", freshToken, nil)
			coreRequireStatus(t, resp, body, 404) // unborn default branch

			resp, body = coreGet(t, e, "GET", "/api/v1/repos/commits?cursor=garbage%3D%3D", fx.token, nil)
			coreRequireStatus(t, resp, body, 400)
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/commits?limit=0", fx.token, nil)
			coreRequireStatus(t, resp, body, 200) // openapi minimum 0 -> default
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/commits?limit=101", fx.token, nil)
			coreRequireStatus(t, resp, body, 400)
		})

		t.Run("get commit resolves branch short and full sha", func(t *testing.T) {
			for _, spec := range []string{"main", fx.c3, fx.c3[:8], "v1-annot"} {
				resp, body := coreGet(t, e, "GET", "/api/v1/repos/commit?sha="+spec, fx.token, nil)
				coreRequireStatus(t, resp, body, 200)
				var got getCommitResponse
				coreDecode(t, body, &got)
				if got.Commit.SHA != fx.c3 {
					t.Errorf("sha=%q resolved %s, want %s", spec, got.Commit.SHA, fx.c3)
				}
			}
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/commit?sha=deadbeef", fx.token, nil)
			coreRequireStatus(t, resp, body, 404)
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/commit", fx.token, nil)
			coreRequireStatus(t, resp, body, 400)
		})

		// Issue #15: upstream deprecated `sha` in favor of `ref`.
		t.Run("get commit ref alias deprecates sha", func(t *testing.T) {
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/commit?ref=main", fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			var got getCommitResponse
			coreDecode(t, body, &got)
			if got.Commit.SHA != fx.c3 {
				t.Errorf("ref=main resolved %s, want %s", got.Commit.SHA, fx.c3)
			}
			// Both styles at once: the current name wins.
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/commit?ref=feature&sha=main", fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			coreDecode(t, body, &got)
			if got.Commit.SHA != fx.c4 {
				t.Errorf("ref=feature&sha=main resolved %s, want %s (ref wins)", got.Commit.SHA, fx.c4)
			}
		})

		t.Run("signed commit signature and payload", func(t *testing.T) {
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/commit?sha=signed", fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			var got getCommitResponse
			coreDecode(t, body, &got)
			if got.Commit.SHA != fx.signed {
				t.Fatalf("sha = %s, want %s", got.Commit.SHA, fx.signed)
			}
			if got.Commit.Signature != coreFakeSignature {
				t.Errorf("signature = %q, want %q", got.Commit.Signature, coreFakeSignature)
			}
			if got.Commit.Payload != fx.rawSigned {
				t.Errorf("payload = %q, want %q", got.Commit.Payload, fx.rawSigned)
			}

			// Unsigned commits omit both keys entirely.
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/commit?sha=main", fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			if strings.Contains(string(body), `"signature"`) || strings.Contains(string(body), `"payload"`) {
				t.Errorf("unsigned commit leaks signature/payload keys: %s", body)
			}
		})
	})
}

func TestReadCoreFiles(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		fx := coreSeedFixture(t, e, org)

		entryTypes := func(entries []fileEntry) string {
			parts := make([]string, 0, len(entries))
			for _, en := range entries {
				parts = append(parts, en.Path+":"+en.Type)
			}
			return strings.Join(parts, " ")
		}

		t.Run("root non-recursive", func(t *testing.T) {
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/files", fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			var got listFilesResponse
			coreDecode(t, body, &got)
			want := "README.md:blob a.txt:blob docs:tree link.txt:symlink range.bin:blob script.sh:blob src:tree vendor:tree"
			if entryTypes(got.Entries) != want {
				t.Errorf("entries = %q, want %q", entryTypes(got.Entries), want)
			}
			if strings.Join(got.Paths, " ") != "README.md a.txt range.bin script.sh" {
				t.Errorf("paths = %v (blob-only mirror)", got.Paths)
			}
			if got.Ref != "main" {
				t.Errorf("ref = %q, want main", got.Ref)
			}
			for _, en := range got.Entries {
				switch en.Path {
				case "script.sh":
					if en.Mode != "100755" {
						t.Errorf("script.sh mode = %q", en.Mode)
					}
				case "link.txt":
					if en.Mode != "120000" {
						t.Errorf("link.txt mode = %q", en.Mode)
					}
				}
			}
		})

		t.Run("recursive", func(t *testing.T) {
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/files?recursive=true", fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			var got listFilesResponse
			coreDecode(t, body, &got)
			want := "README.md:blob a.txt:blob docs/guide.md:blob link.txt:symlink range.bin:blob script.sh:blob src/main.go:blob src/util/helper.go:blob vendor/dep:submodule"
			if entryTypes(got.Entries) != want {
				t.Errorf("entries = %q, want %q", entryTypes(got.Entries), want)
			}
		})

		t.Run("subtree path", func(t *testing.T) {
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/files?path=src", fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			var got listFilesResponse
			coreDecode(t, body, &got)
			if entryTypes(got.Entries) != "src/main.go:blob src/util:tree" {
				t.Errorf("src entries = %q", entryTypes(got.Entries))
			}
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/files?path=src/&recursive=true", fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			coreDecode(t, body, &got)
			if entryTypes(got.Entries) != "src/main.go:blob src/util/helper.go:blob" {
				t.Errorf("src recursive entries = %q", entryTypes(got.Entries))
			}
			// A blob path lists the entry itself (git ls-tree semantics).
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/files?path=README.md", fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			coreDecode(t, body, &got)
			if entryTypes(got.Entries) != "README.md:blob" {
				t.Errorf("blob-path entries = %q", entryTypes(got.Entries))
			}
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/files?path=no/such/dir", fx.token, nil)
			coreRequireStatus(t, resp, body, 404)
		})

		t.Run("ref variants", func(t *testing.T) {
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/files?ref="+fx.c1, fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			var got listFilesResponse
			coreDecode(t, body, &got)
			if entryTypes(got.Entries) != "README.md:blob a.txt:blob" {
				t.Errorf("c1 entries = %q", entryTypes(got.Entries))
			}
			if got.Ref != fx.c1 {
				t.Errorf("ref echo = %q", got.Ref)
			}
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/files?ref=v0-light", fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/files?ref=missing", fx.token, nil)
			coreRequireStatus(t, resp, body, 404)
		})

		t.Run("pagination walk", func(t *testing.T) {
			var all []string
			cursor := ""
			for page := 0; ; page++ {
				if page > 10 {
					t.Fatal("pagination never exhausted")
				}
				u := "/api/v1/repos/files?recursive=true&limit=4"
				if cursor != "" {
					u += "&cursor=" + cursor
				}
				resp, body := coreGet(t, e, "GET", u, fx.token, nil)
				coreRequireStatus(t, resp, body, 200)
				var got listFilesResponse
				coreDecode(t, body, &got)
				for _, en := range got.Entries {
					all = append(all, en.Path)
				}
				if !got.HasMore {
					break
				}
				cursor = got.NextCursor
			}
			want := "README.md a.txt docs/guide.md link.txt range.bin script.sh src/main.go src/util/helper.go vendor/dep"
			if strings.Join(all, " ") != want {
				t.Errorf("walked %v", all)
			}
		})

		t.Run("bad params and unborn", func(t *testing.T) {
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/files?limit=5001", fx.token, nil)
			coreRequireStatus(t, resp, body, 400)
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/files?cursor=bogus%3D", fx.token, nil)
			coreRequireStatus(t, resp, body, 400)
			_, freshToken := coreFreshRepo(t, e, org)
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/files", freshToken, nil)
			coreRequireStatus(t, resp, body, 404)
		})

		t.Run("metadata", func(t *testing.T) {
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/files/metadata", fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			var got listFilesMetadataResponse
			coreDecode(t, body, &got)
			if len(got.Files) != 9 {
				t.Fatalf("files = %+v", got.Files)
			}
			wantLast := map[string]string{
				"README.md":          fx.c2, // touched again in c2
				"a.txt":              fx.c1, // feature's edit is not on main
				"docs/guide.md":      fx.c2,
				"link.txt":           fx.c3,
				"range.bin":          fx.c3,
				"script.sh":          fx.c3,
				"src/main.go":        fx.c2,
				"src/util/helper.go": fx.c2,
				"vendor/dep":         fx.c3,
			}
			for _, f := range got.Files {
				if f.LastCommitSHA != wantLast[f.Path] {
					t.Errorf("%s last_commit_sha = %s, want %s", f.Path, f.LastCommitSHA, wantLast[f.Path])
				}
				if f.Type == "blob" && f.Size <= 0 {
					t.Errorf("%s size = %d", f.Path, f.Size)
				}
				if f.Path == "README.md" && f.Size != int64(len(coreReadmeV2)) {
					t.Errorf("README.md size = %d, want %d", f.Size, len(coreReadmeV2))
				}
				meta, ok := got.Commits[f.LastCommitSHA]
				if !ok {
					t.Errorf("commits map missing %s", f.LastCommitSHA)
					continue
				}
				if meta.Author != "core-test" || meta.Message == "" {
					t.Errorf("commit meta = %+v", meta)
				}
				// openapi: files/metadata `date` is the AUTHOR date.
				if meta.Date != coreAuthorDateUTC {
					t.Errorf("commit date = %q, want author date %q", meta.Date, coreAuthorDateUTC)
				}
			}
			if len(got.Commits) != 3 {
				t.Errorf("commits map has %d entries, want 3 (deduplicated)", len(got.Commits))
			}
			// Linear main history: order counts back from the ref tip (c3).
			wantOrder := map[string]int{fx.c3: 0, fx.c2: 1, fx.c1: 2}
			for sha, meta := range got.Commits {
				if meta.Order != wantOrder[sha] {
					t.Errorf("commit %s order = %d, want %d", sha, meta.Order, wantOrder[sha])
				}
			}
			// The order field is on the wire even when it is 0.
			var raw struct {
				Commits map[string]map[string]json.RawMessage `json:"commits"`
			}
			coreDecode(t, body, &raw)
			if o, ok := raw.Commits[fx.c3]["order"]; !ok || string(o) != "0" {
				t.Errorf("tip commit raw order = %q (present %v), want 0", o, ok)
			}
		})

		t.Run("metadata always recursive and paginated", func(t *testing.T) {
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/files/metadata?recursive=false&limit=4", fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			var got listFilesMetadataResponse
			coreDecode(t, body, &got)
			if len(got.Files) != 4 || !got.HasMore || got.NextCursor == "" {
				t.Fatalf("page1 = %+v", got)
			}
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/files/metadata?limit=1001", fx.token, nil)
			coreRequireStatus(t, resp, body, 400)

			_, freshToken := coreFreshRepo(t, e, org)
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/files/metadata", freshToken, nil)
			coreRequireStatus(t, resp, body, 404) // invariant 3: unborn -> 404
		})
	})
}

// TestReadCoreFilesMetadataOrder covers the commit `order` rank (issue
// #292): the commit's absolute position in the resolved ref's topological
// history (not a rank within the page), independent of author dates,
// stable across pages, and identical on the named-repo alias.
//
//	base (a.txt, shared/one.txt, shared/two.txt) -- m1 (main.txt) ------ M (merge.txt, main)
//	    \-- s1 (side1.txt) -- s2 (side2.txt) (side) ---------------------/
//
// Author dates disagree with topology: s1 2030 > s2 2029 > base 2024 > m1 2001.
func TestReadCoreFilesMetadataOrder(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		ctx := context.Background()
		org := e.newOrg(t)
		repoURL := "https://example.com/" + org + "/order-" + randomHex(t, 4)
		repo, err := e.store.CreateRepo(ctx, org, repoURL, "main")
		if err != nil {
			t.Fatalf("create repo: %v", err)
		}
		dir := t.TempDir()
		commit := func(msg, authorDate string, files ...string) string {
			for _, f := range files {
				coreWriteFile(t, dir, f, f+"\n")
			}
			coreGit(t, dir, "add", ".")
			coreGit(t, dir, "commit", "-q", "--date="+authorDate, "-m", msg)
			return coreRevParse(t, dir, "HEAD")
		}
		coreGit(t, dir, "init", "-q", "-b", "main", ".")
		base := commit("base", coreAuthorDate, "a.txt", "shared/one.txt", "shared/two.txt")
		coreGit(t, dir, "checkout", "-q", "-b", "side")
		s1 := commit("s1", "2030-01-01T00:00:00+00:00", "side1.txt")
		s2 := commit("s2", "2029-01-01T00:00:00+00:00", "side2.txt")
		coreGit(t, dir, "checkout", "-q", "main")
		m1 := commit("m1", "2001-01-01T00:00:00+00:00", "main.txt")
		coreGit(t, dir, "merge", "-q", "--no-ff", "--no-commit", "side")
		merge := commit("M", coreAuthorDate, "merge.txt")

		// An empty commit on top of M: it is the ref's commit but no
		// listed file's last commit, so no entry may carry order 0.
		coreGit(t, dir, "commit", "-q", "--allow-empty", "-m", "empty tip")
		emptyTip := coreRevParse(t, dir, "HEAD")
		coreGit(t, dir, "reset", "-q", "--hard", merge)

		coreSeedStores(t, e, repo.RepoID, dir, map[string]string{
			"refs/heads/main":      merge,
			"refs/heads/side":      s2,
			"refs/heads/empty-tip": emptyTip,
		})
		token := e.mint(t, tokenSpec{iss: org, repo: repoURL, scopes: []string{auth.ScopeGitRead}})

		wantLast := map[string]string{
			"a.txt": base, "shared/one.txt": base, "shared/two.txt": base,
			"main.txt": m1, "side1.txt": s1, "side2.txt": s2, "merge.txt": merge,
		}
		// git topo-order from M: M, then the merge's last parent's line
		// (s2, s1), then the first parent's line (m1), then base.
		wantOrder := map[string]int{merge: 0, s2: 1, s1: 2, m1: 3, base: 4}

		// fetchAll walks every page and checks that each commit keeps one
		// order value across pages.
		fetchAll := func(t *testing.T, path string) (map[string]string, map[string]int) {
			t.Helper()
			last, order := map[string]string{}, map[string]int{}
			cursor := ""
			for range 20 {
				u := path
				if cursor != "" {
					u += "&cursor=" + cursor
				}
				resp, body := coreGet(t, e, "GET", u, token, nil)
				coreRequireStatus(t, resp, body, 200)
				var got listFilesMetadataResponse
				coreDecode(t, body, &got)
				for _, f := range got.Files {
					last[f.Path] = f.LastCommitSHA
					if _, ok := got.Commits[f.LastCommitSHA]; !ok {
						t.Fatalf("commits map missing %s", f.LastCommitSHA)
					}
				}
				for sha, meta := range got.Commits {
					if prev, seen := order[sha]; seen && prev != meta.Order {
						t.Errorf("commit %s order %d on one page, %d on another", sha, prev, meta.Order)
					}
					order[sha] = meta.Order
				}
				if !got.HasMore {
					return last, order
				}
				cursor = got.NextCursor
			}
			t.Fatal("pagination did not terminate")
			return nil, nil
		}

		for name, prefix := range map[string]string{
			"v1":    "/api/v1/repos/files/metadata?",
			"alias": "/api/repos/" + url.PathEscape(repoURL) + "/files/metadata?",
		} {
			t.Run(name, func(t *testing.T) {
				for _, limit := range []string{"limit=1000", "limit=2"} {
					last, order := fetchAll(t, prefix+limit)
					if fmt.Sprint(last) != fmt.Sprint(wantLast) {
						t.Errorf("%s: last commits = %v, want %v", limit, last, wantLast)
					}
					if fmt.Sprint(order) != fmt.Sprint(wantOrder) {
						t.Errorf("%s: order = %v, want %v", limit, order, wantOrder)
					}
				}
			})
		}

		t.Run("relative to the resolved ref", func(t *testing.T) {
			_, order := fetchAll(t, "/api/v1/repos/files/metadata?ref=side&limit=1000")
			want := map[string]int{s2: 0, s1: 1, base: 2}
			if fmt.Sprint(order) != fmt.Sprint(want) {
				t.Errorf("order at side = %v, want %v", order, want)
			}
		})

		t.Run("absolute rank: 0 absent when the tip is not listed", func(t *testing.T) {
			_, order := fetchAll(t, "/api/v1/repos/files/metadata?ref=empty-tip&limit=1000")
			want := map[string]int{merge: 1, s2: 2, s1: 3, m1: 4, base: 5}
			if fmt.Sprint(order) != fmt.Sprint(want) {
				t.Errorf("order at empty-tip = %v, want %v", order, want)
			}
		})
	})
}

func TestReadCoreFileStreaming(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		fx := coreSeedFixture(t, e, org)
		blobSHA := coreRevParse(t, fx.work, fx.c3+":range.bin")
		etag := `"` + blobSHA + `"`
		fileURL := "/api/v1/repos/file?path=range.bin"

		t.Run("plain 200 with headers", func(t *testing.T) {
			resp, body := coreGet(t, e, "GET", fileURL, fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			if string(body) != coreRangeContent {
				t.Errorf("body = %q", body)
			}
			h := resp.Header
			if h.Get("X-Blob-Sha") != blobSHA {
				t.Errorf("X-Blob-Sha = %q, want %q", h.Get("X-Blob-Sha"), blobSHA)
			}
			if h.Get("X-Last-Commit-Sha") != fx.c3 {
				t.Errorf("X-Last-Commit-Sha = %q, want %q", h.Get("X-Last-Commit-Sha"), fx.c3)
			}
			if h.Get("ETag") != etag {
				t.Errorf("ETag = %q, want %q", h.Get("ETag"), etag)
			}
			if h.Get("Accept-Ranges") != "bytes" {
				t.Errorf("Accept-Ranges = %q", h.Get("Accept-Ranges"))
			}
			if h.Get("Content-Length") != fmt.Sprint(len(coreRangeContent)) {
				t.Errorf("Content-Length = %q", h.Get("Content-Length"))
			}
			// openapi: "Always `application/octet-stream`" — never sniffed.
			if ct := h.Get("Content-Type"); ct != "application/octet-stream" {
				t.Errorf("Content-Type = %q, want application/octet-stream", ct)
			}
			if _, err := time.Parse(http.TimeFormat, h.Get("Last-Modified")); err != nil {
				t.Errorf("Last-Modified %q: %v", h.Get("Last-Modified"), err)
			}
		})

		t.Run("range 206", func(t *testing.T) {
			resp, body := coreGet(t, e, "GET", fileURL, fx.token, map[string]string{"Range": "bytes=5-9"})
			coreRequireStatus(t, resp, body, 206)
			if string(body) != "56789" {
				t.Errorf("body = %q", body)
			}
			if cr := resp.Header.Get("Content-Range"); cr != "bytes 5-9/20" {
				t.Errorf("Content-Range = %q", cr)
			}
			if ct := resp.Header.Get("Content-Type"); ct != "application/octet-stream" {
				t.Errorf("206 Content-Type = %q, want application/octet-stream", ct)
			}
		})

		t.Run("unsatisfiable range 416", func(t *testing.T) {
			resp, body := coreGet(t, e, "GET", fileURL, fx.token, map[string]string{"Range": "bytes=99-"})
			coreRequireStatus(t, resp, body, 416)
			if cr := resp.Header.Get("Content-Range"); cr != "bytes */20" {
				t.Errorf("Content-Range = %q", cr)
			}
		})

		t.Run("conditional matrix", func(t *testing.T) {
			// If-None-Match current -> 304, stale -> 200.
			resp, body := coreGet(t, e, "GET", fileURL, fx.token, map[string]string{"If-None-Match": etag})
			coreRequireStatus(t, resp, body, 304)
			if len(body) != 0 {
				t.Errorf("304 body = %q", body)
			}
			resp, body = coreGet(t, e, "GET", fileURL, fx.token, map[string]string{"If-None-Match": `"stale"`})
			coreRequireStatus(t, resp, body, 200)

			// If-Match mismatch -> 412, match -> 200.
			resp, body = coreGet(t, e, "GET", fileURL, fx.token, map[string]string{"If-Match": `"stale"`})
			coreRequireStatus(t, resp, body, 412)
			resp, body = coreGet(t, e, "GET", fileURL, fx.token, map[string]string{"If-Match": etag})
			coreRequireStatus(t, resp, body, 200)

			// If-Range: matching validator -> 206 partial, stale -> 200 full.
			resp, body = coreGet(t, e, "GET", fileURL, fx.token, map[string]string{"Range": "bytes=0-4", "If-Range": etag})
			coreRequireStatus(t, resp, body, 206)
			if string(body) != "01234" {
				t.Errorf("If-Range body = %q", body)
			}
			resp, body = coreGet(t, e, "GET", fileURL, fx.token, map[string]string{"Range": "bytes=0-4", "If-Range": `"stale"`})
			coreRequireStatus(t, resp, body, 200)
			if string(body) != coreRangeContent {
				t.Errorf("stale If-Range body = %q", body)
			}

			// Last-Modified arm: replaying the served value -> 304; a date
			// before the commit -> 412 on If-Unmodified-Since.
			resp, _ = coreGet(t, e, "GET", fileURL, fx.token, nil)
			lastMod := resp.Header.Get("Last-Modified")
			resp, body = coreGet(t, e, "GET", fileURL, fx.token, map[string]string{"If-Modified-Since": lastMod})
			coreRequireStatus(t, resp, body, 304)
			resp, body = coreGet(t, e, "GET", fileURL, fx.token, map[string]string{"If-Unmodified-Since": "Mon, 01 Jan 1990 00:00:00 GMT"})
			coreRequireStatus(t, resp, body, 412)
		})

		t.Run("HEAD parity", func(t *testing.T) {
			resp, body := coreGet(t, e, "HEAD", fileURL, fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			if len(body) != 0 {
				t.Errorf("HEAD body = %q", body)
			}
			h := resp.Header
			if h.Get("X-Blob-Sha") != blobSHA || h.Get("ETag") != etag ||
				h.Get("Content-Length") != fmt.Sprint(len(coreRangeContent)) {
				t.Errorf("HEAD headers: X-Blob-Sha=%q ETag=%q Content-Length=%q",
					h.Get("X-Blob-Sha"), h.Get("ETag"), h.Get("Content-Length"))
			}
			resp, body = coreGet(t, e, "HEAD", fileURL, fx.token, map[string]string{"If-None-Match": etag})
			coreRequireStatus(t, resp, body, 304)
		})

		t.Run("ref and symlink reads", func(t *testing.T) {
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/file?path=feature.txt&ref=feature", fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			if string(body) != coreFeatureContent {
				t.Errorf("feature.txt = %q", body)
			}
			// Extension-based detection stays off: .txt is still served as
			// application/octet-stream ("Always", per openapi).
			if ct := resp.Header.Get("Content-Type"); ct != "application/octet-stream" {
				t.Errorf(".txt Content-Type = %q, want application/octet-stream", ct)
			}
			// Symlink blobs stream the link target bytes.
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/file?path=link.txt", fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			if string(body) != coreSymlinkTarget {
				t.Errorf("link.txt = %q", body)
			}
			// Historic ref reads the old blob.
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/file?path=README.md&ref="+fx.c1, fx.token, nil)
			coreRequireStatus(t, resp, body, 200)
			if string(body) != "hello readme v1\n" {
				t.Errorf("README.md@c1 = %q", body)
			}
			if h := resp.Header.Get("X-Last-Commit-Sha"); h != fx.c1 {
				t.Errorf("X-Last-Commit-Sha = %q, want %q", h, fx.c1)
			}
		})

		t.Run("errors", func(t *testing.T) {
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/file", fx.token, nil)
			coreRequireStatus(t, resp, body, 400) // missing path
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/file?path=no.such", fx.token, nil)
			coreRequireStatus(t, resp, body, 404)
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/file?path=docs", fx.token, nil)
			coreRequireStatus(t, resp, body, 404) // trees are not files
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/file?path=README.md&ref=missing", fx.token, nil)
			coreRequireStatus(t, resp, body, 404)

			_, freshToken := coreFreshRepo(t, e, org)
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/file?path=README.md", freshToken, nil)
			coreRequireStatus(t, resp, body, 404) // unborn branch (invariant 3)
		})
	})
}
