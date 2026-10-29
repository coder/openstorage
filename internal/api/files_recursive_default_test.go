package api

import (
	"bytes"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// TestFilesRecursiveDefaultsToTrue pins upstream spec 40e43e00 (#598):
// GET .../files documents `recursive` with "Defaults to true", so a call
// without it (or with an empty value) lists the whole tree, and
// recursive=false keeps listing direct children only. Both path styles.
func TestFilesRecursiveDefaultsToTrue(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := coreSeedFixture(t, e, e.newOrg(t))
		const (
			whole    = "README.md:blob a.txt:blob docs/guide.md:blob link.txt:symlink range.bin:blob script.sh:blob src/main.go:blob src/util/helper.go:blob vendor/dep:submodule"
			children = "README.md:blob a.txt:blob docs:tree link.txt:symlink range.bin:blob script.sh:blob src:tree vendor:tree"
			srcWhole = "src/main.go:blob src/util/helper.go:blob"
			srcKids  = "src/main.go:blob src/util:tree"
		)
		for _, prefix := range []string{"/api/v1/repos", "/api/repos/" + url.PathEscape(fx.repo.URL)} {
			get := func(t *testing.T, query string) (listFilesResponse, []byte) {
				t.Helper()
				resp, body := coreGet(t, e, "GET", prefix+"/files?"+query, fx.token, nil)
				coreRequireStatus(t, resp, body, http.StatusOK)
				var got listFilesResponse
				coreDecode(t, body, &got)
				return got, body
			}
			entries := func(l listFilesResponse) string {
				parts := make([]string, 0, len(l.Entries))
				for _, en := range l.Entries {
					parts = append(parts, en.Path+":"+en.Type)
				}
				return strings.Join(parts, " ")
			}
			t.Run(prefix, func(t *testing.T) {
				for _, tc := range []struct{ query, want string }{
					{"", whole},
					{"recursive=", whole},
					{"recursive=true", whole},
					{"recursive=false", children},
					{"path=src", srcWhole},
					{"path=src&recursive=false", srcKids},
				} {
					if got, _ := get(t, tc.query); entries(got) != tc.want {
						t.Errorf("files?%s entries = %q, want %q", tc.query, entries(got), tc.want)
					}
				}

				// The default is the same listing as recursive=true, byte for byte.
				_, omitted := get(t, "ref=main")
				_, explicit := get(t, "ref=main&recursive=true")
				if !bytes.Equal(omitted, explicit) {
					t.Errorf("body without recursive (%d bytes) differs from recursive=true (%d bytes)", len(omitted), len(explicit))
				}

				// Pages of a call without recursive walk the whole tree.
				var walked []string
				cursor := ""
				for page := 0; ; page++ {
					if page > 10 {
						t.Fatal("pagination never exhausted")
					}
					q := "limit=4"
					if cursor != "" {
						q += "&cursor=" + cursor
					}
					got, _ := get(t, q)
					walked = append(walked, entries(got))
					if !got.HasMore {
						break
					}
					cursor = got.NextCursor
				}
				if strings.Join(walked, " ") != whole {
					t.Errorf("paged walk without recursive = %q, want %q", strings.Join(walked, " "), whole)
				}

				resp, body := coreGet(t, e, "GET", prefix+"/files?recursive=yes", fx.token, nil)
				coreRequireStatus(t, resp, body, http.StatusBadRequest)
			})
		}
	})
}
