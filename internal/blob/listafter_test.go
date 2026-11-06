package blob

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"gocloud.dev/blob/s3blob"
)

// listAfterKeys spans two repos and a second prefix, so that "after" values
// fall before, inside, between and beyond the keys under the listed prefix.
var listAfterKeys = []string{
	"lfs/r1/o1",
	"packs/r1/pack-a.pack",
	"packs/r1/pack-b.pack",
	"packs/r1/pack-c.pack",
	"packs/r2/pack-a.pack",
	"packs/r2/pack-b.pack",
	"sweep/quarantine/n1/packs/r1/pack-a.pack",
}

var listAfterCases = []string{
	"",                          // no cursor: everything
	"a",                         // before the prefix
	"packs/",                    // the prefix itself
	"packs/r1/pack-a.pack",      // an existing key: excluded
	"packs/r1/pack-b",           // between two keys
	"packs/r1/pack-b.pack",      // an existing key mid-listing
	"packs/r2/pack-b.pack",      // the last key under the prefix
	"packs/r2/pack-b.pack\x7e",  // just beyond the last key
	"packs/r9",                  // beyond every key under the prefix
	"z",                         // beyond every key
	"packs/r1/pack-b.pack\x01x", // not a plain S3 key
}

// wantAfter is the reference: the keys under prefix that sort after after.
func wantAfter(keys []string, prefix, after string) []string {
	var want []string
	for _, k := range keys {
		if strings.HasPrefix(k, prefix) && k > after {
			want = append(want, k)
		}
	}
	sort.Strings(want)
	return want
}

// checkListPagesAfter runs list over every case and page size and compares it
// with the reference, including the page rules of ListPages.
func checkListPagesAfter(t *testing.T, list func(prefix, after string, pageSize int, fn func([]ListedObject) error) error) {
	t.Helper()
	for _, prefix := range []string{PacksRoot, ""} {
		for _, after := range listAfterCases {
			for _, pageSize := range []int{1, 2, 100} {
				var got []string
				err := list(prefix, after, pageSize, func(page []ListedObject) error {
					if len(page) == 0 || len(page) > pageSize {
						t.Errorf("prefix %q after %q page %d: got a page of %d objects", prefix, after, pageSize, len(page))
					}
					for _, o := range page {
						got = append(got, o.Key)
					}
					return nil
				})
				if err != nil {
					t.Fatalf("prefix %q after %q page %d: %v", prefix, after, pageSize, err)
				}
				if want := wantAfter(listAfterKeys, prefix, after); !reflect.DeepEqual(got, want) {
					t.Errorf("prefix %q after %q page %d:\n got  %q\n want %q", prefix, after, pageSize, got, want)
				}
			}
		}
	}
}

func seedFile(t *testing.T) *File {
	t.Helper()
	ctx := context.Background()
	b, _ := openFile(t)
	src := filepath.Join(t.TempDir(), "src")
	if err := os.WriteFile(src, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, k := range listAfterKeys {
		if err := b.UploadFile(ctx, k, src); err != nil {
			t.Fatalf("UploadFile(%s): %v", k, err)
		}
	}
	return b
}

func TestFile_ListPagesAfter(t *testing.T) {
	b := seedFile(t)
	checkListPagesAfter(t, func(prefix, after string, pageSize int, fn func([]ListedObject) error) error {
		return ListPagesAfter(context.Background(), b, prefix, after, pageSize, fn)
	})
}

// bareStore hides File's ListPagesAfter: a decorator that embeds BlobStore
// has no such method.
type bareStore struct{ BlobStore }

func TestListPagesAfter_FallbackFiltersAStoreWithoutIt(t *testing.T) {
	var store BlobStore = bareStore{seedFile(t)}
	if _, ok := store.(PageListerAfter); ok {
		t.Fatal("bareStore must not offer ListPagesAfter")
	}
	checkListPagesAfter(t, func(prefix, after string, pageSize int, fn func([]ListedObject) error) error {
		return ListPagesAfter(context.Background(), store, prefix, after, pageSize, fn)
	})
}

func TestListPagesAfter_FnErrorAbortsTheScan(t *testing.T) {
	b := seedFile(t)
	stop := fmt.Errorf("stop")
	calls := 0
	err := ListPagesAfter(context.Background(), b, PacksRoot, "packs/r1/pack-a.pack", 1, func([]ListedObject) error {
		calls++
		return stop
	})
	if err != stop || calls != 1 {
		t.Fatalf("err = %v after %d calls, want the fn error after 1", err, calls)
	}
}

// fakeS3List serves ListObjects (v1) and ListObjectsV2 over a fixed key set
// and records each list request's query.
type fakeS3List struct {
	keys []string
	// pageCap caps the keys of one provider page; gocloud asks for 1000.
	pageCap int
	// ignoreStart makes the server ignore start-after and marker, like an
	// S3-compatible service that has not implemented them.
	ignoreStart bool

	mu      sync.Mutex
	queries []url.Values
	seen    map[string]int
}

func (f *fakeS3List) requests() []url.Values {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]url.Values(nil), f.queries...)
}

func (f *fakeS3List) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f.mu.Lock()
	f.queries = append(f.queries, q)
	if f.seen == nil {
		f.seen = map[string]int{}
	}
	f.seen[q.Encode()]++
	runaway := f.seen[q.Encode()] > 40
	f.mu.Unlock()
	if runaway {
		// A listing that never advances sends the same request forever.
		http.Error(w, "the same list request, again and again", http.StatusBadRequest)
		return
	}
	v2 := q.Get("list-type") == "2"
	// A token the server issued starts with "tok:"; anything else in
	// start-after or marker is the caller's start key.
	start := ""
	switch tok := q.Get("continuation-token"); {
	case v2 && tok != "":
		start = strings.TrimPrefix(tok, "tok:")
	case v2 && !f.ignoreStart:
		start = q.Get("start-after")
	case !v2 && strings.HasPrefix(q.Get("marker"), "tok:"):
		start = strings.TrimPrefix(q.Get("marker"), "tok:")
	case !v2 && !f.ignoreStart:
		start = q.Get("marker")
	}
	max, _ := strconv.Atoi(q.Get("max-keys"))
	if max <= 0 {
		max = 1000
	}
	if f.pageCap > 0 {
		max = min(max, f.pageCap)
	}
	type content struct {
		Key          string
		LastModified string
		ETag         string
		Size         int64
		StorageClass string
	}
	var out struct {
		XMLName               xml.Name `xml:"ListBucketResult"`
		Name                  string
		Prefix                string
		KeyCount              int `xml:",omitempty"`
		MaxKeys               int
		IsTruncated           bool
		NextContinuationToken string `xml:",omitempty"`
		NextMarker            string `xml:",omitempty"`
		Contents              []content
	}
	out.Name, out.Prefix, out.MaxKeys = "b", q.Get("prefix"), max
	for _, k := range wantAfter(f.keys, q.Get("prefix"), start) {
		if len(out.Contents) == max {
			out.IsTruncated = true
			last := out.Contents[len(out.Contents)-1].Key
			if v2 {
				out.NextContinuationToken = "tok:" + last
			} else {
				out.NextMarker = "tok:" + last
			}
			break
		}
		out.Contents = append(out.Contents, content{Key: k, LastModified: time.Unix(0, 0).UTC().Format("2006-01-02T15:04:05.000Z"), ETag: `"x"`, Size: 1, StorageClass: "STANDARD"})
	}
	out.KeyCount = len(out.Contents)
	w.Header().Set("Content-Type", "application/xml")
	_, _ = w.Write([]byte(xml.Header))
	_ = xml.NewEncoder(w).Encode(out)
}

func newFakeS3Store(t *testing.T, f *fakeS3List, legacy bool) *S3 {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	client := s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(srv.URL),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("id", "secret", ""),
	})
	bucket, err := s3blob.OpenBucket(context.Background(), client, "b", &s3blob.Options{UseLegacyList: legacy})
	if err != nil {
		t.Fatalf("s3blob.OpenBucket: %v", err)
	}
	t.Cleanup(func() { _ = bucket.Close() })
	return &S3{bucket: bucket, client: client, name: "b"}
}

// listing requests only: the SDK sends one GET per provider page.
func listQueries(f *fakeS3List) []url.Values {
	var out []url.Values
	for _, q := range f.requests() {
		if q.Has("prefix") || q.Has("list-type") {
			out = append(out, q)
		}
	}
	return out
}

func TestS3_ListPagesAfter_StartsTheProviderListingAfterTheKey(t *testing.T) {
	for _, tc := range []struct {
		name   string
		legacy bool
		// the query parameter that carries the key on the first request
		param string
	}{
		{"ListObjectsV2", false, "start-after"},
		{"legacy ListObjects", true, "marker"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeS3List{keys: listAfterKeys, pageCap: 1}
			b := newFakeS3Store(t, f, tc.legacy)
			after := "packs/r1/pack-b.pack"
			var got []string
			if err := b.ListPagesAfter(context.Background(), PacksRoot, after, 1, func(page []ListedObject) error {
				for _, o := range page {
					got = append(got, o.Key)
				}
				return nil
			}); err != nil {
				t.Fatalf("ListPagesAfter: %v", err)
			}
			if want := wantAfter(listAfterKeys, PacksRoot, after); !reflect.DeepEqual(got, want) {
				t.Fatalf("keys = %q, want %q", got, want)
			}
			reqs := listQueries(f)
			if len(reqs) < 3 {
				t.Fatalf("want a first page and continuation pages, got %d list requests", len(reqs))
			}
			if reqs[0].Get(tc.param) != after {
				t.Errorf("first request %s = %q, want %q (query %v)", tc.param, reqs[0].Get(tc.param), after, reqs[0])
			}
			for i, q := range reqs[1:] {
				if tc.legacy {
					if !strings.HasPrefix(q.Get("marker"), "tok:") {
						t.Errorf("continuation request %d does not carry the server's marker: %v", i+1, q)
					}
				} else if q.Has("start-after") {
					t.Errorf("continuation request %d carries start-after: %v", i+1, q)
				}
			}
		})
	}
}

// A server that ignores start-after and marker answers from the start of the
// prefix: the client filter keeps the contract.
func TestS3_ListPagesAfter_ServerThatIgnoresTheStartKey(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		f := &fakeS3List{keys: listAfterKeys, pageCap: 2, ignoreStart: true}
		b := newFakeS3Store(t, f, legacy)
		checkListPagesAfter(t, func(prefix, after string, pageSize int, fn func([]ListedObject) error) error {
			return ListPagesAfter(context.Background(), b, prefix, after, pageSize, fn)
		})
	}
}

func TestS3_ListPagesAfter_Cases(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		b := newFakeS3Store(t, &fakeS3List{keys: listAfterKeys, pageCap: 2}, legacy)
		checkListPagesAfter(t, func(prefix, after string, pageSize int, fn func([]ListedObject) error) error {
			return ListPagesAfter(context.Background(), b, prefix, after, pageSize, fn)
		})
	}
}

// A key that gocloud would escape is not sent as StartAfter: the request must
// carry the escaped spelling, which this package does not reproduce.
func TestS3_ListPagesAfter_NeverSendsAKeyThatGocloudEscapes(t *testing.T) {
	for _, after := range []string{"packs/r1/\x01", "packs/r1/__0x2f__", "packs/../r1", "packs/r1/\u00e9"} {
		f := &fakeS3List{keys: listAfterKeys}
		b := newFakeS3Store(t, f, false)
		if err := b.ListPagesAfter(context.Background(), PacksRoot, after, 10, func([]ListedObject) error { return nil }); err != nil {
			t.Fatalf("after %q: %v", after, err)
		}
		for _, q := range listQueries(f) {
			if q.Has("start-after") {
				t.Errorf("after %q was sent as start-after: %v", after, q)
			}
		}
	}
}

func TestS3KeyIsPlain(t *testing.T) {
	for key, want := range map[string]bool{
		"packs/r1/pack-a.pack": true,
		"packs/repo_1/x":       true,
		"":                     false,
		"a\nb":                 false,
		"a__0x2f__b":           false,
		"a/../b":               false,
		"caf\u00e9":            false,
	} {
		if got := s3KeyIsPlain(key); got != want {
			t.Errorf("s3KeyIsPlain(%q) = %v, want %v", key, got, want)
		}
	}
}

// A real S3 implementation, in the same lane as the other SeaweedFS tests.
func TestS3_ListPagesAfter_OnSeaweedFS(t *testing.T) {
	if testing.Short() {
		t.Skip("seaweedfs list test needs docker")
	}
	ctx := context.Background()
	addr := startSeaweedFS(t, false)
	t.Setenv("AWS_ACCESS_KEY_ID", seaweedFSAccessKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", seaweedFSSecretKey)
	b, err := OpenS3(ctx, fmt.Sprintf("s3://list-after?endpoint=http://%s&s3ForcePathStyle=true&region=us-east-1", addr))
	if err != nil {
		t.Fatalf("OpenS3: %v", err)
	}
	defer b.Close()
	ensureBucketReady(t, b)
	src := filepath.Join(t.TempDir(), "src")
	if err := os.WriteFile(src, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, k := range listAfterKeys {
		if err := b.UploadFile(ctx, k, src); err != nil {
			t.Fatalf("UploadFile(%s): %v", k, err)
		}
	}
	checkListPagesAfter(t, func(prefix, after string, pageSize int, fn func([]ListedObject) error) error {
		return b.ListPagesAfter(ctx, prefix, after, pageSize, fn)
	})
}
