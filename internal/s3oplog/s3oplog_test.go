package s3oplog

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// One line per round trip, seven whitespace-separated fields. Scoped work
// logs its role as role:scope, which is how TestS3WAL_PushOpCensus tells
// background round trips from a push's own.
func TestLogOpLine(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "ops")
	if err != nil {
		t.Fatal(err)
	}
	savedFile, savedRole := logFile, role
	logFile, role = f, "openstorage:serve"
	t.Cleanup(func() {
		logFile, role = savedFile, savedRole
		_ = f.Close()
	})
	req := smithyhttp.NewStackRequest().(*smithyhttp.Request)
	if req.URL, err = url.Parse("https://s3.example/bucket/_wal/meta.json"); err != nil {
		t.Fatal(err)
	}
	next := middleware.DeserializeHandlerFunc(func(context.Context, middleware.DeserializeInput) (middleware.DeserializeOutput, middleware.Metadata, error) {
		return middleware.DeserializeOutput{RawResponse: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusNotModified}}}, middleware.Metadata{}, nil
	})
	for _, tc := range []struct {
		name     string
		ctx      context.Context
		wantRole string
	}{
		{"request", context.Background(), "openstorage:serve"},
		{"scoped", WithScope(context.Background(), "keyrefresh"), "openstorage:serve:keyrefresh"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := awsmiddleware.SetOperationName(tc.ctx, "GetObject")
			if _, _, err := logOp(ctx, middleware.DeserializeInput{Request: req}, next); err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(f.Name())
			if err != nil {
				t.Fatal(err)
			}
			lines := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
			got := strings.Fields(lines[len(lines)-1])
			if len(got) != 7 {
				t.Fatalf("line %q has %d fields, want 7", lines[len(lines)-1], len(got))
			}
			want := []string{strconv.Itoa(os.Getpid()), tc.wantRole, "GetObject", "304"}
			if !slices.Equal(got[1:5], want) || got[6] != "/bucket/_wal/meta.json" {
				t.Fatalf("fields %q, want pid/role/op/status %q and path /bucket/_wal/meta.json", got, want)
			}
			if _, err := strconv.ParseInt(got[0], 10, 64); err != nil {
				t.Fatalf("timestamp %q: %v", got[0], err)
			}
			if _, err := strconv.ParseInt(got[5], 10, 64); err != nil {
				t.Fatalf("duration %q: %v", got[5], err)
			}
		})
	}
}

// A scope that is empty or holds a colon or whitespace would break the
// line's field split or the role:scope suffix, so WithScope panics.
func TestWithScopeRejectsInvalidScope(t *testing.T) {
	for _, scope := range []string{"", "a:b", "a b", "a\tb", "a\nb"} {
		t.Run(strconv.Quote(scope), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("WithScope(%q) did not panic", scope)
				}
			}()
			WithScope(context.Background(), scope)
		})
	}
}
