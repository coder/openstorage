package githttp

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// lfsCountedBody counts the bytes that its reader hands out. The handler
// reads it on the test's own goroutine, so the counter needs no lock.
type lfsCountedBody struct {
	r io.Reader
	n int64
}

func (c *lfsCountedBody) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func gzipOf(t *testing.T, chunks ...[]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range chunks {
		if _, err := zw.Write(c); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// batchGzip posts a gzip batch body without credentials: the body is decoded
// before the request is authenticated, so a body that decodes ends in 401.
func batchGzip(t *testing.T, wire io.Reader) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/repo.git/info/lfs/objects/batch", wire)
	req.Header.Set("Content-Encoding", "gzip")
	rec := httptest.NewRecorder()
	(&Server{}).handleLFSBatch(rec, req, "repo")
	return rec
}

// TestLFSBatchGzipDecodedCap: the 4 MiB bound of a batch body counts the
// decoded bytes of a gzip body as well as its compressed bytes. A body that
// inflates past it is refused after the server has read a small part of it,
// and a body at the bound still decodes.
func TestLFSBatchGzipDecodedCap(t *testing.T) {
	const prefix, suffix = `{"operation":"download","pad":"`, `"}`
	pad := bytes.Repeat([]byte("a"), 1<<20)

	t.Run("a body that inflates past the cap is refused early", func(t *testing.T) {
		chunks := [][]byte{[]byte(prefix)}
		for range 64 {
			chunks = append(chunks, pad)
		}
		compressed := gzipOf(t, append(chunks, []byte(suffix))...)
		if len(compressed) >= lfsMaxBatchBody {
			t.Fatalf("the test body is %d bytes compressed: it must pass the compressed cap", len(compressed))
		}
		wire := &lfsCountedBody{r: bytes.NewReader(compressed)}
		rec := batchGzip(t, wire)
		var got struct {
			Message string `json:"message"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil || rec.Code != http.StatusBadRequest ||
			got.Message != "bad batch request: request body too large" {
			t.Fatalf("answer %d %s, want 400 and the too-large message", rec.Code, rec.Body)
		}
		// The decoded stream is many times the cap, so the server stops
		// early in the compressed stream and never reads its end.
		if wire.n > int64(len(compressed))/4 {
			t.Fatalf("the server read %d of %d compressed bytes before it refused the body", wire.n, len(compressed))
		}
	})

	// A decoded body of exactly n bytes: the pad grows the JSON string.
	body := func(n int) []byte {
		fill := n - len(prefix) - len(suffix)
		return append(append([]byte(prefix), bytes.Repeat([]byte("a"), fill)...), suffix...)
	}
	for _, tc := range []struct {
		name string
		n    int
		want int
	}{
		{"a small body decodes", 100, http.StatusUnauthorized},
		{"a body at the cap decodes", lfsMaxBatchBody, http.StatusUnauthorized},
		{"a body one byte over the cap is refused", lfsMaxBatchBody + 1, http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := batchGzip(t, bytes.NewReader(gzipOf(t, body(tc.n))))
			if rec.Code != tc.want {
				t.Fatalf("status %d, want %d (body %.200s)", rec.Code, tc.want, rec.Body)
			}
			if tc.want == http.StatusBadRequest && !strings.Contains(rec.Body.String(), "request body too large") {
				t.Fatalf("answer %s", rec.Body)
			}
		})
	}
}
