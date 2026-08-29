// Fuzz targets for package githttp's untrusted wire surfaces (issue #91):
// sniffAtomicPush parses raw receive-pack request bytes before publication,
// and handleLFSBatch decodes client-supplied JSON before authentication
// (the requested operation selects the required scope). Both must degrade
// gracefully on arbitrary input.
package githttp

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// TestFuzzSkipEnvBoot pins the TestMain early-out contract (Codex review):
// workers always skip the e2e boot, a coordinator skips only under the
// match-nothing `-run '^$'` idiom, and every other invocation — including a
// coordinator that will run ordinary tests before fuzzing — keeps the full
// boot.
func TestFuzzSkipEnvBoot(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want bool
	}{
		{"plain go test", nil, false},
		{"run filter without fuzzing", []string{"-test.run=^$"}, false},
		{"worker", []string{"-test.fuzzworker", "-test.fuzz=^FuzzX$"}, true},
		{"worker bool spelling", []string{"--test.fuzzworker=true"}, true},
		{"coordinator, match-nothing run", []string{"-test.run=^$", "-test.fuzz=^FuzzX$"}, true},
		{"coordinator, space-separated run", []string{"-test.run", "^$", "-test.fuzz=^FuzzX$"}, true},
		{"coordinator without run", []string{"-test.fuzz=^FuzzX$"}, false},
		{"coordinator with real run pattern", []string{"-test.run=TestPush", "-test.fuzz=^FuzzX$"}, false},
		{"coordinator with empty run pattern", []string{"-test.run=", "-test.fuzz=^FuzzX$"}, false},
	}
	for _, tc := range cases {
		if got := fuzzSkipEnvBoot(tc.args); got != tc.want {
			t.Errorf("%s: fuzzSkipEnvBoot(%q) = %v, want %v", tc.name, tc.args, got, tc.want)
		}
	}
}

// fuzzPkt frames payload as one pkt-line.
func fuzzPkt(payload string) string {
	return fmt.Sprintf("%04x", len(payload)+4) + payload
}

// refSniffAtomic is a second, independent implementation of the
// update-requests capability scan (defensive double-algorithm oracle),
// evaluated over the whole input slice instead of a stream. It mirrors the
// documented grammar — pkt-line framing, shallow prefix skip, first-command
// NUL capabilities, C-string cap-list termination, and the fail-closed peek
// budget — without sharing sniffAtomicPush's streaming code.
func refSniffAtomic(in []byte) bool {
	pos := 0
	for pos < sniffPeekBudget {
		if pos+4 > len(in) {
			return false
		}
		n, err := strconv.ParseUint(string(in[pos:pos+4]), 16, 32)
		if err != nil || n > maxPktLen {
			return false
		}
		if n < 4 {
			return false
		}
		end := pos + int(n)
		if end > len(in) {
			return false
		}
		payload := in[pos+4 : end]
		pos = end
		if bytes.HasPrefix(payload, []byte("shallow ")) {
			continue
		}
		_, after, ok := bytes.Cut(payload, []byte{0})
		if !ok {
			return false
		}
		caps := after
		if next := bytes.IndexByte(caps, 0); next >= 0 {
			caps = caps[:next]
		}
		return slices.Contains(strings.Fields(string(caps)), "atomic")
	}
	return true // budget exhausted inside the shallow prefix: fail closed
}

func FuzzSniffAtomicPush(f *testing.F) {
	const oid1 = "1111111111111111111111111111111111111111"
	const oid2 = "2222222222222222222222222222222222222222"
	cmd := oid1 + " " + oid2 + " refs/heads/main"
	for _, seed := range [][]byte{
		nil,
		[]byte("0000"),
		[]byte(fuzzPkt(cmd+"\x00atomic report-status") + "0000PACK"),
		[]byte(fuzzPkt(cmd+"\x00report-status quiet") + "0000"),
		[]byte(fuzzPkt(cmd)), // no NUL: no capabilities
		[]byte(fuzzPkt("shallow "+oid1) + fuzzPkt(cmd+"\x00atomic")),
		[]byte(fuzzPkt(cmd + "\x00atomic\x00junk")), // C-string caps: atomic
		[]byte(fuzzPkt(cmd + "\x00junk\x00atomic")), // C-string caps: NOT atomic
		[]byte(fuzzPkt("push-cert\x00atomic")),
		[]byte("0001"),
		[]byte("0002"),
		[]byte("0003"),
		[]byte("0004"),
		[]byte("ffff" + strings.Repeat("x", 64)),
		[]byte("00zz"),
		[]byte("00"),      // truncated length header
		[]byte("0010abc"), // truncated payload
		// Budget exhaustion: >1 MiB of shallow lines must fail closed.
		bytes.Repeat([]byte(fuzzPkt("shallow "+oid1)), sniffPeekBudget/len(fuzzPkt("shallow "+oid1))+2),
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		atomic, replay := sniffAtomicPush(bytes.NewReader(data))
		if want := refSniffAtomic(data); atomic != want {
			t.Fatalf("sniffAtomicPush = %v, reference oracle = %v", atomic, want)
		}
		// The replay reader hands the request to git afterwards: it must
		// reproduce the input byte-for-byte, whatever the sniff decided.
		got, err := io.ReadAll(replay)
		if err != nil {
			t.Fatalf("replay reader failed: %v", err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("replay returned %d bytes, want the original %d", len(got), len(data))
		}
	})
}

func FuzzLFSBatch(f *testing.F) {
	valid := `{"operation":"download","transfers":["basic"],"hash_algo":"sha256",` +
		`"objects":[{"oid":"` + strings.Repeat("a", 64) + `","size":123}]}`
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	if _, err := zw.Write([]byte(valid)); err != nil {
		f.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		f.Fatal(err)
	}
	for _, seed := range []struct {
		body    []byte
		gzipped bool
	}{
		{[]byte(valid), false},
		{gz.Bytes(), true},
		{[]byte(valid), true}, // Content-Encoding lies: not gzip
		{[]byte(`{"operation":"upload","objects":[]}`), false},
		{[]byte(`{"operation":"steal"}`), false},
		{[]byte(`{"operation":"download","transfers":["custom"],"hash_algo":"sha512"}`), false},
		{[]byte(`{"operation":"download","objects":[{"oid":"XYZ","size":-1}]}`), false},
		{[]byte(`{`), false},
		{nil, false},
	} {
		f.Add(seed.body, seed.gzipped)
	}
	f.Fuzz(func(t *testing.T, body []byte, gzipped bool) {
		// A zero Server suffices: the request carries no credentials, so
		// the handler must fail before touching any server dependency —
		// the property under test is that the pre-auth body decode
		// degrades to 400/401, never a panic or a success.
		srv := &Server{}
		req := httptest.NewRequest(http.MethodPost, "/repo.git/info/lfs/objects/batch", bytes.NewReader(body))
		if gzipped {
			req.Header.Set("Content-Encoding", "gzip")
		}
		rec := httptest.NewRecorder()
		srv.handleLFSBatch(rec, req, "repo")
		switch rec.Code {
		case http.StatusBadRequest:
		case http.StatusUnauthorized:
			if rec.Header().Get("WWW-Authenticate") == "" {
				t.Fatal("401 without a WWW-Authenticate challenge")
			}
		default:
			t.Fatalf("handleLFSBatch = %d for an unauthenticated request, want 400 or 401", rec.Code)
		}
	})
}
