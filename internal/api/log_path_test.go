package api

import (
	"bytes"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/gitread"
)

// A client chooses the URL path, and a request line can hold 1 MiB. Every
// log line that names the path must clip it: one request would otherwise
// write a line of the size of the path, with or without a token.
func TestLogLinesClipTheRequestPath(t *testing.T) {
	path := "/" + strings.Repeat("p", 100_000)
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	srv := &server{logger: logger}
	fail := errors.New("boom")

	for name, run := range map[string]func(w http.ResponseWriter, r *http.Request){
		"access log": func(w http.ResponseWriter, r *http.Request) {
			logRequests(logger)(http.NotFoundHandler()).ServeHTTP(w, r)
		},
		"panic": func(w http.ResponseWriter, r *http.Request) {
			recoverPanics(logger)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("boom") })).ServeHTTP(w, r)
		},
		"internal error": func(w http.ResponseWriter, r *http.Request) { srv.internalErr(w, r, "op", fail) },
		"git read limit": func(w http.ResponseWriter, r *http.Request) { srv.internalErr(w, r, "op", gitread.ErrOutputLimit) },
		"commit-pack":    func(w http.ResponseWriter, r *http.Request) { srv.wcInternal(w, r, "op", fail) },
		"merge":          func(w http.ResponseWriter, r *http.Request) { srv.wmInternal(w, r, "op", fail) },
		"sync": func(w http.ResponseWriter, r *http.Request) {
			(&syncServer{logger: logger}).internalErr(w, r, "op", fail)
		},
		"webhooks": func(w http.ResponseWriter, r *http.Request) {
			(&webhooksAPI{logger: logger}).internalErr(w, r, "op", fail)
		},
	} {
		t.Run(name, func(t *testing.T) {
			logs.Reset()
			run(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
			line := logs.String()
			if line == "" {
				t.Fatal("nothing was logged")
			}
			if len(line) > 8<<10 { // the panic line holds a stack trace
				t.Fatalf("the log line is %d bytes, want it bounded: %.200s", len(line), line)
			}
			if want := "...(100001 bytes)"; !strings.Contains(line, want) {
				t.Errorf("log line %.300q lacks %q", line, want)
			}
		})
	}
}
