package api

import (
	"bytes"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/githttp"
	"github.com/coder/openstorage/internal/gitread"
)

// A request line can be up to 1 MiB (Go's default MaxHeaderBytes), and
// logRequests logs every request, unauthenticated ones too. Each log
// record keeps at most 1024 bytes of the request's path and method.
func TestLogRecordsBoundTheRequestLine(t *testing.T) {
	longPath := "/api/v1/" + strings.Repeat("p", 700<<10)
	longMethod := strings.Repeat("M", 200<<10)
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	s := &server{logger: logger}
	panics := recoverPanics(logger)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("boom") }))
	handlers := map[string]http.Handler{
		"http request": logRequests(logger)(http.NotFoundHandler()),
		"internal error": http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			s.internalErr(w, r, "op", errors.New("x"))
		}),
		"panic in handler": panics,
		"wcInternal": http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			s.wcInternal(w, r, "op", errors.New("x"))
		}),
		"wmInternal": http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			s.wmInternal(w, r, "op", errors.New("x"))
		}),
		"ref held": http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			s.writeRefHeldErr(w, r, "op", githttp.ErrRefHeld)
		}),
		"git read limit": http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			s.writeGitLimitErr(w, r, "op", gitread.ErrBusy)
		}),
		"webhooks internal error": http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			(&webhooksAPI{logger: logger}).internalErr(w, r, "op", errors.New("x"))
		}),
		"sync internal error": http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			(&syncServer{logger: logger}).internalErr(w, r, "op", errors.New("x"))
		}),
	}
	for name, h := range handlers {
		for _, req := range []*http.Request{
			httptest.NewRequest(http.MethodGet, longPath, nil),
			httptest.NewRequest(longMethod, "/api/v1/repos", nil),
		} {
			buf.Reset()
			h.ServeHTTP(httptest.NewRecorder(), req)
			// The panic record carries a stack trace: allow for it.
			if n := buf.Len(); n > 16<<10 {
				t.Errorf("%s (%d-byte path, %d-byte method): the log record is %d bytes", name, len(req.URL.Path), len(req.Method), n)
			}
			if !strings.Contains(buf.String(), " bytes)") {
				t.Errorf("%s: the log record does not give the cut value's size", name)
			}
		}
	}
	buf.Reset()
	logRequests(logger)(http.NotFoundHandler()).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/v1/repos", nil))
	if !strings.Contains(buf.String(), "method=GET path=/api/v1/repos ") {
		t.Errorf("a short request line is logged as is: %s", buf.String())
	}
}
