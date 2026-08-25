package sync

// Outbound push egress guard (Codex security round on #90): upstream_host
// is caller-linked data, so without a guard the forward is a server-side
// request forgery path — a repo:write tenant could point a repository at a
// loopback/RFC1918 git service and have the server publish attacker refs
// there with force. git does its own dialing, so the dial-time policy the
// webhook TargetGuard pioneered (issue #53) cannot be applied in-process;
// instead every http(s) push runs through a short-lived loopback forward
// proxy whose dials go through TargetGuard.DialContext. The operator
// escape hatch (CODESTORAGE_SYNC_PUSH_ALLOW_PRIVATE) relaxes only the
// private receiver classes, exactly like the webhook hatch: metadata,
// broadcast, and the special-purpose registry stay denied.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"strings"
	stdsync "sync"
	"time"

	"github.com/coder/code-storage/internal/events"
)

// egressProxy fronts exactly one outbound `git push`. Plain-http upstreams
// arrive as absolute-form requests and are round-tripped with the guarded
// dialer; https upstreams arrive as CONNECT and get a guarded tunnel (TLS
// stays end-to-end). Denials are recorded so the worker can classify the
// failed push as configuration, not weather.
type egressProxy struct {
	ln    net.Listener
	srv   *http.Server
	guard events.TargetGuard

	mu     stdsync.Mutex
	denial string
}

func startEgressProxy(allowPrivate bool) (*egressProxy, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("egress proxy listen: %w", err)
	}
	p := &egressProxy{ln: ln, guard: events.TargetGuard{AllowPrivate: allowPrivate}}
	p.srv = &http.Server{Handler: p, ReadHeaderTimeout: 30 * time.Second}
	go func() { _ = p.srv.Serve(ln) }()
	return p, nil
}

// URL is the proxy endpoint for the git subprocess's proxy environment.
func (p *egressProxy) URL() string { return "http://" + p.ln.Addr().String() }

// Close tears the proxy down; hijacked CONNECT tunnels die with their
// client (the git subprocess), which is already gone when the worker
// closes the proxy.
func (p *egressProxy) Close() { _ = p.srv.Close() }

// deniedReason returns the recorded policy denial, if any egress was
// refused while the proxy served.
func (p *egressProxy) deniedReason() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.denial
}

func (p *egressProxy) recordDenial(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.denial == "" {
		p.denial = err.Error()
	}
}

// vetted wraps the guarded dial, recording policy denials (and only
// policy denials: an unreachable-but-allowed target must keep its
// transient classification).
func (p *egressProxy) vetted(ctx context.Context, network, addr string) (net.Conn, error) {
	conn, err := p.guard.DialContext(ctx, network, addr)
	if err != nil && errors.Is(err, events.ErrTargetDenied) {
		p.recordDenial(err)
	}
	return conn, err
}

func (p *egressProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodConnect {
		p.serveConnect(w, r)
		return
	}
	p.serveAbsolute(w, r)
}

// proxyEnv strips every inherited proxy setting — duplicate env keys
// resolve first-match in children, so appending without stripping would
// let an ambient value shadow the guard (the EnvScopes lesson) — and
// points the git subprocess at the vetting proxy, both letter cases
// because libcurl reads `http_proxy` lowercase-only but the https
// variants in either case.
func proxyEnv(env []string, proxyURL string) []string {
	out := make([]string, 0, len(env)+4)
	for _, kv := range env {
		if key, _, ok := strings.Cut(kv, "="); ok {
			switch strings.ToUpper(key) {
			case "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY":
				continue
			}
		}
		out = append(out, kv)
	}
	return append(out,
		"http_proxy="+proxyURL,
		"https_proxy="+proxyURL,
		"HTTPS_PROXY="+proxyURL,
		"HTTP_PROXY="+proxyURL,
	)
}

// serveConnect vets and dials the tunnel target, then splices bytes. The
// dial happens BEFORE the 200 so a denial reaches git as a proxy error.
func (p *egressProxy) serveConnect(w http.ResponseWriter, r *http.Request) {
	upstream, err := p.vetted(r.Context(), "tcp", r.Host)
	if err != nil {
		http.Error(w, "egress denied: "+err.Error(), http.StatusForbidden)
		return
	}
	defer upstream.Close()
	client, buf, err := http.NewResponseController(w).Hijack()
	if err != nil {
		http.Error(w, "hijack: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer client.Close()
	if _, err := buf.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		return
	}
	if err := buf.Flush(); err != nil {
		return
	}
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(upstream, buf); done <- struct{}{} }()
	go func() { _, _ = io.Copy(client, upstream); done <- struct{}{} }()
	<-done // either direction closing ends the tunnel; defers close both
}

// serveAbsolute round-trips a plain-http absolute-form request through the
// guarded dialer.
func (p *egressProxy) serveAbsolute(w http.ResponseWriter, r *http.Request) {
	if !r.URL.IsAbs() {
		http.Error(w, "proxy expects absolute-form request URIs", http.StatusBadRequest)
		return
	}
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL = pr.In.URL // already absolute; keep the target verbatim
			pr.Out.Host = pr.In.Host
		},
		Transport: &http.Transport{
			DialContext:       p.vetted,
			DisableKeepAlives: true, // re-resolve and re-vet every attempt
			Proxy:             nil,  // never chain to an environment proxy
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			status := http.StatusBadGateway
			if errors.Is(err, events.ErrTargetDenied) {
				status = http.StatusForbidden
			}
			http.Error(w, "egress: "+err.Error(), status)
		},
	}
	rp.ServeHTTP(w, r)
}
