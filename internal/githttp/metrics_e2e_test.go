package githttp

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// metricsNode is a serve process started with custom listen settings and
// full stdout/stderr capture, for the ADR-47 listener drills.
type metricsNode struct {
	cmd         *exec.Cmd
	addr        string // API/Git listener
	metricsAddr string // empty when no metrics listener was announced
	stderr      *syncBuffer
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// startMetricsNode runs `openstorage serve --listen listen` with extraEnv.
// It returns once the API listener is announced (and the metrics line,
// which serve prints right after, when wantMetrics), or with the exit
// error and stderr when serve fails to start.
func startMetricsNode(t *testing.T, listen string, wantMetrics bool, extraEnv ...string) (*metricsNode, error) {
	t.Helper()
	cmd := exec.Command(binPath, "serve", "--listen", listen, "--cache-dir", t.TempDir())
	cmd.Env = append(os.Environ(),
		"OPENSTORAGE_PG_URL="+dbURL,
		"OPENSTORAGE_S3_URL="+blobURL,
		"OPENSTORAGE_ORG="+testOrg,
		"OPENSTORAGE_ORG_PUBKEYS="+orgKeysPath,
		"OPENSTORAGE_TEST_MODE=1",
		"OPENSTORAGE_METRICS_LISTEN=",
	)
	if testBackend == backendS3WAL {
		cmd.Env = append(cmd.Env, "OPENSTORAGE_S3WAL_LEASE_TTL=0")
	}
	cmd.Env = append(cmd.Env, extraEnv...)
	n := &metricsNode{cmd: cmd, stderr: &syncBuffer{}}
	cmd.Stderr = n.stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	lines := make(chan string, 4)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
	}()
	deadline := time.After(30 * time.Second)
	for {
		select {
		case line, ok := <-lines:
			if !ok {
				werr := cmd.Wait()
				return nil, fmt.Errorf("serve exited before announcing (%v); stderr:\n%s", werr, n.stderr)
			}
			if a, found := strings.CutPrefix(line, "metrics listening on "); found {
				n.metricsAddr = a
			} else if a, found := strings.CutPrefix(line, "listening on "); found {
				n.addr = a
			}
			if n.addr != "" && (!wantMetrics || n.metricsAddr != "") {
				go func() {
					for range lines {
					}
				}()
				return n, nil
			}
		case <-deadline:
			return nil, fmt.Errorf("serve startup timed out; stderr:\n%s", n.stderr)
		}
	}
}

func httpGet(t *testing.T, url, token string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

// listeningSockets counts TCP sockets in LISTEN state owned by pid.
func listeningSockets(t *testing.T, pid int) int {
	t.Helper()
	inodes := map[string]bool{}
	fdDir := fmt.Sprintf("/proc/%d/fd", pid)
	entries, err := os.ReadDir(fdDir)
	if err != nil {
		t.Fatalf("read %s: %v", fdDir, err)
	}
	for _, e := range entries {
		target, err := os.Readlink(filepath.Join(fdDir, e.Name()))
		if err != nil {
			continue // fd closed while scanning
		}
		if in, ok := strings.CutPrefix(target, "socket:["); ok {
			inodes[strings.TrimSuffix(in, "]")] = true
		}
	}
	count := 0
	for _, table := range []string{"tcp", "tcp6"} {
		raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/net/%s", pid, table))
		if err != nil {
			continue // no IPv6 table
		}
		for _, line := range strings.Split(string(raw), "\n")[1:] {
			f := strings.Fields(line)
			if len(f) > 9 && f[3] == "0A" && inodes[f[9]] { // 0A = LISTEN
				count++
			}
		}
	}
	return count
}

// TestMetricsListenerOffByDefault: without OPENSTORAGE_METRICS_LISTEN,
// serve announces no metrics listener and owns exactly one listening
// socket (the API/Git listener).
func TestMetricsListenerOffByDefault(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("socket census reads /proc")
	}
	n, err := startMetricsNode(t, "127.0.0.1:0", false)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond) // let a stray metrics line (if any) arrive
	if n.metricsAddr != "" {
		t.Fatalf("metrics listener announced without opt-in: %s", n.metricsAddr)
	}
	if got := listeningSockets(t, n.cmd.Process.Pid); got != 1 {
		t.Fatalf("serve owns %d listening sockets by default, want 1 (API only)", got)
	}
	if code, body := httpGet(t, "http://"+n.addr+"/metrics", ""); code == http.StatusOK || strings.Contains(body, "openstorage_") {
		t.Fatalf("API port served metrics: %d %q", code, body)
	}
}

// TestMetricsListenerSeparateFromAPI: with the opt-in, /metrics answers on
// the metrics listener only. The API/Git port never serves it (with or
// without a valid token), debug routes are absent on both, and Git
// traffic through the instrumented handler is counted.
func TestMetricsListenerSeparateFromAPI(t *testing.T) {
	n, err := startMetricsNode(t, "127.0.0.1:0", true, "OPENSTORAGE_METRICS_LISTEN=127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if n.metricsAddr == n.addr {
		t.Fatalf("metrics and API share %s", n.addr)
	}
	repo := registerRepo(t, "t-metrics-e2e")
	token := mintTestToken(t, repo, "git:read,git:write", "")
	for _, tok := range []string{"", token} {
		if code, body := httpGet(t, "http://"+n.addr+"/metrics", tok); code == http.StatusOK || strings.Contains(body, "openstorage_") {
			t.Fatalf("API port served metrics (token=%v): %d %q", tok != "", code, body)
		}
	}
	for _, addr := range []string{n.addr, n.metricsAddr} {
		for _, path := range []string{"/debug/vars", "/debug/pprof/"} {
			if code, _ := httpGet(t, "http://"+addr+path, ""); code != http.StatusNotFound {
				t.Errorf("GET %s%s = %d, want 404", addr, path, code)
			}
		}
	}
	if code, _ := httpGet(t, "http://"+n.metricsAddr+"/healthz", ""); code != http.StatusNotFound {
		t.Errorf("metrics listener serves /healthz: %d", code)
	}

	gitCount := func() float64 {
		code, body := httpGet(t, "http://"+n.metricsAddr+"/metrics", "")
		if code != http.StatusOK {
			t.Fatalf("GET /metrics = %d", code)
		}
		var total float64
		for line := range strings.SplitSeq(body, "\n") {
			if strings.HasPrefix(line, `openstorage_http_requests_total{code_class="2xx",surface="git"} `) {
				v, err := strconv.ParseFloat(line[strings.LastIndex(line, " ")+1:], 64)
				if err != nil {
					t.Fatalf("parse %q: %v", line, err)
				}
				total += v
			}
		}
		return total
	}
	before := gitCount()
	work := initWorkRepo(t)
	url := fmt.Sprintf("http://t:%s@%s/%s.git", token, n.addr, repo)
	if out, err := tryGit(work, "push", url, "HEAD:refs/heads/main"); err != nil {
		t.Fatalf("push: %v\n%s\nserve stderr:\n%s", err, out, n.stderr)
	}
	if out, err := tryGit(t.TempDir(), "clone", url, "clone"); err != nil {
		t.Fatalf("clone: %v\n%s\nserve stderr:\n%s", err, out, n.stderr)
	}
	if after := gitCount(); after-before < 4 {
		t.Fatalf("push+clone counted %v git 2xx requests, want >= 4 (streamed through the instrumented handler)", after-before)
	}
}

// TestMetricsListenerBindFailuresStopStartup: an address in use, an
// invalid address, and the API's own address all fail startup with a
// nonzero exit before the API listener is announced.
func TestMetricsListenerBindFailuresStopStartup(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	free, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	apiAddr := free.Addr().String()
	_ = free.Close()

	for name, tc := range map[string]struct{ listen, metrics string }{
		"in use":            {"127.0.0.1:0", busy.Addr().String()},
		"invalid":           {"127.0.0.1:0", "not-an-address"},
		"whitespace only":   {"127.0.0.1:0", "  "},
		"same as API":       {apiAddr, apiAddr},
		"unresolvable host": {"127.0.0.1:0", "no-such-host.invalid:9464"},
	} {
		t.Run(name, func(t *testing.T) {
			n, err := startMetricsNode(t, tc.listen, true, "OPENSTORAGE_METRICS_LISTEN="+tc.metrics)
			if err == nil {
				t.Fatalf("serve started (api %s, metrics %s) despite metrics bind %q", n.addr, n.metricsAddr, tc.metrics)
			}
			if !strings.Contains(err.Error(), "OPENSTORAGE_METRICS_LISTEN") {
				t.Fatalf("startup failure does not name the variable: %v", err)
			}
		})
	}
}

// TestMetricsListenerWarnsOnUnspecifiedBind: binding every interface is
// allowed but warned about (the listener has no authentication).
func TestMetricsListenerWarnsOnUnspecifiedBind(t *testing.T) {
	n, err := startMetricsNode(t, "127.0.0.1:0", true, "OPENSTORAGE_METRICS_LISTEN=:0")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(n.stderr.String(), "no authentication") {
		t.Fatalf("no unauthenticated-bind warning on stderr:\n%s", n.stderr)
	}
}
