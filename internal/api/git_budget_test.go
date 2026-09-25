package api

import (
	"bufio"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/gitread"
)

// withGitReadPolicy swaps the process-wide read-side git policy for one
// test (api tests are sequential).
func withGitReadPolicy(t *testing.T, p gitReadPolicy) *gitReadPolicy {
	t.Helper()
	old := gitReads
	gitReads = p
	t.Cleanup(func() { gitReads = old })
	return &gitReads
}

// testGitReadPolicy is the production policy shape with test-sized knobs.
func testGitReadPolicy(slots int, timeout time.Duration, output int64, limits gitread.Limits) gitReadPolicy {
	return gitReadPolicy{
		slots:          gitread.NewSlots(slots, slots, 0),
		streamSlots:    gitread.NewSlots(slots, slots, 0),
		timeout:        timeout,
		archiveTimeout: timeout,
		outputBytes:    output,
		limits:         limits,
		writeIdle:      time.Minute,
		delivery:       time.Minute,
		streamDelivery: time.Minute,
	}
}

func requireSlotsFree(t *testing.T, p *gitReadPolicy) {
	t.Helper()
	if n := p.slots.InUse() + p.streamSlots.InUse(); n != 0 {
		t.Fatalf("%d git read slots still held after the responses", n)
	}
}

// deepFixture seeds a repo whose main branch is a linear history of n
// commits: cold.txt exists only in the root commit, hot.txt changes in
// every commit (the #313 attack shape, scaled down).
type deepFixture struct {
	token     string
	tip, root string
}

func seedDeepHistory(t *testing.T, e *testEnv, org string, n int) deepFixture {
	t.Helper()
	url := "https://example.com/" + org + "/deep-" + randomHex(t, 4)
	repo, err := e.store.CreateRepo(context.Background(), org, url, "main")
	if err != nil {
		t.Fatalf("create repo: %v", err)
	}
	dir := t.TempDir()
	coreGit(t, dir, "init", "-q", "-b", "main", ".")
	var in strings.Builder
	for i := range n {
		fmt.Fprintf(&in, "commit refs/heads/main\nmark :%d\ncommitter D <d@example.com> %d +0000\ndata 2\nc\n", i+1, 1700000000+i)
		if i > 0 {
			fmt.Fprintf(&in, "from :%d\n", i)
		} else {
			in.WriteString("M 100644 inline cold.txt\ndata 5\ncold\n")
		}
		fmt.Fprintf(&in, "M 100644 inline hot.txt\ndata %d\n%d\n", len(fmt.Sprint(i))+1, i)
	}
	cmd := exec.Command("git", "fast-import", "--quiet")
	cmd.Dir = dir
	cmd.Stdin = strings.NewReader(in.String())
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("fast-import: %v\n%s", err, out)
	}
	tip := coreRevParse(t, dir, "main")
	root := strings.TrimSpace(coreGit(t, dir, "rev-list", "--max-parents=0", "main"))
	coreSeedStores(t, e, repo.RepoID, dir, map[string]string{"refs/heads/main": tip})
	return deepFixture{
		token: e.mint(t, tokenSpec{iss: org, repo: url, scopes: []string{auth.ScopeGitRead}}),
		tip:   tip, root: root,
	}
}

func requireLimitStatus(t *testing.T, resp *http.Response, body []byte, want int, retryAfter bool, msgPart string) {
	t.Helper()
	coreRequireStatus(t, resp, body, want)
	if got := resp.Header.Get("Retry-After"); (got != "") != retryAfter {
		t.Fatalf("Retry-After = %q, want present=%v", got, retryAfter)
	}
	if !strings.Contains(string(body), msgPart) {
		t.Fatalf("body %s does not mention %q", body, msgPart)
	}
}

func skipWithoutHeapCap(t *testing.T) {
	t.Helper()
	if !gitread.HeapLimitSupported() {
		t.Skip("git heap caps are enforced on Linux only")
	}
}

func TestGitReadBudgetDeepHistory(t *testing.T) {
	skipWithoutHeapCap(t)
	// Pin the worst case: full hydrates write a commit-graph (ADR-47), which
	// shrinks a deep --skip walk below the small test cap. The graph is
	// best-effort, so the limits must hold without one; git children of
	// this test ignore it.
	t.Setenv("GIT_CONFIG_COUNT", "1")
	t.Setenv("GIT_CONFIG_KEY_0", "core.commitGraph")
	t.Setenv("GIT_CONFIG_VALUE_0", "false")
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		fx := seedDeepHistory(t, e, org, 20000)
		small := gitread.Limits{HeavyHeap: 4 << 20, LightHeap: gitread.LightHeapBytes, MaxBlob: gitread.MaxBlobBytes}

		t.Run("commits deep skip hits the heap cap", func(t *testing.T) {
			p := withGitReadPolicy(t, testGitReadPolicy(2, time.Minute, gitread.DefaultOutputBytes, small))
			cursor := coreCursorEncode(cursorKindCommits, fx.tip, "19990")
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/commits?cursor="+cursor, fx.token, nil)
			requireLimitStatus(t, resp, body, http.StatusUnprocessableEntity, false, "memory")
			requireSlotsFree(t, p)
			// The service survives: a shallow page still works.
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/commits?limit=5", fx.token, nil)
			coreRequireStatus(t, resp, body, http.StatusOK)
			requireSlotsFree(t, p)
		})

		t.Run("files/metadata per-file walk hits the heap cap", func(t *testing.T) {
			p := withGitReadPolicy(t, testGitReadPolicy(2, time.Minute, gitread.DefaultOutputBytes, small))
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/files/metadata", fx.token, nil)
			requireLimitStatus(t, resp, body, http.StatusUnprocessableEntity, false, "memory")
			requireSlotsFree(t, p)
		})

		t.Run("blame of a root-commit file hits the heap cap", func(t *testing.T) {
			withGitReadPolicy(t, testGitReadPolicy(2, time.Minute, gitread.DefaultOutputBytes, small))
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/blame?path=cold.txt", fx.token, nil)
			requireLimitStatus(t, resp, body, http.StatusUnprocessableEntity, false, "memory")
		})

		t.Run("buffered output over the request allowance", func(t *testing.T) {
			p := withGitReadPolicy(t, testGitReadPolicy(2, time.Minute, 8<<10, gitread.DefaultLimits()))
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/commits?limit=100", fx.token, nil)
			requireLimitStatus(t, resp, body, http.StatusUnprocessableEntity, false, "output")
			requireSlotsFree(t, p)
		})

		t.Run("default policy serves the same requests", func(t *testing.T) {
			cursor := coreCursorEncode(cursorKindCommits, fx.tip, "19990")
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/commits?cursor="+cursor, fx.token, nil)
			coreRequireStatus(t, resp, body, http.StatusOK)
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/files/metadata", fx.token, nil)
			coreRequireStatus(t, resp, body, http.StatusOK)
			requireSlotsFree(t, &gitReads)
		})
	})
}

func TestGitReadBudgetAdmissionAndDeadline(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		org := e.newOrg(t)
		fx := coreSeedFixture(t, e, org)

		t.Run("saturated slots answer 503 with Retry-After", func(t *testing.T) {
			p := withGitReadPolicy(t, testGitReadPolicy(1, time.Minute, gitread.DefaultOutputBytes, gitread.DefaultLimits()))
			// Another request holds the only slot: it ran one Heavy child
			// and has not finished.
			holder := gitread.NewBudget(p.slots, "another-repo", time.Minute, gitread.DefaultOutputBytes)
			hctx := gitread.WithBudget(context.Background(), holder)
			if _, err := gitread.Run(hctx, gitread.Cmd{Class: gitread.Heavy, Dir: t.TempDir(), Args: []string{"version"}}); err != nil {
				t.Fatal(err)
			}
			for _, path := range []string{
				"/api/v1/repos/commits",
				"/api/v1/repos/commit?ref=main", // single-object (Light) children only
				"/api/v1/repos/files/metadata",
				"/api/v1/repos/diff?ref=main",
				"/api/v1/repos/merge/preview?source_branch=feature&target_branch=main",
			} {
				resp, body := coreGet(t, e, "GET", path, fx.token, nil)
				requireLimitStatus(t, resp, body, http.StatusServiceUnavailable, true, "busy")
			}
			// Downloads draw from their own pool.
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/file?path=README.md", fx.token, nil)
			coreRequireStatus(t, resp, body, http.StatusOK)
			resp, body = e.doReq(t, "POST", "/api/v1/repos/archive", fx.token, []byte(`{}`), "")
			coreRequireStatus(t, resp, body, http.StatusOK)
			holder.Close()

			// And the other way round: a held download slot refuses
			// downloads, not JSON reads.
			streams := gitread.NewBudget(p.streamSlots, "another-repo", time.Minute, gitread.DefaultOutputBytes)
			if _, err := gitread.Run(gitread.WithBudget(context.Background(), streams),
				gitread.Cmd{Class: gitread.Light, Dir: t.TempDir(), Args: []string{"version"}}); err != nil {
				t.Fatal(err)
			}
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/file?path=README.md", fx.token, nil)
			requireLimitStatus(t, resp, body, http.StatusServiceUnavailable, true, "busy")
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/commit?ref=main", fx.token, nil)
			coreRequireStatus(t, resp, body, http.StatusOK)
			streams.Close()
			requireSlotsFree(t, p)
		})

		t.Run("one repository cannot take the whole pool", func(t *testing.T) {
			pol := testGitReadPolicy(3, time.Minute, gitread.DefaultOutputBytes, gitread.DefaultLimits())
			pol.slots = gitread.NewSlots(3, 2, 0)
			p := withGitReadPolicy(t, pol)
			other := seedDeepHistory(t, e, org, 10)
			// Two slow requests of fx's repository hold its whole share.
			key := "repo\x00" + fx.repo.RepoID
			var held []*gitread.Budget
			for range 2 {
				b := gitread.NewBudget(p.slots, key, time.Minute, gitread.DefaultOutputBytes)
				if _, err := gitread.Run(gitread.WithBudget(context.Background(), b),
					gitread.Cmd{Class: gitread.Light, Dir: t.TempDir(), Args: []string{"version"}}); err != nil {
					t.Fatal(err)
				}
				held = append(held, b)
			}
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/commits", fx.token, nil)
			requireLimitStatus(t, resp, body, http.StatusServiceUnavailable, true, "busy")
			// A token naming the repository by ID shares the same quota.
			byID := e.mint(t, tokenSpec{iss: org, repo: fx.repo.RepoID, scopes: []string{auth.ScopeGitRead}})
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/commits", byID, nil)
			requireLimitStatus(t, resp, body, http.StatusServiceUnavailable, true, "busy")
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/commits", other.token, nil)
			coreRequireStatus(t, resp, body, http.StatusOK)
			for _, b := range held {
				b.Close()
			}
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/commits", fx.token, nil)
			coreRequireStatus(t, resp, body, http.StatusOK)
			resp, body = coreGet(t, e, "GET", "/api/v1/repos/commits", byID, nil)
			coreRequireStatus(t, resp, body, http.StatusOK)
			requireSlotsFree(t, p)
		})

		t.Run("spent time budget answers 503 without Retry-After", func(t *testing.T) {
			p := withGitReadPolicy(t, testGitReadPolicy(2, time.Nanosecond, gitread.DefaultOutputBytes, gitread.DefaultLimits()))
			for _, path := range []string{
				"/api/v1/repos/commits",
				"/api/v1/repos/merge/preview?source_branch=feature&target_branch=main",
			} {
				resp, body := coreGet(t, e, "GET", path, fx.token, nil)
				requireLimitStatus(t, resp, body, http.StatusServiceUnavailable, false, "time budget")
			}
			requireSlotsFree(t, p)
		})

		t.Run("blob over the size limit answers 422 before streaming", func(t *testing.T) {
			withGitReadPolicy(t, testGitReadPolicy(2, time.Minute, gitread.DefaultOutputBytes,
				gitread.Limits{HeavyHeap: gitread.HeavyHeapBytes, LightHeap: gitread.LightHeapBytes, MaxBlob: 4}))
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/file?path=README.md", fx.token, nil)
			requireLimitStatus(t, resp, body, http.StatusUnprocessableEntity, false, "size limit")
			resp, body = e.doReq(t, "POST", "/api/v1/repos/archive", fx.token, []byte(`{}`), "")
			requireLimitStatus(t, resp, body, http.StatusUnprocessableEntity, false, "size limit")
		})

		t.Run("ref resolution limit is not reported as not found", func(t *testing.T) {
			skipWithoutHeapCap(t)
			withGitReadPolicy(t, testGitReadPolicy(2, time.Minute, gitread.DefaultOutputBytes,
				gitread.Limits{HeavyHeap: gitread.HeavyHeapBytes, LightHeap: 64 << 10, MaxBlob: gitread.MaxBlobBytes}))
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/commits?ref=main", fx.token, nil)
			requireLimitStatus(t, resp, body, http.StatusUnprocessableEntity, false, "memory")
		})

		t.Run("git errors quoting request input are not memory limits", func(t *testing.T) {
			skipWithoutHeapCap(t)
			// git reports "fatal: no such path out of memory in <sha>".
			resp, body := coreGet(t, e, "GET", "/api/v1/repos/blame?path=out%20of%20memory", fx.token, nil)
			coreRequireStatus(t, resp, body, http.StatusNotFound)
		})
	})
}

// seedBigBlob seeds a repo whose main branch has big.bin: size bytes of
// incompressible data, so a download cannot hide in socket buffers.
func seedBigBlob(t *testing.T, e *testEnv, org string, size int) string {
	t.Helper()
	url := "https://example.com/" + org + "/big-" + randomHex(t, 4)
	repo, err := e.store.CreateRepo(context.Background(), org, url, "main")
	if err != nil {
		t.Fatalf("create repo: %v", err)
	}
	dir := t.TempDir()
	coreGit(t, dir, "init", "-q", "-b", "main", ".")
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "big.bin"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	coreGit(t, dir, "add", ".")
	coreGit(t, dir, "commit", "-q", "-m", "big")
	coreSeedStores(t, e, repo.RepoID, dir, map[string]string{"refs/heads/main": coreRevParse(t, dir, "HEAD")})
	return e.mint(t, tokenSpec{iss: org, repo: url, scopes: []string{auth.ScopeGitRead}})
}

// rawDownload opens a raw HTTP/1.1 connection with a small receive buffer,
// sends GET path, and returns the connection after the response headers.
func rawDownload(t *testing.T, e *testEnv, path, token string) (net.Conn, *bufio.Reader) {
	t.Helper()
	d := net.Dialer{Control: func(_, _ string, c syscall.RawConn) error {
		var serr error
		if err := c.Control(func(fd uintptr) {
			serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 4096)
		}); err != nil {
			return err
		}
		return serr
	}}
	conn, err := d.Dial("tcp", strings.TrimPrefix(e.srv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: test\r\nAuthorization: Bearer %s\r\n\r\n", path, token); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(conn)
	status, err := br.ReadString('\n')
	if err != nil || !strings.Contains(status, " 200 ") {
		t.Fatalf("status line %q, err %v", status, err)
	}
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line == "\r\n" {
			return conn, br
		}
	}
}

// slotsFreeAfter polls until no slot is held and the request's staged
// download file is gone (the handler has returned), returning how long
// that took, or -1 after within.
func slotsFreeAfter(p *gitReadPolicy, tmp string, within time.Duration) time.Duration {
	start := time.Now()
	for time.Since(start) <= within {
		staged, _ := filepath.Glob(filepath.Join(tmp, "openstorage-file-*"))
		if p.slots.InUse()+p.streamSlots.InUse() == 0 && len(staged) == 0 {
			return time.Since(start)
		}
		time.Sleep(20 * time.Millisecond)
	}
	return -1
}

// A client that stops reading, or reads only a trickle, cannot keep its
// request's slot (and payload) forever: write deadlines close the
// connection and the request ends.
func TestGitReadBudgetStalledAndTricklingClients(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		const size = 24 << 20
		token := seedBigBlob(t, e, e.newOrg(t), size)

		t.Run("stalled reader", func(t *testing.T) {
			tmp := t.TempDir()
			t.Setenv("TMPDIR", tmp) // the handler stages the blob here
			pol := testGitReadPolicy(2, time.Minute, gitread.DefaultOutputBytes, gitread.DefaultLimits())
			pol.writeIdle = 300 * time.Millisecond
			p := withGitReadPolicy(t, pol)
			conn, br := rawDownload(t, e, "/api/v1/repos/file?path=big.bin", token)
			took := slotsFreeAfter(p, tmp, 20*time.Second) // never reads the body
			if took < 0 {
				t.Fatal("stalled download kept its slot or staged file for 20 s")
			}
			t.Logf("slot and staged file released %v after the headers", took)
			_ = conn.SetReadDeadline(time.Now().Add(20 * time.Second))
			n, _ := io.Copy(io.Discard, br)
			if n >= size {
				t.Fatalf("stalled client still received all %d bytes", n)
			}
		})

		t.Run("trickling reader", func(t *testing.T) {
			tmp := t.TempDir()
			t.Setenv("TMPDIR", tmp)
			pol := testGitReadPolicy(2, time.Minute, gitread.DefaultOutputBytes, gitread.DefaultLimits())
			pol.writeIdle = 5 * time.Second
			pol.streamDelivery = time.Second
			p := withGitReadPolicy(t, pol)
			conn, br := rawDownload(t, e, "/api/v1/repos/file?path=big.bin", token)
			_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			buf := make([]byte, 8<<10)
			var got int64
			start := time.Now()
			freed := make(chan time.Duration, 1)
			go func() { freed <- slotsFreeAfter(p, tmp, 30*time.Second) }()
			for {
				n, err := br.Read(buf)
				got += int64(n)
				if err != nil {
					break
				}
				time.Sleep(20 * time.Millisecond) // about 400 KB/s: 24 MiB would take a minute
			}
			if got >= size {
				t.Fatalf("trickling client received all %d bytes", got)
			}
			// Bytes already in socket buffers keep draining after the
			// deadline; the slot must be free well before that.
			took := <-freed
			t.Logf("slot released after %v; connection closed after %v and %d bytes", took, time.Since(start), got)
			if took < 0 || took > 4*time.Second {
				t.Fatalf("slot held %v under a 1 s delivery limit", took)
			}
		})
	})
}

// The write deadline of a git:read response is cleared when it finishes,
// so a later request on the same keep-alive connection (here one without
// a git budget) is not cut off by it.
func TestGitReadBudgetDeadlineClearedForKeepAlive(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := coreSeedFixture(t, e, e.newOrg(t))
		pol := testGitReadPolicy(2, time.Minute, gitread.DefaultOutputBytes, gitread.DefaultLimits())
		pol.writeIdle, pol.delivery = 100*time.Millisecond, 200*time.Millisecond
		withGitReadPolicy(t, pol)
		client := e.srv.Client()
		get := func(path string) (int, bool) {
			var reused bool
			ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
				GotConn: func(i httptrace.GotConnInfo) { reused = i.Reused },
			})
			req, err := http.NewRequestWithContext(ctx, "GET", e.srv.URL+path, nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer "+fx.token)
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("GET %s: %v", path, err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			return resp.StatusCode, reused
		}
		if code, _ := get("/api/v1/repos/commits"); code != http.StatusOK {
			t.Fatalf("commits = %d", code)
		}
		time.Sleep(400 * time.Millisecond) // past the first response's deadline
		code, reused := get("/api/v1/repos/branches")
		if code != http.StatusOK || !reused {
			t.Fatalf("branches on the reused connection = %d (reused %v)", code, reused)
		}
	})
}
