package api

import (
	"bytes"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"testing"
)

// The whole-stderr reading that applyPatch did before wcApplyStderr, kept
// as the oracle: status and server failure from every line, the message
// from the trimmed text.
func applyOracle(dirs []string, exit int, stderr string) (status int, server bool, msg string) {
	full := strings.TrimSpace(stderr)
	msg = full
	if len(msg) > wcMaxApplyMsgBytes {
		msg = strings.ToValidUTF8(msg[:wcMaxApplyMsgBytes], "") + " …"
	}
	status = http.StatusInternalServerError
	payload, clash := false, false
	for line := range strings.Lines(stderr) {
		for _, p := range wcApplyPayloadLines {
			payload = payload || strings.HasPrefix(line, p)
		}
		clash = clash || strings.HasPrefix(line, wcApplyClashLine)
		server = server || strings.HasPrefix(line, "fatal: ")
	}
	switch {
	case exit == 1:
		status = http.StatusConflict
	case exit != 128:
	case payload:
		status = http.StatusBadRequest
	case clash:
		status = http.StatusConflict
	}
	for _, d := range dirs {
		server = server || strings.Contains(stderr, d)
	}
	return status, server, msg
}

// FuzzApplyStderrChunks: wcApplyStderr reads stderr in the pieces git
// writes it in, and answers as the oracle does for the whole of it.
func FuzzApplyStderrChunks(f *testing.F) {
	for _, seed := range []struct {
		exit   int
		stderr string
		cuts   []byte
	}{
		{128, "error: invalid path '.git./x'\n", []byte{3, 9}},
		{128, "noise\nerror: unable to add cache entry for d\n", []byte{7, 20, 33}},
		{128, "fatal: Unable to create '/srv/scratch/index.lock': File exists.\n", []byte{30, 1}},
		{128, "error: x /srv/scr", []byte{12}},
		{128, "error: open '/srv/scratch/x': denied\n", []byte{17}},
		{1, "error: patch failed: f:1\nerror: f: patch does not apply\n", nil},
		{128, "error: patch with only garbage at ", []byte{5}},
		{128, "", nil},
	} {
		f.Add(seed.exit, seed.stderr, seed.cuts)
	}
	dirs := []string{"/srv/scratch", "/srv/cache/repo"}
	f.Fuzz(func(t *testing.T, exit int, stderr string, cuts []byte) {
		c := &wcApplyStderr{needles: dirs}
		rest := stderr
		for _, n := range cuts {
			n := min(int(n), len(rest))
			_, _ = c.Write([]byte(rest[:n]))
			rest = rest[n:]
		}
		_, _ = c.Write([]byte(rest))
		ws, wsrv, wmsg := applyOracle(dirs, exit, stderr)
		if got := c.message(); len(stderr) <= wcApplyHeadBytes && got != wmsg {
			t.Fatalf("message = %q, want %q", got, wmsg)
		}
		if got := c.status(exit); got != ws {
			t.Fatalf("status(%d) of %q = %d, want %d", exit, stderr, got, ws)
		}
		if got := c.serverFailure(); got != wsrv {
			t.Fatalf("serverFailure of %q = %v, want %v", stderr, got, wsrv)
		}
	})
}

// git apply prints a line per file that fails, so its stderr grows with
// the patch. wcApplyStderr keeps about 128 KiB of it whatever its size,
// and still answers as for the whole.
func TestApplyStderrKeepsABoundedPart(t *testing.T) {
	var buf bytes.Buffer
	for i := range 20000 {
		fmt.Fprintf(&buf, "error: f%d.txt: does not exist in index\n", i)
	}
	buf.WriteString("error: unable to add cache entry for d\n")
	dirs := []string{"/srv/scratch"}
	c := &wcApplyStderr{needles: dirs}
	for p := buf.Bytes(); len(p) > 0; {
		n := min(len(p), 4096)
		_, _ = c.Write(p[:n])
		p = p[n:]
	}
	if kept := len(c.head) + len(c.tail.String()) + len(c.line) + len(c.carry); kept > 2*64<<10+256 {
		t.Errorf("kept %d of %d bytes of stderr", kept, buf.Len())
	}
	ws, wsrv, wmsg := applyOracle(dirs, 128, buf.String())
	if got := c.message(); got != wmsg {
		t.Errorf("message = %.80q..., want %.80q...", got, wmsg)
	}
	if got := c.status(128); got != ws {
		t.Errorf("status = %d, want %d", got, ws)
	}
	if got := c.serverFailure(); got != wsrv {
		t.Errorf("serverFailure = %v, want %v", got, wsrv)
	}
}

// A patch of 20,000 files that the base does not have: git apply prints
// about 850 KB of stderr. The answer is the 409 it was before, with the
// message cut to wcMaxApplyMsgBytes.
func TestDiffCommitManyFailingFiles(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wctServer(t, e)
		repo, writeTok, _ := wctRepo(t, e)
		resp, body := wctPost(t, srv, "/api/v1/repos/commit-pack", writeTok, bytes.NewReader(wctLines(t,
			wctMeta("main", "base", []map[string]any{{"path": "a.txt", "content_id": "ida", "operation": "upsert"}}, nil),
			wctChunk("ida", []byte("a\n"), true))))
		wctRequireOK(t, resp, body)
		var patch bytes.Buffer
		for i := range 20000 {
			fmt.Fprintf(&patch, "diff --git a/f%d.txt b/f%d.txt\n--- a/f%d.txt\n+++ b/f%d.txt\n@@ -1 +1 @@\n-a\n+b\n", i, i, i, i)
		}
		var frames []any
		frames = append(frames, wctMeta("main", "many", nil, nil))
		for p := patch.Bytes(); len(p) > 0; {
			n := min(len(p), 1<<20)
			frames = append(frames, wctDiffChunk(p[:n], n == len(p)))
			p = p[n:]
		}
		before := wmRefsSnapshot(t, e, repo.RepoID)
		resp, body = wctPost(t, srv, "/api/v1/repos/diff-commit", writeTok, bytes.NewReader(wctLines(t, frames...)))
		env := wctRequireFail(t, resp, body, http.StatusConflict, "conflict")
		m := env.Result.Message
		if !strings.HasPrefix(m, "patch does not apply: error: f0.txt: does not exist in index") || !strings.HasSuffix(m, " …") || len(m) > wcMaxApplyMsgBytes+64 {
			t.Fatalf("message = %.200q (%d bytes), want git's first lines cut to %d bytes", m, len(m), wcMaxApplyMsgBytes)
		}
		if after := wmRefsSnapshot(t, e, repo.RepoID); !maps.Equal(before, after) {
			t.Fatalf("a failed diff-commit moved refs: %v -> %v", before, after)
		}
	})
}

// diff-commit takes at most wcMaxDiffBytes of decoded patch, summed over
// its chunks, the commit-pack limit. Past that it answers 400 invalid, as
// for a chunk over 4 MiB, and moves no ref. The test lowers the limit.
func TestDiffCommitTotalSizeLimit(t *testing.T) {
	if wcMaxDiffBytes != wcMaxCommitBlobBytes {
		t.Fatalf("wcMaxDiffBytes = %d, want the commit-pack limit %d", wcMaxDiffBytes, wcMaxCommitBlobBytes)
	}
	patch := "diff --git a/n.txt b/n.txt\nnew file mode 100644\n--- /dev/null\n+++ b/n.txt\n@@ -0,0 +1 @@\n+x\n"
	old := wcMaxDiffBytes
	wcMaxDiffBytes = int64(len(patch))
	t.Cleanup(func() { wcMaxDiffBytes = old })
	runBackends(t, func(t *testing.T, e *testEnv) {
		srv := wctServer(t, e)
		repo, writeTok, _ := wctRepo(t, e)
		post := func(chunks ...string) (*http.Response, []byte) {
			t.Helper()
			frames := []any{wctMeta("main", "d", nil, nil)}
			for i, c := range chunks {
				frames = append(frames, wctDiffChunk([]byte(c), i == len(chunks)-1))
			}
			return wctPost(t, srv, "/api/v1/repos/diff-commit", writeTok, bytes.NewReader(wctLines(t, frames...)))
		}
		before := wmRefsSnapshot(t, e, repo.RepoID)
		resp, body := post(patch[:20], patch[20:]+"\n")
		env := wctRequireFail(t, resp, body, http.StatusBadRequest, "invalid")
		if want := fmt.Sprintf("diff exceeds %d bytes", len(patch)); env.Result.Message != want || env.Commit != nil {
			t.Fatalf("body = %s, want %q and no commit", body, want)
		}
		if after := wmRefsSnapshot(t, e, repo.RepoID); !maps.Equal(before, after) {
			t.Fatalf("an over-limit diff-commit moved refs: %v -> %v", before, after)
		}
		// Exactly at the limit, in two chunks: the commit lands.
		resp, body = post(patch[:20], patch[20:])
		wctRequireOK(t, resp, body)
	})
}
