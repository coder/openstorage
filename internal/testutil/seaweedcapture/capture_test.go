package seaweedcapture

import (
	"context"
	"encoding/json/v2"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

const testSession = "issue281-test"

// withCapture drives only the public request-local consumer/lifecycle seam.
func withCapture(t *testing.T, run func(testcontainers.GenericContainerRequest, *fakeContainer)) string {
	t.Helper()
	dir := captureEnv(t)
	c := &fakeContainer{id: strings.Repeat("b", 64)}
	_, err := start(t.Context(), testcontainers.GenericContainerRequest{Started: true}, func(ctx context.Context, req testcontainers.GenericContainerRequest) (testcontainers.Container, error) {
		for _, hooks := range req.LifecycleHooks {
			for _, hook := range hooks.PostCreates {
				if err := hook(ctx, c); err != nil {
					t.Fatal(err)
				}
			}
		}
		run(req, c)
		return c, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, "seaweed-"+testSession+"-"+c.id)
}

func finalize(t *testing.T, req testcontainers.GenericContainerRequest, c *fakeContainer) {
	t.Helper()
	for _, hooks := range req.LifecycleHooks {
		for _, hook := range hooks.PreTerminates {
			if err := hook(t.Context(), c); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func readStatus(t *testing.T, path string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(path, "status.json"))
	if err != nil {
		t.Fatal(err)
	}
	var status map[string]any
	if err := json.Unmarshal(data, &status); err != nil {
		t.Fatal(err)
	}
	return status
}

func TestUnsetIsUnchanged(t *testing.T) {
	t.Setenv("OPENSTORAGE_TEST_ARTIFACT_DIR", "")
	t.Setenv("TESTCONTAINERS_SESSION_ID", "")
	called := false
	_, err := start(t.Context(), testcontainers.GenericContainerRequest{Image: "unchanged", Started: true}, func(_ context.Context, req testcontainers.GenericContainerRequest) (testcontainers.Container, error) {
		called = true
		if req.Image != "unchanged" || req.LogConsumerCfg != nil || req.Logger != nil || len(req.LifecycleHooks) != 0 {
			t.Fatal("unset path changed request")
		}
		return nil, nil
	})
	if err != nil || !called {
		t.Fatalf("called=%t err=%v", called, err)
	}
}

func TestRejectUnsafeArtifactsBeforeContainerCreation(t *testing.T) {
	for _, scenario := range []string{"empty-session", "traversal-session", "long-session", "public-directory", "missing-directory", "symlink-directory"} {
		t.Run(scenario, func(t *testing.T) {
			dir := captureEnv(t)
			switch scenario {
			case "empty-session":
				t.Setenv("TESTCONTAINERS_SESSION_ID", "")
			case "traversal-session":
				t.Setenv("TESTCONTAINERS_SESSION_ID", "../../escape")
			case "long-session":
				t.Setenv("TESTCONTAINERS_SESSION_ID", strings.Repeat("a", 65))
			case "public-directory":
				if err := os.Chmod(dir, 0o755); err != nil {
					t.Fatal(err)
				}
			case "missing-directory":
				t.Setenv("OPENSTORAGE_TEST_ARTIFACT_DIR", filepath.Join(dir, "absent"))
			case "symlink-directory":
				link := filepath.Join(t.TempDir(), "link")
				if err := os.Symlink(dir, link); err != nil {
					t.Fatal(err)
				}
				t.Setenv("OPENSTORAGE_TEST_ARTIFACT_DIR", link)
			}
			_, err := start(t.Context(), testcontainers.GenericContainerRequest{Started: true}, func(context.Context, testcontainers.GenericContainerRequest) (testcontainers.Container, error) {
				t.Fatal("created before validation")
				return nil, nil
			})
			if err == nil {
				t.Fatal("unsafe input accepted")
			}
		})
	}
}

func TestReceiptErrorsAndPendingEvidence(t *testing.T) {
	path := withCapture(t, func(req testcontainers.GenericContainerRequest, c *fakeContainer) {
		path := filepath.Join(os.Getenv("OPENSTORAGE_TEST_ARTIFACT_DIR"), "seaweed-"+testSession+"-"+c.id)
		if readStatus(t, path)["phase"] != "pending" {
			t.Fatal("missing initial receipt")
		}
		// The pinned logger reports these conditions without returning them.
		for range 100 {
			req.Logger.Printf("Unexpected error reading logs: %v", "private-secret")
		}
		req.Logger.Printf("timeout waiting for log production goroutine to exit; a goroutine may have leaked")
		req.Logger.Printf("failed reading container logs: %v\npartial container logs (%s):\n%s", "private-secret", "private-secret", "private-secret")
		req.Logger.Printf("container logs (%s):\n%s", "private-secret", "private-secret")
		finalize(t, req, c)
		finalize(t, req, c) // Repeated termination must not rewrite/close twice.
		req.LogConsumerCfg.Consumers[0].Accept(testcontainers.Log{LogType: testcontainers.StdoutLog, Content: []byte("too late")})
	})
	status := readStatus(t, path)
	errors := status["errors"].([]any)
	want := []string{"stream-error", "drain-timeout", "startup-log-read-error", "missing-output"}
	if len(errors) != len(want) {
		t.Fatalf("errors=%v", errors)
	}
	for i, code := range want {
		if errors[i] != code {
			t.Errorf("error[%d]=%v, want %s", i, errors[i], code)
		}
	}
	if status["stdout_bytes"] != float64(0) {
		t.Fatal("late data accepted")
	}
}

func TestReceiptDoesNotFollowPreexistingTemporarySymlink(t *testing.T) {
	withCapture(t, func(req testcontainers.GenericContainerRequest, c *fakeContainer) {
		path := filepath.Join(os.Getenv("OPENSTORAGE_TEST_ARTIFACT_DIR"), "seaweed-"+testSession+"-"+c.id)
		if err := os.Symlink("stdout.log", filepath.Join(path, "status.tmp")); err != nil {
			t.Fatal(err)
		}
		finalize(t, req, c)
		data, err := os.ReadFile(filepath.Join(path, "stdout.log"))
		if err != nil {
			t.Fatal(err)
		}
		if len(data) != 0 {
			t.Fatalf("receipt clobbered stdout: %q", data)
		}
		if readStatus(t, path)["phase"] != "pending" {
			t.Fatal("receipt replaced through unsafe temporary file")
		}
	})
}

func TestStartupFailureDrainsBeforeRemoval(t *testing.T) {
	dir := captureEnv(t)
	c := &fakeContainer{id: strings.Repeat("c", 64)}
	path := filepath.Join(dir, "seaweed-"+testSession+"-"+c.id)
	native := errors.New("native startup failure")
	removed := false
	_, err := start(t.Context(), testcontainers.GenericContainerRequest{Started: true}, func(ctx context.Context, req testcontainers.GenericContainerRequest) (testcontainers.Container, error) {
		for _, hooks := range req.LifecycleHooks {
			for _, hook := range hooks.PostCreates {
				if err := hook(ctx, c); err != nil {
					t.Fatal(err)
				}
			}
		}
		c.terminate = func() {
			// Model Stop's final buffered event before PreTerminates/removal.
			req.LogConsumerCfg.Consumers[0].Accept(testcontainers.Log{LogType: testcontainers.StderrLog, Content: []byte("shutdown event\n")})
			finalize(t, req, c)
			if readStatus(t, path)["phase"] != "finalized" {
				t.Fatal("removed before finalization")
			}
			data, err := os.ReadFile(filepath.Join(path, "stderr.log"))
			if err != nil || string(data) != "shutdown event\n" {
				t.Fatal("removed before drain")
			}
			removed = true
		}
		return c, native
	})
	if !errors.Is(err, native) || !removed {
		t.Fatalf("native=%v removed=%t", err, removed)
	}
	status := readStatus(t, path)
	if got := status["errors"].([]any); len(got) != 1 || got[0] != "startup-error" {
		t.Fatalf("errors=%v", got)
	}
}

func TestWriteFailureIsExplicit(t *testing.T) {
	path := withCapture(t, func(req testcontainers.GenericContainerRequest, c *fakeContainer) {
		// Inject a failed file descriptor; chmod cannot revoke an open fd.
		consumer := req.LogConsumerCfg.Consumers[0].(*capture)
		if err := consumer.stdout.Close(); err != nil {
			t.Fatal(err)
		}
		consumer.Accept(testcontainers.Log{LogType: testcontainers.StdoutLog, Content: []byte("lost event")})
		finalize(t, req, c)
	})
	found := false
	for _, code := range readStatus(t, path)["errors"].([]any) {
		if code == "write-error" {
			found = true
		}
	}
	if !found {
		t.Fatal("write failure not reported")
	}
}

func TestCaptureDirectoryCollisionDoesNotClobber(t *testing.T) {
	dir := captureEnv(t)
	c := &fakeContainer{id: strings.Repeat("d", 64)}
	path := filepath.Join(dir, "seaweed-"+testSession+"-"+c.id)
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "stdout.log"), []byte("sentinel"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := start(t.Context(), testcontainers.GenericContainerRequest{Started: true}, func(ctx context.Context, req testcontainers.GenericContainerRequest) (testcontainers.Container, error) {
		for _, hooks := range req.LifecycleHooks {
			for _, hook := range hooks.PostCreates {
				if err := hook(ctx, c); err != nil {
					return c, err
				}
			}
		}
		t.Fatal("collision accepted")
		return nil, nil
	})
	if err == nil {
		t.Fatal("collision accepted")
	}
	data, err := os.ReadFile(filepath.Join(path, "stdout.log"))
	if err != nil || string(data) != "sentinel" {
		t.Fatal("existing evidence changed")
	}
}

// Embedding makes unexpected fixture operations fail rather than silently pass.
type fakeContainer struct {
	testcontainers.Container
	id        string
	terminate func()
}

func (c *fakeContainer) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	if c.terminate != nil {
		c.terminate()
	}
	return nil
}

func (c *fakeContainer) GetContainerID() string { return c.id }
func (c *fakeContainer) SessionID() string      { return testSession }
func (c *fakeContainer) Inspect(context.Context) (*container.InspectResponse, error) {
	return &container.InspectResponse{Config: &container.Config{Labels: map[string]string{
		"org.testcontainers.sessionId": testSession,
	}}}, nil
}
func (c *fakeContainer) GetLogProductionErrorChannel() <-chan error {
	ch := make(chan error, 1)
	ch <- context.Canceled // The pinned producer reports this after natural EOF.
	close(ch)
	return ch
}

func captureEnv(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OPENSTORAGE_TEST_ARTIFACT_DIR", dir)
	t.Setenv("TESTCONTAINERS_SESSION_ID", testSession)
	return dir
}

func TestBoundedAttributedPrivateStreams(t *testing.T) {
	dir := captureEnv(t)
	c := &fakeContainer{id: strings.Repeat("a", 64)}
	_, err := start(t.Context(), testcontainers.GenericContainerRequest{Started: true}, func(ctx context.Context, req testcontainers.GenericContainerRequest) (testcontainers.Container, error) {
		if req.LogConsumerCfg == nil {
			t.Fatal("consumer not attached to request")
		}
		for _, hooks := range req.LifecycleHooks {
			for _, hook := range hooks.PostCreates {
				if err := hook(ctx, c); err != nil {
					t.Fatal(err)
				}
			}
		}
		consumer := req.LogConsumerCfg.Consumers[0]
		consumer.Accept(testcontainers.Log{LogType: testcontainers.StdoutLog, Content: []byte("stdout event\n")})
		consumer.Accept(testcontainers.Log{LogType: testcontainers.StderrLog, Content: []byte(strings.Repeat("e", 1024*1024+1))})
		// TC runs PreTerminates after its default Stop/drain. The real-container
		// tests additionally verify that ordering against the pinned library.
		for _, hooks := range req.LifecycleHooks {
			for _, hook := range hooks.PreTerminates {
				if err := hook(ctx, c); err != nil {
					t.Fatal(err)
				}
			}
		}
		return c, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "seaweed-"+testSession+"-"+c.id)
	for _, name := range []string{"stdout.log", "stderr.log", "status.json"} {
		st, err := os.Stat(filepath.Join(path, name))
		if err != nil {
			t.Fatal(err)
		}
		if st.Mode().Perm() != 0o600 {
			t.Errorf("%s permissions = %o", name, st.Mode().Perm())
		}
	}
	out, err := os.ReadFile(filepath.Join(path, "stdout.log"))
	if err != nil || string(out) != "stdout event\n" {
		t.Fatalf("stdout = %q, error = %v", out, err)
	}
	out, err = os.ReadFile(filepath.Join(path, "stderr.log"))
	if err != nil || len(out) != 1024*1024 {
		t.Fatalf("stderr length = %d, error = %v", len(out), err)
	}
	data, err := os.ReadFile(filepath.Join(path, "status.json"))
	if err != nil {
		t.Fatal(err)
	}
	var receipt struct {
		Version   int    `json:"version"`
		Session   string `json:"session"`
		Container string `json:"container"`
		Phase     string `json:"phase"`
		Truncated bool   `json:"truncated"`
	}
	if err := json.Unmarshal(data, &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Version != 1 || receipt.Session != testSession || receipt.Container != c.id || receipt.Phase != "finalized" || !receipt.Truncated {
		t.Fatalf("bad receipt: %s", data)
	}
}
