// Package seaweedcapture retains opt-in, private logs for SeaweedFS test
// fixtures. It must not be imported by production code.
package seaweedcapture

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

const streamLimit = 1024 * 1024

var (
	sessionPattern   = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)
	containerPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
)

type receipt struct {
	Version     int      `json:"version"`
	Session     string   `json:"session"`
	Container   string   `json:"container"`
	Phase       string   `json:"phase"`
	Coverage    string   `json:"coverage"`
	StdoutBytes int      `json:"stdout_bytes"`
	StderrBytes int      `json:"stderr_bytes"`
	Truncated   bool     `json:"truncated"`
	Errors      []string `json:"errors"`
}

type capture struct {
	mu             sync.Mutex
	status         receipt
	root           *os.Root
	stdout, stderr *os.File
	ctx            context.Context
	finished       bool
}

// Start is GenericContainer with private log retention when
// OPENSTORAGE_TEST_ARTIFACT_DIR is set. The caller creates that directory with
// mode 0700 and supplies TESTCONTAINERS_SESSION_ID before any container starts.
// Runtime capture failures are receipts, not replacement test verdicts.
func Start(ctx context.Context, req testcontainers.GenericContainerRequest) (testcontainers.Container, error) {
	return start(ctx, req, testcontainers.GenericContainer)
}

func start(ctx context.Context, req testcontainers.GenericContainerRequest, create func(context.Context, testcontainers.GenericContainerRequest) (testcontainers.Container, error)) (testcontainers.Container, error) {
	dir := os.Getenv("OPENSTORAGE_TEST_ARTIFACT_DIR")
	if dir == "" {
		return create(ctx, req)
	}
	session := os.Getenv("TESTCONTAINERS_SESSION_ID")
	if !sessionPattern.MatchString(session) {
		return nil, errors.New("seaweed capture: explicit safe TESTCONTAINERS_SESSION_ID required")
	}
	st, err := os.Lstat(dir)
	if err != nil || !st.IsDir() || st.Mode().Perm() != 0o700 {
		return nil, errors.New("seaweed capture: artifact directory must exist with mode 0700 and not be a symlink")
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, errors.New("seaweed capture: cannot open artifact directory")
	}
	defer root.Close()
	if req.LogConsumerCfg != nil || req.Logger != nil || !req.Started {
		return nil, errors.New("seaweed capture: expected started fixture without existing log configuration")
	}
	cap := &capture{ctx: ctx, status: receipt{Version: 1, Session: session, Phase: "pending", Coverage: "best-effort"}}
	req.Logger = cap
	req.LogConsumerCfg = &testcontainers.LogConsumerConfig{Consumers: []testcontainers.LogConsumer{cap}}
	req.LifecycleHooks = append(req.LifecycleHooks, testcontainers.ContainerLifecycleHooks{
		PostCreates: []testcontainers.ContainerHook{func(ctx context.Context, c testcontainers.Container) error {
			return cap.attach(ctx, root, c)
		}},
		// User PostStops run BEFORE TC's default drain. PreTerminates run
		// after Stop and its default log-producer drain, but before removal.
		PreTerminates: []testcontainers.ContainerHook{func(_ context.Context, c testcontainers.Container) error {
			cap.finish(c)
			return nil // A capture failure must not prevent container removal.
		}},
	})
	c, err := create(ctx, req)
	if err != nil {
		cap.problem("startup-error")
		if c != nil {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if cleanupErr := c.Terminate(cleanupCtx); cleanupErr != nil {
				cap.problem("teardown-error")
			}
		}
		cap.finish(c)
	}
	return c, err
}

func (c *capture) attach(ctx context.Context, root *os.Root, container testcontainers.Container) error {
	id := container.GetContainerID()
	if !containerPattern.MatchString(id) || container.SessionID() != c.status.Session {
		return errors.New("seaweed capture: container attribution mismatch")
	}
	info, err := container.Inspect(ctx)
	if err != nil || info == nil || info.Config == nil || info.Config.Labels["org.testcontainers.sessionId"] != c.status.Session {
		return errors.New("seaweed capture: session label not verified")
	}
	c.status.Container = id
	name := "seaweed-" + c.status.Session + "-" + id
	if err := root.Mkdir(name, 0o700); err != nil {
		return errors.New("seaweed capture: cannot exclusively create capture directory")
	}
	c.root, err = root.OpenRoot(name)
	if err != nil {
		return errors.New("seaweed capture: cannot open capture directory")
	}
	c.stdout, err = c.root.OpenFile("stdout.log", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return errors.New("seaweed capture: cannot create stdout file")
	}
	c.stderr, err = c.root.OpenFile("stderr.log", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return errors.New("seaweed capture: cannot create stderr file")
	}
	c.save()
	return nil
}

func (c *capture) problem(code string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.addError(code)
}

func (c *capture) addError(code string) {
	if !slices.Contains(c.status.Errors, code) {
		c.status.Errors = append(c.status.Errors, code)
	}
}

// Printf receives the pinned library's request-local logger. Never forward
// raw startup log dumps (including partial dumps) or interpolated arguments.
// TC reports retry errors and forced-drain warnings here, not on its error
// channel. Normal reconnects can still skip records: coverage is best-effort.
func (c *capture) Printf(format string, _ ...any) {
	switch {
	case strings.Contains(format, "Unexpected error reading logs"):
		c.problem("stream-error")
	case strings.Contains(format, "timeout waiting for log production"):
		c.problem("drain-timeout")
	case strings.Contains(format, "failed accessing container logs"), strings.Contains(format, "failed reading container logs"):
		c.problem("startup-log-read-error")
	}
}

func (c *capture) Accept(log testcontainers.Log) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finished {
		return
	}
	file, count := c.stdout, &c.status.StdoutBytes
	switch log.LogType {
	case testcontainers.StdoutLog:
	case testcontainers.StderrLog:
		file, count = c.stderr, &c.status.StderrBytes
	default:
		c.addError("unknown-stream")
		return
	}
	if file == nil {
		c.addError("missing-file")
		return
	}
	keep := min(len(log.Content), streamLimit-*count)
	if keep < len(log.Content) {
		c.status.Truncated = true
	}
	n, err := file.Write(log.Content[:keep])
	*count += n
	if err != nil || n != keep {
		c.addError("write-error")
	}
}

func (c *capture) finish(container testcontainers.Container) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finished {
		return
	}
	// Accept must remain runnable while waiting for the producer's terminal
	// acknowledgment. The pinned library bounds its own default Stop drain.
	c.mu.Unlock()
	var code string
	var done <-chan error
	if container != nil {
		done = container.GetLogProductionErrorChannel()
	}
	if done == nil {
		code = "missing-producer"
	} else {
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				code = "producer-stop-error"
			}
		case <-time.After(2 * time.Second):
			code = "producer-ack-timeout"
		}
	}
	c.mu.Lock()
	if c.finished {
		return
	}
	c.finished = true
	if code != "" {
		c.addError(code)
	}
	if c.ctx.Err() != nil {
		c.addError("capture-context-canceled")
	}
	if c.status.StdoutBytes+c.status.StderrBytes == 0 {
		c.addError("missing-output")
	}
	for _, f := range []*os.File{c.stdout, c.stderr} {
		if f == nil {
			c.addError("missing-file")
			continue
		}
		if err := f.Sync(); err != nil {
			c.addError("sync-error")
		}
		if err := f.Close(); err != nil {
			c.addError("close-error")
		}
	}
	c.status.Phase = "finalized"
	c.save()
	if c.root != nil {
		_ = c.root.Close()
	}
	c.report()
}

// A pending receipt survives interruption; final status replacement is atomic.
// JSON contains fixed error codes and validated identities, never log contents.
func (c *capture) save() {
	if c.root == nil {
		c.addError("missing-receipt")
		return
	}
	data, err := json.Marshal(c.status)
	if err != nil {
		panic(err)
	} // This fixed receipt has only JSON scalar fields.
	f, err := c.root.OpenFile("status.tmp", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		c.addError("receipt-write-error")
		return
	}
	_, writeErr := f.Write(append(data, '\n'))
	syncErr := f.Sync()
	closeErr := f.Close()
	if errors.Join(writeErr, syncErr, closeErr) != nil {
		c.addError("receipt-write-error")
		return
	}
	if err := c.root.Rename("status.tmp", "status.json"); err != nil {
		c.addError("receipt-write-error")
	}
}

func (c *capture) report() {
	data, err := json.Marshal(c.status)
	if err != nil {
		panic(err)
	}
	fmt.Fprintf(os.Stderr, "SEAWEED_CAPTURE %s\n", data)
}
