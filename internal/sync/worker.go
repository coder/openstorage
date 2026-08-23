package sync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// RepoInfo is the minimal repo identity a sync run needs: the storage key
// (loopback push path element) and the JWT `repo` claim value (event
// repository.url).
type RepoInfo struct {
	RepoID string
	URL    string
}

// RepoLookup resolves a live repo by org + repo_id (production adapter:
// meta.Store.GetRepoByID). Unknown or soft-deleted repos must return an
// error wrapping ErrNotFound; the worker then drops the job silently.
type RepoLookup func(ctx context.Context, org, repoID string) (RepoInfo, error)

// Loopback is the worker's push target: this server's own git smart-HTTP
// endpoint (phase-5 Decision 4 — sync reuses pack storage, ref policies,
// CAS, and push-event emission by doing a real `git push`).
type Loopback struct {
	// BaseURL prefixes push URLs as {BaseURL}/{repo_id}.git. Any scheme git
	// understands works (http(s):// in production, file:// in tests). It
	// must terminate in THIS process when Token mints keys only this
	// process can verify (issue #45): the serve command passes its own
	// bound listener, never the external (load-balanced) base URL.
	BaseURL string
	// Token mints a short-lived git:write JWT for the loopback push (basic
	// auth `t:<jwt>`, docs/explanation/git-protocol-and-namespaces.md). The token is passed to git via GIT_ASKPASS
	// and never logged. May return "" when BaseURL needs no auth (file://).
	Token func(ctx context.Context, org string, repo RepoInfo) (string, error)
}

// defaultRunTimeout caps one sync run end-to-end.
const defaultRunTimeout = 10 * time.Minute

// Worker executes sync runs (docs/reference/webhooks.md lifecycle): claim the run, emit
// repo.sync.started, mirror-fetch the upstream, loopback-push to storage,
// then emit succeeded/failed with the exact documented error strings.
// DELETE /repos/base at any point cancels silently (no failed event).
type Worker struct {
	Upstreams   UpstreamStore
	Credentials CredentialStore
	Repos       RepoLookup
	Events      EventSink
	Loopback    Loopback
	Logger      *slog.Logger
	// Timeout caps one run; 0 means defaultRunTimeout.
	Timeout time.Duration
	// AllowProtocol overrides GIT_ALLOW_PROTOCOL for the run's git
	// transfers; empty means "http:https". Only tests widen it (file://
	// endpoints); production keeps the default so hostile upstream rows
	// can never reach local files, ssh, or transport helpers (ext::).
	AllowProtocol string
}

// allowProtocol resolves the effective GIT_ALLOW_PROTOCOL value.
func (w *Worker) allowProtocol() string {
	if w.AllowProtocol == "" {
		return "http:https"
	}
	return w.AllowProtocol
}

func (w *Worker) assertReady() {
	assertf(w.Upstreams != nil, "Worker.Upstreams is required")
	assertf(w.Credentials != nil, "Worker.Credentials is required")
	assertf(w.Repos != nil, "Worker.Repos is required")
	assertf(w.Events != nil, "Worker.Events is required")
	assertf(w.Loopback.BaseURL != "" && w.Loopback.Token != nil, "Worker.Loopback is required")
	assertf(w.Logger != nil, "Worker.Logger is required")
}

// syncFailure carries the exact documented failure string plus the real
// cause for server-side logs.
type syncFailure struct {
	code  string // one of the Fail* constants
	cause error
}

func (f *syncFailure) Error() string { return f.code + ": " + f.cause.Error() }
func (f *syncFailure) Unwrap() error { return f.cause }

// errDetached signals that DELETE /repos/base raced the run — the
// documented semantics are silent cancellation (no failed event).
var errDetached = errors.New("upstream detached during sync")

// failureCode maps a run error onto the documented repo.sync.failed string.
func failureCode(err error) string {
	assertf(err != nil, "failureCode: nil error")
	var f *syncFailure
	if errors.As(err, &f) {
		return f.code
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return FailGeneric
	}
	return FailInternal
}

// Run executes one sync run for job. It returns an error for logging by the
// queue; documented outcomes (detached, overlapping run, failed sync with an
// emitted event) are not errors to the caller beyond that.
func (w *Worker) Run(ctx context.Context, job Job) error {
	w.assertReady()
	assertf(job.Org != "" && job.RepoID != "", "Run: incomplete job %+v", job)
	timeout := w.Timeout
	if timeout <= 0 {
		timeout = defaultRunTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	repo, err := w.Repos(ctx, job.Org, job.RepoID)
	if errors.Is(err, ErrNotFound) {
		return nil // repo deleted while queued: nothing to sync or emit
	}
	if err != nil {
		return fmt.Errorf("resolve repo %s: %w", job.RepoID, err)
	}
	assertf(repo.RepoID == job.RepoID, "RepoLookup returned %q for %q", repo.RepoID, job.RepoID)

	up, err := w.Upstreams.BeginSync(ctx, job.RepoID)
	switch {
	case errors.Is(err, ErrNotFound):
		return nil // detached before start: cancel silently (docs/reference/webhooks.md)
	case errors.Is(err, ErrSyncInProgress):
		return nil // overlapping run: skip (invariant 11)
	case err != nil:
		return fmt.Errorf("begin sync %s: %w", job.RepoID, err)
	}

	startedAt := eventTime(time.Now())
	isFirst := up.SuccessCount == 0
	base := Event{
		Repository:  EventRepository{ID: repo.RepoID, URL: repo.URL},
		RunCount:    up.RunCount,
		IsFirstSync: isFirst,
		StartedAt:   startedAt,
	}
	started := base
	started.Type = EventSyncStarted
	w.Events.Emit(ctx, job.Org, EventSyncStarted, started.marshal())

	runErr := w.transfer(ctx, job, repo, up)
	if errors.Is(runErr, errDetached) {
		return nil // detach raced the run: the row is gone, cancel silently
	}

	fin, err := w.Upstreams.FinishSync(ctx, job.RepoID, runErr == nil)
	if errors.Is(err, ErrNotFound) {
		return nil // detached mid-run: cancel silently (docs/reference/webhooks.md)
	}
	if err != nil {
		return fmt.Errorf("finish sync %s: %w", job.RepoID, err)
	}

	terminal := base
	terminal.RunCount = fin.RunCount // includes the run that just finished
	terminal.CompletedAt = eventTime(time.Now())
	if runErr == nil {
		terminal.Type = EventSyncSucceeded
		w.Events.Emit(ctx, job.Org, EventSyncSucceeded, terminal.marshal())
		return nil
	}
	terminal.Type = EventSyncFailed
	terminal.Error = failureCode(runErr)
	w.Events.Emit(ctx, job.Org, EventSyncFailed, terminal.marshal())
	w.Logger.Warn("upstream sync failed",
		"org", job.Org, "repo", job.RepoID, "error", terminal.Error, "cause", runErr)
	return runErr
}

// mirrorRefspecs mirror heads and tags; the leading + lets upstream
// force-pushes propagate. Deliberately NOT --mirror on the loopback push:
// that would delete server-side namespace refs absent upstream.
var mirrorRefspecs = []string{"+refs/heads/*:refs/heads/*", "+refs/tags/*:refs/tags/*"}

// transfer performs the fetch + loopback push. A nil return means success;
// errDetached means DELETE /repos/base raced the run; every other failure
// is a *syncFailure carrying the documented error string.
func (w *Worker) transfer(ctx context.Context, job Job, repo RepoInfo, up Upstream) error {
	if up.Owner == "" || up.Name == "" || up.UpstreamHost == "" {
		return &syncFailure{code: FailConfig, cause: fmt.Errorf(
			"incomplete upstream row: owner=%q name=%q host=%q", up.Owner, up.Name, up.UpstreamHost)}
	}

	tmp, err := os.MkdirTemp("", "codestorage-sync-*")
	if err != nil {
		return &syncFailure{code: FailInternal, cause: err}
	}
	defer os.RemoveAll(tmp)
	mirror := filepath.Join(tmp, "mirror.git")

	if stderr, err := runGit(ctx, tmp, hermeticGitEnv(w.allowProtocol()), "init", "--bare", mirror); err != nil {
		return &syncFailure{code: FailInternal, cause: fmt.Errorf("git init: %w: %s", err, stderr)}
	}

	// Upstream credential: absent = public upstream. Username defaults to
	// "git" for token-only providers (docs/reference/http-api.md: username optional).
	username, password := "", ""
	switch cred, err := w.Credentials.CredentialForRepo(ctx, job.RepoID); {
	case err == nil:
		username, password = cred.Username, cred.Password
	case !errors.Is(err, ErrNotFound):
		return &syncFailure{code: FailInternal, cause: fmt.Errorf("load credential: %w", err)}
	}
	if username == "" {
		username = "git"
	}
	fetchEnv, err := authGitEnv(filepath.Join(tmp, "askpass-fetch.sh"), username, password, w.allowProtocol())
	if err != nil {
		return &syncFailure{code: FailInternal, cause: err}
	}
	fetchArgs := append([]string{"fetch", "--prune", up.CloneURL()}, mirrorRefspecs...)
	if stderr, err := runGit(ctx, mirror, fetchEnv, fetchArgs...); err != nil {
		return &syncFailure{code: classifyFetchFailure(stderr), cause: fmt.Errorf("git fetch: %w: %s", err, stderr)}
	}

	// Detach barrier between fetch and the storage push: DELETE /repos/base
	// must cancel in-flight runs without a failed event (docs/reference/webhooks.md).
	if _, err := w.Upstreams.GetUpstream(ctx, job.RepoID); errors.Is(err, ErrNotFound) {
		return errDetached
	} else if err != nil {
		return &syncFailure{code: FailInternal, cause: fmt.Errorf("detach check: %w", err)}
	}

	// Empty upstream: nothing to push, the run still counts as a success.
	refs, stderr, err := runGitOut(ctx, mirror, hermeticGitEnv(w.allowProtocol()), "for-each-ref", "--count=1", "--format=%(refname)")
	if err != nil {
		return &syncFailure{code: FailInternal, cause: fmt.Errorf("git for-each-ref: %w: %s", err, stderr)}
	}
	if strings.TrimSpace(refs) == "" {
		return nil
	}

	token, err := w.Loopback.Token(ctx, job.Org, repo)
	if err != nil {
		return &syncFailure{code: FailInternal, cause: fmt.Errorf("mint loopback token: %w", err)}
	}
	// docs/explanation/git-protocol-and-namespaces.md: git basic auth is username "t", password = JWT.
	pushEnv, err := authGitEnv(filepath.Join(tmp, "askpass-push.sh"), "t", token, w.allowProtocol())
	if err != nil {
		return &syncFailure{code: FailInternal, cause: err}
	}
	// The loopback push is a self-call (Loopback.BaseURL doc): proxy env
	// must never reroute it through a load balancer, where the token could
	// land on a replica that cannot verify it (issue #45). os/exec keeps
	// the last duplicate env entry, so these override inherited settings.
	// The upstream FETCH above keeps proxy env untouched on purpose.
	pushEnv = append(pushEnv, "NO_PROXY=*", "no_proxy=*")
	pushURL := strings.TrimRight(w.Loopback.BaseURL, "/") + "/" + repo.RepoID + ".git"
	pushArgs := append([]string{"push", pushURL}, mirrorRefspecs...)
	if stderr, err := runGit(ctx, mirror, pushEnv, pushArgs...); err != nil {
		return &syncFailure{code: FailPush, cause: fmt.Errorf("git push: %w: %s", err, stderr)}
	}
	return nil
}

// classifyFetchFailure maps git fetch stderr onto the documented failure
// strings. Auth is checked first (an unreachable-looking 403 is still auth),
// then network reachability; everything else is a generic clone failure.
func classifyFetchFailure(stderr string) string {
	s := strings.ToLower(stderr)
	for _, marker := range []string{
		"authentication failed",
		"terminal prompts disabled", // 401 with no credential: git can't prompt
		"invalid username or password",
		"access denied",
		"returned error: 401",
		"returned error: 403",
	} {
		if strings.Contains(s, marker) {
			return FailAuth
		}
	}
	for _, marker := range []string{
		"could not resolve host",
		"connection refused",
		"couldn't connect to server",
		"could not connect to server",
		"failed to connect to",
		"unable to connect",
		"connection timed out",
		"operation timed out",
		"network is unreachable",
		"no route to host",
	} {
		if strings.Contains(s, marker) {
			return FailUnreachable
		}
	}
	return FailClone
}

// hermeticGitEnv is the base environment for worker git invocations:
// prompts disabled, host/global git config masked out (credential helpers
// or URL rewrites there must not leak into sync transfers), and transports
// pinned via GIT_ALLOW_PROTOCOL — the fetch URL derives from an
// attacker-controlled upstream_host, so file://, ssh://, and transport
// helpers (ext::) must be unreachable even if a hostile row slips past the
// API-layer validation.
func hermeticGitEnv(allowProtocol string) []string {
	assertf(allowProtocol != "", "hermeticGitEnv: empty allowProtocol")
	return append(os.Environ(),
		"GIT_TERMINAL_PROMPT=0",
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_CONFIG_SYSTEM=/dev/null",
		"GIT_ALLOW_PROTOCOL="+allowProtocol,
	)
}

// authGitEnv extends hermeticGitEnv with a GIT_ASKPASS helper feeding the
// given basic-auth credentials. Secrets travel via the child process
// environment only — never argv (visible in ps) and never logs. An empty
// password installs no helper (public upstream / file:// loopback).
func authGitEnv(scriptPath, username, password, allowProtocol string) ([]string, error) {
	env := hermeticGitEnv(allowProtocol)
	if password == "" {
		return env, nil
	}
	assertf(username != "", "authGitEnv: password without username")
	const script = `#!/bin/sh
case "$1" in
[Uu]sername*) printf '%s\n' "$CODESTORAGE_ASKPASS_USERNAME" ;;
*) printf '%s\n' "$CODESTORAGE_ASKPASS_PASSWORD" ;;
esac
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o700); err != nil {
		return nil, fmt.Errorf("write askpass helper: %w", err)
	}
	return append(env,
		"GIT_ASKPASS="+scriptPath,
		"CODESTORAGE_ASKPASS_USERNAME="+username,
		"CODESTORAGE_ASKPASS_PASSWORD="+password,
	), nil
}

// runGit executes one git command, returning its stderr for failure
// classification. Credentials never appear in argv or stderr (askpass).
func runGit(ctx context.Context, dir string, env []string, args ...string) (stderr string, err error) {
	_, stderr, err = runGitOut(ctx, dir, env, args...)
	return stderr, err
}

func runGitOut(ctx context.Context, dir string, env []string, args ...string) (stdout, stderr string, err error) {
	assertf(dir != "" && len(args) > 0, "runGit: missing dir/args")
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	cmd.Env = env
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf
	cmd.Stdin = bytes.NewReader(nil)
	err = cmd.Run()
	if err != nil && ctx.Err() != nil {
		err = fmt.Errorf("%w (%v)", ctx.Err(), err)
	}
	return outBuf.String(), errBuf.String(), err
}
