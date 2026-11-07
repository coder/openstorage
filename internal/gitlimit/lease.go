package gitlimit

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

// LeaseFile is the name of a cache repository's lease file, <repo>/LeaseFile
// (issue #490). Every git child that serve starts in a cache repository
// holds a shared flock on it, as its inherited file descriptor LeaseFD, for as
// long as the child's process tree lives: git, its hooks and their children
// all keep the descriptor. So winning the lock exclusively (TryExclusive) proves
// that no git that serve started for the repository is alive, and none can
// own one of its ref locks. The kernel arbitrates the lock across users, PID
// namespaces and containers on the same host, and an orphan of a crashed
// serve keeps holding it. The file is never removed.
//
// Coverage is the whole proof: a git child in a cache repository that was
// started without the lease can own a ref lock that cleanup then removes. So
// every such child starts through StartIn, RunIn, OutputIn or
// CombinedOutputIn, or attaches a Lease itself (Lease.Attach). The audit test
// in this package fails on a new git start that does neither, and a binary
// built with the openstorage_viewcheck tag panics on a start that runs in a
// repository without a lease. Git that serve did not start holds no lease;
// the docs tell operators not to run it in a cache repository while serve
// runs, and not to reassign descriptor LeaseFD in a hook.
const LeaseFile = "codestorage.gitlease.lock"

// EpochFile is the name of the lease epoch file, <repo>/EpochFile. The first
// process that opens a repository's lease creates it, empty and with
// create-exclusive, so its modification time is the moment a lease-aware
// process first started git there. A ref lock that is older was made by a git
// that may hold no lease (a binary from before the lease, or git run by hand),
// and cleanup keeps it (#490). The file is never removed.
const EpochFile = "codestorage.gitlease.epoch"

// LeaseFD is the descriptor a git child holds the lease on. Git reads the
// descriptors 2 to 9 as trace targets (GIT_TRACE=3 writes to descriptor 3),
// so the lease sits above them, and the descriptors 3 to LeaseFD-1 are closed
// in the child.
const LeaseFD = 10

// ErrLeaseBusy: the lease is held exclusively (or, for TryExclusive, shared).
var ErrLeaseBusy = errors.New("git lease is busy")

// ErrNoEpoch: the repository has no lease epoch file.
var ErrNoEpoch = errors.New("git lease epoch is missing")

const (
	// leaseWait bounds how long a git start waits for an exclusive holder.
	// An exclusive holder runs for the milliseconds of one file move, so a
	// start that waits this long is stuck behind a hung one and fails.
	leaseWait = 30 * time.Second
	leasePoll = 5 * time.Millisecond
)

// checkUnleased and checkLeased are set only by the openstorage_viewcheck
// build tag (leasecheck_viewcheck.go): checkUnleased sees every Start that
// takes no lease, and checkLeased every StartIn.
var (
	checkUnleased func(cmd *exec.Cmd)
	checkLeased   func(repoPath string, cmd *exec.Cmd)
)

// Lease is an open lease file with a flock on it. Close releases the lock
// unless a child that inherited the descriptor still holds it.
type Lease struct {
	f      *os.File
	shared bool
}

// OpenLease takes a shared lease on the repository at repoPath, creating the
// lease file if it is missing. It waits (bounded) while the lease is held
// exclusively. It fails with ctx's error once ctx ends, with ErrStopping once
// StopChildren began, and with ErrLeaseBusy if the wait does not end.
func OpenLease(ctx context.Context, repoPath string) (*Lease, error) {
	assertf(repoPath != "", "OpenLease: empty repoPath")
	if err := ensureEpoch(repoPath); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(repoPath, LeaseFile), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open git lease: %w", err)
	}
	deadline := time.Now().Add(leaseWait)
	for {
		err := flock(f, syscall.LOCK_SH)
		if err == nil {
			return &Lease{f: f, shared: true}, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			_ = f.Close()
			return nil, fmt.Errorf("lock git lease: %w", err)
		}
		switch {
		case ctx.Err() != nil:
			err = context.Cause(ctx)
		case Stopping():
			err = ErrStopping
		case time.Now().After(deadline):
			err = ErrLeaseBusy
		default:
			select {
			case <-ctx.Done():
			case <-time.After(leasePoll):
			}
			continue
		}
		_ = f.Close()
		return nil, err
	}
}

// TryExclusive takes the lease exclusively without waiting. It succeeds only
// when no process holds the lease, shared or exclusive: no git that serve
// started for the repository is alive. Until Close, StartIn and OpenLease
// wait. It returns ErrLeaseBusy when the lease is held.
func TryExclusive(repoPath string) (*Lease, error) {
	assertf(repoPath != "", "TryExclusive: empty repoPath")
	if err := ensureEpoch(repoPath); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(repoPath, LeaseFile), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open git lease: %w", err)
	}
	if err := flock(f, syscall.LOCK_EX); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrLeaseBusy
		}
		return nil, fmt.Errorf("lock git lease: %w", err)
	}
	return &Lease{f: f}, nil
}

// ensureEpoch creates the epoch file when it is missing.
func ensureEpoch(repoPath string) error {
	f, err := os.OpenFile(filepath.Join(repoPath, EpochFile), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	switch {
	case err == nil:
		return f.Close()
	case errors.Is(err, fs.ErrExist):
		return nil
	}
	return fmt.Errorf("create git lease epoch: %w", err)
}

// LeaseEpoch returns the lease epoch of the repository at repoPath: the
// modification time of its epoch file (see EpochFile). It fails with
// ErrNoEpoch when the file is missing, and with another error when it is not
// a regular file.
func LeaseEpoch(repoPath string) (time.Time, error) {
	fi, err := os.Lstat(filepath.Join(repoPath, EpochFile))
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return time.Time{}, ErrNoEpoch
	case err != nil:
		return time.Time{}, fmt.Errorf("read git lease epoch: %w", err)
	case !fi.Mode().IsRegular():
		return time.Time{}, fmt.Errorf("git lease epoch is not a regular file: %s", fi.Mode())
	}
	return fi.ModTime(), nil
}

// Attach hands the lease to cmd as its descriptor LeaseFD. The caller starts
// cmd itself and closes the Lease after Start: the child keeps its own copy,
// so the lock lives exactly as long as the child's process tree. A Lease may
// be attached to several commands.
func (l *Lease) Attach(cmd *exec.Cmd) {
	assertf(l.shared, "Lease.Attach: not a shared lease")
	assertf(len(cmd.ExtraFiles) == 0, "Lease.Attach: cmd already has ExtraFiles")
	// Entry i becomes descriptor 3+i, and a nil entry leaves it closed.
	cmd.ExtraFiles = make([]*os.File, LeaseFD-3+1)
	cmd.ExtraFiles[LeaseFD-3] = l.f
}

// Close closes the parent's copy of the lease file.
func (l *Lease) Close() error { return l.f.Close() }

// flock tries one flock operation without blocking.
func flock(f *os.File, how int) error {
	rc, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var ferr error
	if err := rc.Control(func(fd uintptr) {
		for {
			ferr = syscall.Flock(int(fd), how|syscall.LOCK_NB)
			if ferr != syscall.EINTR {
				return
			}
		}
	}); err != nil {
		return err
	}
	return ferr
}

// StartIn is Start for a git child that runs in the cache repository at
// repoPath: cmd inherits a shared lease as descriptor LeaseFD (see LeaseFile).
// ctx bounds the wait for the lease (see OpenLease), not the child.
func StartIn(ctx context.Context, repoPath string, cmd *exec.Cmd) error {
	if checkLeased != nil {
		checkLeased(repoPath, cmd)
	}
	l, err := OpenLease(ctx, repoPath)
	if err != nil {
		return err
	}
	defer l.Close()
	l.Attach(cmd)
	return children.start(cmd)
}

// RunIn is StartIn followed by Wait.
func RunIn(ctx context.Context, repoPath string, cmd *exec.Cmd) error {
	if err := StartIn(ctx, repoPath, cmd); err != nil {
		return err
	}
	return Wait(cmd)
}

// OutputIn is exec.Cmd.Output through the registry and the lease of the
// repository at repoPath. cmd.Stdout must be nil.
func OutputIn(ctx context.Context, repoPath string, cmd *exec.Cmd) ([]byte, error) {
	assertf(cmd.Stdout == nil, "gitlimit.OutputIn: Stdout already set")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := RunIn(ctx, repoPath, cmd)
	return out.Bytes(), err
}

// CombinedOutputIn is exec.Cmd.CombinedOutput through the registry and the
// lease of the repository at repoPath.
func CombinedOutputIn(ctx context.Context, repoPath string, cmd *exec.Cmd) ([]byte, error) {
	assertf(cmd.Stdout == nil && cmd.Stderr == nil, "gitlimit.CombinedOutputIn: Stdout or Stderr already set")
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := RunIn(ctx, repoPath, cmd)
	return out.Bytes(), err
}
