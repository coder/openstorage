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
// repository without a lease. Git that serve did not start holds no lease,
// so operators must not run it in a cache repository while serve runs, and
// must not reassign descriptor LeaseFD in a hook. The release that first
// takes the lease exclusively (the stale ref lock cleanup of #490) writes
// those rules into docs/how-to/deploy-production.md.
const LeaseFile = "codestorage.gitlease.lock"

// EpochFile is the name of the lease epoch file, <repo>/EpochFile. The code
// that creates a cache repository's directory writes it, empty, right after
// it made the directory and before git runs there (CreateEpoch), so the file
// marks a repository that a lease-aware serve created: no git of an older
// release ran in it before, and every git that serve starts in it takes the
// lease. A repository without the file was created by an older release, or
// its creation was interrupted, and an older release's git, which holds no
// lease, can still run there: cleanup keeps every lock in it (#490). The
// file's modification time is the moment of creation, and cleanup also keeps
// a ref lock that is not newer. OpenLease and TryExclusive never write it:
// a late epoch in an existing directory would make a lock of an older
// release's git look newer than the epoch. The file is never removed.
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

// leaseWait bounds how long a git start waits for an exclusive holder. An
// exclusive holder runs for the milliseconds of one file move, so a start
// that waits this long is stuck behind a hung one and fails. A variable so
// that a test can shorten it.
var leaseWait = 30 * time.Second

const leasePoll = 5 * time.Millisecond

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
	f      *os.File // nil: the repository directory is read-only and has no lease file
	shared bool
}

// OpenLease takes a shared lease on the repository at repoPath, creating the
// lease file if it is missing. A repository directory that serve cannot
// write, and that has no lease file, gets a lease that holds nothing: no git
// of serve can take a ref lock there, and a refusal would only break the
// reads that work there. It waits (bounded) while the lease is held
// exclusively. It fails with ctx's error once ctx ends, with ErrStopping once
// StopChildren began, and with ErrLeaseBusy if the wait does not end.
func OpenLease(ctx context.Context, repoPath string) (*Lease, error) {
	assertf(repoPath != "", "OpenLease: empty repoPath")
	path := filepath.Join(repoPath, LeaseFile)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil && (errors.Is(err, fs.ErrPermission) || errors.Is(err, syscall.EROFS)) {
		var openErr error
		if f, openErr = os.OpenFile(path, os.O_RDWR, 0); errors.Is(openErr, fs.ErrNotExist) {
			return &Lease{shared: true}, nil
		} else if openErr == nil {
			err = nil
		}
	}
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

// CreateEpoch writes the epoch file of the cache repository at repoPath, which
// the caller has just created (see EpochFile). It fails when the file exists.
func CreateEpoch(repoPath string) error {
	assertf(repoPath != "", "CreateEpoch: empty repoPath")
	f, err := os.OpenFile(filepath.Join(repoPath, EpochFile), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create git lease epoch: %w", err)
	}
	return f.Close()
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
	if l.f == nil {
		return
	}
	// Entry i becomes descriptor 3+i, and a nil entry leaves it closed.
	cmd.ExtraFiles = make([]*os.File, LeaseFD-3+1)
	cmd.ExtraFiles[LeaseFD-3] = l.f
}

// Close closes the parent's copy of the lease file.
func (l *Lease) Close() error {
	if l.f == nil {
		return nil
	}
	return l.f.Close()
}

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
