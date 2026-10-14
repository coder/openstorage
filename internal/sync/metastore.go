package sync

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/coder/openstorage/internal/meta"
)

// MetaStores adapts the durable meta.EventingStore rows to this package's
// CredentialStore/UpstreamStore contracts: field-for-field type translation
// plus sentinel mapping (meta.ErrNotFound/ErrAlreadyExists onto the sync
// sentinels handlers and the worker match on).
func MetaStores(ms meta.EventingStore) (CredentialStore, UpstreamStore) {
	assertf(ms != nil, "MetaStores: nil meta store")
	return metaCredentials{ms}, metaUpstreams{ms}
}

// mapMetaErr rewraps meta sentinels as the given sync sentinels, keeping
// the original message for logs.
func mapMetaErr(err, notFound, exists error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, meta.ErrNotFound):
		return fmt.Errorf("%w: %v", notFound, err)
	case exists != nil && errors.Is(err, meta.ErrAlreadyExists):
		return fmt.Errorf("%w: %v", exists, err)
	default:
		return err
	}
}

type metaCredentials struct{ ms meta.EventingStore }

var _ CredentialStore = metaCredentials{}

func fromMetaCredential(c meta.GitCredential) Credential {
	return Credential{ID: c.ID, RepoID: c.RepoID, Username: c.Username, Password: c.Password, CreatedAt: c.CreatedAt}
}

func (s metaCredentials) CreateCredential(ctx context.Context, cred Credential) (Credential, error) {
	row, err := s.ms.CreateGitCredential(ctx, meta.GitCredential{
		RepoID: cred.RepoID, Username: cred.Username, Password: cred.Password,
	})
	if err != nil {
		return Credential{}, mapMetaErr(err, ErrNotFound, ErrCredentialExists)
	}
	return fromMetaCredential(row), nil
}

func (s metaCredentials) GetCredential(ctx context.Context, id string) (Credential, error) {
	row, err := s.ms.GetGitCredential(ctx, id)
	if err != nil {
		return Credential{}, mapMetaErr(err, ErrNotFound, nil)
	}
	return fromMetaCredential(row), nil
}

func (s metaCredentials) CredentialMeta(ctx context.Context, id string) (Credential, error) {
	row, err := s.ms.GitCredentialMeta(ctx, id)
	if err != nil {
		return Credential{}, mapMetaErr(err, ErrNotFound, nil)
	}
	assertf(row.Password == "", "GitCredentialMeta returned a password for %q", id)
	return fromMetaCredential(row), nil
}

func (s metaCredentials) UpdateCredential(ctx context.Context, id, username, password string) (Credential, error) {
	row, err := s.ms.UpdateGitCredential(ctx, id, username, password)
	if err != nil {
		return Credential{}, mapMetaErr(err, ErrNotFound, nil)
	}
	return fromMetaCredential(row), nil
}

func (s metaCredentials) DeleteCredential(ctx context.Context, id string) error {
	return mapMetaErr(s.ms.DeleteGitCredential(ctx, id), ErrNotFound, nil)
}

func (s metaCredentials) CredentialForRepo(ctx context.Context, repoID string) (Credential, error) {
	row, err := s.ms.GitCredentialForRepo(ctx, repoID)
	if err != nil {
		return Credential{}, mapMetaErr(err, ErrNotFound, nil)
	}
	return fromMetaCredential(row), nil
}

type metaUpstreams struct{ ms meta.EventingStore }

var (
	_ UpstreamStore    = metaUpstreams{}
	_ ShutdownRestorer = metaUpstreams{}
)

func fromMetaUpstream(u meta.RepoUpstream) Upstream {
	return Upstream{
		RepoID: u.RepoID, Provider: u.Provider, Owner: u.Owner, Name: u.Name,
		UpstreamHost: u.UpstreamHost, RunCount: u.RunCount, SuccessCount: u.SuccessCount,
		Syncing: u.Syncing, SyncPending: u.SyncPending,
		SyncClaimHolder: u.SyncClaimHolder, SyncClaimExpiresAt: u.SyncClaimExpiresAt,
		ForwardOnPush: u.ForwardOnPush, PushPending: u.PushPending,
		PushSyncing: u.PushSyncing, PushClaimHolder: u.PushClaimHolder,
		PushClaimExpiresAt: u.PushClaimExpiresAt, PushClaimForce: u.PushClaimForce,
		PushRunCount: u.PushRunCount, PushSuccessCount: u.PushSuccessCount,
		CreatedAt: u.CreatedAt,
	}
}

func (s metaUpstreams) PutUpstream(ctx context.Context, up Upstream) (Upstream, error) {
	row, err := s.ms.SetRepoUpstream(ctx, meta.RepoUpstream{
		RepoID: up.RepoID, Provider: up.Provider, Owner: up.Owner, Name: up.Name,
		UpstreamHost: up.UpstreamHost, ForwardOnPush: up.ForwardOnPush,
	})
	if err != nil {
		return Upstream{}, mapMetaErr(err, ErrNotFound, ErrUpstreamExists)
	}
	return fromMetaUpstream(row), nil
}

func (s metaUpstreams) GetUpstream(ctx context.Context, repoID string) (Upstream, error) {
	row, err := s.ms.GetRepoUpstream(ctx, repoID)
	if err != nil {
		return Upstream{}, mapMetaErr(err, ErrNotFound, nil)
	}
	return fromMetaUpstream(row), nil
}

func (s metaUpstreams) DeleteUpstream(ctx context.Context, repoID string) error {
	return mapMetaErr(s.ms.DeleteRepoUpstream(ctx, repoID), ErrNotFound, nil)
}

func (s metaUpstreams) RequestSync(ctx context.Context, repoID string) error {
	return mapMetaErr(s.ms.RequestSync(ctx, repoID), ErrNotFound, nil)
}

func (s metaUpstreams) BeginSync(ctx context.Context, repoID, holder string, lease time.Duration) (Upstream, string, error) {
	row, claimed, stolenFrom, err := s.ms.TryBeginSync(ctx, repoID, holder, lease)
	if err != nil {
		return Upstream{}, "", mapMetaErr(err, ErrNotFound, nil)
	}
	if !claimed {
		return Upstream{}, "", fmt.Errorf("%w: repo %s", ErrSyncInProgress, repoID)
	}
	return fromMetaUpstream(row), stolenFrom, nil
}

func (s metaUpstreams) FinishSync(ctx context.Context, repoID, holder string, success bool) (Upstream, error) {
	row, err := s.ms.FinishSync(ctx, repoID, holder, success)
	if errors.Is(err, meta.ErrClaimLost) {
		return Upstream{}, fmt.Errorf("%w: %v", ErrClaimLost, err)
	}
	if err != nil {
		return Upstream{}, mapMetaErr(err, ErrNotFound, nil)
	}
	return fromMetaUpstream(row), nil
}

func (s metaUpstreams) RequestPush(ctx context.Context, repoID string, force bool) error {
	return mapMetaErr(s.ms.RequestPush(ctx, repoID, force), ErrNotFound, nil)
}

// RequestPushNow records an explicit push request (API push-upstream). On
// the local schedule (ADR-50: SQLite, S3WAL) the same write makes a
// waiting retry due now and keeps its retry position. PostgreSQL has no
// due time, so it is RequestPush there.
func (s metaUpstreams) RequestPushNow(ctx context.Context, repoID string, force bool) error {
	if ls, ok := meta.LocalSchedule(s.ms); ok {
		return mapMetaErr(ls.RequestPushNow(ctx, repoID, force), ErrNotFound, nil)
	}
	return s.RequestPush(ctx, repoID, force)
}

func (s metaUpstreams) BeginPush(ctx context.Context, repoID, holder string, lease time.Duration) (Upstream, bool, string, error) {
	row, claimed, force, stolenFrom, err := s.ms.TryBeginPush(ctx, repoID, holder, lease)
	if err != nil {
		return Upstream{}, false, "", mapMetaErr(err, ErrNotFound, nil)
	}
	if !claimed {
		return Upstream{}, false, "", fmt.Errorf("%w: repo %s", ErrPushInProgress, repoID)
	}
	return fromMetaUpstream(row), force, stolenFrom, nil
}

// ShutdownRestorer ends a run that serve's shutdown cancelled without
// counting it (issue #343, PostgreSQL): the claim is released and the
// request (for push, the consumed level) re-marked. MemStore and PG-backed
// stores implement it; SQLite and S3WAL restore through LocalLanes instead.
type ShutdownRestorer interface {
	RestoreSync(ctx context.Context, repoID, holder string) (Upstream, error)
	RestorePush(ctx context.Context, repoID, holder string, force bool) (Upstream, error)
}

var errRestoreUnsupported = errors.New("metadata store cannot restore shutdown-cancelled runs")

func (s metaUpstreams) RestoreSync(ctx context.Context, repoID, holder string) (Upstream, error) {
	rs, ok := s.ms.(meta.RunRestoreStore)
	if !ok {
		return Upstream{}, fmt.Errorf("restore sync %s: %T: %w", repoID, s.ms, errRestoreUnsupported)
	}
	return mapFinishErr(rs.RestoreSync(ctx, repoID, holder))
}

func (s metaUpstreams) RestorePush(ctx context.Context, repoID, holder string, force bool) (Upstream, error) {
	rs, ok := s.ms.(meta.RunRestoreStore)
	if !ok {
		return Upstream{}, fmt.Errorf("restore push %s: %T: %w", repoID, s.ms, errRestoreUnsupported)
	}
	return mapFinishErr(rs.RestorePush(ctx, repoID, holder, force))
}

func mapFinishErr(row meta.RepoUpstream, err error) (Upstream, error) {
	if errors.Is(err, meta.ErrClaimLost) {
		return Upstream{}, fmt.Errorf("%w: %v", ErrClaimLost, err)
	}
	if err != nil {
		return Upstream{}, mapMetaErr(err, ErrNotFound, nil)
	}
	return fromMetaUpstream(row), nil
}

func (s metaUpstreams) FinishPush(ctx context.Context, repoID, holder string, success bool) (Upstream, error) {
	row, err := s.ms.FinishPush(ctx, repoID, holder, success)
	if errors.Is(err, meta.ErrClaimLost) {
		return Upstream{}, fmt.Errorf("%w: %v", ErrClaimLost, err)
	}
	if err != nil {
		return Upstream{}, mapMetaErr(err, ErrNotFound, nil)
	}
	return fromMetaUpstream(row), nil
}
