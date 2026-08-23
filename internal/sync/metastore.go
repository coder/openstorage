package sync

import (
	"context"
	"errors"
	"fmt"

	"github.com/coder/code-storage/internal/meta"
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

var _ UpstreamStore = metaUpstreams{}

func fromMetaUpstream(u meta.RepoUpstream) Upstream {
	return Upstream{
		RepoID: u.RepoID, Provider: u.Provider, Owner: u.Owner, Name: u.Name,
		UpstreamHost: u.UpstreamHost, RunCount: u.RunCount, SuccessCount: u.SuccessCount,
		Syncing: u.Syncing, SyncPending: u.SyncPending, CreatedAt: u.CreatedAt,
	}
}

func (s metaUpstreams) PutUpstream(ctx context.Context, up Upstream) (Upstream, error) {
	row, err := s.ms.SetRepoUpstream(ctx, meta.RepoUpstream{
		RepoID: up.RepoID, Provider: up.Provider, Owner: up.Owner, Name: up.Name,
		UpstreamHost: up.UpstreamHost,
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

func (s metaUpstreams) BeginSync(ctx context.Context, repoID string) (Upstream, error) {
	row, claimed, err := s.ms.TryBeginSync(ctx, repoID)
	if err != nil {
		return Upstream{}, mapMetaErr(err, ErrNotFound, nil)
	}
	if !claimed {
		return Upstream{}, fmt.Errorf("%w: repo %s", ErrSyncInProgress, repoID)
	}
	return fromMetaUpstream(row), nil
}

func (s metaUpstreams) FinishSync(ctx context.Context, repoID string, success bool) (Upstream, error) {
	row, err := s.ms.FinishSync(ctx, repoID, success)
	if err != nil {
		return Upstream{}, mapMetaErr(err, ErrNotFound, nil)
	}
	return fromMetaUpstream(row), nil
}
