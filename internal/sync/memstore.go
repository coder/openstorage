package sync

import (
	"context"
	stdsync "sync"
	"time"
)

// MemStore is an in-memory CredentialStore + UpstreamStore: the test
// double and the local-mode stand-in until the durable meta-backed rows
// land (foundation slice). Safe for concurrent use.
type MemStore struct {
	mu        stdsync.Mutex
	creds     map[string]Credential // by ID
	credRepos map[string]string     // repoID -> credential ID (one per repo)
	upstreams map[string]Upstream   // by repoID
}

var (
	_ CredentialStore = (*MemStore)(nil)
	_ UpstreamStore   = (*MemStore)(nil)
)

func NewMemStore() *MemStore {
	return &MemStore{
		creds:     make(map[string]Credential),
		credRepos: make(map[string]string),
		upstreams: make(map[string]Upstream),
	}
}

func (m *MemStore) CreateCredential(_ context.Context, cred Credential) (Credential, error) {
	assertf(cred.RepoID != "", "CreateCredential: empty RepoID")
	assertf(cred.ID == "", "CreateCredential: caller must not assign IDs (got %q)", cred.ID)
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, taken := m.credRepos[cred.RepoID]; taken {
		return Credential{}, ErrCredentialExists
	}
	cred.ID = NewCredentialID()
	cred.CreatedAt = time.Now().UTC()
	m.creds[cred.ID] = cred
	m.credRepos[cred.RepoID] = cred.ID
	return cred, nil
}

func (m *MemStore) GetCredential(_ context.Context, id string) (Credential, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cred, ok := m.creds[id]
	if !ok {
		return Credential{}, ErrNotFound
	}
	return cred, nil
}

func (m *MemStore) CredentialMeta(_ context.Context, id string) (Credential, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cred, ok := m.creds[id]
	if !ok {
		return Credential{}, ErrNotFound
	}
	cred.Password = ""
	return cred, nil
}

func (m *MemStore) UpdateCredential(_ context.Context, id, username, password string) (Credential, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cred, ok := m.creds[id]
	if !ok {
		return Credential{}, ErrNotFound
	}
	cred.Username = username
	cred.Password = password
	m.creds[id] = cred
	return cred, nil
}

func (m *MemStore) DeleteCredential(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cred, ok := m.creds[id]
	if !ok {
		return ErrNotFound
	}
	delete(m.creds, id)
	delete(m.credRepos, cred.RepoID)
	return nil
}

func (m *MemStore) CredentialForRepo(_ context.Context, repoID string) (Credential, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id, ok := m.credRepos[repoID]
	if !ok {
		return Credential{}, ErrNotFound
	}
	cred, ok := m.creds[id]
	assertf(ok, "credRepos points at missing credential %q", id)
	return cred, nil
}

func (m *MemStore) PutUpstream(_ context.Context, up Upstream) (Upstream, error) {
	assertf(up.RepoID != "", "PutUpstream: empty RepoID")
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, taken := m.upstreams[up.RepoID]; taken {
		return Upstream{}, ErrUpstreamExists
	}
	up.CreatedAt = time.Now().UTC()
	m.upstreams[up.RepoID] = up
	return up, nil
}

func (m *MemStore) GetUpstream(_ context.Context, repoID string) (Upstream, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	up, ok := m.upstreams[repoID]
	if !ok {
		return Upstream{}, ErrNotFound
	}
	return up, nil
}

func (m *MemStore) DeleteUpstream(_ context.Context, repoID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.upstreams[repoID]; !ok {
		return ErrNotFound
	}
	delete(m.upstreams, repoID)
	return nil
}

func (m *MemStore) BeginSync(_ context.Context, repoID string) (Upstream, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	up, ok := m.upstreams[repoID]
	if !ok {
		return Upstream{}, ErrNotFound
	}
	if up.Syncing {
		return Upstream{}, ErrSyncInProgress
	}
	up.Syncing = true
	m.upstreams[repoID] = up
	return up, nil
}

func (m *MemStore) FinishSync(_ context.Context, repoID string, success bool) (Upstream, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	up, ok := m.upstreams[repoID]
	if !ok {
		return Upstream{}, ErrNotFound
	}
	assertf(up.Syncing, "FinishSync without BeginSync (repo %q)", repoID)
	up.Syncing = false
	up.RunCount++
	if success {
		up.SuccessCount++
	}
	m.upstreams[repoID] = up
	return up, nil
}
