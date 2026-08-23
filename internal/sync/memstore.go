package sync

import (
	"context"
	"fmt"
	"os"
	"strconv"
	stdsync "sync"
	"time"
)

// LegacyClaimHolder mirrors meta.LegacyClaimHolder (the sync package must
// not import meta): a claim adopted from a pre-lease writer under a grace.
const LegacyClaimHolder = "!legacy-grace"

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

func (m *MemStore) RequestSync(_ context.Context, repoID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	up, ok := m.upstreams[repoID]
	if !ok {
		return ErrNotFound
	}
	up.SyncPending = true
	m.upstreams[repoID] = up
	return nil
}

func (m *MemStore) BeginSync(_ context.Context, repoID, holder string, lease time.Duration) (Upstream, string, error) {
	assertf(holder != "", "BeginSync: empty holder")
	assertf(lease > 0, "BeginSync: non-positive lease %v", lease)
	m.mu.Lock()
	defer m.mu.Unlock()
	up, ok := m.upstreams[repoID]
	if !ok {
		return Upstream{}, "", ErrNotFound
	}
	stolenFrom := ""
	if up.Syncing {
		if time.Now().Before(up.SyncClaimExpiresAt) {
			return Upstream{}, "", ErrSyncInProgress
		}
		if up.SyncClaimHolder == "" {
			// Legacy claim (pre-lease writer, mixed fleet): adopt with a
			// full grace instead of stealing what may be a live run
			// (meta.LegacyClaimHolder semantics, issue #86).
			up.SyncClaimHolder, up.SyncClaimExpiresAt = LegacyClaimHolder, time.Now().Add(lease)
			m.upstreams[repoID] = up
			return Upstream{}, "", ErrSyncInProgress
		}
		if up.SyncClaimHolder == LegacyClaimHolder {
			// Stealing an adopted legacy claim needs the operator's
			// drained-fleet assertion (meta.EnvSyncStealLegacy).
			if v, err := strconv.ParseBool(os.Getenv("CODESTORAGE_SYNC_STEAL_LEGACY")); err != nil || !v {
				return Upstream{}, "", ErrSyncInProgress
			}
		}
		stolenFrom = up.SyncClaimHolder // expired lease (or lapsed grace) stolen (issue #86)
	}
	// The claim consumes the pending marker: this run's upstream fetch
	// postdates every request marked before the claim.
	up.Syncing, up.SyncPending = true, false
	up.SyncClaimHolder, up.SyncClaimExpiresAt = holder, time.Now().Add(lease)
	m.upstreams[repoID] = up
	return up, stolenFrom, nil
}

func (m *MemStore) FinishSync(_ context.Context, repoID, holder string, success bool) (Upstream, error) {
	assertf(holder != "", "FinishSync: empty holder")
	m.mu.Lock()
	defer m.mu.Unlock()
	up, ok := m.upstreams[repoID]
	if !ok {
		return Upstream{}, ErrNotFound
	}
	if !up.Syncing {
		// No run in flight: a late finisher whose stolen claim was already
		// released by the thief (issue #86) — same answer as the durable
		// backends' `AND syncing` guard.
		return Upstream{}, fmt.Errorf("no run in flight for %q: %w", repoID, ErrNotFound)
	}
	if up.SyncClaimHolder != holder {
		return Upstream{}, fmt.Errorf("now held by %q: %w", up.SyncClaimHolder, ErrClaimLost)
	}
	up.Syncing = false
	up.SyncClaimHolder, up.SyncClaimExpiresAt = "", time.Time{}
	up.RunCount++
	if success {
		up.SuccessCount++
	}
	m.upstreams[repoID] = up
	return up, nil
}
