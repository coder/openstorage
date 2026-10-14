// s3wal meta backend conformance (issue #133 Phase 3): the SAME test
// bodies the pg/sqlite lanes run via runBackends are fanned out over the
// s3wal backend on the in-memory MemCAS substrate — the parity oracle for
// every method group, the claim-lease expiry/steal/late-finish semantics,
// and the cursor pagination shapes. TestSyncClaimLegacyAdoption is the one
// deliberate omission: it reproduces PRE-LEASE SQL replica writes with raw
// SQL surgery, and no such binary can ever have written an s3wal meta
// document (the backend asserts those claim shapes impossible instead of
// adopting them — see s3walmeta_eventing.go).
//
// Backend-specific coverage lives alongside: the injected-clock lease
// test mirrors sqlite_clock_test.go, and the UTF-8 write rejection is the
// s3wal-only codec-boundary guard.

package meta

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/ref/s3wal"
)

// TestS3WALMetaDecode isolates format rejection from the other document guards.
func TestS3WALMetaDecode(t *testing.T) {
	const fields = `"seq":1,"writer":"0123456789abcdef0123456789abcdef","next_org_id":0,"next_key_id":0}`
	for _, tc := range []struct{ name, body, wantErr string }{
		{"malformed JSON", `{`, "s3walmeta: document decode:"},
		{"missing version", `{` + fields, "s3walmeta: document version 0 unsupported (want 2)"},
		{"zero version", `{"v":0,` + fields, "s3walmeta: document version 0 unsupported (want 2)"},
		{"v1 needs conversion", `{"v":1,` + fields, "s3walmeta: document version 1 needs conversion: stop serve, then run `openstorage s3wal-meta convert --to 2`"},
		{"foreign version", `{"v":3,` + fields, "s3walmeta: document version 3 unsupported (want 2)"},
		{"valid v2", `{"v":2,` + fields, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc, err := decodeS3WALMetaDoc([]byte(tc.body))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("decode error = %v, want %q", err, tc.wantErr)
				}
				if doc != nil {
					t.Fatalf("rejected document = %+v, want nil", doc)
				}
				// Only version rejections are typed (issue #445): the hook
				// refuses on them and fails open on everything else.
				var fv *FormatVersionError
				if typed, want := errors.As(err, &fv), strings.Contains(tc.wantErr, "version"); typed != want {
					t.Fatalf("decode error %v: typed = %v, want %v", err, typed, want)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if doc == nil || doc.V != 2 || doc.Seq != 1 || doc.Writer != "0123456789abcdef0123456789abcdef" || doc.NextOrgID != 0 || doc.NextKeyID != 0 {
				t.Fatalf("decoded document = %+v, want the valid v2 control", doc)
			}
		})
	}
}

// failGetCAS makes every document read fail like an unreachable store.
type failGetCAS struct{ s3wal.ConditionalStore }

var errStoreDown = errors.New("store down")

func (failGetCAS) Get(context.Context, string) (s3wal.Object, error) {
	return s3wal.Object{}, errStoreDown
}

// TestCheckFormat: CheckFormat types a wrong format and only that (issue
// #445). It reads without writing, and backends without a format version
// pass without I/O.
func TestCheckFormat(t *testing.T) {
	const v1 = `{"v":1,"seq":1,"writer":"0123456789abcdef0123456789abcdef","next_org_id":0,"next_key_id":0}`
	var fv *FormatVersionError
	for _, tc := range []struct {
		name     string
		seed     string // "" leaves the document missing
		down     bool
		wantType bool
		wantErr  string
	}{
		{name: "missing document"},
		{name: "v2", seed: `{"v":2,"seq":1,"writer":"0123456789abcdef0123456789abcdef","next_org_id":0,"next_key_id":0}`},
		{name: "v1", seed: v1, wantType: true, wantErr: "convert --to 2"},
		{name: "v3", seed: `{"v":3,"seq":1}`, wantType: true, wantErr: "document version 3 unsupported (want 2)"},
		{name: "corrupt", seed: `{`, wantErr: "document decode"},
		{name: "store down", down: true, wantErr: errStoreDown.Error()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := s3wal.NewMemCAS()
			if tc.seed != "" {
				if _, err := mem.Create(t.Context(), s3walMetaKey, []byte(tc.seed)); err != nil {
					t.Fatal(err)
				}
			}
			var cas s3wal.ConditionalStore = mem
			if tc.down {
				cas = failGetCAS{mem}
			}
			st := NewS3WAL(cas)
			t.Cleanup(st.Close)
			before, _ := mem.Get(t.Context(), s3walMetaKey)
			err := CheckFormat(t.Context(), st)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("CheckFormat = %v, want %q", err, tc.wantErr)
			}
			if got := errors.As(err, &fv); got != tc.wantType {
				t.Fatalf("CheckFormat error %v: typed = %v, want %v", err, got, tc.wantType)
			}
			after, _ := mem.Get(t.Context(), s3walMetaKey)
			if !bytes.Equal(before.Body, after.Body) || before.ETag != after.ETag {
				t.Fatalf("CheckFormat changed the document: %q -> %q", before.Body, after.Body)
			}
		})
	}
	t.Run("sqlite", func(t *testing.T) {
		st := sqliteBackend(t)
		if err := CheckFormat(t.Context(), st); err != nil {
			t.Fatal(err)
		}
	})
}

// s3walBackend is the s3wal lane fixture: a fresh MemCAS-backed store per
// subtest (like the sqlite lane's fresh DB file).
func s3walBackend(t *testing.T) Backend {
	t.Helper()
	st := NewS3WAL(s3wal.NewMemCAS())
	t.Cleanup(st.Close) // joins the org-key refresher goroutine
	return st
}

// TestS3WALMetaConformance runs every shared Backend conformance body
// against the s3wal backend.
func TestS3WALMetaConformance(t *testing.T) {
	bodies := []struct {
		name string
		fn   func(*testing.T, Backend)
	}{
		{"UpsertOrgWithKeys", testUpsertOrgWithKeys},
		{"UpsertOrgWithKeysConcurrent", testUpsertOrgWithKeysConcurrent},
		{"PublicKeysForOrgUnknown", testPublicKeysForOrgUnknown},
		{"SigningKeys", testSigningKeys},
		{"CreateAndGetRepo", testCreateAndGetRepo},
		{"SoftDeleteRepo", testSoftDeleteRepo},
		{"UpdateRepoDefaultBranch", testUpdateRepoDefaultBranch},
		{"ListReposPagination", testListReposPagination},
		{"ListReposQFilter", testListReposQFilter},
		{"ListReposErrors", testListReposErrors},
		{"Webhooks", testWebhooks},
		{"GitCredentials", testGitCredentials},
		{"RepoUpstream", testRepoUpstream},
		{"CreateLinkedRepo", testCreateLinkedRepo},
		{"CreateState", testCreateState},
		{"SyncLifecycle", testSyncLifecycle},
		{"TryBeginSyncConcurrent", testTryBeginSyncConcurrent},
		{"SyncClaimLease", testSyncClaimLease},
		{"FinishSyncRequiresInFlightRun", testFinishSyncRequiresInFlightRun},
		{"RequestSyncPendingLifecycle", testRequestSyncPendingLifecycle},
		{"PushClaimLease", testPushClaimLease},
		{"PushClaimForceInheritance", testPushClaimForceInheritance},
		{"PushPendingLevels", testPushPendingLevels},
		{"PushLaneIndependentOfSyncLane", testPushLaneIndependent},
		{"SetRepoUpstreamForwardOnPush", testSetUpstreamForward},
		{"RepoPurgeMetaSeams", testRepoPurgeMetaSeams},
		{"ResolveRepoKey", testResolveRepoKey},
	}
	for _, body := range bodies {
		t.Run(body.name, func(t *testing.T) { body.fn(t, s3walBackend(t)) })
	}
}

// TestS3WALMetaWithNowFunc mirrors TestSQLiteWithNowFunc: WithNowFunc pins
// the store's clock, so claim-lease expiry is a pure function of the
// injected instant — the DST-harness contract.
func TestS3WALMetaWithNowFunc(t *testing.T) {
	ctx := context.Background()
	clock := time.Unix(1700000000, 0).UTC()
	s := NewS3WAL(s3wal.NewMemCAS(), WithNowFunc(func() time.Time { return clock }))
	t.Cleanup(s.Close)
	if err := s.InitSchema(ctx); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	if _, err := s.UpsertOrgWithKeys(ctx, "clock-org", []string{"-----BEGIN PUBLIC KEY-----\ndummy\n-----END PUBLIC KEY-----\n"}); err != nil {
		t.Fatalf("seed org: %v", err)
	}
	repo, err := s.CreateRepo(ctx, "clock-org", "https://example.com/clock.git", "main")
	if err != nil {
		t.Fatalf("CreateRepo: %v", err)
	}

	up, err := s.SetRepoUpstream(ctx, RepoUpstream{RepoID: repo.RepoID, Provider: "github", Owner: "acme", Name: "demo", UpstreamHost: "github.com"})
	if err != nil {
		t.Fatalf("SetRepoUpstream: %v", err)
	}
	if !up.CreatedAt.Equal(clock) {
		t.Errorf("CreatedAt = %v, want the injected instant %v", up.CreatedAt, clock)
	}

	// Claim under the injected clock: expiry lands exactly at clock+lease.
	row, claimed, stolenFrom, err := s.TryBeginSync(ctx, repo.RepoID, "holder-a", 10*time.Second)
	if err != nil || !claimed || stolenFrom != "" {
		t.Fatalf("claim = (%v, %q, %v), want clean claim", claimed, stolenFrom, err)
	}
	if want := clock.Add(10 * time.Second); !row.SyncClaimExpiresAt.Equal(want) {
		t.Errorf("expiry = %v, want %v (injected clock + lease)", row.SyncClaimExpiresAt, want)
	}

	// One tick before expiry the claim is refused; wall time never enters.
	clock = clock.Add(9 * time.Second)
	if _, claimed, stolenFrom, err := s.TryBeginSync(ctx, repo.RepoID, "holder-b", 10*time.Second); err != nil || claimed || stolenFrom != "" {
		t.Fatalf("pre-expiry claim = (%v, %q, %v), want refused", claimed, stolenFrom, err)
	}

	// At expiry (expires <= now) the claim is stolen deterministically.
	clock = clock.Add(1 * time.Second)
	row, claimed, stolenFrom, err = s.TryBeginSync(ctx, repo.RepoID, "holder-b", 10*time.Second)
	if err != nil || !claimed || stolenFrom != "holder-a" {
		t.Fatalf("at-expiry claim = (%v, %q, %v), want steal from holder-a", claimed, stolenFrom, err)
	}
	if want := clock.Add(10 * time.Second); !row.SyncClaimExpiresAt.Equal(want) {
		t.Errorf("stolen-claim expiry = %v, want %v", row.SyncClaimExpiresAt, want)
	}

	// The dead holder's late finish still fails the holder guard.
	if _, err := s.FinishSync(ctx, repo.RepoID, "holder-a", true); !errors.Is(err, ErrClaimLost) {
		t.Errorf("late FinishSync err = %v, want ErrClaimLost", err)
	}
}

// TestS3WALMetaRejectsInvalidUTF8 covers the codec-boundary write guard
// (ErrValueNotUTF8): values JSON would silently rewrite to U+FFFD are
// rejected before any document mutation, while reads with such arguments
// are clean misses.
func TestS3WALMetaRejectsInvalidUTF8(t *testing.T) {
	ctx := context.Background()
	s := NewS3WAL(s3wal.NewMemCAS())
	t.Cleanup(s.Close)
	const bad = "raw-\xff-bytes"

	if _, err := s.UpsertOrgWithKeys(ctx, bad, []string{"pem"}); !errors.Is(err, ErrValueNotUTF8) {
		t.Errorf("UpsertOrgWithKeys(bad name) err = %v, want ErrValueNotUTF8", err)
	}
	if _, err := s.UpsertOrgWithKeys(ctx, "org", []string{bad}); !errors.Is(err, ErrValueNotUTF8) {
		t.Errorf("UpsertOrgWithKeys(bad pem) err = %v, want ErrValueNotUTF8", err)
	}
	if _, err := s.UpsertOrgWithKeys(ctx, "org", []string{"pem"}); err != nil {
		t.Fatalf("seed org: %v", err)
	}
	if _, err := s.CreateRepo(ctx, "org", bad, "main"); !errors.Is(err, ErrValueNotUTF8) {
		t.Errorf("CreateRepo(bad url) err = %v, want ErrValueNotUTF8", err)
	}
	if _, err := s.CreateRepo(ctx, "org", "https://example.com/x.git", bad); !errors.Is(err, ErrValueNotUTF8) {
		t.Errorf("CreateRepo(bad branch) err = %v, want ErrValueNotUTF8", err)
	}
	repo, err := s.CreateRepo(ctx, "org", "https://example.com/x.git", "main")
	if err != nil {
		t.Fatalf("CreateRepo: %v", err)
	}
	if _, err := s.UpdateRepoDefaultBranch(ctx, "org", repo.URL, "main", bad); !errors.Is(err, ErrValueNotUTF8) {
		t.Errorf("UpdateRepoDefaultBranch(bad new) err = %v, want ErrValueNotUTF8", err)
	}
	if _, err := s.AddSigningKey(ctx, "org", bad); !errors.Is(err, ErrValueNotUTF8) {
		t.Errorf("AddSigningKey(bad text) err = %v, want ErrValueNotUTF8", err)
	}
	if _, err := s.CreateWebhook(ctx, "org", bad, "sec"); !errors.Is(err, ErrValueNotUTF8) {
		t.Errorf("CreateWebhook(bad url) err = %v, want ErrValueNotUTF8", err)
	}
	if _, err := s.CreateWebhook(ctx, "org", "https://x.test/h", bad); !errors.Is(err, ErrValueNotUTF8) {
		t.Errorf("CreateWebhook(bad secret) err = %v, want ErrValueNotUTF8", err)
	}
	if _, err := s.CreateGitCredential(ctx, GitCredential{RepoID: repo.RepoID, Password: bad}); !errors.Is(err, ErrValueNotUTF8) {
		t.Errorf("CreateGitCredential(bad password) err = %v, want ErrValueNotUTF8", err)
	}
	if _, err := s.SetRepoUpstream(ctx, RepoUpstream{RepoID: repo.RepoID, Provider: bad}); !errors.Is(err, ErrValueNotUTF8) {
		t.Errorf("SetRepoUpstream(bad provider) err = %v, want ErrValueNotUTF8", err)
	}
	if _, err := s.SetRepoUpstream(ctx, RepoUpstream{RepoID: repo.RepoID, Provider: "github", Owner: "o", Name: "n", UpstreamHost: "github.com"}); err != nil {
		t.Fatalf("SetRepoUpstream: %v", err)
	}
	if _, _, _, err := s.TryBeginSync(ctx, repo.RepoID, bad, time.Minute); !errors.Is(err, ErrValueNotUTF8) {
		t.Errorf("TryBeginSync(bad holder) err = %v, want ErrValueNotUTF8", err)
	}
	if _, _, _, _, err := s.TryBeginPush(ctx, repo.RepoID, bad, time.Minute); !errors.Is(err, ErrValueNotUTF8) {
		t.Errorf("TryBeginPush(bad holder) err = %v, want ErrValueNotUTF8", err)
	}

	// Reads with raw-byte arguments are clean misses, never errors.
	if _, err := s.GetRepoByURL(ctx, "org", bad); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetRepoByURL(bad url) err = %v, want ErrNotFound", err)
	}
	if _, err := s.PublicKeysForOrg(ctx, bad); !errors.Is(err, ErrNotFound) {
		t.Errorf("PublicKeysForOrg(bad org) err = %v, want ErrNotFound", err)
	}
}

// TestS3WALMetaSecretsEncryptedAtRest proves the shared secrets codec is
// applied unchanged: with a codec, webhook secrets and credential
// passwords are stored as envelopes in the raw document (never plaintext)
// and round-trip back to plaintext through the read paths.
func TestS3WALMetaSecretsEncryptedAtRest(t *testing.T) {
	ctx := context.Background()
	cas := s3wal.NewMemCAS()
	s := NewS3WAL(cas, WithSecretsCodec(newCodec(t, newTestKey(t))))
	t.Cleanup(s.Close)
	org := seedOrg(t, s)
	repo, err := s.CreateRepo(ctx, org, "https://example.com/enc.git", "main")
	if err != nil {
		t.Fatalf("CreateRepo: %v", err)
	}

	hook, err := s.CreateWebhook(ctx, org, "https://x.test/hook", "hook-s3cret")
	if err != nil {
		t.Fatalf("CreateWebhook: %v", err)
	}
	if _, err := s.CreateGitCredential(ctx, GitCredential{RepoID: repo.RepoID, Username: "bot", Password: "cred-s3cret"}); err != nil {
		t.Fatalf("CreateGitCredential: %v", err)
	}

	// The raw document must carry envelopes, never the plaintext.
	obj, err := cas.Get(ctx, s3walMetaKey)
	if err != nil {
		t.Fatalf("raw get: %v", err)
	}
	for _, secret := range []string{"hook-s3cret", "cred-s3cret"} {
		if bytes.Contains(obj.Body, []byte(secret)) {
			t.Errorf("raw document contains plaintext %q", secret)
		}
	}

	// Read paths hand plaintext back.
	if got, err := s.GetWebhook(ctx, hook.ID); err != nil || got.Secret != "hook-s3cret" {
		t.Errorf("GetWebhook = (%+v, %v), want plaintext secret", got, err)
	}
	if got, err := s.GitCredentialForRepo(ctx, repo.RepoID); err != nil || got.Password != "cred-s3cret" {
		t.Errorf("GitCredentialForRepo = (%+v, %v), want plaintext password", got, err)
	}
	// Reserved-prefix writes are rejected like the SQL twins.
	if _, err := s.CreateWebhook(ctx, org, "https://x.test/h2", "enc:v1:forged"); !errors.Is(err, ErrReservedSecret) {
		t.Errorf("reserved-prefix secret err = %v, want ErrReservedSecret", err)
	}
}
