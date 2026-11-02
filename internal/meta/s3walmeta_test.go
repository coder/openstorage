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
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/ref/s3wal"
)

// TestS3WALMetaDecode isolates format rejection from the other document guards.
func TestS3WALMetaDecode(t *testing.T) {
	const fields = `"seq":1,"writer":"0123456789abcdef0123456789abcdef","next_org_id":0,"next_key_id":0}`
	for _, tc := range []struct{ name, body, wantErr string }{
		{"malformed JSON", `{`, "s3walmeta: _wal/meta.json is corrupt (document decode:"},
		{"seq zero", `{"v":2,"seq":0,"writer":"w"}`, "document seq 0 invalid"},
		{"no writer", `{"v":2,"seq":1}`, "document seq 1 has no writer nonce"},
		// Impossible id counters (issue #478): typed, never a panic.
		{"negative org counter", `{"v":2,"seq":1,"writer":"w","next_org_id":-1,"next_key_id":0}`, "document seq 1 has negative id counters (-1, 0)"},
		{"negative key counter", `{"v":2,"seq":1,"writer":"w","next_org_id":0,"next_key_id":-3}`, "document seq 1 has negative id counters (0, -3)"},
		{"org id zero", `{"v":2,"seq":1,"writer":"w","next_org_id":1,"next_key_id":0,"orgs":{"o":{"id":0,"created_at":1,"public_keys":[]}}}`, `document seq 1: org "o" has id 0 outside 1..1`},
		{"org id above counter", `{"v":2,"seq":1,"writer":"w","next_org_id":1,"next_key_id":0,"orgs":{"o":{"id":2,"created_at":1,"public_keys":[]}}}`, `document seq 1: org "o" has id 2 outside 1..1`},
		{"duplicate org id", `{"v":2,"seq":1,"writer":"w","next_org_id":2,"next_key_id":0,"orgs":{"a":{"id":1,"created_at":1,"public_keys":[]},"b":{"id":1,"created_at":1,"public_keys":[]}}}`, "document seq 1: org id 1 is used twice"},
		{"key id above counter", `{"v":2,"seq":1,"writer":"w","next_org_id":1,"next_key_id":1,"orgs":{"o":{"id":1,"created_at":1,"public_keys":[],"signing_keys":[{"id":2,"key_text":"k","created_at":1}]}}}`, `document seq 1: org "o" signing key id 2 outside 1..1`},
		{"null org row", `{"v":2,"seq":1,"writer":"w","next_org_id":1,"next_key_id":0,"orgs":{"o":null}}`, `document seq 1: org "o" has no row`},
		{"duplicate key id", `{"v":2,"seq":1,"writer":"w","next_org_id":2,"next_key_id":1,"orgs":{"a":{"id":1,"created_at":1,"public_keys":[],"signing_keys":[{"id":1,"key_text":"k","created_at":1}]},"b":{"id":2,"created_at":1,"public_keys":[],"signing_keys":[{"id":1,"key_text":"k","created_at":1}]}}}`, "document seq 1: signing key id 1 is used twice"},
		{"missing version", `{` + fields, "s3walmeta: document version 0 unsupported (want 2)"},
		{"zero version", `{"v":0,` + fields, "s3walmeta: document version 0 unsupported (want 2)"},
		{"v1 needs conversion", `{"v":1,` + fields, "s3walmeta: document version 1 needs conversion: stop serve, then run `openstorage s3wal-meta convert --to 2`"},
		{"foreign version", `{"v":3,` + fields, "s3walmeta: document version 3 unsupported (want 2)"},
		{"valid v2", `{"v":2,` + fields, ""},
		{"valid v2 with ids", `{"v":2,"seq":1,"writer":"w","next_org_id":3,"next_key_id":2,"orgs":{"a":{"id":1,"created_at":1,"public_keys":[],"signing_keys":[{"id":2,"key_text":"k","created_at":1}]},"b":{"id":3,"created_at":1,"public_keys":[]}}}`, ""},
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
				// Every rejection is typed (issues #445, #466): the hook
				// refuses on both types and fails open only on store errors.
				var fv *FormatVersionError
				var cd *CorruptDocumentError
				version := strings.Contains(tc.wantErr, "version")
				isVersion, isCorrupt := errors.As(err, &fv), errors.As(err, &cd)
				if isVersion != version || isCorrupt == version {
					t.Fatalf("decode error %v: version-typed = %v, corrupt-typed = %v", err, isVersion, isCorrupt)
				}
				for _, want := range []string{"_wal/meta.json is corrupt", "backup-restore-s3wal.md#restore-and-validate-before-cutover"} {
					if isCorrupt && !strings.Contains(err.Error(), want) {
						t.Fatalf("corrupt-document error %q lacks %q", err, want)
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.name == "valid v2 with ids" {
				if doc == nil || doc.NextOrgID != 3 || doc.NextKeyID != 2 || len(doc.Orgs) != 2 {
					t.Fatalf("decoded document = %+v, want the valid control with ids", doc)
				}
				return
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

// TestCheckFormat: CheckFormat types a wrong format (issue #445) and a
// corrupt document (issue #466), never a store error. It reads without
// writing, and backends without a format version pass without I/O.
func TestCheckFormat(t *testing.T) {
	const v1 = `{"v":1,"seq":1,"writer":"0123456789abcdef0123456789abcdef","next_org_id":0,"next_key_id":0}`
	var fv *FormatVersionError
	var cd *CorruptDocumentError
	for _, tc := range []struct {
		name        string
		seed        string // "" leaves the document missing
		down        bool
		wantType    bool // a *FormatVersionError
		wantCorrupt bool // a *CorruptDocumentError
		wantErr     string
	}{
		{name: "missing document"},
		{name: "v2", seed: `{"v":2,"seq":1,"writer":"0123456789abcdef0123456789abcdef","next_org_id":0,"next_key_id":0}`},
		{name: "v1", seed: v1, wantType: true, wantErr: "convert --to 2"},
		{name: "v3", seed: `{"v":3,"seq":1}`, wantType: true, wantErr: "document version 3 unsupported (want 2)"},
		{name: "corrupt", seed: `{`, wantCorrupt: true, wantErr: "document decode"},
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
			if got := errors.As(err, &cd); got != tc.wantCorrupt {
				t.Fatalf("CheckFormat error %v: corrupt-typed = %v, want %v", err, got, tc.wantCorrupt)
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

// countGetsCAS counts document reads, so a test can wait for the org-key
// refresher to run.
type countGetsCAS struct {
	s3wal.ConditionalStore
	reads atomic.Int32
}

func (c *countGetsCAS) Get(ctx context.Context, key string) (s3wal.Object, error) {
	c.reads.Add(1)
	return c.ConditionalStore.Get(ctx, key)
}

func (c *countGetsCAS) GetIfChanged(ctx context.Context, key, etag string) (s3wal.Object, error) {
	c.reads.Add(1)
	return c.ConditionalStore.GetIfChanged(ctx, key, etag)
}

// TestS3WALMetaCorruptCountersNoPanic (issue #478): a stored document with
// a negative id counter is damaged input, so the store returns the typed
// error to every reader, and its org-key refresher keeps running. Before
// the fix, the decode assertion panicked serve and the hook.
func TestS3WALMetaCorruptCountersNoPanic(t *testing.T) {
	mem := s3wal.NewMemCAS()
	body := []byte(`{"v":2,"seq":4,"writer":"0123456789abcdef0123456789abcdef","next_org_id":-1,"next_key_id":0}`)
	if _, err := mem.Create(t.Context(), s3walMetaKey, body); err != nil {
		t.Fatal(err)
	}
	cas := &countGetsCAS{ConditionalStore: mem}
	st := newS3WALStore(cas, nil, nil, resolveOptions([]Option{withKeyRefreshInterval(time.Millisecond)}))
	t.Cleanup(st.Close)
	var cd *CorruptDocumentError
	if _, err := st.GetRepoByID(t.Context(), "o", "repo-1"); !errors.As(err, &cd) {
		t.Fatalf("GetRepoByID = %v, want a *CorruptDocumentError", err)
	}
	if err := CheckFormat(t.Context(), st); !errors.As(err, &cd) || !strings.Contains(err.Error(), "negative id counters") {
		t.Fatalf("CheckFormat = %v, want the negative-counter *CorruptDocumentError", err)
	}
	waitReads(t, cas, 3) // a panicking refresher kills the test binary; a stopped one fails (issue #487)
	got, _ := mem.Get(t.Context(), s3walMetaKey)
	if !bytes.Equal(got.Body, body) {
		t.Fatalf("the store changed the document: %s", got.Body)
	}
}

// waitReads waits until cas has served n more document reads, failing the
// test instead of hanging when the refresher stops reading.
func waitReads(t *testing.T, cas *countGetsCAS, n int32) {
	t.Helper()
	start := cas.reads.Load()
	for deadline := time.Now().Add(10 * time.Second); cas.reads.Load() < start+n; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("the org-key refresher made %d of %d reads in 10 s", cas.reads.Load()-start, n)
		}
	}
}

// TestS3WALMetaStaleDocumentNoPanic (issue #488): when the stored document
// goes back to an older seq while the process runs (a stale read, or the
// object replaced out of band), every read and write returns the typed
// error and writes nothing, and the org-key refresher keeps running. Before
// the fix, the watermark assertion panicked serve on the refresher's next
// tick. A document at or past the watermark is served again.
func TestS3WALMetaStaleDocumentNoPanic(t *testing.T) {
	ctx := t.Context()
	mem := s3wal.NewMemCAS()
	cas := &countGetsCAS{ConditionalStore: mem}
	st := newS3WALStore(cas, nil, nil, resolveOptions([]Option{withKeyRefreshInterval(time.Millisecond)}))
	t.Cleanup(st.Close)
	if _, err := st.UpsertOrgWithKeys(ctx, "o", []string{"pem"}); err != nil {
		t.Fatal(err)
	}
	old, err := mem.Get(ctx, s3walMetaKey)
	if err != nil {
		t.Fatal(err)
	}
	repo, err := st.CreateRepo(ctx, "o", "https://example.com/stale.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	newest, err := mem.Get(ctx, s3walMetaKey)
	if err != nil {
		t.Fatal(err)
	}
	// Put the older document back, as a restore into the live bucket would.
	if _, err := mem.Advance(ctx, s3walMetaKey, old.Body, newest.ETag); err != nil {
		t.Fatal(err)
	}
	replaced, _ := mem.Get(ctx, s3walMetaKey)

	var stale *StaleDocumentError
	if _, err := st.GetRepoByID(ctx, "o", repo.RepoID); !errors.As(err, &stale) || stale.Got != 1 || stale.Seen != 2 {
		t.Fatalf("GetRepoByID = %v, want a *StaleDocumentError for seq 1 after seq 2", err)
	}
	for _, want := range []string{"_wal/meta.json at seq 1 does not continue the document this process last saw at seq 2", "backup-restore-s3wal.md#restore-and-validate-before-cutover"} {
		if !strings.Contains(stale.Error(), want) {
			t.Fatalf("error %q lacks %q", stale, want)
		}
	}
	if _, err := st.CreateRepo(ctx, "o", "https://example.com/other.git", "main"); !errors.As(err, &stale) {
		t.Fatalf("CreateRepo = %v, want a *StaleDocumentError", err)
	}
	waitReads(t, cas, 3) // a panicking refresher kills the test binary
	if got, _ := mem.Get(ctx, s3walMetaKey); got.ETag != replaced.ETag {
		t.Fatalf("the store wrote through the older document: %s", got.Body)
	}

	// A fresh process (a hook) advances the restored lineage back to seq 2
	// with different contents. The numeric catch-up must not end the
	// refusal: the regression latches (Codex P1 on #496).
	fresh := NewS3WAL(mem)
	t.Cleanup(fresh.Close)
	if _, err := fresh.CreateRepo(ctx, "o", "https://example.com/fork.git", "main"); err != nil {
		t.Fatal(err)
	}
	forked, _ := mem.Get(ctx, s3walMetaKey)
	if _, err := st.GetRepoByID(ctx, "o", repo.RepoID); !errors.As(err, &stale) || stale.Got != 2 || stale.Seen != 2 {
		t.Fatalf("GetRepoByID on the forked seq 2 = %v, want a *StaleDocumentError", err)
	}

	// Same seq and writer nonce as the cached document, other rows (an
	// out-of-band edit that keeps the version fields), and the cached
	// document reformatted: neither is the same bytes, so neither ends
	// the refusal (Codex P2 on #496).
	var edited map[string]any
	if err := json.Unmarshal(newest.Body, &edited); err != nil {
		t.Fatal(err)
	}
	delete(edited, "repos")
	editedBody, err := json.Marshal(edited)
	if err != nil {
		t.Fatal(err)
	}
	var indented bytes.Buffer
	if err := json.Indent(&indented, newest.Body, "", "  "); err != nil {
		t.Fatal(err)
	}
	prev := forked.ETag
	for name, body := range map[string][]byte{"edited": editedBody, "reformatted": indented.Bytes()} {
		etag, err := mem.Advance(ctx, s3walMetaKey, body, prev)
		if err != nil {
			t.Fatal(err)
		}
		prev = etag
		if _, err := st.GetRepoByID(ctx, "o", repo.RepoID); !errors.As(err, &stale) || stale.Got != 2 {
			t.Fatalf("GetRepoByID on the %s seq 2 = %v, want a *StaleDocumentError", name, err)
		}
	}

	// The exact document this process last saw is served again (a stale
	// read that the store later corrects): reads succeed.
	if _, err := mem.Advance(ctx, s3walMetaKey, newest.Body, prev); err != nil {
		t.Fatal(err)
	}
	if got, err := st.GetRepoByID(ctx, "o", repo.RepoID); err != nil || got.RepoID != repo.RepoID {
		t.Fatalf("GetRepoByID after the store caught up = %+v, %v", got, err)
	}
}

// TestS3WALMetaVanishedDocumentLatches (Codex P2 on #496): when meta.json
// vanishes while the process runs and another process recreates a new
// lineage past the cached seq, the process keeps refusing it. Only the
// cached document itself ends the refusal.
func TestS3WALMetaVanishedDocumentLatches(t *testing.T) {
	ctx := t.Context()
	mem := s3wal.NewMemCAS()
	st := NewS3WAL(mem)
	t.Cleanup(st.Close)
	if _, err := st.UpsertOrgWithKeys(ctx, "o", []string{"pem"}); err != nil {
		t.Fatal(err)
	}
	repo, err := st.CreateRepo(ctx, "o", "https://example.com/vanish.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	cached, _ := mem.Get(ctx, s3walMetaKey) // seq 2
	if err := mem.Delete(ctx, s3walMetaKey); err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetRepoByID(ctx, "o", repo.RepoID); err == nil || !strings.Contains(err.Error(), "vanished after seq 2") {
		t.Fatalf("GetRepoByID after the vanish = %v, want the vanished error", err)
	}
	// A fresh process recreates the document and advances it to seq 3.
	fresh := NewS3WAL(mem)
	t.Cleanup(fresh.Close)
	for _, name := range []string{"a", "b", "c"} {
		if _, err := fresh.UpsertOrgWithKeys(ctx, name, []string{"pem"}); err != nil {
			t.Fatal(err)
		}
	}
	recreated, _ := mem.Get(ctx, s3walMetaKey)
	var stale *StaleDocumentError
	if _, err := st.GetRepoByID(ctx, "o", repo.RepoID); !errors.As(err, &stale) || stale.Got != 3 || stale.Seen != 2 {
		t.Fatalf("GetRepoByID on the recreated seq 3 = %v, want a *StaleDocumentError", err)
	}
	if _, err := st.CreateRepo(ctx, "o", "https://example.com/other.git", "main"); !errors.As(err, &stale) {
		t.Fatalf("CreateRepo = %v, want a *StaleDocumentError", err)
	}
	if got, _ := mem.Get(ctx, s3walMetaKey); got.ETag != recreated.ETag {
		t.Fatal("the store wrote through the recreated document")
	}
	if _, err := mem.Advance(ctx, s3walMetaKey, cached.Body, recreated.ETag); err != nil {
		t.Fatal(err)
	}
	if got, err := st.GetRepoByID(ctx, "o", repo.RepoID); err != nil || got.RepoID != repo.RepoID {
		t.Fatalf("GetRepoByID after the cached document returned = %+v, %v", got, err)
	}
}

// TestS3WALMetaUndecodableReplacementLatches (issue #501): when the cached
// meta.json changes into bytes that do not decode (corrupt, or a wrong
// format version) and a valid document from another lineage at a higher
// seq replaces those, the process keeps refusing. Only the cached
// document itself ends the refusal.
func TestS3WALMetaUndecodableReplacementLatches(t *testing.T) {
	for _, tc := range []struct{ name, bad, wantErr string }{
		{"corrupt", `{"v":2,"seq":`, "_wal/meta.json is corrupt"},
		{"v1", `{"v":1,"seq":2,"writer":"0123456789abcdef0123456789abcdef","next_org_id":1,"next_key_id":0}`, "document version 1 needs conversion"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			mem := s3wal.NewMemCAS()
			st := NewS3WAL(mem)
			t.Cleanup(st.Close)
			if _, err := st.UpsertOrgWithKeys(ctx, "o", []string{"pem"}); err != nil {
				t.Fatal(err)
			}
			repo, err := st.CreateRepo(ctx, "o", "https://example.com/undecodable.git", "main")
			if err != nil {
				t.Fatal(err)
			}
			cached, _ := mem.Get(ctx, s3walMetaKey) // seq 2
			etag, err := mem.Advance(ctx, s3walMetaKey, []byte(tc.bad), cached.ETag)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := st.GetRepoByID(ctx, "o", repo.RepoID); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("GetRepoByID on the %s bytes = %v, want %q", tc.name, err, tc.wantErr)
			}
			// A valid document from another lineage at seq 5: the cached
			// rows with a foreign writer nonce.
			var foreign map[string]any
			if err := json.Unmarshal(cached.Body, &foreign); err != nil {
				t.Fatal(err)
			}
			foreign["seq"], foreign["writer"] = 5, "fedcba9876543210fedcba9876543210"
			foreignBody, err := json.Marshal(foreign)
			if err != nil {
				t.Fatal(err)
			}
			if etag, err = mem.Advance(ctx, s3walMetaKey, foreignBody, etag); err != nil {
				t.Fatal(err)
			}
			var stale *StaleDocumentError
			if _, err := st.GetRepoByID(ctx, "o", repo.RepoID); !errors.As(err, &stale) || stale.Got != 5 || stale.Seen != 2 {
				t.Fatalf("GetRepoByID on the foreign seq 5 = %v, want a *StaleDocumentError", err)
			}
			if _, err := st.CreateRepo(ctx, "o", "https://example.com/other.git", "main"); !errors.As(err, &stale) {
				t.Fatalf("CreateRepo = %v, want a *StaleDocumentError", err)
			}
			if got, _ := mem.Get(ctx, s3walMetaKey); got.ETag != etag {
				t.Fatal("the store wrote through the foreign document")
			}
			if _, err := mem.Advance(ctx, s3walMetaKey, cached.Body, etag); err != nil {
				t.Fatal(err)
			}
			if got, err := st.GetRepoByID(ctx, "o", repo.RepoID); err != nil || got.RepoID != repo.RepoID {
				t.Fatalf("GetRepoByID after the cached document returned = %+v, %v", got, err)
			}
		})
	}
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
		{"ListReposPrefixFilter", testListReposPrefixFilter},
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
